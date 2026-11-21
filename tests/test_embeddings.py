"""Embedding batching/reuse/deadlines preserve model and cache integrity."""

import asyncio

import pytest
import pytest_asyncio

from adapters.providers import EmbeddingValidationError, ProviderAdapter
from core.embeddings import EmbeddingService
from storage.cache_store import CacheStore, text_hash
from tests.test_providers import FakeContext, FakeEmbedding


@pytest_asyncio.fixture
async def cache(tmp_path):
    store = CacheStore(tmp_path / "cache.db")
    await store.open()
    try:
        yield store
    finally:
        await store.close()


def service(cache, *, context=None, batch_size=16, revision=""):
    context = context or FakeContext()
    return EmbeddingService(
        ProviderAdapter(context), cache, provider_id="embedding-test",
        batch_size=batch_size, revision=revision,
    ), context


@pytest.mark.asyncio
async def test_batches_deduplicate_reuse_and_respect_embedding_concurrency(cache):
    embedding, context = service(cache)
    texts = [str(index) for index in range(35)]
    result = await embedding.embed(texts + texts[:2], namespace="space")
    assert len(result) == 37
    assert result[:2] == result[-2:]
    assert [len(call) for call in context.embedding.calls] == [16, 16, 3]
    assert context.embedding.maximum_active == 2
    assert await embedding.embed(texts, namespace="space") == result[:35]
    assert len(context.embedding.calls) == 3
    # Returned vectors do not share mutable list objects with cached callers.
    result[0][0] = 999
    assert (await embedding.cached(texts[:1], namespace="space"))[0][0] != 999


@pytest.mark.asyncio
async def test_cache_only_read_never_calls_provider(cache):
    embedding, context = service(cache)
    assert await embedding.cached(["current", "history"], namespace="space") == [None, None]
    assert context.embedding.calls == []
    await embedding.embed(["history"], namespace="space")
    cached = await embedding.cached(["current", "history"], namespace="space")
    assert cached[0] is None and cached[1] is not None
    assert context.embedding.calls == [["history"]]


@pytest.mark.asyncio
async def test_same_text_does_not_reuse_across_spaces_models_or_revisions(cache):
    embedding, context = service(cache)
    await embedding.embed(["text"], namespace="one")
    await embedding.embed(["text"], namespace="two")
    before = embedding.fingerprint
    context.embedding.model = "new-model"
    assert embedding.fingerprint != before
    await embedding.embed(["text"], namespace="one")
    embedding.revision = "manually-invalidated"
    await embedding.embed(["text"], namespace="one")
    assert len(context.embedding.calls) == 4


@pytest.mark.asyncio
async def test_node_identity_cache_and_same_space_message_reuse(cache):
    embedding, context = service(cache)
    await embedding.embed(["quoted message"], namespace="space")
    vectors = await embedding.embed(
        ["quoted message", "new node", "new node"], namespace="space", kind="node",
        identities=[("a", 1), ("b", 1), ("c", 1)],
    )
    assert context.embedding.calls == [["quoted message"], ["new node"]]
    assert vectors[1] == vectors[2]
    await embedding.embed(["new node"], namespace="space", kind="node", identities=[("b", 2)])
    assert context.embedding.calls == [["quoted message"], ["new node"]]
    assert (await cache.health())["counts"] == {"message": 1, "node": 4}


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["text", "space", "model", "revision"])
async def test_node_text_fallback_does_not_cross_embedding_inputs(cache, change):
    embedding, context = service(cache)
    await embedding.embed(["original"], namespace="space", kind="node", identities=[("node", 1)])
    text, space = ("changed" if change == "text" else "original"), ("other" if change == "space" else "space")
    if change == "model":
        context.embedding.model = "other-model"
    elif change == "revision":
        embedding.revision = "other-revision"
    assert await embedding.cached([text], namespace=space, kind="node", identities=[("node", 2)]) == [None]
    await embedding.embed([text], namespace=space, kind="node", identities=[("node", 2)])
    assert context.embedding.calls == [["original"], [text]]


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup", ["expired", "evicted"])
async def test_node_text_reuse_obeys_expiration_and_capacity_cleanup(cache, cleanup):
    now = [100.0]
    cache.clock = lambda: now[0]
    cache.ttls["node"] = 10
    embedding, context = service(cache)
    await embedding.embed(["original"], namespace="space", kind="node", identities=[("node", 1)])
    if cleanup == "expired":
        now[0] = 110
        assert await embedding.cached(["original"], namespace="space", kind="node", identities=[("node", 2)]) == [None]
    else:
        cache.limits["node"] = 0
    await cache.cleanup()
    cache.limits["node"] = 10
    assert (await cache.health())["entry_count"] == 0
    assert await embedding.cached(["original"], namespace="space", kind="node", identities=[("node", 2)]) == [None]
    await embedding.embed(["original"], namespace="space", kind="node", identities=[("node", 2)])
    assert context.embedding.calls == [["original"], ["original"]]


@pytest.mark.asyncio
async def test_fully_reused_message_vectors_are_saved_under_node_version_keys(cache):
    embedding, context = service(cache)
    await embedding.embed(["quoted message"], namespace="space")
    await embedding.embed(["quoted message"], namespace="space", kind="node", identities=[("a", 1)])
    assert context.embedding.calls == [["quoted message"]]
    assert (await cache.health())["counts"] == {"message": 1, "node": 1}
    assert (await cache.get_vectors(
        embedding.fingerprint, "space", [text_hash("quoted message")],
        kind="node", identities=[("a", 1)],
    ))[0] is not None


@pytest.mark.asyncio
async def test_batch_size_one_supports_single_input_provider_without_retry(cache):
    embedding, context = service(cache, batch_size=1)

    def only_single(texts):
        if len(texts) != 1:
            raise ValueError("this provider supports one input")
        return [[1, 2]]

    context.embedding.response = only_single
    assert len(await embedding.embed(["a", "b"], namespace="space")) == 2
    assert context.embedding.calls == [["a"], ["b"]]


@pytest.mark.asyncio
async def test_overlapping_calls_reuse_after_waiting_for_first_call(cache):
    embedding, context = service(cache)
    context.embedding.gate = asyncio.Event()
    first = asyncio.create_task(embedding.embed(["text"], namespace="space"))
    await context.embedding.started.wait()
    second = asyncio.create_task(embedding.embed(["text"], namespace="space"))
    context.embedding.gate.set()
    a, b = await asyncio.gather(first, second)
    assert a == b
    assert context.embedding.calls == [["text"]]


@pytest.mark.asyncio
async def test_waiting_for_reuse_is_bounded_by_request_deadline(cache):
    embedding, context = service(cache)
    context.embedding.gate = asyncio.Event()
    first = asyncio.create_task(embedding.embed(["text"], namespace="space"))
    await context.embedding.started.wait()
    try:
        diagnostics = {}
        with pytest.raises(TimeoutError):
            await embedding.embed(["other"], namespace="space", timeout_seconds=0.02, diagnostics=diagnostics)
        assert context.embedding.calls == [["text"]]
        assert diagnostics["failure_stage"] == "service_queue"
        assert diagnostics["service_queue_seconds"] >= .01
        assert diagnostics["planned_batch_count"] == 0 and diagnostics["provider_call_count"] == 0
        assert diagnostics["provider_queue_seconds_sum"] == diagnostics["provider_call_seconds_sum"] == 0
    finally:
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
    context.embedding.gate.set()
    await embedding.embed(["recovered"], namespace="space")


@pytest.mark.asyncio
async def test_embedding_diagnostics_are_per_call_and_count_cold_hot_batches(cache):
    embedding, context = service(cache, batch_size=2)
    cold, hot = {}, {}
    texts = ["独立甲", "独立乙", "独立丙", "独立甲"]
    await embedding.embed(texts, namespace="space", diagnostics=cold)
    saved_cold = dict(cold)
    await embedding.embed(texts, namespace="space", diagnostics=hot)
    assert cold == saved_cold
    assert cold["planned_batch_count"] == cold["provider_call_count"] == cold["provider_completed_batch_count"] == 2
    assert cold["cache_hit_count"] == 0
    assert hot["cache_hit_count"] == 4
    assert hot["planned_batch_count"] == hot["provider_call_count"] == 0
    assert hot["provider_queue_seconds_sum"] == hot["provider_call_seconds_sum"] == 0
    assert cold["provider_call_seconds_sum"] > 0 and cold["service_total_seconds"] > 0
    assert len(context.embedding.calls) == 2


@pytest.mark.asyncio
async def test_embedding_batch_queue_and_provider_call_are_separately_visible(cache):
    embedding, context = service(cache, batch_size=1)
    await embedding.adapter.reconfigure(llm_concurrency=2, embedding_concurrency=1)
    context.embedding.gate = asyncio.Event()
    diagnostics = {}
    with pytest.raises(TimeoutError):
        await embedding.embed(["独立甲", "独立乙"], namespace="space", timeout_seconds=.05, diagnostics=diagnostics)
    assert diagnostics["failure_stage"] == "provider_batches"
    assert diagnostics["error_type"] == "TimeoutError"
    assert diagnostics["planned_batch_count"] == 2
    assert diagnostics["provider_call_count"] == 1
    assert diagnostics["provider_completed_batch_count"] == 0
    assert diagnostics["provider_failure_stages"] == ["provider_call", "provider_queue"]
    assert diagnostics["provider_queue_seconds_sum"] >= .01
    assert diagnostics["provider_call_seconds_sum"] >= .01
    assert len(context.embedding.calls) == 1 and context.embedding.active == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["timeout", "cancel", "provider_error", "invalid_batch"])
async def test_failed_call_awaits_all_batches_and_writes_no_partial_cache(cache, failure):
    embedding, context = service(cache, batch_size=1)
    if failure in {"timeout", "cancel"}:
        context.embedding.gate = asyncio.Event()
    elif failure == "provider_error":
        context.embedding.failure = ConnectionError("temporary outage")
    else:
        context.embedding.response = lambda texts: [[1, 2]] if texts == ["a"] else [[0, 0]]
    task = asyncio.create_task(embedding.embed(
        ["a", "b", "c"], namespace="space", timeout_seconds=0.02 if failure == "timeout" else 30
    ))
    if failure == "cancel":
        await context.embedding.started.wait()
        task.cancel()
    expected = {
        "timeout": TimeoutError,
        "cancel": asyncio.CancelledError,
        "provider_error": ConnectionError,
        "invalid_batch": EmbeddingValidationError,
    }[failure]
    with pytest.raises(expected):
        await task
    assert context.embedding.active == 0
    assert (await cache.health())["entry_count"] == 0
    assert embedding._lock.locked() is False


@pytest.mark.asyncio
async def test_dynamic_dimension_is_consistent_across_batches_and_restart(tmp_path):
    path = tmp_path / "cache.db"
    cache = CacheStore(path)
    await cache.open()
    context = FakeContext(FakeEmbedding(dimension=0))
    embedding, _ = service(cache, context=context, batch_size=1)
    first = await embedding.embed(["a"], namespace="space")
    fingerprint = embedding.fingerprint
    assert await cache.get_dimension(fingerprint) == 2
    await cache.close()
    cache = CacheStore(path)
    await cache.open()
    restarted, _ = service(cache, context=context, batch_size=1)
    try:
        assert await restarted.embed(["a"], namespace="space") == first
        assert context.embedding.calls == [["a"]]
        context.embedding.response = [[1, 2, 3]]
        with pytest.raises(EmbeddingValidationError, match="dimension"):
            await restarted.embed(["b"], namespace="other-space")
        assert await cache.get_vectors(fingerprint, "other-space", [text_hash("b")]) == [None]
        context.embedding.response = lambda texts: [[1, 2]] if texts == ["c"] else [[1, 2, 3]]
        with pytest.raises(EmbeddingValidationError, match="dimension"):
            await restarted.embed(["c", "d"], namespace="space")
        assert (await cache.health())["entry_count"] == 1
    finally:
        await cache.close()


@pytest.mark.asyncio
async def test_model_change_during_network_call_is_not_cached(cache):
    embedding, context = service(cache)
    context.embedding.gate = asyncio.Event()
    task = asyncio.create_task(embedding.embed(["text"], namespace="space"))
    await context.embedding.started.wait()
    context.embedding.model = "changed-mid-flight"
    context.embedding.gate.set()
    with pytest.raises(EmbeddingValidationError, match="configuration changed"):
        await task
    assert (await cache.health())["entry_count"] == 0


@pytest.mark.asyncio
async def test_cache_dimension_conflict_from_other_service_rolls_back(cache, monkeypatch):
    context = FakeContext(FakeEmbedding(dimension=0))
    embedding, _ = service(cache, context=context)
    original = cache.put_vectors

    async def conflicting_write(fingerprint, namespace, hashes, vectors, **kwargs):
        await original(fingerprint, "other-space", [text_hash("other")], [[1, 2, 3]])
        await original(fingerprint, namespace, hashes, vectors, **kwargs)

    monkeypatch.setattr(cache, "put_vectors", conflicting_write)
    with pytest.raises(ValueError, match="dimension"):
        await embedding.embed(["text"], namespace="space")
    assert await cache.get_vectors(embedding.fingerprint, "space", [text_hash("text")]) == [None]
    assert (await cache.health())["entry_count"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["cached", "network_result"])
async def test_normalization_worker_cancellation_releases_lock_without_late_writes(cache, monkeypatch, phase):
    import threading

    import core.embeddings as module

    embedding, context = service(cache)
    loop = asyncio.get_running_loop()
    main_thread = threading.get_ident()
    started, finished = asyncio.Event(), asyncio.Event()
    release = threading.Event()
    helper = "_normalize_cached_vectors" if phase == "cached" else "normalize_vectors"
    original = getattr(module, helper)

    def paused_worker(vectors, *args, **kwargs):
        if phase == "network_result" and not vectors:
            return original(vectors, *args, **kwargs)
        try:
            assert threading.get_ident() != main_thread
            loop.call_soon_threadsafe(started.set)
            assert release.wait(5), "test did not release normalization"
            return original(vectors, *args, **kwargs)
        finally:
            loop.call_soon_threadsafe(finished.set)

    monkeypatch.setattr(module, helper, paused_worker)
    task = asyncio.create_task(embedding.embed(["synthetic text"], namespace="space"))
    try:
        await asyncio.wait_for(started.wait(), timeout=3)
        assert embedding._lock.locked()
        assert (await asyncio.wait_for(cache.health(), timeout=3))["entry_count"] == 0
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not embedding._lock.locked()
        release.set()
        await asyncio.wait_for(finished.wait(), timeout=3)
        assert (await cache.health())["entry_count"] == 0
        assert len(context.embedding.calls) == (0 if phase == "cached" else 1)
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
