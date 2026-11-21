"""Public host adapter contracts, without SDK credentials or network calls."""

import asyncio
import json
import math
from types import SimpleNamespace

import pytest

from adapters.providers import (
    EmbeddingValidationError,
    ProviderAdapter,
    ProviderUnavailableError,
    normalize_vectors,
)


class FakeEmbedding:
    def __init__(self, dimension=2):
        self.provider_config = {
            "id": "embedding-test", "embedding_model": "embed-v1",
            "embedding_api_base": "https://example.com/v1",
            "embedding_api_key": "never-copy-this-secret",
        }
        self.model = "embed-v1"
        self.dimension = dimension
        self.calls = []
        self.active = 0
        self.maximum_active = 0
        self.started = asyncio.Event()
        self.gate = None
        self.response = None
        self.failure = None
        self.cancelled = 0

    def get_model(self):
        return ""  # The real OpenAI embedding provider behaves this way.

    def get_dim(self):
        return self.dimension

    async def get_embeddings(self, texts):
        self.calls.append(list(texts))
        self.active += 1
        self.maximum_active = max(self.maximum_active, self.active)
        self.started.set()
        try:
            if self.gate is not None:
                await self.gate.wait()
            else:
                await asyncio.sleep(0)
            if self.failure is not None:
                raise self.failure
            if callable(self.response):
                return self.response(texts)
            if self.response is not None:
                return self.response
            return [[float(len(text)), 1.0] for text in texts]
        except asyncio.CancelledError:
            self.cancelled += 1
            raise
        finally:
            self.active -= 1

    async def get_embeddings_batch(self, *args, **kwargs):
        raise AssertionError("adapter must not add the host batch helper's retry loop")


class FakeChat:
    provider_config = {"id": "chat-test", "key": ["never-copy-chat-secret"]}

    def get_model(self):
        return "chat-v1"


class FakeContext:
    def __init__(self, embedding=None):
        self.embedding = embedding or FakeEmbedding()
        self.chat = FakeChat()
        self.calls = []
        self.chat_gate = None
        self.chat_started = asyncio.Event()
        self.chat_active = 0
        self.chat_maximum = 0
        self.chat_failure = None
        self.chat_response = '{"result":"no_change"}'

    def get_all_providers(self):
        return [self.chat]

    def get_all_embedding_providers(self):
        return [self.embedding]

    def get_provider_by_id(self, provider_id):
        return {"embedding-test": self.embedding, "chat-test": self.chat}.get(provider_id)

    async def llm_generate(self, **kwargs):
        self.calls.append(kwargs)
        self.chat_active += 1
        self.chat_maximum = max(self.chat_maximum, self.chat_active)
        self.chat_started.set()
        try:
            if self.chat_gate is not None:
                await self.chat_gate.wait()
            if self.chat_failure is not None:
                raise self.chat_failure
            return SimpleNamespace(completion_text=self.chat_response)
        finally:
            self.chat_active -= 1


MESSAGES = [{"role": "system", "content": "rules"}, {"role": "user", "content": "payload"}]


def test_safe_model_metadata_and_embedding_fingerprint():
    context = FakeContext()
    adapter = ProviderAdapter(context)
    models = adapter.list_models()
    assert models["embedding"][0]["model"] == "embed-v1"
    assert models["embedding"][0]["dimension"] == 2
    assert models["llm"][0]["provider_id"] == "chat-test"
    serialized = json.dumps(models)
    assert "secret" not in serialized
    assert "api_base" not in serialized
    before = adapter.embedding_fingerprint("embedding-test")
    context.embedding.provider_config["embedding_api_key"] = "another-secret"
    assert adapter.embedding_fingerprint("embedding-test") == before
    context.embedding.provider_config["embedding_api_base"] = "https://username:password@example.com/v1?api_key=secret"
    assert adapter.embedding_fingerprint("embedding-test") == before
    context.embedding.model = "embed-v2"
    assert adapter.embedding_fingerprint("embedding-test") != before


@pytest.mark.parametrize("change", ["endpoint", "dimension", "revision", "class", "model_fallback"])
def test_fingerprint_covers_effective_configuration(change):
    context = FakeContext()
    adapter = ProviderAdapter(context)
    before = adapter.embedding_fingerprint("embedding-test")
    revision = ""
    if change == "endpoint":
        context.embedding.provider_config["embedding_api_base"] = "https://different.test/v1"
    elif change == "dimension":
        context.embedding.dimension = 3
    elif change == "revision":
        revision = "changed"
    elif change == "class":
        class OtherEmbedding(FakeEmbedding):
            pass
        context.embedding = OtherEmbedding()
    else:
        context.embedding.model = ""
        context.embedding.provider_config["embedding_model"] = "different"
    assert adapter.embedding_fingerprint("embedding-test", revision) != before


@pytest.mark.asyncio
async def test_llm_calls_public_context_once_and_preserves_message_order():
    context = FakeContext()
    adapter = ProviderAdapter(context)
    assert await adapter.generate(MESSAGES, provider_id="chat-test") == context.chat_response
    assert context.calls == [{"chat_provider_id": "chat-test", "contexts": MESSAGES}]
    assert context.calls[0]["contexts"] is not MESSAGES
    context.chat_failure = ConnectionError("synthetic transport failure")
    with pytest.raises(ConnectionError):
        await adapter.generate(MESSAGES, provider_id="chat-test")
    assert len(context.calls) == 2


@pytest.mark.asyncio
async def test_llm_timeout_includes_queue_and_does_not_block_embedding():
    context = FakeContext()
    context.chat_gate = asyncio.Event()
    adapter = ProviderAdapter(context, llm_concurrency=1, embedding_concurrency=1)
    first = asyncio.create_task(adapter.generate(MESSAGES, provider_id="chat-test"))
    await context.chat_started.wait()
    try:
        diagnostics = {}
        with pytest.raises(TimeoutError):
            await adapter.generate(MESSAGES, provider_id="chat-test", timeout_seconds=0.02, diagnostics=diagnostics)
        assert diagnostics["queue_seconds"] >= .01 and not diagnostics["call_started"]
        assert "call_seconds" not in diagnostics
        assert len(context.calls) == 1
        assert len(await adapter.embed_batch(["message"], provider_id="embedding-test")) == 1
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        context.chat_gate.set()
        await adapter.generate(MESSAGES, provider_id="chat-test")
        assert context.chat_active == 0
        assert context.chat_maximum == 1
    finally:
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)


@pytest.mark.asyncio
async def test_embedding_timeout_includes_queue_and_cancellation_releases_slot():
    context = FakeContext()
    context.embedding.gate = asyncio.Event()
    adapter = ProviderAdapter(context, embedding_concurrency=1)
    first = asyncio.create_task(adapter.embed_batch(["first"], provider_id="embedding-test"))
    await context.embedding.started.wait()
    try:
        diagnostics = {}
        with pytest.raises(TimeoutError):
            await adapter.embed_batch(["second"], provider_id="embedding-test", timeout_seconds=0.02,
                                      diagnostics=diagnostics)
        assert context.embedding.calls == [["first"]]
        assert diagnostics["failure_stage"] == "provider_queue"
        assert diagnostics["error_type"] == "TimeoutError"
        assert diagnostics["queue_seconds"] >= .01
        assert diagnostics["call_seconds"] == 0
        assert not diagnostics["call_started"] and not diagnostics["completed"]
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        context.embedding.gate.set()
        vectors = await adapter.embed_batch(["recovered"], provider_id="embedding-test")
        assert math.isclose(sum(value * value for value in vectors[0]), 1)
        assert context.embedding.active == 0
    finally:
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)


@pytest.mark.asyncio
async def test_embedding_provider_timeout_reports_call_time_without_adding_retries():
    context = FakeContext()
    context.embedding.gate = asyncio.Event()
    adapter = ProviderAdapter(context)
    diagnostics = {}
    with pytest.raises(TimeoutError):
        await adapter.embed_batch(["synthetic query"], provider_id="embedding-test", timeout_seconds=.02,
                                  diagnostics=diagnostics)
    assert diagnostics["failure_stage"] == "provider_call"
    assert diagnostics["error_type"] == "TimeoutError"
    assert diagnostics["call_started"] and not diagnostics["completed"]
    assert diagnostics["call_seconds"] >= .01
    assert len(context.embedding.calls) == 1
    assert context.embedding.active == 0


def _live_limit_calls(kind, *, limit):
    context = FakeContext()
    context.chat_gate = asyncio.Event()
    context.embedding.gate = asyncio.Event()
    adapter = ProviderAdapter(context, llm_concurrency=limit, embedding_concurrency=limit)
    if kind == "llm":
        call = lambda: adapter.generate(MESSAGES, provider_id="chat-test")
        active = lambda: context.chat_active
        started = context.chat_started
    else:
        call = lambda: adapter.embed_batch(["query"], provider_id="embedding-test")
        active = lambda: context.embedding.active
        started = context.embedding.started
    return adapter, call, active, started


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["llm", "embedding"])
async def test_live_concurrency_decrease_keeps_active_calls_and_limits_new_calls(kind):
    adapter, call, active, started = _live_limit_calls(kind, limit=3)
    tasks = []
    try:
        for _ in range(3):
            started.clear()
            tasks.append(asyncio.create_task(call()))
            await asyncio.wait_for(started.wait(), timeout=1)
        await adapter.reconfigure(llm_concurrency=1, embedding_concurrency=1)
        assert active() == 3 and all(not task.done() for task in tasks)
        started.clear()
        tasks.append(asyncio.create_task(call()))
        await asyncio.sleep(0)
        assert not started.is_set()
        for task in tasks[:2]:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            assert not started.is_set()
        assert active() == 1
        tasks[2].cancel()
        await asyncio.gather(tasks[2], return_exceptions=True)
        await asyncio.wait_for(started.wait(), timeout=1)
        assert active() == 1
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    assert active() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["llm", "embedding"])
async def test_live_concurrency_increase_wakes_queued_calls_without_releasing_active_call(kind):
    adapter, call, active, started = _live_limit_calls(kind, limit=1)
    tasks = [asyncio.create_task(call())]
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        started.clear()
        tasks.append(asyncio.create_task(call()))
        await asyncio.sleep(0)
        assert not started.is_set()
        await adapter.reconfigure(llm_concurrency=2, embedding_concurrency=2)
        await asyncio.wait_for(started.wait(), timeout=1)
        assert active() == 2 and all(not task.done() for task in tasks)
        started.clear()
        tasks.append(asyncio.create_task(call()))
        await asyncio.sleep(0)
        assert not started.is_set()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    assert active() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["llm", "embedding"])
async def test_increase_after_shrinking_does_not_overbook_existing_calls(kind):
    adapter, call, active, started = _live_limit_calls(kind, limit=3)
    tasks = []
    try:
        for _ in range(3):
            started.clear()
            tasks.append(asyncio.create_task(call()))
            await asyncio.wait_for(started.wait(), timeout=1)
        await adapter.reconfigure(llm_concurrency=1, embedding_concurrency=1)
        started.clear()
        tasks.append(asyncio.create_task(call()))
        await asyncio.sleep(0)
        await adapter.reconfigure(llm_concurrency=2, embedding_concurrency=2)
        await asyncio.sleep(0)
        assert not started.is_set() and active() == 3
        tasks[0].cancel()
        await asyncio.gather(tasks[0], return_exceptions=True)
        assert not started.is_set() and active() == 2
        tasks[1].cancel()
        await asyncio.gather(tasks[1], return_exceptions=True)
        await asyncio.wait_for(started.wait(), timeout=1)
        assert active() == 2
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    assert active() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["llm", "embedding"])
async def test_cancel_queued_call_during_live_resize_does_not_leak_capacity(kind):
    adapter, call, active, started = _live_limit_calls(kind, limit=1)
    tasks = [asyncio.create_task(call())]
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        started.clear()
        tasks.append(asyncio.create_task(call()))
        await asyncio.sleep(0)
        assert not started.is_set()
        await adapter.reconfigure(llm_concurrency=2, embedding_concurrency=2)
        tasks[1].cancel()
        with pytest.raises(asyncio.CancelledError):
            await tasks[1]
        assert active() == 1 and not started.is_set()
        tasks.append(asyncio.create_task(call()))
        await asyncio.wait_for(started.wait(), timeout=1)
        assert active() == 2
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    assert active() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [0, -1, True, 1.5])
async def test_reconfigure_validates_both_limits_before_changing_either(invalid):
    adapter, call, active, started = _live_limit_calls("llm", limit=1)
    tasks = [asyncio.create_task(call())]
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        started.clear()
        tasks.append(asyncio.create_task(call()))
        await asyncio.sleep(0)
        with pytest.raises(ValueError, match="positive integer"):
            await adapter.reconfigure(llm_concurrency=2, embedding_concurrency=invalid)
        await asyncio.sleep(0)
        assert not started.is_set() and active() == 1
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,provider_id", [("llm", "missing"), ("llm", "embedding-test"), ("embedding", "chat-test")])
async def test_wrong_provider_capability_rejected(kind, provider_id):
    adapter = ProviderAdapter(FakeContext())
    with pytest.raises(ProviderUnavailableError):
        if kind == "llm":
            await adapter.generate(MESSAGES, provider_id=provider_id)
        else:
            await adapter.embed_batch(["query"], provider_id=provider_id)


@pytest.mark.parametrize("vectors", [[], [[1, 2], [3, 4]], [[]], [[0, 0]], [[1]], [[1, float("nan")]], [[float("inf"), 1]], [[True, 1]], [["1", 2]]])
def test_embedding_response_validation(vectors):
    with pytest.raises(EmbeddingValidationError):
        normalize_vectors(vectors, count=1, dimension=2)


def test_dynamic_dimension_and_large_finite_values():
    assert normalize_vectors([[1e308, 1e308]], count=1)[0] == pytest.approx([2 ** -0.5, 2 ** -0.5])
    with pytest.raises(EmbeddingValidationError, match="dimension"):
        normalize_vectors([[1, 2], [1, 2, 3]], count=2)


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan"), True])
async def test_invalid_deadline_never_calls_host(timeout):
    context = FakeContext()
    adapter = ProviderAdapter(context)
    with pytest.raises(ValueError):
        await adapter.generate(MESSAGES, provider_id="chat-test", timeout_seconds=timeout)
    with pytest.raises(ValueError):
        await adapter.embed_batch(["query"], provider_id="embedding-test", timeout_seconds=timeout)
    assert not context.calls and not context.embedding.calls


def test_llm_fingerprint_tracks_model_policy_without_credential_rotation():
    context = FakeContext()
    context.chat.provider_config = {"id": "chat-test", "api_base": "https://example.test/v1", "key": ["synthetic"]}
    adapter = ProviderAdapter(context)
    initial = adapter.llm_fingerprint("chat-test")
    context.chat.provider_config["key"] = ["rotated"]
    context.chat.provider_config["api_base"] = "https://user:password@example.test/v1?token=private"
    assert adapter.llm_fingerprint("chat-test") == initial
    assert "example" not in initial and "synthetic" not in initial
    assert adapter.llm_fingerprint("chat-test", "new-revision") != initial
    context.chat.provider_config["temperature"] = 0.3
    assert adapter.llm_fingerprint("chat-test") != initial
    context.chat.provider_config.pop("temperature")
    context.chat.get_model = lambda: "chat-v2"
    assert adapter.llm_fingerprint("chat-test") != initial
    with pytest.raises(ProviderUnavailableError):
        adapter.llm_fingerprint("embedding-test")
