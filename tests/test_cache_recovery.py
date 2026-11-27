"""可清理缓存故障不丢失已算向量，清理工作不进入频繁写路径。"""

import asyncio
import sqlite3

import pytest

from storage.cache_store import CacheStore, text_hash
from tests.test_embeddings import cache, service  # noqa: F401


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["get_vectors", "get_dimension"])
async def test_cache_read_failure_recomputes_only_requested_vectors(cache, monkeypatch, method):
    embedding, context = service(cache)

    async def fail(*args, **kwargs):
        raise sqlite3.OperationalError("synthetic unavailable cache")

    monkeypatch.setattr(cache, method, fail)
    if method == "get_vectors":
        assert await embedding.cached(["query"], namespace="space") == [None]
        assert context.embedding.calls == []
    errors = {}
    vectors = await embedding.embed(["query", "query"], namespace="space", cache_errors=errors)
    assert len(vectors) == 2 and vectors[0] == vectors[1]
    assert context.embedding.calls == [["query"]]
    assert errors == {"read": "OperationalError"}


@pytest.mark.asyncio
@pytest.mark.parametrize("slow", [False, True])
async def test_cache_write_failure_keeps_computed_vectors(cache, monkeypatch, slow):
    embedding, context = service(cache)

    async def fail(*args, **kwargs):
        if slow:
            await asyncio.sleep(10)
        raise OSError("synthetic write failure")

    monkeypatch.setattr(cache, "put_vectors", fail)
    errors = {}
    vectors = await embedding.embed(["query"], namespace="space", timeout_seconds=1, cache_errors=errors)
    assert vectors and len(vectors[0]) == 2
    assert len(context.embedding.calls) == 1
    assert errors == {"write": "TimeoutError" if slow else "OSError"}
    assert (await cache.health())["entry_count"] == 0


@pytest.mark.asyncio
async def test_corrupt_cache_is_a_miss_but_valid_provider_result_is_preserved(cache):
    embedding, context = service(cache)
    await embedding.embed(["query"], namespace="space")
    await cache.database.execute("UPDATE embedding_cache SET vector_json='[0,0]'")
    assert await embedding.cached(["query"], namespace="space") == [None]
    result = await embedding.embed(["query"], namespace="space")
    assert result and len(context.embedding.calls) == 2


@pytest.mark.asyncio
async def test_write_does_not_cleanup_and_each_cleanup_transaction_is_bounded(tmp_path, monkeypatch):
    now = [100.0]
    store = CacheStore(tmp_path / "cache.db", clock=lambda: now[0], message_ttl_seconds=1, message_limit=1)
    await store.open()
    store.cleanup_batch_size = 3
    cleanup = store._cleanup

    async def unexpected(*args):
        raise AssertionError("write must not scan the whole cache")

    try:
        monkeypatch.setattr(store, "_cleanup", unexpected)
        await store.put_vectors("model", "space", [text_hash(str(i)) for i in range(8)], [[1, 0]] * 8)
        monkeypatch.setattr(store, "_cleanup", cleanup)
        now[0] = 102
        assert await store.get_vectors("model", "space", [text_hash("0")]) == [None]
        assert await store.cleanup() == 3
        assert (await store.health())["entry_count"] == 5
        assert await store.cleanup() == 3
        assert await store.cleanup() == 2
        assert await store.cleanup() == 0
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_background_cleanup_reclaims_overflow_and_stops_on_close(tmp_path):
    store = CacheStore(tmp_path / "cache.db", message_limit=1)
    store.cleanup_interval_seconds = .01
    await store.open()
    try:
        await store.put_vectors("model", "space", [text_hash("a"), text_hash("b")], [[1, 0], [0, 1]])
        async with asyncio.timeout(2):
            while (await store.health())["entry_count"] > 1:
                await asyncio.sleep(.01)
        task = store._cleanup_task
    finally:
        await store.close()
    assert task.done()


@pytest.mark.asyncio
async def test_access_timestamp_write_failure_preserves_cached_vector(cache):
    embedding, context = service(cache)
    await embedding.embed(["query"], namespace="space")
    await cache.database.execute(
        "CREATE TRIGGER fail_touch BEFORE UPDATE OF accessed_at ON embedding_cache "
        "BEGIN SELECT RAISE(ABORT, 'synthetic touch failure'); END"
    )
    errors = {}
    vector, = await embedding.cached(["query"], namespace="space", cache_errors=errors)
    assert vector and errors == {"touch": "IntegrityError"}
    assert context.embedding.calls == [["query"]]
