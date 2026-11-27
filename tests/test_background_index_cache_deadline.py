"""后台必要索引写入使用总期限，在线慢缓存仍及时返回已算向量。"""

import asyncio
import time

import pytest

from core.extraction_context import node_vectors
from tests.test_embeddings import cache, service  # noqa: F401


NODE = {"node_id": "synthetic-index-node", "content_version": 2,
        "content_text": "独立合成的预约已确认。"}


async def exact(embedding):
    return await embedding.cached(
        [NODE["content_text"]], namespace="space", kind="node",
        identities=[(NODE["node_id"], NODE["content_version"])], allow_text_reuse=False,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("reuse", [False, True])
async def test_background_slow_write_persists_without_repeated_embedding(cache, monkeypatch, reuse):
    embedding, context = service(cache)
    if reuse:
        await embedding.embed([NODE["content_text"]], namespace="space")
    original = cache.put_vectors

    async def slow_write(*args, **kwargs):
        await asyncio.sleep(.18)
        await original(*args, **kwargs)

    monkeypatch.setattr(cache, "put_vectors", slow_write)
    vectors, missing = await node_vectors(
        embedding, [NODE], "space", deadline=time.monotonic() + 2, require_persisted=True,
    )
    assert missing == 1 and vectors == await exact(embedding)
    assert context.embedding.calls == [[NODE["content_text"]]]
    assert await node_vectors(
        embedding, [NODE], "space", deadline=time.monotonic() + 2, require_persisted=True,
    ) == (vectors, 0)
    assert context.embedding.calls == [[NODE["content_text"]]]


@pytest.mark.asyncio
async def test_background_write_waits_for_database_lock_within_deadline(cache, monkeypatch):
    embedding, context = service(cache)
    original = cache.put_vectors
    locked, writing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    holders = []

    async def hold_database():
        async with cache.database._lock:
            locked.set()
            await release.wait()

    async def contended_write(*args, **kwargs):
        holders.append(asyncio.create_task(hold_database()))
        await locked.wait()
        writing.set()
        await original(*args, **kwargs)

    monkeypatch.setattr(cache, "put_vectors", contended_write)
    task = asyncio.create_task(node_vectors(
        embedding, [NODE], "space", deadline=time.monotonic() + 2, require_persisted=True,
    ))
    try:
        async with asyncio.timeout(2):
            await writing.wait()
            await asyncio.sleep(.18)
            assert not task.done()
            release.set()
            vectors, missing = await task
        assert missing == 1 and vectors == await exact(embedding)
        assert context.embedding.calls == [[NODE["content_text"]]]
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, *holders, return_exceptions=True)


@pytest.mark.asyncio
async def test_online_slow_cache_returns_computed_vector_without_waiting_for_write(cache, monkeypatch):
    embedding, context = service(cache)
    stopped = asyncio.Event()

    async def slow_write(*args, **kwargs):
        try:
            await asyncio.sleep(10)
        finally:
            stopped.set()

    monkeypatch.setattr(cache, "put_vectors", slow_write)
    errors = {}
    async with asyncio.timeout(1):
        vectors = await embedding.embed([NODE["content_text"]], namespace="space", timeout_seconds=2, cache_errors=errors)
    assert vectors and stopped.is_set()
    assert errors == {"write": "TimeoutError"}
    assert context.embedding.calls == [[NODE["content_text"]]]
    assert (await cache.health())["entry_count"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_background_timeout_and_cancellation_stop_write_without_partial_cache(cache, monkeypatch, cancel):
    embedding, context = service(cache)
    entered, stopped = asyncio.Event(), asyncio.Event()

    async def blocked_write(*args, **kwargs):
        entered.set()
        try:
            await asyncio.sleep(10)
        finally:
            stopped.set()

    monkeypatch.setattr(cache, "put_vectors", blocked_write)
    task = asyncio.create_task(node_vectors(
        embedding, [NODE], "space", deadline=time.monotonic() + (2 if cancel else .15),
        require_persisted=True,
    ))
    async with asyncio.timeout(2):
        await entered.wait()
        if cancel:
            task.cancel()
        with pytest.raises(asyncio.CancelledError if cancel else TimeoutError):
            await task
    assert stopped.is_set() and not cache.database._lock.locked()
    assert not embedding._lock.locked()
    assert context.embedding.calls == [[NODE["content_text"]]]
    assert await exact(embedding) == [None]
    assert (await cache.health())["entry_count"] == 0


@pytest.mark.asyncio
async def test_index_readback_is_also_bounded_by_same_deadline(cache, monkeypatch):
    embedding, context = service(cache)
    original = embedding.cached
    calls = 0
    stopped = asyncio.Event()

    async def delayed_readback(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            try:
                await asyncio.sleep(10)
            finally:
                stopped.set()
        return await original(*args, **kwargs)

    monkeypatch.setattr(embedding, "cached", delayed_readback)
    async with asyncio.timeout(2):
        with pytest.raises(TimeoutError):
            await node_vectors(embedding, [NODE], "space", deadline=time.monotonic() + .15,
                               require_persisted=True)
    assert stopped.is_set()
    monkeypatch.setattr(embedding, "cached", original)
    vectors, missing = await node_vectors(
        embedding, [NODE], "space", deadline=time.monotonic() + 2, require_persisted=True,
    )
    assert missing == 0 and vectors == await exact(embedding)
    assert context.embedding.calls == [[NODE["content_text"]]]
