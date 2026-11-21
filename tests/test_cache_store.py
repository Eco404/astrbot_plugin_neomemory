"""TTL, capacity, restart and dimension integrity for disposable vectors."""

import sqlite3

import pytest

from storage.cache_store import CacheStore, text_hash


@pytest.mark.asyncio
async def test_cache_roundtrip_restart_and_no_source_text(tmp_path):
    path = tmp_path / "cache.db"
    cache = CacheStore(path)
    await cache.open()
    digest = text_hash("a private body that must never be copied")
    await cache.put_vectors("model-a", "space-a", [digest], [[0.6, 0.8]])
    await cache.close()
    reopened = CacheStore(path)
    await reopened.open()
    try:
        assert await reopened.get_vectors("model-a", "space-a", [digest]) == [[0.6, 0.8]]
        assert await reopened.get_vectors("model-a", "space-b", [digest]) == [None]
        assert await reopened.get_vectors("model-b", "space-a", [digest]) == [None]
        assert await reopened.get_dimension("model-a") == 2
        health = await reopened.health()
        assert health["counts"] == {"message": 1, "node": 0}
        assert health["schema_version"] == 1
        row = await reopened.database.fetchone("SELECT * FROM embedding_cache")
        assert "a private body" not in repr(tuple(row))
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_absolute_ttl_and_cleanup_on_open(tmp_path):
    now = [100.0]
    path = tmp_path / "cache.db"
    cache = CacheStore(path, clock=lambda: now[0], message_ttl_seconds=10)
    await cache.open()
    digest = text_hash("text")
    await cache.put_vectors("model", "space", [digest], [[1, 0]])
    now[0] = 109
    assert (await cache.get_vectors("model", "space", [digest]))[0] == [1, 0]
    now[0] = 110
    assert await cache.get_vectors("model", "space", [digest]) == [None]
    # Access at 109 must not extend the absolute lifetime.
    await cache.close()
    reopened = CacheStore(path, clock=lambda: now[0], message_ttl_seconds=10)
    await reopened.open()
    try:
        assert (await reopened.health())["entry_count"] == 0
        assert await reopened.get_dimension("model") == 2
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_separate_capacities_keep_recently_accessed_entries(tmp_path):
    now = [100.0]
    cache = CacheStore(tmp_path / "cache.db", clock=lambda: now[0], message_limit=2, node_limit=1)
    await cache.open()
    hashes = [text_hash(value) for value in ("a", "b", "c")]
    try:
        await cache.put_vectors("model", "space", hashes[:2], [[1, 0], [0, 1]])
        now[0] = 101
        await cache.get_vectors("model", "space", hashes[:1])
        now[0] = 102
        await cache.put_vectors("model", "space", hashes[2:], [[1, 1]])
        await cache.cleanup()
        values = await cache.get_vectors("model", "space", hashes)
        assert values == [[1, 0], None, [1, 1]]
        await cache.put_vectors("model", "space", hashes[:2], [[1, 0], [0, 1]], kind="node", identities=[("node-a", 1), ("node-b", 1)])
        await cache.cleanup()
        assert (await cache.health())["counts"] == {"message": 2, "node": 1}
    finally:
        await cache.close()


@pytest.mark.asyncio
async def test_node_cache_requires_id_version_text_and_space(tmp_path):
    cache = CacheStore(tmp_path / "cache.db")
    await cache.open()
    digest = text_hash("node body")
    try:
        await cache.put_vectors("model", "space", [digest], [[1, 0]], kind="node", identities=[("node", 1)])
        assert await cache.get_vectors("model", "space", [digest], kind="node", identities=[("node", 1)]) == [[1, 0]]
        assert await cache.get_vectors("model", "space", [digest], kind="node", identities=[("node", 2)]) == [None]
        assert await cache.get_vectors("model", "space", [digest], kind="node", identities=[("other-node", 1)]) == [None]
        assert await cache.get_vectors("model", "other-space", [digest], kind="node", identities=[("node", 1)]) == [None]
        with pytest.raises(ValueError, match="identities"):
            await cache.get_vectors("model", "space", [digest], kind="node")
    finally:
        await cache.close()


@pytest.mark.asyncio
async def test_node_invalidation_covers_models_versions_but_preserves_space_and_message_cache(tmp_path):
    cache = CacheStore(tmp_path / "cache.db")
    await cache.open()
    digest = text_hash("shared synthetic body")
    try:
        for model in ("model-a", "model-b"):
            await cache.put_vectors(model, "space-a", [digest], [[1, 0]])
            await cache.put_vectors(model, "space-a", [digest] * 3, [[1, 0]] * 3,
                                    kind="node", identities=[("target", 1), ("target", 2), ("other-node", 1)])
            await cache.put_vectors(model, "space-b", [digest], [[1, 0]], kind="node", identities=[("target", 1)])
        assert await cache.invalidate_nodes("space-a", ["target", "target", "absent"]) == 4
        for model in ("model-a", "model-b"):
            assert await cache.get_vectors(model, "space-a", [digest]) == [[1, 0]]
            assert await cache.get_vectors(model, "space-a", [digest] * 3, kind="node",
                                           identities=[("target", 1), ("target", 2), ("other-node", 1)]) == [None, None, [1, 0]]
            assert await cache.get_vectors(model, "space-b", [digest], kind="node", identities=[("target", 1)]) == [[1, 0]]
            assert await cache.get_dimension(model) == 2
        assert await cache.invalidate_nodes("space-a", []) == 0
        assert await cache.invalidate_nodes("space-a", ["target"]) == 0
    finally:
        await cache.close()


@pytest.mark.asyncio
async def test_node_invalidation_is_atomic_across_bounded_chunks(tmp_path, monkeypatch):
    cache = CacheStore(tmp_path / "cache.db")
    await cache.open()
    ids = [f"node-{index}" for index in range(401)]
    digest = text_hash("synthetic")
    try:
        await cache.put_vectors("model", "space", [digest] * 2, [[1, 0]] * 2,
                                kind="node", identities=[(ids[0], 1), (ids[-1], 1)])
        connection = cache.database.connection
        execute = connection.execute
        calls = 0

        async def fail_second_chunk(sql, *args):
            nonlocal calls
            if sql.startswith("DELETE FROM embedding_cache WHERE namespace"):
                calls += 1
                if calls == 2:
                    raise RuntimeError("synthetic invalidation failure")
            return await execute(sql, *args)

        monkeypatch.setattr(connection, "execute", fail_second_chunk)
        with pytest.raises(RuntimeError, match="synthetic"):
            await cache.invalidate_nodes("space", ids)
        monkeypatch.setattr(connection, "execute", execute)
        assert (await cache.health())["counts"]["node"] == 2
        assert await cache.invalidate_nodes("space", ids) == 2
    finally:
        await cache.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("namespace,ids", [("", ["a"]), (False, ["a"]), ("space", "node"), ("space", ["a", ""]), ("space", [False])])
async def test_node_invalidation_validates_before_deleting(tmp_path, namespace, ids):
    cache = CacheStore(tmp_path / "cache.db")
    await cache.open()
    try:
        await cache.put_vectors("model", "space", [text_hash("body")], [[1, 0]], kind="node", identities=[("a", 1)])
        with pytest.raises(ValueError):
            await cache.invalidate_nodes(namespace, ids)
        assert (await cache.health())["counts"]["node"] == 1
    finally:
        await cache.close()


@pytest.mark.asyncio
async def test_dimension_mismatch_and_invalid_data_do_not_commit_partial_batch(tmp_path):
    cache = CacheStore(tmp_path / "cache.db")
    await cache.open()
    hashes = [text_hash(value) for value in ("a", "b")]
    try:
        await cache.put_vectors("model", "space", hashes[:1], [[1, 0]])
        with pytest.raises(ValueError, match="dimension"):
            await cache.put_vectors("model", "other-space", hashes, [[1, 0, 0], [0, 1, 0]])
        with pytest.raises(ValueError, match="finite"):
            await cache.put_vectors("another-model", "space", hashes, [[1, 0], [float("nan"), 1]])
        assert (await cache.health())["entry_count"] == 1
        assert await cache.get_dimension("another-model") == 0
    finally:
        await cache.close()


@pytest.mark.asyncio
async def test_future_schema_is_rejected_without_mutating_file(tmp_path):
    path = tmp_path / "future.db"
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA user_version = 42")
    cache = CacheStore(path)
    with pytest.raises(ValueError, match="newer"):
        await cache.open()
    with pytest.raises(RuntimeError, match="not open"):
        _ = cache.database.connection
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 42
        assert connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == []


@pytest.mark.asyncio
async def test_zero_capacity_retains_no_vectors(tmp_path):
    cache = CacheStore(tmp_path / "cache.db", message_limit=0)
    await cache.open()
    try:
        await cache.put_vectors("model", "space", [text_hash("text")], [[1, 0]])
        assert (await cache.health())["entry_count"] == 0
    finally:
        await cache.close()


@pytest.mark.asyncio
async def test_cleanup_failure_rolls_back_expiry_and_capacity_changes(tmp_path, monkeypatch):
    now = [1.0]
    cache = CacheStore(tmp_path / "cache.db", clock=lambda: now[0], message_ttl_seconds=1)
    await cache.open()
    try:
        await cache.put_vectors("model", "space", [text_hash("text")], [[1, 0]])
        now[0] = 3
        connection = cache.database.connection
        original = connection.execute

        async def fail_after_expired(sql, *args):
            if "ORDER BY accessed_at" in sql:
                raise RuntimeError("synthetic cleanup failure")
            return await original(sql, *args)

        monkeypatch.setattr(connection, "execute", fail_after_expired)
        with pytest.raises(RuntimeError, match="synthetic"):
            await cache.cleanup()
        monkeypatch.setattr(connection, "execute", original)
        assert (await cache.health())["entry_count"] == 1
        assert await cache.cleanup() == 1
    finally:
        await cache.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["read", "write"])
async def test_cpu_worker_cancellation_leaves_no_late_cache_mutation(tmp_path, monkeypatch, operation):
    """Coordinate a paused worker rather than asserting a machine-speed threshold."""
    import asyncio
    import threading

    import storage.cache_store as module

    now = [100.0]
    cache = CacheStore(tmp_path / "cache.db", clock=lambda: now[0])
    await cache.open()
    digest = text_hash("synthetic text")
    if operation == "read":
        await cache.put_vectors("model", "space", [digest], [[0.6, 0.8]])
    now[0] = 200
    loop = asyncio.get_running_loop()
    main_thread = threading.get_ident()
    started, finished = asyncio.Event(), asyncio.Event()
    release = threading.Event()
    helper = "_decode_vectors" if operation == "read" else "_encode_vectors"
    original = getattr(module, helper)

    def paused_worker(*args):
        try:
            assert threading.get_ident() != main_thread
            loop.call_soon_threadsafe(started.set)
            assert release.wait(5), "test did not release worker"
            return original(*args)
        finally:
            loop.call_soon_threadsafe(finished.set)

    monkeypatch.setattr(module, helper, paused_worker)
    task = asyncio.create_task(
        cache.get_vectors("model", "space", [digest]) if operation == "read"
        else cache.put_vectors("model", "space", [digest], [[0.6, 0.8]])
    )
    try:
        await asyncio.wait_for(started.wait(), timeout=3)
        # The event loop and SQLite lock remain available while pure CPU work
        # runs elsewhere. A held database transaction would deadlock this probe.
        assert (await asyncio.wait_for(cache.health(), timeout=3))["entry_count"] == (1 if operation == "read" else 0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        release.set()
        await asyncio.wait_for(finished.wait(), timeout=3)
        row = await cache.database.fetchone("SELECT accessed_at FROM embedding_cache")
        if operation == "read":
            assert row[0] == 100  # Cancelled decoding never updates access time.
        else:
            assert row is None
            assert await cache.get_dimension("model") == 0
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await cache.close()


@pytest.mark.asyncio
async def test_corrupt_cached_vector_does_not_refresh_lru_access(tmp_path):
    now = [100.0]
    cache = CacheStore(tmp_path / "cache.db", clock=lambda: now[0])
    await cache.open()
    digest = text_hash("synthetic text")
    try:
        await cache.put_vectors("model", "space", [digest], [[0.6, 0.8]])
        await cache.database.execute("UPDATE embedding_cache SET vector_json='[0,0]'")
        now[0] = 200
        with pytest.raises(ValueError, match="nonzero"):
            await cache.get_vectors("model", "space", [digest])
        row = await cache.database.fetchone("SELECT accessed_at FROM embedding_cache")
        assert row[0] == 100
    finally:
        await cache.close()
