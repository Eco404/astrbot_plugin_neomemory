"""成功抽取后的向量准备：真实存储、合成输入和可恢复故障。"""

import asyncio
import json
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest
import pytest_asyncio

from core.embeddings import EmbeddingService
from core.extraction_context import IndexCacheUnavailable, node_vectors
from storage.cache_store import CacheStore
from tests.test_extraction_context import SemanticFixtureAdapter
from tests.test_extraction_apply import capture
from tests.test_maintenance_engine import (
    enqueue_and_claim, environment, execute, node, output, restart,
)


@pytest_asyncio.fixture
async def indexing_env(environment, tmp_path):
    env = environment
    cache = CacheStore(tmp_path / "cache.db")
    await cache.open()
    adapter = SemanticFixtureAdapter(env.memory)
    service = EmbeddingService(adapter, cache, provider_id="synthetic", batch_size=16)
    env.runtime.cache = cache
    env.runtime.recall = SimpleNamespace(embeddings=service)
    env.vector_adapter, env.embeddings, env.cache = adapter, service, cache
    try:
        yield env
    finally:
        await cache.close()


def create_candidates(env, count=1):
    env.adapter.output = json.dumps(output([
        node(env.records[0].source_id, ref=f"local_event_{index}", content=f"第 {index} 次合成预约已确定。")
        for index in range(count)
    ]))


async def ready_retry(env, job):
    await env.memory.database.execute(
        "UPDATE memory_jobs SET available_at=0 WHERE job_id=?", (job["job_id"],),
    )
    restart(env)
    return await env.engine.jobs.claim("resumed-worker")


@pytest.mark.asyncio
async def test_vector_cache_write_failure_preserves_commit_and_retries_only_index(indexing_env, monkeypatch):
    env = indexing_env
    create_candidates(env)
    original = env.cache.put_vectors

    async def unavailable(*args, **kwargs):
        raise OSError("synthetic cache unavailable")

    monkeypatch.setattr(env.cache, "put_vectors", unavailable)
    waiting = await execute(env, await enqueue_and_claim(env))
    assert waiting["state"] == "retry_wait", (waiting["error_code"], waiting["error_type"], waiting["checkpoint"].get("window", {}).get("validation_errors"))
    assert waiting["error_code"] == "index_cache_unavailable"
    assert waiting["step"] == "window_index"
    assert set(waiting["checkpoint"]["window"]) == {"source_ids", "source_cursor"}
    assert waiting["checkpoint"]["window_index"]["indexed"] == 0
    inspection = await env.engine.describe(waiting["job_id"])
    assert inspection["committed_window"]["total"] == 1
    assert len(await env.memory.list_nodes(space_id=env.space["space_id"])) == 1
    assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 1
    monkeypatch.setattr(env.cache, "put_vectors", original)
    result = await execute(env, await ready_retry(env, waiting))
    assert result["state"] == "succeeded"
    assert result["result"]["index"] == {"indexed": 1, "configured": True}
    assert result["checkpoint"] == {}
    assert len(env.adapter.calls) == 1
    assert len(env.vector_adapter.calls) == 2
    assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("clear_cache", [False, True])
async def test_partial_index_retry_reuses_committed_batch_without_reextracting(indexing_env, clear_cache):
    env = indexing_env
    create_candidates(env, 20)
    env.vector_adapter.failure = TimeoutError("synthetic partial failure")
    env.vector_adapter.fail_on = 2
    waiting = await execute(env, await enqueue_and_claim(env))
    assert waiting["state"] == "retry_wait", (waiting["error_code"], waiting["error_type"], waiting["checkpoint"].get("window", {}).get("validation_errors"))
    assert waiting["error_code"] == "window_index_temporary_failure"
    assert waiting["checkpoint"]["window_index"] == {
        "batch_id": waiting["checkpoint"]["window_index"]["batch_id"], "indexed": 16, "total": 20,
    }
    assert len(env.adapter.calls) == 1
    if clear_cache:
        await env.cache.database.execute("DELETE FROM embedding_cache")
    env.vector_adapter.failure = None
    result = await execute(env, await ready_retry(env, waiting))
    assert result["state"] == "succeeded"
    assert [len(batch) for batch in env.vector_adapter.calls] == ([16, 4, 16, 4] if clear_cache else [16, 4, 4])
    assert len(env.adapter.calls) == 1
    assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 1


@pytest.mark.asyncio
async def test_interrupt_after_vector_write_before_progress_uses_cache_receipt(indexing_env, monkeypatch):
    env = indexing_env
    create_candidates(env, 20)
    original = env.engine._save

    async def interrupt_progress(job, checkpoint, step, *args):
        if step == "window_index" and checkpoint["window_index"]["indexed"] == 16:
            raise asyncio.CancelledError()
        return await original(job, checkpoint, step, *args)

    monkeypatch.setattr(env.engine, "_save", interrupt_progress)
    job = await enqueue_and_claim(env)
    with pytest.raises(asyncio.CancelledError):
        await env.engine.runner._execute(job)
    saved = await env.engine.jobs.get(job["job_id"])
    assert saved["state"] == "queued"
    assert saved["checkpoint"]["window_index"]["indexed"] == 0
    result = await execute(env, await ready_retry(env, saved))
    assert result["state"] == "succeeded"
    assert [len(batch) for batch in env.vector_adapter.calls] == [16, 4]
    assert len(env.adapter.calls) == 1


@pytest.mark.asyncio
async def test_message_vector_is_promoted_to_current_node_key_without_network(indexing_env):
    env = indexing_env
    content = "合成预约已经完成。"
    await env.embeddings.embed([content], namespace=env.space["space_id"])
    candidate = {"node_id": "synthetic-node", "content_version": 3, "content_text": content}
    await node_vectors(env.embeddings, [candidate], env.space["space_id"],
                       deadline=time.monotonic() + 10, require_persisted=True)
    assert len(env.vector_adapter.calls) == 1
    stored = await env.embeddings.cached([content], namespace=env.space["space_id"], kind="node",
                                        identities=[("synthetic-node", 3)], allow_text_reuse=False)
    assert stored == [[1.0, 0.0, 0.0]]


@pytest.mark.asyncio
async def test_emotion_only_version_reuses_old_node_vector_but_persists_current_key(indexing_env):
    env = indexing_env
    original = await env.memory.create_node(space_id=env.space["space_id"], node_type="event",
        content_text="合成预约已经完成。", importance=0.6, source_ids=[env.records[0].source_id])
    await node_vectors(env.embeddings, [original], env.space["space_id"],
                       deadline=time.monotonic() + 10, require_persisted=True)
    current = await env.memory.update_node_content(original["node_id"], original["content_text"],
        emotion={"tags": [{"tag": "joy", "intensity": 0.4}], "description": "预约完成，我有些期待。"})
    assert current["content_version"] == 2
    assert (await env.cache.health())["counts"] == {"node": 1, "message": 0}
    vectors, missing = await node_vectors(env.embeddings, [current], env.space["space_id"],
        deadline=time.monotonic() + 10, require_persisted=False)
    assert vectors == [[1.0, 0.0, 0.0]] and missing == 0
    exact = dict(namespace=env.space["space_id"], kind="node", identities=[(current["node_id"], 2)], allow_text_reuse=False)
    assert await env.embeddings.cached([current["content_text"]], **exact) == [None]
    env.cache.limits["node"] = 0
    with pytest.raises(IndexCacheUnavailable):
        await node_vectors(env.embeddings, [current], env.space["space_id"],
            deadline=time.monotonic() + 10, require_persisted=True)
    assert await env.embeddings.cached([current["content_text"]], **exact) == [None]
    env.cache.limits["node"] = 10_000
    _, missing = await node_vectors(env.embeddings, [current], env.space["space_id"],
        deadline=time.monotonic() + 10, require_persisted=True)
    assert missing == 1
    assert await env.embeddings.cached([current["content_text"]], **exact) == vectors
    assert len(env.vector_adapter.calls) == 1
    assert (await env.cache.health())["counts"] == {"node": 2, "message": 0}


@pytest.mark.asyncio
async def test_disabled_node_cache_cannot_claim_successful_offline_index(indexing_env):
    env = indexing_env
    env.cache.limits["node"] = 0
    candidate = {"node_id": "synthetic-node", "content_version": 1, "content_text": "合成预约。"}
    with pytest.raises(IndexCacheUnavailable):
        await node_vectors(env.embeddings, [candidate], env.space["space_id"],
                           deadline=time.monotonic() + 10, require_persisted=True)
    assert len(env.vector_adapter.calls) == 1


@pytest.mark.asyncio
async def test_rebuild_indexes_each_window_before_preparing_next_context(indexing_env, monkeypatch):
    env = indexing_env
    # This provider fixture exercises only the source extraction protocol.
    env.engine.config = replace(env.engine.config, topic_consolidation_enabled=False)
    for number in range(3, 7):
        await capture(env.sources, number=number)
    checked = []
    original = env.engine._context_ids

    async def inspect_context(space, generation_id, sources, **kwargs):
        previous = await env.memory.list_nodes(space_id=space["space_id"], generation_id=generation_id)
        if previous:
            vectors = await env.embeddings.cached(
                [item["content_text"] for item in previous], namespace=space["space_id"], kind="node",
                identities=[(item["node_id"], item["content_version"]) for item in previous],
                allow_text_reuse=False,
            )
            assert all(vector is not None for vector in vectors)
            checked.append(len(previous))
        return await original(space, generation_id, sources, **kwargs)

    async def candidate(messages):
        env.adapter.output = json.dumps(output([
            node("s1", content=f"第 {len(env.adapter.calls)} 次合成预约。"),
        ]))

    monkeypatch.setattr(env.engine, "_context_ids", inspect_context)
    env.adapter.on_generate = candidate
    result = await execute(env, await enqueue_and_claim(env, "rebuild"))
    assert result["state"] == "succeeded", result["error_code"]
    assert result["result"]["source_count"] == 6
    assert len(env.adapter.calls) == 3
    assert checked == [1, 2]
    assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 3


@pytest.mark.asyncio
async def test_batch_index_selects_current_body_changes_only(indexing_env):
    env = indexing_env
    original_body = "预约在周六。"
    next_body = "预约从周六改到了周日。"
    previous, batches = None, []
    for index, (title, body, expected_count) in enumerate((
        ("初始标题", original_body, 1),
        ("展示标题", original_body, 0),
        ("展示标题", original_body, 0),
        ("展示标题", next_body, 1),
    )):
        if index:
            for number in range(index * 2 + 1, index * 2 + 3):
                await capture(env.sources, number=number)
        env.adapter.output = json.dumps(output([node("s1", title=title, content=body, old=previous)]))
        result = await execute(env, await enqueue_and_claim(env, key=f"window-{index}"))
        assert result["state"] == "succeeded", result["error_code"]
        batch_id = result["result"]["batch_id"]
        batches.append(batch_id)
        indexed = await env.memory.extraction_batch_index_nodes(batch_id)
        assert len(indexed) == expected_count
        assert result["result"]["index"]["indexed"] == expected_count
        previous = (await env.memory.list_nodes(space_id=env.space["space_id"]))[0]
    assert previous["content_version"] == 2
    assert await env.memory.extraction_batch_index_nodes(batches[0]) == []
    assert not any("标题" in text for batch in env.vector_adapter.calls for text in batch)


@pytest.mark.asyncio
async def test_configured_candidate_embedding_failure_defers_extraction_without_recent_fallback(indexing_env):
    env = indexing_env
    await env.memory.create_node(space_id=env.space["space_id"], node_type="event",
        content_text="合成旧预约。", source_ids=[env.records[0].source_id])
    env.vector_adapter.failure = ConnectionError("synthetic candidate failure")
    waiting = await execute(env, await enqueue_and_claim(env))
    assert waiting["state"] == "retry_wait"
    assert waiting["error_code"] == "context_embedding_temporary_failure"
    assert env.adapter.calls == []
    assert await env.memory.database.fetchall("SELECT * FROM extraction_batches") == []
    env.vector_adapter.failure = None
    result = await execute(env, await ready_retry(env, waiting))
    assert result["state"] == "succeeded"
    assert len(env.adapter.calls) == 1


@pytest.mark.asyncio
async def test_context_preparation_consumes_the_same_deadline_before_llm(indexing_env, monkeypatch):
    env = indexing_env
    await env.memory.create_node(space_id=env.space["space_id"], node_type="event",
        content_text="合成旧预约。", source_ids=[env.records[0].source_id])
    env.config = replace(env.config, extraction_timeout_seconds=0.1)
    restart(env)
    original = env.vector_adapter.embed_batch

    async def delayed(texts, **kwargs):
        await asyncio.sleep(1)
        return await original(texts, **kwargs)

    monkeypatch.setattr(env.vector_adapter, "embed_batch", delayed)
    waiting = await execute(env, await enqueue_and_claim(env))
    assert waiting["state"] == "retry_wait"
    assert waiting["error_code"] in {"extraction_timeout", "context_embedding_temporary_failure"}
    assert env.adapter.calls == []
    assert "window" not in waiting["checkpoint"]
    assert await env.memory.database.fetchall("SELECT * FROM extraction_batches") == []

@pytest.mark.asyncio
async def test_fresh_recovery_finishes_outstanding_index_without_new_sources(indexing_env):
    env = indexing_env
    create_candidates(env, 2)
    env.vector_adapter.failure = ValueError("index unavailable")
    job = await enqueue_and_claim(env, "extract", automatic=True)
    initial = await execute(env, job)
    assert initial["state"] == "failed" and initial["step"] == "window_index"
    assert (await env.cache.health())["counts"]["node"] == 0
    await env.memory.database.execute("UPDATE memory_jobs SET available_at=0 WHERE job_id=?", (job["job_id"],))
    # A missing provider fails before any new model request on the recovery lease.
    recall = env.runtime.recall
    env.runtime.recall = None
    resumed = await execute(env, await env.engine.jobs.claim("resume"))
    assert resumed["attempts"][-1]["model_calls"] == 0
    assert resumed["recovery_mode"] == "fresh"
    failed = await execute(env, await env.engine.jobs.claim("fresh"))
    assert failed["state"] == "failed" and failed["result"] is None
    env.runtime.recall = recall
    env.vector_adapter.failure = None
    retry = await env.engine.retry(job["job_id"], fresh=True)
    assert retry["job_id"] == job["job_id"]
    completed = await execute(env, await env.engine.jobs.claim("manual"))
    assert completed["result"]["index"]["indexed"] == 2
    assert (await env.cache.health())["counts"]["node"] == 2
    assert len(env.adapter.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["topic_consolidate", "topic_rebuild", "rebuild", "reindex"])
async def test_required_index_survives_provider_removal_and_restoration(indexing_env, monkeypatch, kind):
    from core.jobs import RetryableJobError
    from tests.test_topic_maintenance import candidate, seed

    env = indexing_env
    if kind == "rebuild":
        create_candidates(env)
    else:
        env.adapter.output = candidate(await seed(env))

    if kind.startswith("topic"):
        from core import topic_maintenance
        original = topic_maintenance.node_vectors
        async def fail_persist(embeddings, nodes, scope, **kwargs):
            if kwargs.get("require_persisted"):
                raise IndexCacheUnavailable()
            return await original(embeddings, nodes, scope, **kwargs)
        monkeypatch.setattr(topic_maintenance, "node_vectors", fail_persist)
    else:
        original = env.memory.index_nodes
        async def fail_final(*args, **kwargs):
            raise RetryableJobError("index_cache_unavailable")
        monkeypatch.setattr(env.memory, "index_nodes", fail_final)

    waiting = await execute(env, await enqueue_and_claim(env, kind))
    assert waiting["state"] == "retry_wait" and waiting["error_code"] == "index_cache_unavailable"
    calls = len(env.adapter.calls)
    active = await env.memory.get_active_generation(env.space["space_id"])

    recall, env.runtime.recall = env.runtime.recall, None
    missing = await execute(env, await ready_retry(env, waiting))
    assert missing["state"] == "failed" and missing["error_code"] == "index_provider_required"
    assert len(env.adapter.calls) == calls
    assert (await env.memory.get_active_generation(env.space["space_id"]))["generation_id"] == active["generation_id"]

    env.runtime.recall = recall
    if kind.startswith("topic"):
        monkeypatch.setattr(topic_maintenance, "node_vectors", original)
    else:
        monkeypatch.setattr(env.memory, "index_nodes", original)
    await env.engine.retry(missing["job_id"])
    done = await execute(env, await env.engine.jobs.claim("restored"))
    assert done["state"] == "succeeded", done["error_code"]
    assert len(env.adapter.calls) == calls
    if kind.startswith("topic"):
        assert len(done["result"]["topic_ids"]) == 1
        assert len(await env.memory.database.fetchall("SELECT * FROM topic_receipts")) == 1
    else:
        result = done["result"]["index"] if kind == "rebuild" else done["result"]
        assert result["configured"] and result["indexed"] > 0


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["provider_call", "provider_queue"])
async def test_outer_index_deadline_preserves_model_stage(indexing_env, monkeypatch, stage):
    from core import maintenance
    from tests.test_topic_maintenance import seed

    env = indexing_env
    env.config = replace(env.config, index_timeout_seconds=.15, job_max_attempts=1)
    restart(env)
    await seed(env)
    async def await_slot_or_adapter(*args, **kwargs):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError as exc:
            exc.neomemory_model_stage = stage
            raise
    monkeypatch.setattr(maintenance, "node_vectors", await_slot_or_adapter)
    result = await execute(env, await enqueue_and_claim(env, "reindex"))
    assert result["state"] == "retry_wait"
    if stage == "provider_call":
        assert result["error_code"] == "window_index_temporary_failure"
        assert result["available_at"] - result["updated_at"] == 600
    else:
        assert result["error_code"] == "model_queue_timeout" and result["attempt"] == 0
