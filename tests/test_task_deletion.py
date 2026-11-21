"""Stopped task cleanup preserves published memory and authoritative sources."""

import asyncio
import json

import pytest

from core.jobs import ReviewRequired
from core.maintenance import MaintenanceEngine
from storage.cache_store import CacheStore, text_hash
from storage.job_store import JobConflict, JobStore
from storage.memory_store import MemoryStore
from tests.test_governance import operation
from tests.test_maintenance_engine import environment, enqueue_and_claim, execute, node, output
from tests.test_short_term import env as short_term_env, claim as claim_summary, execute as execute_summary, current as current_summary


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["queued", "retry_wait", "failed", "succeeded", "cancelled", "running"])
async def test_delete_all_non_running_states(environment, state):
    env = environment
    task = await env.engine.enqueue("extract", env.space["space_id"], "task")
    if state == "running":
        await env.engine.jobs.claim("worker")
    else:
        async with env.memory.database.transaction() as connection:
            await connection.execute("UPDATE memory_jobs SET state=? WHERE job_id=?", (state, task["job_id"]))
    if state == "running":
        with pytest.raises(JobConflict):
            await env.engine.delete(task["job_id"])
        assert await env.engine.jobs.get(task["job_id"])
    else:
        assert (await env.engine.delete(task["job_id"]))["deleted"] is True
        assert await env.engine.jobs.get(task["job_id"]) is None
    assert await env.memory.database.fetchall("PRAGMA foreign_key_check") == []


@pytest.mark.asyncio
async def test_claim_and_delete_serialize(environment):
    env = environment
    task = await env.engine.enqueue("extract", env.space["space_id"], "task")
    other = MemoryStore(env.memory.path)
    await other.open()
    try:
        deleting, claiming = await asyncio.gather(
            env.engine.jobs.delete(task["job_id"]),
            JobStore(other.database).claim("other-worker"), return_exceptions=True)
        if isinstance(deleting, JobConflict):
            assert claiming["state"] == "running"
            assert (await env.engine.jobs.get(task["job_id"]))["state"] == "running"
        else:
            assert deleting["deleted"] is True
            assert claiming is None
            assert await env.engine.jobs.get(task["job_id"]) is None
    finally:
        await other.close()


async def unfinished_rebuild(env, monkeypatch):
    env.adapter.output = json.dumps(output([node(env.records[0].source_id)]))

    async def fail_index(*args):
        raise ReviewRequired("index_unavailable")

    monkeypatch.setattr(MaintenanceEngine, "_index_generation", fail_index)
    result = await execute(env, await enqueue_and_claim(env, "rebuild"))
    assert result["state"] == "failed", result
    assert result["error_code"] == "index_unavailable"
    return result


@pytest.mark.asyncio
@pytest.mark.parametrize("generation_status", ["building", "failed"])
async def test_delete_unpublished_rebuild_cleans_generation_and_vectors(environment, monkeypatch, generation_status):
    env = environment
    active = await env.memory.get_active_generation(env.space["space_id"])
    task = await unfinished_rebuild(env, monkeypatch)
    await env.engine.jobs.retry(task["job_id"])
    claimed = await env.engine.jobs.claim("candidate-owner")
    await env.engine.jobs.archive_candidate(task["job_id"], claimed["lease_token"], {"raw": "rejected candidate"}, "invalid_json")
    await env.engine.jobs.cancel(task["job_id"])
    generation_id = task["checkpoint"]["generation_id"]
    rows = await env.memory.database.fetchall("SELECT * FROM memory_nodes WHERE generation_id=?", (generation_id,))
    assert len(rows) == 1
    assert await env.memory.database.fetchall("SELECT * FROM memory_derivations")
    async with env.memory.database.transaction() as connection:
        await connection.execute("UPDATE memory_generations SET status=? WHERE generation_id=?", (generation_status, generation_id))
    cache = CacheStore(env.memory.path.parent / "cache.db")
    await cache.open()
    env.runtime.cache = cache
    try:
        hashes = [text_hash("合成记忆正文")]
        identities = [(rows[0]["node_id"], rows[0]["content_version"])]
        await cache.put_vectors("embedding", env.space["space_id"], hashes, [[1, 0]], kind="node", identities=identities)
        env.engine._pending_reads[generation_id] = "cached"
        env.engine._read_hints[generation_id] = "cached"
        result = await env.engine.delete(task["job_id"])
        assert result["removed_generation_id"] == generation_id
        assert result["removed_node_count"] == 1
        for table in ("memory_jobs", "memory_job_candidates", "rebuild_runs", "rebuild_receipts", "memory_nodes", "memory_node_versions", "memory_derivations", "extraction_batches", "extraction_batch_sources", "derived_cursors"):
            assert await env.memory.database.fetchall(f"SELECT * FROM {table}") == [], table
        assert await env.memory.database.fetchone("SELECT * FROM memory_generations WHERE generation_id=?", (generation_id,)) is None
        assert (await env.memory.get_active_generation(env.space["space_id"]))["generation_id"] == active["generation_id"]
        assert await cache.get_vectors("embedding", env.space["space_id"], hashes, kind="node", identities=identities) == [None]
        assert generation_id not in env.engine._pending_reads
        assert generation_id not in env.engine._read_hints
        assert all([await env.sources.has_source(record.source_id) for record in env.records])
        assert await env.memory.database.fetchall("PRAGMA foreign_key_check") == []
    finally:
        await cache.close()
        env.runtime.cache = None


@pytest.mark.asyncio
async def test_cache_cleanup_failure_keeps_task_and_draft_retryable(environment, monkeypatch):
    env = environment
    task = await unfinished_rebuild(env, monkeypatch)

    async def fail_cache(*args):
        raise OSError("cache busy")

    with pytest.raises(OSError):
        await env.engine.jobs.delete(task["job_id"], invalidate_nodes=fail_cache)
    assert await env.engine.jobs.get(task["job_id"])
    assert await env.memory.database.fetchone("SELECT * FROM memory_generations WHERE generation_id=?", (task["checkpoint"]["generation_id"],))
    assert await env.memory.database.fetchall("SELECT * FROM rebuild_runs")
    assert (await env.engine.delete(task["job_id"]))["removed_node_count"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("bulk", [False, True])
async def test_successful_rebuild_task_deletion_keeps_published_memory(environment, bulk):
    env = environment
    env.adapter.output = json.dumps(output([node(env.records[0].source_id)]))
    task = await execute(env, await enqueue_and_claim(env, "rebuild"))
    assert task["state"] == "succeeded", task
    active = await env.memory.get_active_generation(env.space["space_id"])
    derivations = await env.memory.database.fetchall("SELECT * FROM memory_derivations")
    if bulk:
        result = await env.engine.clear_completed(scope_key=env.space["space_id"])
        assert result == {"deleted_count": 1, "removed_node_count": 0}
    else:
        result = await env.engine.delete(task["job_id"])
        assert result["removed_generation_id"] is None
    assert await env.memory.get_active_generation(env.space["space_id"]) == active
    assert len(await env.memory.database.fetchall("SELECT * FROM memory_nodes")) == 1
    assert await env.memory.database.fetchall("SELECT * FROM memory_derivations") == derivations
    assert await env.memory.database.fetchall("SELECT * FROM rebuild_runs") == []
    assert await env.memory.database.fetchall("PRAGMA foreign_key_check") == []


@pytest.mark.asyncio
async def test_governance_job_deletion_preserves_applied_provenance(environment):
    env = environment
    item = await env.memory.create_node(space_id=env.space["space_id"], node_type="atomic", content_text="旧描述", source_ids=[env.records[0].source_id])
    op = operation("edit", [item], patch={"content_text": "人工修正描述"})
    task = await execute(env, await enqueue_and_claim(env, "governance", operation=op))
    assert task["state"] == "succeeded"
    provenance = await env.memory.database.fetchall("SELECT * FROM memory_derivations")
    await env.engine.delete(task["job_id"])
    assert (await env.memory.get_node(item["node_id"]))["content_text"] == "人工修正描述"
    assert await env.memory.database.fetchall("SELECT * FROM memory_derivations") == provenance
    assert await env.memory.database.fetchall("SELECT * FROM governance_receipts WHERE job_id=?", (task["job_id"],))
    assert await env.memory.database.fetchall("PRAGMA foreign_key_check") == []


@pytest.mark.asyncio
async def test_delete_drains_cancelled_worker_before_cleanup(environment):
    env = environment
    task = await env.engine.enqueue("extract", env.space["space_id"], "task")
    await env.engine.jobs.cancel(task["job_id"])
    started = asyncio.Event()
    drained = asyncio.Event()

    async def lingering():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            drained.set()

    worker = asyncio.create_task(lingering())
    env.engine.runner._workers[task["job_id"]] = worker
    await started.wait()
    await env.engine.delete(task["job_id"])
    assert drained.is_set()
    assert worker.done()
    assert await env.engine.jobs.get(task["job_id"]) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("bulk", [False, True])
async def test_note_task_deletion_clears_receipt_but_preserves_note(environment, bulk):
    from storage.console_store import NoteService

    env = environment
    service = NoteService(env.memory, env.engine.jobs)
    task = await service.enqueue(env.space["space_id"], "note", {"action": "create", "content_text": "保留的人工笔记"})
    claimed = await env.engine.jobs.claim("notes")
    result = await service.run(claimed)
    await env.engine.jobs.finish(task["job_id"], claimed["lease_token"], result=result)
    assert await env.memory.database.fetchall("SELECT * FROM note_receipts")
    if bulk:
        assert (await env.engine.clear_completed())["deleted_count"] == 1
    else:
        await env.engine.delete(task["job_id"])
    assert await env.memory.database.fetchall("SELECT * FROM note_receipts") == []
    assert await env.memory.database.fetchone("SELECT * FROM notes WHERE note_id=?", (result["note_id"],))
    assert await env.memory.database.fetchall("PRAGMA foreign_key_check") == []


@pytest.mark.asyncio
@pytest.mark.parametrize("bulk", [False, True])
async def test_short_term_task_deletion_preserves_summary_and_source_coverage(short_term_env, bulk):
    env = short_term_env
    task = await execute_summary(env, await claim_summary(env))
    assert task["state"] == "succeeded", task
    current = await current_summary(env)
    coverage = await env.memory.database.fetchall("SELECT * FROM short_term_sources")
    assert await env.memory.database.fetchall("SELECT * FROM short_term_receipts")
    if bulk:
        assert (await env.jobs.clear_completed())["deleted_count"] == 1
    else:
        await env.jobs.delete(task["job_id"])
    assert await env.memory.database.fetchall("SELECT * FROM short_term_receipts") == []
    assert await current_summary(env) == current
    assert await env.memory.database.fetchall("SELECT * FROM short_term_sources") == coverage
    assert await env.memory.database.fetchall("PRAGMA foreign_key_check") == []


async def completed_job(jobs, scope, key, *, archive=False):
    job = await jobs.submit('model_test', scope, {}, key)
    claimed = await jobs.claim('bulk-test')
    assert claimed['job_id'] == job['job_id']
    if archive:
        await jobs.archive_candidate(job['job_id'], claimed['lease_token'], {'raw': 'synthetic rejected output'}, 'invalid_json')
    return await jobs.finish(job['job_id'], claimed['lease_token'], result={'synthetic': True})


@pytest.mark.asyncio
async def test_clear_completed_all_pages_scope_and_state_selection(environment):
    env = environment
    scope = env.space['space_id']
    completed = [await completed_job(env.engine.jobs, scope, f'completed-{i}', archive=i == 0) for i in range(25)]
    other = await completed_job(env.engine.jobs, 'other-space', 'completed-elsewhere')
    retained = []
    for state in ('running', 'failed', 'cancelled', 'queued', 'retry_wait'):
        job = await env.engine.jobs.submit('model_test', f'scope-{state}', {}, f'keep-{state}')
        if state == 'running':
            assert (await env.engine.jobs.claim('state-worker'))['job_id'] == job['job_id']
        else:
            async with env.memory.database.transaction() as connection:
                target_scope = scope if state in ('failed', 'cancelled', 'queued') else f'scope-{state}'
                await connection.execute('UPDATE memory_jobs SET scope_key=?,state=? WHERE job_id=?', (target_scope, state, job['job_id']))
        retained.append(job['job_id'])
    assert await env.memory.database.fetchall('SELECT * FROM memory_job_candidates')
    assert await env.engine.clear_completed(scope_key=scope) == {'deleted_count': 25, 'removed_node_count': 0}
    assert all([await env.engine.jobs.get(job['job_id']) is None for job in completed])
    assert all([await env.engine.jobs.get(job_id) is not None for job_id in retained])
    assert await env.engine.jobs.get(other['job_id']) is not None
    assert await env.memory.database.fetchall('SELECT * FROM memory_job_candidates') == []
    assert await env.engine.clear_completed(scope_key=scope) == {'deleted_count': 0, 'removed_node_count': 0}
    assert await env.engine.clear_completed() == {'deleted_count': 1, 'removed_node_count': 0}
    assert all([await env.engine.jobs.get(job_id) is not None for job_id in retained])
    assert all([await env.sources.has_source(record.source_id) for record in env.records])
    assert await env.memory.database.fetchall('PRAGMA foreign_key_check') == []


@pytest.mark.asyncio
async def test_clear_completed_rolls_back_whole_batch_on_cleanup_failure(environment, monkeypatch):
    env = environment
    jobs = env.engine.jobs
    first = await completed_job(jobs, env.space['space_id'], 'first', archive=True)
    second = await completed_job(jobs, env.space['space_id'], 'second')
    original = jobs._delete_stopped
    calls = 0

    async def fail_second(connection, job, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError('synthetic cleanup failure')
        return await original(connection, job, **kwargs)

    monkeypatch.setattr(jobs, '_delete_stopped', fail_second)
    with pytest.raises(OSError, match='synthetic cleanup'):
        await env.engine.clear_completed()
    assert await jobs.get(first['job_id']) is not None
    assert await jobs.get(second['job_id']) is not None
    assert len(await env.memory.database.fetchall('SELECT * FROM memory_job_candidates')) == 1
    monkeypatch.setattr(jobs, '_delete_stopped', original)
    assert await env.engine.clear_completed() == {'deleted_count': 2, 'removed_node_count': 0}


@pytest.mark.asyncio
@pytest.mark.parametrize("publish", [False, True])
async def test_delete_topic_rebuild_only_evicts_unpublished_draft_vectors(environment, publish):
    from storage.topic_rebuild_publish import publish_topics, topic_rebuild_basis
    from storage.topic_rebuild_stage import export_topics, persist_topic_stage, topic_rebuild_stage
    from storage.topic_store import TopicStore
    from tests.test_topic_maintenance import candidate, seed

    env = environment
    nodes = await seed(env)
    space_id = env.space["space_id"]
    generation_id = nodes[0]["generation_id"]
    job = await enqueue_and_claim(env, "topic_rebuild")
    basis = await topic_rebuild_basis(env.memory, space_id, generation_id)
    async with topic_rebuild_stage(env.memory, space_id, generation_id, job) as stage:
        store = TopicStore(stage)
        prepared = await store.prepare(space_id, generation_id, [node["node_id"] for node in nodes])
        receipt = await store.apply(job, prepared, json.loads(candidate(nodes)))
        await persist_topic_stage(env.memory, stage, job, {"prepared": prepared}, receipt)
        drafts, reviewed = await export_topics(stage)
    topic_id, = receipt["topic_ids"]
    if publish:
        await publish_topics(env.memory, job, generation_id, drafts, reviewed, basis)
        await env.engine.jobs.finish(job["job_id"], job["lease_token"])
    else:
        await env.engine.jobs.cancel(job["job_id"])

    cache = CacheStore(env.memory.path.parent / "cache.db")
    await cache.open()
    env.runtime.cache = cache
    try:
        hashes = [text_hash(drafts[0]["content"]), text_hash(nodes[0]["content_text"])]
        identities = [(topic_id, 1), (nodes[0]["node_id"], 1)]
        vectors = [[1., 0.], [0., 1.]]
        await cache.put_vectors("provider", space_id, hashes, vectors, kind="node", identities=identities)
        result = await env.engine.delete(job["job_id"])
        assert result["removed_node_count"] == (0 if publish else 1)
        values = await cache.get_vectors("provider", space_id, hashes, kind="node", identities=identities)
        assert values == [vectors[0] if publish else None, vectors[1]]
        assert not await env.memory.database.fetchall("SELECT * FROM topic_rebuild_stage")
        assert not await env.memory.database.fetchall("SELECT * FROM topic_receipts")
    finally:
        env.runtime.cache = None
        await cache.close()
