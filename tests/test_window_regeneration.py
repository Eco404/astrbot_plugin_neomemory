"""Regenerate a failed candidate without discarding committed replay windows."""

import asyncio
import json

import pytest

from core.extraction_regeneration import VERSION as REGENERATION_VERSION
from storage.job_store import JobConflict, JobStore, LeaseLost
from storage.sqlite_executor import SQLiteExecutor
from tests.test_extraction_apply import capture
from tests.test_maintenance_engine import enqueue_and_claim, execute, output
from tests.test_maintenance_engine import environment as environment


async def fail_candidate(env, *, kind="extract"):
    env.adapter.output = "invalid candidate"
    failed = await execute(env, await enqueue_and_claim(env, kind))
    assert failed["state"] == "failed"
    return failed


@pytest.mark.asyncio
async def test_regenerate_rebuild_preserves_committed_windows_and_failure_evidence(environment):
    env = environment
    for number in range(3, 7):
        await capture(env.sources, number=number)

    async def respond(messages):
        env.adapter.output = "invalid second window" if len(env.adapter.calls) == 2 else json.dumps(output())

    env.adapter.on_generate = respond
    failed = await execute(env, await enqueue_and_claim(env, "rebuild"))
    assert failed["state"] == "failed"
    assert failed["checkpoint"]["done"] == 2
    target = failed["checkpoint"]["generation_id"]
    before = await env.memory.database.fetchall("SELECT * FROM extraction_batches ORDER BY batch_id")
    regenerated = await env.engine.retry(failed["job_id"], regenerate_window=True)
    assert regenerated["job_id"] == failed["job_id"]
    history = await env.engine.jobs.list_candidates(failed["job_id"])
    assert regenerated["checkpoint"] == {
        **failed["checkpoint"],
        "window": {**{key: value for key, value in failed["checkpoint"]["window"].items()
                      if key not in {"raw", "model_request_digest", "validation_errors", "response_diagnostics"}},
                   "regeneration": {"version": REGENERATION_VERSION, "candidate_id": history[0]["candidate_id"]}},
    }
    assert regenerated["progress_done"] == 2
    assert await env.memory.database.fetchall("SELECT * FROM extraction_batches ORDER BY batch_id") == before
    completed = await execute(env, await env.engine.jobs.claim("regenerated"))
    assert completed["state"] == "succeeded"
    assert completed["result"]["generation_id"] == target
    assert completed["result"]["source_count"] == 6
    assert len(env.adapter.calls) == 4
    assert completed["checkpoint"] == {}
    history = await env.engine.jobs.list_candidates(failed["job_id"])
    assert len(history) == 1
    assert history[0]["window"] == failed["checkpoint"]["window"]
    assert history[0]["error_code"] == "invalid_output"
    detail = await env.engine.describe(failed["job_id"])
    assert detail["candidate_history"] == history
    assert not detail["regenerate_window_available"]


@pytest.mark.asyncio
async def test_regeneration_is_atomic_and_idempotent_across_connections(environment):
    env = environment
    failed = await fail_candidate(env)
    connection = SQLiteExecutor(env.memory.database.path)
    await connection.open()
    try:
        other = JobStore(connection)
        results = await asyncio.gather(
            env.engine.jobs.regenerate_window(failed["job_id"]), other.regenerate_window(failed["job_id"]))
        assert {item["job_id"] for item in results} == {failed["job_id"]}
        assert all(item["state"] == "queued" for item in results)
        assert len(await other.list_candidates(failed["job_id"])) == 1
        assert len(await other.list(states=["queued"])) == 1
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_competing_operation_blocks_regeneration_without_archiving_or_losing_raw(environment):
    env = environment
    failed = await fail_candidate(env)
    await env.engine.jobs.submit("reindex", env.space["space_id"], {}, "competing")
    with pytest.raises(JobConflict):
        await env.engine.retry(failed["job_id"], regenerate_window=True)
    assert (await env.engine.jobs.get(failed["job_id"]))["checkpoint"] == failed["checkpoint"]
    assert await env.engine.jobs.list_candidates(failed["job_id"]) == []


@pytest.mark.asyncio
async def test_regeneration_and_new_task_race_have_one_scope_owner(environment):
    env = environment
    failed = await fail_candidate(env)
    connection = SQLiteExecutor(env.memory.database.path)
    await connection.open()
    try:
        other = JobStore(connection)
        results = await asyncio.gather(
            env.engine.jobs.regenerate_window(failed["job_id"]),
            other.submit("reindex", env.space["space_id"], {}, "racing-task"), return_exceptions=True)
        assert sum(isinstance(item, dict) for item in results) == 1
        assert sum(isinstance(item, JobConflict) for item in results) == 1
        assert len(await other.list(states=["queued"])) == 1
        if isinstance(results[0], JobConflict):
            assert (await other.get(failed["job_id"]))["checkpoint"] == failed["checkpoint"]
            assert await other.list_candidates(failed["job_id"]) == []
        else:
            assert len(await other.list_candidates(failed["job_id"])) == 1
    finally:
        await connection.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["persona", "model"])
async def test_regeneration_keeps_configuration_guards(environment, change):
    env = environment
    failed = await fail_candidate(env)
    if change == "persona":
        env.persona["text"] = "A different persona."
    else:
        env.adapter.model_revision = "changed"
    await env.engine.retry(failed["job_id"], regenerate_window=True)
    rejected = await execute(env, await env.engine.jobs.claim("changed"))
    assert rejected["state"] == "failed"
    assert rejected["error_code"] == "configuration_changed"
    assert len(env.adapter.calls) == 1
    assert len(await env.engine.jobs.list_candidates(failed["job_id"])) == 1


@pytest.mark.asyncio
async def test_regeneration_keeps_input_digest_guard(environment):
    env = environment
    known = await env.memory.create_node(space_id=env.space["space_id"], node_type="event",
        content_text="A public synthetic event.", source_ids=[env.records[0].source_id])
    failed = await fail_candidate(env)
    assert known["node_id"] in failed["checkpoint"]["window"]["known_node_ids"]
    await env.memory.update_node_content(known["node_id"], "A corrected public synthetic event.")
    await env.engine.retry(failed["job_id"], regenerate_window=True)
    rejected = await execute(env, await env.engine.jobs.claim("changed-input"))
    assert rejected["state"] == "failed"
    assert rejected["error_code"] == "context_changed"
    assert len(env.adapter.calls) == 1


@pytest.mark.asyncio
async def test_candidate_archival_requires_current_lease_and_survives_finish(environment):
    env = environment
    job = await enqueue_and_claim(env)
    window = {"source_ids": [env.records[0].source_id], "raw": "rejected candidate"}
    with pytest.raises(LeaseLost):
        await env.engine.jobs.archive_candidate(job["job_id"], "expired", window, "invalid_output")
    first = await env.engine.jobs.archive_candidate(job["job_id"], job["lease_token"], window, "invalid_output")
    second = await env.engine.jobs.archive_candidate(job["job_id"], job["lease_token"], window, "invalid_output")
    assert first == second
    await env.engine.jobs.finish(job["job_id"], job["lease_token"])
    assert len(await env.engine.jobs.list_candidates(job["job_id"])) == 1


@pytest.mark.asyncio
async def test_regeneration_requires_candidate_and_exclusive_mode(environment):
    env = environment
    job = await enqueue_and_claim(env)
    await env.engine.jobs.cancel(job["job_id"])
    with pytest.raises(ValueError, match="candidate"):
        await env.engine.retry(job["job_id"], regenerate_window=True)
    with pytest.raises(ValueError, match="exclusive"):
        await env.engine.retry(job["job_id"], fresh=True, regenerate_window=True)
