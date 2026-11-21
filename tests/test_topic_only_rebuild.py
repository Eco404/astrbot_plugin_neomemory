"""Exercise topic replacement through the maintenance runner, including interrupted windows."""

import json

import pytest

from tests.test_maintenance_engine import environment, enqueue_and_claim, execute, restart
from tests.test_topic_maintenance import candidate, seed


@pytest.mark.asyncio
async def test_interrupted_rebuild_resumes_drafts_and_publishes_final_text_as_v1(environment):
    env = environment
    nodes = await seed(env, count=9)
    old = await env.memory.create_node(space_id=env.space["space_id"], node_type="topic",
        content_text="旧主题", source_ids=[env.records[0].source_id])
    old_id = old["node_id"]

    async def generate(messages):
        user_input = json.loads(next(m["content"] for m in messages if m["role"] == "user"))
        assert old_id not in {n["node_id"] for n in user_input["nodes"]}
        assert await env.memory.get_node(old_id)
        if len(env.adapter.calls) == 1:
            env.adapter.output = candidate(nodes[:2])
        elif len(env.adapter.calls) == 2:
            raise ConnectionError("provider unavailable")
        else:
            draft, = [n for n in user_input["nodes"] if n["node_type"] == "topic"]
            result = json.loads(candidate(nodes[:3]))
            result["changes"][0].update(operation="update", topic_id=draft["node_id"],
                expected_version=1, content="多个窗口归纳出的最终主题。")
            env.adapter.output = json.dumps(result)

    env.adapter.on_generate = generate
    job = await enqueue_and_claim(env, "topic_rebuild")
    failed = await execute(env, job)
    assert failed["state"] == "retry_wait" and failed["progress_done"] == 8, failed
    visible = await env.memory.list_nodes(space_id=env.space["space_id"])
    assert {n["node_id"] for n in visible if n["node_type"] == "topic"} == {old_id}

    restart(env)
    await env.memory.database.execute("UPDATE memory_jobs SET available_at=0 WHERE job_id=?", (job["job_id"],))
    completed = await execute(env, await env.engine.jobs.claim("resume", lease_seconds=60))
    assert completed["state"] == "succeeded", completed
    assert len(env.adapter.calls) == 3
    assert completed["result"]["removed_count"] == 1 and completed["result"]["reviewed_count"] == 9
    new_id, = completed["result"]["topic_ids"]
    topic = await env.memory.get_node(new_id)
    assert topic["content_text"] == "多个窗口归纳出的最终主题。" and topic["content_version"] == 1
    assert len(await env.memory.list_node_versions(new_id)) == 1
    assert await env.memory.get_node(old_id) is None
    assert not await env.memory.database.fetchall("SELECT 1 FROM topic_rebuild_stage")
    assert not await env.memory.database.fetchall("SELECT 1 FROM extraction_batches")
