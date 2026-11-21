"""Topic synthesis is part of the unpublished rebuild, with resumable nested progress."""

import json
from dataclasses import replace

import pytest

from core.jobs import RetryableJobError, ReviewRequired
from tests.test_extraction_apply import capture
from tests.test_maintenance_engine import environment, enqueue_and_claim, execute, node, output

pytestmark = pytest.mark.asyncio


def provider(env, monkeypatch, *, fail_topic=None, capture_later=False):
    calls = {"extract": 0, "topic": 0, "later": [], "fail_topic": fail_topic}

    async def generate(messages, **kwargs):
        inputs = json.loads(next(message["content"] for message in messages if message["role"] == "user"))
        if inputs.get("schema_version") == "topic-consolidation.v1":
            calls["topic"] += 1
            if capture_later and not calls["later"]:
                calls["later"].append(await capture(env.sources, number=3))
            if calls["fail_topic"] == "transient" and calls["topic"] == 1:
                raise ConnectionError("synthetic provider disconnect")
            if calls["fail_topic"] == "invalid":
                return "invalid json"
            return json.dumps({"schema_version": "topic-consolidation.v1", "changes": [{
                "operation": "create", "topic_id": None, "expected_version": None,
                "title": "社区观星计划", "content": "社区观星计划包含两场独立的活动，各有不同的参与对象。",
                "importance": .6, "emotion": None,
                "member_ids": [item["node_id"] for item in inputs["nodes"] if item["node_type"] == "event"]}]})
        calls["extract"] += 1
        sources = inputs["sources"]
        return json.dumps(output([
            node(sources[0]["source_id"], ref="local_first", content="社区观星计划的第一场活动已经完成。"),
            node(sources[1]["source_id"], ref="local_second", content="社区观星计划的第二场活动已经完成，参与对象与第一场不同。"),
        ]))

    monkeypatch.setattr(env.adapter, "generate", generate)
    return calls


async def resume(env, job_id):
    await env.memory.database.execute("UPDATE memory_jobs SET available_at=0 WHERE job_id=?", (job_id,))
    claimed = await env.engine.jobs.claim("resume", lease_seconds=60)
    assert claimed["job_id"] == job_id
    return await execute(env, claimed)


async def assert_published_once(env, result):
    assert result["state"] == "succeeded", (result["error_code"], result["error_type"])
    generation = await env.memory.get_active_generation(env.space["space_id"])
    assert generation["activity_round"] == 1
    nodes = await env.memory.list_nodes(space_id=env.space["space_id"])
    assert sorted(item["node_type"] for item in nodes) == ["event", "event", "topic"]
    sources = await env.memory.database.fetchall(
        "SELECT source_id FROM extraction_batch_sources WHERE generation_id=?", (generation["generation_id"],))
    assert {row["source_id"] for row in sources} == {record.source_id for record in env.records}
    assert len(sources) == 2
    assert len(await env.memory.database.fetchall("SELECT * FROM topic_receipts")) == 1
    assert result["result"]["source_count"] == 2


async def test_topic_provider_failure_resumes_without_replaying_events(environment, monkeypatch):
    env = environment
    old = await env.memory.get_active_generation(env.space["space_id"])
    calls = provider(env, monkeypatch, fail_topic="transient", capture_later=True)
    job = await enqueue_and_claim(env, "rebuild")
    failed = await execute(env, job)
    assert failed["state"] == "retry_wait", failed
    assert failed["checkpoint"]["phase"] == "topics"
    nested = failed["checkpoint"]["topic_synthesis"]
    assert nested["topic_window"]["prepared"]["nodes"]
    assert failed["checkpoint"]["done"] == 2
    assert (await env.memory.get_active_generation(env.space["space_id"]))["generation_id"] == old["generation_id"]
    target = failed["checkpoint"]["generation_id"]
    assert len(await env.memory.list_nodes(space_id=env.space["space_id"], generation_id=target)) == 2
    completed = await resume(env, job["job_id"])
    await assert_published_once(env, completed)
    assert calls["extract"] == 1 and calls["topic"] == 2
    assert len(calls["later"]) == 1
    assert not await env.engine.pipeline.consumed(target, [calls["later"][0].source_id])


async def test_committed_topic_index_failure_preserves_nested_receipt(environment, monkeypatch):
    env = environment
    old = await env.memory.get_active_generation(env.space["space_id"])
    calls = provider(env, monkeypatch)
    original = type(env.engine)._topic_index
    index_attempts = []

    async def fail_once(self, *args):
        index_attempts.append(True)
        if len(index_attempts) == 1:
            raise RetryableJobError("index_cache_unavailable")
        return await original(self, *args)

    monkeypatch.setattr(type(env.engine), "_topic_index", fail_once)
    job = await enqueue_and_claim(env, "rebuild")
    failed = await execute(env, job)
    assert failed["state"] == "retry_wait", failed
    target = failed["checkpoint"]["generation_id"]
    assert (await env.memory.get_active_generation(env.space["space_id"]))["generation_id"] == old["generation_id"]
    assert len(await env.memory.list_nodes(space_id=env.space["space_id"], generation_id=target)) == 3
    assert failed["checkpoint"]["topic_synthesis"]["topic_window"]["receipt_key"]
    completed = await resume(env, job["job_id"])
    await assert_published_once(env, completed)
    assert calls["extract"] == calls["topic"] == 1


async def test_switch_failure_does_not_repeat_topic_or_extraction(environment, monkeypatch):
    env = environment
    old = await env.memory.get_active_generation(env.space["space_id"])
    calls = provider(env, monkeypatch)
    original = env.engine.pipeline.activate_rebuild
    switches = []

    async def fail_once(*args, **kwargs):
        switches.append(True)
        if len(switches) == 1:
            raise RetryableJobError("synthetic_switch_failure")
        return await original(*args, **kwargs)

    monkeypatch.setattr(env.engine.pipeline, "activate_rebuild", fail_once)
    job = await enqueue_and_claim(env, "rebuild")
    failed = await execute(env, job)
    assert failed["state"] == "retry_wait", failed
    assert failed["checkpoint"]["phase"] == "switch"
    assert failed["checkpoint"]["topic_result"]["reviewed_count"] == 2
    assert (await env.memory.get_active_generation(env.space["space_id"]))["generation_id"] == old["generation_id"]
    completed = await resume(env, job["job_id"])
    await assert_published_once(env, completed)
    assert calls["extract"] == calls["topic"] == 1


async def test_invalid_topic_never_publishes_partial_rebuild(environment, monkeypatch):
    env = environment
    old = await env.memory.get_active_generation(env.space["space_id"])
    calls = provider(env, monkeypatch, fail_topic="invalid")
    failed = await execute(env, await enqueue_and_claim(env, "rebuild"))
    assert failed["state"] == "failed" and failed["error_code"] == "invalid_topic_output", failed
    assert (await env.memory.get_active_generation(env.space["space_id"]))["generation_id"] == old["generation_id"]
    assert not await env.engine.pipeline.get_rebuild_receipt(failed["job_id"])
    target = failed["checkpoint"]["generation_id"]
    assert (await env.memory.database.fetchone("SELECT status FROM memory_generations WHERE generation_id=?", (target,)))["status"] == "building"
    assert len(await env.memory.list_nodes(space_id=env.space["space_id"], generation_id=target)) == 2
    assert calls["extract"] == calls["topic"] == 1


async def test_explicit_regeneration_resumes_nested_topic_without_replaying_events(environment, monkeypatch):
    env = environment
    env.engine.config = replace(env.config, extraction_auto_regeneration=True)
    calls = provider(env, monkeypatch, fail_topic="invalid")
    failed = await execute(env, await enqueue_and_claim(env, "rebuild"))
    assert failed["state"] == "failed"
    assert calls["extract"] == 1 and calls["topic"] == 4
    target = failed["checkpoint"]["generation_id"]
    topic_window = failed["checkpoint"]["topic_synthesis"]["topic_window"]
    assert topic_window["corrections"] == 3
    detail = await env.engine.describe(failed["job_id"])
    assert detail["regenerate_window_available"]
    assert detail["validation_errors"]
    assert detail["automatic_corrections"] == detail["automatic_correction_limit"] == 3
    assert detail["candidate_format_invalid"]
    calls["fail_topic"] = None
    queued = await env.engine.retry(failed["job_id"], regenerate_window=True)
    assert queued["job_id"] == failed["job_id"]
    assert queued["checkpoint"]["generation_id"] == target
    assert queued["checkpoint"]["topic_synthesis"]["topic_window"]["corrections"] == 3
    assert "raw" not in queued["checkpoint"]["topic_synthesis"]["topic_window"]
    completed = await resume(env, failed["job_id"])
    await assert_published_once(env, completed)
    assert calls["extract"] == 1 and calls["topic"] == 5
    assert len(await env.engine.jobs.list_candidates(failed["job_id"])) == 4


async def test_committed_topic_cannot_be_regenerated_when_only_indexing_failed(environment, monkeypatch):
    env = environment
    calls = provider(env, monkeypatch)
    original = type(env.engine)._topic_index

    async def fail(self, *args):
        raise ReviewRequired("index_requires_review")

    monkeypatch.setattr(type(env.engine), "_topic_index", fail)
    failed = await execute(env, await enqueue_and_claim(env, "rebuild"))
    assert failed["state"] == "failed"
    detail = await env.engine.describe(failed["job_id"])
    assert not detail["regenerate_window_available"]
    assert detail["candidate"]["schema_version"] == "topic-consolidation.v1"
    with pytest.raises(ValueError, match="already committed"):
        await env.engine.retry(failed["job_id"], regenerate_window=True)
    monkeypatch.setattr(type(env.engine), "_topic_index", original)
    await env.engine.retry(failed["job_id"])
    completed = await resume(env, failed["job_id"])
    await assert_published_once(env, completed)
    assert calls["extract"] == calls["topic"] == 1
