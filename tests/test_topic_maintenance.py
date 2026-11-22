"""Durable synthesis preserves events, source consumption and activity rounds."""

import asyncio
import json
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest

from core.jobs import RetryableJobError
from scripts.probe_context_coverage import FixedEmbeddings
from storage.topic_store import TopicStore
from tests.test_maintenance_engine import environment, enqueue_and_claim, execute


@pytest.mark.asyncio
async def test_topic_neighbors_cover_external_matches_across_pages(environment, monkeypatch):
    env = environment
    vectors = {"seed-a": [1, 0, 0], "seed-b": [0, 1, 0], "seed-b2": [0, 1, 0],
               "common": [.99, 0, .141], "shared": [0, .8, .6]}
    vectors.update({f"majority-{index}": [.96, 0, .28] for index in range(12)})
    nodes = {key: {"node_id": key, "content_text": key, "content_version": 1, "manual_protected": False} for key in vectors}
    seeds = [nodes[key] for key in ("seed-a", "seed-b", "seed-b2")]

    async def page(_space, _generation, *, after, limit):
        if not after:
            return {"nodes": seeds, "after": "first", "has_more": True}
        return {"nodes": [row for row in nodes.values() if row not in seeds], "has_more": False}

    monkeypatch.setattr(env.memory, "extraction_context_page", page)
    env.runtime.recall = SimpleNamespace(embeddings=FixedEmbeddings(vectors))
    env.engine.config = replace(env.config, topic_context_nodes=8)
    ids = await env.engine._topic_context(env.space, "generation", seeds, time.monotonic() + 10)
    assert ids[:3] == [row["node_id"] for row in seeds]
    assert {"common", "shared"} <= set(ids)
    assert len(ids) == len(set(ids)) == 8


async def seed(env, count=2):
    return [await env.memory.create_node(space_id=env.space["space_id"], node_type="event",
        content_text=f"合成社区观星计划的第{index + 1}次独立活动，各次参与对象不同。",
        source_ids=[env.records[0].source_id]) for index in range(count)]


def candidate(nodes):
    return json.dumps({"schema_version": "topic-consolidation.v1", "changes": [{
        "operation": "create", "topic_id": None, "expected_version": None,
        "title": "持续的社区观星计划", "content": "社区观星计划由多次独立活动组成，参与对象逐次变化。",
        "importance": .6, "emotion": None, "member_ids": [node["node_id"] for node in nodes]}]})


@pytest.mark.asyncio
async def test_topic_job_preserves_events_sources_and_rounds(environment):
    env = environment
    nodes = await seed(env)
    env.adapter.output = candidate(nodes)
    job = await enqueue_and_claim(env, "topic_consolidate")
    completed = await execute(env, job)
    assert completed["state"] == "succeeded", completed
    assert completed["result"]["reviewed_count"] == 2
    assert len(completed["result"]["topic_ids"]) == 1
    for original in nodes:
        actual = await env.memory.get_node(original["node_id"])
        assert actual["content_text"] == original["content_text"] and actual["content_version"] == 1
    generation = await env.memory.get_active_generation(env.space["space_id"])
    assert generation["activity_round"] == 0
    assert not await env.memory.database.fetchall("SELECT * FROM extraction_batch_sources")
    assert not await TopicStore(env.memory).dirty_nodes(env.space["space_id"], generation["generation_id"])


@pytest.mark.asyncio
async def test_index_failure_resumes_committed_topic_without_model_repeat(environment, monkeypatch):
    env = environment
    env.adapter.output = candidate(await seed(env))
    job = await enqueue_and_claim(env, "topic_consolidate")
    original = env.engine._topic_index

    async def fail(*args):
        raise RetryableJobError("index_cache_unavailable")

    monkeypatch.setattr(type(env.engine), "_topic_index", fail)
    failed = await execute(env, job)
    assert failed["state"] == "retry_wait"
    assert len(env.adapter.calls) == 1
    monkeypatch.setattr(type(env.engine), "_topic_index", lambda self, *args: original(*args))
    await env.memory.database.execute("UPDATE memory_jobs SET available_at=0 WHERE job_id=?", (job["job_id"],))
    resumed = await env.engine.jobs.claim("resume", lease_seconds=60)
    completed = await execute(env, resumed)
    assert completed["state"] == "succeeded", completed
    assert len(env.adapter.calls) == 1
    count = await env.memory.database.fetchone("SELECT COUNT(*) AS n FROM memory_nodes WHERE node_type='topic'")
    assert count["n"] == 1


@pytest.mark.asyncio
async def test_topic_invalid_output_has_bounded_persisted_correction(environment):
    env = environment
    env.engine.config = replace(env.config, extraction_auto_regeneration=True)
    await seed(env)
    env.adapter.output = "not json"
    job = await enqueue_and_claim(env, "topic_consolidate")
    failed = await execute(env, job)
    assert failed["state"] == "failed", failed
    assert failed["error_code"] == "invalid_topic_output"
    assert len(env.adapter.calls) == 4
    assert failed["checkpoint"]["topic_window"]["corrections"] == 3
    await env.engine.retry(job["job_id"])
    resumed = await env.engine.jobs.claim("resume", lease_seconds=60)
    await execute(env, resumed)
    assert len(env.adapter.calls) == 4


@pytest.mark.asyncio
async def test_low_frequency_schedule_and_no_change_review(environment):
    env = environment
    nodes = await seed(env)
    generation = await env.memory.get_active_generation(env.space["space_id"])
    await env.engine._maybe_enqueue_topics(env.space, generation)
    assert not await env.engine.jobs.list()
    await env.memory.database.execute("UPDATE memory_nodes SET updated_at=?", (time.time() - 4000,))
    await env.engine._maybe_enqueue_topics(env.space, generation)
    jobs = await env.engine.jobs.list()
    assert len(jobs) == 1 and jobs[0]["kind"] == "topic_consolidate"
    env.adapter.output = '{"schema_version":"topic-consolidation.v1","changes":[]}'
    claimed = await env.engine.jobs.claim("worker", lease_seconds=60)
    assert (await execute(env, claimed))["state"] == "succeeded"
    await env.engine._maybe_enqueue_topics(env.space, generation)
    assert len(await env.engine.jobs.list()) == 1
    assert not await env.engine.jobs.list(visible_only=True)
    assert len(env.adapter.calls) == 1


@pytest.mark.asyncio
async def test_same_space_task_is_exclusive(environment):
    env = environment
    await seed(env)
    from storage.job_store import JobConflict
    await env.engine.enqueue("topic_consolidate", env.space["space_id"], "one")
    with pytest.raises(JobConflict):
        await env.engine.enqueue("topic_consolidate", env.space["space_id"], "two")
    with pytest.raises(JobConflict):
        await env.engine.enqueue("rebuild", env.space["space_id"], "three")


@pytest.mark.asyncio
async def test_context_vector_network_failure_is_retryable_before_model(environment, monkeypatch):
    env = environment
    await seed(env)

    async def fail(*args):
        raise ConnectionError("synthetic embedding disconnect")

    monkeypatch.setattr(type(env.engine), "_topic_context", fail)
    result = await execute(env, await enqueue_and_claim(env, "topic_consolidate"))
    assert result["state"] == "retry_wait"
    assert result["error_code"] == "context_embedding_temporary_failure"
    assert not env.adapter.calls


@pytest.mark.asyncio
async def test_duplicate_member_feedback_explains_constraint_and_recovers(environment):
    env = environment
    env.engine.config = replace(env.config, extraction_auto_regeneration=True)
    nodes = await seed(env)
    valid = candidate(nodes)
    invalid = json.loads(valid)
    invalid["changes"][0]["member_ids"] = [nodes[0]["node_id"]] * 2
    env.adapter.output = json.dumps(invalid)

    async def correct(messages):
        if len(env.adapter.calls) == 2:
            assert "member_ids must be unique" in messages[-1]["content"]
            env.adapter.output = valid

    env.adapter.on_generate = correct
    completed = await execute(env, await enqueue_and_claim(env, "topic_consolidate"))
    assert completed["state"] == "succeeded", completed
    assert len(env.adapter.calls) == 2


@pytest.mark.asyncio
async def test_cancel_after_topic_commit_resumes_receipt_without_model_repeat(environment, monkeypatch):
    import asyncio

    env = environment
    env.adapter.output = candidate(await seed(env))
    original_apply = TopicStore.apply

    async def commit_then_interrupt(self, *args, **kwargs):
        await original_apply(self, *args, **kwargs)
        raise asyncio.CancelledError()

    monkeypatch.setattr(TopicStore, "apply", commit_then_interrupt)
    job = await enqueue_and_claim(env, "topic_consolidate")
    with pytest.raises(asyncio.CancelledError):
        await execute(env, job)
    after_commit = await env.engine.jobs.get(job["job_id"])
    assert "topic_window" in after_commit["checkpoint"]
    assert len(env.adapter.calls) == 1
    monkeypatch.setattr(TopicStore, "apply", original_apply)
    resumed = await env.engine.jobs.claim("restart", lease_seconds=60)
    result = await execute(env, resumed)
    assert result["state"] == "succeeded", result
    assert result["result"]["reviewed_count"] == 2
    assert len(env.adapter.calls) == 1
    assert len(await env.memory.database.fetchall("SELECT * FROM topic_receipts")) == 1
    assert len(await env.memory.database.fetchall("SELECT * FROM memory_nodes WHERE node_type='topic'")) == 1


@pytest.mark.asyncio
async def test_new_accumulation_can_reuse_previously_reviewed_nodes(environment):
    env = environment
    old_nodes = await seed(env)
    env.adapter.output = '{"schema_version":"topic-consolidation.v1","changes":[]}'
    job = await enqueue_and_claim(env, "topic_consolidate")
    assert (await execute(env, job))["state"] == "succeeded"
    newest = await env.memory.create_node(space_id=env.space["space_id"], node_type="event",
        content_text="社区观星计划继续开展第三场独立活动。", source_ids=[env.records[1].source_id])
    env.adapter.output = candidate(old_nodes + [newest])
    second = await enqueue_and_claim(env, "topic_consolidate", key="second-accumulation")
    completed = await execute(env, second)
    assert completed["state"] == "succeeded", completed
    assert completed["result"]["reviewed_count"] == 3
    model_input = json.loads(env.adapter.calls[-1][-1]["content"])
    assert {node["node_id"] for node in model_input["nodes"]} >= {node["node_id"] for node in old_nodes}
    assert len(completed["result"]["topic_ids"]) == 1
    assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 0


@pytest.mark.asyncio
async def test_topic_job_fixed_cutoff_leaves_later_content_pending(environment):
    env = environment
    old_nodes = await seed(env)
    queued = await env.engine.enqueue("topic_consolidate", env.space["space_id"], "bounded-job")
    later = await env.memory.create_node(space_id=env.space["space_id"], node_type="event",
        content_text="稍后写入的独立活动。", source_ids=[env.records[1].source_id])
    assert later["updated_at"] > queued["payload"]["before"]
    env.adapter.output = candidate(old_nodes)
    claimed = await env.engine.jobs.claim("worker", lease_seconds=60)
    completed = await execute(env, claimed)
    assert completed["state"] == "succeeded", completed
    assert completed["result"]["reviewed_count"] == 2
    dirty = await TopicStore(env.memory).dirty_nodes(env.space["space_id"], later["generation_id"])
    assert [node["node_id"] for node in dirty] == [later["node_id"]]


@pytest.mark.asyncio
async def test_persona_change_during_topic_generation_requires_reprepare(environment):
    env = environment
    env.adapter.output = candidate(await seed(env))

    async def change_persona(messages):
        env.persona["text"] = "另一个当前人格定义"

    env.adapter.on_generate = change_persona
    job = await enqueue_and_claim(env, "topic_consolidate")
    failed = await execute(env, job)
    assert failed["state"] == "failed" and failed["error_code"] == "configuration_changed"
    assert not await env.memory.database.fetchall("SELECT * FROM topic_receipts")
    assert not await env.memory.database.fetchall("SELECT * FROM topic_reviews")


@pytest.mark.asyncio
async def test_provider_incomplete_output_cannot_commit_even_when_json_parses(environment, monkeypatch):
    env = environment
    nodes = await seed(env)

    async def truncated(messages, *, diagnostics=None, **kwargs):
        diagnostics["finish_reason"] = "length"
        return candidate(nodes)

    monkeypatch.setattr(env.adapter, "generate", truncated)
    job = await enqueue_and_claim(env, "topic_consolidate")
    failed = await execute(env, job)
    assert failed["state"] == "failed" and failed["error_code"] == "invalid_topic_output"
    assert not await env.memory.database.fetchall("SELECT * FROM topic_receipts")


@pytest.mark.asyncio
async def test_failed_later_topic_window_does_not_receive_fresh_automatic_retry_budget(environment):
    env = environment
    env.engine.config = replace(env.config, extraction_auto_regeneration=True)
    await seed(env, count=16)
    generation = await env.memory.get_active_generation(env.space["space_id"])

    async def respond(messages):
        env.adapter.output = ('{"schema_version":"topic-consolidation.v1","changes":[]}'
                              if len(env.adapter.calls) == 1 else "invalid json")

    env.adapter.on_generate = respond
    assert await env.engine._maybe_enqueue_topics(env.space, generation)
    claimed = await env.engine.jobs.claim("topic-worker", lease_seconds=60)
    failed = await execute(env, claimed)
    assert failed["state"] == "failed" and failed["checkpoint"]["done"] == 8
    assert failed["checkpoint"]["topic_window"]["corrections"] == 3
    assert len(env.adapter.calls) == 5
    assert await TopicStore(env.memory).count_dirty(env.space["space_id"], generation["generation_id"]) == 8
    assert not await env.engine._maybe_enqueue_topics(env.space, generation)
    assert len(await env.engine.jobs.list()) == 1
    status = await env.engine.automatic_status(env.space["space_id"])
    assert status["state"] == "enabled" and status["blocking_jobs"] == []
    assert [job["job_id"] for job in status["topic_blocking_jobs"]] == [failed["job_id"]]
    await env.engine.scan_once()
    jobs = await env.engine.jobs.list()
    extraction = next(job for job in jobs if job["kind"] == "extract")
    assert extraction["state"] == "queued"
    await env.engine.jobs.cancel(extraction["job_id"])
    await env.engine.delete(failed["job_id"])
    assert not (await env.engine.automatic_status(env.space["space_id"]))["topic_blocking_jobs"]
    assert await env.engine._maybe_enqueue_topics(env.space, generation)

@pytest.mark.asyncio
@pytest.mark.parametrize("window_expires", [True, False])
async def test_topic_timeout_keeps_call_diagnostics_and_retries_frozen_request(environment, window_expires):
    env = environment
    await seed(env)
    env.engine.config = replace(env.config, extraction_timeout_seconds=1)
    requests = []

    async def generate(messages, *, diagnostics, **options):
        requests.append(messages)
        if len(requests) == 1:
            diagnostics.update(queue_seconds=0.01, call_started=True)
            try:
                if window_expires:
                    await asyncio.Event().wait()
                raise TimeoutError()
            finally:
                diagnostics["call_seconds"] = 0.9
        return json.dumps({"schema_version": "topic-consolidation.v1", "changes": []})

    env.adapter.generate = generate
    job = await enqueue_and_claim(env, "topic_rebuild")
    failed = await execute(env, job)
    assert failed["state"] == "retry_wait"
    assert failed["error_code"] == ("topic_timeout" if window_expires else "provider_temporary_failure")
    detail = await env.engine.describe(job["job_id"])
    assert detail["response_diagnostics"] == {"queue_seconds": 0.01, "call_started": True, "call_seconds": 0.9}
    assert not detail["regenerate_window_available"]
    await env.memory.database.execute("UPDATE memory_jobs SET available_at=0 WHERE job_id=?", (job["job_id"],))
    completed = await execute(env, await env.engine.jobs.claim("resume", lease_seconds=60))
    assert completed["state"] == "succeeded", completed
    assert requests == [requests[0], requests[0]]


@pytest.mark.asyncio
async def test_topic_correction_and_commit_restore_window_user(environment):
    env = environment
    env.engine.config = replace(env.config, extraction_auto_regeneration=True)
    nodes = await seed(env)
    identity = env.space["logical_user_id"]
    original = await env.memory.update_node_content(nodes[0]["node_id"],
        content_text="{{user:" + identity + "}}继续准备望远镜。")
    nodes[0] = original
    calls = []

    async def respond(messages):
        calls.append(messages)
        prompt = json.loads(messages[1]["content"])
        assert identity not in messages[1]["content"]
        assert prompt["allowed_user_ids"] == ["u1"]
        assert any("{{user:u1}}" in node["content_text"] for node in prompt["nodes"])
        if len(calls) == 2:
            error = json.loads(messages[-1]["content"])["errors"][0]
            assert error["constraints"]["allowed_user_ids"] == ["u1"]
        alias = "u2" if len(calls) == 1 else "u1"
        ref = "{{user:" + alias + "}}"
        output = json.loads(candidate(nodes))
        output["changes"][0].update(title=ref + "的观星安排", content=ref + "持续准备观星。",
            emotion={"tags": [{"tag": "joy", "intensity": .5}], "description": ref + "期待晴夜。"})
        env.adapter.output = json.dumps(output)

    env.adapter.on_generate = respond
    result = await execute(env, await enqueue_and_claim(env, "topic_consolidate"))
    assert result["state"] == "succeeded", result
    assert len(calls) == 2
    node = await env.memory.get_node(result["result"]["topic_ids"][0])
    for text in [node["title"], node["content_text"], node["emotion"]["description"]]:
        assert "{{user:" + identity + "}}" in text


@pytest.mark.asyncio
async def test_single_eligible_seed_among_protected_neighbors_commits_empty_review(environment):
    env = environment
    first, *others = await seed(env, count=8)
    for n in others:
        await env.memory.set_manual_protection(n['node_id'], True)
    job = await enqueue_and_claim(env, "topic_consolidate")
    result = await execute(env, job)
    assert result['state'] == 'succeeded', result['error_code']
    assert result['result']['reviewed_count'] == 1 and not result['result']['topic_ids']
    assert not env.adapter.calls
    assert not await TopicStore(env.memory).count_dirty(first['space_id'], first['generation_id'])


@pytest.mark.asyncio
async def test_topic_corrects_protected_members_before_commit(environment):
    env = environment
    env.engine.config = replace(env.config, extraction_auto_regeneration=True)
    nodes = await seed(env, count=3)
    await env.memory.set_manual_protection(nodes[-1]['node_id'], True)
    env.adapter.output = candidate(nodes)

    async def correct(messages):
        if len(env.adapter.calls) == 2:
            issue, = json.loads(messages[-1]['content'])['errors']
            assert issue['remove_from_proposal'] == [nodes[-1]['node_id']]
            env.adapter.output = candidate(nodes[:2])

    env.adapter.on_generate = correct
    result = await execute(env, await enqueue_and_claim(env, 'topic_consolidate'))
    assert result['state'] == 'succeeded', result['error_code']
    assert len(env.adapter.calls) == 2 and len(result['result']['topic_ids']) == 1


@pytest.mark.asyncio
async def test_unlock_requeues_same_seed_after_automatic_success(environment):
    env = environment
    only, = await seed(env, count=1)
    generation = await env.memory.get_active_generation(env.space["space_id"])
    env.engine.config = replace(env.config, topic_min_changes=1)
    for step in range(3):
        if step:
            await env.memory.set_manual_protection(only["node_id"], True)
            await env.memory.set_manual_protection(only["node_id"], False)
        assert await env.engine._maybe_enqueue_topics(env.space, generation)
        assert not await env.engine._maybe_enqueue_topics(env.space, generation)
        result = await execute(env, await env.engine.jobs.claim("topic-test", lease_seconds=60))
        assert result["state"] == "succeeded" and result["result"]["reviewed_count"] == 1
        assert not await env.engine._maybe_enqueue_topics(env.space, generation)
    assert not env.adapter.calls
