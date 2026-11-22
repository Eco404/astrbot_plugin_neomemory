"""Actual UTF-8 request size bounds evidence and intact membership expansion."""

import copy
import json
import time
from dataclasses import replace

import pytest

from core.jobs import ReviewRequired
from core.topic_budget import fit_topic_evidence, message_bytes
from core.topic_prompt import build_messages
from storage.topic_store import TopicStore
from tests.test_maintenance_engine import environment, enqueue_and_claim, execute


def evidence_rows(count=4):
    return [{"node_id": f"n{i}", "complete": False, "sources": [{
        "source_id": "shared-source-" + "界" * 120,
        "actor_kind": "user", "actor_logical_user_id": "u" + "人" * 100,
        "conversation_logical_user_id": "u" + "人" * 100,
        "observed_at": 123.0, "content_text": "我独自参加活动，Agent没有亲自参加。🌟" * 80,
        "basis_node_id": f"n{i}", "basis_version": 1, "truncated": False}]} for i in range(count)]


def test_evidence_uses_full_serialized_non_ascii_and_metadata_cost():
    evidence = evidence_rows()
    original = copy.deepcopy(evidence)
    nodes = [{"node_id": f"n{i}", "node_type": "event", "content_text": "独立经历。"} for i in range(4)]
    base = message_bytes(build_messages("温和人格", nodes, []))
    fitted, diagnostics = fit_topic_evidence("温和人格", nodes, [], evidence, base + 5000)
    assert fitted is not None
    assert diagnostics["source_excerpts_retained"] > 0
    assert diagnostics["source_excerpts_shortened"] + diagnostics["source_excerpts_omitted"] > 0
    assert message_bytes(build_messages("温和人格", nodes, [], evidence=fitted)) <= base + 5000
    assert evidence == original
    for row in fitted:
        assert row["complete"] is False and row["sources"]
        for source in row["sources"]:
            assert source["content_text"]
            assert source["source_id"] == original[0]["sources"][0]["source_id"]
            if source["content_text"] != original[0]["sources"][0]["content_text"]:
                assert source["truncated"]


def test_attribution_evidence_is_not_silently_replaced_with_empty_context():
    nodes = [{"node_id": "n0", "node_type": "event", "content_text": "独立经历。"}]
    base = message_bytes(build_messages("人格", nodes, []))
    fitted, diagnostics = fit_topic_evidence("人格", nodes, [], evidence_rows(1), base + 10)
    assert fitted is None and diagnostics["source_excerpts_retained"] == 0
    fitted, _ = fit_topic_evidence("人格", nodes, [], [], base)
    assert fitted == []


async def groups(env, count=8, members=4, length=1500):
    result = [[] for _ in range(count)]
    for member in range(members):
        for group in range(count):
            node = await env.memory.create_node(space_id=env.space["space_id"], node_type="event",
                content_text=(f"独立项目{group}场次{member}。" + "各自的事实条件与过程。" * 400)[:length],
                source_ids=[env.records[0].source_id])
            result[group].append(node)
    topics = []
    for group, items in enumerate(result):
        topic = await env.memory.create_node(space_id=env.space["space_id"], node_type="topic",
            content_text=(f"项目{group}的持续概况。" + "多项经历的长期关联。" * 100)[:1000],
            source_ids=[env.records[0].source_id])
        topics.append(topic)
        for item in items:
            await env.memory.add_edge(topic["node_id"], item["node_id"], "contains")
    return result, topics


@pytest.mark.asyncio
async def test_optional_large_topic_does_not_starve_a_smaller_neighbor(environment, monkeypatch):
    env = environment
    large, topics = await groups(env, count=1, members=5, length=2000)
    small, _ = await groups(env, count=1, members=2, length=50)
    seed = await env.memory.create_node(space_id=env.space["space_id"], node_type="event",
        content_text="独立待整理事件", source_ids=[env.records[0].source_id])
    seed_id = seed["node_id"]

    async def ranked(_space, _gen, seeds, _deadline):
        return [seed_id, large[0][0]["node_id"], small[0][0]["node_id"]]

    monkeypatch.setattr(env.engine, "_topic_context", ranked)
    env.engine.config = replace(env.config, extraction_max_prompt_bytes=24000)
    prepared, _, diagnostics = await env.engine._prepare_topic_window(
        TopicStore(env.memory), env.space, seed["generation_id"], [seed])
    visible = {node["node_id"] for node in prepared["nodes"]}
    assert prepared["seed_ids"] == [seed_id]
    assert {node["node_id"] for node in small[0]} <= visible
    assert topics[0]["node_id"] not in visible
    assert visible.isdisjoint(node["node_id"] for node in large[0])
    assert diagnostics["prompt_bytes"] + diagnostics["reserved_feedback_bytes"] <= 24000


@pytest.mark.asyncio
async def test_split_ranks_neighbors_for_retained_seeds(environment, monkeypatch):
    env = environment
    large, _ = await groups(env, count=1, members=5, length=2000)
    small, _ = await groups(env, count=1, members=2, length=50)
    first_seed = await env.memory.create_node(space_id=env.space["space_id"], node_type="event",
        content_text="待整理的简短事件", source_ids=[env.records[0].source_id])
    seeds = [first_seed, large[0][0]]

    async def ranked(_space, _gen, active_seeds, _deadline):
        ids = [node["node_id"] for node in active_seeds]
        if len(ids) == 1:
            ids.append(small[0][0]["node_id"])
        return ids

    monkeypatch.setattr(env.engine, "_topic_context", ranked)
    env.engine.config = replace(env.config, extraction_max_prompt_bytes=24000)
    prepared, _, _ = await env.engine._prepare_topic_window(
        TopicStore(env.memory), env.space, first_seed["generation_id"], seeds)
    assert prepared["seed_ids"] == [first_seed["node_id"]]
    visible = {node["node_id"] for node in prepared["nodes"]}
    assert {node["node_id"] for node in small[0]} <= visible
    assert visible.isdisjoint(node["node_id"] for node in large[0])


@pytest.mark.asyncio
async def test_prepare_reduces_optional_roots_before_seeds_and_keeps_complete_members(environment):
    env = environment
    rows, topics = await groups(env)
    env.engine.config = replace(env.config, extraction_max_prompt_bytes=60000)
    store = TopicStore(env.memory)
    prepared, persona, diagnostics = await env.engine._prepare_topic_window(
        store, env.space, topics[0]["generation_id"], rows[0][:1])
    assert diagnostics["seed_count"] == 1
    assert diagnostics["optional_roots_retained"] < 29
    assert diagnostics["source_excerpts_retained"] > 0
    assert diagnostics["prompt_bytes"] + diagnostics["reserved_feedback_bytes"] <= 60000
    node_ids = {node["node_id"] for node in prepared["nodes"]}
    for topic, members in zip(topics, rows, strict=True):
        if topic["node_id"] in node_ids:
            assert {member["node_id"] for member in members} <= node_ids
    assert message_bytes(build_messages(persona, prepared["nodes"], prepared["edges"],
                                        evidence=prepared["evidence"],
                                        allowed_user_ids=prepared["allowed_user_ids"])) == diagnostics["prompt_bytes"]


@pytest.mark.asyncio
async def test_real_pipeline_splits_seed_batch_without_consuming_or_truncating_other_seeds(environment):
    env = environment
    rows, topics = await groups(env, count=4, members=3, length=1500)
    env.engine.config = replace(env.config, extraction_max_prompt_bytes=36000)
    seen = []

    async def respond(messages):
        seen.append(messages)
        env.adapter.output = '{"schema_version":"topic-consolidation.v1","changes":[]}'

    env.adapter.on_generate = respond
    result = await execute(env, await enqueue_and_claim(env, "topic_consolidate"))
    assert result["state"] == "succeeded", result
    assert result["result"]["reviewed_count"] == 12
    assert len(seen) > 2
    for messages in seen:
        assert message_bytes(messages) <= 36000
        payload = json.loads(messages[1]["content"])
        assert payload["evidence"]
        ids = {node["node_id"] for node in payload["nodes"]}
        for topic, members in zip(topics, rows, strict=True):
            if topic["node_id"] in ids:
                assert {member["node_id"] for member in members} <= ids
    assert not await TopicStore(env.memory).dirty_nodes(env.space["space_id"], topics[0]["generation_id"])
    assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 0


@pytest.mark.asyncio
async def test_single_required_closure_overflow_stays_visible_without_model_calls(environment):
    env = environment
    await groups(env, count=1, members=4, length=3000)
    env.engine.config = replace(env.config, extraction_max_prompt_bytes=20000)
    failed = await execute(env, await enqueue_and_claim(env, "topic_consolidate"))
    assert failed["state"] == "failed" and failed["error_code"] == "prompt_budget_exceeded"
    assert env.adapter.calls == []
    assert failed["checkpoint"]["topic_budget"]["seed_count"] == 1
    assert failed["checkpoint"]["topic_budget"]["base_prompt_bytes"] > 20000
    assert not await env.memory.database.fetchall("SELECT * FROM topic_reviews")


@pytest.mark.asyncio
async def test_full_correction_request_is_rechecked_against_total_budget(environment):
    env = environment
    rows, topics = await groups(env, count=1, members=2, length=200)
    env.engine.config = replace(env.config, extraction_max_prompt_bytes=30000)
    job = await enqueue_and_claim(env, "topic_consolidate")
    store = TopicStore(env.memory)
    prepared, persona, diagnostics = await env.engine._prepare_topic_window(
        store, env.space, topics[0]["generation_id"], rows[0])
    from core.extraction_apply import digest
    route = job["payload"]["model_policy"]["default_provider_id"]
    _, signature = await env.engine._execution(env.space, {"provider_id": route})
    checkpoint = {"done": 0, "total": 2, "topic_window": {
        "receipt_key": "test-feedback", "prepared": prepared, "signature": signature,
        "prompt_version": env.engine.topic_prompt_version,
        "request_digest": digest(build_messages(persona, prepared["nodes"], prepared["edges"],
                                                evidence=prepared["evidence"],
                                                allowed_user_ids=prepared["allowed_user_ids"])),
        "errors": [{"type": "异常" * 30000}], "corrections": 1, "correction_limit": 3}}
    with pytest.raises(ReviewRequired) as error:
        await env.engine._topic_window(job, checkpoint, store, env.space, topics[0]["generation_id"])
    assert error.value.code == "prompt_budget_exceeded"
    assert not env.adapter.calls


def test_group_target_after_truncated_excerpt_is_not_retained_as_visible_evidence():
    source = {"source_id": "s1", "actor_kind": "user", "actor_logical_user_id": "maintainer",
              "group_message": True, "observed_at": 100.0, "content_text": "开始编排。" * 140 + "[@1] 校对完毕。",
              "group_addressing": [{"kind": "mention", "actor_kind": "user", "logical_user_id": "editor",
                                     "marker": "[@1]"}], "truncated": False}
    row = {"node_id": "n1", "complete": False, "sources": [source]}
    options = {"allowed_user_ids": ["maintainer", "editor"]}
    complete = build_messages("人格", [], [], evidence=[row], **options)
    budget = message_bytes(complete) - 1500
    evidence, diagnostics = fit_topic_evidence("人格", [], [], [row], budget, **options)
    assert diagnostics["source_excerpts_shortened"] == 1
    messages = build_messages("人格", [], [], evidence=evidence, **options)
    excerpt = json.loads(messages[1]["content"])["evidence"][0]["sources"][0]
    assert excerpt["truncated"] and excerpt["group_addressing"] == []
    assert "[@1]" not in excerpt["content_text"]
    assert message_bytes(messages) <= budget
    assert source["group_addressing"][0]["logical_user_id"] == "editor"


def test_forward_only_evidence_remains_attributed_when_budget_shortens_it():
    source = {"source_id": "s1", "actor_kind": "user", "actor_logical_user_id": "curator",
              "observed_at": 1, "content_text": "", "forwarded_messages": [
                  {"author": "投稿人", "text": "", "forwarded_messages": [
                      {"author": "领队", "text": "我在测量水位。" * 80}]}]}
    evidence = [{"node_id": "n1", "complete": False, "sources": [source]}]
    complete = build_messages("人格", [], [], evidence=evidence)
    limit = message_bytes(complete) - 400
    fitted, diagnostics = fit_topic_evidence("人格", [], [], evidence, limit)
    assert diagnostics["source_excerpts_available"] == diagnostics["source_excerpts_shortened"] == 1
    prompt = build_messages("人格", [], [], evidence=fitted)
    assert message_bytes(prompt) <= limit
    excerpt = json.loads(prompt[1]["content"])["evidence"][0]["sources"][0]
    assert excerpt["sender_comment"] == "" and excerpt["truncated"]
    quote = excerpt["forwarded_messages"][0]["forwarded_messages"][0]
    assert quote["author"] == "领队" and 0 < len(quote["text"]) < len("我在测量水位。" * 80)
    source["forwarded_messages"] = [{"content_available": False}]
    fitted, diagnostics = fit_topic_evidence("人格", [], [], evidence, limit)
    assert diagnostics["source_excerpts_retained"] == 1
    assert fitted[0]["sources"][0]["forwarded_messages"] == [{"content_available": False}]
