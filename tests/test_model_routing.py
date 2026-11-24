"""Exercise provider selection with real sources, job checkpoints and model calls."""

import json
from dataclasses import replace

import pytest

from core.identity import EventIdentity
from core.source_ingest import CapturedSource
from core.topic_schema import VERSION
from tests.test_maintenance_engine import environment, enqueue_and_claim, execute, restart


async def source(env, stream, number, *, route=None):
    identity = EventIdentity("platform", "test", "FriendMessage", stream, "u", "", "u",
                             "u", "bot", f"session-{number}")
    return await env.sources.capture(CapturedSource(
        identity=identity, actor_kind="agent" if route is not None else "user",
        actor_account_id="bot" if route is not None else "u", persona_id="p",
        content_text="A synthetic message", observed_at=number * 100.,
        metadata={"chat_model": route} if route is not None else {}))


def record_calls(env):
    calls = []
    generate = env.adapter.generate

    async def recorded(messages, **kwargs):
        calls.append((kwargs["provider_id"], kwargs.get("model")))
        return await generate(messages, **kwargs)

    env.adapter.generate = recorded
    return calls


@pytest.mark.asyncio
async def test_backlog_explains_minimum_per_stream(environment):
    env = environment
    env.engine.config = replace(env.config, private_min_messages=3)
    await source(env, "second", 3)
    backlog = await env.engine.extraction_backlog(env.space)
    assert (backlog.pending, backlog.largest_stream) == (3, 2)
    assert "单个会话最多 2 条" in backlog.hint and "还差 1 条" in backlog.hint
    assert (await env.engine.model_preview("extract", env.space))["waiting_message"] == backlog.hint


@pytest.mark.asyncio
async def test_mixed_stream_rebuild_follows_windows_and_falls_back_only_for_legacy(environment):
    env = environment
    restart(env, config=replace(env.config, llm_provider_id="", topic_consolidation_enabled=False))
    await source(env, "second", 3)
    await source(env, "second", 4, route={"provider_id": "chat-b", "basis": "request_provider",
                                        "model_override": "explicit", "reported_model": "reported"})
    await source(env, "third", 5)
    await source(env, "third", 6, route={"provider_id": "chat-c", "basis": "session_config",
                                        "requested_model": "config-name", "reported_model": "reported"})
    await source(env, "third", 7)
    preview = await env.engine.model_preview("rebuild", env.space)
    assert preview["window"]["missing_windows"] == 1
    assert preview["window"]["providers"] == ["chat-b", "chat-c"]
    with pytest.raises(ValueError, match="model_required"):
        await env.engine.enqueue("rebuild", env.space["space_id"], "unresolved")
    calls = record_calls(env)
    job = await enqueue_and_claim(env, "rebuild", model_selection={"missing_window_provider_id": "fallback"})
    await env.memory.set_space_policy(env.space["space_id"], {"model_mode": "fixed", "provider_id": "changed"})
    result = await execute(env, job)
    assert result["state"] == "succeeded", result
    assert calls == [("fallback", None), ("chat-b", "explicit"), ("chat-c", None)]
    assert await env.engine.pending_source_count(env.space) == 1


@pytest.mark.asyncio
async def test_missing_automatic_route_pauses_and_fresh_selection_recovers(environment):
    env = environment
    restart(env, config=replace(env.config, llm_provider_id=""))
    await env.engine.scan_once()
    job = await env.engine.jobs.claim("scan-worker")
    result = await execute(env, job)
    assert (result["state"], result["error_code"]) == ("failed", "model_required")
    assert await env.engine.pending_source_count(env.space) == 2
    assert not env.adapter.calls
    assert (await env.engine.describe(job["job_id"]))["restart_required"]
    await env.engine.retry(job["job_id"], fresh=True, model_selection={"window_provider_id": "chosen"})
    calls = record_calls(env)
    retry = await env.engine.jobs.claim("next-worker")
    assert (await execute(env, retry))["state"] == "succeeded"
    assert calls == [("chosen", None)]


@pytest.mark.asyncio
async def test_live_streams_keep_unconsumed_tail_and_models_separate(environment):
    env = environment
    await env.memory.set_space_policy(env.space["space_id"], {"model_mode": "chat"})
    b1 = await source(env, "stream-b", 3)
    job = await enqueue_and_claim(env, key="a", model_selection={"window_provider_id": "a"})
    assert job["payload"]["source_ids"] == [row.source_id for row in env.records]
    assert (await execute(env, job))["state"] == "succeeded"
    assert await env.engine.pending_source_count(env.space) == 1
    b2 = await source(env, "stream-b", 4, route={"provider_id": "b", "basis": "session_config"})
    job = await enqueue_and_claim(env, key="b")
    assert job["payload"]["source_ids"] == [b1.source_id, b2.source_id]
    assert job["payload"]["model_route"] == {"provider_id": "b", "basis": "session_config"}
    assert (await execute(env, job))["state"] == "succeeded"
    assert await env.engine.pending_source_count(env.space) == 0


@pytest.mark.asyncio
async def test_topic_requires_unambiguous_member_routes(environment):
    env = environment
    await env.memory.set_space_policy(env.space["space_id"], {"model_mode": "chat", "topic_follow_main": True})
    for number, provider in enumerate(("a", "b"), 1):
        await source(env, provider, 10 * number)
        message = await source(env, provider, 10 * number + 1, route={"provider_id": provider, "basis": "request_provider"})
        await env.memory.create_node(space_id=env.space["space_id"], node_type="event",
            content_text="A synthetic event", source_ids=[message.source_id])
    preview = (await env.engine.model_preview("topic_rebuild", env.space))["topic"]
    assert preview["mode"] == "missing" and preview["providers"] == ["a", "b"]
    await env.memory.set_space_policy(env.space["space_id"], {"topic_provider_id": "synthesis"})
    assert (await env.engine.model_preview("topic_rebuild", env.space))["topic"]["provider_id"] == "synthesis"


@pytest.mark.asyncio
@pytest.mark.parametrize("space_policy", [
    {"model_mode": "chat", "topic_follow_main": True},
    {"model_mode": "fixed", "provider_id": "different-space-main"},
])
async def test_topic_stage_uses_the_confirmed_model_of_its_members(environment, space_policy):
    env = environment
    restart(env, config=replace(env.config, llm_provider_id=""))
    await env.memory.set_space_policy(env.space["space_id"], space_policy)
    await source(env, "stage", 10)
    reply = await source(env, "stage", 11, route={"provider_id": "stage-chat", "basis": "request_provider",
                                                "model_override": "stage-override"})
    for text in ("I visited a planetarium", "I booked another visit"):
        await env.memory.create_node(space_id=env.space["space_id"], node_type="event",
                                     content_text=text, source_ids=[reply.source_id])
    env.adapter.output = json.dumps({"schema_version": VERSION, "changes": []})
    calls = record_calls(env)
    result = await execute(env, await enqueue_and_claim(env, "topic_rebuild"))
    assert result["state"] == "succeeded", result
    assert calls == [("stage-chat", "stage-override")]


@pytest.mark.asyncio
async def test_specialist_models_and_rebuild_steps(environment):
    env = environment
    restart(env, config=replace(env.config, topic_provider_id="global-topic", topic_consolidation_enabled=True))
    for kind, include_topics, lanes in (
        ("topic_rebuild", False, {"topic"}), ("relationship_rebuild", False, {"window"}),
        ("context_rebuild", True, {"topic", "window"}), ("rebuild", False, {"topic", "window"}),
    ):
        preview = await env.engine.model_preview(kind, env.space, include_topics=include_topics)
        assert set(preview) == lanes
        if "topic" in lanes:
            assert preview["topic"]["provider_id"] == "global-topic" and not preview["topic"]["follows_main"]

    space_id = env.space["space_id"]
    await env.memory.set_space_policy(space_id, {"model_mode": "fixed", "provider_id": "space-main"})
    preview = await env.engine.model_preview("rebuild", env.space)
    assert preview["window"]["provider_id"] == "space-main"
    assert not preview["topic"]["follows_main"] and preview["topic"]["provider_id"] == "global-topic"
    restart(env, config=replace(env.config, topic_provider_id=""))
    preview = await env.engine.model_preview("rebuild", env.space, selection={"window_provider_id": "manual-main"})
    assert not preview["topic"]["follows_main"] and preview["topic"]["provider_id"] == env.config.llm_provider_id

    await env.memory.set_space_policy(space_id, {"topic_follow_main": True})
    preview = await env.engine.model_preview("rebuild", env.space)
    assert preview["topic"]["follows_main"] and preview["topic"]["provider_id"] == "space-main"
    changed = await env.engine.model_preview("rebuild", env.space, selection={"window_provider_id": "manual-main"})
    assert changed["topic"]["provider_id"] == changed["window"]["provider_id"] == "manual-main"

    await env.memory.set_space_policy(space_id, {"topic_provider_id": "space-topic"})
    for text in ("I visited a planetarium", "I booked another visit"):
        await env.memory.create_node(space_id=space_id, node_type="event", content_text=text,
                                     source_ids=[env.records[0].source_id])
    job = await enqueue_and_claim(env, "topic_rebuild")
    await env.memory.set_space_policy(space_id, {"topic_provider_id": "changed-after-queue"})
    calls = record_calls(env)
    env.adapter.output = json.dumps({"schema_version": VERSION, "changes": []})
    assert (await execute(env, job))["state"] == "succeeded"
    assert calls == [("space-topic", None)]


@pytest.mark.asyncio
async def test_existing_multi_stream_replay_resumes_its_original_cursor(environment):
    env = environment
    await source(env, "alternate", 3)
    await source(env, "alternate", 4)
    job = await enqueue_and_claim(env, "rebuild")
    job["payload"].pop("model_policy")

    async def original_order_only(*args, **kwargs):
        pytest.fail("existing replay has no per-stream offsets")

    env.engine.replay_plan = original_order_only
    result = await execute(env, job)
    assert result["state"] == "succeeded", result
    assert result["result"]["source_count"] == 4
