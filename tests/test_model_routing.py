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
    await env.memory.set_space_policy(env.space["space_id"], {"model_mode": "chat"})
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
async def test_topic_stage_uses_the_confirmed_model_of_its_members(environment):
    env = environment
    await env.memory.set_space_policy(env.space["space_id"], {"model_mode": "chat"})
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
