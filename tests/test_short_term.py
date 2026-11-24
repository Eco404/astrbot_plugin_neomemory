"""Rolling continuity with real databases, model reuse, and generation fencing."""

import asyncio
import json
from types import SimpleNamespace

import pytest
import pytest_asyncio

from core.identity import EventIdentity
from core.jobs import JobRunner
from core.memory_spaces import private_space
from core.short_term import ShortTermService, conversation_key
from core.short_term_prompts import (
    HOST_SUMMARY_PREFIX,
    SCHEMA_VERSION,
    host_has_summary,
    parse_output,
)
from core.source_ingest import CapturedSource
from storage.job_store import JobConflict, JobStore
from storage.memory_store import MemoryStore
from storage.source_store import SourceStore


class Provider:
    def __init__(self):
        self.calls = []
        self.requests = []
        self.revision = "1"
        self.output = None
        self.hook = None

    def llm_fingerprint(self, provider_id, revision=""):
        return f"{provider_id}:{revision}:{self.revision}"

    async def generate(self, messages, **kwargs):
        self.calls.append(messages)
        self.requests.append(kwargs)
        if self.hook:
            await self.hook()
        if self.output is not None:
            return self.output
        prompt = json.loads(messages[-1]["content"])
        return json.dumps({"schema_version": SCHEMA_VERSION,
            "summary_text": "用户正在准备面试，尚未确定模拟面试时间。",
            "evidence_source_ids": [row["source_id"] for row in prompt["new_sources"]]})


async def add(env, start=1, count=8, *, conv="chat", stream="u", persona="p", text="我正在准备面试"):
    records = []
    for index in range(start, start + count):
        identity = EventIdentity("platform", "test", "FriendMessage", f"platform:{stream}", stream,
                                 "", "u", "user", "bot", f"{conv}:{persona}:{index}")
        record = await env.sources.capture(CapturedSource(identity=identity, actor_kind="user",
            content_text=text, observed_at=index * 100, actor_account_id="u", persona_id=persona,
            metadata={"host_conversation_id": conv} if conv else {}))
        records.append(record)
    return records


def restart(env):
    env.service = ShortTermService(env.sources, env.memory, env.jobs, env.adapter, env.config,
                                  lambda _: env.persona)
    env.runner = JobRunner(env.jobs, {"short_term": env.service.run})


@pytest_asyncio.fixture
async def env(tmp_path):
    sources = SourceStore(tmp_path / "sources.db")
    memory = MemoryStore(tmp_path / "memory.db", source_exists=sources.has_source)
    await sources.open()
    await memory.open()
    env = SimpleNamespace(sources=sources, memory=memory, jobs=JobStore(memory.database),
        adapter=Provider(), persona="认真温和的朋友", config=SimpleNamespace(
            enabled=True, llm_provider_id="fake", short_term_enabled=True,
            short_term_keep_messages=2, short_term_min_messages=3, short_term_max_messages=4,
            short_term_max_chars=1000, short_term_summary_chars=400, extraction_timeout_seconds=10,
            extraction_max_prompt_bytes=256000, job_max_attempts=2))
    env.records = await add(env)
    env.stream_id = env.records[0].stream_id
    env.space = await memory.get_or_create_space(private_space("p", env.records[0].logical_user_id))
    restart(env)
    try:
        yield env
    finally:
        await env.runner.stop()
        await memory.close()
        await sources.close()


async def claim(env):
    job = await env.service.maybe_enqueue(env.space["space_id"], "chat", stream_id=env.stream_id)
    assert job
    claimed = await env.jobs.claim("test", lease_seconds=60)
    assert claimed["job_id"] == job["job_id"]
    return claimed


async def execute(env, job):
    await env.runner._execute(job)
    return await env.jobs.get(job["job_id"])


async def current(env):
    return await env.service.get_current(env.space["space_id"], "chat", stream_id=env.stream_id)


@pytest.mark.asyncio
async def test_rolling_summary_reserves_latest_raw_and_only_adds_new_sources(env):
    job = await claim(env)
    assert not env.adapter.calls  # Admission never performs inference.
    assert job["payload"]["source_ids"] == [r.source_id for r in env.records[:4]]
    assert (await execute(env, job))["state"] == "succeeded"
    assert not await env.jobs.list(visible_only=True)
    state = await current(env)
    assert state["coverage"]["to_sequence"] == 4
    assert state["coverage"]["source_count"] == 4
    assert state["coverage"]["from_observed_at"] == 100
    assert state["coverage"]["to_observed_at"] == 400
    rules = env.adapter.calls[0][0]["content"]
    assert "用“我”写自己的回应和行动" in rules
    assert "分清谁有这个想法、谁明确答应" in rules
    assert "保留当次分工的范围" in rules
    assert await env.service.maybe_enqueue(env.space["space_id"], "chat", stream_id=env.stream_id) is None
    await add(env, start=9, count=2)
    next_job = await claim(env)
    assert next_job["payload"]["source_ids"] == [r.source_id for r in env.records[4:8]]
    assert (await execute(env, next_job))["state"] == "succeeded"
    prompt = json.loads(env.adapter.calls[-1][-1]["content"])
    assert prompt["previous_summary"] == state["summary_text"]
    assert len(prompt["new_sources"]) == 4
    updated = await current(env)
    assert updated["coverage"]["source_count"] == 8
    assert updated["coverage"]["from_observed_at"] == 100
    assert updated["coverage"]["to_observed_at"] == 800
    assert updated["content_version"] == 2
    refs = await env.memory.database.fetchall("SELECT source_id FROM short_term_sources WHERE state_id=?",
                                             (updated["state_id"],))
    assert len(refs) == 8
    assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 0


@pytest.mark.asyncio
async def test_explicit_unknown_conversation_and_cross_stream_never_mix(env):
    other = await add(env, count=8, conv="another")
    await add(env, count=8, stream="other")
    assert await env.service.maybe_enqueue(env.space["space_id"], None, stream_id=env.stream_id) is None
    assert await env.service.maybe_enqueue(env.space["space_id"], "chat") is None  # ambiguous stream
    job = await claim(env)
    assert not set(job["payload"]["source_ids"]) & {record.source_id for record in other}
    await execute(env, job)
    assert await env.service.get_current(env.space["space_id"], "another", stream_id=env.stream_id) is None
    assert conversation_key(env.stream_id, "chat") != conversation_key("other", "chat")


@pytest.mark.asyncio
async def test_generation_change_during_model_refuses_stale_commit(env):
    job = await claim(env)

    async def switch():
        generation = await env.memory.create_generation(env.space["space_id"])
        await env.memory.activate_generation(env.space["space_id"], generation["generation_id"])

    env.adapter.hook = switch
    result = await execute(env, job)
    assert result["state"] == "failed"
    assert result["error_code"] == "summary_context_changed"
    assert await current(env) is None
    assert await env.service.store.receipt(job["job_id"]) is None


@pytest.mark.asyncio
async def test_saved_model_output_survives_restart_without_reinference(env, monkeypatch):
    job = await claim(env)

    async def interrupted(*args, **kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(env.service.store, "apply", interrupted)
    with pytest.raises(asyncio.CancelledError):
        await execute(env, job)
    assert (await env.jobs.get(job["job_id"]))["checkpoint"]["raw"]
    restart(env)
    resumed = await env.jobs.claim("replacement")
    assert (await execute(env, resumed))["state"] == "succeeded"
    assert len(env.adapter.calls) == 1
    assert (await current(env))["coverage"]["source_count"] == 4


@pytest.mark.asyncio
async def test_commit_then_crash_replays_receipt_once(env, monkeypatch):
    job = await claim(env)
    original = env.service.store.apply

    async def interrupted(*args, **kwargs):
        await original(*args, **kwargs)
        raise asyncio.CancelledError()

    monkeypatch.setattr(env.service.store, "apply", interrupted)
    with pytest.raises(asyncio.CancelledError):
        await execute(env, job)
    restart(env)
    resumed = await env.jobs.claim("replacement")
    assert (await execute(env, resumed))["state"] == "succeeded"
    assert len(env.adapter.calls) == 1
    assert (await current(env))["content_version"] == 1


@pytest.mark.asyncio
async def test_failure_pauses_only_same_conversation_and_explicit_fresh_retries(env):
    job = await claim(env)
    env.adapter.output = "not valid JSON"
    failed = await execute(env, job)
    assert failed["state"] == "failed"
    assert failed["checkpoint"]["corrections"] == 3
    await add(env, start=9, count=6)
    assert await env.service.maybe_enqueue(env.space["space_id"], "chat", stream_id=env.stream_id) is None
    await add(env, count=8, conv="other")
    other = await env.service.maybe_enqueue(env.space["space_id"], "other", stream_id=env.stream_id)
    assert other and other["job_id"] != job["job_id"]
    await env.jobs.cancel(other["job_id"])
    env.adapter.output = None
    retry = await env.service.retry(job["job_id"], fresh=True)
    assert retry["checkpoint"] == {}
    assert (await env.service.retry(job["job_id"], fresh=True))["job_id"] == retry["job_id"]
    assert (await execute(env, await env.jobs.claim("retry")))["state"] == "succeeded"
    assert len(env.adapter.calls) == 5


@pytest.mark.asyncio
async def test_resume_rejects_changed_persona_then_fresh_reprepares(env, monkeypatch):
    job = await claim(env)

    async def interrupted(*args, **kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(env.service.store, "apply", interrupted)
    with pytest.raises(asyncio.CancelledError):
        await execute(env, job)
    env.persona = "人格发生了修改"
    restart(env)
    result = await execute(env, await env.jobs.claim("replacement"))
    assert result["state"] == "failed"
    assert result["error_code"] == "configuration_or_context_changed"
    assert len(env.adapter.calls) == 1
    await env.service.retry(job["job_id"], fresh=True)
    assert (await execute(env, await env.jobs.claim("fresh")))["state"] == "succeeded"
    assert len(env.adapter.calls) == 2


@pytest.mark.asyncio
async def test_cancel_during_model_fences_summary_write(env):
    job = await claim(env)

    async def cancel():
        await env.jobs.cancel(job["job_id"])

    env.adapter.hook = cancel
    assert (await execute(env, job))["state"] == "cancelled"
    assert await current(env) is None
    assert not await env.service.store.receipt(job["job_id"])


@pytest.mark.asyncio
async def test_scope_conflict_cannot_start_duplicate_task(env):
    first, second = await asyncio.gather(
        env.service.maybe_enqueue(env.space["space_id"], "chat", stream_id=env.stream_id),
        env.service.maybe_enqueue(env.space["space_id"], "chat", stream_id=env.stream_id),
    )
    assert first or second
    assert len(await env.jobs.list()) == 1
    with pytest.raises(JobConflict):
        await env.jobs.submit("rebuild", env.space["space_id"], {}, "parallel-rebuild")


@pytest.mark.asyncio
async def test_missing_source_refuses_to_skip_window(env):
    job = await claim(env)
    await env.sources.database.execute("DELETE FROM source_records WHERE source_id=?", (job["payload"]["source_ids"][1],))
    assert (await execute(env, job))["error_code"] == "source_range_changed"
    assert not env.adapter.calls
    assert await current(env) is None


@pytest.mark.asyncio
async def test_huge_source_is_reviewable_without_unbounded_model_call(env):
    env.config.short_term_max_chars = 1
    job = await claim(env)
    assert len(job["payload"]["source_ids"]) == 1
    assert (await execute(env, job))["error_code"] == "source_budget_exceeded"
    assert not env.adapter.calls
    assert await current(env) is None


@pytest.mark.asyncio
async def test_model_has_no_memory_transaction_and_new_messages_stay_for_later(env):
    job = await claim(env)

    async def during_model():
        assert not env.memory.database._lock.locked()
        await add(env, start=9, count=3)

    env.adapter.hook = during_model
    assert (await execute(env, job))["state"] == "succeeded"
    assert (await current(env))["coverage"]["to_sequence"] == 4


@pytest.mark.asyncio
async def test_host_summary_suppresses_generation_and_read(env):
    await execute(env, await claim(env))
    contexts = [{"role": "user", "content": HOST_SUMMARY_PREFIX + " previous discussion"}]
    assert await env.service.maybe_enqueue(env.space["space_id"], "chat", stream_id=env.stream_id,
                                           host_contexts=contexts) is None
    assert await env.service.get_current(env.space["space_id"], "chat", stream_id=env.stream_id,
                                         host_contexts=contexts) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("persona", ["default", "_chatui_default_"])
async def test_default_personas_skip_without_model_calls(env, persona):
    records = await add(env, persona=persona)
    space = await env.memory.get_or_create_space(private_space(persona, records[0].logical_user_id))
    assert await env.service.maybe_enqueue(space["space_id"], "chat", stream_id=env.stream_id) is None
    assert not env.adapter.calls


@pytest.mark.asyncio
async def test_empty_summary_still_advances_and_does_not_retry_noise(env):
    env.adapter.output = json.dumps({"schema_version": SCHEMA_VERSION, "summary_text": "", "evidence_source_ids": []})
    job = await claim(env)
    assert (await execute(env, job))["state"] == "succeeded"
    assert (await current(env))["summary_text"] == ""
    assert (await current(env))["coverage"]["to_sequence"] == 4
    assert await env.service.maybe_enqueue(env.space["space_id"], "chat", stream_id=env.stream_id) is None


@pytest.mark.asyncio
async def test_timeout_uses_job_retry_without_inline_provider_retry(env):
    job = await claim(env)

    async def timeout():
        raise TimeoutError("do not save api secret or provider response")

    env.adapter.hook = timeout
    result = await execute(env, job)
    assert result["state"] == "retry_wait"
    assert result["error_code"] == "provider_temporary_failure"
    assert len(env.adapter.calls) == 1
    assert "secret" not in json.dumps(result)


@pytest.mark.asyncio
async def test_http_503_uses_job_retry_without_inline_provider_retry(env):
    job = await claim(env)

    class HttpServiceUnavailable(Exception):
        code = 503

    async def unavailable():
        raise HttpServiceUnavailable("temporary provider load")

    env.adapter.hook = unavailable
    result = await execute(env, job)
    assert result["state"] == "retry_wait"
    assert result["error_code"] == "provider_temporary_failure"
    assert len(env.adapter.calls) == 1


@pytest.mark.asyncio
async def test_fresh_retry_trims_legacy_window_to_complete_source_budget(env):
    job = await claim(env)
    env.config.short_term_max_chars = 25
    failed = await execute(env, job)
    assert failed["state"] == "failed"
    assert failed["error_code"] == "source_budget_exceeded"

    fresh = await env.service.retry(job["job_id"], fresh=True)
    assert len(fresh["payload"]["source_ids"]) < len(job["payload"]["source_ids"])
    assert fresh["payload"]["source_ids"] == job["payload"]["source_ids"][:3]


@pytest.mark.parametrize("change", [
    {"schema_version": "wrong"}, {"summary_text": "x" * 401},
    {"evidence_source_ids": ["outside"]}, {"evidence_source_ids": ["s1", "s1"]},
    {"evidence_source_ids": [1]}, {"extra": True},
])
def test_prompt_schema_rejects_unreliable_output(change):
    output = {"schema_version": SCHEMA_VERSION, "summary_text": "pending interview", "evidence_source_ids": ["s1"]}
    output.update(change)
    with pytest.raises(ValueError):
        parse_output(json.dumps(output), source_ids=["s1"], summary_chars=400)


def test_schema_rejects_duplicate_json_keys_and_marker_detection_is_explicit():
    with pytest.raises(ValueError):
        parse_output('{"schema_version":"short-term.v1","schema_version":"short-term.v1"}',
                     source_ids=["s1"], summary_chars=400)
    assert host_has_summary([SimpleNamespace(role="user", content=[SimpleNamespace(text=HOST_SUMMARY_PREFIX + " hi")])])
    assert not host_has_summary([{"role": "assistant", "content": HOST_SUMMARY_PREFIX}])
    assert not host_has_summary([{"role": "user", "content": "Can you summarize our chat?"}])
    assert not host_has_summary(None)


@pytest.mark.asyncio
async def test_first_nonempty_summary_requires_evidence_but_inherited_summary_can_reuse_it(env):
    first = await claim(env)
    env.adapter.output = json.dumps({"schema_version": SCHEMA_VERSION,
        "summary_text": "我记得用户正在准备面试。", "evidence_source_ids": []})
    assert (await execute(env, first))["error_code"] == "invalid_short_term_output"
    assert await current(env) is None
    env.adapter.output = None
    await env.service.retry(first["job_id"], fresh=True)
    assert (await execute(env, await env.jobs.claim("fresh")))["state"] == "succeeded"
    previous = await current(env)
    await add(env, start=9, count=2)
    env.adapter.output = json.dumps({"schema_version": SCHEMA_VERSION,
        "summary_text": previous["summary_text"], "evidence_source_ids": []})
    assert (await execute(env, await claim(env)))["state"] == "succeeded"
    assert (await current(env))["summary_text"] == previous["summary_text"]
    assert (await current(env))["coverage"]["source_count"] == 8


@pytest.mark.asyncio
@pytest.mark.parametrize("persona", ["default", "_chatui_default_", "[%None]"])
async def test_legacy_default_summary_task_succeeds_with_skip_result(env, persona):
    space = await env.memory.get_or_create_space(private_space(persona, env.records[0].logical_user_id))
    queued = await env.jobs.submit("short_term", space["space_id"], {}, "legacy-default")
    result = await execute(env, await env.jobs.claim("legacy"))
    assert result["job_id"] == queued["job_id"]
    assert result["state"] == "succeeded"
    assert result["result"] == {"skipped": True, "reason": "unsupported_persona"}
    assert not env.adapter.calls


@pytest.mark.asyncio
async def test_cancelled_window_waits_for_explicit_retry_even_when_more_sources_arrive(env):
    env.config.short_term_max_messages = 10
    queued = await env.service.maybe_enqueue(env.space["space_id"], "chat", stream_id=env.stream_id)
    assert len(queued["payload"]["source_ids"]) == 6
    await env.jobs.cancel(queued["job_id"])
    await add(env, start=9, count=10)
    # More sources would form a different, overlapping 10-item payload. It must
    # not bypass the explicit cancellation of the previous 6-item window.
    assert await env.service.maybe_enqueue(env.space["space_id"], "chat", stream_id=env.stream_id) is None
    assert len(await env.jobs.list()) == 1
    await env.service.retry(queued["job_id"], fresh=True)
    assert (await execute(env, await env.jobs.claim("retry")))["state"] == "succeeded"
    following = await env.service.maybe_enqueue(env.space["space_id"], "chat", stream_id=env.stream_id)
    assert following
    assert following["payload"]["previous_version"] == 1
    assert not set(following["payload"]["source_ids"]) & set(queued["payload"]["source_ids"])


@pytest.mark.asyncio
async def test_chat_model_is_scoped_to_summary_prefix_and_frozen_across_retry(env):
    space = env.space["space_id"]
    await env.memory.set_space_policy(space, {"model_mode": "chat"})
    # Old and newer assistant replies are inside the summary. The latest
    # uncompressed messages belong to a future summary window.
    for i, provider in ((1, "first"), (3, "summarize-here"), (7, "reserved-tail")):
        route = {"provider_id": provider, "basis": "request_provider", "model_override": provider + "-model",
                 "reported_model": "response-alias"}
        await env.sources.database.execute(
            "UPDATE source_records SET actor_kind='agent',metadata_json=json_set(metadata_json,'$.chat_model',json(?))"
            " WHERE source_id=?", (json.dumps(route), env.records[i].source_id))
    job = await claim(env)
    assert job["payload"]["model_route"]["provider_id"] == "summarize-here"
    await env.memory.set_space_policy(space, {"model_mode": "fixed", "provider_id": "next"})
    env.adapter.output = "invalid"
    env.config.extraction_auto_regeneration = False
    assert (await execute(env, job))["state"] == "failed"
    assert env.adapter.requests[-1]["provider_id"] == "summarize-here"
    assert env.adapter.requests[-1]["model"] == "summarize-here-model"
    await env.service.retry(job["job_id"])
    assert (await execute(env, await env.jobs.claim("resume")))["state"] == "failed"
    assert len(env.adapter.requests) == 1  # Ordinary retry reused the saved response.

    preview = (await env.service.model_preview(job))["window"]
    assert preview["provider_id"] == "next" and preview["basis"] == "space"
    env.config.short_term_provider_id = "dedicated"
    assert (await env.service.model_preview(job))["window"]["basis"] == "short_term"
    fresh = await env.service.retry(job["job_id"], fresh=True, model_selection={"window_provider_id": "chosen"})
    assert fresh["payload"]["source_ids"] == job["payload"]["source_ids"]
    env.adapter.output = None
    assert (await execute(env, await env.jobs.claim("fresh")))["state"] == "succeeded"
    assert env.adapter.requests[-1]["provider_id"] == "chosen" and "model" not in env.adapter.requests[-1]


@pytest.mark.asyncio
async def test_missing_model_has_recoverable_short_term_window(env):
    env.config.llm_provider_id = ""
    job = await claim(env)
    assert (await execute(env, job))["error_code"] == "model_required"
    assert await env.service.maybe_enqueue(env.space["space_id"], "chat", stream_id=env.stream_id) is None
    assert (await env.service.model_preview(job))["window"]["missing_windows"] == 1
    with pytest.raises(ValueError, match="model_required"):
        await env.service.retry(job["job_id"], fresh=True)
    env.config.short_term_provider_id = "summary"
    fresh = await env.service.retry(job["job_id"], fresh=True)
    assert fresh["payload"]["model_route"] == {"provider_id": "summary", "basis": "short_term"}
    env.config.short_term_provider_id = "changed-after-prepare"
    assert (await execute(env, await env.jobs.claim("fresh")))["state"] == "succeeded"
    assert env.adapter.requests[-1]["provider_id"] == "summary"


@pytest.mark.asyncio
@pytest.mark.parametrize("configured", ["text", "json_object", "json_schema"])
async def test_output_mode_and_window_schema_apply_to_first_call_and_correction(env, configured):
    env.config.extraction_output_format = configured
    job = await claim(env)
    sources = job["payload"]["source_ids"]
    valid = json.dumps({"schema_version": SCHEMA_VERSION, "summary_text": "周末约定尚待确认。",
                        "evidence_source_ids": [sources[0]]})

    async def answer():
        env.adapter.output = valid + '"}' if len(env.adapter.requests) == 1 else valid
        env.config.extraction_output_format = "text"

    env.adapter.hook = answer
    assert (await execute(env, job))["state"] == "succeeded"
    assert len(env.adapter.requests) == 2
    for request in env.adapter.requests:
        assert request["output_format"] == configured
        if configured == "json_schema":
            schema = request["output_schema"]
            assert set(schema["required"]) == set(json.loads(valid))
            assert schema["properties"]["evidence_source_ids"]["items"]["enum"] == sources
        else:
            assert request["output_schema"] is None
    assert (await current(env))["content_version"] == 1


@pytest.mark.asyncio
async def test_automatic_reprepare_replans_the_eligible_short_term_prefix(env):
    job = await claim(env)
    env.config.short_term_max_chars = 25
    failed = await execute(env, job)
    env.jobs.clock = lambda: failed["available_at"]
    resume = await env.jobs.claim("resume")
    pending = await execute(env, resume)
    assert pending["recovery_mode"] == "fresh"
    env.runner.reprepare = env.service.auto_reprepare
    old = await execute(env, await env.jobs.claim("reprepare"))
    current = await env.jobs.claim("fresh")
    assert old["result"]["replacement_job_id"] == current["job_id"]
    assert current["payload"]["source_ids"] == job["payload"]["source_ids"][:3]
    assert current["automatic_origin"]
    assert (await execute(env, current))["state"] == "succeeded"
