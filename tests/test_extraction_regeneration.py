"""Explicit regeneration feeds back bounded validation errors without relaxing schema."""

import json
from dataclasses import replace

import pytest

from core.extraction_apply import digest
from core.extraction_references import ExtractionReferences, VERSION as REFERENCE_VERSION
from core.extraction_regeneration import VERSION, validation_feedback
from tests.test_maintenance_engine import environment, enqueue_and_claim, execute, restart, node, output


def invalid_time(env, content="Synthetic appointment with unknown occurrence time."):
    change = node(env.records[0].source_id, content=content)
    change["event_time"] = {"start": None, "end": None, "text": None}
    return json.dumps(output([change]))


async def failed_time(env):
    env.adapter.output = invalid_time(env)
    failed = await execute(env, await enqueue_and_claim(env))
    assert failed["error_code"] == "invalid_output"
    return failed


@pytest.mark.asyncio
async def test_manual_regeneration_supplies_unknown_time_rule_and_commits_once(environment):
    env = environment
    failed = await failed_time(env)
    original = failed["checkpoint"]["window"]
    regenerated = await env.engine.retry(failed["job_id"], regenerate_window=True)
    descriptor = regenerated["checkpoint"]["window"]["regeneration"]
    assert descriptor["version"] == VERSION
    archived = await env.engine.jobs.get_candidate(failed["job_id"], descriptor["candidate_id"])
    assert archived["window"] == original
    assert await env.engine.jobs.get_candidate("another-job", descriptor["candidate_id"]) is None

    async def respond(messages):
        assert messages[:2] == env.adapter.calls[0]
        assert len(messages) == 4
        assert messages[2] == {"role": "assistant", "content": original["raw"]}
        feedback = json.loads(messages[3]["content"])
        assert feedback["protocol"] == VERSION
        assert feedback["errors"] == [{"path": "$.node_changes[0].event_time", "type": "unknown_event_time",
            "constraints": {"const": None},
            "instruction": "事件发生时间未知时，event_time 必须为 JSON null；不得使用 start、end、text 全为 null 的对象，也不得补造时间。"}]
        candidate = json.loads(invalid_time(env))
        candidate["node_changes"][0]["event_time"] = None
        env.adapter.output = json.dumps(candidate)

    env.adapter.on_generate = respond
    completed = await execute(env, await env.engine.jobs.claim("regenerate"))
    assert completed["state"] == "succeeded"
    assert len(env.adapter.calls) == 2
    assert completed["result"]["source_count"] == 2
    assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 1
    assert len(await env.memory.list_nodes(space_id=env.space["space_id"])) == 1
    assert (await env.engine.jobs.get_candidate(failed["job_id"], descriptor["candidate_id"]))["window"] == original


@pytest.mark.asyncio
async def test_repeated_manual_regeneration_keeps_only_immediately_previous_draft(environment):
    env = environment
    first = await failed_time(env)
    await env.engine.retry(first["job_id"], regenerate_window=True)
    env.adapter.output = invalid_time(env, "Second independent synthetic draft.")
    second = await execute(env, await env.engine.jobs.claim("second"))
    assert second["state"] == "failed"
    assert len(env.adapter.calls) == 2
    await env.engine.retry(first["job_id"], regenerate_window=True)
    env.adapter.output = json.dumps(output())
    completed = await execute(env, await env.engine.jobs.claim("third"))
    assert completed["state"] == "succeeded"
    assert len(env.adapter.calls) == 3
    assert len(env.adapter.calls[1]) == len(env.adapter.calls[2]) == 4
    assert env.adapter.calls[2][2]["content"] == second["checkpoint"]["window"]["raw"]
    assert env.adapter.calls[2][2]["content"] != first["checkpoint"]["window"]["raw"]
    assert len(await env.engine.jobs.list_candidates(first["job_id"])) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("fault,code", [
    ("version", "regeneration_version_unsupported"),
    ("candidate", "regeneration_candidate_unavailable"),
    ("archive_context", "regeneration_context_changed"),
])
async def test_regeneration_binding_failures_stop_before_model(environment, fault, code):
    env = environment
    failed = await failed_time(env)
    regenerated = await env.engine.retry(failed["job_id"], regenerate_window=True)
    checkpoint = regenerated["checkpoint"]
    descriptor = checkpoint["window"]["regeneration"]
    if fault == "archive_context":
        archived = await env.engine.jobs.get_candidate(failed["job_id"], descriptor["candidate_id"])
        archived["window"]["input_digest"] = "another-input"
        await env.memory.database.execute("UPDATE memory_job_candidates SET window_json=? WHERE candidate_id=?",
            (json.dumps(archived["window"]), descriptor["candidate_id"]))
    else:
        descriptor["version" if fault == "version" else "candidate_id"] = "unsupported"
        await env.memory.database.execute("UPDATE memory_jobs SET checkpoint_json=? WHERE job_id=?",
            (json.dumps(checkpoint), failed["job_id"]))
    result = await execute(env, await env.engine.jobs.claim("binding-check"))
    assert result["state"] == "failed" and result["error_code"] == code
    assert len(env.adapter.calls) == 1
    assert len(await env.engine.jobs.list_candidates(failed["job_id"])) == 1


@pytest.mark.asyncio
async def test_regeneration_network_failure_uses_existing_durable_retry_only(environment):
    env = environment
    failed = await failed_time(env)
    await env.engine.retry(failed["job_id"], regenerate_window=True)

    async def unavailable(messages):
        raise TimeoutError("private transport response")

    env.adapter.on_generate = unavailable
    waiting = await execute(env, await env.engine.jobs.claim("network"))
    assert waiting["state"] == "retry_wait"
    assert waiting["error_code"] == "provider_temporary_failure"
    assert len(env.adapter.calls) == 2
    assert waiting["checkpoint"]["window"]["model_request_digest"] == digest(env.adapter.calls[1])
    assert "raw" not in waiting["checkpoint"]["window"]
    env.adapter.on_generate = None
    env.adapter.output = json.dumps(output())
    await env.memory.database.execute("UPDATE memory_jobs SET available_at=0 WHERE job_id=?", (failed["job_id"],))
    completed = await execute(env, await env.engine.jobs.claim("network-retry"))
    assert completed["state"] == "succeeded"
    assert len(env.adapter.calls) == 3
    assert env.adapter.calls[2] == env.adapter.calls[1]


@pytest.mark.asyncio
async def test_regeneration_budget_checks_entire_request_without_truncating(environment):
    env = environment
    prepared = await env.engine.extraction.prepare(space_id=env.space["space_id"],
        source_ids=tuple(record.source_id for record in env.records), persona=env.persona["text"],
        reference_protocol=REFERENCE_VERSION)
    base_messages = ExtractionReferences(prepared).messages()
    budget = sum(len(message["content"].encode()) for message in base_messages) + 10
    env.config = replace(env.config, extraction_max_prompt_bytes=budget)
    restart(env)
    failed = await failed_time(env)
    await env.engine.retry(failed["job_id"], regenerate_window=True)
    result = await execute(env, await env.engine.jobs.claim("budget"))
    assert result["state"] == "failed" and result["error_code"] == "prompt_budget_exceeded"
    assert len(env.adapter.calls) == 1
    history = await env.engine.jobs.list_candidates(failed["job_id"])
    assert history[0]["window"] == failed["checkpoint"]["window"]


def test_feedback_never_echoes_validation_exception_values_or_unknown_field_names():
    private_key = "private-field-name-sentinel"
    candidate = output()
    candidate[private_key] = "private-value-sentinel"
    archived = {"error_code": "invalid_output", "window": {"raw": json.dumps(candidate)}}
    feedback = validation_feedback(archived)
    encoded = json.dumps(feedback)
    assert "private-field-name-sentinel" not in encoded and "private-value-sentinel" not in encoded
    assert feedback["errors"][0]["type"] == "extra_forbidden"


@pytest.mark.parametrize("raw,hint", [
    ('{"items":[{"label":"synthetic","count":2]}', "括号"),
    ('{\n"合成" "private-value-sentinel"\n}', "冒号"),
    ('{"items":[]} private-value-sentinel', "额外内容"),
    ('{"items":', "缺少合法 JSON 值"),
])
def test_json_feedback_uses_parser_coordinates_not_stored_diagnostics(raw, hint):
    with pytest.raises(json.JSONDecodeError) as caught:
        json.loads(raw)
    archived = {"error_code": "invalid_output", "window": {"raw": raw,
        "validation_errors": [{"path": "private-field-sentinel", "pos": -1}]}}
    before = json.dumps(archived)
    feedback = validation_feedback(archived)
    error = feedback["errors"][0]
    assert (error["lineno"], error["colno"], error["pos"]) == (
        caught.value.lineno, caught.value.colno, caught.value.pos)
    assert hint in error["instruction"]
    assert "sentinel" not in json.dumps(feedback)
    assert json.dumps(archived) == before


def test_typed_candidate_feedback_does_not_reintroduce_program_versions():
    candidate = output([node("s1", old={"node_id": "n1", "content_version": 9})])
    archived = {"error_code": "expected_node_version_mismatch", "window": {"raw": json.dumps(candidate),
        "validation_errors": [{"path": "$.node_changes[0].expected_version", "type": "expected_node_version_mismatch"}]}}
    before = json.dumps(archived, sort_keys=True)
    feedback = validation_feedback(archived)
    assert feedback["errors"] == []
    assert json.dumps(archived, sort_keys=True) == before
