"""Malformed synthetic candidates retain bounded diagnostics for manual review."""

import json

import pytest

from core.extraction_regeneration import validation_feedback
from tests.test_maintenance_engine import enqueue_and_claim, execute, restart, output
from tests.test_maintenance_engine import environment as environment


def respond_with_raw(env, raw):
    async def generate(messages, **kwargs):
        env.adapter.calls.append(messages)
        return raw

    env.adapter.generate = generate


@pytest.mark.asyncio
@pytest.mark.parametrize("raw,position", [
    ('{\n "私密" "private-text-sentinel"\n}', {"lineno": 2, "colno": 7, "pos": 8}),
    ('{"x":', {"lineno": 1, "colno": 6, "pos": 5}),
    ('{} private-text-sentinel', {"lineno": 1, "colno": 4, "pos": 3}),
    ('```json\n{}\n```', {"lineno": 1, "colno": 1, "pos": 0}),
    ('{"schema_version":"extraction.v2","private-key-sentinel":1,"private-key-sentinel":2}', {}),
    ('{"schema_version":"extraction.v2","node_changes":[{"private-key-sentinel":1,"private-key-sentinel":2}]}', {}),
    ('{"schema_version":"extraction.v2","private-key-sentinel":NaN}', {}),
    ('{"schema_version":"extraction.v2","private-key-sentinel":Infinity}', {}),
    ('{"schema_version":"extraction.v2","private-key-sentinel":-Infinity}', {}),
    ('[]', {}),
    ('{"schema_version":"unsupported-sentinel"}', {}),
], ids=["missing-colon-unicode", "truncated", "trailing-text", "markdown",
    "duplicate-key", "nested-duplicate-key", "nan", "positive-infinity", "negative-infinity",
    "wrong-envelope", "unsupported-schema"])
async def test_invalid_json_persists_numeric_location_without_content_or_retry(environment, raw, position):
    env = environment
    respond_with_raw(env, raw)
    failed = await execute(env, await enqueue_and_claim(env))
    assert failed["state"] == "failed"
    assert failed["error_type"] == "ReviewRequired"
    assert failed["error_code"] == "invalid_output"
    assert failed["attempt"] == 1 and len(env.adapter.calls) == 1
    window = failed["checkpoint"]["window"]
    expected = [{"path": "$", "type": "duplicate_json_key" if raw.count("private-key-sentinel") > 1 else "json_invalid", **position}]
    assert window["validation_errors"] == expected
    assert window["raw"] == raw and "canonical_raw" not in window
    assert "sentinel" not in json.dumps(expected)
    assert all(type(value) is int for key, value in expected[0].items() if key in position)
    assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 0
    assert (await env.memory.database.fetchone("SELECT count(*) FROM extraction_batches"))[0] == 0
    restart(env)
    detail = await env.engine.describe(failed["job_id"])
    assert detail["validation_errors"] == expected
    assert detail["candidate"] is None and detail["candidate_format_invalid"]
    assert detail["regenerate_window_available"]
    assert (await env.engine.automatic_status(env.space["space_id"]))["state"] == "paused"
    assert await env.engine.jobs.claim("no-automatic-retry") is None
    assert len(env.adapter.calls) == 1


@pytest.mark.asyncio
async def test_schema_validation_retains_existing_field_diagnostics(environment):
    env = environment
    candidate = output()
    candidate["node_changes"] = "private-text-sentinel"
    respond_with_raw(env, json.dumps(candidate))
    failed = await execute(env, await enqueue_and_claim(env))
    assert failed["state"] == "failed" and failed["error_code"] == "invalid_output"
    assert failed["checkpoint"]["window"]["validation_errors"] == [{"path": ["node_changes"], "type": "list_type"}]
    assert len(env.adapter.calls) == 1


@pytest.mark.asyncio
async def test_explicit_regeneration_sends_parser_location_and_archives_candidate(environment):
    env = environment
    raw = '{\n "私密" "private-text-sentinel"\n}'
    respond_with_raw(env, raw)
    failed = await execute(env, await enqueue_and_claim(env))
    expected = [{"path": "$", "type": "json_invalid", "lineno": 2, "colno": 7, "pos": 8}]
    assert failed["checkpoint"]["window"]["validation_errors"] == expected
    feedback_before = validation_feedback({"window": {"raw": raw}, "error_code": "invalid_output"})
    assert {key: feedback_before["errors"][0][key] for key in expected[0]} == expected[0]
    assert "冒号" in feedback_before["errors"][0]["instruction"]
    assert "sentinel" not in json.dumps(feedback_before)
    regenerated = await env.engine.retry(failed["job_id"], regenerate_window=True)
    window = regenerated["checkpoint"]["window"]
    assert "validation_errors" not in window and "raw" not in window
    archived = await env.engine.jobs.get_candidate(failed["job_id"], window["regeneration"]["candidate_id"])
    assert archived["window"]["raw"] == raw
    assert archived["window"]["validation_errors"] == expected
    respond_with_raw(env, json.dumps(output()))
    completed = await execute(env, await env.engine.jobs.claim("explicit-regenerate"))
    assert completed["state"] == "succeeded"
    assert completed["checkpoint"] == {}
    assert len(env.adapter.calls) == 2
    assert env.adapter.calls[1][2] == {"role": "assistant", "content": raw}
    assert json.loads(env.adapter.calls[1][3]["content"]) == feedback_before
    assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 1
