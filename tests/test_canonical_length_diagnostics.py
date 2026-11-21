"""Stable-reference expansion keeps canonical limits and actionable diagnostics."""

import json

import pytest
from core.text_lengths import readable_text_length

from core.extraction_regeneration import validation_feedback
from core.extraction_schema import parse_extraction_output
from tests.test_extraction_apply import profile
from tests.test_maintenance_engine import enqueue_and_claim, execute, node, output
from tests.test_maintenance_engine import environment
from tests.test_maintenance_json_diagnostics import respond_with_raw

FIELDS = [
    ("node", 3000, ["node_changes", 0, "content"]),
    ("profile", 600, ["profile_changes", 0, "description"]),
    ("long_term", 500, ["relationship_change", "long_term", "body"]),
    ("aftereffect", 150, ["relationship_change", "aftereffect", "body"]),
]
SHORT_TEXT = "{{user:u1}}只在安静时练琴，不在夜间打扰别人。"


def candidate(kind, text):
    if kind == "node":
        value = output([node("s1", content=text)])
    elif kind == "profile":
        value = output(profiles=[profile("s1", "u1", description=text)])
    else:
        relation = {"logical_user_id": "u1", "expected_version": 0, "source_ids": ["s1"],
                    "long_term": {"op": "keep", "body": ""}, "aftereffect": {"op": "keep", "body": ""}}
        relation[kind] = {"op": "replace", "body": text}
        value = output(relationship=relation)
    return value


def at_path(value, path):
    for part in path:
        value = value[part]
    return value


def bounded_candidate(env, kind, readable_length):
    stable_ref = "{{user:" + env.space["logical_user_id"] + "}}"
    text = SHORT_TEXT + "甲" * (readable_length - readable_text_length(SHORT_TEXT))
    return json.dumps(candidate(kind, text), ensure_ascii=False), text.replace("{{user:u1}}", stable_ref)


async def stored_text(env, kind):
    if kind == "node":
        return (await env.memory.list_nodes(space_id=env.space["space_id"]))[0]["content_text"]
    if kind == "profile":
        return (await env.memory.list_profiles(space_id=env.space["space_id"]))[0]["description"]
    return (await env.memory.get_relationship(space_id=env.space["space_id"],
        logical_user_id=env.space["logical_user_id"]))["state"][kind]["body"]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,limit,path", FIELDS)
@pytest.mark.parametrize("overage", [0, 1], ids=["exact-canonical-limit", "one-over-canonical-limit"])
async def test_canonical_length_boundary_never_truncates_or_changes_limit(environment, kind, limit, path, overage):
    env = environment
    raw, restored = bounded_candidate(env, kind, limit + overage)
    assert readable_text_length(at_path(json.loads(raw), path)) == limit + overage
    assert readable_text_length(restored) == limit + overage
    assert len(restored) > limit
    respond_with_raw(env, raw)
    result = await execute(env, await enqueue_and_claim(env))
    if not overage:
        assert result["state"] == "succeeded"
        assert await stored_text(env, kind) == restored
        assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 1
    else:
        assert result["state"] == "failed" and result["error_code"] == "invalid_output"
        window = result["checkpoint"]["window"]
        assert window["raw"] == raw
        assert "canonical_raw" not in window
        assert readable_text_length(at_path(json.loads(window["raw"]), path)) == limit + 1
        assert window["validation_errors"] == [{"path": path, "type": "readable_too_long"}]
        detail = await env.engine.describe(result["job_id"])
        assert detail["validation_errors"] == window["validation_errors"]
        assert detail["candidate_format_invalid"] and detail["regenerate_window_available"]
        assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 0
        for table in ("memory_nodes", "user_profiles", "relationship_states", "extraction_batches", "memory_derivations"):
            assert (await env.memory.database.fetchone(f"SELECT count(*) FROM {table}"))[0] == 0
        assert await env.engine.jobs.claim("no-automatic-retry") is None
    assert len(env.adapter.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,limit,path", FIELDS)
async def test_explicit_regeneration_uses_saved_canonical_field_feedback(environment, kind, limit, path):
    env = environment
    raw, restored = bounded_candidate(env, kind, limit + 1)
    respond_with_raw(env, raw)
    failed = await execute(env, await enqueue_and_claim(env))
    assert failed["error_code"] == "invalid_output"
    previous = failed["checkpoint"]["window"]
    regenerated = await env.engine.retry(failed["job_id"], regenerate_window=True)
    archive = await env.engine.jobs.get_candidate(failed["job_id"],
        regenerated["checkpoint"]["window"]["regeneration"]["candidate_id"])
    assert archive["window"] == previous
    feedback = validation_feedback(archive, schema_version="extraction.v1")
    expected_path = "$" + "".join(f"[{part}]" if isinstance(part, int) else f".{part}" for part in path)
    assert len(feedback["errors"]) == 1
    diagnostic = feedback["errors"][0]
    assert diagnostic["path"] == expected_path and diagnostic["type"] == "readable_too_long"
    for concept in ("可读字符", "完整用户引用", "主体", "否定", "条件"):
        assert concept in diagnostic["instruction"]
    encoded_feedback = json.dumps(feedback, ensure_ascii=False)
    assert restored not in encoded_feedback and env.space["logical_user_id"] not in encoded_feedback
    assert "canonical_raw" not in regenerated["checkpoint"]["window"]
    respond_with_raw(env, json.dumps(candidate(kind, SHORT_TEXT), ensure_ascii=False))
    completed = await execute(env, await env.engine.jobs.claim("explicit-regenerate"))
    assert completed["state"] == "succeeded" and len(env.adapter.calls) == 2
    assert env.adapter.calls[1][2]["content"] == raw
    assert json.loads(env.adapter.calls[1][3]["content"]) == feedback
    assert "只在安静时" in await stored_text(env, kind)
    assert "不在夜间" in await stored_text(env, kind)
    assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 1


@pytest.mark.asyncio
async def test_single_user_placeholder_counts_as_one_tag_character_after_expansion(environment):
    env = environment
    value = candidate("profile", "独立合成画像。")
    value["profile_changes"][0]["tags"] = ["{{user:u1}}"]
    assert len(value["profile_changes"][0]["tags"][0]) == 11
    raw = json.dumps(value, ensure_ascii=False)
    parse_extraction_output(raw)
    stable_ref = "{{user:" + env.space["logical_user_id"] + "}}"
    assert len(stable_ref) > 24
    respond_with_raw(env, raw)
    completed = await execute(env, await enqueue_and_claim(env))
    assert completed["state"] == "succeeded"
    profile, = await env.memory.list_profiles(space_id=env.space["space_id"])
    assert profile["tags"] == [stable_ref]
    assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 1


@pytest.mark.asyncio
async def test_checkpoint_retry_fills_missing_canonical_diagnostics_without_model_call(environment):
    env = environment
    raw, _ = bounded_candidate(env, "node", 3001)
    respond_with_raw(env, raw)
    failed = await execute(env, await enqueue_and_claim(env))
    checkpoint = failed["checkpoint"]
    assert "canonical_raw" not in checkpoint["window"]
    checkpoint["window"].pop("validation_errors")
    await env.memory.database.execute(
        "UPDATE memory_jobs SET checkpoint_json=?,error_code='context_or_evidence_invalid' WHERE job_id=?",
        (json.dumps(checkpoint), failed["job_id"]))
    await env.engine.retry(failed["job_id"])
    rechecked = await execute(env, await env.engine.jobs.claim("checkpoint-recheck"))
    assert rechecked["state"] == "failed" and rechecked["error_code"] == "invalid_output"
    assert rechecked["checkpoint"]["window"]["validation_errors"] == [
        {"path": ["node_changes", 0, "content"], "type": "readable_too_long"}]
    assert rechecked["checkpoint"]["window"]["raw"] == raw
    assert "canonical_raw" not in rechecked["checkpoint"]["window"]
    assert len(env.adapter.calls) == 1
    assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 0
