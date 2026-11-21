"""Schema-derived feedback stays exact without reflecting arbitrary model values."""

import json

import pytest

from core.extraction_regeneration import validation_feedback
from tests.test_maintenance_engine import node, output
from tests.test_maintenance_engine import environment as environment
from tests.test_maintenance_engine import enqueue_and_claim, execute


def draft():
    change = {**node("s1"), "title": "独立设备维修", "emotion": None}
    return output([change])


def feedback(candidate, **kwargs):
    return validation_feedback({"error_code": "invalid_output", "window": {
        "raw": json.dumps(candidate, ensure_ascii=False)}}, **kwargs)


@pytest.mark.parametrize("kind", ["enum", "count", "pattern", "readable"])
def test_precise_constraints_come_from_schema(kind):
    candidate = draft()
    change = candidate["node_changes"][0]
    if kind in {"enum", "count"}:
        tags = ["private-label-sentinel"] if kind == "enum" else ["joy", "trust", "concern", "affection"]
        change["emotion"] = {"tags": [{"tag": tag, "intensity": .4} for tag in tags], "description": "我感到轻松。"}
    elif kind == "pattern":
        change["local_ref"] = "local_非法名称"
    else:
        change["node_type"] = "atomic"
        change["content"] = "字" * 801
    value = feedback(candidate)
    error = value["errors"][0]
    if kind == "enum":
        assert error["path"] == "$.node_changes[0].emotion.tags[0].tag"
        assert "joy" in error["constraints"]["enum"]
        assert "sentinel" not in json.dumps(value)
    elif kind == "count":
        assert error["constraints"]["maxItems"] == 3
    elif kind == "pattern":
        assert error["constraints"]["pattern"] == r"^local_[A-Za-z0-9_-]+$"
        assert "非法名称" not in json.dumps(value, ensure_ascii=False)
    else:
        assert error["type"] == "readable_too_long"
        assert error["path"] == "$.node_changes[0].content"
        assert error["constraints"]["x-readable-maxLength"] == 800


def test_missing_basis_and_invalid_local_ref_receive_separate_instructions():
    candidate = draft()
    candidate["node_changes"][0].pop("base_node_ids")
    candidate["node_changes"][0]["local_ref"] = "local_非法名称"
    errors = feedback(candidate)["errors"]
    assert {(issue["path"], issue["type"]) for issue in errors} == {
        ("$.node_changes[0].base_node_ids", "missing"),
        ("$.node_changes[0].local_ref", "string_pattern_mismatch"),
    }


def test_feedback_is_bounded_without_modifying_candidate():
    candidate = draft()
    candidate["node_changes"] = [{**candidate["node_changes"][0], "local_ref": "local_非法"} for _ in range(20)]
    before = json.dumps(candidate)
    assert len(feedback(candidate)["errors"]) == 12
    assert json.dumps(candidate) == before


@pytest.mark.asyncio
async def test_canonical_capacity_is_distinct_from_field_length_and_context_failure(environment):
    env = environment
    candidate = draft()
    candidate["node_changes"][0]["content"] = "{{user:u1}}" * 3000
    env.adapter.output = json.dumps(candidate)
    failed = await execute(env, await enqueue_and_claim(env))
    assert failed["error_code"] == "invalid_output"
    window = failed["checkpoint"]["window"]
    assert len(window["raw"]) < 120000 < len(window["canonical_raw"])
    error = window["validation_errors"][0]
    assert error == {"path": "$", "type": "output_capacity_exceeded", "stage": "canonical",
                     "actual_chars": len(window["canonical_raw"]), "limit_chars": 120000}
    diagnostic = validation_feedback({"error_code": "invalid_output", "window": window})["errors"][0]
    assert diagnostic["actual_chars"] == len(window["canonical_raw"])
    assert diagnostic["stage"] == "canonical" and diagnostic["limit_chars"] == 120000
    assert "no_change" in diagnostic["instruction"]
    assert await env.memory.list_nodes(space_id=env.space["space_id"]) == []
    assert len(env.adapter.calls) == 1
    await env.engine.retry(failed["job_id"], regenerate_window=True)
    env.adapter.output = json.dumps(draft())
    completed = await execute(env, await env.engine.jobs.claim("repair-capacity"))
    assert completed["state"] == "succeeded"
    assert len(await env.engine.jobs.list_candidates(failed["job_id"])) == 1


def test_raw_total_capacity_reports_size_without_echoing_candidate():
    candidate = draft()
    candidate["node_changes"][0]["content"] = "x" * 120000
    value = feedback(candidate)
    assert value["errors"][0]["type"] == "output_capacity_exceeded"
    assert value["errors"][0]["stage"] == "raw"
