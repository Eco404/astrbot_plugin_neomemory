"""Private image request ordering, extraction evidence, and text-only replay."""

import json

import pytest

from core.extraction_schema import extraction_json_schema
from core.vision_request import VisionOutputError, split_vision_output, vision_messages, vision_schema


def messages():
    schema = extraction_json_schema()
    sources = [
        {"source_id": "s1", "actor_kind": "user", "actor_logical_user_id": "u1", "content_text": "第一条"},
        {"source_id": "s2", "actor_kind": "agent", "actor_logical_user_id": None, "content_text": "第二条"},
    ]
    return [{"role": "system", "content": "rules\nJSON schema:\n" + json.dumps(schema)},
            {"role": "user", "content": json.dumps({"sources": sources, "persona": "P"})}]


def attachment(alias, slot, **kwargs):
    return {"source_id": "stable-" + alias, "source_alias": alias, "slot": slot, **kwargs}


def test_render_attaches_after_own_source_and_retains_missing_slots_and_feedback():
    base = messages()
    tail = [{"role": "assistant", "content": "rejected"}, {"role": "user", "content": "try again"}]
    slots = [attachment("s2", 1, caption="蓝色"),
             attachment("s1", 3, jpeg=b"third", authors=["小乔", None]),
             attachment("s1", 1, jpeg=b"first"), attachment("s1", 2, reason="missing")]
    request = vision_messages(base + tail, slots)
    schema = json.loads(request[0]["content"].split("JSON schema:\n")[1])
    assert schema == vision_schema(extraction_json_schema())
    assert schema["$defs"] and "$defs" not in schema["properties"]["extraction"]
    parts = request[1]["content"]
    assert [p["type"] for p in parts] == ["text", "text", "text", "image_url", "text", "text", "image_url", "text", "text"]
    assert json.loads(parts[1]["text"])["发言者"].startswith("{{user:u1}}")
    assert json.loads(parts[4]["text"]) == {"source_id": "s1", "slot": 2, "图片状态": "不可读", "reason": "missing"}
    assert json.loads(parts[5]["text"])["转发署名链"] == ["小乔", None]
    assert "记忆主人" in json.loads(parts[7]["text"])["发言者"]
    assert parts[-1]["text"].endswith('"宿主图片描述":"蓝色"}')
    assert request[2:] == tail and request[1] != base[1]


def test_reconstruction_uses_text_even_for_provider_without_vision():
    base = messages()
    req = vision_messages(base, [attachment("s1", 2, evidence="贴纸上写着周六", evidence_origin="image")],
                          collect_evidence=False)
    assert isinstance(req[1]["content"], str)
    assert "贴纸上写着周六" in req[1]["content"]
    assert "image_evidence" not in req[0]["content"]
    assert json.loads(req[0]["content"].split("JSON schema:\n")[1]) == extraction_json_schema()


def entry(alias, slot=1):
    return {"source_id": alias, "slot": slot, "description": "黄色"}


def test_parse_restores_source_and_uses_frozen_origin():
    slots = [attachment("s1", 1, sha256="digest"), attachment("s2", 1, caption="黄色")]
    extraction = {"profile_changes": [{"source_ids": ["s1", "s2"]}], "relationship_change": None}
    raw, evidence = split_vision_output(json.dumps({"extraction": extraction, "image_evidence": [entry("s2"), entry("s1")]}), slots)
    assert json.loads(raw) == extraction
    assert [(row["source_id"], row["origin"]) for row in evidence] == [
        ("stable-s2", "host_caption"), ("stable-s1", "image")]
    assert not any("jpeg" in row or "sha256" in row for row in evidence)


@pytest.mark.parametrize(("entries", "no_change"), [
    ([entry("s1"), entry("s1")], False),
    ([entry("s1", 3)], False),
    ([entry("s2")], False),
    ([entry("s3")], False),
    ([entry("s4")], False),
    ([entry("s1")], True),
])
def test_rejects_invalid_or_unused_image_evidence(entries, no_change):
    slots = [attachment("s1", 1, sha256="digest"), attachment("s2", 1, caption="黄色"),
             attachment("s3", 1, reason="missing"), attachment("s4", 1, evidence="黄色")]
    candidate = {"node_changes": [{"source_ids": ["s1", "s3", "s4"]}],
                 "result": "no_change" if no_change else "changes"}
    with pytest.raises(VisionOutputError) as exc:
        split_vision_output(json.dumps({"extraction": candidate, "image_evidence": entries}), slots)
    assert exc.value.path.startswith("$.image_evidence[")


def test_regeneration_feedback_points_to_nested_extraction_or_image_slot():
    from core.extraction_regeneration import automatic_correction_allowed, validation_feedback

    slots = [attachment("s1", 1, sha256="digest")]
    exc = {"schema_version": "extraction.v1", "result": "no_change", "reason": "invalid_enum",
           "node_changes": [], "profile_changes": [], "relationship_change": None, "edges": [], "coverage_changes": []}
    window = {"raw": json.dumps({"extraction": exc, "image_evidence": []}), "image_slots": slots}
    archived = {"window": window, "error_code": "invalid_output"}
    feedback = validation_feedback(archived, schema_version="extraction.v1")
    assert [e["path"] for e in feedback["errors"]] == ["$.extraction.reason"]

    window["raw"] = json.dumps({"extraction": exc, "image_evidence": [entry("s1")]})
    image_errors = validation_feedback(archived, schema_version="extraction.v1")["errors"]
    assert image_errors[0]["path"] == "$.image_evidence[0]"
    assert automatic_correction_allowed("invalid_output", {"validation_errors": image_errors})

    window["raw"] = '{"extraction":'
    errors = validation_feedback(archived, schema_version="extraction.v1")["errors"]
    assert errors[0]["type"] == "json_invalid" and errors[0]["pos"] == len(window["raw"])
