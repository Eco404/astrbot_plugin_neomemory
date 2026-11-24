"""Emotion protocol, stable identities and semantic version boundaries."""

import copy
import json

import pytest

from core.memory_emotions import validate_emotion
from core.extraction_apply import ExtractionService
from pydantic import ValidationError
from core.extraction_references import ExtractionReferences, ReferenceValidationError
from core.extraction_schema import extraction_json_schema, parse_extraction_output
from core.extraction_regeneration import validation_feedback
from core.prompts import build_extraction_prompt
from tests.test_extraction_apply import node, output, prepare, stores


def candidate(source, emotion, **kwargs):
    return {**output([{**node(source, **kwargs), "title": "展出后的期待", "emotion": emotion}]),
            "schema_version": "extraction.v1", "coverage_changes": []}


def emotion(description="听到作品展出，我有些期待。"):
    return {"tags": [{"tag": "joy", "intensity": 0.4}], "description": description}


def test_production_coverage_rule_counts_emotion():
    current = build_extraction_prompt(persona="合成人格", sources=[])[0]["content"]
    assert "正文或规范化emotion实际改变则内容版本+1" in current
    assert "正文完全相同或不更新时仍是原 content_version" not in current
    assert "事件正文或emotion变化使旧覆盖失效" in current


def test_emotion_protocol_is_explicit_nullable_and_strict():
    value = candidate("s1", None)
    assert parse_extraction_output(json.dumps(value)).node_changes[0].emotion is None
    missing = copy.deepcopy(value)
    del missing["node_changes"][0]["emotion"]
    with pytest.raises(ValueError):
        parse_extraction_output(json.dumps(missing))
    for invalid in ({}, {**emotion(), "tags": ["neutral", "joy"]},
                    {**emotion(), "intensity": True}, {**emotion(), "intensity": 1.1},
                    {**emotion(), "tags": ["joy", "joy"]}, emotion("甲" * 161)):
        with pytest.raises(ValueError):
            parse_extraction_output(json.dumps(candidate("s1", invalid)))
    schema = extraction_json_schema("extraction.v1")
    assert "emotion" in schema["$defs"]["NodeChange"]["required"]
    for model in schema["$defs"].values():
        if model.get("type") == "object":
            assert model["additionalProperties"] is False
            assert set(model["required"]) == set(model["properties"])


def test_emotion_repair_feedback_preserves_field_path():
    invalid = candidate("s1", {**emotion(), "tags": [{"tag": "joy", "intensity": 2.0}]})
    feedback = validation_feedback({"window": {"raw": json.dumps(invalid)}, "error_code": "invalid_output"}, schema_version="extraction.v1")
    assert feedback["errors"][0]["path"] == "$.node_changes[0].emotion.tags[0].intensity"


@pytest.mark.asyncio
async def test_emotion_reference_is_encoded_decoded_and_unknown_member_rejected(stores):
    batch, source = await prepare(stores)
    value = candidate(source.source_id, emotion("听到{{user:" + source.logical_user_id + "}}分享，我有些期待。"))
    refs = ExtractionReferences(batch)
    encoded = json.loads(refs.encode_output(json.dumps(value)))
    assert "{{user:u1}}" in encoded["node_changes"][0]["emotion"]["description"]
    assert json.loads(refs.decode(json.dumps(encoded))) == value
    encoded["node_changes"][0]["emotion"]["description"] = "{{user:unknown}}很高兴。"
    with pytest.raises(ReferenceValidationError) as failure:
        refs.decode(json.dumps(encoded))
    assert failure.value.path.endswith(".emotion.description")


@pytest.mark.asyncio
async def test_emotion_update_is_versioned_and_invalid_input_cannot_clear_it(stores):
    sources, memory, _ = stores
    service = ExtractionService(sources, memory)
    env = sources, memory, service
    first, source = await prepare(env)
    receipt = await service.apply(first, json.dumps(candidate(source.source_id, emotion())), model_id="synthetic")
    node_id = receipt["result"]["node_refs"]["local_interview"]
    before = await memory.get_node(node_id)
    second, changed_source = await prepare(env, number=2, known=(node_id,))
    assert json.loads(second.messages()[1]["content"])["known_nodes"][0]["emotion"] == validate_emotion(emotion())
    updated = emotion("事情已有结果，我的期待转为平静。")
    updated["tags"] = [{"tag": "neutral", "intensity": 0.1}]
    await memory.database.execute("UPDATE memory_nodes SET reinforcement=.1 WHERE node_id=?", (node_id,))
    await service.apply(second, json.dumps(candidate(changed_source.source_id, updated,
        old=before, content=before["content_text"])), model_id="synthetic")
    after = await memory.get_node(node_id)
    assert after["content_text"] == before["content_text"]
    assert after["content_version"] == before["content_version"] + 1
    assert after["reinforcement"] == 0 and after["emotion"] == validate_emotion(updated)
    versions = await memory.list_node_versions(node_id)
    assert [item["emotion"] for item in versions] == [validate_emotion(emotion()), validate_emotion(updated)]
    third, final_source = await prepare(env, number=3, known=(node_id,))
    missing = output([node(final_source.source_id, old=after)])
    del missing["node_changes"][0]["emotion"]
    with pytest.raises(ValidationError):
        await service.apply(third, json.dumps(missing), model_id="synthetic")
    assert (await memory.get_node(node_id))["emotion"] == validate_emotion(updated)
