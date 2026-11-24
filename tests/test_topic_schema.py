"""Topic proposals preserve event identity and readable-text contracts."""

import json

import pytest
from pydantic import ValidationError

from core.topic_prompt import build_messages
from core.topic_schema import VERSION, model_output_schema, output_schema, parse_output, parse_output_for_context


def proposal(**changes):
    return {"operation": "create", "topic_id": None, "expected_version": None,
            "title": "社区协作", "content": "{{user:u1}}的社区协作从调研推进到试验。",
            "importance": 0.55, "emotion": None, "member_ids": ["e1", "e2"], **changes}


def encoded(*changes):
    return json.dumps({"schema_version": "topic-consolidation.v1", "changes": list(changes)})


def test_empty_changes_is_a_complete_no_change_decision():
    assert parse_output(encoded()) == {"schema_version": "topic-consolidation.v1", "changes": []}


def test_model_schema_omits_topic_version_and_parser_restores_it():
    schema = model_output_schema()
    assert "schema_version" not in schema["properties"]
    assert "schema_version" not in schema.get("required", [])
    assert "expected_version" not in schema["$defs"]["TopicChange"]["properties"]
    assert "expected_version" not in schema["$defs"]["TopicChange"].get("required", [])
    value = proposal(operation="update", topic_id="t1", expected_version=99)
    parsed = parse_output_for_context(encoded(value), {
        "nodes": [{"node_id": "t1", "node_type": "topic", "content_version": 4}],
    }, validate_supplied_versions=False)
    assert parsed["changes"][0]["expected_version"] == 4


def test_topic_model_output_without_program_fields_gets_schema_version():
    parsed = parse_output_for_context(json.dumps({"changes": []}), {"nodes": []})
    assert parsed == {"schema_version": VERSION, "changes": []}


def test_topic_creation_needs_distinct_members_but_existing_topic_can_keep_one():
    for members in (["e1"], ["e1", "e1"]):
        with pytest.raises(ValidationError):
            parse_output(encoded(proposal(member_ids=members)))
    value = parse_output(encoded(proposal(operation="update", topic_id="t1", expected_version=2,
                                         member_ids=["e1"])))
    assert value["changes"][0]["member_ids"] == ["e1"]


def test_topic_identity_and_versions_must_match_the_operation():
    for change in (proposal(topic_id="t1"), proposal(expected_version=1),
                   proposal(operation="update"), proposal(operation="update", topic_id="t1")):
        with pytest.raises(ValidationError):
            parse_output(encoded(change))
    update = proposal(operation="update", topic_id="t1", expected_version=1)
    with pytest.raises(ValidationError):
        parse_output(encoded(update, update))


def test_topic_text_limits_count_identity_references_as_one_character():
    reference = "{{user:" + "u" * 150 + "}}"
    assert parse_output(encoded(proposal(content=reference + "字" * 1999)))["changes"]
    with pytest.raises(ValidationError):
        parse_output(encoded(proposal(content=reference + "字" * 2000)))


def test_topic_does_not_accept_source_fabrication_or_member_rewrites():
    for extra in ({"source_ids": ["invented"]}, {"node_changes": []}):
        with pytest.raises(ValidationError):
            parse_output(encoded(proposal(**extra)))


def test_prompt_exposes_member_direction_without_private_metadata():
    nodes = [{"node_id": "e1", "node_type": "event", "content_version": 3,
              "content_text": "独立事件。", "metadata": {"irrelevant": "internal"}},
             {"node_id": "t1", "node_type": "topic", "content_version": 2,
              "content_text": "整体概况。"}]
    edges = [{"source_node_id": "t1", "target_node_id": "e1", "relation_type": "contains",
              "edge_id": "internal-id"}]
    messages = build_messages("自然表达", nodes, edges)
    value = json.loads(messages[1]["content"])
    assert "content_version" not in value["nodes"][0]
    assert "metadata" not in value["nodes"][0]
    assert value["edges"] == [{"source_node_id": "t1", "target_node_id": "e1", "relation_type": "contains"}]
    assert output_schema()["additionalProperties"] is False


def test_prompt_includes_known_stale_topic_as_read_only_context():
    messages = build_messages("人格", [{"node_id": "t1", "node_type": "topic", "content_version": 1,
        "content_text": "待复核概况", "metadata": {"summary_stale": True, "internal": "omit"}}], [])
    node = json.loads(messages[1]["content"])["nodes"][0]
    assert node["summary_stale"] is True and "metadata" not in node
    assert "不能另建同义主题" in messages[0]["content"]


def test_topic_prompt_preserves_source_speaker_and_does_not_assume_actor():
    from core.extraction_observations import source_observation_fields
    evidence = [{"node_id": "e1", "sources": [{"source_id": "s1", "actor_kind": "user",
        "actor_logical_user_id": "u1", "observed_at": 100.0,
        "content_text": "我同事阿遥负责那次活动，不是我。"}]}]
    messages = build_messages("人格", [], [], evidence=evidence, allowed_user_ids=["u1"])
    source = evidence[0]["sources"][0]
    assert json.loads(messages[1]["content"])["evidence"] == [
        {"node_id": "e1", "sources": [source | source_observation_fields(source)]}]
    assert "observed_at_iso_utc" not in source
    assert "发言者不必是行动者" in messages[0]["content"]


def test_related_topic_directory_exposes_only_visible_members_as_read_only():
    messages = build_messages("人格", [{"node_id": "e1", "node_type": "event"}], [],
        related_topics=[{"node_id": "t1", "title": "共同项目", "content_text": "已有项目概况。",
                         "content_version": 2, "member_ids": ["e1", "not-visible"]}])
    payload = json.loads(messages[1]["content"])
    assert payload["related_topics"][0]["visible_member_ids"] == ["e1"]
    assert payload["related_topics"][0]["read_only"] is True
    assert "member_ids" not in payload["related_topics"][0]
