"""Synthetic protocol checks independent of private conversation content."""

import copy
import json
from dataclasses import replace

import pytest

from core.extraction_apply import PreparedExtraction
from core.extraction_references import (
    VERSION, ExtractionReferences, ReferenceValidationError,
)
from core.extraction_schema import parse_extraction_output


@pytest.fixture
def batch():
    return PreparedExtraction(
        batch_id="batch_synthetic", generation_id="gen_synthetic", expected_round=0,
        space={"space_id": "space_long", "space_type": "private",
               "persona_id": "persona_long", "logical_user_id": "user_first_long"},
        persona="Do not replace literal s1, n1, u1 or user_first_long in prose.",
        sources=(
            {"source_id": "source_first_long", "actor_kind": "user",
             "actor_logical_user_id": "user_first_long",
             "conversation_logical_user_id": "user_first_long",
             "stream_id": "stream_long", "sequence_no": 1, "observed_at": 100.0,
             "content_text": "My equipment label is s1 n1 u1."},
            {"source_id": "source_second_long", "actor_kind": "user",
             "actor_logical_user_id": "user_second_long",
             "conversation_logical_user_id": None, "observed_at": 200.0,
             "content_text": "I own another instrument."},
        ),
        known_nodes=(
            {"node_id": "node_event_long", "node_type": "event", "content_version": 1,
             "content_text": "A rehearsal used the equipment labelled s1."},
            {"node_id": "node_atomic_long", "node_type": "atomic", "content_version": 2,
             "content_text": "The label is s1 n1 u1."},
        ),
        profiles=({"logical_user_id": "user_second_long", "tags": ["musician"],
                   "description": "An instrument owner.", "content_version": 1},),
        relationship={"logical_user_id": "user_first_long", "content_version": 1,
                      "state": {"format": "relationship.slots.v1",
                                "long_term": {"body": "We discuss rehearsals.", "observed_at": 10.0},
                                "aftereffect": {"body": "", "observed_at": None}}},
    )


@pytest.fixture
def candidate():
    return {
        "schema_version": "extraction.v1", "result": "changes", "reason": "changes",
        "node_changes": [
            {"local_ref": "local_event", "node_type": "event", "operation": "update",
             "node_id": "node_event_long", "expected_version": 1,
             "content": "The rehearsal uses s1 n1 u1 and source_first_long labels.",
             "importance": 0.5, "source_ids": ["source_first_long", "source_second_long"],
             "title": "Group rehearsal", "base_node_ids": [], "emotion": None, "event_time": None},
            {"local_ref": "local_detail", "node_type": "atomic", "operation": "create",
             "node_id": None, "expected_version": None, "content": "A second instrument exists.",
             "importance": 0.4, "title": "Other instrument", "base_node_ids": [], "emotion": None, "source_ids": ["source_second_long"], "event_time": None},
        ],
        "edges": [
            {"from_ref": "local_event", "to_ref": "local_detail",
             "relation_type": "contains", "source_ids": ["source_second_long"]},
            {"from_ref": "node_event_long", "to_ref": "node_atomic_long",
             "relation_type": "contains", "source_ids": ["source_first_long"]},
        ],
        "coverage_changes": [{"event_ref": "node_event_long", "atomic_ref": "node_atomic_long",
                              "expected_event_version": 2, "expected_atomic_version": 2,
                              "coverage_kind": "partial", "source_ids": ["source_first_long"]}],
        "profile_changes": [{"logical_user_id": "user_second_long", "expected_version": 1,
                             "tags": ["instrument owner"], "description": "Owns an instrument.",
                             "source_ids": ["source_second_long"]}],
        "relationship_change": {"logical_user_id": "user_first_long", "expected_version": 1,
                                "long_term": {"op": "replace", "body": "We discuss s1 equipment."},
                                "aftereffect": {"op": "keep", "body": ""}, "source_ids": ["source_first_long"]},
    }


def test_input_projection_reuses_identity_and_keeps_prose(batch):
    refs = ExtractionReferences(batch)
    original = batch.messages()
    messages = refs.messages()
    assert VERSION == "reference.v1"
    assert messages[0] == original[0]
    data = json.loads(messages[1]["content"])
    assert data["space"]["logical_user_id"] == "u1"
    assert data["space"]["space_id"] == "space_long"
    assert data["sources"][0]["source_id"] == "s1"
    assert data["sources"][0]["actor_logical_user_id"] == "u1"
    assert data["sources"][0]["conversation_logical_user_id"] == "u1"
    assert data["sources"][1]["actor_logical_user_id"] == "u2"
    assert data["sources"][1]["conversation_logical_user_id"] is None
    assert data["known_profiles"][0]["logical_user_id"] == "u2"
    assert data["known_relationship"]["logical_user_id"] == "u1"
    assert [n["node_id"] for n in data["known_nodes"]] == ["n1", "n2"]
    assert data["persona"] == batch.persona
    assert data["sources"][0]["content_text"] == batch.sources[0]["content_text"]
    assert data["sources"][0]["stream_id"] == "stream_long"
    assert data["known_nodes"][0]["content_text"] == batch.known_nodes[0]["content_text"]
    assert batch.messages() == original


def test_output_roundtrip_covers_every_reference_position(batch, candidate):
    refs = ExtractionReferences(batch)
    wire = refs.encode_output(json.dumps(candidate))
    encoded = json.loads(wire)
    assert encoded["node_changes"][0]["source_ids"] == ["s1", "s2"]
    assert encoded["node_changes"][0]["node_id"] == "n1"
    assert encoded["node_changes"][1]["node_id"] is None
    assert encoded["edges"][0]["from_ref"] == "local_event"
    assert encoded["edges"][0]["to_ref"] == "local_detail"
    assert encoded["edges"][1]["from_ref"] == "n1"
    assert encoded["edges"][1]["to_ref"] == "n2"
    assert encoded["coverage_changes"][0]["event_ref"] == "n1"
    assert encoded["coverage_changes"][0]["atomic_ref"] == "n2"
    assert encoded["profile_changes"][0]["logical_user_id"] == "u2"
    assert encoded["relationship_change"]["logical_user_id"] == "u1"
    assert encoded["node_changes"][0]["content"] == candidate["node_changes"][0]["content"]
    restored = refs.decode(wire)
    assert json.loads(restored) == candidate
    assert parse_extraction_output(restored).model_dump(mode="json") == candidate


@pytest.mark.parametrize("path,invalid", [
    (("node_changes", 0, "source_ids", 0), "s99"),
    (("node_changes", 0, "source_ids", 0), "source_first_long"),
    (("node_changes", 0, "node_id"), "n99"),
    (("edges", 1, "from_ref"), "s1"),
    (("edges", 1, "to_ref"), "n99"),
    (("edges", 0, "source_ids", 0), "s99"),
    (("coverage_changes", 0, "event_ref"), "n99"),
    (("coverage_changes", 0, "atomic_ref"), "u1"),
    (("coverage_changes", 0, "source_ids", 0), "s99"),
    (("profile_changes", 0, "logical_user_id"), "u99"),
    (("profile_changes", 0, "source_ids", 0), "s99"),
    (("relationship_change", "logical_user_id"), "n1"),
    (("relationship_change", "source_ids", 0), "s99"),
])
def test_unknown_or_wrong_kind_reference_is_rejected_with_safe_path(batch, candidate, path, invalid):
    refs = ExtractionReferences(batch)
    wire = json.loads(refs.encode_output(json.dumps(candidate)))
    target = wire
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = invalid
    with pytest.raises(ReferenceValidationError) as caught:
        refs.decode(json.dumps(wire))
    expected_path = "$" + "".join(f"[{p}]" if isinstance(p, int) else f".{p}" for p in path)
    assert caught.value.path == expected_path
    assert caught.value.code == "unknown_reference"
    assert invalid not in str(caught.value)


def test_schema_validation_precedes_reference_decoding(batch, candidate):
    refs = ExtractionReferences(batch)
    wire = json.loads(refs.encode_output(json.dumps(candidate)))
    wire["edges"][0]["from_ref"] = "local_missing"
    with pytest.raises(ValueError, match="unknown local_ref"):
        refs.decode(json.dumps(wire))
    with pytest.raises(ValueError, match="duplicate JSON key"):
        refs.decode('{"schema_version":"extraction.v1","schema_version":"extraction.v1"}')


def test_reconstructed_batch_has_same_mapping_and_empty_context_is_supported(batch, candidate):
    first = ExtractionReferences(batch)
    reconstructed = ExtractionReferences(copy.deepcopy(batch))
    assert first.messages() == reconstructed.messages()
    assert reconstructed.decode(first.encode_output(json.dumps(candidate))) == first.decode(
        first.encode_output(json.dumps(candidate))
    )
    sparse = replace(batch, known_nodes=(), profiles=(), relationship=None,
                     space={"space_type": "group", "logical_user_id": None})
    data = json.loads(ExtractionReferences(sparse).messages()[1]["content"])
    assert data["known_nodes"] == []
    assert data["known_profiles"] == []
    assert data["known_relationship"] is None
    assert data["space"]["logical_user_id"] is None


def test_no_change_preserves_supported_schema(batch):
    version = "extraction.v1"
    refs = ExtractionReferences(batch)
    output = {"schema_version": version, "result": "no_change", "reason": "already_known",
              "node_changes": [], "edges": [], "profile_changes": [], "relationship_change": None}
    output["coverage_changes"] = []
    decoded = json.loads(refs.decode(json.dumps(output)))
    assert decoded == {**output, "reason": "no_meaningful_content"}


def test_encode_fixture_rejects_non_window_canonical_evidence(batch, candidate):
    candidate["node_changes"][0]["source_ids"][0] = "source_from_other_window"
    with pytest.raises(ReferenceValidationError):
        ExtractionReferences(batch).encode_output(json.dumps(candidate))


def test_derived_user_placeholders_roundtrip_without_rewriting_literals(batch, candidate):
    candidate["node_changes"][0]["content"] = (
        "我与{{user:user_first_long}}排练；小舟是约定称呼，u1和用户是原话。"
    )
    candidate["node_changes"][0]["event_time"] = {
        "start": None, "end": None,
        "text": "{{user:user_first_long}}所说的上个月",
    }
    candidate["profile_changes"][0]["description"] = "{{user:user_second_long}}拥有乐器。"
    candidate["profile_changes"][0]["tags"] = ["{{user:user_first_long}}"]
    candidate["relationship_change"]["long_term"]["body"] = "我期待与{{user:user_first_long}}再排练。"
    original = copy.deepcopy(candidate)
    refs = ExtractionReferences(batch)
    wire = refs.encode_output(json.dumps(candidate))
    data = json.loads(wire)
    assert data["node_changes"][0]["content"] == (
        "我与{{user:u1}}排练；小舟是约定称呼，u1和用户是原话。"
    )
    assert data["node_changes"][0]["event_time"]["text"] == "{{user:u1}}所说的上个月"
    assert data["profile_changes"][0]["description"] == "{{user:u2}}拥有乐器。"
    assert data["profile_changes"][0]["tags"] == ["{{user:u1}}"]
    assert data["relationship_change"]["long_term"]["body"] == "我期待与{{user:u1}}再排练。"
    assert json.loads(refs.decode(wire)) == original
    assert candidate == original


@pytest.mark.parametrize("path", [
    ("node_changes", 0, "content"),
    ("node_changes", 0, "event_time", "text"),
    ("profile_changes", 0, "description"),
    ("profile_changes", 0, "tags", 0),
    ("relationship_change", "long_term", "body"),
])
@pytest.mark.parametrize("invalid", ["u99", "user_first_long"])
def test_unknown_prose_reference_is_rejected_at_field_path(batch, candidate, path, invalid):
    candidate["node_changes"][0]["event_time"] = {
        "start": None, "end": None, "text": "上个月",
    }
    refs = ExtractionReferences(batch)
    wire = json.loads(refs.encode_output(json.dumps(candidate)))
    target = wire
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = "{{user:" + invalid + "}}"
    with pytest.raises(ReferenceValidationError) as caught:
        refs.decode(json.dumps(wire))
    expected_path = "$" + "".join(f"[{p}]" if isinstance(p, int) else f".{p}" for p in path)
    assert caught.value.path == expected_path
    assert caught.value.kind == "user"
    assert caught.value.code == "unknown_reference"
    assert invalid not in str(caught.value)


def test_known_prose_references_register_absent_members_and_leave_sources_untouched(batch):
    known_nodes = copy.deepcopy(batch.known_nodes)
    known_nodes[0]["content_text"] = (
        "我记得{{user:former_member}}与{{user:user_second_long}}排练，小舟和u1是称呼。"
    )
    known_nodes[0]["event_time_text"] = "{{user:former_member}}提到的去年"
    profiles = copy.deepcopy(batch.profiles)
    profiles[0]["description"] = "{{user:user_second_long}}是{{user:former_member}}的搭档。"
    profiles[0]["tags"] = ["{{user:former_member}}"]
    relationship = copy.deepcopy(batch.relationship)
    relationship["state"]["long_term"]["body"] = "我期待与{{user:user_first_long}}重逢。"
    sources = copy.deepcopy(batch.sources)
    sources[0]["content_text"] = "原话含{{user:former_member}}、{{user:u99}}与用户。"
    prepared = replace(
        batch, known_nodes=known_nodes, profiles=profiles, relationship=relationship,
        sources=sources, persona="人格原文{{user:persona_only}}不可改写。",
    )
    original = copy.deepcopy(prepared)
    refs = ExtractionReferences(prepared)
    data = json.loads(refs.messages()[1]["content"])
    assert data["known_nodes"][0]["content_text"] == (
        "我记得{{user:u3}}与{{user:u2}}排练，小舟和u1是称呼。"
    )
    assert data["known_nodes"][0]["event_time_text"] == "{{user:u3}}提到的去年"
    assert data["known_profiles"][0]["description"] == "{{user:u2}}是{{user:u3}}的搭档。"
    assert data["known_profiles"][0]["tags"] == ["{{user:u3}}"]
    assert data["known_relationship"]["state"]["long_term"]["body"] == "我期待与{{user:u1}}重逢。"
    assert data["sources"][0]["content_text"] == original.sources[0]["content_text"]
    assert data["persona"] == original.persona
    assert prepared == original


def test_window_reordering_changes_aliases_without_changing_identity(batch, candidate):
    prepared = replace(
        batch, space={"space_id": "group", "space_type": "group"},
        profiles=(), relationship=None,
        known_nodes=({**batch.known_nodes[0],
                      "content_text": "我记得{{user:former_member}}与{{user:user_first_long}}排练。"},
                     batch.known_nodes[1]),
    )
    reordered = replace(prepared, sources=tuple(reversed(prepared.sources)))
    first = ExtractionReferences(prepared)
    second = ExtractionReferences(reordered)
    candidate["profile_changes"][0]["expected_version"] = 0
    candidate["relationship_change"]["expected_version"] = 0
    candidate["node_changes"][0]["content"] = "我与{{user:user_first_long}}和{{user:former_member}}排练。"
    first_wire = first.encode_output(json.dumps(candidate))
    second_wire = second.encode_output(json.dumps(candidate))
    assert json.loads(first_wire)["node_changes"][0]["content"] == "我与{{user:u1}}和{{user:u3}}排练。"
    assert json.loads(second_wire)["node_changes"][0]["content"] == "我与{{user:u2}}和{{user:u3}}排练。"
    first_context = json.loads(first.messages()[1]["content"])
    second_context = json.loads(second.messages()[1]["content"])
    assert first_context["known_nodes"][0]["content_text"] == "我记得{{user:u3}}与{{user:u1}}排练。"
    assert second_context["known_nodes"][0]["content_text"] == "我记得{{user:u3}}与{{user:u2}}排练。"
    assert json.loads(first.decode(first_wire)) == candidate
    assert json.loads(second.decode(second_wire)) == candidate


def test_group_mentions_quotes_and_unknowns_share_the_speaker_dictionary(batch):
    first, second = batch.sources
    sources = (
        first | {"actor_account_id": "101", "content_text": "<@!202> <@404> 审稿完成了吗？"},
        second | {"actor_account_id": "202", "stream_id": first["stream_id"], "message_id": "m2"},
        first | {"source_id": "third", "metadata_json": json.dumps({"group_addressing": [
            {"kind": "reply", "message_id": "m2"}, {"kind": "mention", "is_self": True},
            {"kind": "mention_all"}]})},
    )
    response = sources[1] | {"source_id": "response", "actor_kind": "agent",
                             "actor_logical_user_id": None, "metadata_json": '{"trigger_message_id":"m2"}'}
    group = replace(batch, space={"space_type": "group"}, sources=(*sources, response))
    data = json.loads(ExtractionReferences(group).messages()[1]["content"])["sources"]
    assert data[0]["content_text"] == "[@1] [@2] 审稿完成了吗？"
    assert data[0]["group_addressing"] == [
        {"kind": "mention", "actor_kind": "user", "logical_user_id": "u2", "marker": "[@1]"},
        {"kind": "mention", "actor_kind": "unknown", "marker": "[@2]"}]
    assert data[2]["group_addressing"] == [
        {"kind": "reply", "actor_kind": "user", "logical_user_id": "u2"},
        {"kind": "mention", "actor_kind": "agent"}, {"kind": "mention_all"}]
    assert all("conversation_logical_user_id" not in row for row in data)
