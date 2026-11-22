"""Recovery metrics preserve identity and protection rules of real validators."""

import json

import pytest

from core.candidate_feedback import CandidateValidationError, validation_errors
from core.extraction_regeneration import validation_feedback
from core.extraction_schema import SCHEMA_VERSION
from core.topic_prompt import build_messages
from core.topic_regeneration import EXPLANATIONS
from core.topic_schema import output_schema
from storage.topic_store import TopicStore
from tests.test_maintenance_engine import environment, enqueue_and_claim
from tests.test_regeneration_contract import complete_candidate
from tests.test_topic_maintenance import candidate, seed


def test_neutral_conflict_reports_the_correct_emotion_collection():
    schema_version = SCHEMA_VERSION
    value = complete_candidate(schema_version)
    value["node_changes"][0]["emotion"]["tags"] = [{"tag": "neutral", "intensity": .1}, {"tag": "trust", "intensity": .6}]
    path = "$.node_changes[0].emotion"
    archive = {"error_code": "invalid_output", "window": {"raw": json.dumps(value)}}
    errors = validation_feedback(archive, schema_version=schema_version)["errors"]
    error = next(row for row in errors if row["type"] == "neutral_emotion_conflict")
    assert error["path"] == path
    assert error["actual_items"] == 2
    assert error["constraints"]["when_neutral_present_maxItems"] == 1


def test_duplicate_emotion_metrics_name_the_rejected_collection():
    schema_version = SCHEMA_VERSION
    value = complete_candidate(schema_version)
    value["node_changes"][0]["emotion"]["tags"] = [{"tag": "trust", "intensity": .1}, {"tag": "trust", "intensity": .6}]
    path = "$.node_changes[0].emotion.tags"
    errors = validation_feedback({"error_code": "invalid_output", "window": {"raw": json.dumps(value)}}, schema_version=schema_version)["errors"]
    violations = [violation for error in errors for violation in error.get("violations", [])]
    assert any(row["path"] == path and row["actual_items"] == 2 and row["distinct_items"] == 1
               and row["constraints"] == {"unique_by": "tag"} for row in violations)


@pytest.mark.asyncio
async def test_member_feedback_does_not_offer_a_protected_new_member(environment):
    env = environment
    nodes = await seed(env, count=3)
    await env.memory.set_manual_protection(nodes[2]["node_id"], protected=True)
    store = TopicStore(env.memory)
    generation = await env.memory.get_active_generation(env.space["space_id"])
    prepared = await store.prepare(env.space["space_id"], generation["generation_id"],
                                   [node["node_id"] for node in nodes])
    output = json.loads(candidate(nodes[:2]))
    output["changes"][0]["member_ids"][1] = "missing_member"
    job = await enqueue_and_claim(env, "topic_consolidate")
    with pytest.raises(CandidateValidationError) as failure:
        await store.apply(job, prepared, output)
    assert failure.value.code == "topic_member_unavailable"
    prompt = build_messages("", prepared["nodes"], prepared["edges"],
                            allowed_user_ids=prepared["allowed_user_ids"])
    errors = validation_errors(failure.value, json.dumps(output), output_schema(), EXPLANATIONS,
                               context=json.loads(prompt[1]["content"]), pipeline="topic")
    assert set(errors[0]["constraints"]["enum"]) == {node["node_id"] for node in nodes[:2]}
    output["changes"][0]["member_ids"][1] = nodes[2]["node_id"]
    with pytest.raises(CandidateValidationError, match="protected member"):
        await store.apply(job, prepared, output)
    assert not await env.memory.database.fetchall("SELECT * FROM topic_receipts")
