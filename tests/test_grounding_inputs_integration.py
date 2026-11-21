"""Grounded input protocols preserve frozen sources, structures and retry context."""

from dataclasses import asdict, replace
import json

import pytest

from core.extraction_apply import ExtractionService, PreparedExtraction
from core.extraction_references import ExtractionReferences
from core.prompts import PROMPT_VERSION
from core.topic_prompt import PROMPT_VERSION as TOPIC_PROMPT_VERSION
from core.topic_prompt import schema_for_prompt_version as topic_schema_for_prompt
from tests.test_extraction_apply import capture, node, prepare, profile
from tests.test_provenance import output, relationship, stores
from tests.test_maintenance_engine import environment, enqueue_and_claim, execute
from tests.test_topic_maintenance import candidate, seed


@pytest.mark.asyncio
async def test_grounding_fields_survive_references_and_persisted_prepared_roundtrip(stores):
    sources, memory, initial_service = stores
    first, source = await prepare(stores)
    result = await initial_service.apply(first, json.dumps(output(
        [node(source.source_id, content="我答应陪对方准备社区活动。")],
        profiles=[profile(source.source_id, source.logical_user_id)],
        relationship=relationship(source.source_id, source.logical_user_id, 0, "我愿意支持对方。"),
    )), model_id="synthetic")
    node_id = result["result"]["node_refs"]["local_interview"]
    user_source = await capture(sources, text="明天由我完成准备。", number=2)
    agent_source = await capture(sources, text="我会陪你商量。", number=3, actor="agent")
    service = ExtractionService(sources, memory)
    batch = await service.prepare(space_id=first.space["space_id"],
        source_ids=(user_source.source_id, agent_source.source_id), persona="合成人格",
        known_node_ids=(node_id,))
    before = ExtractionReferences(batch).messages()
    assert batch.prompt_version == PROMPT_VERSION
    payload = json.loads(before[1]["content"])
    assert [row["speaker_perspective"] for row in payload["sources"]] == ["conversation_participant", "persona_self"]
    assert [row["observed_at"] for row in payload["sources"]] == [200, 300]
    assert payload["sources"][0]["actor_logical_user_id"] == "u1"
    assert payload["sources"][1]["actor_logical_user_id"] is None
    for row in (payload["known_nodes"][0], payload["known_profiles"][0], payload["known_relationship"]):
        assert row["observation_anchors"][0]["observed_from_iso_utc"] == "1970-01-01T00:01:40+00:00"
    frozen = PreparedExtraction(**json.loads(json.dumps(asdict(batch))))
    await sources.database.execute("UPDATE source_records SET observed_at=999999 WHERE source_id=?", (source.source_id,))
    assert frozen.input_digest == batch.input_digest
    assert ExtractionReferences(frozen).messages() == before
    applied = await service.apply(frozen, json.dumps(output(
        [node(user_source.source_id, old=batch.known_nodes[0], content="我会和对方商量活动准备。")],
        profiles=[profile(user_source.source_id, user_source.logical_user_id,
                          version=1, description="对方愿意负责社区活动准备。")],
        relationship=relationship(user_source.source_id, user_source.logical_user_id, 1, "我更愿意靠近，也想保留自己的节奏。"),
    )), model_id="synthetic")
    assert applied["result"]["node_refs"]["local_interview"] == node_id
    assert (await memory.get_node(node_id))["content_version"] == 2
    actual_profile = (await memory.list_profiles(space_id=batch.space["space_id"]))[0]
    actual_relation = await memory.get_relationship(space_id=batch.space["space_id"], logical_user_id=user_source.logical_user_id)
    assert actual_profile["content_version"] == 2
    assert actual_relation["content_version"] == 2
    assert actual_relation["state"]["long_term"]["body"] == "我更愿意靠近，也想保留自己的节奏。"
    for kind, entity_id in (("node", node_id), ("profile", actual_profile["profile_id"]),
                            ("relationship", actual_relation["relationship_id"])):
        records = await memory.list_version_derivations(kind, entity_id, 2)
        assert records[0]["sources"] == [{"source_id": user_source.source_id, "source_kind": "source_record"}]


@pytest.mark.asyncio
@pytest.mark.parametrize("existed", [False, True])
async def test_ordinary_grounded_structure_is_frozen_until_commit(stores, existed):
    sources, memory, _ = stores
    service = ExtractionService(sources, memory)
    env = (sources, memory, service)
    first, source = await prepare(env)
    event = await memory.create_node(space_id=first.space["space_id"], node_type="event",
        content_text="社区活动开始筹备。", source_ids=[source.source_id])
    atomic = await memory.create_node(space_id=first.space["space_id"], node_type="atomic",
        content_text="筹备时需要准备画具。", source_ids=[source.source_id])
    if existed:
        await memory.add_edge(event["node_id"], atomic["node_id"], "contains")
    batch, _ = await prepare(env, number=2, known=(event["node_id"], atomic["node_id"]))
    payload = json.loads(ExtractionReferences(batch).messages()[1]["content"])
    assert bool(payload.get("known_edges")) is existed
    if existed:
        await memory.database.execute("DELETE FROM memory_edges WHERE source_node_id=? AND target_node_id=?",
            (event["node_id"], atomic["node_id"]))
    else:
        await memory.add_edge(event["node_id"], atomic["node_id"], "contains")
    with pytest.raises(ValueError, match="event member structure changed"):
        await service.apply(batch, json.dumps(output()), model_id="synthetic")
    assert not await memory.database.fetchall("SELECT * FROM extraction_batches")


@pytest.mark.asyncio
async def test_topic_correction_uses_same_frozen_evidence_and_member_structure(environment):
    env = environment
    prompt_version = TOPIC_PROMPT_VERSION
    env.engine.config = replace(env.config, extraction_auto_regeneration=True)
    nodes = await seed(env)
    atomic = await env.memory.create_node(space_id=env.space["space_id"], node_type="atomic",
        content_text="首场活动需要提前布置观测设备。", source_ids=[env.records[0].source_id])
    await env.memory.add_edge(nodes[0]["node_id"], atomic["node_id"], "contains")
    valid = json.loads(candidate(nodes))
    valid['schema_version'] = topic_schema_for_prompt(prompt_version)
    bad = json.loads(candidate(nodes))
    bad['schema_version'] = topic_schema_for_prompt(prompt_version)
    bad["changes"][0]["member_ids"] = bad["changes"][0]["member_ids"][:1]
    env.adapter.output = json.dumps(bad)

    async def correct(messages):
        payload = json.loads(messages[1]["content"])
        assert payload["prompt_version"] == prompt_version
        assert payload["time_semantics"]["speaker_local_timezone"] == "unknown"
        evidence = [source for row in payload["evidence"] for source in row["sources"]]
        assert evidence
        assert all(source["observed_at_iso_utc"] == "1970-01-01T00:01:40+00:00" for source in evidence)
        assert all(source["speaker_perspective"] == "conversation_participant" for source in evidence)
        assert {"source_node_id": nodes[0]["node_id"], "target_node_id": atomic["node_id"],
                "relation_type": "contains"} in payload["edges"]
        if len(env.adapter.calls) == 1:
            await env.sources.database.execute("UPDATE source_records SET observed_at=999999 WHERE source_id=?",
                (env.records[0].source_id,))
            return
        assert messages[:2] == env.adapter.calls[0]
        feedback = json.loads(messages[-1]["content"])
        error = next(row for row in feedback["errors"] if row["type"] == "topic_create_members")
        assert error["constraints"]["minItems"] == 2
        assert error["distinct_items"] == 1
        env.adapter.output = json.dumps(valid)

    env.adapter.on_generate = correct
    result = await execute(env, await enqueue_and_claim(env, "topic_consolidate"))
    assert result["state"] == "succeeded", result
    assert len(env.adapter.calls) == 2
