"""Ordinary extraction sees complete topic membership without changing event granularity."""

import json
from dataclasses import replace

import pytest
import pytest_asyncio

from core.extraction_apply import ExtractionService, digest
from core.extraction_references import ExtractionReferences
from core.memory_spaces import private_space
from storage.memory_store import MemoryStore
from storage.source_store import SourceStore
from tests.test_extraction_apply import capture

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def env(tmp_path):
    sources = SourceStore(tmp_path / "source.db")
    await sources.open()
    memory = MemoryStore(tmp_path / "memory.db", source_exists=sources.has_source)
    await memory.open()
    original = await capture(sources, "社区观星计划包含独立活动。", number=1)
    space = await memory.get_or_create_space(private_space("p", original.logical_user_id))
    events = [await memory.create_node(space_id=space["space_id"], node_type="event",
              content_text=f"独立观星活动{i}", source_ids=[original.source_id]) for i in range(2)]
    topic = await memory.create_node(space_id=space["space_id"], node_type="topic",
        title="社区观星计划", content_text="社区观星计划包含多次独立活动。", source_ids=[original.source_id])
    for event in events:
        await memory.add_edge(topic["node_id"], event["node_id"], "contains")
    latest = await capture(sources, "社区观星计划有了新的长期安排。", number=2)
    service = ExtractionService(sources, memory)
    yield sources, memory, service, space, events, topic, latest
    await memory.close()
    await sources.close()


async def prepare(env, identifiers):
    _, _, service, space, _, _, latest = env
    return await service.prepare(space_id=space["space_id"], source_ids=(latest.source_id,),
        persona="温和的陪伴者", known_node_ids=tuple(identifiers))


def no_change():
    return {"schema_version": "extraction.v1", "result": "no_change", "reason": "already_known",
            "node_changes": [], "edges": [], "profile_changes": [],
            "relationship_change": None, "coverage_changes": []}


async def test_member_candidate_adds_parent_topic_and_complete_siblings(env):
    _, _, _, _, events, topic, _ = env
    batch = await prepare(env, [events[0]["node_id"]])
    assert batch.known_nodes[0]["node_id"] == events[0]["node_id"]
    assert {node["node_id"] for node in batch.known_nodes} == {
        topic["node_id"], events[0]["node_id"], events[1]["node_id"]}
    assert len(batch.known_edges) == 2
    payload = json.loads(batch.messages()[1]["content"])
    assert payload["known_edges"] == list(batch.known_edges)
    refs = ExtractionReferences(batch)
    wire = json.loads(refs.messages()[1]["content"])
    aliases = {node["node_id"] for node in wire["known_nodes"]}
    assert aliases == {"n1", "n2", "n3"}
    assert all(edge["source_node_id"] in aliases and edge["target_node_id"] in aliases
               for edge in wire["known_edges"])
    assert refs.messages()[0] == batch.messages()[0]


async def test_direct_topic_candidate_includes_complete_members(env):
    _, _, _, _, events, topic, _ = env
    batch = await prepare(env, [topic["node_id"]])
    assert batch.known_nodes[0]["node_id"] == topic["node_id"]
    assert {edge["target_node_id"] for edge in batch.known_edges} == {node["node_id"] for node in events}
    assert batch.input_digest != replace(batch, known_edges=()).input_digest


async def test_no_topic_uses_the_same_digest_inputs_and_omits_optional_field(env):
    _, memory, _, space, _, _, latest = env
    isolated = await memory.create_node(space_id=space["space_id"], node_type="event",
        content_text="与观星无关的独立经历。", source_ids=[latest.source_id])
    batch = await prepare(env, [isolated["node_id"]])
    assert not batch.known_edges
    assert len(batch.known_nodes) == 1
    assert "known_edges" not in json.loads(batch.messages()[1]["content"])
    expected_digest = digest({"space": batch.space, "generation": batch.generation_id,
        "round": batch.expected_round, "sources": batch.sources, "nodes": batch.known_nodes,
        "profiles": batch.profiles, "relationship": batch.relationship,
        "persona": batch.persona, "prompt": batch.prompt_version, "schema": "extraction.v1"})
    assert batch.input_digest == expected_digest


@pytest.mark.parametrize("change", ["remove", "add"])
async def test_no_change_still_rejects_membership_snapshot_changes(env, change):
    _, memory, service, space, events, topic, latest = env
    batch = await prepare(env, [topic["node_id"]])
    if change == "remove":
        await memory.database.execute("DELETE FROM memory_edges WHERE source_node_id=? AND target_node_id=?",
                                      (topic["node_id"], events[0]["node_id"]))
    else:
        new = await memory.create_node(space_id=space["space_id"], node_type="event",
            content_text="另一场独立活动。", source_ids=[latest.source_id])
        await memory.add_edge(topic["node_id"], new["node_id"], "contains")
    with pytest.raises(ValueError, match="membership changed"):
        await service.apply(batch, json.dumps(no_change()), model_id="synthetic")
    assert not await memory.database.fetchall("SELECT * FROM extraction_batches")
    assert (await memory.get_active_generation(space["space_id"]))["activity_round"] == 0


async def test_regular_window_updates_topic_without_merging_or_covering_events(env):
    _, memory, service, _, events, topic, latest = env
    batch = await prepare(env, [events[0]["node_id"]])
    candidate = no_change()
    candidate.update(result="changes", reason="changes", node_changes=[{
        "local_ref": "local_topic", "node_type": "topic", "operation": "update",
        "node_id": topic["node_id"], "expected_version": 1,
        "title": "社区观星计划", "content": "社区观星计划继续开展多次独立活动，并有了新的长期安排。",
        "importance": .6, "emotion": None, "source_ids": [latest.source_id], "event_time": None,
        "base_node_ids": []}])
    refs = ExtractionReferences(batch)
    canonical = refs.decode(refs.encode_output(json.dumps(candidate)))
    await service.apply(batch, canonical, model_id="synthetic")
    assert (await memory.get_node(topic["node_id"]))["content_version"] == 2
    for event in events:
        assert (await memory.get_node(event["node_id"]))["content_text"] == event["content_text"]
        assert (await memory.get_node(event["node_id"]))["content_version"] == 1
    assert len(await memory.database.fetchall("SELECT * FROM memory_edges WHERE source_node_id=?", (topic["node_id"],))) == 2
    assert not await memory.database.fetchall("SELECT * FROM node_coverage")


async def test_stale_parent_is_not_automatically_added_to_regular_candidates(env):
    _, memory, _, _, events, topic, _ = env
    await memory.database.execute("UPDATE memory_nodes SET metadata_json=? WHERE node_id=?",
                                  ('{"summary_stale":true}', topic["node_id"]))
    batch = await prepare(env, [events[0]["node_id"]])
    assert [node["node_id"] for node in batch.known_nodes] == [events[0]["node_id"]]
    assert not batch.known_edges
