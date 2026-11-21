"""Shared members expose other topic ownership without recursively loading the graph."""

import json

import pytest

from core.topic_budget import fit_topic_evidence, message_bytes
from core.topic_prompt import build_messages
from storage.topic_store import TopicStore
from tests.test_topic_store import env, begin, output

pytestmark = pytest.mark.asyncio


async def graph(env):
    memory, store, _, space, events = env
    left = await memory.create_node(space_id=space["space_id"], node_type="topic",
        title="共同展览项目", content_text="共同展览项目包含筹备与设计。", source_ids=["source-left"])
    right = await memory.create_node(space_id=space["space_id"], node_type="topic",
        title="设计学习历程", content_text="设计学习包含展览设计和其他独立实践。", source_ids=["source-right"])
    await memory.add_edge(left["node_id"], events[0]["node_id"], "contains")
    await memory.add_edge(left["node_id"], events[1]["node_id"], "contains")
    await memory.add_edge(right["node_id"], events[1]["node_id"], "contains")
    await memory.add_edge(right["node_id"], events[2]["node_id"], "contains")
    return left, right


async def test_optional_root_gets_parent_and_full_members(env):
    memory, store, _, space, events = env
    left, right = await graph(env)
    newcomer = await memory.create_node(space_id=space["space_id"], node_type="event",
        content_text="新的项目进展", source_ids=["new-source"])
    prepared = await store.prepare(space["space_id"], newcomer["generation_id"],
        [newcomer["node_id"], events[0]["node_id"]], seed_ids=[newcomer["node_id"]])
    ids = {node["node_id"] for node in prepared["nodes"]}
    assert {newcomer["node_id"], left["node_id"], events[0]["node_id"], events[1]["node_id"]} == ids
    assert [topic["node_id"] for topic in prepared["related_topics"]] == [right["node_id"]]


async def test_shared_sibling_parent_is_read_only_and_does_not_expand_graph(env):
    _, store, _, space, events = env
    left, right = await graph(env)
    prepared = await store.prepare(space["space_id"], left["generation_id"],
                                   [events[0]["node_id"]], seed_ids=[events[0]["node_id"]])
    ids = {node["node_id"] for node in prepared["nodes"]}
    assert right["node_id"] not in ids and events[2]["node_id"] not in ids
    related = prepared["related_topics"]
    assert related[0]["member_ids"] == sorted([events[1]["node_id"], events[2]["node_id"]])
    assert related[0]["read_only"]
    payload = json.loads(build_messages("人格", prepared["nodes"], prepared["edges"], related_topics=related)[1]["content"])
    assert payload["related_topics"][0]["visible_member_ids"] == [events[1]["node_id"]]
    assert "member_ids" not in payload["related_topics"][0]
    assert events[2]["node_id"] not in json.dumps(payload)
    job = await begin(env)
    with pytest.raises(ValueError, match="identity/version"):
        await store.apply(job, prepared, output(events[:2], right))


@pytest.mark.parametrize("mutation", ["title", "protection", "stale", "status", "membership", "new_parent"])
async def test_read_only_directory_changes_invalidate_even_no_change(env, mutation):
    memory, store, _, space, events = env
    left, right = await graph(env)
    prepared = await store.prepare(space["space_id"], left["generation_id"], [events[0]["node_id"]])
    if mutation == "title":
        await memory.database.execute("UPDATE memory_nodes SET title='另一概况' WHERE node_id=?", (right["node_id"],))
    elif mutation == "protection":
        await memory.database.execute("UPDATE memory_nodes SET manual_protected=1 WHERE node_id=?", (right["node_id"],))
    elif mutation == "stale":
        await memory.database.execute("UPDATE memory_nodes SET metadata_json=? WHERE node_id=?",
                                      ('{"summary_stale":true}', right["node_id"]))
    elif mutation == "status":
        await memory.database.execute("UPDATE memory_nodes SET status='disabled' WHERE node_id=?", (right["node_id"],))
    elif mutation == "membership":
        await memory.database.execute("DELETE FROM memory_edges WHERE source_node_id=? AND target_node_id=?",
                                      (right["node_id"], events[2]["node_id"]))
    else:
        parent = await memory.create_node(space_id=space["space_id"], node_type="topic",
            content_text="另一个有独立意义的持续关联", source_ids=["third-source"])
        await memory.add_edge(parent["node_id"], events[1]["node_id"], "contains")
    job = await begin(env)
    with pytest.raises(ValueError, match="context changed"):
        await store.apply(job, prepared, {"changes": []})
    assert not await memory.database.fetchall("SELECT * FROM topic_reviews")


async def test_directory_is_counted_as_required_prompt_content(env):
    _, store, _, space, events = env
    left, _ = await graph(env)
    prepared = await store.prepare(space["space_id"], left["generation_id"], [events[0]["node_id"]])
    base_without = message_bytes(build_messages("人格", prepared["nodes"], prepared["edges"]))
    base_with = message_bytes(build_messages("人格", prepared["nodes"], prepared["edges"],
                                             related_topics=prepared["related_topics"]))
    assert base_with > base_without
    fitted, diagnostics = fit_topic_evidence("人格", prepared["nodes"], prepared["edges"], [],
                                            base_without, related_topics=prepared["related_topics"])
    assert fitted is None and diagnostics["base_prompt_bytes"] == base_with
