"""Topic preparation exposes and fences visible event/atomic containment."""

import pytest

from core.extraction_apply import digest
from tests.test_topic_store import begin, env, initial_topic, output, prepared

pytestmark = pytest.mark.asyncio


async def atomic(env, label):
    memory, _, _, space, _ = env
    return await memory.create_node(space_id=space["space_id"], node_type="atomic",
        content_text=label, source_ids=["synthetic-source"])


async def test_visible_member_structure_does_not_expand_event_children(env):
    memory, store, _, space, nodes = env
    visible = await atomic(env, "绘画活动的具体安排。")
    hidden = await atomic(env, "未选择进入上下文的活动细节。")
    for child in (visible, hidden):
        await memory.add_edge(nodes[0]["node_id"], child["node_id"], "contains")
    batch = await store.prepare(space["space_id"], nodes[0]["generation_id"],
                                [nodes[0]["node_id"], visible["node_id"]])
    assert batch["member_structure_version"] == 1
    assert {item["node_id"] for item in batch["nodes"]} == {nodes[0]["node_id"], visible["node_id"]}
    assert batch["edges"] == [{"source_node_id": nodes[0]["node_id"],
        "target_node_id": visible["node_id"], "relation_type": "contains"}]
    assert batch["input_digest"] == digest({key: value for key, value in batch.items() if key != "input_digest"})


@pytest.mark.parametrize("add_after_prepare", [True, False])
async def test_changed_visible_member_structure_requires_reprepare(env, add_after_prepare):
    memory, store, _, _, nodes = env
    child = await atomic(env, "绘画活动的安排。")
    if not add_after_prepare:
        await memory.add_edge(nodes[0]["node_id"], child["node_id"], "contains")
    batch = await prepared(env, ids=[nodes[0]["node_id"], child["node_id"]])
    if add_after_prepare:
        await memory.add_edge(nodes[0]["node_id"], child["node_id"], "contains")
    else:
        await memory.database.execute("DELETE FROM memory_edges WHERE source_node_id=? AND target_node_id=?",
            (nodes[0]["node_id"], child["node_id"]))
    job = await begin(env)
    with pytest.raises(ValueError, match="topic member structure changed"):
        await store.apply(job, batch, {"changes": []})
    assert not await memory.database.fetchall("SELECT * FROM topic_receipts")
    assert not await memory.database.fetchall("SELECT * FROM topic_reviews")


async def test_old_prepared_protocol_does_not_reinterpret_member_structure(env):
    memory, store, _, _, nodes = env
    child = await atomic(env, "绘画活动的安排。")
    batch = await prepared(env, ids=[nodes[0]["node_id"], child["node_id"]])
    del batch["member_structure_version"]
    batch["input_digest"] = digest({key: value for key, value in batch.items() if key != "input_digest"})
    await memory.add_edge(nodes[0]["node_id"], child["node_id"], "contains")
    result = await store.apply(await begin(env), batch, {"changes": []})
    assert result["changed_node_ids"] == []


async def test_topic_membership_snapshot_and_updates_keep_complete_member_semantics(env):
    memory, store, _, _, nodes = env
    topic = await initial_topic(env)
    child = await atomic(env, "同一事件下的细节。")
    await memory.add_edge(nodes[0]["node_id"], child["node_id"], "contains")
    batch = await prepared(env, ids=[topic["node_id"], child["node_id"]], seeds=[child["node_id"]])
    assert len(batch["edges"]) == 4
    result = await store.apply(await begin(env), batch, output(nodes[:1], topic, content=topic["content_text"]))
    assert result["topic_ids"] == [topic["node_id"]]
    topic_edges = await memory.database.fetchall("SELECT target_node_id FROM memory_edges WHERE source_node_id=?",
        (topic["node_id"],))
    assert [item["target_node_id"] for item in topic_edges] == [nodes[0]["node_id"]]
    child_edge = await memory.database.fetchone("SELECT 1 FROM memory_edges WHERE source_node_id=? AND target_node_id=?",
        (nodes[0]["node_id"], child["node_id"]))
    assert child_edge is not None
