"""Topic synthesis preserves event identities and records its versioned basis."""

import json
import time
import uuid

import pytest
import pytest_asyncio

from core.memory_spaces import private_space
from storage.job_store import JobStore, LeaseLost
from storage.memory_store import MemoryStore
from storage.pipeline_store import PipelineStore
from storage.topic_store import TopicStore

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def env(tmp_path):
    memory = MemoryStore(tmp_path / "memory.db")
    await memory.open()
    space = await memory.get_or_create_space(private_space("p", "u"))
    nodes = [await memory.create_node(space_id=space["space_id"], node_type="event",
             content_text=f"独立活动 {i}", source_ids=[f"source-{i}"], importance=0.6)
             for i in range(3)]
    yield memory, TopicStore(memory), JobStore(memory.database), space, nodes
    await memory.close()


async def begin(env):
    _, _, jobs, space, _ = env
    await jobs.submit("topic_consolidate", space["space_id"], {}, uuid.uuid4().hex)
    return await jobs.claim("test", lease_seconds=300)


async def prepared(env, *, ids=None, seeds=None):
    _, store, _, space, nodes = env
    return await store.prepare(space["space_id"], nodes[0]["generation_id"],
                               ids or [node["node_id"] for node in nodes], seed_ids=seeds)


def output(nodes, old=None, *, content="几次独立活动共同构成持续的社区绘画计划。"):
    return {"schema_version": "topic-consolidation.v1", "changes": [{
        "operation": "update" if old else "create", "topic_id": old["node_id"] if old else None,
        "expected_version": old["content_version"] if old else None, "title": "社区绘画计划",
        "content": content, "importance": 0.6, "emotion": None,
        "member_ids": [node["node_id"] for node in nodes]}]}


async def initial_topic(env):
    memory, store, jobs, _, nodes = env
    job = await begin(env)
    result = await store.apply(job, await prepared(env), output(nodes))
    await jobs.finish(job["job_id"], job["lease_token"])
    return await memory.get_node(result["topic_ids"][0])


async def test_create_basis_sources_receipt_and_events_unchanged(env):
    memory, store, _, space, nodes = env
    job = await begin(env)
    batch = await prepared(env)
    result = await store.apply(job, batch, output(nodes))
    repeated = await store.apply(job, batch, output(nodes))
    assert repeated["replayed"] and repeated["topic_ids"] == result["topic_ids"]
    assert result["changed_node_ids"] == result["topic_ids"]
    topic = await memory.get_node(result["topic_ids"][0])
    assert topic["node_type"] == "topic" and topic["content_version"] == 1
    for node in nodes:
        current = await memory.get_node(node["node_id"])
        assert current == node
    derivations = await memory.list_version_derivations("node", topic["node_id"])
    assert {basis["entity_id"] for basis in derivations[0]["base_versions"]} == {n["node_id"] for n in nodes}
    assert derivations[0]["sources"] == []  # Member versions, not invented new conversation evidence.
    assert len(await memory.database.fetchall("SELECT * FROM node_sources WHERE node_id=?", (topic["node_id"],))) == 3
    assert (await memory.get_active_generation(space["space_id"]))["activity_round"] == 0
    assert not await memory.database.fetchall("SELECT * FROM extraction_batch_sources")
    assert not await store.dirty_nodes(space["space_id"], nodes[0]["generation_id"])


async def test_prepare_expands_parent_and_all_members(env):
    _, _, _, _, nodes = env
    topic = await initial_topic(env)
    batch = await prepared(env, ids=[nodes[0]["node_id"]], seeds=[nodes[0]["node_id"]])
    assert {n["node_id"] for n in batch["nodes"]} == {topic["node_id"], *(n["node_id"] for n in nodes)}
    assert len(batch["edges"]) == 3
    assert batch["seed_ids"] == [nodes[0]["node_id"]]


async def test_membership_only_change_does_not_reset_content_or_strength(env):
    memory, store, _, _, nodes = env
    topic = await initial_topic(env)
    job = await begin(env)
    batch = await prepared(env, ids=[topic["node_id"]], seeds=[])
    result = await store.apply(job, batch, output(nodes[:1], topic, content=topic["content_text"]))
    current = await memory.get_node(topic["node_id"])
    assert result["changed_node_ids"] == []
    for field in ("content_version", "updated_at", "strength_reset_at", "strength_reset_round", "reinforcement"):
        assert current[field] == topic[field]
    edges = await memory.database.fetchall("SELECT * FROM memory_edges WHERE source_node_id=?", (topic["node_id"],))
    assert [row["target_node_id"] for row in edges] == [nodes[0]["node_id"]]
    dependencies = await memory.database.fetchall("SELECT * FROM topic_dependencies WHERE topic_id=?", (topic["node_id"],))
    assert len(dependencies) == 1


async def test_content_update_keeps_identity_and_updates_dependency(env):
    memory, store, _, _, nodes = env
    topic = await initial_topic(env)
    revised = await memory.update_node_content(nodes[0]["node_id"], content_text="活动有新进展")
    batch = await prepared(env, ids=[revised["node_id"]], seeds=[revised["node_id"]])
    job = await begin(env)
    result = await store.apply(job, batch, output(nodes, topic, content="共同计划有了新的进展。"))
    assert result["topic_ids"] == [topic["node_id"]]
    current = await memory.get_node(topic["node_id"])
    assert current["content_version"] == 2
    dependency = await memory.database.fetchone("SELECT * FROM topic_dependencies WHERE topic_id=? AND member_id=?",
                                               (topic["node_id"], revised["node_id"]))
    assert dependency["member_version"] == revised["content_version"]


async def test_no_change_reviews_seeds_only_and_new_versions_become_dirty(env):
    memory, store, jobs, space, nodes = env
    job = await begin(env)
    batch = await prepared(env, seeds=[nodes[0]["node_id"]])
    await store.apply(job, batch, {"changes": []})
    await jobs.finish(job["job_id"], job["lease_token"])
    dirty = await store.dirty_nodes(space["space_id"], nodes[0]["generation_id"])
    assert {n["node_id"] for n in dirty} == {n["node_id"] for n in nodes[1:]}
    await memory.update_node_content(nodes[0]["node_id"], content_text="新的真实进展")
    dirty = await store.dirty_nodes(space["space_id"], nodes[0]["generation_id"])
    assert len(dirty) == 3
    assert next(n for n in dirty if n["node_id"] == nodes[0]["node_id"])["processed_at"] is not None
    assert not await store.dirty_nodes(space["space_id"], nodes[0]["generation_id"], before=0)


@pytest.mark.parametrize("mutation", ["content", "protected", "stale"])
async def test_no_change_rejects_changed_snapshot(env, mutation):
    memory, store, _, _, nodes = env
    batch = await prepared(env)
    if mutation == "content":
        await memory.update_node_content(nodes[0]["node_id"], content_text="新的真实进展")
    elif mutation == "protected":
        await memory.database.execute("UPDATE memory_nodes SET manual_protected=1 WHERE node_id=?", (nodes[0]["node_id"],))
    else:
        await memory.database.execute("UPDATE memory_nodes SET metadata_json=? WHERE node_id=?",
                                      (json.dumps({"summary_stale": True}), nodes[0]["node_id"]))
    job = await begin(env)
    with pytest.raises(ValueError, match="context changed"):
        await store.apply(job, batch, {"changes": []})
    assert not await memory.database.fetchall("SELECT * FROM topic_reviews")


@pytest.mark.parametrize("state", ["protected", "stale"])
async def test_protected_or_stale_topic_cannot_be_automatically_revised(env, state):
    memory, store, _, _, nodes = env
    topic = await initial_topic(env)
    if state == "protected":
        await memory.database.execute("UPDATE memory_nodes SET manual_protected=1 WHERE node_id=?", (topic["node_id"],))
    else:
        await memory.database.execute("UPDATE memory_nodes SET metadata_json=? WHERE node_id=?",
                                      ('{"summary_stale":true}', topic["node_id"]))
    batch = await prepared(env)
    job = await begin(env)
    with pytest.raises(PermissionError):
        await store.apply(job, batch, output(nodes, topic, content="不应写入"))
    assert (await memory.get_node(topic["node_id"]))["content_version"] == 1


async def test_bad_member_rolls_back_entire_batch(env):
    memory, store, _, _, nodes = env
    batch = await prepared(env)
    job = await begin(env)
    candidate = output(nodes)
    candidate["changes"].extend(output(nodes)["changes"])
    candidate["changes"][1]["member_ids"] = [nodes[0]["node_id"], "unknown"]
    with pytest.raises(ValueError, match="member"):
        await store.apply(job, batch, candidate)
    assert not await memory.database.fetchall("SELECT * FROM memory_nodes WHERE node_type='topic'")
    assert not await memory.database.fetchall("SELECT * FROM topic_reviews")
    assert not await memory.database.fetchall("SELECT * FROM topic_receipts")


async def test_lost_lease_cannot_commit(env):
    _, store, jobs, _, nodes = env
    batch = await prepared(env)
    job = await begin(env)
    await jobs.release(job["job_id"], job["lease_token"])
    with pytest.raises(LeaseLost):
        await store.apply(job, batch, output(nodes))


async def test_online_reinforcement_does_not_invalidate_semantic_input(env):
    memory, store, _, _, nodes = env
    batch = await prepared(env)
    await memory.database.execute("UPDATE memory_nodes SET reinforcement=0.1,last_recalled_at=10 WHERE node_id=?",
                                  (nodes[0]["node_id"],))
    job = await begin(env)
    assert (await store.apply(job, batch, output(nodes)))["topic_ids"]


async def test_count_dirty_matches_selection_and_cutoff(env):
    _, store, _, space, nodes = env
    assert await store.count_dirty(space["space_id"], nodes[0]["generation_id"]) == 3
    assert await store.count_dirty(space["space_id"], nodes[0]["generation_id"], before=0) == 0


async def test_unknown_user_reference_is_not_committed(env):
    memory, store, _, _, nodes = env
    job = await begin(env)
    with pytest.raises(ValueError, match="logical user"):
        await store.apply(job, await prepared(env), output(nodes, content="{{user:unknown}}的持续项目。"))
    assert not await memory.database.fetchall("SELECT * FROM topic_receipts")


async def test_multiple_topics_can_share_a_member(env):
    memory, store, _, _, nodes = env
    job = await begin(env)
    candidate = output(nodes[:2])
    candidate["changes"].extend(output(nodes[1:])["changes"])
    result = await store.apply(job, await prepared(env), candidate)
    assert len(result["topic_ids"]) == 2
    links = await memory.database.fetchall("SELECT * FROM memory_edges WHERE target_node_id=?", (nodes[1]["node_id"],))
    assert len(links) == 2


async def test_protected_member_cannot_gain_or_lose_a_relation(env):
    memory, store, _, _, nodes = env
    await memory.database.execute("UPDATE memory_nodes SET manual_protected=1 WHERE node_id=?", (nodes[0]["node_id"],))
    job = await begin(env)
    with pytest.raises(PermissionError, match="protected member"):
        await store.apply(job, await prepared(env), output(nodes))


async def test_rebuild_can_synthesize_only_into_its_owned_generation(env):
    memory, store, jobs, space, nodes = env
    await jobs.submit("rebuild", space["space_id"], {}, uuid.uuid4().hex)
    job = await jobs.claim("test", lease_seconds=300)
    generation = await PipelineStore(memory).begin_rebuild(job, space["space_id"], nodes[0]["generation_id"])
    members = [await memory.create_node(space_id=space["space_id"], generation_id=generation["generation_id"],
                node_type="event", content_text=f"新的重放活动{i}", source_ids=[f"replay-{i}"]) for i in range(2)]
    batch = await store.prepare(space["space_id"], generation["generation_id"], [n["node_id"] for n in members])
    result = await store.apply(job, batch, output(members))
    assert (await memory.get_node(result["topic_ids"][0]))["generation_id"] == generation["generation_id"]
    assert (await memory.get_active_generation(space["space_id"]))["generation_id"] == nodes[0]["generation_id"]


async def test_manual_rereview_keeps_unchanged_topic_and_exits_after_no_change(env):
    memory, store, jobs, space, nodes = env
    topic = await initial_topic(env)
    before_topic = await memory.get_node(topic["node_id"])
    before_derivations = await memory.list_version_derivations("node", topic["node_id"])
    generation_id = nodes[0]["generation_id"]
    assert await store.count_dirty(space["space_id"], generation_id) == 0
    start = time.time()
    selected = await store.dirty_nodes(space["space_id"], generation_id, before=start, review_before=start)
    assert len(selected) == await store.count_dirty(space["space_id"], generation_id, before=start, review_before=start) == 3
    job = await begin(env)
    batch = await store.prepare(space["space_id"], generation_id, [node["node_id"] for node in selected])
    result = await store.apply(job, batch, {"changes": []})
    await jobs.finish(job["job_id"], job["lease_token"])
    assert result["reviewed_count"] == 3 and result["changed_node_ids"] == []
    assert await store.count_dirty(space["space_id"], generation_id, before=start, review_before=start) == 0
    assert not await store.dirty_nodes(space["space_id"], generation_id, before=start, review_before=start)
    assert await memory.get_node(topic["node_id"]) == before_topic
    assert await memory.list_version_derivations("node", topic["node_id"]) == before_derivations
    assert await store.count_dirty(space["space_id"], generation_id) == 0
    reviews = await memory.database.fetchall("SELECT processed_at FROM topic_reviews")
    assert all(row["processed_at"] >= start for row in reviews)


async def test_manual_rereview_still_respects_frozen_node_cutoff(env):
    memory, store, _, space, nodes = env
    await initial_topic(env)
    start = time.time()
    await memory.update_node_content(nodes[0]["node_id"], "整理开始后出现的实质变化。")
    selected = await store.dirty_nodes(space["space_id"], nodes[0]["generation_id"],
                                       before=start, review_before=start)
    assert {node["node_id"] for node in selected} == {node["node_id"] for node in nodes[1:]}
    assert await store.count_dirty(space["space_id"], nodes[0]["generation_id"], before=start, review_before=start) == 2
