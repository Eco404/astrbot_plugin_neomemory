"""Ordinary and dedicated topic writes keep explicit member version evidence."""

import json
import uuid

import pytest
import pytest_asyncio

from core.extraction_apply import ExtractionService
from core.memory_spaces import private_space
from storage.job_store import JobStore
from storage.memory_store import MemoryStore
from storage.source_store import SourceStore
from storage.topic_store import TopicStore
from tests.test_extraction_apply import capture, node, output
from tests.test_topic_store import output as topic_output

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def env(tmp_path):
    sources = SourceStore(tmp_path / "source.db")
    memory = MemoryStore(tmp_path / "memory.db", source_exists=sources.has_source)
    await sources.open()
    await memory.open()
    try:
        yield sources, memory, ExtractionService(sources, memory)
    finally:
        await memory.close()
        await sources.close()


def candidate(nodes=(), edges=()):
    return {**output([{**item, "title": "社区活动", "emotion": None, "base_node_ids": []} for item in nodes], edges=edges),
            "schema_version": "extraction.v1", "coverage_changes": []}


def edge(source, topic, member):
    return {"from_ref": topic, "to_ref": member, "relation_type": "contains", "source_ids": [source]}


async def next_batch(env, number=1, known=()):
    sources, memory, service = env
    source = await capture(sources, "社区绘画计划开展了独立活动。", number=number)
    space = await memory.get_or_create_space(private_space("p", source.logical_user_id))
    batch = await service.prepare(space_id=space["space_id"], source_ids=(source.source_id,),
                                  persona="温和的合成陪伴者", known_node_ids=known)
    return batch, source


async def dependencies(memory, topic):
    rows = await memory.database.fetchall("SELECT * FROM topic_dependencies WHERE topic_id=?", (topic["node_id"],))
    return {row["member_id"]: row["member_version"] for row in rows}


async def create_topic(env):
    _, memory, service = env
    batch, source = await next_batch(env)
    payload = candidate([
        node(source.source_id, ref="local_first", content="在公园进行绘画活动。"),
        node(source.source_id, ref="local_second", content="在室内进行绘画活动。"),
        node(source.source_id, ref="local_topic", kind="topic", content="两次活动推动社区绘画计划。"),
    ], [edge(source.source_id, "local_topic", ref) for ref in ("local_first", "local_second")])
    result = await service.apply(batch, json.dumps(payload), model_id="synthetic")
    refs = result["result"]["node_refs"]
    return [await memory.get_node(refs[ref]) for ref in ("local_topic", "local_first", "local_second")]


async def test_ordinary_creation_records_members_and_precise_basis(env):
    _, memory, _ = env
    topic, first, second = await create_topic(env)
    assert await dependencies(memory, topic) == {first["node_id"]: 1, second["node_id"]: 1}
    derivations = await memory.list_version_derivations("node", topic["node_id"])
    assert {(base["entity_id"], base["version_no"]) for base in derivations[0]["base_versions"]} == {
        (first["node_id"], 1), (second["node_id"], 1)}
    assert len(derivations[0]["sources"]) == 1
    assert len(await memory.database.fetchall("SELECT * FROM extraction_batches")) == 1


async def test_ordinary_partial_update_keeps_untouched_member_basis(env):
    _, memory, service = env
    topic, first, second = await create_topic(env)
    revised = await memory.update_node_content(first["node_id"], "公园绘画活动后来完成收尾。")
    batch, source = await next_batch(env, 2, (topic["node_id"],))
    payload = candidate([
        node(source.source_id, ref="local_third", content="第三次活动在展馆进行。"),
        node(source.source_id, ref="local_topic", kind="topic", old=topic, content="社区绘画计划包含三次活动。"),
    ], [edge(source.source_id, "local_topic", "local_third")])
    result = await service.apply(batch, json.dumps(payload), model_id="synthetic")
    third_id = result["result"]["node_refs"]["local_third"]
    assert await dependencies(memory, topic) == {first["node_id"]: 1, second["node_id"]: 1, third_id: 1}
    derivation = (await memory.list_version_derivations("node", topic["node_id"], 2))[0]
    assert {(base["entity_id"], base["version_no"]) for base in derivation["base_versions"]} == {
        (topic["node_id"], 1), (third_id, 1)}
    # The existing topic base preserves history; it does not pretend that a
    # currently visible updated member was explicitly used in this synthesis.
    assert revised["content_version"] == 2


async def test_edge_only_is_membership_not_content_provenance(env):
    _, memory, service = env
    batch, source = await next_batch(env)
    topic = await memory.create_node(space_id=batch.space["space_id"], node_type="topic",
        content_text="社区绘画计划仍在继续。", source_ids=[source.source_id])
    await memory.database.execute("DELETE FROM memory_derivations WHERE node_version_id=?", (topic["node_version_id"],))
    member = await memory.create_node(space_id=batch.space["space_id"], node_type="event",
        content_text="举行了一次独立绘画活动。", source_ids=[source.source_id])
    batch, source = await next_batch(env, 2, (topic["node_id"], member["node_id"]))
    await service.apply(batch, json.dumps(candidate(edges=[edge(source.source_id, topic["node_id"], member["node_id"])])),
                        model_id="synthetic")
    assert await memory.get_node(topic["node_id"]) == topic
    assert await dependencies(memory, topic) == {member["node_id"]: 1}
    version = (await memory.list_node_versions(topic["node_id"]))[0]
    assert version["provenance_status"] == "unrecorded"
    operation = version["derivations"][0]
    assert operation["operation"] == "membership" and operation["base_versions"] == []
    assert operation["semantic"]["member_versions"] == [{"kind": "node", "entity_id": member["node_id"], "version_no": 1}]


async def test_same_batch_member_update_uses_final_explicit_edge_version(env):
    _, memory, service = env
    topic, first, _ = await create_topic(env)
    batch, source = await next_batch(env, 2, (topic["node_id"],))
    payload = candidate([
        node(source.source_id, ref="local_first", old=first, content="公园绘画活动已经结束。"),
        node(source.source_id, ref="local_topic", kind="topic", old=topic, content="社区绘画计划有一项活动完成。"),
    ], [edge(source.source_id, "local_topic", "local_first")])
    await service.apply(batch, json.dumps(payload), model_id="synthetic")
    assert (await dependencies(memory, topic))[first["node_id"]] == 2
    basis = (await memory.list_version_derivations("node", topic["node_id"], 2))[0]["base_versions"]
    assert {"kind": "node", "entity_id": first["node_id"], "version_no": 2} in basis


async def test_no_change_does_not_invent_missing_basis_but_explicit_confirmation_records_it(env):
    _, memory, _ = env
    topic, first, second = await create_topic(env)
    await memory.database.execute("DELETE FROM topic_dependencies WHERE topic_id=?", (topic["node_id"],))
    before = await memory.list_version_derivations("node", topic["node_id"])
    store, jobs = TopicStore(memory), JobStore(memory.database)
    await jobs.submit("topic_consolidate", topic["space_id"], {}, uuid.uuid4().hex)
    job = await jobs.claim("test", lease_seconds=300)
    batch = await store.prepare(topic["space_id"], topic["generation_id"], [topic["node_id"]])
    await store.apply(job, batch, {"changes": []})
    assert await dependencies(memory, topic) == {}
    assert await memory.list_version_derivations("node", topic["node_id"]) == before
    await jobs.finish(job["job_id"], job["lease_token"])
    await jobs.submit("topic_consolidate", topic["space_id"], {}, uuid.uuid4().hex)
    job = await jobs.claim("test", lease_seconds=300)
    change = topic_output([first, second], topic, content=topic["content_text"])
    change["changes"][0]["title"] = topic["title"]
    await store.apply(job, batch, change)
    assert await dependencies(memory, topic) == {first["node_id"]: 1, second["node_id"]: 1}
    assert (await memory.get_node(topic["node_id"]))["content_version"] == 1
    assert len(await memory.list_version_derivations("node", topic["node_id"])) == 2


async def test_invalid_edge_rolls_back_dependencies_and_all_node_writes(env):
    _, memory, service = env
    batch, source = await next_batch(env)
    payload = candidate([
        node(source.source_id, ref="local_event", content="独立活动。"),
        node(source.source_id, ref="local_topic", kind="topic", content="持续计划。"),
    ], [edge(source.source_id, "local_topic", "local_event"),
        edge(source.source_id, "local_event", "local_topic")])
    with pytest.raises(ValueError, match="contains hierarchy"):
        await service.apply(batch, json.dumps(payload), model_id="synthetic")
    assert not await memory.database.fetchall("SELECT * FROM topic_dependencies")
    assert not await memory.database.fetchall("SELECT * FROM memory_nodes")
    assert not await memory.database.fetchall("SELECT * FROM extraction_batches")


@pytest.mark.parametrize("update_member", [False, True])
async def test_explicit_member_basis_uses_prepared_version_without_rewriting_unmentioned_members(env, update_member):
    sources, memory, _ = env
    topic, first, second = await create_topic(env)
    first = await memory.update_node_content(first["node_id"], "公园活动新增了一次讨论。")
    if not update_member:
        await memory.database.execute("UPDATE memory_nodes SET manual_protected=1 WHERE node_id=?", (first["node_id"],))
    service = ExtractionService(sources, memory)
    batch, source = await next_batch((sources, memory, service), 2, (topic["node_id"],))
    changes = [node(source.source_id, ref="local_topic", kind="topic", old=topic,
                    content="社区绘画计划增加讨论内容。" if update_member else topic["content_text"])]
    if update_member:
        changes.append(node(source.source_id, ref="local_first", old=first, content="公园讨论已经结束。"))
    payload = candidate(changes)
    payload["schema_version"] = "extraction.v1"
    for item in payload["node_changes"]:
        item["base_node_ids"] = [first["node_id"]] if item["node_type"] == "topic" else []
    await service.apply(batch, json.dumps(payload), model_id="synthetic")
    assert await dependencies(memory, topic) == {first["node_id"]: 2, second["node_id"]: 1}
    records = await memory.list_version_derivations("node", topic["node_id"])
    if update_member:
        assert (await memory.get_node(first["node_id"]))["content_version"] == 3
        assert {"kind": "node", "entity_id": first["node_id"], "version_no": 2} in records[-1]["base_versions"]
    else:
        assert (await memory.get_node(topic["node_id"]))["content_version"] == 1
        membership = next(record for record in records if record["operation"] == "membership")
        assert membership["base_versions"] == []
        assert membership["semantic"]["member_versions"] == [
            {"kind": "node", "entity_id": first["node_id"], "version_no": 2}]
