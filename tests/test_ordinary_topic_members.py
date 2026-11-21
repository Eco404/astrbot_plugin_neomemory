"""Ordinary extraction enforces the same topic creation floor as consolidation."""

import json

import pytest
import pytest_asyncio

from core.extraction_apply import ExtractionService
from core.prompts import schema_for_prompt_version
from core.extraction_errors import InvalidExtractionCandidate
from storage.memory_store import MemoryStore
from storage.source_store import SourceStore
from tests.test_extraction_apply import node
from tests.test_topic_extraction_dependencies import candidate, dependencies, edge, next_batch

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


def proposal(batch, nodes=(), edges=()):
    value = candidate(nodes, edges)
    value["schema_version"] = schema_for_prompt_version(batch.prompt_version)
    for item in value["node_changes"]:
        item["base_node_ids"] = []
    return value


async def assert_unconsumed(memory, batch):
    for table in ("extraction_batches", "extraction_batch_sources", "topic_dependencies"):
        assert not await memory.database.fetchall(f"SELECT * FROM {table}")
    generation = await memory.database.fetchone(
        "SELECT activity_round FROM memory_generations WHERE generation_id=?", (batch.generation_id,))
    assert generation["activity_round"] == batch.expected_round


@pytest.mark.parametrize("member_count", [0, 1])
async def test_new_topic_below_floor_rolls_back_all_candidate_writes(env, member_count):
    _, memory, service = env
    batch, source = await next_batch(env)
    changes = [node(source.source_id, ref="local_topic", kind="topic", content="持续推进社区绘画。")]
    changes.extend(node(source.source_id, ref=f"local_member_{i}", content="举行独立绘画活动。")
                   for i in range(member_count))
    payload = proposal(batch, changes, [edge(source.source_id, "local_topic", f"local_member_{i}")
                                 for i in range(member_count)])
    with pytest.raises(InvalidExtractionCandidate) as error:
        await service.apply(batch, json.dumps(payload), model_id="synthetic")
    assert error.value.code == "ordinary_topic_create_members"
    assert error.value.path == "$.node_changes[0]"
    for table in ("memory_nodes", "memory_edges", "memory_node_versions", "memory_derivations"):
        assert not await memory.database.fetchall(f"SELECT * FROM {table}")
    await assert_unconsumed(memory, batch)


@pytest.mark.parametrize("use_existing_member", [False, True])
async def test_new_topic_can_reference_new_and_known_members(env, use_existing_member):
    _, memory, service = env
    batch, source = await next_batch(env)
    existing = None
    if use_existing_member:
        existing = await memory.create_node(space_id=batch.space["space_id"], node_type="atomic",
            content_text="绘画计划以社区公益为目标。", source_ids=[source.source_id])
        batch, source = await next_batch(env, 2, (existing["node_id"],))
    changes = [node(source.source_id, ref="local_topic", kind="topic", content="持续推进社区绘画。"),
               node(source.source_id, ref="local_event", content="首次举行室外绘画活动。")]
    second_ref = existing["node_id"] if existing else "local_second"
    if not existing:
        changes.append(node(source.source_id, ref=second_ref, kind="atomic", content="计划以社区公益为目标。"))
    payload = proposal(batch, changes, [edge(source.source_id, "local_topic", ref)
                                 for ref in ("local_event", second_ref)])
    result = await service.apply(batch, json.dumps(payload), model_id="synthetic")
    refs = result["result"]["node_refs"]
    topic = await memory.get_node(refs["local_topic"])
    second_id = existing["node_id"] if existing else refs[second_ref]
    assert await dependencies(memory, topic) == {refs["local_event"]: 1, second_id: 1}


async def test_two_aliases_for_one_existing_member_do_not_meet_floor(env):
    _, memory, service = env
    batch, source = await next_batch(env)
    member = await memory.create_node(space_id=batch.space["space_id"], node_type="event",
        content_text="社区举办了绘画活动。", source_ids=[source.source_id])
    batch, source = await next_batch(env, 2, (member["node_id"],))
    payload = proposal(batch, [
        node(source.source_id, ref="local_topic", kind="topic", content="持续推进社区绘画。"),
        node(source.source_id, ref="local_member", old=member, content="社区绘画活动已经结束。"),
    ], [edge(source.source_id, "local_topic", ref) for ref in ("local_member", member["node_id"])])
    with pytest.raises(InvalidExtractionCandidate) as error:
        await service.apply(batch, json.dumps(payload), model_id="synthetic")
    assert error.value.code == "ordinary_topic_create_members"
    assert (await memory.get_node(member["node_id"]))["content_version"] == 1
    assert len(await memory.database.fetchall("SELECT * FROM memory_nodes")) == 1
    assert not await memory.database.fetchall("SELECT * FROM memory_edges")
    await assert_unconsumed(memory, batch)


async def test_existing_single_member_topic_can_be_updated(env):
    _, memory, service = env
    batch, source = await next_batch(env)
    topic = await memory.create_node(space_id=batch.space["space_id"], node_type="topic",
        content_text="社区绘画项目。", source_ids=[source.source_id])
    member = await memory.create_node(space_id=batch.space["space_id"], node_type="event",
        content_text="社区举办了绘画活动。", source_ids=[source.source_id])
    await memory.add_edge(topic["node_id"], member["node_id"], "contains")
    batch, source = await next_batch(env, 2, (topic["node_id"],))
    await service.apply(batch, json.dumps(proposal(batch, [
        node(source.source_id, ref="local_topic", kind="topic", old=topic, content="社区绘画项目继续推进。")
    ])), model_id="synthetic")
    assert (await memory.get_node(topic["node_id"]))["content_version"] == 2
