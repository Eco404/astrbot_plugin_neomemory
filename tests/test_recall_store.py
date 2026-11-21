"""Scope, version, transaction and reinforcement guarantees for recall storage."""

import asyncio
import math
import sqlite3

import pytest
import pytest_asyncio

from core.memory_spaces import private_space
from storage.memory_store import MemoryStore
from storage.recall_store import RECALL_LEDGER_RETENTION_SECONDS


@pytest_asyncio.fixture
async def recall_store(tmp_path):
    store = MemoryStore(tmp_path / "memory.db")
    await store.open()
    space = await store.get_or_create_space(private_space("persona", "user"))
    generation = await store.get_active_generation(space["space_id"])
    yield store, space["space_id"], generation["generation_id"]
    await store.close()


async def node(store, space_id, text="记得一起旅行。", **kwargs):
    return await store.create_node(
        space_id=space_id,
        node_type=kwargs.pop("node_type", "atomic"),
        content_text=text,
        source_ids=["source"],
        **kwargs,
    )


def hit(item, *, direct=True):
    return {"node_id": item["node_id"], "content_version": item["content_version"], "direct": direct}


@pytest.mark.asyncio
async def test_snapshot_scopes_nodes_edges_and_coverage(recall_store):
    store, space_id, generation_id = recall_store
    atomic = await node(store, space_id)
    event = await node(store, space_id, node_type="event")
    historical = await node(store, space_id, status="historical")
    disabled = await node(store, space_id, status="disabled")
    for status in ("superseded", "deleted"):
        await node(store, space_id, status=status)
    await store.add_edge(event["node_id"], atomic["node_id"], "contains")
    await store.add_edge(event["node_id"], disabled["node_id"], "related")
    await store.add_coverage(event["node_id"], atomic["node_id"])
    other = await store.get_or_create_space(private_space("persona", "other"))
    await node(store, other["space_id"], importance=1.0)
    building = await store.create_generation(space_id)
    await node(store, space_id, generation_id=building["generation_id"])

    result = await store.retrieval_snapshot(space_id)
    assert result["generation_id"] == generation_id
    assert result["activity_round"] == 0
    assert result["truncated"] is False
    assert {item["node_id"] for item in result["nodes"]} == {atomic["node_id"], event["node_id"]}
    assert result["nodes"][0]["content_text"]
    assert "strength_reset_at" in result["nodes"][0]
    assert len(result["edges"]) == len(result["coverage"]) == 1
    assert result["coverage"][0]["event_content_version"] == 1
    active = await store.retrieval_snapshot(space_id, include_historical=True)
    assert {item["node_id"] for item in active["nodes"]} == {
        atomic["node_id"], event["node_id"], historical["node_id"],
    }
    assert (await store.retrieval_snapshot("absent"))["nodes"] == []


@pytest.mark.asyncio
async def test_snapshot_cap_is_explicit_and_prefers_importance(recall_store):
    store, space_id, _ = recall_store
    important = await node(store, space_id, importance=0.9)
    await node(store, space_id, importance=0.1)
    result = await store.retrieval_snapshot(space_id, limit=1)
    assert result["truncated"] is True
    assert [item["node_id"] for item in result["nodes"]] == [important["node_id"]]
    with pytest.raises(ValueError):
        await store.retrieval_snapshot(space_id, limit=2001)


@pytest.mark.asyncio
async def test_stale_summary_cannot_recall_suppress_or_reinforce(recall_store):
    store, space_id, generation_id = recall_store
    atomic = await node(store, space_id, "正确的独立细节", importance=0.5)
    event = await node(store, space_id, "需要人工复核的旧概括", node_type="event", importance=1.0)
    await store.add_edge(event["node_id"], atomic["node_id"], "contains")
    await store.add_coverage(event["node_id"], atomic["node_id"])
    original = await store.retrieval_snapshot(space_id)
    assert len(original["nodes"]) == 2 and original["coverage"]
    await store.database.execute("UPDATE memory_nodes SET metadata_json=? WHERE node_id=?",
                                 ('{"summary_stale":true,"summary_stale_job_id":"governance"}', event["node_id"]))
    result = await store.retrieval_snapshot(space_id, limit=1)
    assert [item["node_id"] for item in result["nodes"]] == [atomic["node_id"]]
    assert result["edges"] == result["coverage"] == [] and not result["truncated"]
    for active in (False, True):
        valid = await store.revalidate_recall(space_id, generation_id,
                                             {event["node_id"]: 1, atomic["node_id"]: 1}, include_historical=active)
        assert [item["node_id"] for item in valid] == [atomic["node_id"]]
        assert await store.reinforce_recall(space_id, generation_id, f"stale-{active}", [hit(event)], active=active) == []
    assert (await store.get_node(event["node_id"]))["metadata"]["summary_stale"] is True
    await store.database.execute("UPDATE memory_nodes SET metadata_json=? WHERE node_id=?",
                                 ('{"summary_stale":false}', event["node_id"]))
    assert [item["node_id"] for item in (await store.retrieval_snapshot(space_id, limit=1))["nodes"]] == [event["node_id"]]


@pytest.mark.asyncio
async def test_index_selection_matches_recall_order_and_universe(recall_store):
    store, space_id, _ = recall_store
    earlier = await node(store, space_id, importance=0.6, node_id="node-a")
    later = await node(store, space_id, importance=0.6, node_id="node-b")
    historical = await node(store, space_id, importance=0.9, status="historical")
    await node(store, space_id, importance=1, metadata={"summary_stale": True})
    await node(store, space_id, importance=1, status="disabled")
    await store.database.execute("UPDATE memory_nodes SET updated_at=100 WHERE node_id=?", (earlier["node_id"],))
    await store.database.execute("UPDATE memory_nodes SET updated_at=200 WHERE node_id=?", (later["node_id"],))
    index = await store.index_nodes(space_id, limit=2)
    active = await store.retrieval_snapshot(space_id, include_historical=True, limit=2)
    assert index["nodes"] == active["nodes"]
    assert [item["node_id"] for item in index["nodes"]] == [historical["node_id"], later["node_id"]]
    passive = await store.retrieval_snapshot(space_id, limit=2)
    assert [item["node_id"] for item in passive["nodes"]] == [later["node_id"]]
    assert passive["truncated"] and index["truncated"]
    assert (await store.index_nodes(space_id, include_historical=False, limit=2))["nodes"] == passive["nodes"]
    # Passive filtering must not pull a node beyond the actual indexed universe.
    capped = await store.retrieval_snapshot(space_id, limit=1)
    assert capped["nodes"] == [] and capped["truncated"]
    # Stable node ID resolves exact importance/update-time ties.
    await store.database.execute("UPDATE memory_nodes SET updated_at=100 WHERE node_id=?", (later["node_id"],))
    full = await store.index_nodes(space_id)
    assert [item["node_id"] for item in full["nodes"]] == [historical["node_id"], earlier["node_id"], later["node_id"]]


@pytest.mark.asyncio
async def test_index_can_target_building_but_not_foreign_retired_or_failed_generation(recall_store):
    store, space_id, old_generation_id = recall_store
    await node(store, space_id)
    building = await store.create_generation(space_id)
    replacement = await node(store, space_id, generation_id=building["generation_id"])
    target = await store.index_nodes(space_id, generation_id=building["generation_id"])
    assert target["generation_id"] == building["generation_id"]
    assert [item["node_id"] for item in target["nodes"]] == [replacement["node_id"]]
    other = await store.get_or_create_space(private_space("persona", "other-index-scope"))
    other_generation = await store.get_active_generation(other["space_id"])
    with pytest.raises(KeyError):
        await store.index_nodes(space_id, generation_id=other_generation["generation_id"])
    failed = await store.create_generation(space_id, status="failed")
    with pytest.raises(KeyError):
        await store.index_nodes(space_id, generation_id=failed["generation_id"])
    await store.activate_generation(space_id, building["generation_id"])
    with pytest.raises(KeyError):
        await store.index_nodes(space_id, generation_id=old_generation_id)
    assert (await store.index_nodes("missing"))["nodes"] == []
    for limit in (0, 2001, True, 1.5):
        with pytest.raises(ValueError):
            await store.index_nodes(space_id, limit=limit)


@pytest.mark.asyncio
async def test_coverage_tracks_both_content_versions_and_can_refresh(recall_store):
    store, space_id, _ = recall_store
    atomic = await node(store, space_id, "旧细节")
    event = await node(store, space_id, "含旧细节的事件", node_type="event")
    assert await store.add_coverage(event["node_id"], atomic["node_id"])
    assert not await store.add_coverage(event["node_id"], atomic["node_id"])
    await store.update_node_content(event["node_id"], "新事件正文")
    assert (await store.retrieval_snapshot(space_id))["coverage"] == []
    assert await store.add_coverage(event["node_id"], atomic["node_id"])
    coverage = (await store.retrieval_snapshot(space_id))["coverage"][0]
    assert coverage["event_content_version"] == 2
    await store.update_node_content(atomic["node_id"], "新细节")
    assert (await store.retrieval_snapshot(space_id))["coverage"] == []
    assert await store.add_coverage(event["node_id"], atomic["node_id"], coverage_kind="partial")
    coverage = (await store.retrieval_snapshot(space_id))["coverage"][0]
    assert coverage["atomic_content_version"] == 2
    assert coverage["coverage_kind"] == "partial"
    with pytest.raises(ValueError, match="event content version"):
        await store.add_coverage(event["node_id"], atomic["node_id"], event_content_version=9)
    with pytest.raises(ValueError, match="atomic content version"):
        await store.add_coverage(event["node_id"], atomic["node_id"], atomic_content_version=True)
    # Storing a real historical assertion is allowed, but it cannot suppress now.
    assert await store.add_coverage(event["node_id"], atomic["node_id"], event_content_version=1)
    assert (await store.retrieval_snapshot(space_id))["coverage"] == []


@pytest.mark.asyncio
async def test_revalidate_excludes_disabled_changed_and_wrong_generation(recall_store):
    store, space_id, generation_id = recall_store
    good = await node(store, space_id)
    disabled = await node(store, space_id)
    changed = await node(store, space_id)
    historical = await node(store, space_id, status="historical")
    other = await store.get_or_create_space(private_space("persona", "other"))
    foreign = await node(store, other["space_id"])
    versions = {item["node_id"]: 1 for item in (good, disabled, changed, historical, foreign)}
    await store.update_node_status(disabled["node_id"], "disabled")
    await store.update_node_content(changed["node_id"], "正文已被修正")
    valid = await store.revalidate_recall(space_id, generation_id, versions)
    assert [item["node_id"] for item in valid] == [good["node_id"]]
    active = await store.revalidate_recall(space_id, generation_id, versions, include_historical=True)
    assert [item["node_id"] for item in active] == [good["node_id"], historical["node_id"]]
    replacement = await store.create_generation(space_id)
    await store.activate_generation(space_id, replacement["generation_id"])
    assert await store.revalidate_recall(space_id, generation_id, versions, include_historical=True) == []
    assert (await store.retrieval_snapshot(space_id))["generation_id"] == replacement["generation_id"]


@pytest.mark.asyncio
async def test_reinforcement_weights_do_not_invalidate_content_snapshot(recall_store):
    store, space_id, generation_id = recall_store
    direct, neighbor = await node(store, space_id), await node(store, space_id)
    credited = await store.reinforce_recall(
        space_id, generation_id, "turn", [hit(direct), hit(neighbor, direct=False)], now=1000,
    )
    assert [item["increment"] for item in credited] == pytest.approx([0.01, 0.0025])
    current = await store.get_node(direct["node_id"])
    assert current["last_recalled_at"] == 1000
    for field in ("updated_at", "strength_reset_at", "strength_reset_round", "content_version", "strength"):
        assert current[field] == direct[field]
    assert (await store.get_active_generation(space_id))["activity_round"] == 0
    active = await store.reinforce_recall(
        space_id, generation_id, "next", [hit(direct), hit(neighbor, direct=False)], now=1300, active=True,
    )
    assert [item["increment"] for item in active] == pytest.approx([0.0196, 0.004975])


@pytest.mark.asyncio
async def test_same_turn_dedup_survives_interleaving_restart_and_mode(recall_store):
    store, space_id, generation_id = recall_store
    item = await node(store, space_id)
    assert await store.reinforce_recall(space_id, generation_id, "first", [hit(item)], now=1000)
    assert await store.reinforce_recall(space_id, generation_id, "second", [hit(item)], now=1300)
    await store.close()
    await store.open()
    assert await store.reinforce_recall(space_id, generation_id, "first", [hit(item)], now=1600, active=True) == []
    assert (await store.get_node(item["node_id"]))["reinforcement"] == pytest.approx(0.0198)


@pytest.mark.asyncio
async def test_cooldown_has_zero_gain_and_each_turn_only_decides_once(recall_store):
    store, space_id, generation_id = recall_store
    item = await node(store, space_id)
    await store.reinforce_recall(space_id, generation_id, "one", [hit(item)], now=1000)
    assert await store.reinforce_recall(space_id, generation_id, "two", [hit(item)], now=1001) == []
    assert await store.reinforce_recall(space_id, generation_id, "two", [hit(item)], now=1301) == []
    assert await store.reinforce_recall(space_id, generation_id, "three", [hit(item)], now=1301)
    assert (await store.get_node(item["node_id"]))["reinforcement"] == pytest.approx(0.0198)


@pytest.mark.asyncio
async def test_reinforcement_budget_shared_between_calls_and_duplicate_hits(recall_store):
    store, space_id, generation_id = recall_store
    items = [await node(store, space_id) for _ in range(8)]
    first = await store.reinforce_recall(
        space_id, generation_id, "turn", [hit(items[0], direct=False), hit(items[0])], now=1000, active=True,
    )
    second = await store.reinforce_recall(
        space_id, generation_id, "turn", [hit(item) for item in items], now=1001, active=True,
    )
    assert first[0]["increment"] == 0.02
    assert sum(item["increment"] for item in first + second) == pytest.approx(0.12)
    assert len(await store.database.fetchall("SELECT * FROM recall_reinforcements")) == 8
    assert await store.reinforce_recall(
        space_id, generation_id, "turn", [hit(items[-1])], now=2000,
    ) == []


@pytest.mark.asyncio
async def test_cumulative_gain_cap_and_new_content_version_reset(recall_store):
    store, space_id, generation_id = recall_store
    item = await node(store, space_id)
    for index in range(40):
        await store.reinforce_recall(
            space_id, generation_id, str(index), [hit(item)], now=1000 + index * 300,
        )
    assert (await store.get_node(item["node_id"]))["reinforcement"] == pytest.approx(0.2)
    updated = await store.update_node_content(item["node_id"], "新经历内容")
    assert updated["reinforcement"] == 0
    # Old version cannot credit; same turn can credit a newly returned new version.
    assert await store.reinforce_recall(space_id, generation_id, "39", [hit(item)], now=12701) == []
    assert await store.reinforce_recall(space_id, generation_id, "39", [hit(updated)], now=12701)


@pytest.mark.asyncio
async def test_reinforcement_never_crosses_scope_lifecycle_or_generation(recall_store):
    store, space_id, generation_id = recall_store
    good = await node(store, space_id)
    historical = await node(store, space_id, status="historical")
    disabled = await node(store, space_id, status="disabled")
    deleted = await node(store, space_id, status="deleted")
    superseded = await node(store, space_id, status="superseded")
    other_space = await store.get_or_create_space(private_space("persona", "other"))
    other = await node(store, other_space["space_id"])
    hits = [hit(item) for item in (good, historical, disabled, deleted, superseded, other)]
    passive = await store.reinforce_recall(space_id, generation_id, "passive", hits, now=1000)
    assert [item["node_id"] for item in passive] == [good["node_id"]]
    active = await store.reinforce_recall(space_id, generation_id, "active", hits, now=1300, active=True)
    assert [item["node_id"] for item in active] == [good["node_id"], historical["node_id"]]
    assert (await store.get_node(historical["node_id"]))["status"] == "historical"
    new_generation = await store.create_generation(space_id)
    await store.activate_generation(space_id, new_generation["generation_id"])
    assert await store.reinforce_recall(space_id, generation_id, "retired", hits, now=1600, active=True) == []


@pytest.mark.asyncio
async def test_reinforcement_transaction_rolls_back_ledger_with_node_failure(recall_store):
    store, space_id, generation_id = recall_store
    item = await node(store, space_id)
    await store.database.execute(
        "CREATE TRIGGER reject_recall BEFORE UPDATE OF reinforcement ON memory_nodes "
        "BEGIN SELECT RAISE(ABORT, 'simulated write failure'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="simulated write failure"):
        await store.reinforce_recall(space_id, generation_id, "turn", [hit(item)], now=1000)
    assert await store.database.fetchall("SELECT * FROM recall_reinforcements") == []
    assert (await store.get_node(item["node_id"]))["reinforcement"] == 0
    await store.database.execute("DROP TRIGGER reject_recall")
    assert await store.reinforce_recall(space_id, generation_id, "turn", [hit(item)], now=1001)


@pytest.mark.asyncio
async def test_concurrent_reinforcement_is_atomic_across_connections(recall_store):
    store, space_id, generation_id = recall_store
    item = await node(store, space_id)
    second = MemoryStore(store.path)
    await second.open()
    try:
        results = await asyncio.gather(*(
            (store if index % 2 else second).reinforce_recall(
                space_id, generation_id, "same-turn", [hit(item)], now=1000,
            ) for index in range(8)
        ))
    finally:
        await second.close()
    assert sum(len(result) for result in results) == 1
    assert (await store.get_node(item["node_id"]))["reinforcement"] == 0.01


@pytest.mark.asyncio
async def test_ledger_has_an_explicit_24h_dedup_window(recall_store):
    store, space_id, generation_id = recall_store
    item = await node(store, space_id)
    await store.reinforce_recall(space_id, generation_id, "turn", [hit(item)], now=1000)
    assert await store.reinforce_recall(
        space_id, generation_id, "turn", [hit(item)], now=1000 + RECALL_LEDGER_RETENTION_SECONDS,
    ) == []
    assert await store.reinforce_recall(
        space_id, generation_id, "turn", [hit(item)], now=1001 + RECALL_LEDGER_RETENTION_SECONDS,
    )
    assert len(await store.database.fetchall("SELECT * FROM recall_reinforcements")) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("now", [math.nan, math.inf, -1, True, "1000", 10**1000])
async def test_invalid_clock_never_writes_reinforcement(recall_store, now):
    store, space_id, generation_id = recall_store
    item = await node(store, space_id)
    with pytest.raises(ValueError, match="now"):
        await store.reinforce_recall(space_id, generation_id, "turn", [hit(item)], now=now)
    assert await store.database.fetchall("SELECT * FROM recall_reinforcements") == []
