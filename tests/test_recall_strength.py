"""Independent eligibility awakening and bounded reinforcement on real SQLite."""

import asyncio
import sqlite3
import time

import pytest

from core.retrieval import RecallPolicy, RecallService, rank_candidates
from core.strength import StrengthPolicy, calculate_strength
from storage.memory_store import MemoryStore
from tests.test_recall_store import hit, node, recall_store  # noqa: F401
from tests.test_retrieval import FakeEmbeddings


async def age(store, generation_id, item, *, reinforcement=0):
    await store.database.execute(
        "UPDATE memory_generations SET activity_round=2000 WHERE generation_id=?",
        (generation_id,),
    )
    await store.database.execute(
        "UPDATE memory_nodes SET strength_reset_at=0,strength_reset_round=0,reinforcement=? WHERE node_id=?",
        (reinforcement, item["node_id"]),
    )


async def score(store, space_id, item, now, policy=StrengthPolicy()):
    current = await store.get_node(item["node_id"])
    generation = await store.get_active_generation(space_id)
    return calculate_strength(current["strength"], importance=current["importance"],
        elapsed_seconds=max(0, now - current["strength_reset_at"]),
        elapsed_rounds=max(0, generation["activity_round"] - current["strength_reset_round"]),
        reinforcement=current["reinforcement"], policy=policy).value


@pytest.mark.asyncio
@pytest.mark.parametrize("reinforcement", [0, 0.2])
async def test_active_wake_restores_eligibility_without_refilling_ordinary_quota(recall_store, reinforcement):
    store, space_id, generation_id = recall_store
    item = await node(store, space_id, importance=0.4, manual_protected=True)
    await age(store, generation_id, item, reinforcement=reinforcement)
    now = 365 * 86400
    assert await score(store, space_id, item, now) == 0
    credited = await store.reinforce_recall(space_id, generation_id, "wake", [hit(item)], active=True, now=now)
    assert len(credited) == 1 and credited[0]["awakened"] is True
    assert credited[0]["increment"] == 0
    current = await store.get_node(item["node_id"])
    assert current["reinforcement"] == reinforcement
    assert current["strength"] == max(0, 0.11 - reinforcement)
    assert current["strength_reset_at"] == now and current["strength_reset_round"] == 2000
    for field in ("importance", "content_text", "content_version", "updated_at", "manual_protected", "status"):
        assert current[field] == item[field]
    assert await score(store, space_id, item, now + 1) >= 0.1
    assert len(await store.database.fetchall("SELECT * FROM recall_reinforcements WHERE increment=0")) == 1
    # Restart and same-turn replay cannot reset the clock or gain again.
    await store.close()
    await store.open()
    assert await store.reinforce_recall(space_id, generation_id, "wake", [hit(item)], active=True, now=now + 1) == []
    assert (await store.get_node(item["node_id"]))["strength_reset_at"] == now
    if reinforcement:
        assert await store.reinforce_recall(space_id, generation_id, "next", [hit(item)], active=True, now=now + 300) == []
        assert (await store.get_node(item["node_id"]))["reinforcement"] == reinforcement


@pytest.mark.asyncio
@pytest.mark.parametrize("status,active", [("active", False), ("historical", True), ("disabled", True),
                                         ("deleted", True), ("superseded", True)])
async def test_only_current_active_return_can_awaken(recall_store, status, active):
    store, space_id, generation_id = recall_store
    item = await node(store, space_id, importance=0.4, status=status)
    await age(store, generation_id, item)
    credited = await store.reinforce_recall(space_id, generation_id, "return", [hit(item)], active=active, now=365 * 86400)
    assert not any(entry["awakened"] for entry in credited)
    current = await store.get_node(item["node_id"])
    assert current["status"] == status and current["strength"] == item["strength"]
    assert current["strength_reset_at"] == current["strength_reset_round"] == 0
    assert await score(store, space_id, item, 365 * 86400) < 0.1


@pytest.mark.asyncio
async def test_zero_gain_wake_does_not_extend_ordinary_cooldown(recall_store):
    store, space_id, generation_id = recall_store
    item = await node(store, space_id, importance=0.4)
    positive = await store.reinforce_recall(space_id, generation_id, "positive", [hit(item)], now=1000)
    reinforcement = positive[0]["reinforcement"]
    await age(store, generation_id, item, reinforcement=reinforcement)
    wake = await store.reinforce_recall(space_id, generation_id, "wake", [hit(item)], active=True, now=1100)
    assert wake[0]["awakened"] and wake[0]["increment"] == 0
    assert await store.reinforce_recall(space_id, generation_id, "early", [hit(item)], now=1299) == []
    # Ordinary cooldown expires 300s after the positive credit, not after awakening.
    later = await store.reinforce_recall(space_id, generation_id, "ready", [hit(item)], now=1300)
    assert later[0]["increment"] > 0 and later[0]["awakened"] is False


@pytest.mark.asyncio
async def test_awakening_does_not_spend_or_require_remaining_turn_quota(recall_store):
    store, space_id, generation_id = recall_store
    item = await node(store, space_id, importance=0.4)
    await age(store, generation_id, item)
    policy = StrengthPolicy(max_turn_reinforcement=0, max_reinforcement=0)
    credited = await store.reinforce_recall(space_id, generation_id, "zero-budget", [hit(item)], active=True,
                                           now=365 * 86400, policy=policy)
    assert credited[0]["awakened"] and credited[0]["increment"] == 0
    assert await score(store, space_id, item, 365 * 86400, policy) == pytest.approx(0.11)


@pytest.mark.asyncio
async def test_lower_ordinary_cap_retains_raw_consumed_quota_during_wake(recall_store):
    store, space_id, generation_id = recall_store
    item = await node(store, space_id, importance=0.4)
    await age(store, generation_id, item, reinforcement=0.2)
    policy = StrengthPolicy(max_reinforcement=0.05)
    await store.reinforce_recall(space_id, generation_id, "wake", [hit(item)], active=True,
                                 now=365 * 86400, policy=policy)
    current = await store.get_node(item["node_id"])
    assert current["reinforcement"] == 0.2
    assert current["strength"] == pytest.approx(0.06)
    assert await score(store, space_id, item, 365 * 86400, policy) == pytest.approx(0.11)
    assert await store.reinforce_recall(space_id, generation_id, "cap-raised", [hit(item)], now=365 * 86400 + 300) == []


@pytest.mark.asyncio
async def test_repeated_awakening_never_refills_consumed_ordinary_cap(recall_store):
    store, space_id, generation_id = recall_store
    item = await node(store, space_id, importance=0.4)
    await age(store, generation_id, item, reinforcement=0.2)
    now = 365 * 86400
    await store.reinforce_recall(space_id, generation_id, "wake-one", [hit(item)], active=True, now=now)
    await store.database.execute("UPDATE memory_generations SET activity_round=4000 WHERE generation_id=?", (generation_id,))
    assert await score(store, space_id, item, now + 300) == 0
    second = await store.reinforce_recall(space_id, generation_id, "wake-two", [hit(item)], active=True, now=now + 300)
    assert second[0]["awakened"] and second[0]["increment"] == 0
    current = await store.get_node(item["node_id"])
    assert current["strength_reset_round"] == 4000 and current["reinforcement"] == 0.2
    assert await store.reinforce_recall(space_id, generation_id, "ordinary", [hit(item)], now=now + 600) == []
    ledger = await store.database.fetchall("SELECT increment FROM recall_reinforcements")
    assert len(ledger) == 3 and sum(row["increment"] for row in ledger) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [0.1, 1.0])
async def test_zero_margin_restores_only_current_instant_not_permanent_eligibility(recall_store, minimum):
    store, space_id, generation_id = recall_store
    item = await node(store, space_id, importance=0.4)
    await age(store, generation_id, item)
    policy = StrengthPolicy(wake_margin=0)
    now = 365 * 86400
    await store.reinforce_recall(space_id, generation_id, "wake", [hit(item)], active=True, now=now,
                                 policy=policy, min_strength=minimum)
    assert await score(store, space_id, item, now, policy) == minimum
    assert await score(store, space_id, item, now + 1, policy) < minimum


@pytest.mark.asyncio
async def test_failed_awakening_rolls_back_baseline_clock_and_ledger(recall_store):
    store, space_id, generation_id = recall_store
    item = await node(store, space_id, importance=0.4)
    await age(store, generation_id, item)
    before = await store.get_node(item["node_id"])
    await store.database.execute("CREATE TRIGGER reject_wake BEFORE UPDATE OF strength ON memory_nodes "
                                 "BEGIN SELECT RAISE(ABORT, 'synthetic write failure'); END")
    with pytest.raises(sqlite3.IntegrityError, match="synthetic write failure"):
        await store.reinforce_recall(space_id, generation_id, "wake", [hit(item)], active=True, now=365 * 86400)
    assert await store.get_node(item["node_id"]) == before
    assert await store.database.fetchall("SELECT * FROM recall_reinforcements") == []
    await store.database.execute("DROP TRIGGER reject_wake")
    assert (await store.reinforce_recall(space_id, generation_id, "wake", [hit(item)], active=True,
                                         now=365 * 86400))[0]["awakened"]


@pytest.mark.asyncio
async def test_concurrent_awakening_decides_once_across_connections(recall_store):
    store, space_id, generation_id = recall_store
    item = await node(store, space_id, importance=0.4)
    await age(store, generation_id, item)
    second = MemoryStore(store.path)
    await second.open()
    try:
        results = await asyncio.gather(*(connection.reinforce_recall(space_id, generation_id, "wake", [hit(item)],
            active=True, now=365 * 86400) for connection in (store, second, store, second)))
    finally:
        await second.close()
    assert sum(len(result) for result in results) == 1
    assert next(result[0] for result in results if result)["awakened"]
    assert (await store.get_node(item["node_id"]))["reinforcement"] == 0


@pytest.mark.asyncio
async def test_active_delivery_awakens_only_final_returns_and_next_passive_recalls(recall_store):
    store, space_id, generation_id = recall_store
    selected = await node(store, space_id, "合成样例中的返程约定", importance=0.4)
    omitted = await node(store, space_id, "合成样例中的其他约定", importance=0.4)
    for item in (selected, omitted):
        await age(store, generation_id, item, reinforcement=0.2)
    embeddings = FakeEmbeddings({selected["content_text"]: [1, 0], omitted["content_text"]: [0.8, 0.6], "query": [1, 0]})
    service = RecallService(store, embeddings, policy=RecallPolicy(
        max_nodes=1, max_neighbors=0, min_strength=0.6,
        strength_policy=StrengthPolicy(wake_margin=0.02),
    ))
    assert (await service.recall(space_id, "query")).hits == []
    active = await service.recall(space_id, "query", active=True)
    assert [item["node_id"] for item in active.hits] == [selected["node_id"]]
    assert (await store.get_node(selected["node_id"]))["strength_reset_at"] == 0
    await service.reinforce(active, "delivered", active=True)
    passive = await service.recall(space_id, "query")
    assert [item["node_id"] for item in passive.hits] == [selected["node_id"]]
    assert (await store.get_node(omitted["node_id"]))["strength_reset_at"] == 0
    assert (await store.get_node(selected["node_id"]))["reinforcement"] == 0.2
    assert (await store.get_node(selected["node_id"]))["strength"] == pytest.approx(0.42)


@pytest.mark.asyncio
async def test_recall_ranking_honors_policy_and_importance_distinct_from_baseline(recall_store):
    store, space_id, generation_id = recall_store
    ordinary = await node(store, space_id, importance=0.1, strength=0.5)
    significant = await node(store, space_id, importance=0.9, strength=0.5)
    await store.database.execute("UPDATE memory_generations SET activity_round=500 WHERE generation_id=?", (generation_id,))
    snapshot = await store.retrieval_snapshot(space_id)
    vectors = [[1, 0] for _ in snapshot["nodes"]]
    query = [([1, 0], 1, True)]
    policy = RecallPolicy(min_strength=0.1, max_neighbors=0)
    ranked = rank_candidates(snapshot, vectors, query, policy, time.time())
    assert [item["node_id"] for item in ranked] == [significant["node_id"]]
    no_decay = RecallPolicy(max_neighbors=0, strength_policy=StrengthPolicy(time_rate=0, activity_loss_per_round=0))
    ranked = rank_candidates(snapshot, vectors, query, no_decay, time.time())
    assert {item["node_id"] for item in ranked} == {ordinary["node_id"], significant["node_id"]}
