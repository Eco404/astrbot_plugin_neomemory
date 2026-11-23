"""Recall stays bounded, skips stale memories and never needs a text model."""

import asyncio
import copy
import math
import sqlite3
import time

import pytest
import pytest_asyncio

from adapters.providers import ProviderAdapter
from core.embeddings import EmbeddingService
from core.memory_spaces import private_space
from core.retrieval import (
    RecallPolicy,
    RecallService,
    rank_candidates,
    render_nodes,
    select_context,
)
from storage.cache_store import CacheStore
from storage.memory_store import MemoryStore
from tests.test_providers import FakeContext, FakeEmbedding

NOW = 10_000.0


def memory_node(identifier, *, kind="atomic", text=None, **updates):
    return {
        "node_id": identifier,
        "node_type": kind,
        "content_text": text or f"记忆：{identifier}",
        "content_version": 1,
        "status": "active",
        "importance": 0.5,
        "strength": 0.5,
        "strength_reset_at": NOW,
        "strength_reset_round": 0,
        "reinforcement": 0,
        "event_time_start": None,
        "event_time_text": None,
        "direct": True,
        "current": True,
        "score": 0.5,
        "connection": "直接匹配",
        **updates,
    }


def snapshot(*nodes, edges=(), coverage=()):
    return {
        "space_id": "space", "generation_id": "generation", "activity_round": 0,
        "nodes": list(nodes), "edges": list(edges), "coverage": list(coverage),
        "truncated": False,
    }


def edge(source, target):
    return {"source_node_id": source, "target_node_id": target, "relation_type": "related"}


def coverage(**overrides):
    return {
        "event_node_id": "event", "atomic_node_id": "atomic",
        "event_content_version": 1, "atomic_content_version": 1,
        "coverage_kind": "full", **overrides,
    }


class FakeMemory:
    def __init__(self, data):
        self.data = data
        self.revalidations = []
        self.reinforcements = []

    async def retrieval_snapshot(self, space_id, **kwargs):
        assert space_id == "space"
        return copy.deepcopy(self.data)

    async def revalidate_recall(self, space_id, generation_id, versions, **kwargs):
        self.revalidations.append((space_id, generation_id, versions, kwargs))
        return [node for node in self.data["nodes"] if node["node_id"] in versions
                and versions[node["node_id"]] == node["content_version"]]

    async def reinforce_recall(self, *args, **kwargs):
        self.reinforcements.append((args, kwargs))


class FakeEmbeddings:
    def __init__(self, vectors, *, cached=None, error=None, on_embed=None):
        self.vectors = vectors
        self.cache = dict(vectors if cached is None else cached)
        self.error = error
        self.on_embed = on_embed
        self.calls = []

    async def cached(self, texts, **kwargs):
        self.calls.append(("cached", list(texts), kwargs))
        return [self.cache.get(text) for text in texts]

    async def embed(self, texts, **kwargs):
        self.calls.append(("embed", list(texts), kwargs))
        if self.on_embed:
            await self.on_embed()
        if self.error:
            raise self.error
        return [self.vectors[text] for text in texts]

    async def text_chat(self, *args, **kwargs):
        raise AssertionError("passive recall must not call a text model")


def test_current_match_precedes_stronger_history_and_role_weights_are_distinct():
    data = snapshot(memory_node("current"), memory_node("user"), memory_node("agent"))
    vectors = [[0.4, 0, 0, math.sqrt(0.84)], [0, 1, 0, 0], [0, 0, 1, 0]]
    policy = RecallPolicy(max_neighbors=0)
    ranked = rank_candidates(data, vectors, [
        ([1, 0, 0, 0], 1.0, True),
        ([0, 1, 0, 0], policy.user_weight, False),
        ([0, 0, 1, 0], policy.agent_weight, False),
    ], policy, NOW)
    assert [node["node_id"] for node in ranked] == ["current", "user", "agent"]
    assert ranked[0]["score"] < ranked[1]["score"]
    assert ranked[1]["score"] == pytest.approx(0.85 * 0.45 + 0.15 * 0.5)
    assert ranked[2]["score"] == pytest.approx(0.85 * 0.4 + 0.15 * 0.5)


def test_repeating_history_hits_deduplicates_without_accumulating_score():
    data = snapshot(memory_node("remembered"))
    policy = RecallPolicy(max_neighbors=0)
    query = ([1, 0], 0.45, False)
    once = rank_candidates(data, [[1, 0]], [query], policy, NOW)
    repeated = rank_candidates(data, [[1, 0]], [query] * 10, policy, NOW)
    assert repeated == once


def test_weak_top_match_does_not_consume_the_only_eligible_candidate_slot():
    weak = memory_node("weak", strength=0.01)
    useful = memory_node("useful", strength=0.5)
    data = snapshot(weak, useful)
    vectors = [[1, 0], [0.8, 0.6]]
    policy = RecallPolicy(candidate_per_query=1, max_neighbors=0)
    passive = rank_candidates(data, vectors, [([1, 0], 1.0, True)], policy, NOW)
    assert [item["node_id"] for item in passive] == ["useful"]
    active = rank_candidates(data, vectors, [([1, 0], 1.0, True)], policy, NOW, active=True)
    assert [item["node_id"] for item in active] == ["weak"]


@pytest.mark.asyncio
async def test_default_history_is_last_ten_user_agent_messages_in_7200_seconds():
    item = memory_node("one")
    memory = FakeMemory(snapshot(item))
    history = [
        {"content_text": f"recent-{i}", "observed_at": NOW - i,
         "actor_kind": "user" if i % 2 else "agent"}
        for i in range(12)
    ] + [
        {"content_text": "too-old", "observed_at": NOW - 7201, "actor_kind": "user"},
        {"content_text": "future", "observed_at": NOW + 1, "actor_kind": "user"},
        {"content_text": "system", "observed_at": NOW, "actor_kind": "system"},
    ]
    vectors = {text: [1, 0] for text in [item["content_text"], "current", *(entry["content_text"] for entry in history)]}
    embeddings = FakeEmbeddings(vectors, cached={item["content_text"]: [1, 0]})
    result = await RecallService(memory, embeddings).recall("space", "current", history=history, now=NOW)
    query_calls = [call for call in embeddings.calls if call[0] == "embed"]
    assert query_calls[0][1] == ["current", *(f"recent-{i}" for i in range(10))]
    assert len(result.hits) == 1


@pytest.mark.asyncio
async def test_history_window_is_configurable_and_includes_cutoff():
    item = memory_node("one")
    history = [
        {"content_text": "boundary", "observed_at": NOW - 7200, "actor_kind": "user"},
        {"content_text": "older", "observed_at": NOW - 7200.1, "actor_kind": "user"},
    ]
    embeddings = FakeEmbeddings({text: [1, 0] for text in (item["content_text"], "current", "boundary", "older")},
                                cached={item["content_text"]: [1, 0]})
    await RecallService(FakeMemory(snapshot(item)), embeddings).recall("space", "current", history=history, now=NOW)
    assert [call[1] for call in embeddings.calls if call[0] == "embed"] == [["current", "boundary"]]
    embeddings.calls.clear()
    await RecallService(FakeMemory(snapshot(item)), embeddings, policy=RecallPolicy(history_limit=0)).recall(
        "space", "current", history=history, now=NOW,
    )
    assert [call[1] for call in embeddings.calls if call[0] == "embed"] == [["current"]]


def test_whole_prose_budget_skips_oversize_node_without_truncating_negation():
    long = memory_node("long", text="准备经历。" * 200)
    short = memory_node("short", text="用户不喜欢黑咖啡，但喜欢拿铁。")
    budget = len(render_nodes([short]).encode("utf-8"))
    selected = select_context([long, short], [], RecallPolicy(budget_tokens=budget))
    assert selected == [short]
    assert selected[0]["content_text"].endswith("但喜欢拿铁。")
    assert len(render_nodes(selected).encode("utf-8")) <= budget


@pytest.mark.parametrize("overrides,suppresses", [
    ({}, True), ({"coverage_kind": "partial"}, False),
    ({"event_content_version": 0}, False), ({"event_content_version": 2}, False),
    ({"atomic_content_version": 2}, False),
])
def test_only_full_coverage_of_both_current_versions_suppresses_atomic(overrides, suppresses):
    atomic = memory_node("atomic", text="用户只在下午喝咖啡时睡不好。")
    event = memory_node("event", kind="event", text="用户调整饮食，只有下午喝咖啡影响睡眠。")
    selected = select_context([atomic, event], [coverage(**overrides)], RecallPolicy())
    assert [node["node_id"] for node in selected] == (["event"] if suppresses else ["atomic", "event"])


def test_event_that_does_not_fit_cannot_suppress_covered_atomic():
    atomic = memory_node("atomic", text="用户只有下午喝咖啡影响睡眠。")
    event = memory_node("event", kind="event", text="调整饮食的过程。" * 200)
    budget = len(render_nodes([atomic]).encode("utf-8"))
    assert select_context([atomic, event], [coverage()], RecallPolicy(budget_tokens=budget)) == [atomic]
    assert select_context([atomic], [coverage()], RecallPolicy()) == [atomic]


def test_time_neighbors_are_labeled_without_causal_inference():
    first = memory_node("seed", kind="event", event_time_start=1000)
    nearby = memory_node("nearby", kind="event", event_time_start=1001)
    distant = memory_node("distant", kind="event", event_time_start=100_000)
    ranked = rank_candidates(snapshot(first, nearby, distant), [[1, 0], [0, 1], [0, 1]],
        [([1, 0], 1.0, True)], RecallPolicy(), NOW)
    assert [item["node_id"] for item in ranked] == ["seed", "nearby"]
    assert ranked[1]["connection"] == "同期经历"
    assert ranked[1]["direct"] is False
    assert "时间邻近不代表因果" in render_nodes(ranked)


def test_neighbor_expansion_is_one_hop_and_has_global_limit():
    nodes = [memory_node(name) for name in ("seed", "one", "two", "three", "grandchild")]
    data = snapshot(*nodes, edges=[edge("seed", "one"), edge("one", "grandchild"),
        edge("seed", "two"), edge("seed", "three")])
    ranked = rank_candidates(data, [[1, 0], *([[0, 1]] * 4)], [([1, 0], 1.0, True)],
        RecallPolicy(max_neighbors=2), NOW)
    assert [item["node_id"] for item in ranked] == ["seed", "one", "two"]
    assert all(not item["direct"] for item in ranked[1:])


@pytest.mark.asyncio
@pytest.mark.parametrize("cached_queries,expected", [(["current"], ["current-node"]), (["history"], ["history-node"])])
async def test_query_embedding_timeout_reuses_available_current_or_history_vectors(cached_queries, expected):
    current, historical = memory_node("current-node"), memory_node("history-node")
    vectors = {current["content_text"]: [1, 0], historical["content_text"]: [0, 1],
               "current": [1, 0], "history": [0, 1]}
    cached = {text: value for text, value in vectors.items() if text in cached_queries or text.startswith("记忆")}
    embeddings = FakeEmbeddings(vectors, cached=cached, error=TimeoutError("network timeout"))
    result = await RecallService(FakeMemory(snapshot(current, historical)), embeddings).recall(
        "space", "current", history=[{"content_text": "history", "actor_kind": "user", "observed_at": NOW}], now=NOW,
    )
    assert [item["node_id"] for item in result.hits] == expected
    assert result.diagnostics["embedding_degraded"] == "TimeoutError"
    assert "deadline_exceeded" not in result.diagnostics
    assert result.diagnostics["query_count"] == 2
    assert result.diagnostics["cached_query_count"] == result.diagnostics["usable_query_count"] == 1
    assert result.diagnostics["current_query_available"] == ("current" in cached_queries)
    assert 0 <= result.diagnostics["query_cache_seconds"] <= result.diagnostics["total_seconds"]
    assert 0 <= result.diagnostics["query_embedding_seconds"] <= result.diagnostics["total_seconds"]


@pytest.mark.asyncio
async def test_empty_node_cache_returns_without_network_indexing_or_query_call():
    item = memory_node("uncached")
    embeddings = FakeEmbeddings({}, cached={})
    service = RecallService(FakeMemory(snapshot(item)), embeddings)
    result = await service.recall("space", "current", now=NOW)
    assert result.text == ""
    assert result.diagnostics["missing_node_vectors"] == 1
    assert all(call[0] == "cached" for call in embeddings.calls)


@pytest.mark.asyncio
async def test_provider_ignoring_deadline_is_cancelled_by_recall_budget():
    item = memory_node("one")
    cancelled = asyncio.Event()

    async def hang():
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    embeddings = FakeEmbeddings({item["content_text"]: [1, 0]}, on_embed=hang)
    service = RecallService(FakeMemory(snapshot(item)), embeddings, policy=RecallPolicy(deadline_seconds=0.03))
    start = time.monotonic()
    result = await service.recall("space", "current", now=NOW)
    assert time.monotonic() - start < 0.5
    assert cancelled.is_set()
    assert result.text == ""
    assert result.diagnostics["deadline_exceeded"] is True
    assert "embedding_degraded" not in result.diagnostics
    assert result.diagnostics["query_count"] == 1 and result.diagnostics["cached_query_count"] == 0
    assert "usable_query_count" not in result.diagnostics
    assert result.diagnostics["query_embedding_seconds"] > 0
    assert result.diagnostics["total_seconds"] >= result.diagnostics["query_embedding_seconds"]


@pytest.mark.asyncio
async def test_real_embedding_service_timeout_retains_history_and_reports_provider_stage(tmp_path):
    cache = CacheStore(tmp_path / "cache.db")
    await cache.open()
    context = FakeContext()
    embedding = EmbeddingService(ProviderAdapter(context), cache, provider_id="embedding-test")
    item = memory_node("independent-memory", text="独立的观测记录")
    try:
        await embedding.embed([item["content_text"]], namespace="space", kind="node",
                              identities=[(item["node_id"], item["content_version"])])
        await embedding.embed(["独立的历史查询"], namespace="space")
        before = len(context.embedding.calls)
        context.embedding.gate = asyncio.Event()
        result = await RecallService(FakeMemory(snapshot(item)), embedding,
            policy=RecallPolicy(deadline_seconds=.25)).recall("space", "尚未缓存的当前查询",
                history=[{"content_text": "独立的历史查询", "actor_kind": "user", "observed_at": NOW}], now=NOW)
        assert result.hits
        assert result.diagnostics["embedding_degraded"] == "TimeoutError"
        assert "deadline_exceeded" not in result.diagnostics
        assert result.diagnostics["cached_query_count"] == result.diagnostics["usable_query_count"] == 1
        assert result.diagnostics["current_query_available"] is False
        detail = result.diagnostics["query_embedding"]
        assert detail["failure_stage"] == "provider_batches"
        assert detail["provider_failure_stages"] == ["provider_call"]
        assert detail["provider_call_count"] == 1 and detail["provider_completed_batch_count"] == 0
        assert detail["provider_call_seconds_sum"] > 0
        assert len(context.embedding.calls) == before + 1
    finally:
        await cache.close()


@pytest_asyncio.fixture
async def real_memory(tmp_path):
    memory = MemoryStore(tmp_path / "memory.db")
    await memory.open()
    space = await memory.get_or_create_space(private_space("persona", "user"))
    try:
        yield memory, space["space_id"]
    finally:
        await memory.close()


async def stored_node(memory, space_id, text, **kwargs):
    return await memory.create_node(space_id=space_id, node_type=kwargs.pop("node_type", "atomic"),
        content_text=text, source_ids=["manual-source"], source_kind="manual", **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["disabled", "generation", "content"])
async def test_real_store_revalidates_changes_before_context_injection(real_memory, mutation):
    memory, space_id = real_memory
    item = await stored_node(memory, space_id, "旧事实")

    async def mutate():
        if mutation == "disabled":
            await memory.update_node_status(item["node_id"], "disabled")
        elif mutation == "content":
            await memory.update_node_content(item["node_id"], "新事实")
        else:
            replacement = await memory.create_generation(space_id)
            await memory.activate_generation(space_id, replacement["generation_id"])

    embeddings = FakeEmbeddings({"旧事实": [1, 0], "query": [1, 0]}, cached={"旧事实": [1, 0]}, on_embed=mutate)
    result = await RecallService(memory, embeddings).recall(space_id, "query")
    assert result.hits == []
    assert result.text == ""


@pytest.mark.asyncio
async def test_active_recall_renders_current_historical_status_after_revalidation(real_memory):
    memory, space_id = real_memory
    item = await stored_node(memory, space_id, "过去的偏好")

    async def make_historical():
        await memory.update_node_status(item["node_id"], "historical")

    embeddings = FakeEmbeddings({"过去的偏好": [1, 0], "query": [1, 0]}, cached={"过去的偏好": [1, 0]}, on_embed=make_historical)
    result = await RecallService(memory, embeddings).recall(space_id, "query", active=True)
    assert len(result.hits) == 1
    assert result.hits[0]["status"] == "historical"
    assert '"status":"historical"' in result.text


@pytest.mark.asyncio
async def test_only_final_injected_hits_are_reinforced_after_coverage_selection(real_memory):
    memory, space_id = real_memory
    atomic = await stored_node(memory, space_id, "用户的面试在周五")
    event = await stored_node(memory, space_id, "用户准备面试并确认周五参加", node_type="event")
    unrelated = await stored_node(memory, space_id, "未被召回的内容")
    await memory.add_coverage(event["node_id"], atomic["node_id"])
    embeddings = FakeEmbeddings({atomic["content_text"]: [1, 0], event["content_text"]: [1, 0],
        unrelated["content_text"]: [0, 1], "query": [1, 0]})
    service = RecallService(memory, embeddings)
    result = await service.recall(space_id, "query")
    assert [item["node_id"] for item in result.hits] == [event["node_id"]]
    assert (await memory.get_node(event["node_id"]))["reinforcement"] == 0
    await service.reinforce(result, "turn-one")
    await service.reinforce(result, "turn-one")
    assert (await memory.get_node(event["node_id"]))["reinforcement"] == pytest.approx(0.01)
    assert (await memory.get_node(atomic["node_id"]))["reinforcement"] == 0
    assert (await memory.get_node(unrelated["node_id"]))["reinforcement"] == 0


@pytest.mark.asyncio
async def test_index_batches_skip_cached_nodes_and_preserve_versioned_keys():
    nodes = [memory_node(str(index)) for index in range(6)]
    embeddings = FakeEmbeddings(
        {node["content_text"]: [1, 0] for node in nodes},
        cached={nodes[0]["content_text"]: [1, 0], nodes[3]["content_text"]: [1, 0]},
    )
    embeddings.batch_size = 2
    result = await RecallService(FakeMemory(snapshot(*nodes)), embeddings).index_space("space")
    calls = [call for call in embeddings.calls if call[0] == "embed"]
    assert [call[1] for call in calls] == [
        [nodes[1]["content_text"]], [nodes[2]["content_text"]],
        [nodes[4]["content_text"], nodes[5]["content_text"]],
    ]
    assert [call[2]["identities"] for call in calls] == [[("1", 1)], [("2", 1)], [("4", 1), ("5", 1)]]
    assert result == {"indexed": 6, "cached": 2, "truncated": False}
    remaining = [call[2]["timeout_seconds"] for call in calls]
    assert 0 < remaining[-1] < remaining[0] <= 60


@pytest.mark.asyncio
async def test_index_falls_back_to_small_batches_without_batch_size_attribute():
    nodes = [memory_node(str(index)) for index in range(18)]
    embeddings = FakeEmbeddings({node["content_text"]: [1, 0] for node in nodes}, cached={})
    await RecallService(FakeMemory(snapshot(*nodes)), embeddings).index_space("space")
    assert [len(call[1]) for call in embeddings.calls if call[0] == "embed"] == [16, 2]


@pytest.mark.asyncio
async def test_index_with_fully_cached_nodes_does_not_enter_embedding_lock():
    item = memory_node("cached")
    embeddings = FakeEmbeddings({item["content_text"]: [1, 0]})
    result = await RecallService(FakeMemory(snapshot(item)), embeddings).index_space("space")
    assert result["cached"] == result["indexed"] == 1
    assert all(call[0] == "cached" for call in embeddings.calls)


@pytest.mark.asyncio
async def test_real_embedding_lock_allows_foreground_query_between_index_batches(tmp_path):
    nodes = [memory_node(str(index)) for index in range(6)]
    context = FakeContext()
    context.embedding.gate = asyncio.Event()
    cache = CacheStore(tmp_path / "cache.db")
    await cache.open()
    embedding = EmbeddingService(ProviderAdapter(context), cache, provider_id="embedding-test", batch_size=2)
    service = RecallService(FakeMemory(snapshot(*nodes)), embedding)
    index_task = asyncio.create_task(service.index_space("space"))
    foreground = None
    try:
        await asyncio.wait_for(context.embedding.started.wait(), timeout=1)
        foreground = asyncio.create_task(embedding.embed(["foreground"], namespace="space"))
        await asyncio.sleep(0)
        context.embedding.gate.set()
        await asyncio.wait_for(asyncio.gather(index_task, foreground), timeout=2)
        assert context.embedding.calls[1] == ["foreground"]
        assert len(context.embedding.calls) == 4
        assert all(len(call) <= 2 for call in context.embedding.calls)
        assert context.calls == []  # No text-model request in either path.
    finally:
        for task in (index_task, foreground):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(*(task for task in (index_task, foreground) if task), return_exceptions=True)
        await cache.close()


@pytest.mark.asyncio
async def test_index_total_timeout_cancels_later_batches_without_refreshing_budget():
    nodes = [memory_node(str(index)) for index in range(5)]

    async def delayed():
        await asyncio.sleep(0.03)

    embeddings = FakeEmbeddings({node["content_text"]: [1, 0] for node in nodes}, cached={}, on_embed=delayed)
    embeddings.batch_size = 1
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        await RecallService(FakeMemory(snapshot(*nodes)), embeddings).index_space("space", timeout_seconds=0.05)
    assert time.monotonic() - started < 0.5
    calls = [call for call in embeddings.calls if call[0] == "embed"]
    assert 1 <= len(calls) <= 2
    budgets = [call[2]["timeout_seconds"] for call in calls]
    assert all(0 < budget < 0.05 for budget in budgets)
    assert budgets == sorted(budgets, reverse=True)


@pytest.mark.asyncio
async def test_cancelling_index_keeps_completed_batches_and_releases_embedding_lock(tmp_path):
    second_started = asyncio.Event()

    class PauseSecondBatch(FakeEmbedding):
        async def get_embeddings(self, texts):
            if self.calls:
                self.gate = asyncio.Event()
                second_started.set()
            return await super().get_embeddings(texts)

    context = FakeContext(PauseSecondBatch())
    cache = CacheStore(tmp_path / "cache.db")
    await cache.open()
    embedding = EmbeddingService(ProviderAdapter(context), cache, provider_id="embedding-test", batch_size=2)
    nodes = [memory_node(str(index)) for index in range(6)]
    task = asyncio.create_task(RecallService(FakeMemory(snapshot(*nodes)), embedding).index_space("space"))
    try:
        await asyncio.wait_for(second_started.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not embedding._lock.locked()
        assert context.embedding.active == 0
        assert len(context.embedding.calls) == 2
        vectors = await embedding.cached([node["content_text"] for node in nodes], namespace="space", kind="node",
            identities=[(node["node_id"], 1) for node in nodes])
        assert all(vector is not None for vector in vectors[:2])
        assert vectors[2:] == [None] * 4
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await cache.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("failing_kind", ["node", "message"])
async def test_cache_failure_degrades_recall_without_network_rebuild(failing_kind):
    item = memory_node("one")

    class BrokenCache(FakeEmbeddings):
        async def cached(self, texts, **kwargs):
            if kwargs.get("kind", "message") == failing_kind:
                raise sqlite3.OperationalError("sensitive database details")
            return await super().cached(texts, **kwargs)

    embeddings = BrokenCache({item["content_text"]: [1, 0], "query": [1, 0]})
    result = await RecallService(FakeMemory(snapshot(item)), embeddings).recall("space", "query", now=NOW)
    if failing_kind == "node":
        assert result.hits == [] and result.text == ""
        assert all(call[0] != "embed" for call in embeddings.calls)
    else:
        assert [hit["node_id"] for hit in result.hits] == [item["node_id"]]
        assert [call[1] for call in embeddings.calls if call[0] == "embed"] == [["query"]]
    assert result.diagnostics["cache_degraded"] == "OperationalError"
    assert "sensitive" not in str(result.diagnostics)


@pytest.mark.asyncio
async def test_cache_cancellation_is_not_swallowed_as_degradation():
    item = memory_node("one")

    class CancelledCache(FakeEmbeddings):
        async def cached(self, texts, **kwargs):
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await RecallService(FakeMemory(snapshot(item)), CancelledCache({})).recall("space", "query", now=NOW)


@pytest.mark.asyncio
async def test_model_change_between_node_and_query_vectors_cannot_mix_coordinates():
    item = memory_node("one")
    embeddings = FakeEmbeddings({item["content_text"]: [1, 0], "query": [1, 0]})
    del embeddings.cache["query"]
    embeddings.fingerprint = "model-before"

    async def switch_model():
        embeddings.fingerprint = "model-after"

    embeddings.on_embed = switch_model
    result = await RecallService(FakeMemory(snapshot(item)), embeddings).recall("space", "query", now=NOW)
    assert result.text == ""
    assert result.diagnostics["embedding_changed"] is True


@pytest.mark.asyncio
async def test_preview_uses_passive_ranking_and_explains_an_unselected_sample():
    now = time.time()
    nodes = [memory_node(key, strength_reset_at=now) for key in ("chosen", "limit", "low", "weak")]
    nodes[-1]["strength"] = 0.
    memory = FakeMemory(snapshot(*nodes))
    vectors = dict(zip([node["content_text"] for node in nodes], [[1, 0], [.8, .6], [0, 1], [.96, .28]], strict=True))
    service = RecallService(memory, FakeEmbeddings({**vectors, "query": [1, 0]}),
                            policy=RecallPolicy(max_nodes=1, candidate_per_query=1, max_neighbors=0))
    expected = await service.recall("space", "query")
    result = await service.preview("space", "query")
    assert result.text == expected.text
    assert [item["node_id"] for item in result.hits] == ["chosen"]
    assert result.hits[0]["similarity"] == pytest.approx(1)
    assert {item["node_id"]: item["reason"] for item in result.diagnostics["preview_candidates"]} == {
        "limit": "超出单次查询候选数", "low": "低于相似度门限", "weak": "低于被动召回强度门限"}
    assert memory.reinforcements == []


@pytest.mark.asyncio
async def test_preview_embeds_only_query_and_leaves_real_vector_cache_untouched(tmp_path):
    nodes = [memory_node("ready", strength_reset_at=time.time()), memory_node("unindexed")]
    memory = FakeMemory(snapshot(*nodes))
    context = FakeContext()
    cache = CacheStore(tmp_path / "cache.db")
    await cache.open()
    try:
        embedding = EmbeddingService(ProviderAdapter(context), cache, provider_id="embedding-test")
        await embedding.embed([nodes[0]["content_text"]], namespace="space", kind="node", identities=[("ready", 1)])
        before = [tuple(row) for row in await cache.database.fetchall("SELECT * FROM embedding_cache")]
        result = await RecallService(memory, embedding).preview("space", "new query")
        after = [tuple(row) for row in await cache.database.fetchall("SELECT * FROM embedding_cache")]
        assert after == before
        assert result.diagnostics["missing_node_vectors"] == 1
        assert context.embedding.calls == [[nodes[0]["content_text"]], ["new query"]]
        assert memory.reinforcements == []
    finally:
        await cache.close()


def test_passive_evidence_recovers_rare_identifier_and_rejects_an_unrelated_query():
    decoys = [memory_node(f"d{i}", text=f"批次 {i} 已经封存") for i in range(5)]
    target = memory_node("target", text="{{user:privatekey42}} 登记的编号是 ZX790。")
    data = snapshot(*decoys, target)
    vectors = [[score, math.sqrt(1 - score ** 2)] for score in (.67, .66, .65, .64, .63, .61)]
    queries = [([1, 0], 1., True)]
    policy = RecallPolicy(candidate_per_query=1, max_neighbors=0, ranking_strategy="gated")
    ranked = rank_candidates(data, vectors, queries, policy, NOW, query_texts=["ZX790？"])
    assert [row["node_id"] for row in ranked] == ["target"]
    exclusions = []
    assert rank_candidates(data, vectors, queries, policy, NOW, query_texts=["privatekey42？"],
                           preview_exclusions=exclusions) == []
    assert "查询匹配证据不足" in {item["reason"] for item in exclusions}
    assert [row["node_id"] for row in rank_candidates(
        data, vectors, queries, policy, NOW, query_texts=["ZX790？"], active=True)] == ["d0"]


def test_history_supplements_a_related_reference_and_drops_an_unrelated_previous_turn():
    data = snapshot(memory_node("boat", text="那艘木船被送去修理。"))
    policy = RecallPolicy(ranking_strategy="gated")
    # The second query denotes the earlier message. Its node is below the
    # current query's independent threshold, but both turns discuss the boat.
    history = ([1, 0], .45, False)
    texts = ["它最后怎么样处理的？", "昨天打捞的木船"]
    related = [([.66, math.sqrt(1 - .66**2)], 1., True), history]
    ranked = rank_candidates(data, [[1, 0]], related, policy, NOW, query_texts=texts)
    assert [(row["node_id"], row["current"]) for row in ranked] == [("boat", False)]
    changed_topic = [([0, 1], 1., True), history]
    assert rank_candidates(data, [[1, 0]], changed_topic, policy, NOW,
                           query_texts=["我有报名足球赛吗？", texts[1]]) == []
    assert rank_candidates(data, [[1, 0]], changed_topic, policy, NOW,
                           query_texts=["后来呢？", texts[1]])[0]["node_id"] == "boat"


def test_relevant_graph_neighbor_remains_eligible_and_an_unsupported_neighbor_is_skipped():
    nodes = [memory_node(str(i), text=f"颜色各异的物件 {i}") for i in range(5)]
    data = snapshot(*nodes, edges=[edge("0", "4"), edge("0", "3")])
    scores = (.8, .79, .78, .77, .51)
    ranked = rank_candidates(data, [[s, math.sqrt(1 - s*s)] for s in scores], [([1, 0], 1., True)],
                             RecallPolicy(candidate_per_query=1, ranking_strategy="gated"), NOW,
                             query_texts=["有没有珍藏？"])
    assert [(row["node_id"], row["connection"]) for row in ranked] == [
        ("0", "直接匹配"), ("3", "相关经历")]


@pytest.mark.asyncio
async def test_gated_query_uses_history_for_a_referent_but_not_a_new_subject_or_acknowledgement():
    node = memory_node("kite", text="气象风筝放在工具室。")
    current = "灯塔多久闪一次？"
    followup = "它后来放在哪儿？"
    previous = "气象风筝收回来了。"
    vectors = {node["content_text"]: [1., 0.], previous: [1., 0.], followup: [0., 1.],
               current: [.65, math.sqrt(1 - .65**2)], "没问题": [1., 0.], "收到": [1., 0.]}
    embeddings = FakeEmbeddings(vectors)
    service = RecallService(FakeMemory(snapshot(node)), embeddings, policy=RecallPolicy(ranking_strategy="gated"))
    history = [{"content_text": text, "actor_kind": "user", "observed_at": NOW - i}
               for i, text in enumerate(("收到", previous, "没问题"))]
    changed = await service.recall("space", current, history=history, now=NOW)
    assert changed.diagnostics["query_count"] == 1 and not changed.hits
    continued = await service.recall("space", followup, history=history, now=NOW)
    assert continued.diagnostics["query_count"] == 2
    assert [hit["node_id"] for hit in continued.hits] == ["kite"]
    ack = await service.recall("space", "收到", history=history, now=NOW)
    assert not ack.hits and "query_embedding" not in ack.diagnostics
    explicit = await service.recall("space", "收到", active=True, now=NOW)
    assert [hit["node_id"] for hit in explicit.hits] == ["kite"]


@pytest.mark.asyncio
async def test_identical_passive_evidence_leaves_room_for_another_date():
    nodes = [memory_node(key, kind="event", text="来过植物园。", event_time_text=date)
             for key, date in (("a", "星期一"), ("b", "星期一"), ("c", "星期二"))]
    service = RecallService(FakeMemory(snapshot(*nodes)), FakeEmbeddings({"来过植物园。": [1., 0.], "植物园": [1., 0.]}),
                            policy=RecallPolicy(ranking_strategy="gated", max_nodes=2))
    result = await service.recall("space", "植物园", now=NOW, preview=True)
    assert [row["node_id"] for row in result.hits] == ["a", "c"]
    assert [(row["node_id"], row["reason"]) for row in result.diagnostics["preview_candidates"]] == [
        ("b", "相同内容已入选")]
