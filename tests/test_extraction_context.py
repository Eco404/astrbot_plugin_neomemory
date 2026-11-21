"""独立合成候选验证：超过近期截断、分页内存、冷热缓存与范围隔离。"""

import math

import pytest
import pytest_asyncio

from core.embeddings import EmbeddingService
from core.extraction_context import (
    CONTEXT_PAGE_SIZE,
    _rank_page_with_query_coverage,
    select_context_ids,
    window_query_texts,
)
from core.memory_spaces import private_space
from storage.cache_store import CacheStore
from storage.memory_store import MemoryStore


class SemanticFixtureAdapter:
    """固定向量只验证候选算法与调用边界，不代表真实Embedding语义效果。"""

    def __init__(self, memory):
        self.memory = memory
        self.calls = []
        self.revision = "fixture-v1"
        self.failure = None
        self.fail_on = None

    def embedding_fingerprint(self, provider_id, revision=""):
        return f"{provider_id}:{self.revision}:{revision}"

    def embedding_dimension(self, provider_id):
        return 3

    async def embed_batch(self, texts, **kwargs):
        assert not self.memory.database._lock.locked()
        self.calls.append(list(texts))
        if self.failure is not None and (self.fail_on is None or len(self.calls) == self.fail_on):
            raise self.failure
        return [[1.0, 0.0, 0.0] if "预约" in text else [0.0, 1.0, 0.0] for text in texts]


@pytest_asyncio.fixture
async def context_env(tmp_path):
    memory = MemoryStore(tmp_path / "memory.db")
    cache = CacheStore(tmp_path / "cache.db")
    await memory.open()
    await cache.open()
    space = await memory.get_or_create_space(private_space("synthetic-persona", "synthetic-user"))
    generation = await memory.get_active_generation(space["space_id"])
    adapter = SemanticFixtureAdapter(memory)
    embeddings = EmbeddingService(adapter, cache, provider_id="synthetic", batch_size=16)
    try:
        yield memory, cache, space, generation, adapter, embeddings
    finally:
        await memory.close()
        await cache.close()


async def seed_nodes(memory, space, generation, count):
    """大规模合成库直接批量装载；每条正文和版本都有独立身份。"""
    async with memory.database.transaction() as connection:
        await connection.executemany(
            "INSERT INTO memory_nodes(node_id,generation_id,space_id,node_type,status,importance,"
            "strength,created_at,updated_at,title) VALUES (?, ?, ?, 'event','active',0.1,0.01,?,?,?)",
            [(f"node_{index:05}", generation["generation_id"], space["space_id"], index, index,
              f"仅展示标题{index}") for index in range(count)],
        )
        await connection.executemany(
            "INSERT INTO memory_node_versions(node_version_id,node_id,version_no,content_text,"
            "change_kind,editor_kind,created_at) VALUES (?, ?, 1, ?, 'created','model',?)",
            [(f"version_{index:05}", f"node_{index:05}",
              "三月预约参观科技馆。" if index == 0 else (
                  "七月另一次预约参观科技馆。" if index == 1 else f"背景话题 {index}。"), index)
             for index in range(count)],
        )


def test_window_queries_cover_head_middle_and_tail_without_joining_texts():
    sources = [{"content_text": f"独立消息 {index}"} for index in range(41)]
    texts = window_query_texts(sources)
    indices = [int(text.split()[-1]) for text in texts]
    assert len(texts) == 10
    assert indices[0] == 0 and indices[-1] == 40
    assert max(right - left for left, right in zip(indices, indices[1:])) <= math.ceil(40 / 9)
    assert all(text in [source["content_text"] for source in sources] for text in texts)
    assert window_query_texts([{"content_text": " "}, {"content_text": "重复"}, {"content_text": "重复"}]) == ["重复"]


@pytest.mark.asyncio
async def test_old_events_beyond_2000_are_candidates_with_cold_and_warm_vectors(context_env, monkeypatch):
    memory, _, space, generation, adapter, embeddings = context_env
    await seed_nodes(memory, space, generation, 2105)
    pages = []
    original = memory.extraction_context_page

    async def inspect_page(*args, **kwargs):
        page = await original(*args, **kwargs)
        if not kwargs.get("recent"):
            pages.append(len(page["nodes"]))
        return page

    monkeypatch.setattr(memory, "extraction_context_page", inspect_page)
    kwargs = dict(space_id=space["space_id"], generation_id=generation["generation_id"],
                  sources=[{"content_text": "三月那次预约有新消息。"}], limit=6, timeout_seconds=30)
    selected = await select_context_ids(memory, embeddings, **kwargs)
    assert selected[:2] == ("node_02104", "node_02103")
    assert {"node_00000", "node_00001"}.issubset(selected)
    assert len(selected) == len(set(selected)) == 6
    assert sum(pages) == 2105 and max(pages) <= CONTEXT_PAGE_SIZE
    call_count = len(adapter.calls)
    assert all(len(texts) <= 16 for texts in adapter.calls)
    assert not any("仅展示标题" in text for texts in adapter.calls for text in texts)
    assert await select_context_ids(memory, embeddings, **kwargs) == selected
    assert len(adapter.calls) == call_count
    assert (await memory.get_node("node_00000"))["strength"] == 0.01
    assert (await memory.get_node("node_00001"))["content_version"] == 1
    assert (await memory.get_active_generation(space["space_id"]))["activity_round"] == 0


@pytest.mark.asyncio
async def test_candidate_scan_excludes_other_scopes_retired_and_disabled_nodes(context_env):
    memory, _, space, generation, _, embeddings = context_env
    await seed_nodes(memory, space, generation, 8)
    for index, status in enumerate(("historical", "disabled", "deleted", "superseded"), start=1):
        await memory.database.execute("UPDATE memory_nodes SET status=? WHERE node_id=?", (status, f"node_{index:05}"))
    await memory.database.execute("UPDATE memory_nodes SET metadata_json=? WHERE node_id='node_00005'", ('{"summary_stale":true}',))
    other = await memory.get_or_create_space(private_space("synthetic-persona", "another-user"))
    await memory.create_node(space_id=other["space_id"], node_type="event", content_text="预约外部资料。", source_ids=["synthetic"])
    building = await memory.create_generation(space["space_id"])
    await memory.create_node(space_id=space["space_id"], generation_id=building["generation_id"], node_type="event",
                             content_text="预约新构建资料。", source_ids=["synthetic"])
    selected = await select_context_ids(memory, embeddings, space_id=space["space_id"],
        generation_id=generation["generation_id"], sources=[{"content_text": "预约进展"}], limit=6, timeout_seconds=10)
    assert set(selected) == {"node_00000", "node_00006", "node_00007"}
    with pytest.raises(ValueError, match="outside"):
        await memory.extraction_context_page(other["space_id"], generation["generation_id"])


@pytest.mark.asyncio
async def test_cached_message_vector_is_reused_without_strength_or_model_side_effect(context_env):
    memory, _, space, generation, adapter, embeddings = context_env
    await seed_nodes(memory, space, generation, 2)
    query = "三月预约参观科技馆。"
    await embeddings.embed([query], namespace=space["space_id"])
    selected = await select_context_ids(memory, embeddings, space_id=space["space_id"],
        generation_id=generation["generation_id"], sources=[{"content_text": query}], limit=2, timeout_seconds=10)
    assert set(selected) == {"node_00000", "node_00001"}
    assert sum(text == query for texts in adapter.calls for text in texts) == 1
    assert await memory.database.fetchall("SELECT * FROM recall_reinforcements") == []


@pytest.mark.asyncio
async def test_uncached_query_failure_is_not_silently_replaced_by_recent_only(context_env):
    memory, _, space, generation, adapter, embeddings = context_env
    await seed_nodes(memory, space, generation, 2)
    adapter.failure = TimeoutError("synthetic timeout")
    with pytest.raises(TimeoutError):
        await select_context_ids(memory, embeddings, space_id=space["space_id"],
            generation_id=generation["generation_id"], sources=[{"content_text": "预约"}], limit=2, timeout_seconds=10)
    assert len(adapter.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [0, 1, 2, 3, 4])
async def test_small_candidate_budgets_never_overflow_when_recent_and_semantic_differ(context_env, limit):
    memory, _, space, generation, adapter, embeddings = context_env
    await seed_nodes(memory, space, generation, 8)
    selected = await select_context_ids(memory, embeddings, space_id=space["space_id"],
        generation_id=generation["generation_id"], sources=[{"content_text": "预约进展"}],
        limit=limit, timeout_seconds=10)
    assert len(selected) == limit
    if limit == 0:
        assert adapter.calls == []
    else:
        assert selected[0] == "node_00007"


def test_ranking_ignores_weak_query_coverage():
    nodes = [{"node_id": "a"}, {"node_id": "b"}]
    vectors = [[1.0, 0.0], [0.0, 1.0]]
    queries = [[1.0, 0.0], [0.0, 0.1]]
    best, coverage = [], {}
    _rank_page_with_query_coverage(best, coverage, nodes, vectors, queries, 1)
    assert coverage == {0: (1.0, "a")}
    assert best == [(1.0, "a")]
