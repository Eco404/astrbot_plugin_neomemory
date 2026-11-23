import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from adapters.providers import ProviderAdapter
from core.adaptive_recall import contextual_query, selection_request
from core.config import NeoMemoryConfig
from core.recall_history import recall_snapshot
from core.retrieval import RecallPolicy, RecallService
from tests.test_retrieval import FakeEmbeddings, FakeMemory, NOW, memory_node, snapshot


def service(query, *, rerank=None, selector=None, **options):
    first = memory_node("first", text="北岸货架换了卡扣。")
    other = memory_node("other", text="乐团的琴弦换了弦轴。")
    vectors = {first["content_text"]: [1., 0.], other["content_text"]: [.4, .9165], query: [1., 0.]}
    return RecallService(FakeMemory(snapshot(first, other)), FakeEmbeddings(vectors),
                         policy=RecallPolicy(**options), rerank=rerank, evidence_selector=selector)


@pytest.mark.parametrize("saved,expected", [({}, "adaptive"), ({"recall_selection_mode": "baseline"}, "baseline"),
    ({"recall_selection_mode": "gated"}, "gated"), ({"recall_selection_mode": "adaptive"}, "adaptive")])
def test_config_preserves_selected_strategy(saved, expected):
    assert NeoMemoryConfig.from_mapping(saved).recall_selection_mode == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("active", [True, False])
async def test_overview_selects_from_wide_pool_before_delivery(active):
    query = "我们修过哪些设备？"
    async def choose(space, history, origin, text, docs, limit, timeout):
        assert space == "space" and text == query and len(docs) == 2 and timeout > 0
        return [next(i for i, d in enumerate(docs) if "琴弦" in d)]
    async def rerank(*args):
        pytest.fail("overview already selected evidence")
    result = await service(query, rerank=rerank, selector=choose).recall(
        "space", query, active=active, preview=not active, retain_candidates=True)
    assert result.diagnostics["evidence_selection"] == "selector"
    assert [n["node_id"] for n in result.candidates] == [n["node_id"] for n in result.hits] == ["other"]
    assert "卡扣" not in result.text


@pytest.mark.asyncio
async def test_zero_evidence_is_final_and_errors_fall_back():
    query = "哪些是之前修过的？"
    async def reject(*args):
        return []
    async def invalid(*args):
        return [-1]
    async def slow(*args):
        await asyncio.sleep(5)
    result = await service(query, rerank=slow, selector=reject).preview("space", query)
    assert not result.hits and "selection_fallback" not in result.diagnostics
    result = await service(query, rerank=reject, selector=invalid).preview("space", query)
    assert not result.hits and result.diagnostics["evidence_selection"] == "rerank"
    result = await service(query, rerank=slow, selector=invalid, deadline_seconds=.25).preview("space", query)
    assert [n["node_id"] for n in result.hits] == ["first"]
    assert result.diagnostics["evidence_selection"] == "local"
    assert result.diagnostics["selection_fallback"] == "TimeoutError"


@pytest.mark.asyncio
async def test_sparse_candidate_below_dense_cutoff_reaches_final_reranker():
    query = "ZX789"
    rare = memory_node("rare", title=query, text="合页要向内装。")
    near = memory_node("near", text="门锁换了。")
    vectors = {rare["content_text"]: [0., 1.], near["content_text"]: [1., 0.], query: [1., 0.]}
    async def rerank(text, docs):
        assert text == query
        return [next(i for i, doc in enumerate(docs) if query in doc)]
    result = await RecallService(FakeMemory(snapshot(near, rare)), FakeEmbeddings(vectors),
                                 rerank=rerank).preview("space", query)
    assert result.hits[0]["node_id"] == "rare" and result.hits[0]["similarity"] == 0
    assert any(row["node_id"] == "near" and "证据不足" in row["reason"] for row in result.diagnostics["preview_candidates"])
    assert recall_snapshot("test", query, 0, result)["selection"]["evidence_selection"] == "rerank"


def test_group_reference_uses_quoted_message_or_addressed_speaker():
    query = "那后来呢？"
    history = [
        dict(actor_kind="user", actor_account_id="other", message_id="m3", content_text="无关的插话"),
        dict(actor_kind="agent", actor_account_id="bot", message_id="m1", trigger_message_id="m1", content_text="对引用的回复"),
        dict(actor_kind="user", actor_account_id="speaker", message_id="m1", content_text="北岸的木桥"),
    ]
    def text(addresses, policy=RecallPolicy()):
        answer = contextual_query(query, history, policy, "speaker", addresses)
        return answer[0] if answer else None
    assert text([{"kind": "reply", "message_id": "m1"}]) == "北岸的木桥\n" + query
    assert text([{"kind": "mention", "account_id": "other"}]) == "无关的插话\n" + query
    assert text([]) == "对引用的回复\n" + query
    assert text([], replace(RecallPolicy(), agent_weight=0)) == "北岸的木桥\n" + query
    assert text([{"kind": "reply", "message_id": "out-of-window"}]) is None


@pytest.mark.asyncio
async def test_current_attachment_skips_old_memory_and_compare_can_recall():
    query = "对比一下这张图和上次的照片"
    s = service(query)
    assert (await s.recall("space", "看看刚才发的这张图")).diagnostics["current_attachment"]
    assert not (await s.recall("space", '<@42> [图片]')).hits
    assert (await s.recall("space", query)).hits


@pytest.mark.asyncio
async def test_host_reranking_uses_declared_score_scale():
    async def rank(q, docs, top_n):
        assert top_n == 2
        return [SimpleNamespace(index=1, relevance_score=.4), SimpleNamespace(index=0, relevance_score=.8)]
    provider = SimpleNamespace(rerank=rank)
    adapter = ProviderAdapter(SimpleNamespace(get_provider_by_id=lambda _: provider))
    assert await adapter.rerank("query", ["one", "two"], provider_id="r") == [0]
    provider.raw_scores = True
    assert await adapter.rerank("query", ["one", "two"], provider_id="r") == [0, 1]
    messages, schema = selection_request("query", ["one", "two"], 1)
    assert [d["编号"] for d in json.loads(messages[1]["content"])["候选"]] == [1, 2]
    assert schema["properties"]["ids"]["items"]["enum"] == [1, 2]
