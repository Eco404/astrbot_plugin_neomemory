"""Topic overviews retain useful event context without another model request."""

from dataclasses import replace

import pytest

from core.active_recall import ActiveFilters
from core.retrieval import RecallPolicy, RecallService, rank_candidates, render_nodes, select_context
from tests.test_retrieval import NOW, FakeEmbeddings, FakeMemory, memory_node, snapshot


def member(topic, event):
    return {"source_node_id": topic, "target_node_id": event, "relation_type": "contains"}


def test_topic_members_precede_other_hits_and_use_query_relevance_not_edge_order():
    topic = memory_node("topic", kind="topic")
    events = [memory_node("low", kind="event"), memory_node("high", kind="event")]
    unrelated = [memory_node(f"other-{n}") for n in range(5)]
    data = snapshot(topic, *unrelated, *events, edges=[member("topic", "low"), member("topic", "high")])
    vectors = [[1, 0]] + [[.9, .1]] * 5 + [[.1, .9], [.2, .8]]
    policy = RecallPolicy(budget_tokens=8000)
    ranked = rank_candidates(data, vectors, [([1, 0], 1., True)], policy, NOW)
    selected = select_context(ranked, [], policy)
    assert [n["node_id"] for n in selected[:3]] == ["topic", "high", "low"]
    assert len(selected) == policy.max_nodes
    assert all(n["connection"] == "主题成员" and n["topic_id"] == "topic" for n in selected[1:3])
    assert all(not n["direct"] for n in selected[1:3])
    one = rank_candidates(data, vectors, [([1, 0], 1., True)], replace(policy, max_neighbors=1), NOW)
    assert [n["node_id"] for n in one[:2]] == ["topic", "high"]
    assert "low" not in {n["node_id"] for n in one}


def test_topic_expansion_keeps_direct_member_flags_and_does_not_duplicate_shared_members():
    data = snapshot(memory_node("a", kind="topic"), memory_node("b", kind="topic"),
                    memory_node("event", kind="event"),
                    edges=[member("a", "event"), member("b", "event")])
    ranked = rank_candidates(data, [[1, 0], [.9, .1], [.8, .2]], [([1, 0], 1., True)], RecallPolicy(), NOW)
    assert [n["node_id"] for n in ranked] == ["a", "event", "b"]
    assert ranked[1]["direct"] and ranked[1]["current"]
    assert ranked[1]["topic_id"] == "a"


def test_topic_can_fit_alone_and_contains_does_not_claim_coverage():
    data = snapshot(memory_node("topic", kind="topic"), memory_node("event", kind="event"),
                    memory_node("atomic"), edges=[member("topic", "event"), member("topic", "atomic")])
    ranked = rank_candidates(data, [[1, 0], [.2, .8], [.8, .2]], [([1, 0], 1., True)], RecallPolicy(), NOW)
    exact = len(render_nodes(ranked[:1]).encode())
    selected = select_context(ranked, [], RecallPolicy(budget_tokens=exact))
    assert [n["node_id"] for n in selected] == ["topic"]
    assert {n["node_id"] for n in select_context(ranked, [], RecallPolicy(budget_tokens=8000))} == {"topic", "event", "atomic"}


def test_topic_does_not_wake_weak_members_during_passive_recall():
    data = snapshot(memory_node("topic", kind="topic"), memory_node("weak", kind="event", strength=.01),
                    edges=[member("topic", "weak")])
    ranked = rank_candidates(data, [[1, 0], [.2, .8]], [([1, 0], 1., True)], RecallPolicy(), NOW)
    assert [n["node_id"] for n in ranked] == ["topic"]
    disabled = rank_candidates(data, [[1, 0], [.2, .8]], [([1, 0], 1., True)], RecallPolicy(max_neighbors=0), NOW, active=True)
    assert [n["node_id"] for n in disabled] == ["topic"]


@pytest.mark.asyncio
async def test_active_topic_filter_stays_strict_with_member_edges():
    data = snapshot(memory_node("topic", kind="topic"), memory_node("event", kind="event"),
                    edges=[member("topic", "event")])
    embeddings = FakeEmbeddings({node["content_text"]: [1, 0] for node in data["nodes"]} | {"query": [1, 0]})
    result = await RecallService(FakeMemory(data), embeddings).recall(
        "space", "query", now=NOW, active=True, node_filter=ActiveFilters(kind="topic").matches)
    assert [n["node_id"] for n in result.hits] == ["topic"]
