"""主动查询的事件时间语义、完整条目预算与笔记区分。"""

import json
from datetime import datetime, timezone

import pytest

from core.active_recall import ActiveFilters, merge_active_result
from core.retrieval import RecallPolicy, RecallResult, render_nodes, select_context


def node(identifier="memory-1", content="周末看展。", **kwargs):
    return {"node_id": identifier, "node_type": "event", "status": "active",
            "content_text": content, "content_version": 1, "direct": True, **kwargs}


def note(identifier="note-1", content="准备看展。"):
    return {"note_id": identifier, "status": "active", "content_text": content}


def entries(text):
    return json.loads(text.split("\n", 1)[1])


def test_utc_dates_include_whole_day_and_intersect_event_intervals():
    filters = ActiveFilters.parse(event_after="2026-11-13", event_before="2026-11-13")
    start = datetime(2026, 11, 13, tzinfo=timezone.utc).timestamp()
    assert filters.event_after == start
    assert filters.event_before == start + 86400 - .000001
    assert filters.matches(node(event_time_start=start - 10, event_time_end=start + 10))
    assert filters.matches(node(event_time_start=start + 86399))
    assert filters.matches(node(event_time_start=None, event_time_end=start + 60))
    assert not filters.matches(node(event_time_start=start + 86400))
    assert not filters.matches(node(event_time_start=None, event_time_end=start + 86400))
    assert not filters.matches(node(event_time_start=None, created_at=start, updated_at=start))
    assert not filters.includes_notes
    offset = ActiveFilters.parse(event_after="2026-11-13T08:00:00+08:00", event_before="2026-11-13T00:00:00Z")
    assert offset.event_after == offset.event_before == start


@pytest.mark.parametrize("kind", ["all", "memory", "atomic", "event", "topic", "note"])
def test_kind_filters_preserve_distinction_between_notes_and_memory_types(kind):
    filters = ActiveFilters.parse(kind=kind, default_limit=7)
    assert filters.limit == 7
    for node_type in ("atomic", "event", "topic"):
        assert filters.matches(node(node_type=node_type)) == (kind in {"all", "memory", node_type})
    assert filters.includes_notes == (kind in {"all", "note"})


@pytest.mark.parametrize("kwargs", [
    {"kind": "unknown"}, {"limit": -1}, {"limit": 31}, {"limit": 1.5}, {"limit": True},
    {"event_after": "2026-11-13T08:00:00"}, {"event_before": "not a date"},
    {"event_after": "2026-11-14", "event_before": "2026-11-13"},
])
def test_invalid_filter_input_is_explicit(kwargs):
    with pytest.raises(ValueError):
        ActiveFilters.parse(**kwargs)


def test_memory_and_notes_interleave_whole_entries_under_one_count_limit():
    result = RecallResult("space", hits=[node("m1"), node("m2"), node("m3")])
    merge_active_result(result, [note("n1"), note("n2"), note("n3")], limit=4, budget=4000)
    payload = entries(result.text)
    assert [(entry["source"], entry["id"]) for entry in payload] == [
        ("memory", "m1"), ("note", "n1"), ("memory", "m2"), ("note", "n2"),
    ]
    assert [item["node_id"] for item in result.hits] == ["m1", "m2"]
    assert result.diagnostics["returned_notes"] == 2
    assert "不证明事情已发生" in result.text


def test_over_budget_entry_is_skipped_whole_and_smaller_later_entries_can_fit():
    result = RecallResult("space", hits=[node("large", "长" * 2000), node("small", "短")])
    merge_active_result(result, [note("n1", "草稿")], limit=3, budget=1000)
    assert len(result.text.encode("utf-8")) <= 1000
    assert [entry["id"] for entry in entries(result.text)] == ["n1", "small"]
    assert "长" not in result.text
    assert [item["node_id"] for item in result.hits] == ["small"]
    too_small = RecallResult("space", hits=[node()])
    assert merge_active_result(too_small, [note()], limit=2, budget=20) == ""
    assert too_small.hits == []


def test_memory_rendering_preserves_name_and_emotion_metadata_without_using_title():
    result = RecallResult("space", user_names={"u": "小禾"}, hits=[node(
        content="与{{user:u}}约好看展。", title="仅展示标题",
        emotion={"tags": ["期待"], "intensity": .5, "description": "期待与{{user:u}}同行。"},
    )])
    merge_active_result(result, [], limit=1, budget=2000)
    payload = entries(result.text)[0]
    assert payload["memory"] == "与小禾约好看展。"
    assert payload["emotion"]["description"] == "期待与小禾同行。"
    assert "仅展示标题" not in result.text


def covers(event, atomic):
    return {"event_node_id": event["node_id"], "atomic_node_id": atomic["node_id"],
            "event_content_version": 1, "atomic_content_version": 1, "coverage_kind": "full"}


def test_active_envelope_refits_atomic_omitted_by_preliminary_event_coverage():
    atomic = node("atomic", "已确定的细节。", node_type="atomic")
    event = node("event", "事件整体叙述。" * 20)
    coverage = [covers(event, atomic)]
    budget = len(render_nodes([event]).encode("utf-8"))
    preliminary = select_context([atomic, event], coverage, RecallPolicy(budget_tokens=budget))
    assert preliminary == [event]
    result = RecallResult("space", hits=preliminary, candidates=[atomic, event])
    merge_active_result(result, [], limit=2, budget=budget, coverage=coverage)
    assert [item["id"] for item in entries(result.text)] == ["atomic"]
    assert result.hits == [atomic]
    assert len(result.text.encode("utf-8")) <= budget


def test_notes_budget_keeps_atomic_when_covering_event_no_longer_fits():
    first = node("first", "其他经历。")
    atomic = node("atomic", "已确定的细节。", node_type="atomic")
    event = node("event", "事件整体叙述。" * 60)
    notes = [note("note", "未来计划。" * 10)]
    fitting = RecallResult("space", hits=[first, atomic])
    merge_active_result(fitting, notes, limit=3, budget=10000)
    budget = len(fitting.text.encode("utf-8"))
    result = RecallResult("space", hits=[first, event], candidates=[first, atomic, event])
    merge_active_result(result, notes, limit=3, budget=budget, coverage=[covers(event, atomic)])
    assert [item["id"] for item in entries(result.text)] == ["first", "note", "atomic"]
    assert [item["node_id"] for item in result.hits] == ["first", "atomic"]
    assert len(result.text.encode("utf-8")) == budget


def test_event_promotion_and_notes_share_actual_count_limit_without_duplicate_atomic():
    atomic = node("atomic", "已确定的细节。", node_type="atomic")
    event = node("event", "事件整体叙述。")
    result = RecallResult("space", hits=[event], candidates=[atomic, event])
    merge_active_result(result, [note("note1"), note("note2")], limit=2, budget=3000,
                        coverage=[covers(event, atomic)])
    assert [item["id"] for item in entries(result.text)] == ["event", "note1"]
    assert result.hits == [event]
