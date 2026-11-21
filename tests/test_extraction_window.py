import math

import pytest

from core.extraction_window import (
    WindowMessage,
    WindowPolicy,
    estimate_tokens,
    select_window,
)


def messages(count, text="内容"):
    return [WindowMessage(f"source-{index}", text) for index in range(count)]


@pytest.mark.parametrize("count", [0, 1, 4])
def test_below_minimum_ignores_idle_topic_and_length_triggers(count):
    result = select_window(
        messages(count, "x" * 100),
        idle_seconds=100_000,
        trusted_topic_switch=True,
        policy=WindowPolicy(max_chars=10),
    )
    assert result.status == "waiting"
    assert result.reason == "below_minimum"
    assert not result.source_ids


@pytest.mark.parametrize(
    ("count", "idle", "topic", "status", "reason"),
    [
        (5, 7199, True, "waiting", "awaiting_trigger"),
        (5, 7200, False, "ready", "idle_timeout"),
        (19, 0, True, "waiting", "awaiting_trigger"),
        (20, 0, False, "waiting", "awaiting_trigger"),
        (20, 0, True, "ready", "topic_switch"),
        (39, 7200, False, "ready", "idle_timeout"),
        (40, 0, False, "ready", "message_limit"),
        (45, 0, False, "ready", "message_limit"),
    ],
)
def test_count_idle_and_trusted_topic_boundaries(count, idle, topic, status, reason):
    result = select_window(
        messages(count), idle_seconds=idle, trusted_topic_switch=topic,
        topic_boundary_index=15 if topic else None,
    )
    assert (result.status, result.reason) == (status, reason)
    if status == "ready":
        assert result.source_ids == tuple(
            f"source-{index}" for index in range(15 if reason == "topic_switch" else min(count, 40))
        )


@pytest.mark.parametrize("boundary", [None, 0, 4, 20])
def test_topic_trigger_without_usable_old_prefix_keeps_all_messages_pending(boundary):
    result = select_window(messages(20), idle_seconds=0, trusted_topic_switch=True,
                           topic_boundary_index=boundary)
    assert (result.status, result.reason, result.source_ids) == ("waiting", "awaiting_trigger", ())


def test_topic_prefix_recounts_budget_and_leaves_new_topic_sources_pending():
    pending = messages(20, "有意义的内容")
    result = select_window(pending, idle_seconds=0, trusted_topic_switch=True, topic_boundary_index=13)
    assert result.source_ids == tuple(item.source_id for item in pending[:13])
    assert result.char_count == sum(len(item.content_text) for item in pending[:13])
    assert result.estimated_tokens == sum(estimate_tokens(item.content_text) for item in pending[:13])
    assert len(pending) == 20


@pytest.mark.parametrize("trigger", ["idle", "count", "length"])
def test_forced_triggers_retain_their_own_prefix_rules(trigger):
    policy = WindowPolicy(max_chars=20 if trigger == "length" else 24000)
    count, idle = (40, 0) if trigger == "count" else (20, 7200 if trigger == "idle" else 0)
    result = select_window(messages(count), idle_seconds=idle, trusted_topic_switch=True,
                           topic_boundary_index=5, policy=policy)
    assert result.reason == {"idle": "idle_timeout", "count": "message_limit", "length": "length_budget"}[trigger]
    assert len(result.source_ids) == {"idle": 20, "count": 40, "length": 10}[trigger]


def test_group_default_count_and_idle_thresholds():
    policy = WindowPolicy.for_group()
    assert (policy.min_messages, policy.topic_messages, policy.max_messages) == (
        40,
        100,
        200,
    )
    assert (
        select_window(messages(39), idle_seconds=1800, policy=policy).status
        == "waiting"
    )
    assert (
        select_window(messages(40), idle_seconds=1799, policy=policy).status
        == "waiting"
    )
    assert (
        select_window(messages(40), idle_seconds=1800, policy=policy).status == "ready"
    )


def test_character_budget_selects_whole_prefix_and_preserves_pending_remainder():
    pending = messages(5, "abc")
    result = select_window(pending, idle_seconds=0, policy=WindowPolicy(max_chars=8))
    assert (result.status, result.reason) == ("ready", "length_budget")
    assert result.source_ids == ("source-0", "source-1")
    assert result.char_count == 6
    assert len(pending) == 5
    assert all(message.content_text == "abc" for message in pending)
    assert pending[len(result.source_ids)].source_id == "source-2"


def test_token_budget_counts_unicode_and_can_trigger_before_character_limit():
    assert estimate_tokens("你好") == len("你好".encode()) + 8
    result = select_window(
        messages(5, "你好"), idle_seconds=0, policy=WindowPolicy(max_tokens=28)
    )
    assert result.status == "ready"
    assert result.source_ids == ("source-0", "source-1")
    assert result.char_count == 4
    assert result.estimated_tokens == 28


@pytest.mark.parametrize(
    "policy", [WindowPolicy(max_chars=3), WindowPolicy(max_tokens=10)]
)
def test_oversized_first_message_is_explicit_and_never_truncated(policy):
    pending = messages(5, "unabridged message")
    result = select_window(pending, idle_seconds=0, policy=policy)
    assert result.status == "oversized"
    assert result.reason == "single_message_exceeds_budget"
    assert result.blocked_source_id == "source-0"
    assert result.source_ids == ()
    assert result.char_count == len("unabridged message")
    assert pending[0].content_text == "unabridged message"


def test_later_arrivals_cannot_change_selected_window():
    pending = messages(5)
    result = select_window(pending, idle_seconds=7200)
    pending.append(WindowMessage("new-arrival", "later"))
    assert result.source_ids == tuple(f"source-{index}" for index in range(5))


def test_duplicate_source_ids_are_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        select_window(
            [WindowMessage("same", "a"), WindowMessage("same", "b")], idle_seconds=0
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"min_messages": 0},
        {"topic_messages": 4},
        {"max_messages": 19},
        {"max_chars": True},
        {"max_tokens": 0},
        {"idle_seconds": math.inf},
        {"idle_seconds": math.nan},
        {"idle_seconds": -1},
    ],
)
def test_invalid_policy_is_rejected(kwargs):
    with pytest.raises(ValueError):
        WindowPolicy(**kwargs)


@pytest.mark.parametrize("idle", [-1, math.inf, math.nan, True])
def test_invalid_elapsed_idle_is_rejected(idle):
    with pytest.raises(ValueError):
        select_window(messages(5), idle_seconds=idle)
