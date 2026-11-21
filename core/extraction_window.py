"""Deterministic extraction windows over an ordered, immutable source snapshot.

The token estimate uses UTF-8 bytes plus a small per-message allowance. This is
deliberately conservative for common byte tokenizers, not a model tokenizer or a
guarantee that the complete prompt fits a provider's context window. Callers must
reserve space for instructions, existing memories and model output separately.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal


def _nonnegative_finite(value: float, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a finite nonnegative number")
    try:
        valid = math.isfinite(value) and value >= 0
    except OverflowError:
        valid = False
    if not valid:
        raise ValueError(f"{name} must be a finite nonnegative number")


@dataclass(frozen=True, slots=True)
class WindowMessage:
    source_id: str
    content_text: str

    def __post_init__(self) -> None:
        if not isinstance(self.source_id, str) or not self.source_id.strip():
            raise ValueError("source_id must be a nonempty string")
        if not isinstance(self.content_text, str):
            raise ValueError("content_text must be a string")


@dataclass(frozen=True, slots=True)
class WindowPolicy:
    min_messages: int = 5
    topic_messages: int = 20
    max_messages: int = 40
    idle_seconds: float = 7200
    max_chars: int = 24_000
    max_tokens: int = 24_000

    def __post_init__(self) -> None:
        for name in (
            "min_messages",
            "topic_messages",
            "max_messages",
            "max_chars",
            "max_tokens",
        ):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if not self.min_messages <= self.topic_messages <= self.max_messages:
            raise ValueError("message thresholds must satisfy min <= topic <= max")
        _nonnegative_finite(self.idle_seconds, "idle_seconds")

    @classmethod
    def for_group(cls) -> WindowPolicy:
        return cls(
            min_messages=40, topic_messages=100, max_messages=200, idle_seconds=1800
        )


@dataclass(frozen=True, slots=True)
class WindowDecision:
    status: Literal["waiting", "ready", "oversized"]
    reason: str
    source_ids: tuple[str, ...] = ()
    char_count: int = 0
    estimated_tokens: int = 0
    blocked_source_id: str | None = None


def estimate_tokens(content_text: str) -> int:
    """Conservative local budget estimate; excludes the rest of the prompt."""
    return len(content_text.encode("utf-8")) + 8


def select_window(
    messages: Sequence[WindowMessage],
    *,
    idle_seconds: float,
    trusted_topic_switch: bool = False,
    topic_boundary_index: int | None = None,
    policy: WindowPolicy = WindowPolicy(),
) -> WindowDecision:
    """Select a fixed prefix without treating the boundary as an event boundary.

    ``messages`` must already be ordered within a single memory space. Below the
    minimum pending count, no trigger, including a length limit, permits extraction.
    Above that minimum, a budget can produce a prefix shorter than the minimum.

    A first message that cannot fit is explicitly rejected as ``oversized``. The
    caller must retain it pending and expose the failure or split it with tracked
    source offsets in a later layer; this function never truncates or consumes it.
    A selected window is identified by its fixed source IDs, so later arrivals
    cannot expand an in-flight extraction.

    仅话题触发时按照可信切点收束旧前缀；新话题留待下一窗口。旧前缀
    必须达到最低数量。长度、数量和空闲触发仍按各自规则处理。
    """
    _nonnegative_finite(idle_seconds, "idle_seconds")
    if not isinstance(trusted_topic_switch, bool):
        raise ValueError("trusted_topic_switch must be a boolean")
    snapshot = tuple(messages)
    if len({item.source_id for item in snapshot}) != len(snapshot):
        raise ValueError("a window snapshot must not contain duplicate source IDs")
    if len(snapshot) < policy.min_messages:
        return WindowDecision("waiting", "below_minimum")

    selected: list[str] = []
    chars = tokens = 0
    budget_reached = False
    for message in snapshot[: policy.max_messages]:
        item_chars = len(message.content_text)
        item_tokens = estimate_tokens(message.content_text)
        if (
            chars + item_chars > policy.max_chars
            or tokens + item_tokens > policy.max_tokens
        ):
            if not selected:
                return WindowDecision(
                    "oversized",
                    "single_message_exceeds_budget",
                    char_count=item_chars,
                    estimated_tokens=item_tokens,
                    blocked_source_id=message.source_id,
                )
            budget_reached = True
            break
        selected.append(message.source_id)
        chars += item_chars
        tokens += item_tokens
        if chars == policy.max_chars or tokens == policy.max_tokens:
            budget_reached = True
            break

    if budget_reached:
        reason = "length_budget"
    elif len(snapshot) >= policy.max_messages:
        reason = "message_limit"
    elif idle_seconds >= policy.idle_seconds:
        reason = "idle_timeout"
    elif (len(snapshot) >= policy.topic_messages and trusted_topic_switch
          and topic_boundary_index is not None
          and policy.min_messages <= topic_boundary_index < len(snapshot)):
        reason = "topic_switch"
        selected = selected[:topic_boundary_index]
        chars = sum(len(item.content_text) for item in snapshot[:topic_boundary_index])
        tokens = sum(estimate_tokens(item.content_text) for item in snapshot[:topic_boundary_index])
    else:
        return WindowDecision("waiting", "awaiting_trigger")
    return WindowDecision("ready", reason, tuple(selected), chars, tokens)
