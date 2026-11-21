"""Pure, bounded memory-strength calculation; never updates usage or counters."""

from __future__ import annotations

import math
from dataclasses import dataclass


def _finite_nonnegative(
    value: float, name: str, *, maximum: float | None = None
) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a finite nonnegative number")
    try:
        valid = math.isfinite(value) and value >= 0
    except OverflowError:
        valid = False
    if not valid or (maximum is not None and value > maximum):
        raise ValueError(f"{name} is outside its finite nonnegative range")


@dataclass(frozen=True, slots=True)
class StrengthPolicy:
    time_scale_seconds: float = 7 * 24 * 3600
    time_rate: float = 0.03
    max_time_loss: float = 0.15
    activity_loss_per_round: float = 0.001
    max_activity_loss: float = 0.6
    max_reinforcement: float = 0.2
    importance_decay_weight: float = 1.0
    reinforcement_gain: float = 0.02
    neighbor_multiplier: float = 0.25
    active_multiplier: float = 2.0
    diminishing_power: float = 1.0
    cooldown_seconds: float = 300.0
    max_turn_reinforcement: float = 0.12
    wake_margin: float = 0.01

    def __post_init__(self) -> None:
        _finite_nonnegative(self.time_scale_seconds, "time_scale_seconds")
        if self.time_scale_seconds == 0:
            raise ValueError("time_scale_seconds must be positive")
        for name, maximum in (
            ("time_rate", 1.0), ("max_time_loss", 0.15),
            ("activity_loss_per_round", 1.0), ("max_activity_loss", 0.6),
            ("max_reinforcement", 0.2), ("importance_decay_weight", 10.0),
            ("reinforcement_gain", 0.2), ("neighbor_multiplier", 1.0),
            ("active_multiplier", 10.0), ("diminishing_power", 4.0),
            ("cooldown_seconds", 86400.0), ("max_turn_reinforcement", 0.12),
            ("wake_margin", 0.1),
        ):
            _finite_nonnegative(getattr(self, name), name, maximum=maximum)
        if self.active_multiplier < 1:
            raise ValueError("active_multiplier must be at least one")
        if self.diminishing_power <= 0:
            raise ValueError("diminishing_power must be positive")


@dataclass(frozen=True, slots=True)
class StrengthResult:
    value: float
    time_loss: float
    activity_loss: float
    reinforcement: float


def _log1p_ratio(numerator: float, denominator: float) -> float:
    # Avoid overflowing numerator / denominator for otherwise valid finite inputs.
    if numerator <= denominator:
        return math.log1p(numerator / denominator)
    return (
        math.log(numerator)
        - math.log(denominator)
        + math.log1p(denominator / numerator)
    )


def calculate_strength(
    baseline: float,
    *,
    importance: float | None = None,
    elapsed_seconds: float,
    elapsed_rounds: int,
    reinforcement: float = 0.0,
    policy: StrengthPolicy = StrengthPolicy(),
) -> StrengthResult:
    """按节点重置后的时间与同空间轮次懒计算，不写状态或访问计数。

    baseline 为当前可重设的强度基准；importance 为节点内容重要性。
    高重要性降低损失速度，时间和活动损失仍分别受累计上限约束。
    reinforcement是经过实际交付、去重、冷却与额度检查的普通强化总量。
    """
    _finite_nonnegative(baseline, "baseline", maximum=1.0)
    importance = baseline if importance is None else importance
    _finite_nonnegative(importance, "importance", maximum=1.0)
    _finite_nonnegative(elapsed_seconds, "elapsed_seconds")
    if type(elapsed_rounds) is not int:
        raise ValueError("elapsed_rounds must be a nonnegative integer")
    _finite_nonnegative(elapsed_rounds, "elapsed_rounds")
    _finite_nonnegative(reinforcement, "reinforcement")

    decay_rate = 1.0 / (1.0 + policy.importance_decay_weight * importance)
    time_loss = min(
        policy.max_time_loss,
        policy.time_rate
        * _log1p_ratio(elapsed_seconds, policy.time_scale_seconds)
        * decay_rate,
    )
    activity_loss = min(
        policy.max_activity_loss, elapsed_rounds * policy.activity_loss_per_round * decay_rate
    )
    applied_reinforcement = min(policy.max_reinforcement, reinforcement)
    value = max(
        0.0, min(1.0, baseline - time_loss - activity_loss + applied_reinforcement)
    )
    return StrengthResult(value, time_loss, activity_loss, applied_reinforcement)


def recall_increment(
    current_strength: float,
    *,
    direct: bool,
    active: bool,
    policy: StrengthPolicy = StrengthPolicy(),
) -> float:
    """计算普通命中增益；冷却、幂等与总额度仍由存储事务统一裁决。"""
    _finite_nonnegative(current_strength, "current_strength", maximum=1.0)
    weight = policy.reinforcement_gain
    if not direct:
        weight *= policy.neighbor_multiplier
    if active:
        weight *= policy.active_multiplier
    return weight * (1.0 - current_strength) ** policy.diminishing_power
