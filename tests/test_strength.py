import math

import pytest

from core.strength import StrengthPolicy, calculate_strength, recall_increment


def test_new_or_reset_memory_starts_at_current_importance():
    result = calculate_strength(0.7, elapsed_seconds=0, elapsed_rounds=0)
    assert result.value == 0.7
    assert result.time_loss == result.activity_loss == result.reinforcement == 0


def test_time_loss_slows_and_cannot_exceed_cap_after_long_absence():
    policy = StrengthPolicy()
    first = calculate_strength(
        0.8, elapsed_seconds=policy.time_scale_seconds, elapsed_rounds=0
    )
    second = calculate_strength(
        0.8, elapsed_seconds=2 * policy.time_scale_seconds, elapsed_rounds=0
    )
    dormant = calculate_strength(0.8, elapsed_seconds=10**15, elapsed_rounds=0)
    assert 0 < second.time_loss - first.time_loss < first.time_loss
    assert dormant.time_loss == policy.max_time_loss
    assert dormant.value == pytest.approx(0.65)


def test_activity_rounds_are_independent_of_time_and_bounded():
    result = calculate_strength(0.8, elapsed_seconds=0, elapsed_rounds=10)
    assert result.time_loss == 0
    assert result.activity_loss == pytest.approx(0.01 / 1.8)
    assert result.value == pytest.approx(0.8 - 0.01 / 1.8)
    saturated = calculate_strength(0.8, elapsed_seconds=0, elapsed_rounds=1_000_000)
    assert saturated.activity_loss == StrengthPolicy().max_activity_loss


def test_reinforcement_is_capped_and_final_score_stays_in_range():
    result = calculate_strength(
        0.95, elapsed_seconds=0, elapsed_rounds=0, reinforcement=1000
    )
    assert result.reinforcement == StrengthPolicy().max_reinforcement
    assert result.value == 1
    faded = calculate_strength(0.1, elapsed_seconds=10**15, elapsed_rounds=1_000_000)
    assert faded.value == 0


def test_custom_policy_can_disable_each_component():
    policy = StrengthPolicy(time_rate=0, activity_loss_per_round=0, max_reinforcement=0)
    result = calculate_strength(
        0.7,
        elapsed_seconds=10**15,
        elapsed_rounds=1_000_000,
        reinforcement=1000,
        policy=policy,
    )
    assert result.value == 0.7
    assert result.time_loss == result.activity_loss == result.reinforcement == 0


def test_lazy_queries_are_repeatable_and_do_not_change_input_state():
    values = {
        "baseline": 0.8,
        "elapsed_seconds": 100_000,
        "elapsed_rounds": 10,
        "reinforcement": 0.03,
    }
    first = calculate_strength(**values)
    second = calculate_strength(**values)
    assert first == second
    assert values["elapsed_rounds"] == 10


def test_finite_extreme_time_and_scale_do_not_overflow():
    result = calculate_strength(
        0.8,
        elapsed_seconds=1e308,
        elapsed_rounds=0,
        policy=StrengthPolicy(time_scale_seconds=1e-308),
    )
    assert math.isfinite(result.value)
    assert result.time_loss == StrengthPolicy().max_time_loss


@pytest.mark.parametrize(
    "overrides",
    [
        {"baseline": -1},
        {"baseline": 1.1},
        {"baseline": math.nan},
        {"importance": -1},
        {"importance": 1.1},
        {"importance": math.nan},
        {"elapsed_seconds": -1},
        {"elapsed_seconds": math.inf},
        {"elapsed_rounds": -1},
        {"elapsed_rounds": 1.5},
        {"elapsed_rounds": True},
        {"reinforcement": -0.1},
        {"reinforcement": math.nan},
        {"reinforcement": math.inf},
        {"importance": True},
    ],
)
def test_invalid_inputs_are_rejected(overrides):
    values = {"baseline": 0.5, "elapsed_seconds": 0, "elapsed_rounds": 0}
    values.update(overrides)
    with pytest.raises(ValueError):
        calculate_strength(**values)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"time_scale_seconds": 0},
        {"time_scale_seconds": math.inf},
        {"time_rate": -0.1},
        {"max_time_loss": 1.1},
        {"activity_loss_per_round": math.nan},
        {"max_activity_loss": -1},
        {"max_reinforcement": math.inf},
        {"time_rate": True},
        {"max_time_loss": 0.151},
        {"max_activity_loss": 0.601},
        {"max_reinforcement": 0.201},
        {"max_turn_reinforcement": 0.121},
        {"importance_decay_weight": -1},
        {"reinforcement_gain": math.nan},
        {"neighbor_multiplier": 1.1},
        {"active_multiplier": 0.9},
        {"diminishing_power": 0},
        {"cooldown_seconds": 86401},
        {"wake_margin": 0.101},
    ],
)
def test_invalid_policy_is_rejected(kwargs):
    with pytest.raises(ValueError):
        StrengthPolicy(**kwargs)


def test_higher_importance_slows_both_losses_without_changing_caps():
    values = dict(elapsed_seconds=14 * 86400, elapsed_rounds=200)
    ordinary = calculate_strength(0.5, importance=0.1, **values)
    significant = calculate_strength(0.5, importance=0.9, **values)
    assert 0 < significant.time_loss < ordinary.time_loss
    assert 0 < significant.activity_loss < ordinary.activity_loss
    assert significant.value > ordinary.value
    for importance in (0.1, 0.9):
        result = calculate_strength(0.5, importance=importance,
                                    elapsed_seconds=1e100, elapsed_rounds=1_000_000)
        assert result.time_loss == StrengthPolicy().max_time_loss
        assert result.activity_loss == StrengthPolicy().max_activity_loss


def test_baseline_and_importance_have_separate_roles_after_awakening():
    low = calculate_strength(0.11, importance=0.8, elapsed_seconds=86400, elapsed_rounds=3)
    high = calculate_strength(0.7, importance=0.8, elapsed_seconds=86400, elapsed_rounds=3)
    assert low.time_loss == high.time_loss
    assert low.activity_loss == high.activity_loss
    assert high.value - low.value == pytest.approx(0.59)


def test_importance_slowdown_can_be_disabled_without_disabling_decay():
    policy = StrengthPolicy(importance_decay_weight=0)
    values = dict(elapsed_seconds=86400, elapsed_rounds=10, policy=policy)
    assert calculate_strength(0.5, importance=0.1, **values) == calculate_strength(0.5, importance=0.9, **values)


@pytest.mark.parametrize("direct,active", [(False, False), (True, False), (False, True), (True, True)])
def test_ordinary_reinforcement_diminishes_with_strength(direct, active):
    gains = [recall_increment(value, direct=direct, active=active) for value in (0, 0.2, 0.8, 1)]
    assert gains[0] > gains[1] > gains[2] > gains[3] == 0


def test_reinforcement_keeps_direct_and_active_weighting_at_equal_strength():
    passive_neighbor = recall_increment(0.5, direct=False, active=False)
    passive_direct = recall_increment(0.5, direct=True, active=False)
    active_neighbor = recall_increment(0.5, direct=False, active=True)
    active_direct = recall_increment(0.5, direct=True, active=True)
    assert passive_direct == 4 * passive_neighbor
    assert active_neighbor == 2 * passive_neighbor
    assert active_direct == 2 * passive_direct
