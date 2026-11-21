"""Coverage experiments must expose information loss and capacity tradeoffs."""

from __future__ import annotations

import copy
import json

import pytest

from core.retrieval import RecallPolicy, render_nodes, select_context
from scripts.coverage_policy_probe import (
    BUDGETS,
    POLICIES,
    evaluate,
    filter_coverage,
    main,
    run_probe,
    synthetic_cases,
)


def case(case_id):
    return next(item for item in synthetic_cases() if item["id"] == case_id)


def test_experiment_uses_production_selector_and_does_not_mutate_inputs():
    value = case("necessary_condition")
    before = copy.deepcopy(value)
    result = evaluate(value, "declared_full", RecallPolicy())
    assert result["text"] == render_nodes(select_context(value["ranked"], value["coverage"], RecallPolicy()))
    assert result["missing_facts"] == ["safety_officer_required"]
    assert evaluate(value, "keep_both", RecallPolicy())["missing_facts"] == []
    assert value == before


@pytest.mark.parametrize("case_id", ["quoted_unverified", "historical_quote"])
def test_literal_inclusion_is_not_treated_as_semantic_equivalence(case_id):
    value = case(case_id)
    result = evaluate(value, "literal_contains", RecallPolicy())
    assert len(filter_coverage(value["ranked"], value["coverage"], "literal_contains")) == 1
    assert result["missing_facts"]
    assert evaluate(value, "keep_both", RecallPolicy())["missing_facts"] == []


@pytest.mark.parametrize("case_id", ["declared_partial", "stale_full"])
@pytest.mark.parametrize("mode", POLICIES)
def test_experimental_filters_keep_version_and_partial_guards(case_id, mode):
    result = evaluate(case(case_id), mode, RecallPolicy())
    assert set(result["selected_ids"]) == {"a", "e"}
    assert not result["missing_facts"]


def test_retaining_redundancy_can_displace_independent_fact_under_node_limit():
    value = case("literal_full")
    baseline = evaluate(value, "declared_full", BUDGETS["node_limited"])
    conservative = evaluate(value, "keep_both", BUDGETS["node_limited"])
    assert baseline["missing_facts"] == []
    assert conservative["missing_facts"] == ["independent_preference"]
    assert conservative["capacity_excluded_ids"] == ["b"]
    assert conservative["duplicate_fact_occurrences"] == 1


def test_coverage_does_not_remove_fitting_atomic_when_event_itself_cannot_fit():
    result = evaluate(case("oversized_parent"), "declared_full", RecallPolicy(budget_tokens=2048))
    assert result["selected_ids"] == ["a"]
    assert not result["missing_facts"]


def test_not_suppressing_does_not_guarantee_room_for_atomic():
    value = case("false_full_budget_pressure")
    result = evaluate(value, "keep_both", RecallPolicy(budget_tokens=550))
    assert result["selected_ids"] == ["e"]
    assert result["missing_facts"] == ["safety_officer_required"]
    assert result["capacity_excluded_ids"] == ["a"]
    assert result["coverage_removed_without_capacity_ids"] == []


def test_all_fixture_results_obey_real_rendered_budget_and_report_annotation_units():
    result = run_probe(synthetic_cases())
    assert len(result["results"]) == 10 * len(BUDGETS) * len(POLICIES)
    assert result["llm_calls"] == result["embedding_calls"] == 0
    for row in result["results"]:
        policy = BUDGETS[row["budget"]]
        assert row["rendered_bytes"] == len(row["text"].encode("utf-8")) <= policy.budget_tokens
        assert len(row["selected_ids"]) <= policy.max_nodes
        assert row["retained_fact_count"] + len(row["missing_facts"]) == row["required_fact_count"]


def test_freeze_precedes_results_and_cannot_overwrite_existing_probe(tmp_path):
    directory = tmp_path / "probe"
    main(["--directory", str(directory)])
    manifest = json.loads((directory / "manifest.json").read_text())
    report = json.loads((directory / "report.json").read_text())
    assert manifest["cases"] == synthetic_cases()
    assert report["manifest_sha256"]
    assert (directory / "report.json").stat().st_mode & 0o777 == 0o600
    original = (directory / "report.json").read_bytes()
    with pytest.raises(FileExistsError):
        main(["--directory", str(directory)])
    assert (directory / "report.json").read_bytes() == original


def test_unknown_policy_is_rejected():
    with pytest.raises(ValueError, match="unknown experimental policy"):
        filter_coverage([], [], "unreviewed")
