"""The opt-in evaluator must not turn missing evidence into a successful check."""

import json

import pytest

from scripts.verify_stability_api import (
    HttpBudget,
    continue_preference_chain,
    current_preference,
    evidence_snapshot,
    finish,
    no_asserted_cat,
    no_positive_hobby,
    old_preference_status,
    preference_cases,
    profile_text,
    record_consumption,
    skipped,
)


def test_quality_evidence_requires_review_even_when_screening_passes():
    check = {"structure_ok": True, "review_required": True}
    finish(check, {"current_profile": True})
    assert check["screening_status"] == "passed"
    assert check["semantic_status"] == "needs_review" and not check["ok"]
    assert continue_preference_chain(check)


def test_semantic_failure_does_not_block_committed_correction_chain():
    check = {"structure_ok": True, "review_required": True}
    finish(check, {"old_preference_rule": False})
    assert check["screening_status"] == "failed" and not check["ok"]
    assert continue_preference_chain(check)
    check["structure_ok"] = False
    finish(check, {"empty_nodes": True})
    assert not continue_preference_chain(check)
    assert check["semantic_status"] == "not_evaluated"


@pytest.mark.parametrize("consumed", [set(), {"source-1"}])
def test_successful_partial_window_cannot_be_reviewed_as_completed_or_continue(consumed):
    check = {"structure_ok": True, "review_required": True}
    record_consumption(check, ["source-1", "source-2"], consumed)
    finish(check, {"current_profile": True})
    assert check["reason"] == "source_window_incomplete"
    assert not check["structure_ok"] and not check["ok"]
    assert check["semantic_status"] == "not_evaluated"
    assert not continue_preference_chain(check)


def test_snapshot_keeps_tags_separate_and_does_not_export_runtime_fields():
    snapshot = evidence_snapshot([
        {"logical_user_id": "alice", "description": "现在不喝咖啡", "tags": ["咖啡爱好者"], "private_runtime": "excluded"},
        {"logical_user_id": "bob", "description": "喜欢摄影", "tags": []},
    ], [{"node_id": "n1", "content_version": 2, "content_text": "原来喝咖啡，现喝茶", "internal": "excluded"}], [None])
    assert snapshot["profiles"][0] == {"logical_user_id": "alice", "description": "现在不喝咖啡", "tags": ["咖啡爱好者"]}
    assert snapshot["profiles"][1]["logical_user_id"] == "bob"
    assert snapshot["nodes"][0] == {"node_id": "n1", "content_version": 2, "content_text": "原来喝咖啡，现喝茶"}
    assert snapshot["relationships"] == []


@pytest.mark.parametrize("variant", ["standard", "paraphrase"])
def test_correction_fixture_does_not_restate_unrelated_fact_in_later_windows(variant):
    cases = preference_cases(variant)
    assert [case[1] for case in cases] == ["黑咖啡", "乌龙茶", "白开水"]
    assert "摄影" in "".join(cases[0][2])
    assert "摄影" not in "".join(cases[1][2] + cases[2][2])


@pytest.mark.asyncio
@pytest.mark.parametrize("mode,count", [("preference-quality", 3), ("qualifiers", 2)])
async def test_quality_chain_retains_failed_screens_and_committed_windows(tmp_path, monkeypatch, mode, count):
    from adapters.providers import ProviderAdapter
    from scripts.profile_evidence import EvidenceWriter
    from scripts.verify_stability_api import run_checks

    calls = []

    async def no_change(self, messages, **kwargs):
        assert "review_requirements" not in messages[-1]["content"]
        assert "expected_answer" not in messages[-1]["content"]
        calls.append(json.loads(messages[-1]["content"])["sources"])
        return json.dumps({"schema_version": "extraction.v1", "result": "no_change",
            "reason": "no_meaningful_content", "node_changes": [], "profile_changes": [],
            "relationship_change": None, "edges": [], "coverage_changes": []})

    monkeypatch.setattr(ProviderAdapter, "generate", no_change)
    writer = EvidenceWriter(tmp_path / "evidence")
    report = await run_checks({
        "ecoapi": {"model": "offline", "base_url": "http://invalid.test/v1", "api_token": "synthetic"},
        "Cloudflare Embedding": {"model": "offline", "api_address": "http://invalid.test", "account_id": "synthetic", "api_token": "synthetic"},
    }, tmp_path, proxy=None, http_budget=count, only=mode, evidence=writer)
    writer.write_report(report)
    assert len(calls) == count  # Wrong semantic output must not stop valid commits.
    assert all(len(batch) == 3 for batch in calls)
    assert all([source["source_id"] for source in batch] == ["s1", "s2", "s3"] for batch in calls)
    assert report["http_requests_total"] == 0  # No real network in this test.
    assert report["provider_limits"]["embedding"] == 0
    assert not report.get("run_error") and not report.get("cleanup_errors")
    canonical_ids = set()
    for check in report["checks"]:
        assert check["structure_ok"] and check["all_sources_consumed"]
        assert check["screening_status"] == "failed"
        assert check["semantic_status"] == "needs_review" and not check["ok"]
        case = json.loads((writer.directory / (check["name"] + ".json")).read_text())
        assert len(case["sources"]) == 3
        canonical_ids.update(source["source_id"] for source in case["sources"])
        assert case["review_requirements"]  # Rubric stays only in local evidence.
        assert case["before"]["profiles"] == case["after"]["profiles"] == []
        assert check["candidate_diagnostics"]["status"] == "profile_not_proposed"
        assert check["candidate_diagnostics"]["consistency_ok"] is True
        assert len(case["model_candidates"]) == 1
        assert case["commit_receipt"]["output_digest"]
        assert case["model_candidates"][0]["reference_protocol"] == "reference.v1"
        assert case["model_candidates"][0]["input_digest"] == case["commit_receipt"]["input_digest"]
    assert len(canonical_ids) == count * 3


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "invalid_output", "wrong_evidence"])
async def test_candidate_evidence_tracks_real_commit_and_rejection(tmp_path, monkeypatch, fault):
    from adapters.providers import ProviderAdapter
    from scripts.profile_evidence import EvidenceWriter
    from scripts.verify_stability_api import run_checks

    async def generated(self, messages, **kwargs):
        payload = json.loads(next(message["content"] for message in messages if message["role"] == "user"))
        assert "review_requirements" not in payload and "expected_answer" not in payload
        if fault == "invalid_output":
            return "not-json"
        source = payload["sources"][0]
        known = payload["known_profiles"]
        assert all("content_version" not in profile for profile in known)
        return json.dumps({"node_changes": [], "edges": [], "coverage_changes": [],
            "relationship_change": None, "profile_changes": [{
                "logical_user_id": source["actor_logical_user_id"],
                "description": "修正后的合成画像。" if known else "初始合成画像。保留的另一项。", "tags": [],
                "remove_sentences": [1] if known else [], "remove_tags": [],
                "source_ids": ["unknown-source" if fault else source["source_id"]],
            }]})

    monkeypatch.setattr(ProviderAdapter, "generate", generated)
    writer = EvidenceWriter(tmp_path / "evidence")
    report = await run_checks({
        "ecoapi": {"model": "offline", "base_url": "http://invalid.test/v1", "api_token": "synthetic"},
        "Cloudflare Embedding": {"model": "offline", "api_address": "http://invalid.test", "account_id": "synthetic", "api_token": "synthetic"},
    }, tmp_path, proxy=None, http_budget=3, only="selection", evidence=writer)
    assert not report.get("run_error") and not report.get("cleanup_errors")
    first, second, independent = report["checks"]
    expected = {None: "profiles_applied", "invalid_output": "invalid_output", "wrong_evidence": "reference_rejected"}[fault]
    for check in (first, independent):
        assert check["candidate_diagnostics"]["status"] == expected
        assert check["structure_ok"] is (fault is None)
        assert check["semantic_status"] != "passed"
        case = json.loads((writer.directory / (check["name"] + ".json")).read_text())
        assert len(case["model_candidates"]) == (4 if fault else 1)
        assert bool(case["commit_receipt"]) is (fault is None)
        assert bool(case["after"]["profiles"]) is (fault is None)
    if fault:
        assert second["reason"] == "prerequisite_structure_failed"
    else:
        assert second["candidate_diagnostics"]["status"] == "profiles_applied"
        assert second["candidate_diagnostics"]["consistency_ok"] is True
        case = json.loads((writer.directory / (second["name"] + ".json")).read_text())
        before, = case["before"]["profiles"]
        after, = case["after"]["profiles"]
        assert after["content_version"] == before["content_version"] + 1
        assert after["description"] == "修正后的合成画像。保留的另一项。"
        change = case["model_candidates"][0]["wire_output"]["profile_changes"][0]
        assert change["remove_sentences"] == [1] and change["description"] == "修正后的合成画像。"


def test_selection_fixtures_keep_optional_and_required_coverage_separate():
    from scripts.profile_quality_cases import selection_cases
    from scripts.verify_stability_api import expected_cases

    cases = selection_cases()
    assert expected_cases("selection") == [case["name"] for case in cases]
    assert cases[0]["user"] == cases[1]["user"] != cases[2]["user"]
    assert cases[1]["depends_on"] == cases[0]["name"] and "depends_on" not in cases[2]
    for case in cases:
        assert len(case["messages"]) == 3
        assert not any(hint in "".join(case["messages"]) for hint in ("请记住", "更新画像", "不替代", "稳定习惯"))
        ids = [requirement["id"] for requirement in case["review_requirements"]]
        assert len(ids) == len(set(ids))
    assert not cases[2]["profile_expected"]


@pytest.mark.asyncio
async def test_quality_embedding_budget_rejects_any_request():
    budget = HttpBudget(5, llm_limit=5, embedding_limit=0)
    with pytest.raises(RuntimeError, match="budget_exceeded"):
        await budget.hook("embedding")(None)
    assert budget.counts == {"llm": 0, "embedding": 0}


@pytest.mark.asyncio
async def test_http_budget_counts_all_provider_attempts_and_rejects_before_excess():
    budget = HttpBudget(8)
    llm, embedding = budget.hook("llm"), budget.hook("embedding")
    for _ in range(6):
        await llm(None)
    with pytest.raises(RuntimeError, match="budget_exceeded"):
        await llm(None)
    await embedding(None)
    await embedding(None)
    with pytest.raises(RuntimeError, match="budget_exceeded"):
        await embedding(None)
    assert budget.counts == {"llm": 6, "embedding": 2}


@pytest.mark.asyncio
async def test_shared_http_budget_can_be_lower_than_per_provider_limits():
    budget = HttpBudget(2)
    await budget.hook("llm")(None)
    await budget.hook("embedding")(None)
    with pytest.raises(RuntimeError):
        await budget.hook("llm")(None)
    assert sum(budget.counts.values()) == 2


def test_transport_failure_is_not_a_semantic_failure_or_false_pass():
    check = {"structure_ok": False, "error_type": "TimeoutError"}
    finish(check, {"no_nodes_created": True})
    assert not check["ok"]
    assert check["semantic_status"] == "not_evaluated"
    assert check["assertions"] == {}


@pytest.mark.parametrize("assertions", [{}, {"current_profile": True, "history": False}])
def test_empty_or_failed_semantic_assertions_cannot_pass(assertions):
    check = {"structure_ok": True}
    finish(check, assertions)
    assert not check["ok"] and check["semantic_status"] == "failed"


def test_skipped_dependency_has_no_claimed_request_or_success():
    check = skipped("second_correction", "prerequisite_failed")
    assert not check["ok"] and check["semantic_status"] == "not_evaluated"
    assert check["http_requests"] == 0


def test_profile_evidence_is_selected_by_logical_user_not_nickname_or_position():
    profiles = [
        {"logical_user_id": "b", "description": "喜欢钢琴", "tags": ["音乐"]},
        {"logical_user_id": "a", "description": "喜欢摄影", "tags": ["拍照"]},
    ]
    assert profile_text(profiles, "a") == "喜欢摄影 拍照"
    assert profile_text(profiles, "missing") == ""


def test_negative_current_preference_is_not_a_keyword_success():
    assert current_preference("当前每天只喝白开水", "白开水")
    assert not current_preference("现在不喝白开水", "白开水")
    assert not current_preference("目前不再喜欢喝白开水", "白开水")


def test_combined_incorrect_group_hobbies_cannot_pass():
    assert not no_positive_hobby("喜欢摄影和钢琴", "钢琴")
    assert not no_positive_hobby("擅长钢琴和摄影", "摄影")
    assert no_positive_hobby("喜欢摄影，不弹钢琴", "钢琴")
    assert no_positive_hobby("对摄影不感兴趣", "摄影")


def test_cat_invented_in_node_without_profile_is_detected():
    assert not no_asserted_cat(["", "Charlie现在养猫"])
    assert not no_asserted_cat(["目前没有猫", "现在养猫，但此前没有猫"])
    assert no_asserted_cat(["目前没有猫", "假设以后养猫，尚未决定"])


@pytest.mark.parametrize("text,status", [
    ("现在喝乌龙茶", "old_absent"),
    ("现在喝乌龙茶，不再喝黑咖啡", "old_only_explicit_current_negative"),
    ("已停止饮用咖啡", "old_only_explicit_current_negative"),
    ("不喝茶，现在喝咖啡", "old_positive_or_ambiguous"),
    ("以前不喝咖啡", "old_positive_or_ambiguous"),
    ("不再喝咖啡，咖啡仍是每天饮料", "old_positive_or_ambiguous"),
])
def test_old_preference_rule_checks_every_mention_and_temporal_context(text, status):
    assert old_preference_status(text, "咖啡") == status


@pytest.mark.asyncio
async def test_preference_only_budget_does_not_hide_extra_embedding_requests():
    budget = HttpBudget(4, llm_limit=3, embedding_limit=1)
    for _ in range(3):
        await budget.hook("llm")(None)
    await budget.hook("embedding")(None)
    for kind in ("llm", "embedding"):
        with pytest.raises(RuntimeError):
            await budget.hook(kind)(None)
