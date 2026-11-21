"""Reviews are useful only if they stay bound to saved, inspectable evidence."""

import copy
import hashlib
import json
import stat

import pytest

from scripts.profile_evidence import (
    EvidenceError,
    EvidenceWriter,
    main,
    validate_review,
)


def evidence(tmp_path, *, structure_ok=True, screening_status="failed", review_requirements=None):
    directory = tmp_path / "evidence"
    writer = EvidenceWriter(directory)
    case = {"name": "correction", "structure_ok": structure_ok,
            "check": {"name": "correction", "structure_ok": structure_ok},
            "after": {"description": "PRIVATE_SYNTHETIC_BODY"}}
    if review_requirements is not None:
        case["review_requirements"] = review_requirements
    digest = writer.write_case("correction", case)
    report = {"expected_cases": ["correction"], "checks": [{"name": "correction", "structure_ok": structure_ok,
                          "screening_status": screening_status, "evidence_sha256": digest}]}
    writer.write_report(report)
    review = {"reviewer": "PRIVATE_REVIEWER", "cases": [{"name": "correction",
              "sha256": digest, "verdict": "passed", "findings": [
                  {"field": "profile.description", "classification": "current_negative",
                   "reason": "PRIVATE_REVIEW_REASON"}]}]}
    if review_requirements is not None:
        review["cases"][0]["assessments"] = [
            {"requirement_id": requirement["id"], "verdict": "passed",
             "observed": "PRIVATE_OBSERVED_TEXT", "reason": "PRIVATE_ASSESSMENT_REASON"}
            for requirement in review_requirements
        ]
    return directory, writer, report, review


def save(path, value):
    path.write_text(json.dumps(value), encoding="utf-8")


def test_explicit_review_can_resolve_failed_screen_without_disclosing_content(tmp_path):
    directory, _, _, review = evidence(tmp_path)
    result = validate_review(directory, review)
    assert result == {"checks": [{"name": "correction", "structure_ok": True,
                                  "screening_status": "failed", "review_status": "passed"}],
                      "run_failed": False, "cleanup_failed": False, "all_passed": True}
    assert "PRIVATE" not in json.dumps(result)


def test_structure_failure_cannot_be_overridden_by_reviewer(tmp_path):
    directory, _, _, review = evidence(tmp_path, structure_ok=False, screening_status="not_evaluated")
    result = validate_review(directory, review)
    assert not result["all_passed"]
    assert result["checks"][0]["review_status"] == "not_evaluated"


@pytest.mark.parametrize("verdict,classification,expected", [
    ("uncertain", "current_fact", "uncertain"),
    ("failed", "current_fact", "failed"),
    ("passed", "uncertain", "uncertain"),
    ("passed", "incorrect_current_fact", "failed"),
    ("uncertain", "incorrect_current_fact", "failed"),
    ("failed", "uncertain", "failed"),
    ("passed", "missing_condition", "failed"),
    ("uncertain", "missing_condition", "failed"),
    ("passed", "altered_condition", "failed"),
    ("uncertain", "altered_condition", "failed"),
])
def test_uncertainty_or_incorrect_findings_never_pass(tmp_path, verdict, classification, expected):
    directory, _, _, review = evidence(tmp_path)
    review["cases"][0]["verdict"] = verdict
    review["cases"][0]["findings"][0]["classification"] = classification
    result = validate_review(directory, review)
    assert not result["all_passed"]
    assert result["checks"][0]["review_status"] == expected


def test_writer_uses_exclusive_private_files_and_never_replaces_evidence(tmp_path):
    directory, writer, report, review = evidence(tmp_path)
    raw = (directory / "correction.json").read_bytes()
    assert hashlib.sha256(raw).hexdigest() == review["cases"][0]["sha256"]
    assert stat.S_IMODE(directory.stat().st_mode) == 0o700
    assert stat.S_IMODE((directory / "correction.json").stat().st_mode) == 0o600
    with pytest.raises(EvidenceError, match="must_be_new"):
        EvidenceWriter(directory)
    with pytest.raises(EvidenceError, match="write_failed"):
        writer.write_case("correction", {"name": "correction", "structure_ok": True})
    assert (directory / "correction.json").read_bytes() == raw
    with pytest.raises(EvidenceError, match="write_failed"):
        writer.write_report(report)


@pytest.mark.parametrize("name", ["../outside", "/outside", "a/b", "a\\b", ".", "", "report", "review", "a\n", "a" * 97, None])
def test_writer_rejects_unsafe_names_before_writing(tmp_path, name):
    writer = EvidenceWriter(tmp_path / "evidence")
    with pytest.raises(EvidenceError, match="invalid_case_name"):
        writer.write_case(name, {"name": name, "structure_ok": True})
    assert list(writer.directory.iterdir()) == []


@pytest.mark.parametrize("mutation", ["change_body", "remove_file", "symlink_file", "invalid_json"])
def test_changed_or_missing_evidence_cannot_be_reviewed(tmp_path, mutation):
    directory, _, _, review = evidence(tmp_path)
    path = directory / "correction.json"
    if mutation == "change_body":
        data = json.loads(path.read_text())
        data["after"]["description"] = "CHANGED_PRIVATE_BODY"
        save(path, data)
    elif mutation == "remove_file":
        path.unlink()
    elif mutation == "symlink_file":
        outside = tmp_path / "outside.json"
        path.rename(outside)
        path.symlink_to(outside)
    else:
        path.write_text("PRIVATE_MALFORMED_JSON")
    with pytest.raises(EvidenceError):
        validate_review(directory, review)


def test_report_cannot_relabel_failed_case_as_structural_success(tmp_path):
    directory, _, report, review = evidence(tmp_path, structure_ok=False)
    report["checks"][0]["structure_ok"] = True
    save(directory / "report.json", report)
    with pytest.raises(EvidenceError, match="structure_mismatch"):
        validate_review(directory, review)


def test_nested_check_cannot_contradict_case_even_when_hashes_are_updated(tmp_path):
    directory, _, report, review = evidence(tmp_path)
    path = directory / "correction.json"
    case = json.loads(path.read_text())
    case["check"]["structure_ok"] = False
    save(path, case)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    report["checks"][0]["evidence_sha256"] = digest
    review["cases"][0]["sha256"] = digest
    save(directory / "report.json", report)
    with pytest.raises(EvidenceError, match="nested_check_mismatch"):
        validate_review(directory, review)


@pytest.mark.parametrize("mutation,error", [
    ("review_hash", "review_case_hash_mismatch"),
    ("report_hash", "report_case_hash_mismatch"),
    ("duplicate_review", "duplicate_review_case"),
    ("duplicate_report", "duplicate_report_case"),
    ("missing_review", "missing_review_case"),
    ("unknown_review", "unknown_review_case"),
    ("traversal_review", "invalid_case_name"),
    ("empty_report", "empty_or_invalid_report_checks"),
    ("nonboolean_structure", "invalid_report_structure"),
])
def test_report_and_review_require_complete_unique_matching_cases(tmp_path, mutation, error):
    directory, _, report, review = evidence(tmp_path)
    if mutation == "review_hash":
        review["cases"][0]["sha256"] = "0" * 64
    elif mutation == "report_hash":
        report["checks"][0]["evidence_sha256"] = "0" * 64
    elif mutation == "duplicate_review":
        review["cases"].append(copy.deepcopy(review["cases"][0]))
    elif mutation == "duplicate_report":
        report["checks"].append(copy.deepcopy(report["checks"][0]))
    elif mutation == "missing_review":
        review["cases"] = []
    elif mutation == "unknown_review":
        review["cases"][0]["name"] = "unknown"
    elif mutation == "traversal_review":
        review["cases"][0]["name"] = "../outside"
    elif mutation == "empty_report":
        report["checks"] = []
    elif mutation == "nonboolean_structure":
        report["checks"][0]["structure_ok"] = 1
    save(directory / "report.json", report)
    with pytest.raises(EvidenceError, match=error):
        validate_review(directory, review)


@pytest.mark.parametrize("field,value", [
    ("reviewer", " "), ("findings", []), ("findings", None),
    ("field", ""), ("reason", " "), ("classification", "made_up"),
    ("classification", []), ("verdict", "not_evaluated"), ("verdict", []),
])
def test_review_requires_explicit_reviewer_classification_and_reason(tmp_path, field, value):
    directory, _, _, review = evidence(tmp_path)
    if field == "reviewer":
        review[field] = value
    elif field in {"findings", "verdict"}:
        review["cases"][0][field] = value
    else:
        review["cases"][0]["findings"][0][field] = value
    with pytest.raises(EvidenceError):
        validate_review(directory, review)


def test_report_is_not_written_when_case_contract_does_not_match(tmp_path):
    writer = EvidenceWriter(tmp_path / "evidence")
    digest = writer.write_case("failed", {"name": "failed", "structure_ok": False})
    with pytest.raises(EvidenceError, match="structure_mismatch"):
        writer.write_report({"checks": [{"name": "failed", "structure_ok": True,
                                         "evidence_sha256": digest}]})
    assert not (writer.directory / "report.json").exists()


@pytest.mark.parametrize("mutation,exit_code", [("pass", 0), ("uncertain", 1), ("missing", 2), ("malformed", 2), ("duplicate_json", 2)])
def test_cli_exit_codes_and_output_never_include_case_or_review_text(tmp_path, capsys, mutation, exit_code):
    directory, _, _, review = evidence(tmp_path)
    path = tmp_path / "review.json"
    if mutation == "uncertain":
        review["cases"][0]["verdict"] = "uncertain"
    if mutation != "missing":
        save(path, review)
    if mutation == "malformed":
        path.write_text("PRIVATE_INVALID_JSON")
    if mutation == "duplicate_json":
        path.write_text('{"reviewer":"PRIVATE_ONE","reviewer":"PRIVATE_TWO","cases":[]}')
    assert main([str(directory), str(path)]) == exit_code
    output = capsys.readouterr().out
    assert "PRIVATE" not in output
    result = json.loads(output)
    if exit_code < 2:
        assert result["all_passed"] == (exit_code == 0)
    else:
        assert isinstance(result["error"], str)


def test_report_and_review_order_need_not_match_and_every_case_must_pass(tmp_path):
    writer = EvidenceWriter(tmp_path / "evidence")
    checks, reviewed = [], []
    for name in ("first", "second"):
        digest = writer.write_case(name, {"name": name, "structure_ok": True})
        checks.append({"name": name, "structure_ok": True, "evidence_sha256": digest})
        reviewed.append({"name": name, "sha256": digest, "verdict": "passed", "findings": [
            {"field": "profile.tags", "classification": "absent", "reason": "Old tag removed."}]})
    writer.write_report({"expected_cases": ["first", "second"], "checks": checks})
    result = validate_review(writer.directory, {"reviewer": "reader", "cases": reviewed[::-1]})
    assert result["all_passed"]
    assert [check["name"] for check in result["checks"]] == ["first", "second"]


@pytest.mark.parametrize("expected", [None, [], ["correction", "missing"], ["correction", "correction"], ["../outside"]])
def test_missing_or_incomplete_case_manifest_never_passes(tmp_path, expected):
    directory, _, report, review = evidence(tmp_path)
    report["expected_cases"] = expected
    save(directory / "report.json", report)
    with pytest.raises(EvidenceError):
        validate_review(directory, review)


@pytest.mark.parametrize("field,value,flag", [
    ("run_error", {"unsafe_message": "PRIVATE_ERROR_TEXT"}, "run_failed"),
    ("run_error", {}, "run_failed"),
    ("cleanup_errors", [{"unsafe_message": "PRIVATE_ERROR_TEXT"}], "cleanup_failed"),
])
def test_run_or_cleanup_error_prevents_overall_pass_without_disclosing_errors(tmp_path, field, value, flag):
    directory, _, report, review = evidence(tmp_path)
    report[field] = value
    save(directory / "report.json", report)
    result = validate_review(directory, review)
    assert result[flag]
    assert not result["all_passed"]
    assert "PRIVATE" not in json.dumps(result)


def test_partial_report_can_be_saved_for_diagnosis_but_cannot_pass(tmp_path):
    writer = EvidenceWriter(tmp_path / "evidence")
    writer.write_report({"checks": [], "expected_cases": ["not_run"], "run_error": {"code": "failed"}})
    assert (writer.directory / "report.json").exists()
    with pytest.raises(EvidenceError):
        validate_review(writer.directory, {"reviewer": "reader", "cases": []})


def requirements():
    return [
        {"id": "only_water", "field": "profile.description",
         "question": "PRIVATE_QUESTION: Is the exclusivity of water retained?",
         "expected_answer": "PRIVATE_EXPECTED: The current profile says only water."},
        {"id": "unrelated_fact", "field": "profile.description",
         "question": "PRIVATE_QUESTION: Is the unrelated current fact retained?",
         "expected_answer": "PRIVATE_EXPECTED: The current profile still includes it."},
    ]


def test_required_review_summary_only_exposes_counts_and_statuses(tmp_path):
    directory, _, _, review = evidence(tmp_path, review_requirements=requirements())
    # Review order does not affect coverage or the evidence's fixed questions.
    review["cases"][0]["assessments"].reverse()
    result = validate_review(directory, review)
    assert result["all_passed"]
    assert result["checks"][0] == {
        "name": "correction", "structure_ok": True, "screening_status": "failed",
        "review_status": "passed", "requirement_counts": {"passed": 2, "failed": 0, "uncertain": 0},
    }
    serialized = json.dumps(result)
    assert "PRIVATE" not in serialized
    assert "only_water" not in serialized
    assert "observed" not in serialized
    assert "question" not in serialized


@pytest.mark.parametrize("verdict,assessment_verdict,expected", [
    ("passed", "failed", "failed"),
    ("uncertain", "failed", "failed"),
    ("passed", "uncertain", "uncertain"),
    ("failed", "uncertain", "failed"),
    ("failed", "passed", "failed"),
    ("uncertain", "passed", "uncertain"),
])
def test_failed_or_uncertain_requirement_cannot_be_hidden_by_case_verdict(
    tmp_path, verdict, assessment_verdict, expected,
):
    directory, _, _, review = evidence(tmp_path, review_requirements=requirements())
    reviewed = review["cases"][0]
    reviewed["verdict"] = verdict
    reviewed["assessments"][0]["verdict"] = assessment_verdict
    result = validate_review(directory, review)
    assert not result["all_passed"]
    assert result["checks"][0]["review_status"] == expected
    counts = result["checks"][0]["requirement_counts"]
    assert counts[assessment_verdict] == (2 if assessment_verdict == "passed" else 1)
    assert sum(counts.values()) == 2


def test_failed_requirement_takes_precedence_over_uncertain_finding(tmp_path):
    directory, _, _, review = evidence(tmp_path, review_requirements=requirements())
    review["cases"][0]["findings"][0]["classification"] = "uncertain"
    review["cases"][0]["assessments"][0]["verdict"] = "failed"
    result = validate_review(directory, review)
    assert result["checks"][0]["review_status"] == "failed"
    assert not result["all_passed"]


def test_missing_condition_finding_overrides_successful_requirement_assessments(tmp_path):
    directory, _, _, review = evidence(tmp_path, review_requirements=requirements())
    review["cases"][0]["findings"][0]["classification"] = "missing_condition"
    result = validate_review(directory, review)
    assert result["checks"][0]["review_status"] == "failed"
    assert result["checks"][0]["requirement_counts"]["passed"] == 2
    assert not result["all_passed"]


@pytest.mark.parametrize("with_requirements", [False, True])
def test_historical_finding_is_allowed_for_long_term_nodes(tmp_path, with_requirements):
    directory, _, _, review = evidence(
        tmp_path, review_requirements=requirements() if with_requirements else None,
    )
    review["cases"][0]["findings"][0] = {
        "field": "nodes.body", "classification": "historical",
        "reason": "Long-term nodes preserve the history of preference changes.",
    }
    assert validate_review(directory, review)["all_passed"]


def test_successful_assessments_cannot_override_structural_failure(tmp_path):
    directory, _, _, review = evidence(
        tmp_path, structure_ok=False, screening_status="not_evaluated", review_requirements=requirements(),
    )
    result = validate_review(directory, review)
    assert result["checks"][0]["review_status"] == "not_evaluated"
    assert result["checks"][0]["requirement_counts"]["passed"] == 2
    assert not result["all_passed"]


@pytest.mark.parametrize("mutation,error", [
    ("absent", "invalid_review_assessments"),
    ("null", "invalid_review_assessments"),
    ("object", "invalid_review_assessments"),
    ("empty", "missing_requirement_assessment"),
    ("missing", "missing_requirement_assessment"),
    ("duplicate", "duplicate_assessment_requirement"),
    ("unknown", "unknown_assessment_requirement"),
    ("nonobject", "invalid_review_assessment"),
])
def test_review_must_cover_every_fixed_requirement_exactly_once(tmp_path, mutation, error):
    directory, _, _, review = evidence(tmp_path, review_requirements=requirements())
    reviewed = review["cases"][0]
    if mutation == "absent":
        del reviewed["assessments"]
    elif mutation == "null":
        reviewed["assessments"] = None
    elif mutation == "object":
        reviewed["assessments"] = {}
    elif mutation == "empty":
        reviewed["assessments"] = []
    elif mutation == "missing":
        reviewed["assessments"].pop()
    elif mutation == "duplicate":
        reviewed["assessments"].append(copy.deepcopy(reviewed["assessments"][0]))
    elif mutation == "unknown":
        reviewed["assessments"][0]["requirement_id"] = "unknown"
    else:
        reviewed["assessments"][0] = "PRIVATE_INVALID_ASSESSMENT"
    with pytest.raises(EvidenceError, match=error):
        validate_review(directory, review)


@pytest.mark.parametrize("field,value,error", [
    ("requirement_id", "", "invalid_requirement_id"),
    ("requirement_id", "../outside", "invalid_requirement_id"),
    ("requirement_id", [], "invalid_requirement_id"),
    ("verdict", "not_evaluated", "invalid_assessment_verdict"),
    ("verdict", [], "invalid_assessment_verdict"),
    ("observed", " ", "invalid_assessment_observed"),
    ("observed", None, "invalid_assessment_observed"),
    ("reason", "", "invalid_assessment_reason"),
    ("reason", {}, "invalid_assessment_reason"),
])
def test_each_assessment_requires_a_safe_id_explicit_verdict_and_review_evidence(
    tmp_path, field, value, error,
):
    directory, _, _, review = evidence(tmp_path, review_requirements=requirements())
    review["cases"][0]["assessments"][0][field] = value
    with pytest.raises(EvidenceError, match=error):
        validate_review(directory, review)


@pytest.mark.parametrize("bad_requirements,error", [
    (None, "empty_or_invalid_review_requirements"),
    ([], "empty_or_invalid_review_requirements"),
    ({}, "empty_or_invalid_review_requirements"),
    ([None], "invalid_review_requirement"),
    ([{}], "invalid_requirement_id"),
    ([requirements()[0], requirements()[0]], "duplicate_requirement_id"),
])
def test_writer_rejects_invalid_requirement_lists_before_writing(tmp_path, bad_requirements, error):
    writer = EvidenceWriter(tmp_path / "evidence")
    with pytest.raises(EvidenceError, match=error):
        writer.write_case("correction", {"name": "correction", "structure_ok": True,
                                         "review_requirements": bad_requirements})
    assert list(writer.directory.iterdir()) == []


@pytest.mark.parametrize("field,value,error", [
    ("id", "", "invalid_requirement_id"),
    ("id", "../outside", "invalid_requirement_id"),
    ("id", "unsafe\n", "invalid_requirement_id"),
    ("id", "a" * 97, "invalid_requirement_id"),
    ("id", [], "invalid_requirement_id"),
    ("field", " ", "invalid_requirement_field"),
    ("question", "", "invalid_requirement_question"),
    ("question", None, "invalid_requirement_question"),
    ("expected_answer", [], "invalid_requirement_expected_answer"),
    ("expected_answer", "\n", "invalid_requirement_expected_answer"),
])
def test_writer_requires_valid_question_contracts(tmp_path, field, value, error):
    writer = EvidenceWriter(tmp_path / "evidence")
    required = requirements()
    required[0][field] = value
    with pytest.raises(EvidenceError, match=error):
        writer.write_case("correction", {"name": "correction", "structure_ok": True,
                                         "review_requirements": required})
    assert list(writer.directory.iterdir()) == []


def test_questions_are_bound_to_saved_evidence_hash(tmp_path):
    directory, _, _, review = evidence(tmp_path, review_requirements=requirements())
    path = directory / "correction.json"
    case = json.loads(path.read_text())
    case["review_requirements"][0]["expected_answer"] = "PRIVATE_REPLACEMENT_EXPECTATION"
    save(path, case)
    with pytest.raises(EvidenceError, match="report_case_hash_mismatch"):
        validate_review(directory, review)


def test_case_requirements_are_validated_again_on_read(tmp_path):
    directory, _, report, review = evidence(tmp_path, review_requirements=requirements())
    path = directory / "correction.json"
    case = json.loads(path.read_text())
    case["review_requirements"][0]["expected_answer"] = " "
    save(path, case)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    report["checks"][0]["evidence_sha256"] = digest
    review["cases"][0]["sha256"] = digest
    save(directory / "report.json", report)
    with pytest.raises(EvidenceError, match="invalid_requirement_expected_answer"):
        validate_review(directory, review)


def test_legacy_case_does_not_allow_assessments_of_nonexistent_requirements(tmp_path):
    directory, _, _, review = evidence(tmp_path)
    review["cases"][0]["assessments"] = [
        {"requirement_id": "invented", "verdict": "passed", "observed": "PRIVATE", "reason": "PRIVATE"},
    ]
    with pytest.raises(EvidenceError, match="unknown_assessment_requirement"):
        validate_review(directory, review)
    review["cases"][0]["assessments"] = []
    result = validate_review(directory, review)
    assert result["all_passed"]
    assert "requirement_counts" not in result["checks"][0]


@pytest.mark.parametrize("assessment_verdict,exit_code", [("passed", 0), ("failed", 1), ("uncertain", 1)])
def test_required_review_cli_keeps_detailed_evidence_private(tmp_path, capsys, assessment_verdict, exit_code):
    directory, _, _, review = evidence(tmp_path, review_requirements=requirements())
    review["cases"][0]["assessments"][0]["verdict"] = assessment_verdict
    path = tmp_path / "review.json"
    save(path, review)
    assert main([str(directory), str(path)]) == exit_code
    output = capsys.readouterr().out
    assert "PRIVATE" not in output
    result = json.loads(output)
    assert result["checks"][0]["requirement_counts"][assessment_verdict] >= 1
