"""Local evidence and explicit offline review for synthetic profile replays.

Callers must supply isolated synthetic material, never production conversations.
Hashes bind a review to the exact saved case; they are not signed attestations.
The CLI emits only structural, screening and review statuses, never case text or
review reasons. It deliberately does not infer semantics or call another model.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any

_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,95}\Z")
_SHA256 = re.compile(r"[a-f0-9]{64}\Z")
_CLASSIFICATIONS = frozenset({
    "current_fact", "current_negative", "historical", "incorrect_current_fact",
    "absent", "uncertain", "missing_condition", "altered_condition",
})
_FAILED_CLASSIFICATIONS = frozenset({
    "incorrect_current_fact", "missing_condition", "altered_condition",
})
_VERDICTS = frozenset({"passed", "failed", "uncertain"})
_STATUSES = frozenset({"passed", "failed", "uncertain", "not_evaluated", "needs_review"})
_MAX_JSON_BYTES = 16 * 1024 * 1024


class EvidenceError(ValueError):
    """An intentionally text-free error code suitable for console output."""


def _name(value: Any) -> str:
    if not isinstance(value, str) or not _NAME.fullmatch(value) or value in {"report", "review"}:
        raise EvidenceError("invalid_case_name")
    return value


def _nonempty(value: Any, code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EvidenceError(code)
    return value


def _hash(value: Any) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise EvidenceError("invalid_evidence_hash")
    return value


def _object_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise EvidenceError("duplicate_json_key")
        result[key] = value
    return result


def _read(path: Path) -> tuple[dict, bytes]:
    try:
        if path.is_symlink() or not path.is_file():
            raise EvidenceError("missing_or_unsafe_evidence_file")
        with path.open("rb") as stream:
            raw = stream.read(_MAX_JSON_BYTES + 1)
        if len(raw) > _MAX_JSON_BYTES:
            raise EvidenceError("evidence_file_too_large")
        data = json.loads(raw, object_pairs_hook=_object_pairs,
                          parse_constant=lambda _: _invalid_constant())
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise EvidenceError("unreadable_evidence_json") from exc
    if not isinstance(data, dict):
        raise EvidenceError("evidence_json_must_be_object")
    return data, raw


def _invalid_constant():
    raise EvidenceError("invalid_json_constant")


def _encode(data: dict) -> bytes:
    if not isinstance(data, dict):
        raise EvidenceError("evidence_json_must_be_object")
    try:
        raw = (json.dumps(data, ensure_ascii=False, sort_keys=True, indent=2,
                          allow_nan=False) + "\n").encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise EvidenceError("unserializable_evidence") from exc
    if len(raw) > _MAX_JSON_BYTES:
        raise EvidenceError("evidence_file_too_large")
    return raw


def _requirement_id(value: Any) -> str:
    if not isinstance(value, str) or not _NAME.fullmatch(value):
        raise EvidenceError("invalid_requirement_id")
    return value


def _review_requirements(data: dict) -> tuple[str, ...] | None:
    # The questions are fixed synthetic-fixture inputs, not model-generated
    # expectations or reviewer-provided replacements for the saved evidence.
    if "review_requirements" not in data:
        return None
    requirements = data["review_requirements"]
    if not isinstance(requirements, list) or not requirements:
        raise EvidenceError("empty_or_invalid_review_requirements")
    identifiers = set()
    for requirement in requirements:
        if not isinstance(requirement, dict):
            raise EvidenceError("invalid_review_requirement")
        identifier = _requirement_id(requirement.get("id"))
        if identifier in identifiers:
            raise EvidenceError("duplicate_requirement_id")
        identifiers.add(identifier)
        for field in ("field", "question", "expected_answer"):
            _nonempty(requirement.get(field), f"invalid_requirement_{field}")
    return tuple(requirement["id"] for requirement in requirements)


def _case_contract(data: dict, name: str, structure_ok: bool | None = None) -> tuple[str, ...] | None:
    if data.get("name") != name or type(data.get("structure_ok")) is not bool:
        raise EvidenceError("invalid_case_contract")
    if structure_ok is not None and data["structure_ok"] is not structure_ok:
        raise EvidenceError("report_case_structure_mismatch")
    if "check" in data:
        check = data["check"]
        if (not isinstance(check, dict) or check.get("name") != name
                or type(check.get("structure_ok")) is not bool
                or check["structure_ok"] is not data["structure_ok"]):
            raise EvidenceError("nested_check_mismatch")
    return _review_requirements(data)


def _directory(directory: Path) -> Path:
    directory = Path(directory)
    if directory.is_symlink() or not directory.is_dir():
        raise EvidenceError("invalid_evidence_directory")
    return directory


def _report_cases(directory: Path, report: dict, *, allow_incomplete: bool = False) -> dict[str, dict]:
    if not isinstance(report, dict):
        raise EvidenceError("invalid_report")
    checks = report.get("checks")
    if not isinstance(checks, list) or (not checks and not allow_incomplete):
        raise EvidenceError("empty_or_invalid_report_checks")
    result = {}
    for check in checks:
        if not isinstance(check, dict):
            raise EvidenceError("invalid_report_check")
        name = _name(check.get("name"))
        if name in result:
            raise EvidenceError("duplicate_report_case")
        if type(check.get("structure_ok")) is not bool:
            raise EvidenceError("invalid_report_structure")
        digest = _hash(check.get("evidence_sha256"))
        case, raw = _read(directory / f"{name}.json")
        if hashlib.sha256(raw).hexdigest() != digest:
            raise EvidenceError("report_case_hash_mismatch")
        requirements = _case_contract(case, name, check["structure_ok"])
        screening = check.get("screening_status", check.get("lexical_status", check.get("semantic_status", "not_evaluated")))
        if not isinstance(screening, str) or screening not in _STATUSES:
            raise EvidenceError("invalid_screening_status")
        result[name] = {"structure_ok": check["structure_ok"], "sha256": digest,
                        "screening_status": screening, "requirements": requirements}
    if not allow_incomplete:
        expected = report.get("expected_cases")
        if not isinstance(expected, list) or not expected:
            raise EvidenceError("missing_or_invalid_expected_cases")
        names = [_name(item) for item in expected]
        if len(set(names)) != len(names):
            raise EvidenceError("duplicate_expected_case")
        if set(names) != result.keys():
            raise EvidenceError("incomplete_report_cases")
    return result


class EvidenceWriter:
    """Create a private directory and write each immutable case exactly once."""

    def __init__(self, directory: Path):
        self.directory = Path(directory)
        try:
            self.directory.mkdir(mode=0o700, parents=True, exist_ok=False)
        except OSError as exc:
            raise EvidenceError("evidence_directory_must_be_new") from exc

    def _write(self, path: Path, data: dict) -> str:
        raw = _encode(data)
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
        except OSError as exc:
            raise EvidenceError("evidence_write_failed") from exc
        return hashlib.sha256(raw).hexdigest()

    def write_case(self, name: str, data: dict) -> str:
        name = _name(name)
        if not isinstance(data, dict):
            raise EvidenceError("invalid_case_contract")
        _case_contract(data, name)
        return self._write(self.directory / f"{name}.json", data)

    def write_report(self, report: dict):
        # A partial run is still useful diagnostic evidence. Only review decides
        # completeness; any case that was saved must already match its digest.
        _report_cases(_directory(self.directory), report, allow_incomplete=True)
        self._write(self.directory / "report.json", report)


def _assessments(case: dict, requirements: tuple[str, ...] | None) -> dict[str, int] | None:
    if requirements is None and "assessments" not in case:
        return None
    assessments = case.get("assessments")
    if not isinstance(assessments, list):
        raise EvidenceError("invalid_review_assessments")
    expected = set(requirements or ())
    assessed = set()
    counts = dict.fromkeys(("passed", "failed", "uncertain"), 0)
    for assessment in assessments:
        if not isinstance(assessment, dict):
            raise EvidenceError("invalid_review_assessment")
        identifier = _requirement_id(assessment.get("requirement_id"))
        if identifier not in expected:
            raise EvidenceError("unknown_assessment_requirement")
        if identifier in assessed:
            raise EvidenceError("duplicate_assessment_requirement")
        assessed.add(identifier)
        verdict = assessment.get("verdict")
        if not isinstance(verdict, str) or verdict not in _VERDICTS:
            raise EvidenceError("invalid_assessment_verdict")
        _nonempty(assessment.get("observed"), "invalid_assessment_observed")
        _nonempty(assessment.get("reason"), "invalid_assessment_reason")
        counts[verdict] += 1
    if assessed != expected:
        raise EvidenceError("missing_requirement_assessment")
    return counts if requirements is not None else None


def validate_review(directory: Path, review: dict) -> dict:
    """Validate explicit review and return a body-free, fail-closed summary.

    A failed lexical screen can be resolved by inspection. A failed structural
    check cannot be overridden. Findings or individual required assessments
    marked uncertain or incorrect prevent a claimed pass, even when the
    reviewer accidentally selects ``passed``.
    """
    directory = _directory(directory)
    report, _ = _read(directory / "report.json")
    checks = _report_cases(directory, report)
    run_failed = report.get("run_error") is not None
    cleanup_errors = report.get("cleanup_errors", [])
    if not isinstance(cleanup_errors, list):
        raise EvidenceError("invalid_cleanup_errors")
    cleanup_failed = bool(cleanup_errors)
    if not isinstance(review, dict):
        raise EvidenceError("invalid_review")
    _nonempty(review.get("reviewer"), "invalid_reviewer")
    cases = review.get("cases")
    if not isinstance(cases, list):
        raise EvidenceError("invalid_review_cases")
    reviewed = {}
    requirement_counts = {}
    for case in cases:
        if not isinstance(case, dict):
            raise EvidenceError("invalid_review_case")
        name = _name(case.get("name"))
        if name in reviewed:
            raise EvidenceError("duplicate_review_case")
        if name not in checks:
            raise EvidenceError("unknown_review_case")
        if _hash(case.get("sha256")) != checks[name]["sha256"]:
            raise EvidenceError("review_case_hash_mismatch")
        verdict = case.get("verdict")
        if not isinstance(verdict, str) or verdict not in _VERDICTS:
            raise EvidenceError("invalid_review_verdict")
        findings = case.get("findings")
        if not isinstance(findings, list) or not findings:
            raise EvidenceError("empty_or_invalid_review_findings")
        classifications = set()
        for finding in findings:
            if not isinstance(finding, dict):
                raise EvidenceError("invalid_review_finding")
            _nonempty(finding.get("field"), "invalid_finding_field")
            _nonempty(finding.get("reason"), "invalid_finding_reason")
            classification = finding.get("classification")
            if not isinstance(classification, str) or classification not in _CLASSIFICATIONS:
                raise EvidenceError("invalid_finding_classification")
            classifications.add(classification)
        counts = _assessments(case, checks[name]["requirements"])
        if counts is not None:
            requirement_counts[name] = counts
        if classifications & _FAILED_CLASSIFICATIONS or (counts and counts["failed"]):
            verdict = "failed"
        elif verdict == "passed" and ("uncertain" in classifications or (counts and counts["uncertain"])):
            verdict = "uncertain"
        reviewed[name] = verdict if checks[name]["structure_ok"] else "not_evaluated"
    if reviewed.keys() != checks.keys():
        raise EvidenceError("missing_review_case")
    results = []
    for name, check in checks.items():
        result = {"name": name, "structure_ok": check["structure_ok"],
                  "screening_status": check["screening_status"],
                  "review_status": reviewed[name]}
        if name in requirement_counts:
            result["requirement_counts"] = requirement_counts[name]
        results.append(result)
    return {"checks": results, "run_failed": run_failed, "cleanup_failed": cleanup_failed,
            "all_passed": not run_failed and not cleanup_failed
            and all(item["review_status"] == "passed" for item in results)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("review", type=Path)
    args = parser.parse_args(argv)
    try:
        review, _ = _read(args.review)
        result = validate_review(args.directory, review)
    except EvidenceError as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["all_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
