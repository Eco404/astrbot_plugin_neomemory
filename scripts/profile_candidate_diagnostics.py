"""Bounded candidate evidence for isolated synthetic evaluations only.

This observes a script-owned adapter without changing the provider result,
production logging, checkpoints, or database writes. Candidate bodies belong in
private evidence files; ``diagnose_profiles`` returns content-free counters and
mechanical consistency checks, never a judgment of profile selection quality.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any

from core.extraction_apply import digest
from core.extraction_references import ExtractionReferences, ReferenceValidationError
from core.extraction_regeneration import build_messages
from core.extraction_schema import MAX_OUTPUT_CHARS, parse_extraction_output

MAX_RECORDS = 8
# These maintenance errors follow a rejected apply operation. Transport errors,
# generic timeouts, configuration changes and unfinished jobs do not prove this.
COMMIT_REJECTION_REASONS = frozenset({
    "context_or_evidence_invalid", "protected_candidate", "source_outside_window",
    "evidence_user_mismatch", "node_outside_context", "node_type_mismatch",
    "expected_node_version_mismatch", "profile_user_mismatch", "group_profile_limit",
    "expected_profile_version_mismatch", "relationship_user_mismatch",
    "expected_relationship_version_mismatch", "coverage_endpoint_type_mismatch",
    "duplicate_coverage_pair", "coverage_event_version_mismatch", "coverage_atomic_version_mismatch",
    "edge_self_reference", "invalid_contains_hierarchy", "supersedes_node_type_mismatch", "supersedes_cycle",
})


async def prepared_request(batch: Any, jobs: Any) -> tuple[Any, list[dict[str, str]], Any]:
    """Reconstruct the complete request from prepared input and durable history."""
    references = ExtractionReferences(batch) if batch.reference_protocol else None
    messages = references.messages() if references is not None else batch.messages()
    regeneration = None
    if batch.job_id is not None:
        job = await jobs.get(batch.job_id)
        window = job["checkpoint"].get("window", {})
        regeneration = window.get("regeneration")
        if regeneration is not None:
            archived = await jobs.get_candidate(batch.job_id, regeneration.get("candidate_id"))
            from core.prompts import schema_for_prompt_version
            messages = build_messages(messages, window, archived,
                schema_version=schema_for_prompt_version(batch.prompt_version))
    return references, messages, regeneration


class CandidateRecorder:
    """Preserve at most eight schema-whitelisted responses, not prompt inputs.

    A slot is reserved before awaiting the provider, so concurrent calls and
    cancellation remain visible. Overflow never prevents an original call; the
    caller can inspect ``overflow_count`` and must not claim complete evidence.
    """

    def __init__(self, generate: Callable[..., Awaitable[Any]]):
        self.generate = generate
        self.records: list[dict[str, Any]] = []
        self.overflow_count = 0
        self.references: ExtractionReferences | None = None
        self.expected_messages: list[dict[str, str]] | None = None
        self.regeneration: dict[str, str] | None = None

    def observe_preparation(self, extraction: Any, jobs: Any) -> None:
        """Bind this isolated sequential evaluator to the actual input snapshot."""
        original = extraction.prepare

        async def prepare(*args: Any, **kwargs: Any) -> Any:
            batch = await original(*args, **kwargs)
            self.references, self.expected_messages, self.regeneration = await prepared_request(batch, jobs)
            return batch

        extraction.prepare = prepare

    async def __call__(self, messages: Any, **kwargs: Any) -> Any:
        references = self.references
        expected_messages = self.expected_messages
        regeneration = self.regeneration
        record: dict[str, Any] | None = None
        if len(self.records) < MAX_RECORDS:
            record = {"status": "pending"}
            self.records.append(record)
        else:
            self.overflow_count += 1
        if references is not None:
            if messages != (expected_messages if expected_messages is not None else references.messages()):
                raise ValueError("model_input_snapshot_mismatch")
            if record is not None:
                record.update(reference_protocol=references.batch.reference_protocol,
                              input_digest=references.batch.input_digest, prompt_sha256=digest(messages))
                if regeneration is not None:
                    record["regeneration"] = dict(regeneration)
        try:
            raw = await self.generate(messages, **kwargs)
        except BaseException as exc:
            if record is not None:
                record.update(
                    status="cancelled" if isinstance(exc, asyncio.CancelledError) else "provider_error",
                    exception_type=type(exc).__name__,
                )
            raise
        if record is not None:
            # Diagnostics must not replace a provider return with an unrelated
            # parser/encoding exception. Production still validates the result.
            try:
                self._capture(record, raw, references)
            except Exception as exc:
                record.clear()
                record.update(status="diagnostic_error", exception_type=type(exc).__name__)
        return raw

    @staticmethod
    def _capture(record: dict[str, Any], raw: Any, references: ExtractionReferences | None = None) -> None:
        if not isinstance(raw, str):
            record.update(status="invalid_output", invalid_kind="not_text")
            return
        record.update(
            raw_length=len(raw),
            raw_sha256=hashlib.sha256(raw.encode("utf-8", errors="surrogatepass")).hexdigest(),
        )
        if len(raw) > MAX_OUTPUT_CHARS:
            record.update(status="invalid_output", invalid_kind="oversize")
            return
        try:
            output = parse_extraction_output(raw).model_dump(mode="json")
        except ValueError:
            record.update(status="invalid_output", invalid_kind="schema")
            return
        if references is not None:
            record.update(wire_raw=raw, wire_output=output, wire_output_digest=digest(output),
                          reference_protocol=references.batch.reference_protocol)
            try:
                canonical = references.decode(raw)
            except ReferenceValidationError as exc:
                record.update(status="reference_invalid", validation_errors=[{"path": exc.path, "type": exc.code}])
                return
            output = parse_extraction_output(canonical).model_dump(mode="json")
        canonical_digest = digest(output)
        record.update(status="valid_output", output=output, output_digest=canonical_digest)


def _profiles(snapshot: Any) -> dict[str, dict[str, Any]] | None:
    if not isinstance(snapshot, Mapping) or not isinstance(snapshot.get("profiles"), list):
        return None
    result: dict[str, dict[str, Any]] = {}
    for profile in snapshot["profiles"]:
        if not isinstance(profile, Mapping):
            return None
        user = profile.get("logical_user_id")
        version = profile.get("content_version")
        tags = profile.get("tags")
        description = profile.get("description")
        if (
            not isinstance(user, str) or not user or user in result
            or type(version) is not int or version < 1
            or not isinstance(tags, list) or any(not isinstance(tag, str) for tag in tags)
            or not isinstance(description, str)
        ):
            return None
        result[user] = {"content_version": version, "tags": tags, "description": description}
    return result


def diagnose_profiles(
    records: Sequence[Mapping[str, Any]],
    before: Any,
    after: Any,
    receipt: Mapping[str, Any] | None,
    *,
    terminal: bool,
    rejection_reason: str | None = None,
) -> dict[str, Any]:
    """Check one response against its receipt and complete profile snapshots.

    ``terminal`` alone never implies rejection: cancellation can outlive an
    awaiting coroutine. Only known apply rejection codes classify a missing
    receipt as ``commit_rejected``; other missing receipts stay unconfirmed.
    A correction chain may precede the final response only with invalid candidates
    for the same input snapshot and explicit regeneration provenance.
    """
    old, actual = _profiles(before), _profiles(after)
    summary = {
        "status": "evidence_incomplete",
        "record_count": len(records),
        "valid_response_count": sum(record.get("status") == "valid_output" for record in records),
        "profile_candidate_count": None,
        "before_profile_count": len(old) if old is not None else None,
        "after_profile_count": len(actual) if actual is not None else None,
        "receipt_present": receipt is not None,
        "receipt_digest_matches": None,
        "commit_confirmed": False,
        "candidate_profiles_match": None,
        "unproposed_profiles_unchanged": None,
        "consistency_ok": None,
        "terminal": bool(terminal),
    }

    def status(value: str, consistency: bool | None = None) -> dict[str, Any]:
        summary.update(status=value, consistency_ok=consistency)
        return summary

    if not records:
        return status("evidence_incomplete" if receipt is not None else "no_response")
    if len(records) != 1:
        input_digest = records[0].get("input_digest")
        if (not input_digest
                or any(record.get("input_digest") != input_digest for record in records)
                or any(record.get("status") not in {"invalid_output", "reference_invalid"} for record in records[:-1])
                or any(not record.get("regeneration", {}).get("candidate_id") for record in records[1:])):
            return summary
    record = records[-1]
    if record.get("status") != "valid_output":
        if receipt is not None:
            if record.get("status") in {"invalid_output", "reference_invalid"}:
                return status("application_mismatch", False)
            return summary
        if record.get("status") == "invalid_output":
            return status("invalid_output")
        if record.get("status") == "reference_invalid":
            if old is not None and actual is not None and actual != old:
                return status("application_mismatch", False)
            return status("reference_rejected" if terminal and rejection_reason == "unknown_reference" else "commit_unconfirmed")
        if record.get("status") == "provider_error":
            return status("provider_error")
        if record.get("status") in {"pending", "cancelled"}:
            return status("no_response")
        return summary
    try:
        # Revalidate saved evidence rather than trusting a manually edited
        # record. The same canonical digest is used by the commit transaction.
        output = parse_extraction_output(json.dumps(record["output"], ensure_ascii=False, allow_nan=False))
        output_digest = digest(output.model_dump(mode="json"))
    except (KeyError, TypeError, ValueError, RecursionError):
        return summary
    if record.get("output_digest") != output_digest:
        return summary
    changes = output.profile_changes
    summary["profile_candidate_count"] = len(changes)
    if receipt is not None:
        summary["receipt_digest_matches"] = receipt.get("output_digest") == output_digest
        if not summary["receipt_digest_matches"]:
            return status("application_mismatch", False)
        summary["commit_confirmed"] = True
    if old is None or actual is None:
        return summary
    if receipt is None:
        if actual != old:
            return status("application_mismatch", False)
        if terminal and rejection_reason in COMMIT_REJECTION_REASONS:
            return status("commit_rejected")
        return status("commit_unconfirmed")

    expected = dict(old)
    preconditions_match = True
    changed_users = set()
    for change in changes:
        user = change.logical_user_id
        changed_users.add(user)
        previous = old.get(user)
        previous_version = previous["content_version"] if previous else 0
        preconditions_match &= change.expected_version == previous_version
        changed = previous is None or (
            previous["description"] != change.description or previous["tags"] != change.tags
        )
        expected[user] = {
            "content_version": previous_version + int(changed),
            "description": change.description,
            "tags": change.tags,
        }
    candidate_match = preconditions_match and all(actual.get(user) == expected[user] for user in changed_users)
    unchanged_match = (
        {user: item for user, item in actual.items() if user not in changed_users}
        == {user: item for user, item in old.items() if user not in changed_users}
    )
    summary.update(candidate_profiles_match=candidate_match, unproposed_profiles_unchanged=unchanged_match)
    if not candidate_match or not unchanged_match:
        return status("application_mismatch", False)
    return status("profiles_applied" if changes else "profile_not_proposed", True)
