"""Candidate/commit attribution must not masquerade as semantic quality."""

import asyncio
import copy
import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from core.extraction_apply import digest
from core.extraction_references import ExtractionReferences
from core.extraction_regeneration import VERSION as REGENERATION_VERSION
from core.extraction_schema import MAX_OUTPUT_CHARS
from scripts.profile_candidate_diagnostics import CandidateRecorder, diagnose_profiles
from tests.test_extraction_references import batch as reference_batch


def candidate(*, user="PRIVATE_USER", tags=None, description="PRIVATE_BODY", version=0):
    return {"logical_user_id": user, "expected_version": version,
            "tags": ["PRIVATE_TAG"] if tags is None else tags,
            "description": description, "source_ids": ["PRIVATE_SOURCE"]}


def output(*changes):
    return {"schema_version": "extraction.v1", "result": "changes" if changes else "no_change",
            "reason": "changes" if changes else "already_known", "node_changes": [], "edges": [],
            "profile_changes": list(changes), "relationship_change": None, "coverage_changes": []}


def valid_record(*changes):
    value = output(*changes)
    return {"status": "valid_output", "output": value, "output_digest": digest(value)}


def snapshot(*profiles):
    return {"profiles": list(profiles), "nodes": [], "relationships": []}


def profile(*, user="PRIVATE_USER", tags=None, description="PRIVATE_BODY", version=1):
    return {"logical_user_id": user, "content_version": version,
            "tags": ["PRIVATE_TAG"] if tags is None else tags, "description": description}


def diagnose(record, before, after, *, committed=True, terminal=True, rejection_reason=None):
    receipt = {"output_digest": record.get("output_digest")} if committed else None
    result = diagnose_profiles([record], before, after, receipt,
                               terminal=terminal, rejection_reason=rejection_reason)
    assert "PRIVATE" not in json.dumps(result)
    return result


@pytest.mark.asyncio
async def test_recorder_preserves_call_and_return_and_records_only_schema_fields():
    value = output(candidate())
    raw = json.dumps(value, indent=2)
    messages = [{"role": "user", "content": "PRIVATE_PROMPT"}]
    seen = []

    async def generate(actual_messages, **kwargs):
        seen.append((actual_messages, kwargs))
        return raw

    recorder = CandidateRecorder(generate)
    assert await recorder(messages, provider_id="PRIVATE_PROVIDER") is raw
    assert seen == [(messages, {"provider_id": "PRIVATE_PROVIDER"})]
    assert recorder.records == [{"status": "valid_output", "output": value,
                                 "output_digest": digest(value), "raw_length": len(raw),
                                 "raw_sha256": hashlib.sha256(raw.encode()).hexdigest()}]
    assert "PRIVATE_PROMPT" not in json.dumps(recorder.records)
    assert "PRIVATE_PROVIDER" not in json.dumps(recorder.records)


@pytest.mark.asyncio
async def test_reference_recorder_preserves_wire_bytes_and_matches_canonical_receipt(reference_batch):
    batch = replace(reference_batch, reference_protocol="reference.v1")
    refs = ExtractionReferences(batch)
    change = candidate(user="user_first_long")
    change["source_ids"] = ["source_first_long"]
    canonical = output(change)
    wire = json.dumps(json.loads(refs.encode_output(json.dumps(canonical))), indent=2)

    async def generate(messages):
        return wire

    recorder = CandidateRecorder(generate)
    recorder.references = refs
    assert await recorder(refs.messages()) is wire
    record = recorder.records[0]
    assert record["wire_raw"] == wire
    assert record["wire_output"]["profile_changes"][0]["logical_user_id"] == "u1"
    assert record["raw_sha256"] == hashlib.sha256(wire.encode()).hexdigest()
    assert record["output"] == canonical and record["output_digest"] == digest(canonical)
    assert record["wire_output_digest"] != record["output_digest"]
    assert record["input_digest"] == batch.input_digest
    assert diagnose(record, snapshot(), snapshot(profile(user="user_first_long")))["consistency_ok"] is True


@pytest.mark.asyncio
async def test_scoped_profile_candidate_records_edits_and_materialized_content(reference_batch):
    batch = replace(reference_batch, reference_protocol="reference.v1", job_id="scoped")
    raw = json.dumps({"profile_changes": [{"logical_user_id": "u2", "tags": [], "description": "Uses a reed.",
                       "remove_sentences": [1], "remove_tags": [], "source_ids": ["s2"]}]}, indent=2)

    async def generate(messages):
        assert "known_nodes" not in json.loads(messages[1]["content"])
        return raw

    async def prepare():
        return batch

    async def get_job(_):
        return {"kind": "profile_rebuild", "checkpoint": {}}

    recorder = CandidateRecorder(generate)
    extraction = SimpleNamespace(prepare=prepare)
    recorder.observe_preparation(extraction, SimpleNamespace(get=get_job))
    await extraction.prepare()
    assert await recorder(recorder.expected_messages) is raw
    record, = recorder.records
    assert record["status"] == "valid_output"
    assert record["wire_raw"] == raw and record["wire_output"] == json.loads(raw)
    change, = record["output"]["profile_changes"]
    assert change["tags"] == ["musician"] and change["description"] == "Uses a reed."
    assert change["logical_user_id"] == "user_second_long" and change["source_ids"] == ["source_second_long"]
    assert record["output_digest"] == digest(record["output"])


@pytest.mark.asyncio
async def test_reference_recorder_rejects_modified_prompt_before_call(reference_batch):
    refs = ExtractionReferences(replace(reference_batch, reference_protocol="reference.v1"))

    async def generate(messages):
        pytest.fail("Modified input must not reach the evaluator's provider")

    recorder = CandidateRecorder(generate)
    recorder.references = refs
    messages = refs.messages()
    payload = json.loads(messages[-1]["content"])
    payload["sources"][0]["content_text"] = "Changed evidence"
    messages[-1]["content"] = json.dumps(payload)
    with pytest.raises(ValueError, match="model_input_snapshot_mismatch"):
        await recorder(messages)


@pytest.mark.asyncio
@pytest.mark.parametrize("tamper", [None, "previous_candidate", "feedback"])
async def test_reference_recorder_validates_complete_regeneration_transcript(reference_batch, tamper):
    batch = replace(reference_batch, reference_protocol="reference.v1", job_id="synthetic-job")
    previous = {"source_ids": [source["source_id"] for source in batch.sources],
        "source_cursor": 2, "known_node_ids": [node["node_id"] for node in batch.known_nodes],
        "input_digest": batch.input_digest, "signature": "fixed-signature", "reference_protocol": "reference.v1",
        "raw": "synthetic invalid JSON"}
    regeneration = {"version": REGENERATION_VERSION, "candidate_id": "candidate_synthetic"}
    window = {key: value for key, value in previous.items() if key != "raw"}
    window["regeneration"] = regeneration

    async def get_job(job_id):
        assert job_id == batch.job_id
        return {"checkpoint": {"window": window}}

    async def get_candidate(job_id, candidate_id):
        assert job_id == batch.job_id and candidate_id == regeneration["candidate_id"]
        return {"candidate_id": candidate_id, "window": previous, "error_code": "invalid_output"}

    async def prepare():
        return batch

    provider_calls = []
    raw = json.dumps(output())

    async def generate(messages):
        provider_calls.append(messages)
        return raw

    recorder = CandidateRecorder(generate)
    extraction = SimpleNamespace(prepare=prepare)
    recorder.observe_preparation(extraction, SimpleNamespace(get=get_job, get_candidate=get_candidate))
    assert await extraction.prepare() is batch
    messages = copy.deepcopy(recorder.expected_messages)
    assert len(messages) == 4 and messages[:2] == ExtractionReferences(batch).messages()
    assert messages[-2] == {"role": "assistant", "content": previous["raw"]}
    assert json.loads(messages[-1]["content"])["protocol"] == REGENERATION_VERSION
    if tamper is not None:
        messages[-2 if tamper == "previous_candidate" else -1]["content"] = "Changed transcript"
        with pytest.raises(ValueError, match="model_input_snapshot_mismatch"):
            await recorder(messages)
        assert not provider_calls
    else:
        assert await recorder(messages) is raw
        assert len(provider_calls) == 1
        assert recorder.records[0]["regeneration"] == regeneration
        assert recorder.records[0]["prompt_sha256"] == digest(messages)


@pytest.mark.asyncio
async def test_unknown_reference_retains_wire_evidence_without_claiming_canonical_commit(reference_batch):
    refs = ExtractionReferences(replace(reference_batch, reference_protocol="reference.v1"))
    change = candidate(user="u1")
    change["source_ids"] = ["s999"]
    raw = json.dumps(output(change))

    async def generate(messages):
        return raw

    recorder = CandidateRecorder(generate)
    recorder.references = refs
    assert await recorder(refs.messages()) is raw
    record = recorder.records[0]
    assert record["wire_raw"] == raw and record["status"] == "reference_invalid"
    assert "output" not in record and "output_digest" not in record
    result = diagnose_profiles([record], snapshot(), snapshot(), None,
                               terminal=True, rejection_reason="unknown_reference")
    assert result["status"] == "reference_rejected" and not result["commit_confirmed"]


@pytest.mark.asyncio
@pytest.mark.parametrize("raw,kind", [("PRIVATE_BAD_JSON", "schema"),
                                     ("PRIVATE_" * MAX_OUTPUT_CHARS, "oversize"),
                                     ({"PRIVATE": "not a string"}, "not_text")])
async def test_recorder_does_not_save_invalid_body_or_change_return(raw, kind):
    async def generate(messages):
        return raw

    recorder = CandidateRecorder(generate)
    assert await recorder([]) is raw
    record = recorder.records[0]
    assert record["status"] == "invalid_output" and record["invalid_kind"] == kind
    assert "output" not in record and "PRIVATE" not in json.dumps(record)


@pytest.mark.asyncio
async def test_recorder_never_saves_unexpected_schema_fields():
    value = output()
    value["PRIVATE_EXTRA"] = "PRIVATE_SECRET"

    async def generate(messages):
        return json.dumps(value)

    recorder = CandidateRecorder(generate)
    await recorder([])
    assert recorder.records[0]["status"] == "invalid_output"
    assert "PRIVATE" not in json.dumps(recorder.records)


@pytest.mark.asyncio
@pytest.mark.parametrize("exception", [TimeoutError("PRIVATE_EXCEPTION"), asyncio.CancelledError("PRIVATE_CANCEL")])
async def test_provider_exception_and_cancellation_propagate_unchanged(exception):
    async def generate(messages):
        raise exception

    recorder = CandidateRecorder(generate)
    with pytest.raises(type(exception)) as caught:
        await recorder([])
    assert caught.value is exception
    assert recorder.records == [{"status": "cancelled" if isinstance(exception, asyncio.CancelledError)
                                 else "provider_error", "exception_type": type(exception).__name__}]
    assert "PRIVATE" not in json.dumps(recorder.records)


@pytest.mark.asyncio
async def test_recorder_limits_concurrent_slots_without_changing_provider_calls():
    gate = asyncio.Event()
    calls = 0

    async def generate(messages):
        nonlocal calls
        calls += 1
        await gate.wait()
        return json.dumps(output())

    recorder = CandidateRecorder(generate)
    tasks = [asyncio.create_task(recorder([])) for _ in range(10)]
    await asyncio.sleep(0)
    assert len(recorder.records) == 8 and recorder.overflow_count == 2
    assert all(record["status"] == "pending" for record in recorder.records)
    gate.set()
    await asyncio.gather(*tasks)
    assert calls == 10 and len(recorder.records) == 8


@pytest.mark.asyncio
async def test_diagnostic_failure_does_not_change_provider_result(monkeypatch):
    raw = json.dumps(output(candidate()))

    def broken_digest(value):
        raise RuntimeError("PRIVATE_DIAGNOSTIC_ERROR")

    monkeypatch.setattr("scripts.profile_candidate_diagnostics.digest", broken_digest)

    async def generate(messages):
        return raw

    recorder = CandidateRecorder(generate)
    assert await recorder([]) is raw
    assert recorder.records == [{"status": "diagnostic_error", "exception_type": "RuntimeError"}]


def test_new_profile_receipt_and_complete_content_confirm_application():
    result = diagnose(valid_record(candidate()), snapshot(), snapshot(profile()))
    assert result["status"] == "profiles_applied"
    assert result["consistency_ok"] is True and result["commit_confirmed"] is True
    assert result["profile_candidate_count"] == 1


@pytest.mark.parametrize("description,tags,version", [("PRIVATE_BODY", ["PRIVATE_TAG"], 3),
                                                     ("PRIVATE_NEW_BODY", ["PRIVATE_TAG"], 4),
                                                     ("PRIVATE_BODY", ["PRIVATE_NEW_TAG"], 4),
                                                     ("", [], 4)])
def test_version_changes_only_with_description_or_full_tags(description, tags, version):
    record = valid_record(candidate(version=3, description=description, tags=tags))
    result = diagnose(record, snapshot(profile(version=3)),
                      snapshot(profile(version=version, description=description, tags=tags)))
    assert result["status"] == "profiles_applied" and result["consistency_ok"] is True


@pytest.mark.parametrize("mutation", ["missing", "description", "tags", "tag_order", "version", "wrong_user"])
def test_profile_application_must_match_full_candidate(mutation):
    record = valid_record(candidate(tags=["PRIVATE_A", "PRIVATE_B"]))
    stored = profile(tags=["PRIVATE_A", "PRIVATE_B"])
    if mutation == "description":
        stored["description"] = "PRIVATE_DIFFERENT"
    elif mutation == "tags":
        stored["tags"] = ["PRIVATE_A"]
    elif mutation == "tag_order":
        stored["tags"].reverse()
    elif mutation == "version":
        stored["content_version"] = 2
    elif mutation == "wrong_user":
        stored["logical_user_id"] = "PRIVATE_OTHER"
    after = snapshot() if mutation == "missing" else snapshot(stored)
    result = diagnose(record, snapshot(), after)
    assert result["status"] == "application_mismatch" and result["consistency_ok"] is False


def test_stale_candidate_cannot_be_confirmed_even_if_after_content_matches():
    record = valid_record(candidate(version=7, description="PRIVATE_NEW"))
    result = diagnose(record, snapshot(profile(version=3)), snapshot(profile(version=4, description="PRIVATE_NEW")))
    assert result["status"] == "application_mismatch"


@pytest.mark.parametrize("mutation", ["delete", "edit", "add"])
def test_profiles_outside_candidate_must_remain_unchanged(mutation):
    before = snapshot(profile(user="PRIVATE_OTHER"))
    after = snapshot(profile(), profile(user="PRIVATE_OTHER"))
    if mutation == "delete":
        after["profiles"].pop()
    elif mutation == "edit":
        after["profiles"][1]["description"] = "PRIVATE_UNEXPECTED"
    else:
        after["profiles"].append(profile(user="PRIVATE_EXTRA"))
    result = diagnose(valid_record(candidate()), before, after)
    assert result["candidate_profiles_match"] is True
    assert result["unproposed_profiles_unchanged"] is False
    assert result["status"] == "application_mismatch"


def test_no_profile_proposal_does_not_claim_semantic_success():
    result = diagnose(valid_record(), snapshot(profile()), snapshot(profile()))
    assert result["status"] == "profile_not_proposed" and result["consistency_ok"] is True
    assert result["profile_candidate_count"] == 0
    assert not any("semantic" in key for key in result)


def test_no_profile_proposed_but_profile_created_is_mismatch():
    result = diagnose(valid_record(), snapshot(), snapshot(profile()))
    assert result["status"] == "application_mismatch" and result["consistency_ok"] is False


@pytest.mark.parametrize("terminal,reason,expected", [
    (True, "context_or_evidence_invalid", "commit_rejected"),
    (True, "protected_candidate", "commit_rejected"),
    (True, "expected_profile_version_mismatch", "commit_rejected"),
    (True, "evidence_user_mismatch", "commit_rejected"),
    (True, "invalid_contains_hierarchy", "commit_rejected"),
    (True, "coverage_event_version_mismatch", "commit_rejected"),
    (False, "protected_candidate", "commit_unconfirmed"),
    (False, "expected_profile_version_mismatch", "commit_unconfirmed"),
    (True, "provider_temporary_failure", "commit_unconfirmed"),
    (True, "PRIVATE_TIMEOUT_MESSAGE", "commit_unconfirmed"),
    (True, None, "commit_unconfirmed"),
])
def test_missing_receipt_distinguishes_explicit_apply_rejection_from_timeout(terminal, reason, expected):
    result = diagnose(valid_record(candidate()), snapshot(), snapshot(), committed=False,
                      terminal=terminal, rejection_reason=reason)
    assert result["status"] == expected and result["consistency_ok"] is None
    assert result["commit_confirmed"] is False


@pytest.mark.parametrize("reason", ["context_or_evidence_invalid", "expected_profile_version_mismatch"])
def test_missing_receipt_with_profile_mutation_never_claims_commit_or_rejection(reason):
    result = diagnose(valid_record(candidate()), snapshot(), snapshot(profile()), committed=False,
                      rejection_reason=reason)
    assert result["status"] == "application_mismatch" and not result["commit_confirmed"]


def test_receipt_digest_mismatch_cannot_confirm_even_identical_profile():
    record = valid_record(candidate())
    result = diagnose_profiles([record], snapshot(), snapshot(profile()),
                               {"output_digest": "PRIVATE_WRONG_DIGEST"}, terminal=True)
    assert result["status"] == "application_mismatch" and result["consistency_ok"] is False
    assert result["receipt_digest_matches"] is False and not result["commit_confirmed"]
    assert "PRIVATE" not in json.dumps(result)


@pytest.mark.parametrize("records,receipt,status", [
    ([], None, "no_response"),
    ([], {"output_digest": "PRIVATE"}, "evidence_incomplete"),
    ([valid_record(), valid_record()], None, "evidence_incomplete"),
    ([{"status": "provider_error", "exception_type": "TimeoutError"}], None, "provider_error"),
    ([{"status": "cancelled"}], None, "no_response"),
    ([{"status": "pending"}], None, "no_response"),
    ([{"status": "invalid_output"}], None, "invalid_output"),
    ([{"status": "diagnostic_error"}], None, "evidence_incomplete"),
    ([{"status": "provider_error"}], {"output_digest": "PRIVATE"}, "evidence_incomplete"),
])
def test_partial_evidence_cannot_establish_consistency(records, receipt, status):
    result = diagnose_profiles(records, snapshot(), snapshot(), receipt, terminal=True)
    assert result["status"] == status and result["consistency_ok"] is None
    assert not result["commit_confirmed"] and "PRIVATE" not in json.dumps(result)


def test_invalid_output_cannot_match_commit_receipt():
    result = diagnose_profiles([{"status": "invalid_output"}], snapshot(), snapshot(),
                               {"output_digest": "PRIVATE"}, terminal=True)
    assert result["status"] == "application_mismatch" and result["consistency_ok"] is False


@pytest.mark.parametrize("mutation", ["missing_snapshot", "duplicate_user", "missing_tags", "bool_version", "changed_digest", "invalid_candidate"])
def test_malformed_or_tampered_evidence_is_incomplete(mutation):
    record = valid_record(candidate())
    before, after = snapshot(), snapshot(profile())
    if mutation == "missing_snapshot":
        after = {}
    elif mutation == "duplicate_user":
        after["profiles"].append(copy.deepcopy(after["profiles"][0]))
    elif mutation == "missing_tags":
        after["profiles"][0].pop("tags")
    elif mutation == "bool_version":
        after["profiles"][0]["content_version"] = True
    elif mutation == "changed_digest":
        record["output_digest"] = "PRIVATE_TAMPERED"
    else:
        record["output"]["profile_changes"][0]["expected_version"] = -1
    result = diagnose(record, before, after)
    assert result["status"] == "evidence_incomplete" and result["consistency_ok"] is None


@pytest.mark.parametrize('corrected', [True, False])
@pytest.mark.parametrize('invalid_kind', ['invalid_output', 'reference_invalid'])
def test_same_input_candidate_correction_is_attributed_to_final_candidate(corrected, invalid_kind):
    final = valid_record(candidate()) if corrected else {'status': invalid_kind}
    final.update(input_digest='synthetic-input', regeneration={'candidate_id': 'synthetic-rejected'})
    records = [{'status': invalid_kind, 'input_digest': 'synthetic-input'}, final]
    result = diagnose_profiles(records, snapshot(), snapshot(profile()) if corrected else snapshot(),
        {'output_digest': final['output_digest']} if corrected else None, terminal=True, rejection_reason='unknown_reference')
    assert result['record_count'] == 2
    assert result['status'] == ('profiles_applied' if corrected else 'reference_rejected' if invalid_kind == 'reference_invalid' else 'invalid_output')
    assert result['commit_confirmed'] is corrected
    records[0]['input_digest'] = 'different-input'
    result = diagnose_profiles(records, snapshot(), snapshot(), None, terminal=True)
    assert result['status'] == 'evidence_incomplete'
