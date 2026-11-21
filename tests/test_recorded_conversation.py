"""Private replay mechanics use synthetic records, never the real conversation."""

import json
import os
import sqlite3
from types import SimpleNamespace

import pytest

from scripts import verify_recorded_conversation as replay
from storage.source_store import SourceStore
from tests.test_extraction_apply import capture


async def source_fixture(tmp_path, *, count=85, text="PRIVATE_SOURCE_BODY", persona="synthetic-persona"):
    source = SourceStore(tmp_path / "input.db")
    await source.open()
    for index in range(1, count + 1):
        await capture(source, text, number=index, persona=persona, actor="agent" if index % 2 == 0 else "user")
    await source.close()
    persona_file = tmp_path / "card.md"
    persona_file.write_text("PRIVATE_PERSONA_BODY", encoding="utf-8")
    return source.path, persona_file


def no_change():
    return {"schema_version": "extraction.v1", "result": "no_change", "reason": "no_meaningful_content",
            "node_changes": [], "edges": [], "profile_changes": [], "relationship_change": None, "coverage_changes": []}


def fake_factory(monkeypatch, *, fail_call=None, malformed=False, profile=False):
    calls, closes = [], []
    async def build(config, sources, memory, directory, credentials_path, budget):
        async def generate(messages, **kwargs):
            budget.active_call, budget.call_count = True, 0
            try:
                await budget.count_request(None)
            finally:
                budget.active_call = False
            payload = json.loads(messages[-1]["content"])
            calls.append(payload)
            if len(calls) == fail_call:
                raise TimeoutError("PRIVATE_PROVIDER_FAILURE")
            if malformed:
                return "PRIVATE_INVALID_OUTPUT"
            output = no_change()
            if profile:
                old = payload["known_profiles"]
                output.update(result="changes", reason="changes", profile_changes=[{
                    "logical_user_id": payload["space"]["logical_user_id"],
                    "expected_version": old[0]["content_version"] if old else 0,
                    "tags": ["PRIVATE_TAG"], "description": f"PRIVATE_PROFILE_{len(calls)}",
                    "source_ids": [next(row["source_id"] for row in payload["sources"] if row["actor_kind"] == "user")],
                }])
            return json.dumps(output)
        async def close():
            closes.append(True)
        adapter = SimpleNamespace(generate=generate, llm_fingerprint=lambda *args: "test-fingerprint")
        runtime = SimpleNamespace(adapter=adapter, recall=None, cache=None, plugin=SimpleNamespace(), close=close)
        return runtime, SimpleNamespace(terminate=close), {"host_version": "synthetic-test", "model": "fake"}
    monkeypatch.setattr(replay, "build_runtime", build)
    return calls, closes


@pytest.mark.asyncio
async def test_offline_plan_uses_default_windows_without_credentials_or_network(tmp_path, monkeypatch):
    source, persona = await source_fixture(tmp_path, count=405)
    before = replay.sha256_file(source)
    async def forbidden(*args):
        raise AssertionError("offline must not load provider")
    monkeypatch.setattr(replay, "build_runtime", forbidden)
    output = tmp_path / "plan"
    report = await replay.replay(source, persona, output, credentials_path=tmp_path / "does-not-exist")
    assert report["state"] == "planned" and report["structure_ok"]
    assert report["planned_windows"] == 11 and report["selected_windows"] == 8
    assert report["selected_sources"] == 320 and report["untested_sources"] == 405
    assert report["http_requests"] == {"llm": 0, "embedding": 0}
    assert report["window_policy"] == {"min_messages": 5, "topic_messages": 20, "max_messages": 40,
                                       "idle_seconds": 7200.0, "max_chars": 24000, "max_tokens": 24000}
    assert replay.sha256_file(source) == before
    assert os.stat(output).st_mode & 0o777 == 0o700
    assert all(os.stat(path).st_mode & 0o777 == 0o600 for path in output.iterdir())
    assert "PRIVATE" not in json.dumps(report)
    plan = json.loads((output / "plan.json").read_text())
    assert plan["windows"][-1]["source_count"] == 5
    assert "PRIVATE_SOURCE_BODY" not in (output / "plan.json").read_text()
    with sqlite3.connect(output / "source.db") as db:
        assert db.execute("SELECT count(*) FROM source_records").fetchone()[0] == 405


@pytest.mark.asyncio
async def test_bounded_prefix_uses_background_rebuild_and_retains_private_evidence(tmp_path, monkeypatch):
    source, persona = await source_fixture(tmp_path)
    calls, closes = fake_factory(monkeypatch, profile=True)
    output = tmp_path / "run"
    report = await replay.replay(source, persona, output, run=True, max_windows=2, http_budget=2)
    assert report["state"] == "finished" and report["structure_ok"]
    assert report["semantic_status"] == "needs_review" and report["generation_activated"]
    assert report["consumed_sources"] == 80 and report["untested_sources"] == 5
    assert len(calls) == 2 and len(closes) == 2
    assert report["job"]["state"] == "succeeded" and report["job"]["attempt"] == 1
    assert report["http_requests"] == {"llm": 2, "embedding": 0}
    assert report["synthetic_only"] is False and report["not_for_tuning"] is True
    assert "PRIVATE" not in json.dumps(report)
    for index, payload in enumerate(calls, 1):
        case = json.loads((output / f"window-{index:03d}.json").read_text())
        assert case["model_input"]["wire_source_ids"] == [row["source_id"] for row in payload["sources"]]
        assert case["model_input"]["source_ids"] == case["source_ids"]
        assert case["model_input"]["reference_protocol"] == "reference.v1"
        assert case["model_input"]["input_digest"] == case["receipt"]["input_digest"]
        assert all(source_id.startswith("src_") for source_id in case["source_ids"])
        assert case["candidates"][0]["wire_output"]["profile_changes"][0]["logical_user_id"] == "u1"
        assert case["candidates"][0]["output"]["profile_changes"][0]["logical_user_id"] != "u1"
        assert case["candidates"][0]["output_digest"] == case["receipt"]["output_digest"]
        assert case["diagnostics"]["candidate_profiles_match"] is True
        assert case["after"]["profiles"][0]["content_version"] == index
        assert "PRIVATE_SOURCE_BODY" not in json.dumps(case)
        assert "PRIVATE_PERSONA_BODY" not in json.dumps(case)
        assert max(row["sequence_no"] for row in payload["sources"]) == index * 40
        assert all(row["sequence_no"] <= index * 40 for row in payload["sources"])
    assert calls[0]["known_profiles"] == []
    assert calls[1]["known_profiles"][0]["description"] == "PRIVATE_PROFILE_1"
    with sqlite3.connect(output / "memory.db") as db:
        assert db.execute("SELECT COUNT(*) FROM extraction_batches").fetchone()[0] == 2
        assert db.execute("SELECT COUNT(*) FROM memory_jobs").fetchone()[0] == 1
    assert (output / "source.db").exists() and (output / "memory.db").exists()


@pytest.mark.asyncio
async def test_first_failure_stops_and_preserves_committed_prefix_and_failed_job(tmp_path, monkeypatch):
    source, persona = await source_fixture(tmp_path)
    calls, closes = fake_factory(monkeypatch, fail_call=2)
    output = tmp_path / "failed"
    report = await replay.replay(source, persona, output, run=True)
    assert report["state"] == "failed" and not report["structure_ok"]
    assert len(calls) == 2 and len(closes) == 2
    assert report["consumed_sources"] == 40 and report["untested_sources"] == 45
    assert report["job"]["state"] == "failed" and report["job"]["attempt"] == 1
    assert report["job"]["error_code"] == "provider_temporary_failure"
    assert report["generation_activated"] is False
    assert "PRIVATE_PROVIDER_FAILURE" not in json.dumps(report)
    assert report["windows"][0]["structure_ok"] and not report["windows"][1]["structure_ok"]
    with sqlite3.connect(output / "memory.db") as db:
        checkpoint = json.loads(db.execute("SELECT checkpoint_json FROM memory_jobs").fetchone()[0])
        assert checkpoint["done"] == 40 and checkpoint["window"]["source_ids"]
        assert db.execute("SELECT COUNT(*) FROM extraction_batches").fetchone()[0] == 1


@pytest.mark.asyncio
async def test_invalid_output_is_not_saved_as_candidate_body_or_success(tmp_path, monkeypatch):
    source, persona = await source_fixture(tmp_path, count=5)
    calls, _ = fake_factory(monkeypatch, malformed=True)
    output = tmp_path / "invalid"
    report = await replay.replay(source, persona, output, run=True)
    assert report["state"] == "failed" and len(calls) == 4
    assert report["job"]["error_code"] == "invalid_output"
    case = json.loads((output / "window-001.json").read_text())
    assert case["candidates"][0]["status"] == "invalid_output"
    assert "PRIVATE_INVALID_OUTPUT" not in json.dumps(case)
    assert case["receipt"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["persona", "source"])
async def test_conversion_manifest_mismatch_rejected_before_provider(tmp_path, monkeypatch, kind):
    source, persona = await source_fixture(tmp_path, count=5)
    value = {"persona": {"sha256": replay.sha256_file(persona)}, "output": {"sha256": replay.sha256_file(source)}}
    value["persona" if kind == "persona" else "output"]["sha256"] = "wrong"
    (tmp_path / "manifest.json").write_text(json.dumps(value))
    calls, _ = fake_factory(monkeypatch)
    report = await replay.replay(source, persona, tmp_path / "mismatch", run=True)
    assert report["state"] == "failed" and not calls
    assert not (tmp_path / "mismatch/source.db").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [1, 4])
async def test_short_tail_is_not_forced_into_model(tmp_path, monkeypatch, count):
    source, persona = await source_fixture(tmp_path, count=count)
    calls, _ = fake_factory(monkeypatch)
    report = await replay.replay(source, persona, tmp_path / "short", run=True)
    assert report["state"] == "failed" and report["selected_windows"] == 0
    assert report["unplanned_sources"] == count and not calls


@pytest.mark.asyncio
async def test_existing_output_is_never_overwritten(tmp_path):
    source, persona = await source_fixture(tmp_path, count=5)
    output = tmp_path / "already"
    output.mkdir()
    marker = output / "marker"
    marker.write_text("keep")
    with pytest.raises(FileExistsError):
        await replay.replay(source, persona, output)
    assert list(output.iterdir()) == [marker] and marker.read_text() == "keep"


@pytest.mark.asyncio
async def test_source_hash_corruption_rejected_without_calls(tmp_path, monkeypatch):
    source, persona = await source_fixture(tmp_path, count=5)
    with sqlite3.connect(source) as db:
        db.execute("UPDATE source_records SET source_hash='invalid'")
    calls, _ = fake_factory(monkeypatch)
    report = await replay.replay(source, persona, tmp_path / "corrupted", run=True)
    assert report["state"] == "failed" and not calls


@pytest.mark.asyncio
async def test_database_with_multiple_personas_rejected(tmp_path, monkeypatch):
    source, persona = await source_fixture(tmp_path, count=5)
    with sqlite3.connect(source) as db:
        db.execute("UPDATE source_records SET persona_id='other' WHERE sequence_no=5")
    calls, _ = fake_factory(monkeypatch)
    report = await replay.replay(source, persona, tmp_path / "mixed", run=True)
    assert report["state"] == "failed" and not calls


@pytest.mark.asyncio
async def test_http_cap_counts_attempts_and_rejects_provider_internal_retry():
    budget = replay.HttpBudget(2)
    with pytest.raises(PermissionError):
        await budget.count_request(None)
    for _ in range(2):
        budget.active_call, budget.call_count = True, 0
        await budget.count_request(None)
        with pytest.raises(PermissionError):
            await budget.count_request(None)
    budget.active_call, budget.call_count = True, 0
    with pytest.raises(PermissionError):
        await budget.count_request(None)
    assert budget.count == 2


@pytest.mark.asyncio
async def test_insufficient_http_budget_rejected_before_runtime(tmp_path, monkeypatch):
    source, persona = await source_fixture(tmp_path)
    calls, _ = fake_factory(monkeypatch)
    report = await replay.replay(source, persona, tmp_path / "budget", run=True, max_windows=3, http_budget=2)
    assert report["state"] == "failed" and not calls


@pytest.mark.asyncio
async def test_oversized_record_stays_unplanned_without_truncation(tmp_path, monkeypatch):
    source, persona = await source_fixture(tmp_path, count=5, text="x" * 25000)
    calls, _ = fake_factory(monkeypatch)
    report = await replay.replay(source, persona, tmp_path / "oversized", run=True)
    assert report["state"] == "failed" and report["planning_status"] == "oversized"
    assert report["selected_sources"] == 0 and report["unplanned_sources"] == 5 and not calls


@pytest.mark.asyncio
async def test_nonchronological_input_rejected_before_model(tmp_path, monkeypatch):
    source, persona = await source_fixture(tmp_path, count=5)
    with sqlite3.connect(source) as db:
        db.execute("UPDATE source_records SET observed_at=0 WHERE sequence_no=3")
    calls, _ = fake_factory(monkeypatch)
    report = await replay.replay(source, persona, tmp_path / "unordered", run=True)
    assert report["state"] == "failed" and not calls


@pytest.mark.asyncio
async def test_backup_contains_committed_wal_without_mutating_input(tmp_path):
    source = SourceStore(tmp_path / "wal-input.db")
    await source.open()
    try:
        for index in range(1, 6):
            await capture(source, "WAL_PRIVATE_BODY", number=index)
        before = replay.sha256_file(source.path)
        replay.backup_source(source.path, tmp_path / "copy.db")
        with sqlite3.connect(tmp_path / "copy.db") as copied:
            assert copied.execute("SELECT COUNT(*) FROM source_records").fetchone()[0] == 5
        assert replay.sha256_file(source.path) == before
    finally:
        await source.close()


@pytest.mark.asyncio
async def test_manifest_canonical_digest_mismatch_caught_even_with_file_hash(tmp_path, monkeypatch):
    source, persona = await source_fixture(tmp_path, count=5)
    manifest = {"persona": {"sha256": replay.sha256_file(persona)},
                "output": {"sha256": replay.sha256_file(source), "rows": 5, "records_sha256": "wrong"}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    calls, _ = fake_factory(monkeypatch)
    report = await replay.replay(source, persona, tmp_path / "bad-record-digest", run=True)
    assert report["state"] == "failed" and not calls
