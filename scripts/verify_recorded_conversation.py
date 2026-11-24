"""Opt-in, bounded replay of a private, single-session source database.

The full database is copied with SQLite backup, never opened for mutation in
place. Default production windows/prompt are retained. This is observational
validation, not a dataset for prompt tuning or a general quality benchmark.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import io
import json
import logging
import os
import sqlite3
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from core.config import NeoMemoryConfig
from core.extraction_apply import digest
from core.maintenance import MaintenanceEngine
from core.prompts import PROMPT_VERSION
from scripts.profile_candidate_diagnostics import CandidateRecorder, diagnose_profiles, prepared_request
from scripts.verify_stage5_api import wait_terminal
from storage.memory_store import MemoryStore
from storage.source_store import SourceStore


def sha256_file(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def private_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
    os.replace(temporary, path)


def safe_error(exc):
    value = {"error_type": type(exc).__name__}
    status = getattr(exc, "status_code", None)
    if type(status) is int:
        value["http_status"] = status
    return value


def backup_source(source, destination):
    """Use read-only source access; include committed WAL contents in the copy."""
    source = Path(source).resolve(strict=True)
    with sqlite3.connect(source.as_uri() + "?mode=ro", uri=True) as original:
        descriptor = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.fchmod(descriptor, 0o600)
        os.close(descriptor)
        with sqlite3.connect(destination) as copied:
            original.backup(copied)
            if copied.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise ValueError("invalid_source_copy")


def verify_conversion_snapshot(database, manifest):
    """Bind committed WAL content to the converter's canonical record digest."""
    fields = ("source_id", "stream_id", "dedup_key", "source_type", "actor_kind", "actor_account_id",
              "persona_id", "content_text", "observed_at", "sequence_no", "message_id", "identity_pending",
              "content_json", "source_hash", "actor_logical_user_id")
    fingerprint, count = hashlib.sha256(), 0
    with sqlite3.connect(Path(database).resolve().as_uri() + "?mode=ro", uri=True) as copied:
        copied.row_factory = sqlite3.Row
        for row in copied.execute("SELECT * FROM source_records ORDER BY rowid"):
            canonical = {key: row[key] for key in fields}
            canonical["metadata"] = json.loads(row["metadata_json"])
            fingerprint.update(json.dumps(canonical, ensure_ascii=False, sort_keys=True,
                                          separators=(",", ":"), allow_nan=False).encode() + b"\n")
            count += 1
    expected = manifest["output"]
    if count != expected["rows"] or fingerprint.hexdigest() != expected["records_sha256"]:
        raise ValueError("conversion_snapshot_mismatch")


class HttpBudget:
    """Hard cap actual HTTP requests, including provider-internal attempts."""

    def __init__(self, limit):
        if type(limit) is not int or not 1 <= limit <= 8:
            raise ValueError("invalid_http_budget")
        self.limit, self.count = limit, 0
        self.active_call = False
        self.call_count = 0

    async def count_request(self, _request):
        if not self.active_call or self.call_count >= 1 or self.count >= self.limit:
            raise PermissionError("recorded_replay_http_budget_exceeded")
        self.count += 1
        self.call_count += 1


async def validate_scope(engine):
    sources, scanner = engine.sources, engine.scanner
    health = await sources.health()
    if health["stream_count"] != 1 or health["source_count"] < 1:
        raise ValueError("single_nonempty_stream_required")
    page = await scanner.discover_spaces(limit=200)
    if len(page.spaces) != 1 or page.spaces[0].space_type != "private":
        raise ValueError("single_private_space_required")
    space = await engine.memory.get_or_create_space(page.spaces[0])
    highwater = await scanner.highwater(space)
    if await scanner.count(space, highwater=highwater) != health["source_count"]:
        raise ValueError("ineligible_or_mixed_sources")
    after, previous_sequence, previous_time, count = 0, 0, float("-inf"), 0
    fingerprint = hashlib.sha256()
    while True:
        page = await scanner.page(space, after=after, highwater=highwater, limit=200)
        for row in page.records:
            if (row["sequence_no"] <= previous_sequence or row["observed_at"] < previous_time
                    or row["persona_id"] != space["persona_id"]):
                raise ValueError("source_order_or_persona_mismatch")
            owner = row["actor_logical_user_id"] if row["actor_kind"] == "user" else row["conversation_logical_user_id"]
            if owner != space["logical_user_id"]:
                raise ValueError("logical_user_mismatch")
            if hashlib.sha256(row["content_text"].encode()).hexdigest() != row["source_hash"]:
                raise ValueError("source_content_digest_mismatch")
            fingerprint.update((digest(row) + "\n").encode())
            previous_sequence, previous_time = row["sequence_no"], row["observed_at"]
            count += 1
        after = page.next_cursor
        if not page.has_more:
            break
    return space, highwater, {"source_count": count, "records_sha256": fingerprint.hexdigest(),
                              "stream_count": 1, "logical_user_count": 1}


async def plan_windows(engine, space, highwater):
    """Use precisely the production rebuild scanner and window decision path."""
    windows, after = [], 0
    total = await engine.scanner.count(space, highwater=highwater)
    while True:
        page = await engine.scanner.page(space, highwater=highwater, after=after, limit=200)
        decision = await engine._decision(space, page.records, replay=True)
        if decision.status != "ready":
            return {"windows": windows, "status": decision.status, "reason": decision.reason,
                    "unplanned_source_count": total - sum(w["source_count"] for w in windows),
                    "blocked_source_id": decision.blocked_source_id}
        by_id = {row["source_id"]: row for row in page.records}
        selected = [by_id[source_id] for source_id in decision.source_ids]
        after = max(row["scan_cursor"] for row in selected)
        windows.append({"index": len(windows) + 1, "source_ids": list(decision.source_ids),
                        "source_count": len(selected), "source_cursor": after,
                        "highwater": {row["stream_id"]: row["sequence_no"] for row in selected},
                        "source_digest": digest(selected), "reason": decision.reason,
                        "char_count": decision.char_count, "estimated_tokens": decision.estimated_tokens})


async def snapshot(memory, space, generation_id):
    nodes, offset = [], 0
    while True:
        page = await memory.list_nodes(space_id=space["space_id"], generation_id=generation_id,
                                       statuses=(), limit=500, offset=offset)
        nodes.extend(page)
        offset += len(page)
        if len(page) < 500:
            break
    profiles = await memory.list_profiles(space_id=space["space_id"], generation_id=generation_id)
    relationship = await memory.get_relationship(space_id=space["space_id"],
        logical_user_id=space["logical_user_id"], generation_id=generation_id)
    def select(items, fields):
        return [{key: item[key] for key in fields if key in item} for item in items]
    return {
        "nodes": select(nodes, ("node_id", "node_type", "content_version", "content_text", "status", "importance")),
        "profiles": select(profiles, ("logical_user_id", "content_version", "tags", "description", "status")),
        "relationships": select([relationship] if relationship else [],
                                ("logical_user_id", "content_version", "state_text", "state", "status")),
    }


class WindowObserver:
    """Instance-only observation; original production handler owns all writes."""

    def __init__(self, engine, directory, selected):
        self.engine, self.directory, self.selected = engine, directory, selected
        self.original_window = engine._window
        self.original_generate = engine.runtime.adapter.generate
        self.original_prepare = engine.extraction.prepare
        self.windows, self.current = [], None
        self.batch = None
        self.references = self.expected_messages = self.regeneration = self.scoped_kind = None
        engine._window = self.window
        engine.runtime.adapter.generate = self.generate
        engine.extraction.prepare = self.prepare

    async def prepare(self, *args, **kwargs):
        self.batch = await self.original_prepare(*args, **kwargs)
        self.references, self.expected_messages, self.regeneration, self.scoped_kind = await prepared_request(self.batch, self.engine.jobs)
        return self.batch

    async def generate(self, messages, **kwargs):
        record = {"status": "pending", "started_at": time.time()}
        self.current["candidates"].append(record)
        references = self.references
        if (messages != self.expected_messages
                or [source["source_id"] for source in self.batch.sources] != self.current["source_ids"]):
            raise ValueError("model_source_scope_mismatch")
        payload = json.loads(self.batch.messages()[-1]["content"])
        wire_payload = json.loads(references.messages()[-1]["content"])
        # Source bodies and persona remain in their original private files.
        self.current["model_input"] = {
            "source_ids": [source["source_id"] for source in payload["sources"]],
            "wire_source_ids": [source["source_id"] for source in wire_payload["sources"]],
            "reference_protocol": self.batch.reference_protocol,
            "input_digest": self.batch.input_digest,
            "prompt_sha256": digest(messages),
            "known_node_versions": [{"node_id": node["node_id"], "content_version": node["content_version"],
                                     "content_sha256": hashlib.sha256(node["content_text"].encode()).hexdigest()}
                                    for node in self.batch.known_nodes],
            "known_profiles": payload["known_profiles"],
            "known_relationship": payload["known_relationship"],
        }
        if self.regeneration is not None:
            self.current["model_input"]["regeneration"] = dict(self.regeneration)
            record["regeneration"] = dict(self.regeneration)
        record["input_digest"] = self.batch.input_digest
        try:
            raw = await self.original_generate(messages, **kwargs)
        except BaseException as exc:
            record.update(status="cancelled" if isinstance(exc, asyncio.CancelledError) else "provider_error", **safe_error(exc))
            raise
        finally:
            record["finished_at"] = time.time()
        CandidateRecorder._capture(record, raw, references, self.scoped_kind)
        return raw

    async def window(self, job, checkpoint, space, generation_id, source_ids, source_cursor, *, building=False):
        index = len(self.windows)
        if index >= len(self.selected) or list(source_ids) != self.selected[index]["source_ids"]:
            raise ValueError("execution_differs_from_frozen_plan")
        batch_id = "batch_" + digest([space["space_id"], generation_id, tuple(source_ids)])
        current = {"window": index + 1, "generation_id": generation_id, "batch_id": batch_id,
                   "source_ids": list(source_ids), "source_digest": self.selected[index]["source_digest"],
                   "before": await snapshot(self.engine.memory, space, generation_id), "candidates": []}
        self.current = current
        reason = None
        try:
            return await self.original_window(job, checkpoint, space, generation_id, source_ids, source_cursor, building=building)
        except BaseException as exc:
            reason = getattr(exc, "code", None)
            current["failure"] = safe_error(exc)
            raise
        finally:
            current["after"] = await snapshot(self.engine.memory, space, generation_id)
            receipt = await self.engine.memory.get_extraction_batch(batch_id)
            current["receipt"] = receipt
            current["diagnostics"] = diagnose_profiles(current["candidates"], current["before"], current["after"], receipt,
                                                       terminal=True, rejection_reason=reason)
            current["consumed_source_count"] = len(await self.engine.pipeline.consumed(generation_id, source_ids))
            current["structure_ok"] = (current["diagnostics"]["consistency_ok"] is True
                                       and current["consumed_source_count"] == len(source_ids))
            current["semantic_status"] = "needs_review" if current["structure_ok"] else "not_evaluated"
            name = f"window-{index + 1:03d}.json"
            private_json(self.directory / name, current)
            self.windows.append({"index": index + 1, "structure_ok": current["structure_ok"],
                                 "semantic_status": current["semantic_status"], "source_count": len(source_ids),
                                 "consumed_source_count": current["consumed_source_count"],
                                 "diagnostics": current["diagnostics"], "evidence_file": name,
                                 "evidence_sha256": sha256_file(self.directory / name)})
            self.current = None


async def build_runtime(config, sources, memory, directory, credentials_path, budget):
    """Only called by --run, after complete offline validation/planning."""
    host_directory = directory / "host-runtime"
    for path in (host_directory, host_directory / "data", host_directory / "data/config"):
        path.mkdir(mode=0o700, exist_ok=True)
    os.environ["ASTRBOT_ROOT"] = str(host_directory.resolve())
    from astrbot import __version__ as host_version
    from astrbot.core.provider.sources.openai_source import ProviderOpenAIOfficial
    from astrbot.core.star.context import Context

    from adapters.runtime import MemoryRuntime
    from scripts.verify_stage4_api import credentials

    secret = credentials(credentials_path)["ecoapi"]
    llm = ProviderOpenAIOfficial({"id": config.llm_provider_id, "type": "openai_chat_completion",
        "model": secret["model"], "api_base": secret["base_url"], "key": [secret["api_token"]],
        "timeout": config.extraction_timeout_seconds}, {})
    try:
        llm.client.max_retries = 0
        llm.client._client.event_hooks["request"].append(budget.count_request)
        providers = {config.llm_provider_id: llm}
        async def get_provider_by_id(provider_id):
            return providers.get(provider_id)
        host = object.__new__(Context)
        host.provider_manager = SimpleNamespace(inst_map=providers, provider_insts=[llm],
            embedding_provider_insts=[], get_provider_by_id=get_provider_by_id)
        plugin = SimpleNamespace(context=host, config=config, data_dir=directory, source_store=sources,
                                 memory_store=memory, maintenance=None, ready=True)
        original_llm_generate = host.llm_generate
        async def single_attempt(**kwargs):
            # The host has its own retry helper in addition to OpenAI SDK retry.
            kwargs["request_max_retries"] = 1
            return await original_llm_generate(**kwargs)
        host.llm_generate = single_attempt
        runtime = MemoryRuntime(plugin)
        original_generate = runtime.adapter.generate
        async def bounded_generate(messages, **kwargs):
            if budget.active_call:
                raise PermissionError("concurrent_recorded_replay")
            budget.active_call, budget.call_count = True, 0
            try:
                return await original_generate(messages, **kwargs)
            finally:
                budget.active_call = False
        runtime.adapter.generate = bounded_generate
        return runtime, llm, {"host_version": host_version, "model": secret["model"]}
    except BaseException:
        await llm.terminate()
        raise


async def replay(source_db, persona_file, output_dir, *, run=False, max_windows=8, http_budget=8, credentials_path=None):
    if type(max_windows) is not int or not 1 <= max_windows <= 8:
        raise ValueError("invalid_window_limit")
    budget = HttpBudget(http_budget)
    directory, source_db, persona_file = Path(output_dir), Path(source_db), Path(persona_file)
    directory.mkdir(mode=0o700, parents=False, exist_ok=False)
    os.chmod(directory, 0o700)
    report = {"format_version": 1, "state": "planning", "synthetic_only": False,
        "single_session_only": True, "not_for_tuning": True, "prompt_version": PROMPT_VERSION,
        "scope": "long_term_rebuild_prefix_only", "not_tested": ["recall", "embedding", "topic_switch", "short_term", "generalization"],
        "tested_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds"),
        "max_windows": max_windows, "http_budget": http_budget, "sdk_retries": 0, "provider_request_attempts": 1, "job_max_attempts": 1,
        "semantic_status": "not_evaluated", "mechanical_validation": "receipt_source_consumption_and_profiles",
        "manual_review_required": ["nodes", "relationships", "coverage", "profile_semantics"], "windows": []}
    private_json(directory / "manifest.json", report)
    sources = memory = engine = runtime = provider = observer = None
    try:
        persona_bytes = persona_file.read_bytes()
        persona = persona_bytes.decode("utf-8")
        if not persona.strip():
            raise ValueError("empty_persona")
        source_hash = sha256_file(source_db)
        report.update(source_sha256=source_hash, persona_sha256=hashlib.sha256(persona_bytes).hexdigest())
        conversion_manifest = source_db.parent / "manifest.json"
        manifest = None
        if conversion_manifest.exists():
            manifest = json.loads(conversion_manifest.read_text(encoding="utf-8"))
            if (manifest["persona"]["sha256"] != report["persona_sha256"]
                    or manifest["output"]["sha256"] != source_hash):
                raise ValueError("conversion_manifest_mismatch")
            report["conversion_manifest_sha256"] = sha256_file(conversion_manifest)
        backup_source(source_db, directory / "source.db")
        if sha256_file(source_db) != source_hash:
            raise ValueError("source_changed_during_backup")
        if manifest is not None:
            verify_conversion_snapshot(directory / "source.db", manifest)
        sources = SourceStore(directory / "source.db")
        descriptor = os.open(directory / "memory.db", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.fchmod(descriptor, 0o600)
        os.close(descriptor)
        memory = MemoryStore(directory / "memory.db", source_exists=sources.has_source)
        await sources.open()
        await memory.open()
        config = NeoMemoryConfig(llm_provider_id="recorded-replay-llm", automatic_extraction_enabled=False,
                                 job_concurrency=1, job_max_attempts=1)
        async def persona_loader(_persona_id):
            return persona
        engine = MaintenanceEngine(sources, memory, SimpleNamespace(recall=None), config, persona_loader)
        space, highwater, scope = await validate_scope(engine)
        report.update(scope)
        report["window_policy"] = asdict(engine.policy(space))
        report["extraction_timeout_seconds"] = config.extraction_timeout_seconds
        plan = await plan_windows(engine, space, highwater)
        selected = plan["windows"][:max_windows]
        private_json(directory / "plan.json", {**plan, "scope": space, "highwater": highwater,
                                                "selection": "first_chronological_windows", "selected_count": len(selected)})
        report.update(planned_windows=len(plan["windows"]), selected_windows=len(selected),
                      selected_sources=sum(window["source_count"] for window in selected),
                      unplanned_sources=plan["unplanned_source_count"], planning_status=plan["status"],
                      plan_sha256=sha256_file(directory / "plan.json"))
        if run and not selected:
            raise ValueError("no_executable_windows")
        if selected:
            restricted = await plan_windows(engine, space, selected[-1]["highwater"])
            if [window["source_ids"] for window in restricted["windows"]] != [window["source_ids"] for window in selected]:
                raise ValueError("truncated_scope_changes_windows")
        if not run:
            report.update(state="planned", structure_ok=plan["status"] != "oversized")
        else:
            if len(selected) > http_budget:
                raise ValueError("selected_windows_exceed_http_budget")
            report["state"] = "running"
            private_json(directory / "manifest.json", report)
            runtime, provider, provider_report = await build_runtime(config, sources, memory, directory,
                                                                    credentials_path, budget)
            report.update(provider_report)
            engine.runtime = runtime
            runtime.plugin.maintenance = engine
            observer = WindowObserver(engine, directory, selected)
            generation = await memory.get_active_generation(space["space_id"])
            payload = {"request": {"kind": "rebuild", "space_id": space["space_id"],
                       "operation_digest": None, "reason": "", "failed_job_id": None},
                       "space_id": space["space_id"], "expected_generation_id": generation["generation_id"],
                       "highwater": selected[-1]["highwater"], "total": report["selected_sources"]}
            job = await engine.jobs.submit("rebuild", space["space_id"], payload, "recorded-replay-prefix", max_attempts=1)
            await engine.runner.start()
            job, transitions = await wait_terminal(engine, kind="rebuild", job_id=job["job_id"],
                timeout=len(selected) * config.extraction_timeout_seconds + 60, stop_on_retry=True)
            report["job_transitions"] = transitions
            report["job"] = {key: job[key] for key in ("state", "step", "progress_done", "progress_total", "attempt", "max_attempts", "error_type", "error_code")}
            report["windows"] = observer.windows
            generation_id = job["result"]["generation_id"] if job["result"] else job["checkpoint"].get("generation_id")
            if generation_id:
                final = await snapshot(memory, space, generation_id)
                private_json(directory / "final.json", {"generation_id": generation_id, **final})
                report["final_counts"] = {key: len(value) for key, value in final.items()}
            active = await memory.get_active_generation(space["space_id"])
            report["generation_activated"] = bool(generation_id and active["generation_id"] == generation_id)
            report["structure_ok"] = (job["state"] == "succeeded" and report["generation_activated"]
                                      and len(observer.windows) == len(selected)
                                      and all(window["structure_ok"] for window in observer.windows))
            report.update(state="finished" if report["structure_ok"] else "failed",
                          semantic_status="needs_review" if report["structure_ok"] else "not_evaluated")
    except Exception as exc:
        report.update(state="failed", structure_ok=False, error=safe_error(exc))
    finally:
        if observer is not None:
            report["windows"] = observer.windows
        report["consumed_sources"] = sum(window["consumed_source_count"] for window in report["windows"])
        report["untested_sources"] = report.get("source_count", 0) - report["consumed_sources"]
        report["http_requests"] = {"llm": budget.count, "embedding": 0}
        closers = [obj.stop if obj is engine else obj.terminate if obj is provider else obj.close
                   for obj in (engine, runtime, memory, sources, provider) if obj is not None]
        for close in closers:
            try:
                await close()
            except Exception as exc:
                report.setdefault("cleanup_errors", []).append(safe_error(exc))
        if report.get("cleanup_errors"):
            report.update(state="failed", structure_ok=False)
        report["artifacts"] = {name: sha256_file(directory / name) for name in ("source.db", "memory.db") if (directory / name).exists()}
        private_json(directory / "report.json", report)
        private_json(directory / "manifest.json", report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-db", type=Path, required=True)
    parser.add_argument("--persona-file", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--credentials", type=Path, default=Path(__file__).resolve().parents[1] / "docs/api_key.md")
    parser.add_argument("--max-windows", type=int, choices=range(1, 9), default=8)
    parser.add_argument("--http-budget", type=int, choices=range(1, 9), default=8)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan-only", action="store_true")
    mode.add_argument("--run", action="store_true")
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    old_umask = os.umask(0o077)
    try:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            from loguru import logger
            logger.remove()
            os.environ["ASTRBOT_ROOT"] = str(args.output_dir.resolve() / "host-runtime")
            report = asyncio.run(replay(args.source_db, args.persona_file, args.output_dir,
                run=args.run, max_windows=args.max_windows, http_budget=args.http_budget,
                credentials_path=args.credentials))
    except Exception as exc:
        report = {"state": "failed", "error": safe_error(exc)}
    finally:
        os.umask(old_umask)
    # All public fields are aggregates, hashes, fixed statuses, or safe errors.
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if report.get("state") == "planned" and report.get("structure_ok"):
        return 0
    return 2 if report.get("state") == "finished" else 1


if __name__ == "__main__":
    raise SystemExit(main())
