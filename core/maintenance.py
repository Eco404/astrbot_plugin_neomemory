"""Durable maintenance orchestration over immutable source windows and receipts."""

from __future__ import annotations

import asyncio
import copy
import json
import time
import uuid
from dataclasses import asdict
from functools import partial

from pydantic import ValidationError

try:
    from ..adapters.providers import VisionUnsupportedError
    from ..storage.console_store import validate_note_operation
    from ..storage.governance_store import GovernanceService
    from ..storage.job_store import JobConflict, JobStore, inspection_checkpoint
    from ..storage.pipeline_store import PipelineStore
    from ..storage.source_scan import SourceScanner
except ImportError:
    from adapters.providers import VisionUnsupportedError
    from storage.console_store import validate_note_operation
    from storage.governance_store import GovernanceService
    from storage.job_store import JobConflict, JobStore, inspection_checkpoint
    from storage.pipeline_store import PipelineStore
    from storage.source_scan import SourceScanner

from .extraction_apply import ExtractionService, digest
from .extraction_context import (
    CONTEXT_VERSION,
    IndexCacheUnavailable,
    node_vectors,
    select_context_ids,
)
from .extraction_errors import (
    DuplicateJSONKey,
    ExtractionCapacityError,
    InvalidExtractionCandidate,
)
from .extraction_references import VERSION as REFERENCE_VERSION
from .extraction_references import ExtractionReferences, ReferenceValidationError
from .extraction_regeneration import VERSION as REGENERATION_VERSION
from .extraction_regeneration import (
    RegenerationContextError,
    automatic_correction_allowed,
)
from .extraction_regeneration import build_messages as regeneration_messages
from .extraction_schema import extraction_model_json_schema, parse_extraction_output
from .candidate_feedback import CandidateValidationError
from .extraction_window import WindowMessage, WindowPolicy, select_window
from .forwarded_content import window_text
from .parallel_rebuild import rebuild_sections
from .context_rebuild import MODELS as CONTEXT_REBUILD_MODELS, extraction_envelope, scoped_messages
from .model_routing import ModelRoutingMixin, ModelSelectionRequired, fixed_route, validate_selection
from .jobs import JobRunner, ModelNetworkError, RetryableJobError, ReviewRequired
from .provider_errors import is_transient_provider_error
from .memory_spaces import RetiredIdentity
from .persona_policy import is_supported_persona
from .prompts import schema_for_prompt_version
from .short_term_prompts import parse_output as parse_short_term_output
from .topic_maintenance import TopicMaintenanceMixin
from .topic_windows import TopicWindowJudge
from .vision_request import VisionOutputError, split_vision_output, vision_messages, vision_schema
from .window_images import collects_evidence, image_counts, load_slots, prepare_slots, text_budget


def _schema_diagnostics(error: ValidationError):
    return [{"path": list(item["loc"]), "type": item["type"]}
        for item in error.errors(include_url=False, include_context=False, include_input=False)[:20]]


class MaintenanceEngine(ModelRoutingMixin, TopicMaintenanceMixin):
    """Model calls are outside transactions; every derived commit is lease fenced."""

    def __init__(self, sources, memory, runtime, config, persona_loader):
        self.sources, self.memory, self.runtime, self.config = sources, memory, runtime, config
        self.persona_loader = persona_loader
        self.image_cache = None
        self.short_term = None
        self.model_tests = None
        self.identity_management = None
        self.jobs = JobStore(memory.database)
        self.pipeline = PipelineStore(memory)
        self.scanner = SourceScanner(sources)
        self.extraction = ExtractionService(sources, memory)
        self.governance = GovernanceService(memory)
        handlers = {**dict.fromkeys(CONTEXT_REBUILD_MODELS, self._rebuild), "extract": self._extract, "rebuild": self._rebuild, "reindex": self._reindex,
                    "governance": self._govern, "reject": self._reject,
                    "topic_consolidate": self._consolidate_topics, "topic_rebuild": self._replace_topics}
        self.runner = JobRunner(self.jobs, {kind: partial(self._supported_handler, handler)
                                            for kind, handler in handlers.items()}, concurrency=config.job_concurrency, lease_seconds=config.job_lease_seconds,
            poll_seconds=config.job_poll_seconds, reprepare=self._auto_reprepare)
        self._scanner_task = None
        self._scan_wake = asyncio.Event()
        self.closing = False
        self._diagnostics = {"last_error_type": None}
        self._space_after = ""
        self._read_hints: dict[str, int] = {}
        self._pending_reads: dict[str, dict] = {}

    @property
    def last_error_type(self):
        return self._diagnostics["last_error_type"]

    @last_error_type.setter
    def last_error_type(self, value):
        self._diagnostics["last_error_type"] = value

    async def _supported_handler(self, handler, job):
        worker = self.snapshot()
        space = await worker.memory.get_space(job["scope_key"])
        if space and not is_supported_persona(space["persona_id"]):
            return {"skipped": True, "reason": "unsupported_persona"}
        return await handler.__func__(worker, job)

    def snapshot(self):
        """Share stores and scheduling state, retain this operation's model bindings."""
        worker = copy.copy(self)
        worker.runtime = copy.copy(self.runtime)
        return worker

    async def automatic_status(self, space_id):
        space = await self.memory.get_space(space_id)
        if not space:
            raise KeyError("unknown memory space")
        if not is_supported_persona(space["persona_id"]):
            return {"state": "skipped", "reason": "unsupported_persona", "blocking_jobs": []}
        if not self.config.automatic_extraction_enabled:
            return {"state": "disabled", "reason": "automatic_extraction_disabled", "blocking_jobs": []}
        generation = await self.memory.get_active_generation(space_id)
        if generation is None:
            return {"state": "enabled", "reason": None, "blocking_jobs": []}
        rows = await self.memory.database.fetchall(
            "SELECT job_id,kind,error_type,error_code,created_at FROM memory_jobs WHERE scope_key=? "
            "AND state='failed' AND kind IN ('extract','rebuild','reject') "
            "AND json_extract(payload_json,'$.expected_generation_id')=? ORDER BY created_at,job_id LIMIT 50",
            (space_id, generation["generation_id"]))
        return {"state": "paused" if rows else "enabled", "reason": "failed_window" if rows else None,
                "blocking_jobs": [dict(row) for row in rows],
                "topic_blocking_jobs": await self._topic_blockers(space_id, generation["generation_id"])}

    async def start(self):
        await self.extraction.flush_image_evidence()
        await self.runner.start()
        self._scanner_task = asyncio.create_task(self._scan_loop(), name="neomemory-source-scanner")

    async def stop(self):
        self.closing = True
        if self._scanner_task is not None:
            self._scanner_task.cancel()
            await asyncio.gather(self._scanner_task, return_exceptions=True)
            self._scanner_task = None
        await self.runner.stop()

    def policy(self, space) -> WindowPolicy:
        cfg = self.config
        prefix = "group" if space["space_type"] == "group" else "private"
        return WindowPolicy(
            min_messages=getattr(cfg, f"{prefix}_min_messages"),
            topic_messages=getattr(cfg, f"{prefix}_topic_messages"),
            max_messages=getattr(cfg, f"{prefix}_max_messages"),
            idle_seconds=getattr(cfg, f"{prefix}_idle_seconds"),
            max_chars=cfg.extraction_max_chars, max_tokens=cfg.extraction_max_tokens)

    async def _scan_loop(self):
        while not self.closing:
            try:
                if retry_indexes := getattr(self.runtime, "retry_pending_indexes", None):
                    retry_indexes()
                await self.scan_once()
            except Exception as exc:
                self.last_error_type = type(exc).__name__
            try:
                await asyncio.wait_for(self._scan_wake.wait(), self.config.extraction_scan_seconds)
            except TimeoutError:
                pass
            self._scan_wake.clear()

    async def scan_once(self):
        worker = self.snapshot()
        try:
            await worker._scan_once()
        finally:
            self._space_after = worker._space_after

    async def _scan_once(self):
        # Discovery runs even with automatic extraction off, so maintenance can
        # address existing source spaces without creating a model request.
        cursor = await self.pipeline.get_state("source_discovery_cursor", 0)
        page = await self.scanner.discover_spaces(after=cursor, limit=200)
        for spec in page.spaces:
            if is_supported_persona(spec.persona_id):
                try:
                    await self.memory.get_or_create_space(spec)
                except RetiredIdentity:
                    continue
        if page.next_cursor != cursor:
            await self.pipeline.set_state("source_discovery_cursor", page.next_cursor)
        if not self.config.automatic_extraction_enabled:
            return
        rows = await self.memory.database.fetchall(
            "SELECT * FROM memory_spaces WHERE space_id>? ORDER BY space_id LIMIT 8", (self._space_after,))
        if not rows:
            self._space_after = ""
            return
        for row in rows:
            space = dict(row)
            self._space_after = space["space_id"]
            if not is_supported_persona(space["persona_id"]):
                continue
            try:
                generation = await self.memory.get_active_generation(space["space_id"])
                blocked = await self.memory.database.fetchone(
                    "SELECT 1 FROM memory_jobs WHERE scope_key=? AND state='failed' "
                    "AND kind IN ('extract','rebuild','reject') "
                    "AND json_extract(payload_json,'$.expected_generation_id')=? LIMIT 1",
                    (space["space_id"], generation["generation_id"]))
                if blocked:
                    continue
                if not (await self.jobs.availability(space["space_id"]))["available"]:
                    continue
                generation = await self.memory.get_active_generation(space["space_id"])
                if await self._maybe_enqueue_topics(space, generation):
                    continue
                pending, decision = await self.live_plan(space, generation["generation_id"])
                if decision.status == "waiting":
                    continue
                key = "auto:" + digest([space["space_id"], generation["generation_id"],
                                        decision.source_ids or [decision.blocked_source_id]])
                key = await self.jobs.next_window_key(key)
                if key is not None:
                    await self._submit_extract(space, generation, pending, decision, key, request={"automatic": True})
            except JobConflict:
                continue
            except Exception as exc:
                self.last_error_type = type(exc).__name__

    async def pending_source_count(self, space):
        if not is_supported_persona(space["persona_id"]):
            return 0
        return await self.scanner.pending_count(space, self.memory.path)

    async def _pending(self, space, generation_id):
        committed = await self.pipeline.get_cursor(generation_id)
        state = self._pending_reads.get(generation_id)
        if state is None or state["committed"] != committed:
            state = {"committed": committed, "after": max(committed, self._read_hints.get(generation_id, 0)), "refs": []}
        # Keep only source references across bounded scan ticks. A partly
        # consumed legacy page must not strand later pending messages forever.
        complete = False
        for _ in range(5):
            page = await self.scanner.page(space, after=state["after"], limit=200)
            consumed = await self.pipeline.consumed(generation_id, [row["source_id"] for row in page.records])
            state["refs"].extend((row["source_id"], row["scan_cursor"]) for row in page.records
                                 if row["source_id"] not in consumed)
            state["after"] = page.next_cursor
            if not state["refs"]:
                self._read_hints[generation_id] = page.next_cursor
            if len(state["refs"]) >= 200 or not page.has_more:
                complete = True
                break
        self._pending_reads[generation_id] = state
        for cache in (self._pending_reads, self._read_hints):
            while len(cache) > 256:
                cache.pop(next(iter(cache)))
        if not complete:
            return []
        self._pending_reads.pop(generation_id, None)
        refs = state["refs"][:200]
        records = await self.sources.get_records(tuple(ref[0] for ref in refs)) if refs else []
        return [{**row, "scan_cursor": ref[1]} for row, ref in zip(records, refs, strict=True)]

    async def _decision(self, space, records, *, manual=False, replay=False):
        policy = self.policy(space)
        now = time.time()
        messages = [WindowMessage(row["source_id"], window_text(row))
                    for row in records]
        # Replay respects a recorded idle gap before the next source instead of
        # treating a large imported history as one simultaneous conversation.
        if replay:
            for index in range(1, len(records)):
                gap = max(0, records[index]["observed_at"] - records[index - 1]["observed_at"])
                decision = select_window(messages[:index],
                    idle_seconds=gap, policy=policy)
                if decision.status != "waiting":
                    return decision
        idle = max(0, now - max((r["observed_at"] for r in records), default=now))
        trusted, boundary = False, None
        if (policy.topic_messages <= len(records) < policy.max_messages and self.runtime.recall is not None):
            judged = await TopicWindowJudge(self.runtime.recall.embeddings).judge(space["space_id"], records)
            trusted = judged.trusted_switch
            boundary = judged.boundary_index
        return select_window(messages,
            idle_seconds=policy.idle_seconds if manual else idle, trusted_topic_switch=trusted,
            topic_boundary_index=boundary, policy=policy)

    async def _existing_request(self, key, request):
        row = await self.memory.database.fetchone("SELECT job_id FROM memory_jobs WHERE idempotency_key=?", (key,))
        if row is None:
            return None
        job = await self.jobs.get(row["job_id"])
        if job["payload"].get("request") != request:
            raise JobConflict("idempotency key already identifies a different request")
        return job

    async def enqueue(self, kind, space_id, idempotency_key, *, operation=None, reason="",
                      failed_job_id=None, include_topics=False, automatic=False, model_selection=None):
        return await self.snapshot()._enqueue(kind, space_id, idempotency_key,
            operation=operation, reason=reason, failed_job_id=failed_job_id,
            include_topics=include_topics, automatic=automatic, model_selection=model_selection)

    async def _enqueue(self, kind, space_id, idempotency_key, *, operation=None, reason="",
                       failed_job_id=None, include_topics=False, automatic=False, model_selection=None, replaces=None):
        if self.closing:
            raise ValueError("maintenance is stopping")
        if kind not in {"extract", "rebuild", "reindex", "governance", "reject", "topic_consolidate", "topic_rebuild", *CONTEXT_REBUILD_MODELS}:
            raise ValueError("unsupported maintenance task")
        request = {"kind": kind, "space_id": space_id,
                   "operation_digest": digest(operation) if operation is not None else None,
                   "reason": reason, "failed_job_id": failed_job_id}
        if automatic:
            request["automatic"] = True
        if include_topics:
            if kind not in CONTEXT_REBUILD_MODELS:
                raise ValueError("parallel topics require a context rebuild")
            request["include_topics"] = True
        if model_selection:
            validate_selection(model_selection)
            if kind not in {"extract", "rebuild", "topic_consolidate", "topic_rebuild", *CONTEXT_REBUILD_MODELS}:
                raise ValueError("this task does not use an extraction model")
            request["model_selection"] = model_selection
        existing = await self._existing_request(idempotency_key, request)
        if existing:
            return existing
        space = await self.memory.get_space(space_id)
        if not space or space["space_id"] != space_id:
            raise ValueError("unknown memory space ID")
        if not is_supported_persona(space["persona_id"]):
            raise ValueError("default persona is unsupported")
        generation = await self.memory.get_active_generation(space_id)
        payload = {"request": request, "space_id": space_id,
                   "expected_generation_id": generation["generation_id"]}
        if include_topics:
            payload["include_topics"] = True
        if kind == "extract":
            records, decision = await self.live_plan(space, generation["generation_id"], manual=True)
            if decision.status == "waiting":
                if replaces:
                    return None
                raise ValueError("pending messages are below the extraction minimum")
            return await self._submit_extract(space, generation, records, decision, idempotency_key, request=request, replaces=replaces)
        if kind == "rebuild" or kind in CONTEXT_REBUILD_MODELS:
            payload["highwater"] = await self.scanner.highwater(space)
            payload["total"] = await self.scanner.count(space, highwater=payload["highwater"])
            if await self.scanner.has_deliveries(space, payload["highwater"]):
                payload["replay_deliveries"] = True
        if kind in {"rebuild", "topic_consolidate", "topic_rebuild", *CONTEXT_REBUILD_MODELS}:
            policy = await self.memory.get_space_policy(space_id)
            payload["model_policy"] = {key: policy[key] for key in ("model_mode", "provider_id", "topic_provider_id")}
            payload["model_policy"]["default_provider_id"] = self.config.llm_provider_id
            preview = await self.model_preview(kind, space, policy=policy, include_topics=include_topics,
                                               selection=model_selection, highwater=payload.get("highwater"))
            if any(row["mode"] == "missing" for row in preview.values()):
                raise ModelSelectionRequired("model_required: select a model for the unresolved pipeline")
        if kind == "topic_consolidate":
            payload["before"] = time.time()
            payload["review_before"] = payload["before"]
        if kind in {"relationship_rebuild", "context_rebuild"} and space["space_type"] != "private":
            raise ValueError("relationship rebuild requires a private space")
        if kind in CONTEXT_REBUILD_MODELS:
            payload["context_basis"] = await self.pipeline.context_basis(kind, generation["generation_id"])
        if kind == "governance":
            if not isinstance(operation, dict):
                raise ValueError("governance operation is required")
            payload["operation"] = operation
            payload["reason"] = reason
        elif kind == "reject":
            failed = await self.jobs.get(failed_job_id)
            if (not failed or failed["state"] != "failed" or failed["scope_key"] != space_id
                    or failed["kind"] != "extract"
                    or failed["payload"].get("expected_generation_id") != generation["generation_id"]):
                raise ValueError("only a failed extraction in the current generation can be rejected")
            window = failed["checkpoint"].get("window", failed["payload"])
            payload["failed_job_id"] = failed_job_id
            payload["source_ids"] = window["source_ids"]
            payload["source_cursor"] = window["source_cursor"]
            batch_id = "batch_" + digest([space_id, generation["generation_id"], tuple(payload["source_ids"])])
            receipt = await self.memory.get_extraction_batch(batch_id)
            if receipt and receipt["model_id"] != "manual-rejection":
                raise ValueError("window already committed; resume the original task")
        return await self.jobs.submit(kind, space_id, payload, idempotency_key,
                                      max_attempts=self.config.job_max_attempts, hide_on_success=automatic, replaces=replaces)

    async def _submit_extract(self, space, generation, records, decision, key, *, request, replaces=None):
        ids = decision.source_ids or (decision.blocked_source_id,)
        by_id = {row["source_id"]: row for row in records}
        payload = {"request": request, "space_id": space["space_id"],
            "expected_generation_id": generation["generation_id"], "source_ids": list(ids),
            "source_cursor": max(by_id[source_id]["scan_cursor"] for source_id in ids),
            "blocked": decision.status == "oversized"}
        policy = await self.memory.get_space_policy(space["space_id"])
        route = self.resolve_window_route(policy, [by_id[i] for i in ids], request.get("model_selection", {}))
        if not route and not request.get("automatic"):
            raise ModelSelectionRequired("model_required: select an extraction model")
        payload["model_route"] = route
        return await self.jobs.submit("extract", space["space_id"], payload, key,
                                      max_attempts=self.config.job_max_attempts,
                                      hide_on_success=bool(request.get("automatic")), replaces=replaces)

    async def delete(self, job_id):
        """Remove a non-running task after its local worker has relinquished resources."""
        job = await self.jobs.get(job_id)
        if job is None:
            raise KeyError(job_id)
        if job["state"] == "running":
            raise JobConflict("running tasks must be stopped before deletion")
        worker = self.runner._workers.get(job_id)
        if job["state"] == "cancelled" and worker is not None and not worker.done():
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        cache = self.runtime.cache
        result = await self.jobs.delete(job_id, invalidate_nodes=cache.invalidate_nodes if cache else None)
        if self.identity_management is not None:
            await self.identity_management.cleanup_receipts()
        if generation_id := result["removed_generation_id"]:
            self._pending_reads.pop(generation_id, None)
            self._read_hints.pop(generation_id, None)
        return result

    async def clear_completed(self, *, scope_key=None):
        """Remove successful task history without affecting unfinished work."""
        cache = self.runtime.cache
        result = await self.jobs.clear_completed(
            scope_key=scope_key, invalidate_nodes=cache.invalidate_nodes if cache else None)
        if self.identity_management is not None:
            await self.identity_management.cleanup_receipts()
        for generation_id in result.pop("removed_generation_ids"):
            self._pending_reads.pop(generation_id, None)
            self._read_hints.pop(generation_id, None)
        return result

    async def _auto_reprepare(self, job):
        if job["kind"] == "short_term":
            return await self.short_term.auto_reprepare(job)
        key = "auto-restart:" + job["job_id"]
        if job["kind"] == "topic_consolidate" and job["idempotency_key"].startswith("topics:auto:"):
            topic_key = job["idempotency_key"].split(":", 3)[:3]
            key = ":".join([*topic_key, job["job_id"]])
        return await self.snapshot()._enqueue(job["kind"], job["scope_key"], key,
            automatic=True, replaces=job)

    async def retry(self, job_id, *, fresh=False, regenerate_window=False, model_selection=None):
        if model_selection and not fresh:
            raise ValueError("model overrides require a fresh preparation")
        if type(fresh) is not bool or type(regenerate_window) is not bool or (fresh and regenerate_window):
            raise ValueError("retry modes must be mutually exclusive booleans")
        if regenerate_window:
            return await self.jobs.regenerate_window(job_id)
        job = await self.jobs.get(job_id)
        if not job or job["state"] not in {"failed", "cancelled"}:
            raise ValueError("task must be failed or cancelled before retry")
        if job["kind"] == "short_term" and self.short_term is not None:
            return await self.short_term.retry(job_id, fresh=fresh, **({"model_selection": model_selection} if model_selection else {}))
        if job["kind"] == "model_test" and self.model_tests is not None:
            return await self.model_tests.retry(job_id, fresh=fresh)
        if job["kind"] == "note" and fresh:
            raise ValueError("refresh and edit the note again; do not discard its version check")
        if not fresh:
            return await self.jobs.retry(job_id)
        kind, space = job["kind"], job["scope_key"]
        request = job["payload"].get("request", {})
        # A new task starts with current inputs and no old model output. Keep the
        # old error record until successful submission, then resolve its blocker.
        created = await self.enqueue(kind, space, "restart:" + uuid.uuid4().hex,
            operation=job["payload"].get("operation"), reason=request.get("reason", ""),
            failed_job_id=job["payload"].get("failed_job_id"), include_topics=bool(job["payload"].get("include_topics")),
            model_selection=model_selection)
        await self.jobs.cancel(job_id)
        return created

    async def describe(self, job_id):
        job = await self.jobs.get(job_id)
        if job is None:
            raise KeyError("unknown task")
        fields = ("job_id", "kind", "scope_key", "state", "step", "progress_done", "progress_total",
                  "attempt", "max_attempts", "automatic_origin", "error_type", "error_code", "created_at", "updated_at",
                  "available_at", "result")
        result = {key: job[key] for key in fields}
        if lanes := job["checkpoint"].get("section_progress"):
            result["section_progress"] = lanes
        result["hidden_from_task_list"] = bool(job["hide_on_success"] and job["state"] == "succeeded")
        checkpoint = inspection_checkpoint(job)
        window = checkpoint.get("window", {})
        result["regenerate_window_available"] = (
            job["state"] in {"failed", "cancelled"} and job["kind"] in {"extract", "rebuild", *CONTEXT_REBUILD_MODELS}
            and "raw" in window)
        history = await self.jobs.list_candidates(job_id, limit=11)
        result["candidate_history"] = history[:10]
        result["candidate_history_has_more"] = len(history) > 10
        result["validation_errors"] = window.get("validation_errors", [])
        result["automatic_corrections"] = window.get("correction_attempt", 0)
        result["automatic_correction_limit"] = window.get("correction_limit", 1) if window else None
        result["response_diagnostics"] = window.get("response_diagnostics", {})
        result["source_ids"] = window.get("source_ids", job["payload"].get("source_ids", []))
        result["model_route"] = (window.get("model_route") or checkpoint.get("topic_window", {}).get("model_route")
                                 or job["payload"].get("model_route"))
        result["candidate"] = None
        if checkpoint.get("window_index"):
            result["committed_window"] = dict(checkpoint["window_index"])
        if job["kind"] == "note":
            try:
                result["candidate"] = validate_note_operation(job["payload"].get("operation"))
            except ValueError:
                result["candidate_format_invalid"] = True
        elif window.get("raw"):
            try:
                result["candidate"] = parse_extraction_output(
                    window.get("canonical_raw", window["raw"])).model_dump(mode="json")
            except ValueError:
                result["candidate_format_invalid"] = True
        elif job["kind"] == "short_term" and checkpoint.get("raw"):
            previous = await self.memory.get_short_term(
                space_id=job["scope_key"], conversation_id=job["payload"]["conversation_key"],
                generation_id=job["payload"]["generation_id"])
            try:
                result["candidate"] = parse_short_term_output(checkpoint["raw"],
                    allowed_user_ids=checkpoint.get("user_ids"),
                    prompt_version=checkpoint["prompt_version"],
                    source_ids=result["source_ids"], summary_chars=self.config.short_term_summary_chars,
                    previous_summary=previous["summary_text"] if previous else "")
            except ValueError:
                result["candidate_format_invalid"] = True
        result["restart_required"] = bool(job["payload"].get("topic_inputs_replaced")) or job["error_code"] in {
            "context_changed", "configuration_changed", "model_required",
            "configuration_or_context_changed",
            "generation_changed", "summary_changed", "summary_context_changed", "model_test_configuration_changed"}
        if job["kind"] == "short_term":
            summary = checkpoint
            result["validation_errors"] = summary.get("errors", [])
            result["automatic_corrections"] = summary.get("corrections", 0)
            result["automatic_correction_limit"] = summary.get("correction_limit", 1) if summary else None
            result["response_diagnostics"] = summary.get("response_diagnostics", {})
            receipt = await self.memory.database.fetchone("SELECT 1 FROM short_term_receipts WHERE job_id=?", (job_id,))
            result["regenerate_window_available"] = job["state"] in {"failed", "cancelled"} and "raw" in summary and not receipt
        topic_window = checkpoint.get("topic_window", {}) or checkpoint.get("topic_synthesis", {}).get("topic_window", {})
        topic_budget = topic_window.get("budget") or checkpoint.get("topic_budget") or checkpoint.get("topic_synthesis", {}).get("topic_budget")
        if topic_budget:
            result["topic_budget"] = topic_budget
            if job["error_code"] == "prompt_budget_exceeded":
                result["restart_required"] = True
        if topic_window:
            from .topic_maintenance import TopicStore
            from .topic_schema import parse_output_for_context as parse_topic_output
            result["regenerate_window_available"] = (
                job["state"] in {"failed", "cancelled"} and "raw" in topic_window
                and await TopicStore(self.memory).receipt(topic_window["receipt_key"]) is None)
            result["validation_errors"] = topic_window.get("errors", [])
            result["automatic_corrections"] = topic_window["corrections"]
            result["automatic_correction_limit"] = topic_window["correction_limit"]
            result["response_diagnostics"] = topic_window.get("response_diagnostics", {})
            if "raw" in topic_window:
                try:
                    prepared = topic_window["prepared"]
                    result["candidate"] = parse_topic_output(
                        topic_window["raw"], prepared,
                        window_user_ids=prepared["allowed_user_ids"])
                except ValueError:
                    result["candidate_format_invalid"] = True
        return result

    async def _save(self, job, checkpoint, step, done=0, total=None):
        await self.jobs.save_checkpoint(job["job_id"], job["lease_token"], checkpoint=checkpoint,
                                       step=step, progress_done=done, progress_total=total)

    async def _persona_text(self, space):
        persona = await self.persona_loader(space["persona_id"])
        if not isinstance(persona, str):
            raise ReviewRequired("persona_unavailable")
        return persona

    async def _execution(self, space, route=None):
        persona = await self._persona_text(space)
        provider = self.config.llm_provider_id if route is None else route.get("provider_id")
        model = self.runtime.adapter.llm_fingerprint(provider, self.config.extraction_model_revision) if provider else None
        if route and route.get("model_override"):
            model = digest([model, route["model_override"]])
        policy = self.policy(space)
        signature = digest({**({"output_format": self.config.extraction_output_format} if self.config.extraction_output_format != "text" else {}), "persona": persona, "model": model, "prompt": self.extraction.prompt_version,
            "references": REFERENCE_VERSION,
            "context_policy": CONTEXT_VERSION,
            "context_embedding": self.runtime.recall.embeddings.fingerprint if self.runtime.recall is not None else None,
            "policy": asdict(policy), "context_nodes": self.config.extraction_context_nodes,
            "prompt_budget": self.config.extraction_max_prompt_bytes})
        return persona, signature

    @staticmethod
    def _embedding_failure(exc, *, context=False):
        if isinstance(exc, IndexCacheUnavailable):
            return RetryableJobError("index_cache_unavailable")
        prefix = "context_embedding" if context else "window_index"
        if is_transient_provider_error(exc):
            return ModelNetworkError(prefix + "_temporary_failure")
        return ReviewRequired(prefix + "_unavailable")

    async def _context_ids(self, space, generation_id, sources, *, timeout_seconds=None, progress=None):
        embeddings = self.runtime.recall.embeddings if self.runtime.recall is not None else None
        try:
            return await select_context_ids(
                self.memory, embeddings, space_id=space["space_id"], generation_id=generation_id,
                sources=sources, limit=self.config.extraction_context_nodes,
                timeout_seconds=self.config.extraction_timeout_seconds if timeout_seconds is None else timeout_seconds,
                progress=progress,
            )
        except Exception as exc:
            raise self._embedding_failure(exc, context=True) from None

    async def _window(self, job, checkpoint, space, generation_id, source_ids, source_cursor, *, building=False):
        # 候选准备与生成共用窗口期限，后台补向量不能在模型调用前无限等待。
        deadline = time.monotonic() + self.config.extraction_timeout_seconds
        try:
            async with asyncio.timeout(self.config.extraction_timeout_seconds):
                while True:
                    try:
                        return await self._window_in_budget(
                            job, checkpoint, space, generation_id, source_ids, source_cursor,
                            building=building, deadline=deadline,
                        )
                    except ReviewRequired as exc:
                        window = checkpoint.get("window", {})
                        if (not self.config.extraction_auto_regeneration
                                or not automatic_correction_allowed(exc.code, window)
                                or window.get("correction_attempt", 0) >= window.get("correction_limit", 1)
                                or "raw" not in window):
                            raise
                        candidate_id = await self.jobs.archive_candidate(
                            job["job_id"], job["lease_token"], window, exc.code)
                        # Persist the correction budget before another model call; resumes
                        # reuse it, and manual regeneration never replenishes it.
                        window["correction_attempt"] = window.get("correction_attempt", 0) + 1
                        for key in ("raw", "canonical_raw", "validation_errors", "model_request_digest", "response_diagnostics"):
                            window.pop(key, None)
                        window["regeneration"] = {"version": REGENERATION_VERSION, "candidate_id": candidate_id}
                        await self._save(job, checkpoint, "correcting", checkpoint.get("done", 0), checkpoint.get("total"))
        except TimeoutError:
            raise RetryableJobError("extraction_timeout") from None

    async def _window_in_budget(self, job, checkpoint, space, generation_id, source_ids, source_cursor, *, building, deadline):
        # A commit can outlive cancellation of its awaiting coroutine. Receipt
        # lookup must precede preparing a new round or reusing stale snapshots.
        batch_id = "batch_" + digest([space["space_id"], generation_id, tuple(source_ids)])
        receipt = await self.memory.get_extraction_batch(batch_id)
        if receipt:
            await self.extraction.flush_image_evidence(batch_id)
            await self._discard_images(job, space, source_ids)
            return receipt
        window = checkpoint.get("window")
        route = (window or {}).get("model_route") or job["payload"].get("model_route")
        if route is None:
            policy = job["payload"].get("model_policy")
            if policy is None:
                policy = await self.memory.get_space_policy(space["space_id"])
            entries = await self.sources.get_records(tuple(source_ids))
            route = self.resolve_window_route(policy, entries, job["payload"].get("request", {}).get("model_selection", {}),
                default=policy.get("default_provider_id"))
        if not self.provider_available(route.get("provider_id")):
            raise ReviewRequired("model_required")
        persona, signature = await self._execution(space, route)
        if window and window["signature"] != signature:
            raise ReviewRequired("configuration_changed")
        scoped_model = CONTEXT_REBUILD_MODELS.get(job["kind"])
        if window:
            if window["source_ids"] != list(source_ids):
                raise ReviewRequired("context_changed")
            known = tuple(window["known_node_ids"])
        elif scoped_model:
            known = ()
        else:
            sources = await self.sources.get_records(tuple(source_ids))
            async def progress(scanned):
                await self._save(job, checkpoint, f"context:{scanned}", checkpoint.get("done", 0), checkpoint.get("total"))

            known = await self._context_ids(space, generation_id, sources,
                timeout_seconds=max(0.001, deadline - time.monotonic()), progress=progress)
        batch = await self.extraction.prepare(space_id=space["space_id"], source_ids=tuple(source_ids),
            persona=persona, known_node_ids=known, generation_id=generation_id,
            job_id=job["job_id"], lease_token=job["lease_token"], allow_building=building,
            source_cursor=source_cursor, reference_protocol=REFERENCE_VERSION)
        if window and batch.input_digest != window["input_digest"]:
            raise ReviewRequired("context_changed")
        if not window:
            window = {"source_ids": list(source_ids), "source_cursor": source_cursor, "model_route": route,
                "known_node_ids": list(known), "input_digest": batch.input_digest, "signature": signature,
                "reference_protocol": REFERENCE_VERSION,
                "correction_limit": self.config.extraction_max_regenerations}
            if space["space_type"] == "private":
                slots = await prepare_slots(self.sources, self.image_cache, batch.sources,
                    limit=self.config.private_image_limit, evidence_only=bool(scoped_model))
                if slots:
                    window["image_slots"] = slots
            if scoped_model:
                window["context_rebuild_kind"] = job["kind"]
            checkpoint["window"] = window
            await self._save(job, checkpoint, "prepared", checkpoint.get("done", 0), checkpoint.get("total"))
        references = ExtractionReferences(batch)
        manifest = window.get("image_slots", [])
        if "raw" not in window:
            base = references.messages()
            if scoped_model:
                schema = scoped_model.model_json_schema()
                base = scoped_messages(base, job["kind"], schema, prompt_version=batch.prompt_version)
            else:
                schema = extraction_model_json_schema(schema_for_prompt_version(batch.prompt_version))
            while True:
                attachments, changed = await load_slots(manifest, self.image_cache)
                if changed:
                    window.pop("model_request_digest", None)
                    window.pop("regeneration", None)
                messages = base
                output_schema = schema
                if "regeneration" in window:
                    archived = await self.jobs.get_candidate(job["job_id"], window["regeneration"].get("candidate_id"))
                    try:
                        messages = regeneration_messages(messages, window, archived,
                            schema_version=schema_for_prompt_version(batch.prompt_version))
                    except RegenerationContextError as exc:
                        raise ReviewRequired(exc.code) from None
                if attachments:
                    collect = collects_evidence(attachments)
                    messages = vision_messages(messages, attachments, collect_evidence=collect)
                    if collect:
                        output_schema = vision_schema(schema)
                if text_budget(messages) > self.config.extraction_max_prompt_bytes:
                    raise ReviewRequired("prompt_budget_exceeded")
                # Bytes are bound by their pinned hashes. Never put a data URL in a checkpoint.
                request_digest = digest(messages)
                if window.get("model_request_digest", request_digest) != request_digest:
                    raise ReviewRequired("model_request_changed")
                window["model_request_digest"] = request_digest
                await self._save(job, checkpoint, "model", checkpoint.get("done", 0), checkpoint.get("total"))
                diagnostics = {}
                try:
                    options = {}
                    if self.config.extraction_output_format != "text":
                        options = {"output_format": self.config.extraction_output_format,
                                   "output_schema": output_schema}
                    raw = await self.runtime.adapter.generate(messages, provider_id=route["provider_id"],
                        **({"model": route["model_override"]} if route.get("model_override") else {}),
                        timeout_seconds=max(0.001, deadline - time.monotonic()), diagnostics=diagnostics, **options)
                    break
                except VisionUnsupportedError:
                    if not any(slot.get("sha256") for slot in manifest):
                        raise
                    for slot in manifest:
                        if slot.pop("sha256", None):
                            slot["reason"] = "vision_unsupported"
                    window.pop("model_request_digest", None)
                    window.pop("regeneration", None)
                except Exception as exc:
                    window["response_diagnostics"] = diagnostics
                    await self._save(job, checkpoint, "model", checkpoint.get("done", 0), checkpoint.get("total"))
                    if is_transient_provider_error(exc):
                        raise ModelNetworkError("provider_temporary_failure") from None
                    raise ReviewRequired("provider_unavailable") from None
            if "regeneration" in window:
                diagnostics["repeated_candidate"] = raw == archived["window"]["raw"]
            window["response_diagnostics"] = diagnostics
            if len(raw) > 120_000:
                raise ReviewRequired("output_budget_exceeded")
            window["raw"] = raw
            await self._save(job, checkpoint, "validate", checkpoint.get("done", 0), checkpoint.get("total"))
        response_info = window.get("response_diagnostics", {})
        if response_info.get("response_status") == "incomplete" or response_info.get("finish_reason") == "length":
            raise ReviewRequired("output_incomplete")
        _, current_signature = await self._execution(space, route)
        if signature != current_signature:
            raise ReviewRequired("configuration_changed")
        try:
            raw = window["raw"]
            evidence = []
            if collects_evidence(manifest):
                raw, evidence = split_vision_output(raw, manifest)
            if scoped_model:
                raw = extraction_envelope(raw, job["kind"], schema_for_prompt_version(batch.prompt_version), batch)
            canonical = references.decode(raw)
        except VisionOutputError as exc:
            window["validation_errors"] = [{"path": exc.path, "type": exc.code, "instruction": exc.instruction}]
            await self._save(job, checkpoint, "validate", checkpoint.get("done", 0), checkpoint.get("total"))
            raise ReviewRequired("invalid_output") from None
        except ReferenceValidationError as exc:
            window["validation_errors"] = [{"path": exc.path, "type": exc.code}]
            await self._save(job, checkpoint, "validate", checkpoint.get("done", 0), checkpoint.get("total"))
            raise ReviewRequired("unknown_reference") from None
        except ExtractionCapacityError as exc:
            window["validation_errors"] = [{"path": "$", "type": "output_capacity_exceeded",
                "stage": "raw", "actual_chars": exc.actual_chars, "limit_chars": exc.limit_chars}]
            await self._save(job, checkpoint, "validate", checkpoint.get("done", 0), checkpoint.get("total"))
            raise ReviewRequired("invalid_output") from None
        except CandidateValidationError as exc:
            path = "$" + "".join(
                f"[{item}]" if isinstance(item, int) else f".{item}" for item in exc.location
            )
            window["validation_errors"] = [{"path": path, "type": exc.code}]
            await self._save(job, checkpoint, "validate", checkpoint.get("done", 0), checkpoint.get("total"))
            raise ReviewRequired(exc.code) from None
        except ValueError as exc:
            if isinstance(exc, ValidationError):
                window["validation_errors"] = _schema_diagnostics(exc)
            else:
                diagnostic = {"path": "$", "type": "duplicate_json_key" if isinstance(exc, DuplicateJSONKey) else "json_invalid"}
                if isinstance(exc, json.JSONDecodeError):
                    # Decoder positions count characters: line/column start at 1, offset at 0.
                    diagnostic.update(lineno=exc.lineno, colno=exc.colno, pos=exc.pos)
                window["validation_errors"] = [diagnostic]
            await self._save(job, checkpoint, "validate", checkpoint.get("done", 0), checkpoint.get("total"))
            raise ReviewRequired("invalid_output") from None
        window["canonical_raw"] = canonical
        await self._save(job, checkpoint, "validate", checkpoint.get("done", 0), checkpoint.get("total"))
        try:
            options = {"image_evidence": evidence} if evidence else {}
            receipt = await self.extraction.apply(batch, canonical, model_id=route["provider_id"], **options)
            await self._discard_images(job, space, source_ids)
            return receipt
        except PermissionError:
            raise ReviewRequired("task_write_not_permitted") from None
        except ExtractionCapacityError as exc:
            window["validation_errors"] = [{"path": "$", "type": "output_capacity_exceeded",
                "stage": "canonical", "actual_chars": exc.actual_chars, "limit_chars": exc.limit_chars}]
            await self._save(job, checkpoint, "validate", checkpoint.get("done", 0), checkpoint.get("total"))
            raise ReviewRequired("invalid_output") from None
        except ValidationError as exc:
            window["validation_errors"] = _schema_diagnostics(exc)
            await self._save(job, checkpoint, "validate", checkpoint.get("done", 0), checkpoint.get("total"))
            raise ReviewRequired("invalid_output") from None
        except InvalidExtractionCandidate as exc:
            window["validation_errors"] = [{"path": exc.path, "type": exc.code}]
            await self._save(job, checkpoint, "validate", checkpoint.get("done", 0), checkpoint.get("total"))
            raise ReviewRequired(exc.code) from None
        except CandidateValidationError as exc:
            path = "$" + "".join(
                f"[{item}]" if isinstance(item, int) else f".{item}" for item in exc.location
            )
            window["validation_errors"] = [{"path": path, "type": exc.code}]
            await self._save(job, checkpoint, "validate", checkpoint.get("done", 0), checkpoint.get("total"))
            raise ReviewRequired(exc.code) from None
        except ValueError as exc:
            if str(exc) == "node protection changed":
                raise ReviewRequired("context_changed") from None
            raise ReviewRequired("context_or_evidence_invalid") from None

    async def _discard_images(self, job, space, source_ids):
        if self.image_cache and space["space_type"] == "private" and job["kind"] not in CONTEXT_REBUILD_MODELS:
            await self.image_cache.discard_many(source_ids)

    async def _extract(self, job):
        payload, checkpoint = job["payload"], dict(job["checkpoint"])
        if payload.get("blocked"):
            raise ReviewRequired("oversized_source")
        space = await self.memory.get_space(payload["space_id"])
        generation = await self.memory.get_active_generation(space["space_id"])
        if generation["generation_id"] != payload["expected_generation_id"]:
            raise ReviewRequired("context_changed")
        checkpoint.setdefault("total", len(payload["source_ids"]))
        receipt = await self._window(job, checkpoint, space, generation["generation_id"],
                                     payload["source_ids"], payload["source_cursor"])
        counts = image_counts(checkpoint.get("window", {}))
        index_result = await self._index_committed_window(job, checkpoint, receipt)
        return {**({"image_inputs": counts} if counts else {}), "batch_id": receipt["batch_id"], "source_count": len(payload["source_ids"]),
                "generation_id": generation["generation_id"], "index": index_result}

    async def _rebuild(self, job):
        if job["payload"].get("include_topics") and job["kind"] in CONTEXT_REBUILD_MODELS:
            return await rebuild_sections(self, job)
        return await self._replay_rebuild(job)

    async def _replay_rebuild(self, job):
        receipt = await self.pipeline.get_rebuild_receipt(job["job_id"])
        if receipt:
            return receipt["result"]
        payload, checkpoint = job["payload"], dict(job["checkpoint"])
        space = await self.memory.get_space(payload["space_id"])
        parent = await self.memory.get_active_generation(space["space_id"])
        if parent["generation_id"] != payload["expected_generation_id"]:
            raise ReviewRequired("context_changed")
        scoped = job["kind"] in CONTEXT_REBUILD_MODELS
        if scoped and await self.pipeline.context_basis(job["kind"], parent["generation_id"]) != payload["context_basis"]:
            raise ReviewRequired("context_changed")
        generation = await self.pipeline.begin_rebuild(job, space["space_id"], payload["expected_generation_id"])
        policy = payload.get("model_policy", {})
        route = fixed_route(policy, policy.get("default_provider_id", self.config.llm_provider_id),
                            payload.get("request", {}).get("model_selection", {}))
        _, signature = await self._execution(space, route)
        if checkpoint.get("signature", signature) != signature:
            raise ReviewRequired("configuration_changed")
        checkpoint.update(signature=signature, total=payload["total"], generation_id=generation["generation_id"])
        checkpoint.setdefault("done", 0)
        checkpoint.setdefault("after", 0)
        if checkpoint.get("phase") != "switch":
            while True:
                if checkpoint.get("window"):
                    window = checkpoint["window"]
                    source_ids, cursor = window["source_ids"], window["source_cursor"]
                else:
                    # Jobs created before stream offsets used one frozen insertion cursor.
                    if "model_policy" in payload and len(payload["highwater"]) > 1:
                        plan = await self.replay_plan(space, payload, checkpoint.get("stream_offsets", {}))
                        if plan is None:
                            checkpoint["pending"] = payload["total"] - checkpoint["done"]
                            break
                        stream, records, decision = plan
                        checkpoint["replay_stream"] = stream
                        cursor = 0
                    else:
                        if payload.get("replay_deliveries"):
                            page = await self.scanner.replay_page(space, payload["highwater"], checkpoint["done"])
                        else:
                            page = await self.scanner.page(space, highwater=payload["highwater"], after=checkpoint["after"])
                        records = page.records
                        decision = await self._decision(space, records, replay=True)
                        if decision.status == "waiting":
                            checkpoint["pending"] = len(records)
                            break
                        chosen = set(decision.source_ids)
                        cursor = max((r["scan_cursor"] for r in records if r["source_id"] in chosen), default=0)
                    if decision.status == "oversized":
                        raise ReviewRequired("oversized_source")
                    source_ids = decision.source_ids
                window_receipt = await self._window(job, checkpoint, space, generation["generation_id"], source_ids, cursor, building=True)
                if not scoped:
                    await self._index_committed_window(job, checkpoint, window_receipt)
                for kind, count in image_counts(checkpoint.get("window", {})).items():
                    counts = checkpoint.setdefault("image_inputs", {})
                    counts[kind] = counts.get(kind, 0) + count
                if stream := checkpoint.pop("replay_stream", None):
                    offsets = checkpoint.setdefault("stream_offsets", {})
                    offsets[stream] = offsets.get(stream, 0) + len(source_ids)
                checkpoint["after"] = cursor
                checkpoint["done"] += len(source_ids)
                checkpoint.pop("window", None)
                await self._save(job, checkpoint, "replay", checkpoint["done"], checkpoint["total"])
            if scoped:
                await self._save(job, checkpoint, "switch", checkpoint["done"], checkpoint["total"])
                try:
                    return await self.pipeline.publish_context(job, generation["generation_id"], {
                        "source_count": checkpoint["done"], "pending_source_count": checkpoint.get("pending", 0)})
                except ValueError as exc:
                    if str(exc) == "context rebuild target changed":
                        raise ReviewRequired("context_changed") from None
                    raise
            if self.config.topic_consolidation_enabled and "topic_result" not in checkpoint:
                checkpoint["phase"] = "topics"
                await self._save(job, checkpoint, "topic_prepare", checkpoint["done"], checkpoint["total"])
                await self._rebuild_topics(job, checkpoint, space, generation["generation_id"])
            # Preserve resumable input progress before optional vector indexing.
            checkpoint["phase"] = "index"
            await self._save(job, checkpoint, "index", checkpoint["done"], checkpoint["total"])
            checkpoint["index_result"] = await self._index_generation(job, space["space_id"], generation["generation_id"])
            checkpoint["phase"] = "switch"
            await self._save(job, checkpoint, "switch", checkpoint["done"], checkpoint["total"])
        result = {"generation_id": generation["generation_id"], "source_count": checkpoint["done"],
                  "pending_source_count": checkpoint.get("pending", 0),
                  "index": checkpoint.get("index_result")}
        if "image_inputs" in checkpoint:
            result["image_inputs"] = checkpoint["image_inputs"]
        switched = await self.pipeline.activate_rebuild(job, space["space_id"], generation["generation_id"],
            payload["expected_generation_id"], result=result,
            invalidate_nodes=self.runtime.cache.invalidate_nodes if self.runtime.cache else None)
        self._pending_reads.pop(payload["expected_generation_id"], None)
        self._read_hints.pop(payload["expected_generation_id"], None)
        return switched["result"]

    async def _index_committed_window(self, job, checkpoint, receipt):
        if self.runtime.recall is None:
            return {"indexed": 0, "configured": False}
        nodes = await self.memory.extraction_batch_index_nodes(receipt["batch_id"])
        if not nodes:
            return {"indexed": 0, "configured": True}
        # 成功批次已经持久保存候选结果和来源，不继续在断点中复制大段输出。
        window = checkpoint.get("window")
        if window is not None:
            counts = image_counts(window)
            checkpoint["window"] = {key: window[key] for key in ("source_ids", "source_cursor")}
            if counts:
                checkpoint["window"]["image_counts"] = counts
        state = {"batch_id": receipt["batch_id"], "indexed": 0, "total": len(nodes)}
        checkpoint["window_index"] = state
        await self._save(job, checkpoint, "window_index", checkpoint.get("done", 0), checkpoint.get("total"))
        service = self.runtime.recall.embeddings
        fingerprint = service.fingerprint
        deadline = time.monotonic() + self.config.index_timeout_seconds
        try:
            async with asyncio.timeout(self.config.index_timeout_seconds):
                for start in range(0, len(nodes), service.batch_size):
                    batch = nodes[start:start + service.batch_size]
                    await node_vectors(service, batch, receipt["space_id"], deadline=deadline, require_persisted=True)
                    if service.fingerprint != fingerprint:
                        raise ValueError("embedding provider changed during window indexing")
                    state["indexed"] += len(batch)
                    await self._save(job, checkpoint, "window_index", checkpoint.get("done", 0), checkpoint.get("total"))
        except Exception as exc:
            raise self._embedding_failure(exc) from None
        checkpoint.pop("window_index", None)
        return {"indexed": len(nodes), "configured": True}

    async def _index_generation(self, job, space_id, generation_id):
        if self.runtime.recall is None:
            return {"indexed": 0, "configured": False}
        service = self.runtime.recall.embeddings
        fingerprint = service.fingerprint
        deadline = time.monotonic() + self.config.index_timeout_seconds
        async with asyncio.timeout(self.config.index_timeout_seconds):
            snapshot = await self.memory.index_nodes(space_id, generation_id=generation_id,
                                                     limit=self.config.recall_index_limit)
            nodes = snapshot["nodes"]
            indexed = repaired = 0
            for start in range(0, len(nodes), service.batch_size):
                batch = nodes[start:start + service.batch_size]
                try:
                    _, missing = await node_vectors(service, batch, space_id, deadline=deadline, require_persisted=True)
                    repaired += missing
                except Exception as exc:
                    raise self._embedding_failure(exc) from None
                if service.fingerprint != fingerprint:
                    raise ReviewRequired("index_embedding_changed")
                indexed += len(batch)
                if job["kind"] == "reindex":
                    await self._save(job, {}, "index", indexed, len(nodes))
                else:
                    saved = await self.jobs.get(job["job_id"])
                    await self._save(job, saved["checkpoint"], f"index:{indexed}/{len(nodes)}",
                                     saved["progress_done"], saved["progress_total"])
            return {"indexed": indexed, "repaired": repaired, "configured": True, "truncated": snapshot["truncated"]}

    async def _reindex(self, job):
        payload = job["payload"]
        generation = await self.memory.get_active_generation(payload["space_id"])
        if generation["generation_id"] != payload["expected_generation_id"]:
            raise ReviewRequired("context_changed")
        await self._save(job, {}, "index")
        return await self._index_generation(job, payload["space_id"], generation["generation_id"])

    async def _govern(self, job):
        payload = job["payload"]
        await self._save(job, {}, "governance", 0, 1)
        result = await self.governance.apply(payload["space_id"], payload["operation"],
            actor="webui", reason=payload.get("reason", ""), job_id=job["job_id"], lease_token=job["lease_token"])
        if self.runtime.cache is not None and not result.get("title_only", False):
            ids = set(result["before_nodes"]) | set(result["nodes"]) | set(result["stale_parent_ids"])
            try:
                await self.runtime.cache.invalidate_nodes(payload["space_id"], ids)
            except Exception as exc:
                # Version/state validation already prevents stale recall. Cache
                # cleanup is expendable and cannot undo a committed correction.
                self.last_error_type = type(exc).__name__
        return result

    async def _reject(self, job):
        payload = job["payload"]
        space = await self.memory.get_space(payload["space_id"])
        generation = await self.memory.get_active_generation(space["space_id"])
        if generation["generation_id"] != payload["expected_generation_id"]:
            raise ReviewRequired("context_changed")
        batch_id = "batch_" + digest([space["space_id"], generation["generation_id"], tuple(payload["source_ids"])])
        receipt = await self.memory.get_extraction_batch(batch_id)
        if receipt and receipt["model_id"] != "manual-rejection":
            raise ReviewRequired("already_committed")
        if receipt is None:
            batch = await self.extraction.prepare(space_id=space["space_id"], source_ids=tuple(payload["source_ids"]),
                persona="人工明确拒绝本窗口的整批候选；保留原始来源。", job_id=job["job_id"],
                lease_token=job["lease_token"], source_cursor=payload["source_cursor"])
            schema_version = schema_for_prompt_version(batch.prompt_version)
            candidate = {"schema_version": schema_version, "result": "no_change", "reason": "no_meaningful_content",
                "node_changes": [], "profile_changes": [], "edges": [], "relationship_change": None,
                "coverage_changes": []}
            raw = json.dumps(candidate)
            receipt = await self.extraction.apply(batch, raw, model_id="manual-rejection")
        await self._discard_images(job, space, payload["source_ids"])
        await self.jobs.cancel(payload["failed_job_id"])
        return {"batch_id": receipt["batch_id"], "source_count": len(payload["source_ids"]), "review": "rejected"}
