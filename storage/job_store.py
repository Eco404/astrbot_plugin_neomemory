"""Durable task admission, progress, leases and fencing in ``memory.db``.

Every admission and lease change is a short BEGIN IMMEDIATE transaction. Multiple
connections/processes therefore share the same scope exclusion guarantees. A
caller modifying memory must assert its lease inside that memory transaction.
"""

from __future__ import annotations

import json
import hashlib
import math
import re
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from .sqlite_executor import SQLiteExecutor

try:
    from ..core.attempts import current_attempt
    from ..core.extraction_apply import digest
    from ..core.extraction_regeneration import VERSION as REGENERATION_VERSION
    from ..core.candidate_feedback import SHORT_TERM_RECOVERY_VERSION, TOPIC_RECOVERY_VERSION
except ImportError:
    from core.attempts import current_attempt
    from core.extraction_apply import digest
    from core.extraction_regeneration import VERSION as REGENERATION_VERSION
    from core.candidate_feedback import SHORT_TERM_RECOVERY_VERSION, TOPIC_RECOVERY_VERSION

ACTIVE_STATES = ("queued", "running", "retry_wait")
JOB_STATES = (*ACTIVE_STATES, "failed", "succeeded", "cancelled")
AUTO_RECOVERY_SECONDS = 6 * 60 * 60
NETWORK_RETRY_SECONDS = 10 * 60
NETWORK_RETRIES = 3
MAX_JSON_BYTES = 1024 * 1024

# Older cancellations only retained the last error. New ones record the exact state.
CANCELLED_WINDOW_RELEASED = """(
    json_extract(result_json,'$.replacement_job_id') IS NOT NULL OR
    COALESCE(json_extract(result_json,'$.cancelled_from')='failed', error_code IS NOT NULL)
)"""


class JobConflict(RuntimeError):
    """Another durable operation owns the requested scope."""


class LeaseLost(RuntimeError):
    """A worker no longer has permission to commit its result."""


def _text(value: str, name: str, *, limit: int = 256) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise ValueError(f"{name} must be a nonempty string of at most {limit} characters")
    return value


def _positive_seconds(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a finite positive number")
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a finite positive number")
    return float(value)


def _json(value: Mapping[str, Any], *, limit=MAX_JSON_BYTES) -> str:
    if not isinstance(value, Mapping):
        raise ValueError("task data must be a JSON object")
    try:
        result = json.dumps(dict(value), ensure_ascii=False, sort_keys=True,
                            separators=(",", ":"), allow_nan=False)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ValueError("task data must contain valid JSON values") from exc
    if len(result.encode("utf-8")) > limit:
        raise ValueError("task data exceeds the storage limit")
    return result


def _checkpoint_json(checkpoint):
    if "sections" in checkpoint:
        # Each concurrent section may have one full-sized frozen window.
        return _json(checkpoint, limit=2 * MAX_JSON_BYTES + 4096)
    return _json(checkpoint)


def _recovery_lanes(job):
    checkpoint = job["checkpoint"]
    if "sections" not in checkpoint:
        yield checkpoint, job["error_code"]
        return
    for name, lane in checkpoint["sections"].items():
        progress = checkpoint["section_progress"][name]
        if progress["state"] != "succeeded":
            yield lane, progress.get("error_code")


def _drafts(job):
    for lane, error_code in _recovery_lanes(job):
        if job["kind"] == "short_term" and "raw" in lane:
            yield "short_term", lane, error_code, ("SELECT 1 FROM short_term_receipts WHERE job_id=?", (job["job_id"],))
        window = lane.get("window", {})
        if "raw" in window:
            generation = lane.get("generation_id", job["payload"].get("expected_generation_id"))
            batch_id = "batch_" + digest([job["scope_key"], generation, window.get("source_ids", [])])
            yield "extraction", window, error_code, ("SELECT 1 FROM extraction_batches WHERE batch_id=?", (batch_id,))
        topic = lane.get("topic_window") or lane.get("topic_synthesis", {}).get("topic_window", {})
        if "raw" in topic:
            yield "topic", topic, error_code, ("SELECT 1 FROM topic_receipts WHERE receipt_key=?", (topic["receipt_key"],))


def _draft_action(kind, window, code, job):
    diagnostics = window.get("response_diagnostics", {})
    if (kind in {"extraction", "short_term"}
            and code in {"output_incomplete", "invalid_short_term_output"}
            and (diagnostics.get("finish_reason") == "length" or diagnostics.get("incomplete_reason") == "max_output_tokens")
            and len(window.get("source_ids", job["payload"].get("source_ids", []))) > 1):
        return "resize"
    if (code in {"invalid_output", "invalid_short_term_output", "invalid_topic_output", "unknown_reference", "output_incomplete"}
            or any(item.get("type") == code for item in window.get("validation_errors", []))):
        return "regenerate"
    return "resume"


async def _recovery_drafts(connection, job):
    drafts = []
    committed = False
    for kind, window, code, receipt in _drafts(job):
        if await (await connection.execute(*receipt)).fetchone():
            committed = True
        else:
            drafts.append((kind, window, code, _draft_action(kind, window, code, job)))
    return drafts, committed


def _restart_required(job):
    return bool(job["payload"].get("topic_inputs_replaced")) or job["error_code"] in {
        "context_changed", "configuration_changed", "model_required", "model_request_changed",
        "configuration_or_context_changed", "generation_changed", "source_range_changed",
        "summary_context_changed", "model_test_configuration_changed", "prompt_budget_exceeded", "oversized_source",
        "source_budget_exceeded",
    }


def inspection_checkpoint(job):
    checkpoint = job["checkpoint"]
    sections = checkpoint.get("sections")
    if sections is None:
        return checkpoint
    name = checkpoint.get("failed_section")
    if name is None:
        name = next((key for key, lane in sections.items()
                     if lane.get("topic_window", {}).get("raw") or lane.get("window", {}).get("raw")), "context")
    return sections[name]


def _job(row) -> dict[str, Any] | None:
    if row is None:
        return None
    result = dict(row)
    for field in ("payload", "checkpoint", "result"):
        value = result.pop(f"{field}_json")
        result[field] = json.loads(value) if value is not None else None
    result["admission"] = result["payload"].pop("_original_admission", {})
    result["attempts"] = json.loads(result.pop("attempts_json"))
    return result


def _attempt_history(job, state, now, **details):
    observation = current_attempt.get() or {}
    remaining = None if job["progress_total"] is None else job["progress_total"] - job["progress_done"]
    if outstanding := job["checkpoint"].get("window_index"):
        remaining = outstanding["total"] - outstanding["indexed"]
    entry = {"remaining": remaining, "attempt": job["attempt"], "step": job["step"], "state": state, "ended_at": now,
             "progress_done": job["progress_done"], "progress_total": job["progress_total"],
             **observation, **details}
    entry["had_final_failure"] = (state == "failed" or job["state"] == "failed"
        or (job["result"] or {}).get("cancelled_from") == "failed"
        or any(item.get("had_final_failure") or item["state"] == "failed" for item in job["attempts"]))
    return json.dumps([*job["attempts"][-11:], entry], separators=(",", ":"))


def _resume_history(job):
    # A failed task from a version without execution telemetry still has a known terminal state.
    observed = any(item.get("had_final_failure") or item["state"] == "failed" for item in job["attempts"])
    if not observed and (job["state"] == "failed" or (job["result"] or {}).get("cancelled_from") == "failed"):
        return _attempt_history(job, "failed", job["updated_at"], error_code=job["error_code"])
    return json.dumps(job["attempts"], separators=(",", ":"))


async def _get(connection, job_id: str) -> dict[str, Any] | None:
    cursor = await connection.execute("SELECT * FROM memory_jobs WHERE job_id=?", (job_id,))
    return _job(await cursor.fetchone())


async def _blockers(connection, scope_key: str, exclude_job_id: str | None = None):
    cursor = await connection.execute(
        "SELECT job_id,kind,scope_key,state FROM memory_jobs "
        "WHERE state IN ('queued','running','retry_wait') "
        "AND (?='*' OR scope_key='*' OR scope_key=?) "
        "AND (? IS NULL OR job_id<>?) ORDER BY created_at,job_id",
        (scope_key, scope_key, exclude_job_id, exclude_job_id),
    )
    return [dict(row) for row in await cursor.fetchall()]


async def assert_scope_available(connection, scope_key: str, *, exclude_job_id=None) -> None:
    """Validate admission within the caller's write transaction, without a race."""
    _text(scope_key, "scope_key")
    if await _blockers(connection, scope_key, exclude_job_id):
        raise JobConflict("another task owns this memory scope")


async def assert_job_lease(
    connection, job_id: str, lease_token: str, *, scope_key: str, now: float | None = None,
) -> dict[str, Any]:
    """Fence a mutation in the same transaction as the memory writes it permits.

    A global ``*`` lease covers every space; a space lease covers only that space.
    Expired leases cannot be renewed or used, even before another worker claims.
    """
    _text(scope_key, "scope_key")
    current = time.time() if now is None else now
    row = await _get(connection, job_id)
    if (
        row is None or row["state"] != "running" or not lease_token
        or row["lease_token"] != lease_token or row["lease_until"] <= current
        or row["scope_key"] not in ("*", scope_key)
    ):
        raise LeaseLost("task lease is absent, expired, or outside this memory scope")
    return row


class JobStore:
    """Uses the already-open MemoryStore executor; owns no separate connection."""

    def __init__(self, database: SQLiteExecutor, *, clock: Callable[[], float] = time.time):
        self.database = database
        self.clock = clock
        self.audit = None

    async def _log_job(self, job, event, level="info"):
        if self.audit is not None:
            details = {key: job[key] for key in ("job_id", "kind", "scope_key", "state", "step",
                       "attempt", "automatic_origin", "recovery_mode", "error_code") if job.get(key) is not None}
            if job["state"] == "retry_wait":
                details["available_at"] = job["available_at"]
            await self.audit.record(level, "task", event, **details)
        return job

    async def next_window_key(self, base_key: str, *, reviewed_again: bool = False) -> str | None:
        """Identify one new automatic attempt for this still-pending window."""
        previous = await self.database.fetchone(
            f"SELECT job_id,state,{CANCELLED_WINDOW_RELEASED} AS released FROM memory_jobs "
            "WHERE idempotency_key=? OR idempotency_key GLOB ? ORDER BY rowid DESC LIMIT 1",
            (base_key, base_key + ":*"))
        if previous is None:
            return base_key
        if ((previous["state"] == "cancelled" and previous["released"])
                or (previous["state"] == "succeeded" and reviewed_again)):
            return base_key + ":" + previous["job_id"]
        return None

    async def submit(
        self, kind: str, scope_key: str, payload: Mapping[str, Any], idempotency_key: str,
        *, max_attempts: int = 2, hide_on_success: bool = False, replaces=None, invalidate_nodes=None,
    ) -> dict[str, Any]:
        _text(kind, "kind", limit=80)
        _text(scope_key, "scope_key")
        _text(idempotency_key, "idempotency_key", limit=512)
        if type(max_attempts) is not int or not 1 <= max_attempts <= 20:
            raise ValueError("max_attempts must be between 1 and 20")
        payload_json = _json(payload)
        if replaces is not None:
            async with self.database.transaction() as connection:
                updated = await self._reprepare(connection, replaces, kind, scope_key, payload, max_attempts, invalidate_nodes)
            return await self._log_job(updated, "任务已重新准备")
        async with self.database.transaction() as connection:
            cursor = await connection.execute(
                "SELECT * FROM memory_jobs WHERE idempotency_key=?", (idempotency_key,),
            )
            existing = await cursor.fetchone()
            if existing is not None:
                job = _job(existing)
                if (job["kind"] != kind or job["scope_key"] != scope_key
                        or (existing["payload_json"] != payload_json and job["admission"].get("digest") != digest(payload))):
                    raise JobConflict("idempotency key already describes another task")
                return job
            if kind == "reindex" and hide_on_success:
                cursor = await connection.execute(
                    "SELECT * FROM memory_jobs WHERE kind='reindex' AND state='failed' AND automatic_origin=1 "
                    "AND scope_key=? AND json_extract(payload_json,'$.expected_generation_id')=? "
                    "ORDER BY updated_at DESC LIMIT 1", (scope_key, payload.get("expected_generation_id")))
                if pending := await cursor.fetchone():
                    return _job(pending)
            job_id = f"job_{uuid.uuid4().hex}"
            now = self.clock()
            await assert_scope_available(connection, scope_key)
            await connection.execute(
                "INSERT INTO memory_jobs (job_id,kind,scope_key,idempotency_key,state,"
                "payload_json,max_attempts,available_at,created_at,updated_at,hide_on_success,automatic_origin) "
                "VALUES (?,?,?,?,'queued',?,?,?,?,?,?,?)",
                (job_id, kind, scope_key, idempotency_key, payload_json,
                 max_attempts, now, now, now, hide_on_success, hide_on_success),
            )
            updated = await _get(connection, job_id)
        return await self._log_job(updated, "任务已入队")

    async def _reprepare(self, connection, original, kind, scope_key, payload, max_attempts, invalidate_nodes):
        job_id = original["job_id"]
        if original.get("lease_token"):
            previous = await self._lease(connection, job_id, original["lease_token"])
            if previous["recovery_mode"] != "fresh":
                raise JobConflict("task has not entered fresh recovery")
            hide = previous["hide_on_success"]
        else:
            previous = await _get(connection, job_id)
            if (previous and previous["state"] == "queued" and previous["admission"]
                    and previous["payload"] == payload and (previous["kind"], previous["scope_key"]) == (kind, scope_key)):
                return previous
            if not previous or previous["state"] not in ("failed", "cancelled"):
                raise JobConflict("only stopped tasks can be reprepared manually")
            if (previous["result"] or {}).get("replacement_job_id"):
                raise JobConflict("task has already been replaced")
            hide = False
        if (previous["kind"], previous["scope_key"]) != (kind, scope_key):
            raise JobConflict("new preparation does not match the task")
        await assert_scope_available(connection, scope_key, exclude_job_id=job_id)
        await self._discard_work(connection, previous, invalidate_nodes=invalidate_nodes, reprepare=True)
        # Keep the original submission key valid even when a new window or provider is selected.
        admission = previous["admission"] or {"digest": digest(previous["payload"]),
                                               "request": previous["payload"].get("request")}
        now = self.clock()
        history = _attempt_history(previous, "queued", now, action="fresh")
        total = len(payload["source_ids"]) if kind in {"extract", "short_term"} else payload.get("total")
        await connection.execute(
            """UPDATE memory_jobs SET payload_json=?,checkpoint_json='{}',step='queued',state='queued',
               progress_done=0,progress_total=?,attempt=0,max_attempts=?,available_at=?,updated_at=?,
               result_json=NULL,recovery_mode=NULL,lease_token=NULL,lease_until=NULL,owner=NULL,
               error_type=NULL,error_code=NULL,hide_on_success=?,attempts_json=? WHERE job_id=?""",
            (_json({**payload, "_original_admission": admission}), total, max_attempts, now, now, hide, history, job_id))
        return await _get(connection, job_id)

    async def claim(self, owner: str, *, lease_seconds: float = 30,
                    exclude_job_ids: tuple[str, ...] = ()) -> dict[str, Any] | None:
        _text(owner, "owner")
        _positive_seconds(lease_seconds, "lease_seconds")
        async with self.database.transaction() as connection:
            now = self.clock()
            # A restart is not itself an execution failure. Resume from the last
            # checkpoint without consuming a configured model retry attempt.
            await connection.execute(
                "UPDATE memory_jobs SET state='queued',lease_token=NULL,lease_until=NULL,"
                "owner=NULL,available_at=?,updated_at=?,attempt=MAX(0,attempt-1) "
                "WHERE state='running' AND lease_until<=?", (now, now, now),
            )
            cursor = await connection.execute(
                "SELECT j.job_id,j.state FROM memory_jobs j WHERE available_at<=? "
                "AND job_id NOT IN (SELECT value FROM json_each(?)) AND ("
                "state IN ('queued','retry_wait') OR (state='failed' AND automatic_origin=1 AND NOT EXISTS ("
                "SELECT 1 FROM memory_jobs active WHERE active.state IN ('running','queued','retry_wait') "
                "AND (active.scope_key=j.scope_key OR active.scope_key='*' OR j.scope_key='*')))) "
                "ORDER BY available_at,created_at,job_id LIMIT 1", (now, json.dumps(exclude_job_ids)),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            job_id = row["job_id"]
            if row["state"] == "failed":
                previous = await _get(connection, job_id)
                checkpoint = await self._prepare_retry(connection, previous)
                await connection.execute(
                    "UPDATE memory_jobs SET attempt=0,recovery_mode='resume',checkpoint_json=?,attempts_json=? WHERE job_id=?",
                    (checkpoint, _resume_history(previous), job_id))
            await connection.execute(
                "UPDATE memory_jobs SET state='running',owner=?,lease_token=?,lease_until=?,"
                "attempt=attempt+1,error_type=NULL,error_code=NULL,updated_at=? WHERE job_id=?",
                (owner, uuid.uuid4().hex, now + lease_seconds, now, job_id),
            )
            updated = await _get(connection, job_id)
        return await self._log_job(updated, "开始执行任务", "debug")

    async def _lease(self, connection, job_id, lease_token):
        job = await _get(connection, job_id)
        return await assert_job_lease(
            connection, job_id, lease_token, scope_key=job["scope_key"] if job else "*",
            now=self.clock(),
        )

    async def heartbeat(self, job_id: str, lease_token: str, *, lease_seconds: float = 30):
        _positive_seconds(lease_seconds, "lease_seconds")
        async with self.database.transaction() as connection:
            await self._lease(connection, job_id, lease_token)
            now = self.clock()
            await connection.execute(
                "UPDATE memory_jobs SET lease_until=?,updated_at=? WHERE job_id=?",
                (now + lease_seconds, now, job_id),
            )
            return await _get(connection, job_id)

    async def save_checkpoint(
        self, job_id: str, lease_token: str, *, checkpoint: Mapping[str, Any], step: str,
        progress_done: int, progress_total: int | None,
    ) -> dict[str, Any]:
        checkpoint_json = _checkpoint_json(checkpoint)
        _text(step, "step", limit=160)
        if type(progress_done) is not int or progress_done < 0:
            raise ValueError("progress_done must be a nonnegative integer")
        if progress_total is not None and (
            type(progress_total) is not int or progress_total < progress_done
        ):
            raise ValueError("progress_total must be unknown or at least progress_done")
        async with self.database.transaction() as connection:
            await self._lease(connection, job_id, lease_token)
            await connection.execute(
                "UPDATE memory_jobs SET recovery_mode=CASE WHEN recovery_mode='resume' AND "
                "(?>progress_done OR ?) "
                "THEN NULL ELSE recovery_mode END,checkpoint_json=?,step=?,progress_done=?,"
                "progress_total=?,error_type=NULL,error_code=NULL,updated_at=? WHERE job_id=?",
                (progress_done, bool((current_attempt.get() or {}).get("model_calls")), checkpoint_json,
                 step, progress_done, progress_total, self.clock(), job_id),
            )
            return await _get(connection, job_id)

    async def finish(self, job_id: str, lease_token: str, *, result=None) -> dict[str, Any]:
        result_json = _json({} if result is None else result)
        async with self.database.transaction() as connection:
            job = await self._lease(connection, job_id, lease_token)
            history = _attempt_history(job, "succeeded", self.clock(), remaining=0)
            await connection.execute(
                "UPDATE memory_jobs SET state='succeeded',lease_token=NULL,lease_until=NULL,"
                "owner=NULL,result_json=?,checkpoint_json='{}',step='complete',recovery_mode=NULL,"
                "progress_done=COALESCE(progress_total,progress_done),error_type=NULL,"
                "error_code=NULL,updated_at=?,attempts_json=? WHERE job_id=?", (result_json, self.clock(), history, job_id),
            )
            updated = await _get(connection, job_id)
        return await self._log_job(updated, "任务已完成")

    @staticmethod
    def retry_delay(attempt):
        return min(300, 5 * 2 ** min(10, max(0, attempt - 1)))

    def next_retry_delay(self, attempt, max_attempts, *, retryable, model_network):
        if retryable and attempt < max_attempts:
            return self.retry_delay(attempt)
        if retryable and model_network and attempt < max_attempts + NETWORK_RETRIES:
            return NETWORK_RETRY_SECONDS
        return None

    async def fail(
        self, job_id: str, lease_token: str, *, error_type: str, error_code: str,
        retryable: bool = False, model_network: bool = False, backoff_seconds: float | None = None,
    ) -> dict[str, Any]:
        # Never store str(exception), URLs, model response bodies or credentials.
        for field, value in (("error_type", error_type), ("error_code", error_code)):
            if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", value):
                raise ValueError(f"{field} must be a short diagnostic identifier")
        if type(retryable) is not bool:
            raise ValueError("retryable must be a boolean")
        if backoff_seconds is not None:
            _positive_seconds(backoff_seconds, "backoff_seconds")
        async with self.database.transaction() as connection:
            job = await self._lease(connection, job_id, lease_token)
            now = self.clock()
            # A long replay can have independent outages in successive windows.
            progress = [job["step"], job["progress_done"], "network" if model_network else error_code]
            prior = job["attempts"][-1] if job["attempts"] else {}
            streak = job["attempt"] if not prior else 1
            if job["attempt"] > 1 and prior.get("state") == "retry_wait" and prior.get("retry_progress") == progress:
                streak = prior["streak"] + 1
            delay = self.next_retry_delay(streak, job["max_attempts"],
                                         retryable=retryable, model_network=model_network)
            mode = job["recovery_mode"]
            observation = current_attempt.get() or {}
            waiting_for_slot = error_code == "model_queue_timeout" and retryable
            if waiting_for_slot:
                state, available = "retry_wait", now + self.retry_delay(1)
                # Carry the preceding real failure through any number of local waits.
                progress, streak = prior.get("retry_progress"), prior.get("streak", 0)
            elif (mode == "resume" and not retryable and job["attempt"] == 1
                  and not observation.get("model_calls") and not observation.get("commits")):
                state, mode, available = "queued", "fresh", now
            elif delay is not None:
                state, available = "retry_wait", now + (backoff_seconds or delay)
                if mode == "resume":
                    mode = None
            else:
                state, mode = "failed", None
                available = now + AUTO_RECOVERY_SECONDS if job["automatic_origin"] else now
            attempt = 0 if state == "queued" else job["attempt"]
            if waiting_for_slot:
                attempt = max(0, attempt - 1)
            history = _attempt_history(job, state, now, error_code=error_code, retry_progress=progress, streak=streak)
            await connection.execute(
                "UPDATE memory_jobs SET attempts_json=?,state=?,recovery_mode=?,attempt=?,lease_token=NULL,lease_until=NULL,owner=NULL,"
                "error_type=?,error_code=?,available_at=?,updated_at=? WHERE job_id=?",
                (history, state, mode, attempt, error_type, error_code, available, now, job_id),
            )
            updated = await _get(connection, job_id)
        if state == "failed":
            return await self._log_job(updated, "任务执行失败", "error")
        return await self._log_job(updated, "任务等待恢复", "warning")

    async def defer_until(self, job_id, lease_token, available_at):
        """Park a task until one of its independently budgeted sections can run."""
        async with self.database.transaction() as connection:
            job = await self._lease(connection, job_id, lease_token)
            history = _attempt_history(job, "retry_wait", self.clock(), branches=job["checkpoint"].get("section_progress", {}))
            await connection.execute(
                "UPDATE memory_jobs SET state='retry_wait',available_at=?,updated_at=?,attempts_json=?,"
                "attempt=MAX(0,attempt-1),owner=NULL,lease_token=NULL,lease_until=NULL WHERE job_id=?",
                (available_at, self.clock(), history, job_id),
            )
        await self._log_job({**job, "state": "retry_wait", "available_at": available_at}, "分支等待重试", "warning")

    async def release(self, job_id: str, lease_token: str) -> dict[str, Any]:
        """Graceful shutdown returns unfinished work to its durable queue."""
        async with self.database.transaction() as connection:
            await self._lease(connection, job_id, lease_token)
            now = self.clock()
            await connection.execute(
                "UPDATE memory_jobs SET state='queued',lease_token=NULL,lease_until=NULL,"
                "owner=NULL,available_at=?,updated_at=?,attempt=MAX(0,attempt-1) WHERE job_id=?",
                (now, now, job_id),
            )
            updated = await _get(connection, job_id)
        return await self._log_job(updated, "任务已交回队列")

    async def cancel(self, job_id: str) -> dict[str, Any]:
        async with self.database.transaction() as connection:
            job = await _get(connection, job_id)
            if job is None:
                raise KeyError(job_id)
            if job["state"] not in (*ACTIVE_STATES, "failed"):
                return job
            await connection.execute(
                "UPDATE memory_jobs SET state='cancelled',recovery_mode=NULL,lease_token=NULL,lease_until=NULL,"
                "owner=NULL,result_json=json_set(COALESCE(result_json,'{}'),'$.cancelled_from',state),"
                "updated_at=? WHERE job_id=?", (self.clock(), job_id),
            )
            updated = await _get(connection, job_id)
        return await self._log_job(updated, "任务已取消", "warning")

    async def delete(self, job_id: str, *, invalidate_nodes=None) -> dict[str, Any]:
        """Delete a stopped task and its unpublished rebuild, preserving committed memory.

        Claiming a queued task and deletion serialize through this transaction.
        Cache invalidation precedes commit: if it fails the task remains available
        for another deletion attempt; any evicted vectors can be regenerated.
        """
        async with self.database.transaction() as connection:
            job = await _get(connection, job_id)
            if job is None:
                raise KeyError(job_id)
            if job["state"] == "running":
                raise JobConflict("running tasks must be stopped before deletion")
            return await self._delete_stopped(connection, job, invalidate_nodes=invalidate_nodes)

    async def clear_completed(self, *, scope_key=None, invalidate_nodes=None) -> dict[str, Any]:
        """Clear all successful tasks in the selected scope in one transaction."""
        async with self.database.transaction() as connection:
            scope_clause = " AND scope_key=?" if scope_key is not None else ""
            cursor = await connection.execute(
                "SELECT job_id,scope_key,kind,hide_on_success FROM memory_jobs WHERE state='succeeded'"
                + scope_clause + " ORDER BY created_at,job_id",
                (scope_key,) if scope_key is not None else (),
            )
            jobs = await cursor.fetchall()
            result = {"deleted_count": len(jobs),
                      "automatic_count": sum(1 for job in jobs if job["hide_on_success"]),
                      "manual_count": sum(1 for job in jobs if not job["hide_on_success"]),
                      "removed_node_count": 0, "removed_generation_ids": []}
            for job in jobs:
                deleted = await self._delete_stopped(connection, job, invalidate_nodes=invalidate_nodes)
                result["removed_node_count"] += deleted["removed_node_count"]
                if deleted["removed_generation_id"] is not None:
                    result["removed_generation_ids"].append(deleted["removed_generation_id"])
            return result

    async def completed_counts(self, *, scope_key=None) -> dict[str, int]:
        """Return successful task counts, including rows hidden from the task list."""
        scope_clause = " AND scope_key=?" if scope_key is not None else ""
        row = await self.database.fetchone(
            "SELECT COUNT(*) AS total, "
            "COALESCE(SUM(CASE WHEN hide_on_success=1 THEN 1 ELSE 0 END), 0) AS automatic_count "
            "FROM memory_jobs WHERE state='succeeded'" + scope_clause,
            (scope_key,) if scope_key is not None else (),
        )
        total = int(row["total"] if row else 0)
        automatic = int(row["automatic_count"] if row else 0)
        return {"total": total, "automatic_count": automatic, "manual_count": total - automatic}

    async def _delete_stopped(self, connection, job, *, invalidate_nodes=None):
        """Shared cleanup for a task selected under the enclosing transaction."""
        result = await self._discard_work(connection, job, invalidate_nodes=invalidate_nodes, reprepare=False)
        for table in ("rebuild_receipts", "rebuild_runs", "short_term_receipts", "note_receipts"):
            await connection.execute(f"DELETE FROM {table} WHERE job_id=?", (job["job_id"],))
        await connection.execute("DELETE FROM memory_jobs WHERE job_id=?", (job["job_id"],))
        return {**result, "job_id": job["job_id"], "deleted": True}

    async def _discard_work(self, connection, job, *, invalidate_nodes, reprepare):
        """Release unpublished data before a fresh preparation or final deletion."""
        job_id = job["job_id"]
        cursor = await connection.execute(
            "SELECT g.generation_id,g.status FROM rebuild_runs r "
            "JOIN memory_generations g ON g.generation_id=r.target_generation_id WHERE r.job_id=?",
            (job_id,),
        )
        generation = await cursor.fetchone()
        removed_generation = None
        node_ids = []
        if generation is not None and (generation["status"] in {"building", "failed"} or (
            not reprepare and generation["status"] == "retired" and job["kind"] in {"profile_rebuild", "relationship_rebuild", "context_rebuild"}
        )):
            removed_generation = generation["generation_id"]
            cursor = await connection.execute(
                "SELECT node_id FROM memory_nodes WHERE generation_id=?", (removed_generation,),
            )
            node_ids = [row["node_id"] for row in await cursor.fetchall()]
        cursor = await connection.execute(
            "SELECT DISTINCT topic.value AS node_id FROM topic_rebuild_stage s "
            "JOIN topic_receipts r USING(receipt_key),json_each(r.result_json,'$.topic_ids') topic "
            "WHERE s.job_id=?", (job_id,))
        node_ids.extend(row["node_id"] for row in await cursor.fetchall())
        if invalidate_nodes is not None and node_ids:
            await invalidate_nodes(job["scope_key"], node_ids)
        await connection.execute("DELETE FROM memory_job_candidates WHERE job_id=?", (job_id,))
        await connection.execute(
            "DELETE FROM topic_receipts WHERE receipt_key IN (SELECT receipt_key FROM topic_rebuild_stage WHERE job_id=?)",
            (job_id,))
        if removed_generation is not None:
            # Publishing a context rebuild can leave a retired target with a receipt.
            await connection.execute("DELETE FROM rebuild_receipts WHERE job_id=?", (job_id,))
            await connection.execute("DELETE FROM rebuild_runs WHERE job_id=?", (job_id,))
            await connection.execute("DELETE FROM memory_generations WHERE generation_id=?", (removed_generation,))
        return {"removed_generation_id": removed_generation, "removed_node_count": len(node_ids)}

    async def recovery_actions(self, job):
        if job["state"] not in {"failed", "cancelled"}:
            return {"regenerate_window_available": False, "restart_required": False, "recovery_action": None}
        async with self.database.transaction(write=False) as connection:
            drafts, _ = await _recovery_drafts(connection, job)
        action = next((action for _, _, _, action in drafts if action != "resume"), "resume")
        fresh = _restart_required(job)
        if fresh:
            action = "reprepare"
        if job["error_code"] in {"note_revision_changed", "identity_confirmation_changed"}:
            action = "review"
        return {"regenerate_window_available": bool(drafts) and not fresh, "restart_required": fresh,
                "recovery_action": action}

    async def _prepare_retry(self, connection, job, *, regenerate=False):
        checkpoint = job["checkpoint"]
        drafts, committed = await _recovery_drafts(connection, job)
        selected = [draft for draft in drafts if draft[3] == "regenerate" or regenerate and draft[3] == "resize"]
        if regenerate and not selected:
            current = inspection_checkpoint(job)
            candidates = (current, current.get("window"), current.get("topic_window"),
                          current.get("topic_synthesis", {}).get("topic_window"))
            selected = [draft for draft in drafts if any(draft[1] is window for window in candidates)]
        for kind, window, code, _ in selected:
            candidate_id = await self._archive_candidate(connection, job["job_id"], window, code or "manual_regeneration")
            for field in ("raw", "canonical_raw", "validation_errors", "response_diagnostics", "model_request_digest"):
                window.pop(field, None)
            versions = {"short_term": SHORT_TERM_RECOVERY_VERSION, "topic": TOPIC_RECOVERY_VERSION,
                        "extraction": REGENERATION_VERSION}
            window["regeneration"] = {"version": versions[kind], "candidate_id": candidate_id}
            if kind == "topic":
                window["regeneration"]["refresh_feedback"] = True
        if regenerate and not selected:
            if committed:
                raise ValueError("candidate already committed; resume its receipt or indexing instead")
            raise ValueError("task has no saved candidate")
        for progress in checkpoint.get("section_progress", {}).values():
            if progress["state"] != "succeeded":
                progress.update(state="queued", attempt=1)
                for field in ("available_at", "error_code", "retry_exhausted", "consecutive_failures", "retry_progress"):
                    progress.pop(field, None)
        return _checkpoint_json(checkpoint)

    async def retry(self, job_id: str, *, regenerate=False) -> dict[str, Any]:
        async with self.database.transaction() as connection:
            job = await _get(connection, job_id)
            if job is None:
                raise KeyError(job_id)
            if (job["result"] or {}).get("replacement_job_id"):
                raise JobConflict("task has already been replaced")
            if job["state"] in ("queued", "retry_wait"):
                return job
            if job["state"] not in ("failed", "cancelled"):
                raise JobConflict("only failed or cancelled tasks can be retried")
            await assert_scope_available(connection, job["scope_key"], exclude_job_id=job_id)
            checkpoint = await self._prepare_retry(connection, job, regenerate=regenerate)
            now = self.clock()
            await connection.execute(
                "UPDATE memory_jobs SET state='queued',recovery_mode=NULL,hide_on_success=0,attempt=0,available_at=?,updated_at=?,"
                "error_type=NULL,error_code=NULL,result_json=NULL,checkpoint_json=?,attempts_json=? WHERE job_id=?",
                (now, now, checkpoint, _resume_history(job), job_id),
            )
            updated = await _get(connection, job_id)
        return await self._log_job(updated, "任务已重新排队")

    async def _archive_candidate(self, connection, job_id, window, error_code):
        if not isinstance(error_code, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", error_code):
            raise ValueError("error_code must be a short diagnostic identifier")
        window_json = _json(window)
        candidate_id = "candidate_" + hashlib.sha256(
            _json({"job_id": job_id, "window": window, "error_code": error_code}).encode()
        ).hexdigest()
        await connection.execute(
            "INSERT OR IGNORE INTO memory_job_candidates "
            "(candidate_id,job_id,window_json,error_code,created_at) VALUES (?,?,?,?,?)",
            (candidate_id, job_id, window_json, error_code, self.clock()),
        )
        return candidate_id

    async def archive_candidate(self, job_id, lease_token, window, error_code):
        """Preserve rejected model output under the same lease as its window."""
        async with self.database.transaction() as connection:
            await self._lease(connection, job_id, lease_token)
            return await self._archive_candidate(connection, job_id, window, error_code)

    async def list_candidates(self, job_id, *, limit=20, offset=0):
        if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or offset < 0:
            raise ValueError("invalid candidate pagination")
        rows = await self.database.fetchall(
            "SELECT * FROM memory_job_candidates WHERE job_id=? "
            "ORDER BY created_at DESC,candidate_id DESC LIMIT ? OFFSET ?", (job_id, limit, offset))
        result = []
        for row in rows:
            item = dict(row)
            item["window"] = json.loads(item.pop("window_json"))
            result.append(item)
        return result

    async def get_candidate(self, job_id, candidate_id):
        row = await self.database.fetchone(
            "SELECT * FROM memory_job_candidates WHERE job_id=? AND candidate_id=?", (job_id, candidate_id))
        if row is None:
            return None
        result = dict(row)
        result["window"] = json.loads(result.pop("window_json"))
        return result

    async def regenerate_window(self, job_id: str) -> dict[str, Any]:
        """Regenerate uncommitted drafts and retain completed work in every section."""
        return await self.retry(job_id, regenerate=True)

    async def get(self, job_id: str) -> dict[str, Any] | None:
        return _job(await self.database.fetchone(
            "SELECT * FROM memory_jobs WHERE job_id=?", (job_id,),
        ))

    async def list(
        self, *, scope_key: str | None = None, states: Iterable[str] | None = None,
        limit: int = 100, offset: int = 0, visible_only: bool = False,
    ) -> list[dict[str, Any]]:
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")
        if type(offset) is not int or offset < 0:
            raise ValueError("offset must be a nonnegative integer")
        conditions, parameters = [], []
        if visible_only:
            conditions.extend(("(state != 'succeeded' OR hide_on_success=0)",
                               "json_extract(result_json,'$.replacement_job_id') IS NULL"))
        if scope_key is not None:
            _text(scope_key, "scope_key")
            conditions.append("scope_key=?")
            parameters.append(scope_key)
        if states is not None:
            if isinstance(states, str):
                raise ValueError("states must be a sequence of task states")
            states = tuple(states)
            if not states or any(state not in JOB_STATES for state in states):
                raise ValueError("states must contain known task states")
            conditions.append(f"state IN ({','.join('?' for _ in states)})")
            parameters.extend(states)
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        rows = await self.database.fetchall(
            f"SELECT * FROM memory_jobs{where} ORDER BY created_at DESC,job_id LIMIT ? OFFSET ?",
            (*parameters, limit, offset),
        )
        return [_job(row) for row in rows]

    async def list_hidden_successes(
        self, *, scope_key: str | None = None, limit: int = 100, offset: int = 0,
    ) -> list[dict[str, Any]]:
        """List automatic successful tasks that are hidden from the active list."""
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")
        if type(offset) is not int or offset < 0:
            raise ValueError("offset must be a nonnegative integer")
        conditions = ["state='succeeded'", "hide_on_success=1",
                      "json_extract(result_json,'$.replacement_job_id') IS NULL"]
        parameters: list[Any] = []
        if scope_key is not None:
            _text(scope_key, "scope_key")
            conditions.append("scope_key=?")
            parameters.append(scope_key)
        rows = await self.database.fetchall(
            "SELECT * FROM memory_jobs WHERE " + " AND ".join(conditions)
            + " ORDER BY created_at DESC,job_id LIMIT ? OFFSET ?",
            (*parameters, limit, offset),
        )
        return [_job(row) for row in rows]

    async def latest_index(self, scope_key: str) -> dict[str, Any] | None:
        row = await self.database.fetchone(
            "SELECT state, updated_at, error_code, progress_done, progress_total, "
            "json_extract(result_json,'$.repaired') AS repaired FROM memory_jobs "
            "WHERE scope_key=? AND kind='reindex' ORDER BY created_at DESC, job_id DESC LIMIT 1", (scope_key,))
        return dict(row) if row else None

    async def availability(self, scope_key: str) -> dict[str, Any]:
        _text(scope_key, "scope_key")
        async with self.database.transaction() as connection:
            blockers = await _blockers(connection, scope_key)
            return {"available": not blockers, "scope_key": scope_key, "blocking_jobs": blockers}

    async def health(self) -> dict[str, Any]:
        rows = await self.database.fetchall("SELECT state,COUNT(*) count FROM memory_jobs GROUP BY state")
        counts = {state: 0 for state in JOB_STATES}
        counts.update({row["state"]: row["count"] for row in rows})
        outcomes = await self.database.fetchall("""
            WITH RECURSIVE failed_chains(job_id) AS (
                SELECT job_id FROM memory_jobs WHERE automatic_origin=1 AND (
                    state='failed' OR json_extract(result_json,'$.cancelled_from')='failed' OR
                    COALESCE(json_extract(attempts_json,'$[#-1].had_final_failure'), EXISTS(
                        SELECT 1 FROM json_each(attempts_json) WHERE json_extract(value,'$.state')='failed')))
                UNION
                SELECT json_extract(j.result_json,'$.replacement_job_id')
                FROM memory_jobs j JOIN failed_chains f ON j.job_id=f.job_id
                WHERE json_extract(j.result_json,'$.replacement_job_id') IS NOT NULL
            )
            SELECT CASE WHEN hide_on_success=0 OR state='cancelled' THEN 'manual'
                        WHEN state='succeeded' THEN 'recovered' ELSE 'pending' END AS outcome, COUNT(*) AS n
            FROM memory_jobs JOIN failed_chains USING(job_id)
            WHERE json_extract(result_json,'$.replacement_job_id') IS NULL GROUP BY outcome
        """)
        recovery = {"recovered": 0, "pending": 0, "manual": 0}
        recovery.update({row["outcome"]: row["n"] for row in outcomes})
        recovery["total"] = sum(recovery.values())
        longest = await self.database.fetchone("""
            SELECT * FROM (
                SELECT job_id, step, updated_at, NULL AS branch,
                       json_extract(attempts_json,'$[#-1].retry_progress[2]') AS reason,
                       json_extract(attempts_json,'$[#-1].streak') AS streak
                FROM memory_jobs WHERE state IN ('failed','retry_wait')
                    AND json_type(checkpoint_json,'$.section_progress') IS NULL
                UNION ALL
                SELECT j.job_id, json_extract(b.value,'$.step'), j.updated_at, b.key,
                       json_extract(b.value,'$.error_code'), json_extract(b.value,'$.consecutive_failures')
                FROM memory_jobs j, json_each(j.checkpoint_json,'$.section_progress') b
                WHERE j.state IN ('running','retry_wait','failed') AND
                    json_extract(b.value,'$.state') IN ('failed','retry_wait')
                    AND json_extract(b.value,'$.error_code')!='model_queue_timeout'
            ) WHERE streak>0 ORDER BY streak DESC, updated_at DESC, job_id LIMIT 1
        """)
        return {"counts": counts, "automatic_recovery": recovery,
                "longest_failure_streak": dict(longest) if longest else None}
