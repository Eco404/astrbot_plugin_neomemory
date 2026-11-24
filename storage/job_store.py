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
    from ..core.extraction_apply import digest
    from ..core.extraction_regeneration import VERSION as REGENERATION_VERSION
    from ..core.candidate_feedback import SHORT_TERM_RECOVERY_VERSION, TOPIC_RECOVERY_VERSION
except ImportError:
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
    return result


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
        *, max_attempts: int = 2, hide_on_success: bool = False, replaces=None,
    ) -> dict[str, Any]:
        _text(kind, "kind", limit=80)
        _text(scope_key, "scope_key")
        _text(idempotency_key, "idempotency_key", limit=512)
        if type(max_attempts) is not int or not 1 <= max_attempts <= 20:
            raise ValueError("max_attempts must be between 1 and 20")
        payload_json = _json(payload)
        async with self.database.transaction() as connection:
            cursor = await connection.execute(
                "SELECT * FROM memory_jobs WHERE idempotency_key=?", (idempotency_key,),
            )
            existing = await cursor.fetchone()
            if existing is not None:
                if (existing["kind"] != kind or existing["scope_key"] != scope_key
                        or existing["payload_json"] != payload_json):
                    raise JobConflict("idempotency key already describes another task")
                return _job(existing)
            if kind == "reindex" and hide_on_success and not replaces:
                cursor = await connection.execute(
                    "SELECT * FROM memory_jobs WHERE kind='reindex' AND state='failed' AND automatic_origin=1 "
                    "AND scope_key=? AND json_extract(payload_json,'$.expected_generation_id')=? "
                    "ORDER BY updated_at DESC LIMIT 1", (scope_key, payload.get("expected_generation_id")))
                if pending := await cursor.fetchone():
                    return _job(pending)
            job_id = f"job_{uuid.uuid4().hex}"
            automatic_origin = hide_on_success
            now = self.clock()
            if replaces:
                previous = await self._lease(connection, replaces["job_id"], replaces["lease_token"])
                if (previous["kind"], previous["scope_key"], previous["recovery_mode"]) != (kind, scope_key, "fresh"):
                    raise JobConflict("replacement does not match the recovering task")
                automatic_origin = previous["automatic_origin"]
                hide_on_success = previous["hide_on_success"]
                await connection.execute(
                    "UPDATE memory_jobs SET state='cancelled',recovery_mode=NULL,updated_at=?,result_json=?,"
                    "lease_token=NULL,lease_until=NULL,owner=NULL WHERE job_id=?",
                    (now, _json({"replacement_job_id": job_id}), previous["job_id"]))
            await assert_scope_available(connection, scope_key)
            await connection.execute(
                "INSERT INTO memory_jobs (job_id,kind,scope_key,idempotency_key,state,"
                "payload_json,max_attempts,available_at,created_at,updated_at,hide_on_success,automatic_origin) "
                "VALUES (?,?,?,?,'queued',?,?,?,?,?,?,?)",
                (job_id, kind, scope_key, idempotency_key, payload_json,
                 max_attempts, now, now, now, hide_on_success, automatic_origin),
            )
            return await _get(connection, job_id)

    async def claim(self, owner: str, *, lease_seconds: float = 30) -> dict[str, Any] | None:
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
                "SELECT j.job_id,j.state FROM memory_jobs j WHERE available_at<=? AND ("
                "state IN ('queued','retry_wait') OR (state='failed' AND automatic_origin=1 AND NOT EXISTS ("
                "SELECT 1 FROM memory_jobs active WHERE active.state IN ('running','queued','retry_wait') "
                "AND (active.scope_key=j.scope_key OR active.scope_key='*' OR j.scope_key='*')))) "
                "ORDER BY available_at,created_at,job_id LIMIT 1", (now,),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            job_id = row["job_id"]
            if row["state"] == "failed":
                previous = await _get(connection, job_id)
                checkpoint = await self._prepare_retry(connection, previous)
                await connection.execute(
                    "UPDATE memory_jobs SET attempt=0,recovery_mode='resume',checkpoint_json=? WHERE job_id=?",
                    (checkpoint, job_id))
            await connection.execute(
                "UPDATE memory_jobs SET state='running',owner=?,lease_token=?,lease_until=?,"
                "attempt=attempt+1,error_type=NULL,error_code=NULL,updated_at=? WHERE job_id=?",
                (owner, uuid.uuid4().hex, now + lease_seconds, now, job_id),
            )
            return await _get(connection, job_id)

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
                "(?>progress_done OR ? IN ('model','topic_model','short_term_model')) "
                "THEN NULL ELSE recovery_mode END,checkpoint_json=?,step=?,progress_done=?,"
                "progress_total=?,error_type=NULL,error_code=NULL,updated_at=? WHERE job_id=?",
                (progress_done, step, checkpoint_json, step, progress_done, progress_total, self.clock(), job_id),
            )
            return await _get(connection, job_id)

    async def finish(self, job_id: str, lease_token: str, *, result=None) -> dict[str, Any]:
        result_json = _json({} if result is None else result)
        async with self.database.transaction() as connection:
            await self._lease(connection, job_id, lease_token)
            await connection.execute(
                "UPDATE memory_jobs SET state='succeeded',lease_token=NULL,lease_until=NULL,"
                "owner=NULL,result_json=?,checkpoint_json='{}',step='complete',recovery_mode=NULL,"
                "progress_done=COALESCE(progress_total,progress_done),error_type=NULL,"
                "error_code=NULL,updated_at=? WHERE job_id=?", (result_json, self.clock(), job_id),
            )
            return await _get(connection, job_id)

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
            delay = self.next_retry_delay(job["attempt"], job["max_attempts"],
                                         retryable=retryable, model_network=model_network)
            mode = job["recovery_mode"]
            if mode == "resume" and not retryable and job["attempt"] == 1:
                state, mode, available = "queued", "fresh", now
            elif delay is not None:
                state, available = "retry_wait", now + (backoff_seconds or delay)
                if mode == "resume":
                    mode = None
            else:
                state, mode = "failed", None
                available = now + AUTO_RECOVERY_SECONDS if job["automatic_origin"] else now
            await connection.execute(
                "UPDATE memory_jobs SET state=?,recovery_mode=?,attempt=?,lease_token=NULL,lease_until=NULL,owner=NULL,"
                "error_type=?,error_code=?,available_at=?,updated_at=? WHERE job_id=?",
                (state, mode, 0 if state == "queued" else job["attempt"], error_type, error_code, available, now, job_id),
            )
            return await _get(connection, job_id)

    async def defer_until(self, job_id, lease_token, available_at):
        """Park a task until one of its independently budgeted sections can run."""
        async with self.database.transaction() as connection:
            await self._lease(connection, job_id, lease_token)
            await connection.execute(
                "UPDATE memory_jobs SET state='retry_wait',available_at=?,updated_at=?,"
                "attempt=MAX(0,attempt-1),owner=NULL,lease_token=NULL,lease_until=NULL WHERE job_id=?",
                (available_at, self.clock(), job_id),
            )

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
            return await _get(connection, job_id)

    async def cancel(self, job_id: str) -> dict[str, Any]:
        async with self.database.transaction() as connection:
            job = await _get(connection, job_id)
            if job is None:
                raise KeyError(job_id)
            if job["state"] in (*ACTIVE_STATES, "failed"):
                await connection.execute(
                    "UPDATE memory_jobs SET state='cancelled',recovery_mode=NULL,lease_token=NULL,lease_until=NULL,"
                    "owner=NULL,result_json=json_set(COALESCE(result_json,'{}'),'$.cancelled_from',state),"
                    "updated_at=? WHERE job_id=?", (self.clock(), job_id),
                )
            return await _get(connection, job_id)

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
                "SELECT job_id,scope_key,kind FROM memory_jobs WHERE state='succeeded'"
                + scope_clause + " ORDER BY created_at,job_id",
                (scope_key,) if scope_key is not None else (),
            )
            jobs = await cursor.fetchall()
            result = {"deleted_count": len(jobs), "removed_node_count": 0, "removed_generation_ids": []}
            for job in jobs:
                deleted = await self._delete_stopped(connection, job, invalidate_nodes=invalidate_nodes)
                result["removed_node_count"] += deleted["removed_node_count"]
                if deleted["removed_generation_id"] is not None:
                    result["removed_generation_ids"].append(deleted["removed_generation_id"])
            return result

    async def _delete_stopped(self, connection, job, *, invalidate_nodes=None):
        """Shared cleanup for a task selected under the enclosing transaction."""
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
            generation["status"] == "retired" and job["kind"] in {"profile_rebuild", "relationship_rebuild", "context_rebuild"}
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
        for table in ("memory_job_candidates", "rebuild_receipts", "rebuild_runs",
                      "short_term_receipts", "note_receipts"):
            await connection.execute(f"DELETE FROM {table} WHERE job_id=?", (job_id,))
        if removed_generation is not None:
            await connection.execute(
                "DELETE FROM memory_generations WHERE generation_id=?", (removed_generation,),
            )
        await connection.execute("DELETE FROM memory_jobs WHERE job_id=?", (job_id,))
        return {"job_id": job_id, "deleted": True, "removed_generation_id": removed_generation,
                "removed_node_count": len(node_ids)}

    async def recovery_actions(self, job):
        if job["state"] not in {"failed", "cancelled"}:
            return {"regenerate_window_available": False, "restart_required": False, "recovery_action": None}
        regenerate = False
        action = "resume"
        async with self.database.transaction(write=False) as connection:
            for kind, window, code, receipt in _drafts(job):
                if not await (await connection.execute(*receipt)).fetchone():
                    regenerate = True
                    candidate_action = _draft_action(kind, window, code, job)
                    if candidate_action != "resume":
                        action = candidate_action
        fresh = _restart_required(job)
        if fresh:
            action = "reprepare"
        if job["error_code"] in {"note_revision_changed", "identity_confirmation_changed"}:
            action = "review"
        return {"regenerate_window_available": regenerate and not fresh, "restart_required": fresh,
                "recovery_action": action}

    async def _prepare_retry(self, connection, job, *, regenerate=False):
        checkpoint = job["checkpoint"]
        replaced = committed = False
        for kind, window, code, receipt in _drafts(job):
            if await (await connection.execute(*receipt)).fetchone():
                committed = True
                continue
            if not regenerate and _draft_action(kind, window, code, job) != "regenerate":
                continue
            candidate_id = await self._archive_candidate(connection, job["job_id"], window, code or "manual_regeneration")
            for field in ("raw", "canonical_raw", "validation_errors", "response_diagnostics", "model_request_digest"):
                window.pop(field, None)
            versions = {"short_term": SHORT_TERM_RECOVERY_VERSION, "topic": TOPIC_RECOVERY_VERSION,
                        "extraction": REGENERATION_VERSION}
            window["regeneration"] = {"version": versions[kind], "candidate_id": candidate_id}
            if kind == "topic":
                window["regeneration"]["refresh_feedback"] = True
            replaced = True
        if regenerate and not replaced:
            if committed:
                raise ValueError("candidate already committed; resume its receipt or indexing instead")
            raise ValueError("task has no saved candidate")
        for progress in checkpoint.get("section_progress", {}).values():
            if progress["state"] != "succeeded":
                progress.update(state="queued", attempt=1)
                for field in ("available_at", "error_code", "retry_exhausted", "consecutive_failures"):
                    progress.pop(field, None)
        return _checkpoint_json(checkpoint)

    async def retry(self, job_id: str, *, regenerate=False) -> dict[str, Any]:
        async with self.database.transaction() as connection:
            job = await _get(connection, job_id)
            if job is None:
                raise KeyError(job_id)
            if job["state"] in ("queued", "retry_wait"):
                return job
            if job["state"] not in ("failed", "cancelled"):
                raise JobConflict("only failed or cancelled tasks can be retried")
            await assert_scope_available(connection, job["scope_key"], exclude_job_id=job_id)
            checkpoint = await self._prepare_retry(connection, job, regenerate=regenerate)
            now = self.clock()
            await connection.execute(
                "UPDATE memory_jobs SET state='queued',recovery_mode=NULL,hide_on_success=0,attempt=0,available_at=?,updated_at=?,"
                "error_type=NULL,error_code=NULL,result_json=NULL,checkpoint_json=? WHERE job_id=?",
                (now, now, checkpoint, job_id),
            )
            return await _get(connection, job_id)

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
            conditions.append("(state != 'succeeded' OR hide_on_success=0)")
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
        return {"counts": counts}
