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
    from ..core.extraction_regeneration import VERSION as REGENERATION_VERSION
    from ..core.candidate_feedback import SHORT_TERM_RECOVERY_VERSION, TOPIC_RECOVERY_VERSION
except ImportError:
    from core.extraction_regeneration import VERSION as REGENERATION_VERSION
    from core.candidate_feedback import SHORT_TERM_RECOVERY_VERSION, TOPIC_RECOVERY_VERSION

ACTIVE_STATES = ("queued", "running", "retry_wait")
JOB_STATES = (*ACTIVE_STATES, "failed", "succeeded", "cancelled")
MAX_JSON_BYTES = 1024 * 1024


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


def _manual_retry_checkpoint(job):
    checkpoint = job["checkpoint"]
    error_code = job.get("error_code")
    candidate_kind = job.get("kind") in {
        "extract", "rebuild", "profile_rebuild", "relationship_rebuild", "context_rebuild",
        "topic_consolidate",
    }
    clear_candidate = bool(
        candidate_kind and (error_code == "invalid_topic_output"
        or error_code in {"invalid_output", "unknown_reference", "output_incomplete", "prompt_budget_exceeded"}
        or (isinstance(error_code, str) and (error_code.startswith("expected_")
                                             or error_code.startswith("coverage_"))))
    )

    def has_candidate_diagnostics(container):
        if not isinstance(container, dict):
            return False
        if container.get("validation_errors") or container.get("errors"):
            return True
        return any(has_candidate_diagnostics(container.get(key)) for key in ("window", "topic_window", "topic_synthesis"))

    clear_candidate = clear_candidate or (candidate_kind and has_candidate_diagnostics(checkpoint))

    def reset_candidate(container):
        """Discard only the failed model draft before a manual resume.

        A normal retry resumes committed windows, but a terminal candidate
        validation failure must not reuse the same raw response and exhausted
        correction counter.  The frozen input and progress remain intact.
        """
        if not isinstance(container, dict):
            return
        for key in ("raw", "canonical_raw", "validation_errors", "model_request_digest",
                    "response_diagnostics"):
            container.pop(key, None)
        container.pop("regeneration", None)
        if "corrections" in container:
            container["corrections"] = 0
        if "correction_attempt" in container:
            container["correction_attempt"] = 0

    def reset_failed_lane(lane):
        if not isinstance(lane, dict):
            return
        # Rebuilds keep the active topic synthesis nested in the lane.
        for candidate in (
            lane,
            lane.get("window"),
            lane.get("topic_window"),
            lane.get("topic_synthesis", {}).get("topic_window")
            if isinstance(lane.get("topic_synthesis"), dict) else None,
        ):
            if clear_candidate and isinstance(candidate, dict) and "raw" in candidate:
                reset_candidate(candidate)

    if checkpoint.get("sections"):
        failed_section = checkpoint.get("failed_section")
        if failed_section in checkpoint["sections"]:
            reset_failed_lane(checkpoint["sections"][failed_section])
        else:
            for lane in checkpoint["sections"].values():
                if lane.get("state") != "succeeded":
                    reset_failed_lane(lane)
    else:
        reset_failed_lane(checkpoint)
        reset_failed_lane(checkpoint.get("window"))
        reset_failed_lane(checkpoint.get("topic_window"))
        synthesis = checkpoint.get("topic_synthesis")
        if isinstance(synthesis, dict):
            reset_failed_lane(synthesis)
    for lane in checkpoint.get("section_progress", {}).values():
        if lane["state"] != "succeeded":
            lane.update(state="queued", attempt=1)
            for field in ("available_at", "error_code", "retry_exhausted"):
                lane.pop(field, None)
    return _checkpoint_json(checkpoint)


def _candidate_windows(checkpoint):
    """Yield failed model windows that should remain inspectable on retry."""
    if not isinstance(checkpoint, dict):
        return
    seen = set()

    def visit(container):
        if not isinstance(container, dict):
            return
        if "raw" in container and id(container) not in seen:
            seen.add(id(container))
            yield container
        for key in ("window", "topic_window"):
            child = container.get(key)
            if isinstance(child, dict):
                yield from visit(child)
        synthesis = container.get("topic_synthesis")
        if isinstance(synthesis, dict):
            yield from visit(synthesis)

    sections = checkpoint.get("sections")
    if isinstance(sections, dict):
        failed_section = checkpoint.get("failed_section")
        lanes = ([sections[failed_section]] if failed_section in sections
                 else [lane for lane in sections.values() if lane.get("state") != "succeeded"])
        for lane in lanes:
            yield from visit(lane)
    else:
        yield from visit(checkpoint)


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

    async def submit(
        self, kind: str, scope_key: str, payload: Mapping[str, Any], idempotency_key: str,
        *, max_attempts: int = 2, hide_on_success: bool = False,
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
            await assert_scope_available(connection, scope_key)
            now = self.clock()
            job_id = f"job_{uuid.uuid4().hex}"
            await connection.execute(
                "INSERT INTO memory_jobs (job_id,kind,scope_key,idempotency_key,state,"
                "payload_json,max_attempts,available_at,created_at,updated_at,hide_on_success) "
                "VALUES (?,?,?,?,'queued',?,?,?,?,?,?)",
                (job_id, kind, scope_key, idempotency_key, payload_json,
                 max_attempts, now, now, now, hide_on_success),
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
                "SELECT job_id FROM memory_jobs WHERE state IN ('queued','retry_wait') "
                "AND available_at<=? ORDER BY available_at,created_at,job_id LIMIT 1", (now,),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            job_id = row["job_id"]
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
                "UPDATE memory_jobs SET checkpoint_json=?,step=?,progress_done=?,"
                "progress_total=?,error_type=NULL,error_code=NULL,updated_at=? WHERE job_id=?",
                (checkpoint_json, step, progress_done, progress_total, self.clock(), job_id),
            )
            return await _get(connection, job_id)

    async def finish(self, job_id: str, lease_token: str, *, result=None) -> dict[str, Any]:
        result_json = _json({} if result is None else result)
        async with self.database.transaction() as connection:
            await self._lease(connection, job_id, lease_token)
            await connection.execute(
                "UPDATE memory_jobs SET state='succeeded',lease_token=NULL,lease_until=NULL,"
                "owner=NULL,result_json=?,checkpoint_json='{}',step='complete',"
                "progress_done=COALESCE(progress_total,progress_done),error_type=NULL,"
                "error_code=NULL,updated_at=? WHERE job_id=?", (result_json, self.clock(), job_id),
            )
            return await _get(connection, job_id)

    @staticmethod
    def retry_delay(attempt):
        return min(300, 5 * 2 ** min(10, max(0, attempt - 1)))

    async def fail(
        self, job_id: str, lease_token: str, *, error_type: str, error_code: str,
        retryable: bool = False, backoff_seconds: float | None = None,
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
            will_retry = retryable and job["attempt"] < job["max_attempts"]
            now = self.clock()
            delay = backoff_seconds if backoff_seconds is not None else self.retry_delay(job["attempt"])
            await connection.execute(
                "UPDATE memory_jobs SET state=?,lease_token=NULL,lease_until=NULL,owner=NULL,"
                "error_type=?,error_code=?,available_at=?,updated_at=? WHERE job_id=?",
                ("retry_wait" if will_retry else "failed", error_type, error_code,
                 now + delay if will_retry else now, now, job_id),
            )
            return await _get(connection, job_id)

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
                    "UPDATE memory_jobs SET state='cancelled',lease_token=NULL,lease_until=NULL,"
                    "owner=NULL,updated_at=? WHERE job_id=?", (self.clock(), job_id),
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
                "SELECT job_id,scope_key FROM memory_jobs WHERE state='succeeded'"
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

    async def retry(self, job_id: str) -> dict[str, Any]:
        """Explicitly resume a failed/cancelled task, after a fresh admission check."""
        async with self.database.transaction() as connection:
            job = await _get(connection, job_id)
            if job is None:
                raise KeyError(job_id)
            if job["state"] in ("queued", "retry_wait"):
                return job  # Repeated UI clicks cannot create a second execution.
            if job["state"] not in ("failed", "cancelled"):
                raise JobConflict("only failed or cancelled tasks can be retried")
            await assert_scope_available(connection, job["scope_key"], exclude_job_id=job_id)
            for window in _candidate_windows(job["checkpoint"]):
                await self._archive_candidate(
                    connection, job_id, window, job["error_code"] or "manual_retry"
                )
            now = self.clock()
            await connection.execute(
                "UPDATE memory_jobs SET state='queued',hide_on_success=0,attempt=0,available_at=?,updated_at=?,"
                "error_type=NULL,error_code=NULL,result_json=NULL,checkpoint_json=? WHERE job_id=?",
                (now, now, _manual_retry_checkpoint(job), job_id),
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
        """Discard only the rejected response, preserving input and replay progress."""
        async with self.database.transaction() as connection:
            job = await _get(connection, job_id)
            if job is None:
                raise KeyError(job_id)
            if job["state"] in ("queued", "retry_wait"):
                return job
            if job["state"] not in ("failed", "cancelled"):
                raise JobConflict("only failed or cancelled tasks can regenerate a window")
            inspected = inspection_checkpoint(job)
            window = inspected.get("window")
            topic = inspected.get("topic_window") or inspected.get("topic_synthesis", {}).get("topic_window")
            if topic and "raw" in topic:
                await assert_scope_available(connection, job["scope_key"], exclude_job_id=job_id)
                cursor = await connection.execute("SELECT 1 FROM topic_receipts WHERE receipt_key=?", (topic["receipt_key"],))
                if await cursor.fetchone():
                    raise ValueError("topic already committed; resume its indexing instead")
                candidate_id = await self._archive_candidate(connection, job_id, topic, job["error_code"] or "manual_regeneration")
                topic.pop("raw")
                topic.pop("response_diagnostics", None)
                topic["regeneration"] = {"version": TOPIC_RECOVERY_VERSION, "candidate_id": candidate_id,
                                         "refresh_feedback": True}
                now = self.clock()
                await connection.execute(
                    "UPDATE memory_jobs SET state='queued',hide_on_success=0,attempt=0,available_at=?,updated_at=?,"
                    "checkpoint_json=?,step='topic_prepare',error_type=NULL,error_code=NULL,result_json=NULL WHERE job_id=?",
                    (now, now, _manual_retry_checkpoint(job), job_id))
                return await _get(connection, job_id)
            if job["kind"] == "short_term" and "raw" in job["checkpoint"]:
                await assert_scope_available(connection, job["scope_key"], exclude_job_id=job_id)
                cursor = await connection.execute("SELECT 1 FROM short_term_receipts WHERE job_id=?", (job_id,))
                if await cursor.fetchone():
                    raise ValueError("summary already committed; resume its receipt instead")
                summary = job["checkpoint"]
                candidate_id = await self._archive_candidate(connection, job_id, summary, job["error_code"] or "manual_regeneration")
                summary.pop("raw")
                summary.pop("response_diagnostics", None)
                summary["regeneration"] = {"version": SHORT_TERM_RECOVERY_VERSION, "candidate_id": candidate_id}
                now = self.clock()
                await connection.execute(
                    "UPDATE memory_jobs SET state='queued',hide_on_success=0,attempt=0,available_at=?,updated_at=?,"
                    "checkpoint_json=?,step='short_term_prepared',error_type=NULL,error_code=NULL,result_json=NULL WHERE job_id=?",
                    (now, now, _json(summary), job_id))
                return await _get(connection, job_id)
            if job["kind"] not in ("extract", "rebuild", "profile_rebuild", "relationship_rebuild", "context_rebuild") or not window or "raw" not in window:
                raise ValueError("task has no saved extraction candidate")
            await assert_scope_available(connection, job["scope_key"], exclude_job_id=job_id)
            candidate_id = await self._archive_candidate(connection, job_id, window, job["error_code"] or "manual_regeneration")
            for key in ("raw", "canonical_raw", "validation_errors", "model_request_digest", "response_diagnostics"):
                window.pop(key, None)
            window["regeneration"] = {"version": REGENERATION_VERSION, "candidate_id": candidate_id}
            now = self.clock()
            await connection.execute(
                "UPDATE memory_jobs SET state='queued',hide_on_success=0,attempt=0,available_at=?,updated_at=?,"
                "checkpoint_json=?,step='prepared',error_type=NULL,error_code=NULL,result_json=NULL "
                "WHERE job_id=?", (now, now, _manual_retry_checkpoint(job), job_id),
            )
            return await _get(connection, job_id)

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
