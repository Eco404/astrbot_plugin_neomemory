"""Persistent extraction cursors and lease-fenced, atomic rebuild activation."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from collections.abc import Iterable, Mapping
from typing import Any

from .job_store import assert_job_lease


async def _one(connection, query, values=()):
    cursor = await connection.execute(query, values)
    return await cursor.fetchone()


def _receipt(row) -> dict[str, Any] | None:
    if row is None:
        return None
    result = dict(row)
    result["result"] = json.loads(result.pop("result_json"))
    return result


def _json(value: Any, *, max_bytes: int | None = 64 * 1024) -> str:
    try:
        result = json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("pipeline state must be JSON data") from exc
    if max_bytes is not None and len(result.encode("utf-8")) > max_bytes:
        raise ValueError("pipeline state exceeds its size limit")
    return result


_PROFILE = ("user_profiles", "profile_id")
_RELATIONSHIP = ("relationship_states", "relationship_id")
CONTEXT_REBUILDS = {
    "profile_rebuild": (_PROFILE,),
    "relationship_rebuild": (_RELATIONSHIP,),
    "context_rebuild": (_PROFILE, _RELATIONSHIP),
}


async def _context_basis(connection, kind, generation_id):
    states = []
    for table, key in CONTEXT_REBUILDS[kind]:
        cursor = await connection.execute(
            f"SELECT {key},content_version,status,updated_at FROM {table} WHERE generation_id=? ORDER BY {key}",
            (generation_id,))
        states.append([tuple(row) for row in await cursor.fetchall()])
    return hashlib.sha256(json.dumps(states).encode()).hexdigest()


async def assert_rebuild_target(connection, job, space_id: str, generation_id: str):
    """The supplied job must already have passed assert_job_lease in this transaction."""
    if job["kind"] != "rebuild" and job["kind"] not in CONTEXT_REBUILDS:
        raise PermissionError("only a rebuild task may write a building generation")
    target = await _one(
        connection,
        "SELECT * FROM rebuild_runs WHERE job_id=? AND space_id=? AND target_generation_id=?",
        (job["job_id"], space_id, generation_id),
    )
    if target is None:
        raise PermissionError("building generation does not belong to this rebuild task")
    return target


class PipelineStore:
    def __init__(self, memory):
        self.memory = memory
        self.database = memory.database

    async def get_cursor(self, generation_id: str) -> int:
        row = await self.database.fetchone(
            "SELECT scan_cursor FROM derived_cursors WHERE generation_id=?", (generation_id,),
        )
        return row["scan_cursor"] if row else 0

    async def consumed(self, generation_id: str, source_ids: Iterable[str]) -> set[str]:
        if isinstance(source_ids, str):
            raise ValueError("source_ids must be a collection of source identities")
        identifiers = tuple(dict.fromkeys(source_ids))
        if any(not isinstance(item, str) or not item for item in identifiers):
            raise ValueError("source_ids must contain nonempty strings")
        result = set()
        for offset in range(0, len(identifiers), 500):
            batch = identifiers[offset:offset + 500]
            rows = await self.database.fetchall(
                "SELECT source_id FROM extraction_batch_sources WHERE generation_id=? "
                f"AND source_id IN ({','.join('?' for _ in batch)})", (generation_id, *batch),
            )
            result.update(row["source_id"] for row in rows)
        return result

    async def get_state(self, key: str, default=None):
        row = await self.database.fetchone("SELECT value_json FROM pipeline_state WHERE state_key=?", (key,))
        return json.loads(row["value_json"]) if row else default

    async def set_state(self, key: str, value: Any, *, max_bytes: int | None = 64 * 1024) -> None:
        if not isinstance(key, str) or not key or len(key) > 256:
            raise ValueError("pipeline state key must contain at most 256 characters")
        value_json = _json(value, max_bytes=max_bytes)
        await self.database.execute(
            "INSERT INTO pipeline_state(state_key,value_json,updated_at) VALUES (?,?,?) "
            "ON CONFLICT(state_key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at",
            (key, value_json, time.time()),
        )

    async def begin_rebuild(self, job, space_id: str, expected_generation_id: str) -> dict[str, Any]:
        async with self.database.transaction() as connection:
            current_job = await assert_job_lease(
                connection, job["job_id"], job["lease_token"], scope_key=space_id,
            )
            if current_job["kind"] != "rebuild" and current_job["kind"] not in CONTEXT_REBUILDS:
                raise PermissionError("only a rebuild task may create a rebuild generation")
            existing = await _one(connection, "SELECT * FROM rebuild_runs WHERE job_id=?", (job["job_id"],))
            if existing is not None:
                if existing["space_id"] != space_id or existing["parent_generation_id"] != expected_generation_id:
                    raise ValueError("rebuild task already belongs to another space or parent generation")
                target = await _one(
                    connection, "SELECT * FROM memory_generations WHERE generation_id=?",
                    (existing["target_generation_id"],),
                )
                return dict(target)
            parent = await _one(
                connection, "SELECT * FROM memory_generations WHERE space_id=? AND status='active'", (space_id,),
            )
            if parent is None or parent["generation_id"] != expected_generation_id:
                raise ValueError("active generation changed before rebuild creation")
            now = time.time()
            generation_id = "gen_rebuild_" + uuid.uuid4().hex
            await connection.execute(
                "INSERT INTO memory_generations(generation_id,space_id,parent_generation_id,status,"
                "reason,created_at,updated_at) VALUES (?,?,?,'building',?,?,?)",
                (generation_id, space_id, expected_generation_id, current_job["kind"], now, now),
            )
            await connection.execute(
                "INSERT INTO rebuild_runs(job_id,space_id,target_generation_id,parent_generation_id,created_at) "
                "VALUES (?,?,?,?,?)", (job["job_id"], space_id, generation_id, expected_generation_id, now),
            )
            await connection.execute("INSERT INTO derived_cursors(generation_id,scan_cursor) VALUES (?,0)", (generation_id,))
            return dict(await _one(connection, "SELECT * FROM memory_generations WHERE generation_id=?", (generation_id,)))

    async def get_rebuild_receipt(self, job_id: str) -> dict[str, Any] | None:
        return _receipt(await self.database.fetchone("SELECT * FROM rebuild_receipts WHERE job_id=?", (job_id,)))

    async def context_basis(self, kind, generation_id):
        async with self.database.transaction(write=False) as connection:
            return await _context_basis(connection, kind, generation_id)

    async def publish_context(self, job, target_id, result):
        """Move only this replay's component, versions and direct citations into the active generation."""
        space_id = job["scope_key"]
        async with self.database.transaction() as connection:
            lease = await assert_job_lease(connection, job["job_id"], job["lease_token"], scope_key=space_id)
            kind = lease["kind"]
            if kind not in CONTEXT_REBUILDS:
                raise PermissionError("not a context rebuild")
            run = await assert_rebuild_target(connection, lease, space_id, target_id)
            parent = run["parent_generation_id"]
            prior = await _one(connection, "SELECT * FROM rebuild_receipts WHERE job_id=?", (job["job_id"],))
            if prior:
                return _receipt(prior)["result"]
            active = await _one(connection, "SELECT generation_id FROM memory_generations WHERE space_id=? AND status='active'", (space_id,))
            if (active is None or active["generation_id"] != parent
                    or await _context_basis(connection, kind, parent) != lease["payload"]["context_basis"]):
                raise ValueError("context rebuild target changed")
            moved = {}
            for table, _ in CONTEXT_REBUILDS[kind]:
                await connection.execute(f"DELETE FROM {table} WHERE generation_id=?", (parent,))
                update = await connection.execute(f"UPDATE {table} SET generation_id=? WHERE generation_id=?", (parent, target_id))
                moved[table] = update.rowcount
            # Replies cite original source IDs directly. The replay's temporary batches
            # must not count as ordinary extraction consumption in the active generation.
            await connection.execute(
                "UPDATE memory_derivations SET batch_id=NULL WHERE batch_id IN "
                "(SELECT batch_id FROM extraction_batches WHERE generation_id=?)", (target_id,))
            await connection.execute("DELETE FROM extraction_batches WHERE generation_id=?", (target_id,))
            await connection.execute("DELETE FROM derived_cursors WHERE generation_id=?", (target_id,))
            now = time.time()
            await connection.execute("UPDATE memory_generations SET status='retired',retired_at=?,updated_at=? WHERE generation_id=?",
                                     (now, now, target_id))
            result = {**result, "generation_id": parent, "rebuilt_count": sum(moved.values()), "counts": moved}
            await connection.execute(
                "INSERT INTO rebuild_receipts(job_id,space_id,target_generation_id,parent_generation_id,result_json,committed_at) "
                "VALUES (?,?,?,?,?,?)", (job["job_id"], space_id, target_id, parent, _json(result), now))
            return result

    async def activate_rebuild(
        self, job, space_id: str, target_generation_id: str, expected_generation_id: str, *,
        result: Mapping[str, Any], invalidate_nodes=None,
    ) -> dict[str, Any]:
        if not isinstance(result, Mapping):
            raise ValueError("rebuild result must be an object")
        result_json = _json(dict(result))
        async with self.database.transaction() as connection:
            current_job = await assert_job_lease(
                connection, job["job_id"], job["lease_token"], scope_key=space_id,
            )
            if current_job["kind"] != "rebuild":
                raise PermissionError("only a full rebuild may replace a generation")
            target_run = await assert_rebuild_target(connection, current_job, space_id, target_generation_id)
            if target_run["parent_generation_id"] != expected_generation_id:
                raise ValueError("rebuild parent differs from its reserved generation")
            previous = await _one(connection, "SELECT * FROM rebuild_receipts WHERE job_id=?", (job["job_id"],))
            if previous is not None:
                return {**_receipt(previous), "replayed": True}
            if current_job["checkpoint"].get("phase") != "switch":
                raise ValueError("rebuild must checkpoint the switch phase before activation")
            active = await _one(
                connection, "SELECT * FROM memory_generations WHERE space_id=? AND status='active'", (space_id,),
            )
            if active is None or active["generation_id"] != expected_generation_id:
                raise ValueError("active generation changed before rebuild activation")
            target = await _one(
                connection, "SELECT * FROM memory_generations WHERE generation_id=? AND space_id=?",
                (target_generation_id, space_id),
            )
            if target is None or target["status"] != "building":
                raise ValueError("rebuild target is no longer a building generation")
            now = time.time()
            await connection.execute(
                "UPDATE memory_generations SET status='retired',retired_at=?,updated_at=? WHERE generation_id=?",
                (now, now, expected_generation_id),
            )
            await connection.execute(
                "UPDATE memory_generations SET status='active',activated_at=?,updated_at=? WHERE generation_id=?",
                (now, now, target_generation_id),
            )
            await self._purge_retired_content(connection, space_id, invalidate_nodes=invalidate_nodes)
            await connection.execute(
                "INSERT INTO rebuild_receipts(job_id,space_id,target_generation_id,parent_generation_id,result_json,committed_at) "
                "VALUES (?,?,?,?,?,?)",
                (job["job_id"], space_id, target_generation_id, expected_generation_id, result_json, now),
            )
            return {**_receipt(await _one(connection, "SELECT * FROM rebuild_receipts WHERE job_id=?", (job["job_id"],))), "replayed": False}

    async def _purge_retired_content(self, connection, space_id, *, invalidate_nodes):
        """Keep generation/task identities, but no retired derived content or versions."""
        retired = "SELECT generation_id FROM memory_generations WHERE space_id=? AND status='retired'"
        if invalidate_nodes is not None:
            cursor = await connection.execute(
                f"SELECT node_id FROM memory_nodes WHERE generation_id IN ({retired})", (space_id,),
            )
            node_ids = [row["node_id"] for row in await cursor.fetchall()]
            if node_ids:
                await invalidate_nodes(space_id, node_ids)
        # Foreign-key cascades remove versions, citations, edges, coverage,
        # reinforcement, topic dependencies and short-term receipts as well.
        for table in (
            "memory_nodes", "user_profiles", "relationship_states", "short_term_states",
            "extraction_batches", "topic_receipts", "governance_receipts", "derived_cursors",
        ):
            await connection.execute(f"DELETE FROM {table} WHERE generation_id IN ({retired})", (space_id,))
