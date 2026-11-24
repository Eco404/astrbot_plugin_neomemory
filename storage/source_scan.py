"""Bounded source discovery and replay in original database insertion order.

The cursor is SQLite's source-record rowid, not a timestamp. Sources are currently
append-only: do not VACUUM or rewrite this table while cursors or replay tasks are
in use. A future source-cleanup migration must introduce a durable insertion ID
before changing that invariant. Stable public source IDs remain unchanged.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiosqlite

try:
    from ..core.memory_spaces import MemorySpaceSpec, group_space, private_space
except ImportError:
    from core.memory_spaces import MemorySpaceSpec, group_space, private_space

from .source_store import SourceStore


@dataclass(frozen=True, slots=True)
class DiscoveryPage:
    spaces: tuple[MemorySpaceSpec, ...]
    next_cursor: int
    has_more: bool


@dataclass(frozen=True, slots=True)
class SourcePage:
    records: list[dict[str, Any]]
    next_cursor: int
    has_more: bool


def _page_arguments(after: int | None, limit: int) -> tuple[int, int]:
    if after is None:
        after = 0
    if type(after) is not int or after < 0:
        raise ValueError("source cursor must be a nonnegative integer")
    if type(limit) is not int or not 1 <= limit <= 200:
        raise ValueError("source page limit must be between 1 and 200")
    return after, limit


def _spec(space: MemorySpaceSpec | Mapping[str, Any]) -> MemorySpaceSpec:
    if isinstance(space, MemorySpaceSpec):
        return space
    return MemorySpaceSpec(**{
        key: space.get(key)
        for key in ("space_type", "persona_id", "logical_user_id", "platform_instance_id", "group_id")
    })


def _record_spec(record: Mapping[str, Any]) -> MemorySpaceSpec | None:
    if (record["source_type"] != "conversation" or record["identity_pending"]
            or record["actor_kind"] not in {"user", "agent"}
            or not (record["persona_id"] or "").strip()):
        return None
    persona_id = record["persona_id"]
    message_type = record["message_type"].casefold()
    if message_type == "groupmessage":
        if record["platform_instance_id"] in {"", "unknown"} or not (record["group_id"] or "").strip():
            return None
        return group_space(persona_id, record["platform_instance_id"], record["group_id"])
    if message_type != "friendmessage":
        return None
    owner = (record["actor_logical_user_id"] if record["actor_kind"] == "user"
             else record["conversation_logical_user_id"])
    return private_space(persona_id, owner) if (owner or "").strip() else None


class SourceScanner:
    """Scan without materializing all conversations or consulting derived data.

    Each private record retains its captured logical-user identity. Later account
    bindings do not silently reassign old records. Agent ownership uses the same
    triggering-user/preceding-user rule as SourceStore.get_records.
    """

    def __init__(self, source_store: SourceStore) -> None:
        self.store = source_store

    async def discover_spaces(self, *, after: int | None = 0, limit: int = 100) -> DiscoveryPage:
        """Discover spaces from at most ``limit`` raw sources; advance even over skipped rows.

        A space can recur in later pages. Consumers should use idempotent space
        creation rather than holding a growing all-space deduplication set.
        """
        cursor, limit = _page_arguments(after, limit)
        rows = await self.store.database.fetchall(
            "SELECT rowid AS scan_cursor,source_id FROM source_records WHERE rowid>? ORDER BY rowid LIMIT ?",
            (cursor, limit + 1),
        )
        selected = rows[:limit]
        spaces: dict[str, MemorySpaceSpec] = {}
        if selected:
            records = await self.store.get_records(tuple(row["source_id"] for row in selected))
            for record in records:
                spec = _record_spec(record)
                if spec:
                    spaces[spec.scope_key] = spec
            owners = [spec.logical_user_id for spec in spaces.values() if spec.space_type == 'private']
            if owners:
                placeholders = ','.join('?' for _ in owners)
                deleted = await self.store.database.fetchall(
                    f"SELECT logical_user_id FROM logical_users WHERE status='deleted' "
                    f"AND logical_user_id IN ({placeholders})", owners)
                retired = {row['logical_user_id'] for row in deleted}
                spaces = {key: spec for key, spec in spaces.items() if spec.logical_user_id not in retired}
        return DiscoveryPage(tuple(spaces.values()), int(selected[-1]["scan_cursor"]) if selected else cursor,
                             len(rows) > limit)

    def _scope_query(self, space: MemorySpaceSpec | Mapping[str, Any],
                     highwater: Mapping[str, int] | None = None) -> tuple[str, tuple[Any, ...]]:
        spec = _spec(space)
        parameters: list[Any] = []
        prefix = ""
        bounds = ""
        owner_bounds = ""
        if highwater is not None:
            if any(not isinstance(stream, str) or not stream.strip() or type(value) is not int or value < 0
                   for stream, value in highwater.items()):
                raise ValueError("highwater must map nonempty stream IDs to nonnegative sequence numbers")
            prefix = "WITH bounds AS (SELECT key AS stream_id,CAST(value AS INTEGER) AS max_sequence FROM json_each(?)), "
            parameters.append(json.dumps(dict(highwater), separators=(",", ":")))
            bounds = "AND r.sequence_no <= COALESCE((SELECT max_sequence FROM bounds WHERE stream_id=r.stream_id),-1)"
            # A late trigger message cannot introduce an older, previously
            # unowned agent record into a frozen replay range.
            owner_bounds = "AND u.sequence_no <= COALESCE((SELECT max_sequence FROM bounds WHERE stream_id=u.stream_id),-1)"
        else:
            prefix = "WITH "
        if spec.space_type == "private":
            owner = f"""CASE WHEN r.actor_kind='user' THEN r.actor_logical_user_id
            WHEN json_extract(r.metadata_json,'$.delivered_proactive') THEN json_extract(r.metadata_json,'$.recipient_logical_user_id')
            WHEN json_extract(r.metadata_json,'$.trigger_message_id') IS NOT NULL
            THEN (SELECT u.actor_logical_user_id FROM source_records u
                WHERE u.stream_id=r.stream_id AND u.persona_id=r.persona_id AND u.actor_kind='user'
                AND u.message_id=json_extract(r.metadata_json,'$.trigger_message_id') {owner_bounds} LIMIT 1)
            ELSE (SELECT u.actor_logical_user_id FROM source_records u
                WHERE u.stream_id=r.stream_id AND u.persona_id=r.persona_id AND u.actor_kind='user'
                AND u.sequence_no<=r.sequence_no {owner_bounds} ORDER BY u.sequence_no DESC LIMIT 1) END"""
            scope = (f"LOWER(s.message_type)='friendmessage' AND ({owner})=? "
                     "AND NOT EXISTS (SELECT 1 FROM logical_users WHERE logical_user_id=? AND status='deleted')")
            scope_parameters = [spec.logical_user_id, spec.logical_user_id]
        else:
            scope = "LOWER(s.message_type)='groupmessage' AND s.platform_instance_id=? AND s.group_id=?"
            scope_parameters = [spec.platform_instance_id, spec.group_id]
        parameters.extend([spec.persona_id, *scope_parameters])
        query = prefix + f"""eligible AS (
            SELECT r.rowid AS scan_cursor,r.source_id,r.stream_id,r.sequence_no,r.observed_at,
                COALESCE(json_extract(r.metadata_json,'$.replay_only'),0) AS replay_only,
                COALESCE(json_extract(r.metadata_json,'$.delivered_proactive'),0) AS delivered
            FROM source_records r JOIN conversation_streams s USING(stream_id)
            WHERE r.identity_pending=0 AND r.source_type='conversation'
            AND r.actor_kind IN ('user','agent') AND r.persona_id=?
            AND {scope} {bounds}) """
        return query, tuple(parameters)

    async def pending_counts(self, space: Mapping[str, Any], memory_path: Path) -> tuple[int, int]:
        """Return the total pending sources and the largest single-stream count."""
        scope, parameters = self._scope_query(space)
        sql = scope + """
            , pending_streams AS (
            SELECT COUNT(*) AS n
            FROM derived.memory_generations g
            LEFT JOIN derived.derived_cursors c ON c.generation_id=g.generation_id
            CROSS JOIN eligible e
            WHERE e.replay_only=0 AND g.space_id=? AND g.status='active'
              AND e.scan_cursor>COALESCE(c.scan_cursor,0)
              AND NOT EXISTS (
                SELECT 1 FROM derived.extraction_batch_sources b
                WHERE b.generation_id=g.generation_id AND b.source_id=e.source_id
              )
            GROUP BY e.stream_id)
            SELECT COALESCE(SUM(n),0), COALESCE(MAX(n),0) FROM pending_streams"""
        async with aiosqlite.connect(self.store.path.resolve().as_uri() + "?mode=ro", uri=True) as connection:
            await connection.execute("ATTACH DATABASE ? AS derived", (memory_path.resolve().as_uri() + "?mode=ro",))
            async with connection.execute(sql, (*parameters, space["space_id"])) as cursor:
                total, largest = await cursor.fetchone()
                return total, largest

    async def highwater(self, space: MemorySpaceSpec | Mapping[str, Any]) -> dict[str, int]:
        """Freeze replay per-stream sequence bounds in one SQLite statement."""
        query, parameters = self._scope_query(space)
        rows = await self.store.database.fetchall(
            query + "SELECT stream_id,MAX(sequence_no) AS max_sequence FROM eligible GROUP BY stream_id",
            parameters,
        )
        return {row["stream_id"]: int(row["max_sequence"]) for row in rows}

    async def count(self, space: MemorySpaceSpec | Mapping[str, Any], *,
                    highwater: Mapping[str, int] | None = None) -> int:
        """Count the same eligible range used by page, without loading raw text."""
        query, parameters = self._scope_query(space, highwater)
        row = await self.store.database.fetchone(query + "SELECT COUNT(*) AS count FROM eligible", parameters)
        return int(row["count"])

    async def page(self, space: MemorySpaceSpec | Mapping[str, Any], *,
                   highwater: Mapping[str, int] | None = None, after: int | None = 0,
                   limit: int = 200) -> SourcePage:
        """Return a bounded, ordered page; ``{}`` is an empty frozen snapshot.

        Record scan_cursor values allow committing just a consumed page prefix.
        New records with old observed_at values still follow the insertion cursor
        and cannot enter a range frozen by sequence highwater.
        """
        cursor, limit = _page_arguments(after, limit)
        query, parameters = self._scope_query(space, highwater)
        live_filter = "AND replay_only=0" if highwater is None else ""
        rows = await self.store.database.fetchall(
            query + f"SELECT scan_cursor,source_id FROM eligible WHERE scan_cursor>? {live_filter} ORDER BY scan_cursor LIMIT ?",
            (*parameters, cursor, limit + 1),
        )
        selected = rows[:limit]
        records = await self.store.get_records(tuple(row["source_id"] for row in selected)) if selected else []
        for record, row in zip(records, selected, strict=True):
            record["scan_cursor"] = int(row["scan_cursor"])
        return SourcePage(records, int(selected[-1]["scan_cursor"]) if selected else cursor, len(rows) > limit)

    async def has_deliveries(self, space, highwater):
        query, args = self._scope_query(space, highwater)
        return bool(await self.store.database.fetchone(query + "SELECT 1 FROM eligible WHERE delivered LIMIT 1", args))

    async def replay_page(self, space, highwater, offset, limit=200):
        """Keep native insertion order and interleave delivered segments by send time.

        The page offset is local to a frozen rebuild. Each source's scan_cursor
        is the highest safe live insertion cursor after consuming that prefix.
        """
        query, args = self._scope_query(space, highwater)
        rows = await self.store.database.fetchall(query + """
            , ordered AS (
                SELECT *, CASE WHEN delivered THEN observed_at ELSE MAX(CASE WHEN NOT delivered THEN observed_at END)
                    OVER (ORDER BY scan_cursor ROWS UNBOUNDED PRECEDING) END AS replay_time FROM eligible
            ) SELECT source_id,
                 COALESCE(MIN(scan_cursor) OVER (ORDER BY replay_time,scan_cursor ROWS BETWEEN 1 FOLLOWING
                     AND UNBOUNDED FOLLOWING)-1, MAX(scan_cursor) OVER ()) AS safe_cursor
              FROM ordered ORDER BY replay_time,scan_cursor LIMIT ? OFFSET ?
        """, (*args, limit, offset))
        if not rows:
            return SourcePage([], 0, False)
        records = await self.store.get_records(tuple(row["source_id"] for row in rows))
        for record, row in zip(records, rows, strict=True):
            record["scan_cursor"] = row["safe_cursor"]
        return SourcePage(records, rows[-1]["safe_cursor"], len(rows) == limit)

    async def pending_stream(self, space, generation_id, stream_id, memory_path):
        """Read one stream independently while other streams still have pending tails."""
        query, args = self._scope_query(space)
        sql = query + """
            SELECT e.source_id,e.scan_cursor FROM eligible e WHERE e.stream_id=? AND e.replay_only=0
            AND e.scan_cursor>COALESCE((SELECT scan_cursor FROM derived.derived_cursors WHERE generation_id=?),0)
            AND NOT EXISTS (SELECT 1 FROM derived.extraction_batch_sources b
                WHERE b.source_id=e.source_id AND b.generation_id=?) ORDER BY e.scan_cursor LIMIT 200"""
        async with aiosqlite.connect(self.store.path.resolve().as_uri() + "?mode=ro", uri=True) as connection:
            await connection.execute("ATTACH DATABASE ? AS derived", (memory_path.resolve().as_uri() + "?mode=ro",))
            async with connection.execute(sql, (*args, stream_id, generation_id, generation_id)) as cursor:
                refs = await cursor.fetchall()
        records = await self.store.get_records(tuple(r[0] for r in refs)) if refs else []
        # Committing a stream can leap over other streams. The per-source receipts
        # advance this stream; the shared cursor must stay at its safe prefix.
        return [dict(record, scan_cursor=0) for record in records]
