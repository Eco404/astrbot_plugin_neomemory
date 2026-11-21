"""Scoped recall snapshots and durable, bounded reinforcement bookkeeping.

The reinforcement ledger guarantees deduplication for 24 hours, including across
process restarts and interleaved passive/tool calls. This is an explicit retry
window, not a permanent audit history. It lives with memory, not in cache.db.
"""

from __future__ import annotations

import json
import math
import time
from collections.abc import Iterable, Mapping
from typing import Any

try:
    from ..core.strength import StrengthPolicy, calculate_strength, recall_increment
except ImportError:
    from core.strength import StrengthPolicy, calculate_strength, recall_increment

MAX_SNAPSHOT_NODES = 2000
RECALL_LEDGER_RETENTION_SECONDS = 24 * 3600

_NODE_SELECT = """
    SELECT n.*, n.metadata_json AS node_metadata_json,
           v.node_version_id, v.version_no AS loaded_version_no,
           v.content_text, v.metadata_json AS content_metadata_json
    FROM memory_nodes n
    JOIN memory_node_versions v
      ON v.node_id=n.node_id AND v.version_no=n.content_version
"""
_FRESH_NODE = "COALESCE(json_extract(n.metadata_json,'$.summary_stale'),0)=0"


def _snapshot_limit(limit: int) -> int:
    if type(limit) is not int or not 1 <= limit <= MAX_SNAPSHOT_NODES:
        raise ValueError(f"limit must be between 1 and {MAX_SNAPSHOT_NODES}")
    return limit


async def _bounded_node_rows(connection, space_id, generation_id, limit):
    """One ordering and freshness predicate for indexing and recall selection."""
    cursor = await connection.execute(
        _NODE_SELECT + f"""
        WHERE n.space_id=? AND n.generation_id=?
          AND n.status IN ('active','historical') AND {_FRESH_NODE}
        ORDER BY n.importance DESC,n.updated_at DESC,n.node_id LIMIT ?
        """, (space_id, generation_id, limit + 1),
    )
    return await cursor.fetchall()


def _finite_nonnegative(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a finite nonnegative number")
    try:
        valid = math.isfinite(value) and value >= 0
    except OverflowError:
        valid = False
    if not valid:
        raise ValueError(f"{name} must be a finite nonnegative number")
    return float(value)


def _statuses(include_historical: bool) -> tuple[str, ...]:
    if type(include_historical) is not bool:
        raise ValueError("include_historical must be a boolean")
    return ("active", "historical") if include_historical else ("active",)


def _placeholders(values: Iterable[Any]) -> str:
    return ",".join("?" for _ in values)


def _versions(values: Mapping[str, int]) -> dict[str, int]:
    if len(values) > MAX_SNAPSHOT_NODES:
        raise ValueError(f"recall accepts at most {MAX_SNAPSHOT_NODES} nodes")
    result = dict(values)
    for node_id, version in result.items():
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a nonempty string")
        if type(version) is not int or version < 1:
            raise ValueError("content_version must be a positive integer")
    return result


class RecallStoreMixin:
    """MemoryStore methods kept separate from general CRUD.

    A snapshot or revalidation holds one short SQLite transaction. No embedding,
    model, or caller code runs inside it. Revalidation is a final point-in-time
    check; a future write cannot be prevented once the transaction has returned.
    """

    async def index_nodes(
        self,
        space_id: str,
        *,
        generation_id: str | None = None,
        include_historical: bool = True,
        limit: int = MAX_SNAPSHOT_NODES,
    ) -> dict[str, Any]:
        """Select one bounded vector-index input using the recall ordering.

        A rebuild may explicitly select its building generation. An explicit
        target must belong to this space and be active/building; retired and
        failed generations cannot silently produce an apparently valid index.
        The cap selects a shared active/historical universe before applying the
        passive status filter, so recall never targets an unindexed tail solely
        because history occupied some index positions. No vectors or graph
        relations are loaded here.
        """
        statuses = _statuses(include_historical)
        limit = _snapshot_limit(limit)
        if generation_id is not None and (not isinstance(generation_id, str) or not generation_id.strip()):
            raise ValueError("generation_id must be a nonempty string")
        result = {"space_id": space_id, "generation_id": None, "activity_round": 0, "nodes": [], "truncated": False}
        async with self.database.transaction(write=False) as connection:
            cursor = await connection.execute(
                "SELECT generation_id,activity_round FROM memory_generations WHERE space_id=? AND "
                + ("status='active'" if generation_id is None else "generation_id=? AND status IN ('active','building')"),
                (space_id,) if generation_id is None else (space_id, generation_id),
            )
            generation = await cursor.fetchone()
            if generation is None:
                if generation_id is not None:
                    raise KeyError("index generation is absent, outside this space, or not active/building")
                return result
            rows = await _bounded_node_rows(connection, space_id, generation["generation_id"], limit)
            result.update(generation_id=generation["generation_id"], activity_round=generation["activity_round"],
                          nodes=[self._node_from_row(row) for row in rows[:limit] if row["status"] in statuses],
                          truncated=len(rows) > limit)
        return result

    async def retrieval_snapshot(
        self,
        space_id: str,
        *,
        include_historical: bool = False,
        limit: int = MAX_SNAPSHOT_NODES,
    ) -> dict[str, Any]:
        statuses = _statuses(include_historical)
        limit = _snapshot_limit(limit)
        result: dict[str, Any] = {
            "space_id": space_id,
            "generation_id": None,
            "activity_round": 0,
            "nodes": [],
            "edges": [],
            "coverage": [],
            "truncated": False,
        }
        async with self.database.transaction(write=False) as connection:
            cursor = await connection.execute(
                "SELECT generation_id,activity_round FROM memory_generations "
                "WHERE space_id=? AND status='active'", (space_id,),
            )
            generation = await cursor.fetchone()
            if generation is None:
                return result
            generation_id = generation["generation_id"]
            result.update(
                generation_id=generation_id,
                activity_round=generation["activity_round"],
            )
            rows = await _bounded_node_rows(connection, space_id, generation_id, limit)
            result["truncated"] = len(rows) > limit
            selected = [row for row in rows[:limit] if row["status"] in statuses]
            result["nodes"] = [self._node_from_row(row) for row in selected]
            node_ids = [row["node_id"] for row in selected]
            if not node_ids:
                return result
            placeholders = _placeholders(node_ids)
            cursor = await connection.execute(
                f"""SELECT * FROM memory_edges
                WHERE space_id=? AND generation_id=?
                  AND source_node_id IN ({placeholders})
                  AND target_node_id IN ({placeholders})
                ORDER BY edge_id""",
                (space_id, generation_id, *node_ids, *node_ids),
            )
            for row in await cursor.fetchall():
                edge = dict(row)
                edge["metadata"] = json.loads(edge.pop("metadata_json"))
                result["edges"].append(edge)
            cursor = await connection.execute(
                f"""SELECT c.* FROM node_coverage c
                JOIN memory_nodes e ON e.node_id=c.event_node_id
                JOIN memory_nodes a ON a.node_id=c.atomic_node_id
                WHERE e.space_id=? AND a.space_id=?
                  AND e.generation_id=? AND a.generation_id=?
                  AND e.node_type='event' AND a.node_type='atomic'
                  AND c.event_content_version=e.content_version
                  AND c.atomic_content_version=a.content_version
                  AND c.event_node_id IN ({placeholders})
                  AND c.atomic_node_id IN ({placeholders})
                ORDER BY c.event_node_id,c.atomic_node_id""",
                (space_id, space_id, generation_id, generation_id, *node_ids, *node_ids),
            )
            result["coverage"] = [dict(row) for row in await cursor.fetchall()]
        return result

    async def revalidate_recall(
        self,
        space_id: str,
        generation_id: str,
        versions: Mapping[str, int],
        *,
        include_historical: bool = False,
    ) -> list[dict[str, Any]]:
        statuses = _statuses(include_historical)
        versions = _versions(versions)
        if not versions:
            return []
        async with self.database.transaction(write=False) as connection:
            cursor = await connection.execute(
                _NODE_SELECT + f"""
                JOIN memory_generations g ON g.generation_id=n.generation_id
                WHERE n.space_id=? AND n.generation_id=?
                  AND g.space_id=? AND g.status='active'
                  AND n.status IN ({_placeholders(statuses)})
                  AND {_FRESH_NODE}
                  AND n.node_id IN ({_placeholders(versions)})
                """,
                (space_id, generation_id, space_id, *statuses, *versions),
            )
            valid = {
                row["node_id"]: self._node_from_row(row)
                for row in await cursor.fetchall()
                if versions[row["node_id"]] == row["content_version"]
            }
            return [valid[node_id] for node_id in versions if node_id in valid]

    async def reinforce_recall(
        self,
        space_id: str,
        generation_id: str,
        turn_id: str,
        hits: Iterable[Mapping[str, Any]],
        *,
        active: bool = False,
        now: float | None = None,
        cooldown_seconds: float | None = None,
        policy: StrengthPolicy = StrengthPolicy(),
        min_strength: float = 0.1,
    ) -> list[dict[str, Any]]:
        """Credit only nodes the caller actually injected or returned.

        The caller supplies stable turn IDs shared by passive and active recall.
        Within 24h a node/content-version gets one decision per turn, including
        decisions with zero gain due to cooldown, budget or the aggregate cap.
        Content updates start a fresh per-version cooldown. All calls within a
        turn/generation share the configured budget (at most 0.12); direct hits
        precede neighbors. An active return can independently awaken a current
        low-strength node without resetting or spending ordinary gain quota.
        """
        if type(active) is not bool:
            raise ValueError("active must be a boolean")
        if not isinstance(turn_id, str) or not turn_id.strip() or len(turn_id) > 512:
            raise ValueError("turn_id must contain 1 to 512 characters")
        min_strength = _finite_nonnegative(min_strength, "min_strength")
        if min_strength > 1:
            raise ValueError("min_strength must not exceed one")
        now = _finite_nonnegative(time.time() if now is None else now, "now")
        cooldown_seconds = _finite_nonnegative(
            policy.cooldown_seconds if cooldown_seconds is None else cooldown_seconds,
            "cooldown_seconds",
        )
        if cooldown_seconds > RECALL_LEDGER_RETENTION_SECONDS:
            raise ValueError("cooldown_seconds cannot exceed the 24h ledger window")
        unique: dict[str, dict[str, Any]] = {}
        for index, item in enumerate(hits):
            if index >= MAX_SNAPSHOT_NODES:
                raise ValueError(f"recall accepts at most {MAX_SNAPSHOT_NODES} hits")
            if set(item) != {"node_id", "content_version", "direct"}:
                raise ValueError("recall hits require node_id, content_version and direct")
            _versions({item["node_id"]: item["content_version"]})
            if type(item["direct"]) is not bool:
                raise ValueError("direct must be a boolean")
            previous = unique.get(item["node_id"])
            if previous and previous["content_version"] != item["content_version"]:
                raise ValueError("one recall cannot contain conflicting node versions")
            unique[item["node_id"]] = {
                **item, "direct": item["direct"] or bool(previous and previous["direct"]),
            }
        if not unique:
            return []
        # Deduplicate before sorting so repeated neighbors cannot consume budget.
        ordered = sorted(unique.values(), key=lambda item: not item["direct"])
        credited: list[dict[str, Any]] = []
        async with self.database.transaction() as connection:
            cursor = await connection.execute(
                "SELECT activity_round FROM memory_generations "
                "WHERE space_id=? AND generation_id=? AND status='active'",
                (space_id, generation_id),
            )
            generation = await cursor.fetchone()
            if generation is None:
                return []
            await connection.execute(
                "DELETE FROM recall_reinforcements WHERE recorded_at < ?",
                (now - RECALL_LEDGER_RETENTION_SECONDS,),
            )
            cursor = await connection.execute(
                "SELECT COALESCE(SUM(increment),0) FROM recall_reinforcements "
                "WHERE generation_id=? AND turn_id=?",
                (generation_id, turn_id),
            )
            remaining = max(0.0, policy.max_turn_reinforcement - (await cursor.fetchone())[0])
            for hit in ordered:
                node_id, version = hit["node_id"], hit["content_version"]
                cursor = await connection.execute(
                    "SELECT reinforcement,last_recalled_at,strength,importance,status,"
                    "strength_reset_at,strength_reset_round FROM memory_nodes n "
                    "WHERE node_id=? AND space_id=? AND generation_id=? "
                    "AND content_version=? AND (status='active' OR (? AND status='historical')) "
                    f"AND {_FRESH_NODE}",
                    (node_id, space_id, generation_id, version, active),
                )
                node = await cursor.fetchone()
                if node is None:
                    continue
                cursor = await connection.execute(
                    "SELECT 1 FROM recall_reinforcements "
                    "WHERE node_id=? AND content_version=? AND turn_id=?",
                    (node_id, version, turn_id),
                )
                if await cursor.fetchone() is not None:
                    continue
                cursor = await connection.execute(
                    "SELECT MAX(recorded_at) FROM recall_reinforcements "
                    "WHERE node_id=? AND content_version=? AND increment>0",
                    (node_id, version),
                )
                last_positive = (await cursor.fetchone())[0]
                # 和排序使用同一策略，在事务内读取最新强化量，重复命中逐步减益。
                current_strength = calculate_strength(
                    node["strength"], importance=node["importance"],
                    elapsed_seconds=max(0.0, now - node["strength_reset_at"]),
                    elapsed_rounds=max(0, generation["activity_round"] - node["strength_reset_round"]),
                    reinforcement=node["reinforcement"], policy=policy,
                ).value
                awakened = active and node["status"] == "active" and current_strength < min_strength
                baseline = node["strength"]
                reset_at, reset_round = node["strength_reset_at"], node["strength_reset_round"]
                if awakened:
                    # 唤醒只恢复被动资格，保留已耗普通强化额度，不把历史事实恢复为当前。
                    target = min(1.0, min_strength + policy.wake_margin)
                    effective_reinforcement = min(policy.max_reinforcement, node["reinforcement"])
                    baseline = max(0.0, target - effective_reinforcement)
                    reset_at, reset_round = now, generation["activity_round"]
                    increment = 0.0
                else:
                    weight = recall_increment(
                        current_strength, direct=hit["direct"], active=active, policy=policy,
                    )
                    increment = min(
                        weight, remaining, max(0.0, policy.max_reinforcement - node["reinforcement"]),
                    )
                    if last_positive is not None and now - last_positive < cooldown_seconds:
                        increment = 0.0
                # Avoid leaving floating point crumbs when a cap was exhausted.
                if increment < 1e-12:
                    increment = 0.0
                await connection.execute(
                    "INSERT INTO recall_reinforcements "
                    "(node_id,content_version,generation_id,turn_id,recorded_at,increment) "
                    "VALUES (?,?,?,?,?,?)",
                    (node_id, version, generation_id, turn_id, now, increment),
                )
                await connection.execute(
                    "UPDATE memory_nodes SET reinforcement=?,last_recalled_at=?,strength=?,"
                    "strength_reset_at=?,strength_reset_round=? WHERE node_id=?",
                    (node["reinforcement"] + increment, max(now, node["last_recalled_at"] or 0),
                     baseline, reset_at, reset_round, node_id),
                )
                remaining = max(0.0, remaining - increment)
                if increment or awakened:
                    credited.append({
                        "node_id": node_id,
                        "content_version": version,
                        "increment": increment,
                        "awakened": awakened,
                        "reinforcement": node["reinforcement"] + increment,
                    })
        return credited
