"""Small scoped context snapshots, validated together with recalled nodes."""

from __future__ import annotations

import json
from typing import Any

from .recall_store import _FRESH_NODE, _NODE_SELECT, _statuses


class ContextStore:
    """Keep independently updated context in one generation at injection time.

    No provider or source-store calls occur inside these short transactions.
    A final validation rejects updated/deactivated entries and old generations;
    it is a point-in-time check, not a lock across the host's model request.
    """

    def __init__(self, memory):
        self.memory = memory

    async def snapshot(self, space_id: str, *, logical_user_id: str | None,
                       stream_id: str, host_conversation_id: str | None,
                       conversation_key: str | None = None) -> dict[str, Any]:
        result = {"space_id": space_id, "generation_id": None,
                  "logical_user_id": logical_user_id, "stream_id": stream_id,
                  "host_conversation_id": host_conversation_id,
                  "conversation_key": conversation_key, "profile": None,
                  "relationship": None, "short_term": None, "active_notes": 0}
        async with self.memory.database.transaction(write=False) as connection:
            cursor = await connection.execute(
                "SELECT s.*, g.generation_id FROM memory_spaces s "
                "LEFT JOIN memory_generations g ON g.space_id=s.space_id AND g.status='active' "
                "WHERE s.space_id=?", (space_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                return result
            generation_id = row["generation_id"]
            result["generation_id"] = generation_id
            if row["space_type"] == "private" and logical_user_id != row["logical_user_id"]:
                # Fail closed even if a caller accidentally supplies another
                # user's profile selector for a private space.
                return result
            if generation_id and logical_user_id:
                cursor = await connection.execute(
                    "SELECT * FROM user_profiles WHERE generation_id=? AND space_id=? "
                    "AND logical_user_id=? AND status='active'",
                    (generation_id, space_id, logical_user_id),
                )
                profile = await cursor.fetchone()
                if profile:
                    result["profile"] = dict(profile)
                    result["profile"]["tags"] = json.loads(result["profile"].pop("tags_json"))
                if row["space_type"] == "private":
                    cursor = await connection.execute(
                        "SELECT * FROM relationship_states WHERE generation_id=? AND space_id=? "
                        "AND logical_user_id=? AND status='active'",
                        (generation_id, space_id, logical_user_id),
                    )
                    relationship = await cursor.fetchone()
                    if relationship:
                        result["relationship"] = dict(relationship)
                        result["relationship"]["state"] = json.loads(result["relationship"].pop("state_json"))
            if generation_id and host_conversation_id and conversation_key:
                cursor = await connection.execute(
                    "SELECT * FROM short_term_states WHERE generation_id=? AND space_id=? "
                    "AND conversation_id=? AND status='active'",
                    (generation_id, space_id, conversation_key),
                )
                short = await cursor.fetchone()
                if short:
                    item = dict(short)
                    item["coverage"] = json.loads(item.pop("coverage_json"))
                    if (item["coverage"].get("stream_id") == stream_id
                            and item["coverage"].get("host_conversation_id") == host_conversation_id):
                        result["short_term"] = item
            cursor = await connection.execute(
                "SELECT COUNT(*) AS count FROM notes WHERE space_id=? AND status='active'", (space_id,),
            )
            result["active_notes"] = (await cursor.fetchone())["count"]
        return result

    async def revalidate(self, snapshot: dict[str, Any], *, recalled_generation_id=None,
                         recalled_hits=(), include_historical=False) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        statuses = _statuses(include_historical)
        result = {**snapshot, "profile": None, "relationship": None, "short_term": None,
                  "active_notes": 0, "recall_coverage": []}
        valid_hits = []
        async with self.memory.database.transaction(write=False) as connection:
            cursor = await connection.execute(
                "SELECT generation_id FROM memory_generations WHERE space_id=? AND status='active'",
                (snapshot["space_id"],),
            )
            row = await cursor.fetchone()
            generation_id = row["generation_id"] if row else None
            if generation_id != snapshot["generation_id"]:
                return result, []
            for name, table, key in (("profile", "user_profiles", "profile_id"),
                                     ("relationship", "relationship_states", "relationship_id"),
                                     ("short_term", "short_term_states", "state_id")):
                item = snapshot.get(name)
                if not item:
                    continue
                cursor = await connection.execute(
                    f"SELECT content_version,updated_at,status FROM {table} "
                    f"WHERE {key}=? AND space_id=? AND generation_id=?",
                    (item[key], snapshot["space_id"], generation_id),
                )
                current = await cursor.fetchone()
                if (current and current["status"] == "active"
                        and current["content_version"] == item["content_version"]
                        and current["updated_at"] == item["updated_at"]):
                    result[name] = item
            cursor = await connection.execute(
                "SELECT COUNT(*) AS count FROM notes WHERE space_id=? AND status='active'",
                (snapshot["space_id"],),
            )
            result["active_notes"] = (await cursor.fetchone())["count"]
            if recalled_hits and recalled_generation_id == generation_id:
                versions = {item["node_id"]: item["content_version"] for item in recalled_hits}
                placeholders = ",".join("?" for _ in versions)
                cursor = await connection.execute(
                    _NODE_SELECT + f" WHERE n.space_id=? AND n.generation_id=? "
                    f"AND n.status IN ({','.join('?' for _ in statuses)}) "
                    f"AND {_FRESH_NODE} AND n.node_id IN ({placeholders})",
                    (snapshot["space_id"], generation_id, *statuses, *versions),
                )
                valid = {row["node_id"]: self.memory._node_from_row(row)
                         for row in await cursor.fetchall()
                         if versions[row["node_id"]] == row["content_version"]}
                valid_hits = [{**item, **valid[item["node_id"]]} for item in recalled_hits
                              if item["node_id"] in valid]
                if valid_hits:
                    placeholders = ",".join("?" for _ in valid_hits)
                    node_ids = [item["node_id"] for item in valid_hits]
                    cursor = await connection.execute(
                        "SELECT c.* FROM node_coverage c "
                        "JOIN memory_nodes e ON e.node_id=c.event_node_id "
                        "JOIN memory_nodes a ON a.node_id=c.atomic_node_id "
                        "WHERE e.space_id=? AND a.space_id=? AND e.generation_id=? AND a.generation_id=? "
                        "AND e.node_type='event' AND a.node_type='atomic' "
                        "AND c.event_content_version=e.content_version AND c.atomic_content_version=a.content_version "
                        f"AND c.event_node_id IN ({placeholders}) AND c.atomic_node_id IN ({placeholders}) "
                        "ORDER BY c.event_node_id,c.atomic_node_id",
                        (snapshot["space_id"], snapshot["space_id"], generation_id, generation_id, *node_ids, *node_ids),
                    )
                    result["recall_coverage"] = [dict(row) for row in await cursor.fetchall()]
        return result, valid_hits
