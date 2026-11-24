"""Isolated working set and resumable window results for a topic-only rebuild."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager

from .job_store import assert_job_lease
from .memory_store import MemoryStore
from .recall_store import _NODE_SELECT


async def _rows(connection, sql, values=()):
    cursor = await connection.execute(sql, values)
    return [dict(row) for row in await cursor.fetchall()]


async def _restore_rows(connection, table, rows):
    if rows:
        columns = tuple(rows[0])
        await connection.executemany(
            f"INSERT OR REPLACE INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
            [tuple(row[key] for key in columns) for row in rows])


async def _load(stage, live, space_id, generation_id, job):
    db = stage.database
    # SQLite copies just this space's current bodies and membership in one read snapshot.
    # URI query parameters are not enabled on this connection, so an attached
    # ``file:///...?mode=ro`` value would be treated as a literal filename.
    await db.execute("ATTACH DATABASE ? AS origin", (str(live.path.resolve()),))
    try:
        async with db.transaction(write=False) as connection:
            await connection.execute("INSERT INTO memory_spaces SELECT * FROM origin.memory_spaces WHERE space_id=?", (space_id,))
            await connection.execute(
                "INSERT INTO memory_generations(generation_id,space_id,status,created_at,activated_at,updated_at,activity_round) "
                "SELECT generation_id,space_id,status,created_at,activated_at,updated_at,activity_round "
                "FROM origin.memory_generations WHERE generation_id=? AND space_id=? AND status='active'",
                (generation_id, space_id))
            await connection.execute(
                "INSERT INTO memory_nodes SELECT n.* FROM origin.memory_nodes n "
                "JOIN main.memory_generations g USING(generation_id) WHERE n.node_type<>'topic' AND n.status='active'")
            await connection.execute(
                "INSERT INTO memory_node_versions SELECT v.* FROM origin.memory_node_versions v "
                "JOIN main.memory_nodes n ON v.node_id=n.node_id AND v.version_no=n.content_version")
            for table in ("node_sources", "node_participants"):
                await connection.execute(
                    f"INSERT INTO {table} SELECT r.* FROM origin.{table} r JOIN main.memory_nodes n USING(node_id)")
            await connection.execute(
                "INSERT INTO memory_edges SELECT e.* FROM origin.memory_edges e "
                "JOIN main.memory_nodes n ON n.node_id=e.source_node_id "
                "JOIN main.memory_nodes m ON m.node_id=e.target_node_id WHERE e.relation_type='contains'")
            # This private connection uses the existing consolidator. Durable writes are
            # fenced against the real lease each time a draft window or publication commits.
            await connection.execute(
                "INSERT INTO memory_jobs(job_id,kind,scope_key,idempotency_key,state,payload_json,"
                "available_at,lease_token,lease_until,owner,created_at,updated_at) "
                "VALUES (?,'topic_consolidate',?,?,'running','{}',0,?,1e99,'isolated-stage',0,0)",
                (job["job_id"], space_id, job["job_id"], job["lease_token"]))
            deltas = await connection.execute(
                "SELECT state_json FROM origin.topic_rebuild_stage WHERE job_id=? ORDER BY rowid", (job["job_id"],))
            async for row in deltas:
                for table, records in json.loads(row["state_json"]).items():
                    await _restore_rows(connection, table, records)
            await connection.execute(
                "INSERT INTO topic_receipts SELECT r.* FROM origin.topic_receipts r "
                "JOIN origin.topic_rebuild_stage s USING(receipt_key) WHERE s.job_id=?", (job["job_id"],))
    finally:
        await db.execute("DETACH DATABASE origin")


@asynccontextmanager
async def topic_rebuild_stage(live_memory, space_id, generation_id, job):
    stage = MemoryStore(":memory:")
    await stage.open()
    try:
        await _load(stage, live_memory, space_id, generation_id, job)
        yield stage
    finally:
        await stage.close()


async def persist_topic_stage(live_memory, stage, job, window, receipt):
    """Save a validated window and its receipt atomically before advancing progress."""
    key = receipt["receipt_key"]
    async with stage.database.transaction(write=False) as connection:
        args = (json.dumps(receipt["topic_ids"]),)
        topics = " IN (SELECT value FROM json_each(?))"
        delta = {}
        for table, column in (
            ("memory_nodes", "node_id"), ("memory_node_versions", "node_id"),
            ("memory_edges", "source_node_id"), ("node_participants", "node_id"),
        ):
            # Current heads suffice for future synthesis. Replacing a topic head
            # cascades its old edges before the window's complete set is inserted.
            condition = column + topics
            if table == "memory_node_versions":
                condition += " AND version_no=(SELECT content_version FROM memory_nodes WHERE node_id=memory_node_versions.node_id)"
            delta[table] = await _rows(connection, f"SELECT * FROM {table} WHERE {condition}", args)
        delta["topic_reviews"] = await _rows(connection, "SELECT * FROM topic_reviews WHERE node_id" + topics,
                                              (json.dumps(window["prepared"]["seed_ids"]),))
        rows = await _rows(connection, "SELECT * FROM topic_receipts WHERE receipt_key=?", (key,))
    data = json.dumps(delta, ensure_ascii=False, separators=(",", ":"))
    async with live_memory.database.transaction() as connection:
        await assert_job_lease(connection, job["job_id"], job["lease_token"], scope_key=job["scope_key"])
        if await _rows(connection, "SELECT 1 FROM topic_rebuild_stage WHERE receipt_key=?", (key,)):
            return
        await _restore_rows(connection, "topic_receipts", rows)
        await connection.execute(
            "INSERT INTO topic_rebuild_stage(job_id,receipt_key,state_json) VALUES (?,?,?)", (job["job_id"], key, data))


async def export_topics(stage):
    async with stage.database.transaction(write=False) as connection:
        nodes = await _rows(connection, _NODE_SELECT + " WHERE n.node_type='topic' ORDER BY n.node_id")
        edges = await _rows(connection,
            "SELECT source_node_id,target_node_id FROM memory_edges JOIN memory_nodes n ON source_node_id=n.node_id "
            "WHERE n.node_type='topic' AND relation_type='contains' ORDER BY source_node_id,target_node_id")
        reviews = await _rows(connection, "SELECT node_id,content_version FROM topic_reviews")
    members = {}
    for edge in edges:
        members.setdefault(edge["source_node_id"], []).append(edge["target_node_id"])
    drafts = []
    for row in nodes:
        node = stage._node_from_row(row)
        drafts.append({"node_id": node["node_id"], "title": node["title"], "content": node["content_text"],
                       "importance": node["importance"], "emotion": node["emotion"],
                       "member_ids": members[node["node_id"]]})
    return drafts, {row["node_id"]: row["content_version"] for row in reviews}
