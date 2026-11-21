"""Publish a completed replacement set of topics into the current memory generation."""

import json
import time

from .job_store import assert_job_lease
from .pipeline_store import CONTEXT_REBUILDS
from .topic_store import TopicStore, _one, _rows, digest


async def _basis(connection, space_id, generation_id):
    scope = (space_id, generation_id)
    nodes = await _rows(connection,
        "SELECT node_id,node_type,title,content_version,status,manual_protected,metadata_json,"
        "importance,emotion_json,updated_at,content_updated_at,event_time_start,event_time_end,event_time_text "
        "FROM memory_nodes WHERE space_id=? AND generation_id=? ORDER BY node_id", scope)
    edges = await _rows(connection,
        "SELECT source_node_id,target_node_id,relation_type FROM memory_edges "
        "WHERE space_id=? AND generation_id=? ORDER BY source_node_id,target_node_id,relation_type", scope)
    sources = await _rows(connection,
        "SELECT n.node_id,s.source_id,s.source_kind FROM memory_nodes n JOIN node_sources s USING(node_id) "
        "WHERE n.space_id=? AND n.generation_id=? ORDER BY n.node_id,s.source_id", scope)
    participants = await _rows(connection,
        "SELECT n.node_id,p.participant_kind,p.participant_id,p.display_name FROM memory_nodes n "
        "JOIN node_participants p USING(node_id) WHERE n.space_id=? AND n.generation_id=? "
        "ORDER BY n.node_id,p.participant_kind,p.participant_id", scope)
    tables = [[tuple(row) for row in rows] for rows in (nodes, edges, sources, participants)]
    return digest([scope, tables])


async def topic_rebuild_basis(memory, space_id, generation_id):
    """Fence retries across edits while allowing foreground recall and reinforcement."""
    async with memory.database.transaction(write=False) as connection:
        return await _basis(connection, space_id, generation_id)


async def published_topics(memory, job_id):
    return await TopicStore(memory).receipt("topic-rebuild:" + job_id)


async def publish_topics(memory, job, generation_id, drafts, reviewed, basis, *, invalidate_nodes=None):
    """Replace all old topic statuses and versions, leaving each final draft at v1."""
    store = TopicStore(memory)
    job_id, space_id = job["job_id"], job["scope_key"]
    receipt_key = "topic-rebuild:" + job_id
    now = time.time()
    async with memory.database.transaction() as connection:
        lease = await assert_job_lease(connection, job_id, job["lease_token"], scope_key=space_id)
        permitted = lease["kind"] == "topic_rebuild" or (
            lease["kind"] in CONTEXT_REBUILDS and lease["payload"].get("include_topics") is True)
        if not permitted or lease["payload"]["expected_generation_id"] != generation_id:
            raise PermissionError("task cannot publish replacement topics")
        receipt = await _one(connection, "SELECT result_json FROM topic_receipts WHERE receipt_key=?", (receipt_key,))
        if receipt:
            return json.loads(receipt["result_json"])
        generation = await _one(connection,
            "SELECT * FROM memory_generations WHERE space_id=? AND generation_id=? AND status='active'",
            (space_id, generation_id))
        if generation is None or await _basis(connection, space_id, generation_id) != basis:
            raise ValueError("topic context changed")

        old_topics = await _rows(connection,
            "SELECT node_id FROM memory_nodes WHERE space_id=? AND generation_id=? AND node_type='topic' "
            "ORDER BY node_id", (space_id, generation_id))
        await connection.execute(
            "DELETE FROM memory_nodes WHERE space_id=? AND generation_id=? AND node_type='topic'",
            (space_id, generation_id))
        members = await _rows(connection,
            "SELECT node_id,content_version FROM memory_nodes WHERE space_id=? AND generation_id=?",
            (space_id, generation_id))
        known = {member["node_id"]: member for member in members}
        for draft in drafts:
            await store._write_topic(connection, generation, known, None, draft, receipt_key, now,
                                     new_node_id=draft["node_id"])
        await connection.executemany(
            "INSERT INTO topic_reviews(node_id,content_version,processed_at) VALUES (?,?,?) "
            "ON CONFLICT(node_id) DO UPDATE SET content_version=excluded.content_version,processed_at=excluded.processed_at",
            [(node_id, version, now) for node_id, version in reviewed.items()])
        await connection.execute(
            "UPDATE memory_jobs SET payload_json=json_set(payload_json,'$.topic_inputs_replaced',1) "
            "WHERE scope_key=? AND kind='topic_consolidate' AND state IN ('failed','cancelled') "
            "AND json_extract(payload_json,'$.expected_generation_id')=?", (space_id, generation_id))
        await connection.execute("DELETE FROM topic_rebuild_stage WHERE job_id=?", (job_id,))
        await connection.execute("DELETE FROM topic_receipts WHERE job_id=?", (job_id,))
        result = {"topic_ids": [draft["node_id"] for draft in drafts],
                  "removed_node_ids": [row["node_id"] for row in old_topics],
                  "reviewed_count": len(reviewed), "generation_id": generation_id}
        await connection.execute(
            "INSERT INTO topic_receipts(receipt_key,job_id,generation_id,input_digest,output_digest,result_json,created_at) "
            "VALUES (?,?,?,?,?,?,?)", (receipt_key, job_id, generation_id, basis, digest(drafts), json.dumps(result), now))
        if invalidate_nodes is not None and old_topics:
            await invalidate_nodes(space_id, result["removed_node_ids"])
        return result
