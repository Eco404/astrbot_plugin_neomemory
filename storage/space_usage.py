"""On-demand byte counts of attributable content, using a separate read-only snapshot."""

from __future__ import annotations

import sqlite3
from contextlib import closing


# Each row is counted once, including retained versions and extraction results.
_MEMORY_PAYLOADS = (
    ("memory_nodes", "title metadata_json emotion_json", ""),
    ("memory_node_versions", "content_text title metadata_json emotion_json", "JOIN memory_nodes USING(node_id)"),
    ("memory_edges", "metadata_json", ""),
    ("user_profiles", "description tags_json", ""),
    ("profile_versions", "description tags_json", "JOIN user_profiles USING(profile_id)"),
    ("relationship_states", "state_text state_json", ""),
    ("relationship_versions", "state_text state_json", "JOIN relationship_states USING(relationship_id)"),
    ("short_term_states", "summary_text coverage_json", ""),
    ("short_term_receipts", "result_json", "JOIN short_term_states USING(state_id)"),
    ("notes", "content_text metadata_json", ""),
    ("extraction_batches", "result_json", ""),
    ("pending_image_evidence", "evidence_json", "JOIN extraction_batches USING(batch_id)"),
    ("recall_history", "snapshot_json", ""),
    ("topic_receipts", "result_json", "JOIN memory_generations USING(generation_id)"),
    ("topic_rebuild_stage", "state_json", "JOIN topic_receipts USING(receipt_key) JOIN memory_generations USING(generation_id)"),
    ("note_receipts", "result_json", ""),
    ("governance_receipts", "result_json reason", ""),
    ("rebuild_receipts", "result_json", ""),
    ("memory_jobs", "payload_json checkpoint_json result_json attempts_json", "JOIN memory_spaces s ON s.space_id=t.scope_key"),
    ("memory_job_candidates", "window_json", "JOIN memory_jobs j USING(job_id) JOIN memory_spaces s ON s.space_id=j.scope_key"),
)


def _payload_bytes(fields, alias):
    return " + ".join(f"COALESCE(length(CAST({alias}.{field} AS BLOB)),0)" for field in fields.split())


def _read_uri(path):
    return path.resolve().as_uri() + "?mode=ro"


# Reproduce the captured private recipient, even after an account is rebound.
# Index the user message IDs once for the whole report. A reply can refer to an
# earlier speaker or to a triggering source that was persisted out of order.
_SOURCE_USAGE = f"""
WITH triggers AS MATERIALIZED (
    SELECT stream_id, persona_id, message_id, actor_logical_user_id, MIN(rowid)
    FROM raw.source_records WHERE actor_kind='user' AND message_id IS NOT NULL
    GROUP BY stream_id, persona_id, message_id
), images AS (
    SELECT source_id, SUM(length(CAST(description AS BLOB))) AS size
    FROM raw.source_image_evidence GROUP BY source_id
), attributed AS (
    SELECT r.persona_id, CASE lower(s.message_type)
        WHEN 'groupmessage' THEN ':platform:' || s.platform_instance_id || ':group:' || s.group_id
        WHEN 'friendmessage' THEN ':user:' || CASE
            WHEN r.actor_kind='user' THEN r.actor_logical_user_id
            WHEN json_extract(r.metadata_json,'$.delivered_proactive')
                THEN json_extract(r.metadata_json,'$.recipient_logical_user_id')
            WHEN json_extract(r.metadata_json,'$.trigger_message_id') IS NOT NULL THEN u.actor_logical_user_id
            ELSE (SELECT prev.actor_logical_user_id FROM raw.source_records prev
                  WHERE prev.stream_id=r.stream_id AND prev.persona_id=r.persona_id
                    AND prev.actor_kind='user' AND prev.sequence_no<=r.sequence_no
                  ORDER BY prev.sequence_no DESC LIMIT 1) END END AS suffix,
        {_payload_bytes("content_text content_json metadata_json", "r")} + COALESCE(i.size,0) AS size
    FROM raw.source_records r JOIN raw.conversation_streams s USING(stream_id)
    LEFT JOIN images i USING(source_id)
    LEFT JOIN triggers u ON r.actor_kind='agent' AND u.stream_id=r.stream_id AND u.persona_id=r.persona_id
                         AND u.message_id=json_extract(r.metadata_json,'$.trigger_message_id')
    WHERE r.source_type='conversation' AND r.identity_pending=0 AND r.actor_kind IN ('user','agent')
)
SELECT s.space_id, SUM(a.size) FROM attributed a
JOIN memory_spaces s ON s.scope_key='persona:' || a.persona_id || a.suffix GROUP BY s.space_id
"""


def space_payload_sizes(memory_path, source_path, cache_path):
    """Count stored text, structured content and vectors across all retained generations."""
    with closing(sqlite3.connect(_read_uri(memory_path), uri=True)) as db:
        db.row_factory = sqlite3.Row
        queries = [
            f"SELECT space_id, SUM({_payload_bytes(fields, 't')}) FROM {table} t {joins} GROUP BY space_id"
            for table, fields, joins in _MEMORY_PAYLOADS
        ]
        if source_path is not None:
            db.execute("ATTACH DATABASE ? AS raw", (_read_uri(source_path),))
            queries.append(_SOURCE_USAGE)
        if cache_path is not None:
            db.execute("ATTACH DATABASE ? AS vectors", (_read_uri(cache_path),))
            queries.append("SELECT namespace, SUM(length(CAST(vector_json AS BLOB))) "
                           "FROM vectors.embedding_cache GROUP BY namespace")
        db.execute("BEGIN")
        spaces = {row["space_id"]: dict(row, size_bytes=0) for row in db.execute(
            "SELECT space_id,space_type,persona_id,logical_user_id,platform_instance_id,group_id FROM memory_spaces"
        )}
        for query in queries:
            for space_id, size in db.execute(query):
                if space_id in spaces:
                    spaces[space_id]["size_bytes"] += size
        return sorted(spaces.values(), key=lambda row: (-row["size_bytes"], row["space_id"]))
