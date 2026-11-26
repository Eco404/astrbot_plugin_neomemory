"""Persist explicitly assessed member versions without reinterpreting old summaries."""

from __future__ import annotations


async def write_member_dependencies(connection, topic_id, member_versions, *, now, replace=False):
    """Share dependency and cumulative lookup writes across both synthesis paths.

    Versions come from the caller's validated model input/output, never a fresh
    lookup of all current members. Partial updates retain untouched dependencies.
    Cumulative sources aid browsing; derivations separately record exact bases.
    """
    if replace:
        await connection.execute("DELETE FROM topic_dependencies WHERE topic_id=?", (topic_id,))
    for member_id, version in member_versions.items():
        await connection.execute(
            "INSERT INTO topic_dependencies(topic_id,member_id,member_version) VALUES (?,?,?) "
            "ON CONFLICT(topic_id,member_id) DO UPDATE SET member_version=excluded.member_version",
            (topic_id, member_id, version))
        await connection.execute(
            "INSERT OR IGNORE INTO node_sources(node_id,source_id,source_kind,created_at) "
            "SELECT ?,source_id,source_kind,? FROM node_sources WHERE node_id=?", (topic_id, now, member_id))
        await connection.execute(
            "INSERT OR IGNORE INTO node_participants(node_id,participant_kind,participant_id,display_name) "
            "SELECT ?,participant_kind,participant_id,display_name FROM node_participants WHERE node_id=?",
            (topic_id, member_id))


async def invalidate_unlocked_reviews(connection, node_id):
    """重新审阅解除保护的节点，以及解除保护主题的现有成员。"""
    await connection.execute(
        "DELETE FROM topic_reviews WHERE node_id=? OR node_id IN ("
        "SELECT e.target_node_id FROM memory_edges e JOIN memory_nodes n ON n.node_id=e.source_node_id "
        "WHERE n.node_id=? AND n.node_type='topic' AND e.relation_type='contains')", (node_id, node_id))


async def reconstruction_members(connection, topic_id):
    """Read the complete current membership and all text used to rebuild a topic."""
    cursor = await connection.execute(
        """SELECT n.node_id,n.content_version,n.status,n.title,v.content_text,
                  COALESCE(json_extract(n.metadata_json,'$.summary_stale'),0) AS stale
           FROM memory_edges e JOIN memory_nodes n ON n.node_id=e.target_node_id
           JOIN memory_node_versions v ON v.node_id=n.node_id AND v.version_no=n.content_version
           WHERE e.source_node_id=? AND e.relation_type='contains' ORDER BY n.node_id""", (topic_id,))
    return [dict(row) for row in await cursor.fetchall()]
