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
