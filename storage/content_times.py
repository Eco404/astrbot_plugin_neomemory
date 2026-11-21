"""Persist source-clock snapshots separately from database operation clocks."""

from __future__ import annotations

import json


async def citation_times(connection, sources, observations):
    """Reuse recorded observation values when a manual split has no raw input."""
    values = dict(observations or {})
    missing = [source_id for source_id, kind in sources
               if kind == "source_record" and source_id not in values]
    for start in range(0, len(missing), 500):
        chunk = missing[start:start + 500]
        cursor = await connection.execute(
            "SELECT source_id,MAX(observed_at) AS observed_at FROM memory_derivation_sources "
            f"WHERE source_kind='source_record' AND source_id IN ({','.join('?' for _ in chunk)}) "
            "AND observed_at IS NOT NULL GROUP BY source_id", chunk)
        values.update((row["source_id"], row["observed_at"]) for row in await cursor.fetchall())
    return [(source_id, kind, values.get(source_id) if kind == "source_record" else None)
            for source_id, kind in sources]


async def refresh_content_time(connection, node_id, version_no, *, pending_versions=()):
    """Snapshot the latest observation in this version's declared content basis.

    Title/membership operations do not date the prose. A split explicitly
    selects sources, so its original whole-node baseline must not date a part.
    Walking version references also handles a batch whose parents precede their
    newly created children in output order. No source database is read here.
    """
    pending = [(node_id, version_no, None)]
    seen = set()
    timestamps = []
    while pending:
        item = pending.pop()
        identity, cutoff = item[:2], item[2]
        if item in seen:
            continue
        seen.add(item)
        cursor = await connection.execute(
            "SELECT v.node_version_id,v.content_updated_at,"
            "(SELECT MAX(d.created_at) FROM memory_derivations d "
            "WHERE d.node_version_id=v.node_version_id "
            "AND d.operation NOT IN ('title_update','membership')) AS last_derivation_at "
            "FROM memory_node_versions v WHERE v.node_id=? AND v.version_no=?", identity)
        version = await cursor.fetchone()
        if version is None:
            continue
        snapshot_applies = (cutoff is None or version["last_derivation_at"] is None
                            or version["last_derivation_at"] <= cutoff)
        if version["content_updated_at"] is not None and snapshot_applies:
            timestamps.append(version["content_updated_at"])
            if identity != (node_id, version_no) and identity not in pending_versions:
                # Later same-version confirmations cannot date an earlier basis.
                continue
        cursor = await connection.execute(
            "SELECT d.derivation_id,d.operation,d.base_versions_json,d.created_at,MAX(s.observed_at) AS observed_at "
            "FROM memory_derivations d LEFT JOIN memory_derivation_sources s "
            "ON s.derivation_id=d.derivation_id WHERE d.node_version_id=? "
            "AND d.operation NOT IN ('title_update','membership') "
            "AND (? IS NULL OR d.created_at<=?) GROUP BY d.derivation_id",
            (version["node_version_id"], cutoff, cutoff))
        for derivation in await cursor.fetchall():
            if derivation["observed_at"] is not None:
                timestamps.append(derivation["observed_at"])
            if derivation["operation"] != "split":
                pending.extend((base["entity_id"], base["version_no"], derivation["created_at"])
                               for base in json.loads(derivation["base_versions_json"])
                               if base["kind"] == "node")
    updated = max(timestamps) if timestamps else None
    await connection.execute(
        "UPDATE memory_node_versions SET content_updated_at=? WHERE node_id=? AND version_no=?",
        (updated, node_id, version_no))
    await connection.execute(
        "UPDATE memory_nodes SET content_updated_at=? WHERE node_id=? AND content_version=?",
        (updated, node_id, version_no))
    return updated
