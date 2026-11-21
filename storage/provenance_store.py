"""Content snapshots and direct citations for each committed derivation."""

from __future__ import annotations

import json
import uuid
from collections import defaultdict

from .version_retention import prune_node_history
from .content_times import citation_times, refresh_content_time

_ENTITIES = {
    "node": ("memory_nodes", "node_id", "memory_node_versions", "node_version_id"),
    "profile": ("user_profiles", "profile_id", "profile_versions", "profile_version_id"),
    "relationship": ("relationship_states", "relationship_id", "relationship_versions", "relationship_version_id"),
}


def version_reference(kind, entity_id, version_no):
    return {"kind": kind, "entity_id": entity_id, "version_no": version_no}


async def snapshot_context_version(connection, kind, entity_id):
    table, entity_key, versions, version_key = _ENTITIES[kind]
    fields = "tags_json,description" if kind == "profile" else "state_text,state_json"
    await connection.execute(
        f"INSERT OR IGNORE INTO {versions}({version_key},{entity_key},version_no,{fields},status,created_at) "
        f"SELECT ?,{entity_key},content_version,{fields},status,updated_at FROM {table} WHERE {entity_key}=?",
        (f"{kind}ver_{uuid.uuid4().hex}", entity_id),
    )


async def record_derivation(
    connection, kind, entity_id, version_no, *, operation, editor_kind, now,
    sources=(), context_sources=(), semantic=None, base_versions=(), batch_id=None, governance_job_id=None,
    source_observations=None, refresh_node_time=True,
):
    """Write only explicitly supplied citations; never expand inherited sources."""
    _, entity_key, versions, version_key = _ENTITIES[kind]
    cursor = await connection.execute(
        f"SELECT {version_key} FROM {versions} WHERE {entity_key}=? AND version_no=?",
        (entity_id, version_no),
    )
    row = await cursor.fetchone()
    if row is None:
        raise KeyError(f"unknown {kind} content version")
    derivation_id = f"derivation_{uuid.uuid4().hex}"
    await connection.execute(
        f"INSERT INTO memory_derivations(derivation_id,{version_key},operation,editor_kind,"
        "batch_id,governance_job_id,base_versions_json,semantic_json,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
        (derivation_id, row[version_key], operation, editor_kind, batch_id, governance_job_id,
         json.dumps(list(base_versions), ensure_ascii=False, separators=(",", ":")),
         json.dumps(semantic or {}, ensure_ascii=False, separators=(",", ":")), now),
    )
    timestamps = await citation_times(connection, list(dict(sources).items()), source_observations)
    await connection.executemany(
        "INSERT INTO memory_derivation_sources(derivation_id,source_id,source_kind,observed_at) VALUES (?,?,?,?)",
        [(derivation_id, source_id, source_kind, observed_at)
         for source_id, source_kind, observed_at in timestamps],
    )
    await connection.executemany(
        "INSERT INTO memory_derivation_context_sources(derivation_id,source_id) VALUES (?,?)",
        [(derivation_id, source_id) for source_id in dict.fromkeys(context_sources)],
    )
    if refresh_node_time and kind == "node" and operation not in {"title_update", "membership"}:
        await refresh_content_time(connection, entity_id, version_no)
        if version_no > 1:
            await prune_node_history(connection, entity_id)


class ProvenanceStoreMixin:
    async def get_observation_anchor_sources(self, kind, entity_id, version_no):
        """Read direct evidence for the first and two latest available versions.

        Version timestamps describe database writes, not observations. Include
        the copied observation time of each direct source when available. Title and
        membership operations do not date the unchanged prose. This deliberately
        does not expand inherited provenance or assert per-fact dates.
        """
        _, entity_key, versions, version_key = _ENTITIES[kind]
        rows = await self.database.fetchall(
            f"""WITH selected AS (
                SELECT * FROM (SELECT {version_key},version_no FROM {versions}
                    WHERE {entity_key}=? AND version_no<=? ORDER BY version_no LIMIT 1)
                UNION
                SELECT * FROM (SELECT {version_key},version_no FROM {versions}
                    WHERE {entity_key}=? AND version_no<=? ORDER BY version_no DESC LIMIT 2)
            )
            SELECT v.version_no, s.source_id, MAX(s.observed_at) AS observed_at FROM selected v
            LEFT JOIN memory_derivations d ON d.{version_key}=v.{version_key}
                AND d.operation NOT IN ('title_update', 'membership')
            LEFT JOIN memory_derivation_sources s ON s.derivation_id=d.derivation_id
                AND s.source_kind='source_record'
            GROUP BY v.version_no, s.source_id
            ORDER BY v.version_no, s.source_id""",
            (entity_id, version_no, entity_id, version_no),
        )
        result = {}
        for row in rows:
            sources = result.setdefault(row["version_no"], {})
            if row["source_id"] is not None:
                sources[row["source_id"]] = row["observed_at"]
        return [
            {"version_no": version, "source_ids": sorted(sources),
             "source_observations": {source_id: observed for source_id, observed in sources.items()
                                     if observed is not None}}
            for version, sources in result.items()
        ]

    async def list_version_derivations(self, kind, entity_id, version_no=None):
        """Return recorded operations, without treating inherited context as direct evidence."""
        if kind not in _ENTITIES:
            raise ValueError("unsupported provenance entity kind")
        table, entity_key, versions, version_key = _ENTITIES[kind]
        async with self.database.transaction(write=False) as connection:
            cursor = await connection.execute(f"SELECT 1 FROM {table} WHERE {entity_key}=?", (entity_id,))
            if await cursor.fetchone() is None:
                raise KeyError(f"unknown {kind}: {entity_id}")
            where = f"v.{entity_key}=?"
            params = [entity_id]
            if version_no is not None:
                where += " AND v.version_no=?"
                params.append(version_no)
            cursor = await connection.execute(
                f"SELECT d.*,v.version_no FROM memory_derivations d JOIN {versions} v "
                f"ON d.{version_key}=v.{version_key} WHERE {where} "
                "ORDER BY v.version_no,d.created_at,d.derivation_id", params,
            )
            records = [dict(row) for row in await cursor.fetchall()]
            cursor = await connection.execute(
                "SELECT s.* FROM memory_derivation_sources s JOIN memory_derivations d "
                f"ON s.derivation_id=d.derivation_id JOIN {versions} v "
                f"ON d.{version_key}=v.{version_key} WHERE {where} ORDER BY s.source_id", params,
            )
            sources = defaultdict(list)
            for row in await cursor.fetchall():
                sources[row["derivation_id"]].append({"source_id": row["source_id"], "source_kind": row["source_kind"]})
            cursor = await connection.execute(
                "SELECT s.* FROM memory_derivation_context_sources s JOIN memory_derivations d "
                f"ON s.derivation_id=d.derivation_id JOIN {versions} v "
                f"ON d.{version_key}=v.{version_key} WHERE {where} ORDER BY s.source_id", params,
            )
            context_sources = defaultdict(list)
            for row in await cursor.fetchall():
                context_sources[row["derivation_id"]].append(row["source_id"])
        for item in records:
            item["base_versions"] = json.loads(item.pop("base_versions_json"))
            item["sources"] = sources[item["derivation_id"]]
            item["context_source_ids"] = context_sources[item["derivation_id"]]
            item["semantic"] = json.loads(item.pop("semantic_json"))
            if item["operation"] == "title_update":
                # 标题修改是展示元数据操作，不能混作模型对正文的语义声明。
                item["title_change"] = {
                    "before": item["semantic"].get("title_before"),
                    "after": item["semantic"].get("title_after"),
                }
                item["semantic"] = {}
            for key in ("node_version_id", "profile_version_id", "relationship_version_id"):
                item.pop(key)
        return records

    async def _attach_derivations(self, kind, entity_id, versions):
        by_version = defaultdict(list)
        for item in await self.list_version_derivations(kind, entity_id):
            by_version[item["version_no"]].append(item)
        for version in versions:
            version["derivations"] = by_version[version["version_no"]]
            # 标题或成员关联不能补齐旧正文的来源凭据。
            content_records = [item for item in version["derivations"]
                               if item["operation"] not in {"title_update", "membership"}]
            version["provenance_status"] = "recorded" if content_records else "unrecorded"
        return versions

    async def _list_context_versions(self, kind, entity_id):
        _, entity_key, versions, _ = _ENTITIES[kind]
        rows = await self.database.fetchall(
            f"SELECT * FROM {versions} WHERE {entity_key}=? ORDER BY version_no", (entity_id,),
        )
        result = []
        for row in rows:
            item = dict(row)
            field, target = ("tags_json", "tags") if kind == "profile" else ("state_json", "state")
            item[target] = json.loads(item.pop(field))
            result.append(item)
        return await self._attach_derivations(kind, entity_id, result)

    async def list_profile_versions(self, profile_id):
        return await self._list_context_versions("profile", profile_id)

    async def list_relationship_versions(self, relationship_id):
        return await self._list_context_versions("relationship", relationship_id)
