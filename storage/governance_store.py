"""Atomic human governance, fenced by durable jobs and optimistic snapshots.

Node operations require ``action``, ``expected_generation_id`` and an
``expected_versions`` node-ID/version mapping. ``expected_updated_at`` must
cover directly governed nodes and checks metadata/status changes as well. Mutation requests never contain arbitrary
SQL, scope selectors, source additions or model-generated repair instructions.

``edit``: node_id, patch (content_text/importance/event_time_*/participants),
          optional relations replacing *all* incident edges.
``split``: node_id, parts [{ref,content_text,importance,source_ids,...patch fields}],
           optional relations using part refs or known node IDs.
``merge``: node_ids, keep_id, content_text, importance; preserves valid redirected
           relations and combines provenance/participants.
Other actions: node_id. Delete is a tombstone; source records remain untouched.
Context entries use entity_kind, entity_id and expected_version together with
expected_generation_id.
Relations use source_ref, target_ref, relation_type. Existing explicit endpoints
must appear in expected_versions. New split refs need no expected version.
"""

from __future__ import annotations

from .topic_dependencies import invalidate_unlocked_reviews

import hashlib
import json
import math
import time
import uuid
from typing import Any

try:
    from ..core.memory_emotions import encode_emotion, validate_emotion
    from ..core.text_lengths import encoded_text_limit, validate_readable_text
    from ..core.memory_nodes import (
        PARTICIPANT_KINDS,
        validate_content,
        validate_edge_type,
        validate_title,
    )
except ImportError:
    from core.memory_emotions import encode_emotion, validate_emotion
    from core.text_lengths import encoded_text_limit, validate_readable_text
    from core.memory_nodes import (
        PARTICIPANT_KINDS,
        validate_content,
        validate_edge_type,
        validate_title,
    )

from .context_governance import apply_context_change, validate_context_operation
from .job_store import assert_job_lease
from .provenance_store import record_derivation, version_reference

_ACTIONS = frozenset(
    {"edit", "split", "merge", "disable", "restore", "delete", "protect", "unprotect"}
)
_COMMON = {
    "action",
    "expected_generation_id",
    "expected_versions",
    "expected_updated_at",
    "expected_titles",
}
_PATCH = {
    "emotion",
    "title",
    "content_text",
    "importance",
    "event_time_start",
    "event_time_end",
    "event_time_text",
    "participants",
}
_FIELDS = {
    "edit": {"node_id", "patch", "relations"},
    "split": {"node_id", "parts", "relations"},
    "merge": {"node_ids", "keep_id", "content_text", "importance", "title", "emotion"},
}


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        allow_nan=False,
        separators=(",", ":"),
    )


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _string(value: Any, name: str, maximum: int = 200) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"invalid {name}")
    return value


def _number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"invalid {name}")
    try:
        valid = math.isfinite(value)
    except OverflowError:
        valid = False
    if not valid:
        raise ValueError(f"invalid {name}")
    return float(value)


def _importance(value: Any) -> float:
    result = _number(value, "importance")
    if not 0 <= result <= 1:
        raise ValueError("importance must be between zero and one")
    return result


def _participants(value: Any) -> list[tuple[str, str, str | None]]:
    if not isinstance(value, list) or len(value) > 100:
        raise ValueError("participants must be a bounded list")
    normalized = {}
    for item in value:
        if not isinstance(item, dict) or set(item) - {
            "participant_kind",
            "participant_id",
            "display_name",
        }:
            raise ValueError("invalid participant")
        kind = item.get("participant_kind")
        if not isinstance(kind, str) or kind not in PARTICIPANT_KINDS:
            raise ValueError("invalid participant kind")
        participant_id = _string(item.get("participant_id"), "participant_id")
        display = item.get("display_name")
        if display is not None and (not isinstance(display, str) or len(display) > 200):
            raise ValueError("invalid participant display_name")
        normalized[(kind, participant_id)] = (kind, participant_id, display)
    return list(normalized.values())


def validate_operation(operation: Any) -> dict[str, Any]:
    """Validate bounded JSON structure before taking the SQLite writer lock."""
    if not isinstance(operation, dict):
        raise ValueError("operation must be an object")
    # A JSON roundtrip prevents a caller from modifying the request during awaits.
    encoded = _json(operation)
    if len(encoded.encode("utf-8")) > 256_000:
        raise ValueError("governance operation exceeds its size limit")
    operation = json.loads(encoded)
    if "entity_kind" in operation:
        return validate_context_operation(operation)
    action = operation.get("action")
    if (
        not isinstance(action, str)
        or action not in _ACTIONS
        or set(operation) - _COMMON - _FIELDS.get(action, {"node_id"})
    ):
        raise ValueError("unknown action or operation field")
    _string(operation.get("expected_generation_id"), "expected_generation_id")
    versions = operation.get("expected_versions")
    if not isinstance(versions, dict) or not 1 <= len(versions) <= 100:
        raise ValueError("expected_versions must contain 1 to 100 entries")
    for node_id, version in versions.items():
        _string(node_id, "node_id")
        if type(version) is not int or version < 1:
            raise ValueError("expected versions must be positive integers")
    updates = operation.get("expected_updated_at", {})
    if not isinstance(updates, dict) or set(updates) - set(versions):
        raise ValueError("expected_updated_at requires expected node versions")
    for value in updates.values():
        _number(value, "expected_updated_at")
    titles = operation.get("expected_titles", {})
    if not isinstance(titles, dict) or set(titles) - set(versions):
        raise ValueError("expected_titles requires expected node versions")
    for title in titles.values():
        if title is not None:
            validate_title(title)
    if action == "merge":
        ids = operation.get("node_ids")
        if (
            not isinstance(ids, list)
            or not 2 <= len(ids) <= 20
            or any(not isinstance(value, str) for value in ids)
            or len(set(ids)) != len(ids)
        ):
            raise ValueError("merge requires 2 to 20 distinct nodes")
        if any(not isinstance(value, str) or value not in versions for value in ids):
            raise ValueError("merge node requires an expected version")
        if operation.get("keep_id") not in ids:
            raise ValueError("keep_id must belong to node_ids")
        _validate_patch(
            {
                "content_text": operation.get("content_text"),
                "importance": operation.get("importance"),
                **({"title": operation["title"]} if "title" in operation else {}),
                "emotion": operation.get("emotion"),
            }
        )
    else:
        _string(operation.get("node_id"), "node_id")
        if operation.get("node_id") not in versions:
            raise ValueError("node_id requires an expected version")
    if action == "edit":
        patch = operation.get("patch", {})
        _validate_patch(patch)
        if not patch and "relations" not in operation:
            raise ValueError("edit requires a patch or explicit relations")
    if action == "split":
        parts = operation.get("parts")
        if not isinstance(parts, list) or not 2 <= len(parts) <= 20:
            raise ValueError("split requires 2 to 20 parts")
        refs = set()
        for part in parts:
            if not isinstance(part, dict) or set(part) - _PATCH - {"ref", "source_ids"}:
                raise ValueError("invalid split part")
            ref = _string(part.get("ref"), "part ref")
            if ref in refs or ref in versions:
                raise ValueError("part refs must be unique and distinct from node IDs")
            refs.add(ref)
            if not {"content_text", "importance"} <= set(part):
                raise ValueError("split parts require content_text and importance")
            _validate_patch(
                {key: value for key, value in part.items() if key in _PATCH}
            )
            sources = part.get("source_ids")
            if not isinstance(sources, list) or not 1 <= len(sources) <= 500:
                raise ValueError("split part requires bounded provenance")
            for source in sources:
                _string(source, "source_id")
    governed_ids = (
        operation["node_ids"] if action == "merge" else [operation["node_id"]]
    )
    if any(node_id not in updates for node_id in governed_ids):
        raise ValueError("expected_updated_at must cover every governed node")
    title_target = (operation["node_id"] if action == "edit" and "title" in operation.get("patch", {})
                    else operation["keep_id"] if action == "merge" and "title" in operation else None)
    if title_target is not None and title_target not in titles:
        raise ValueError("title edits require expected_titles for the target node")
    if "relations" in operation:
        relations = operation["relations"]
        if not isinstance(relations, list) or len(relations) > 100:
            raise ValueError("relations must be a bounded list")
        for edge in relations:
            if not isinstance(edge, dict) or set(edge) != {
                "source_ref",
                "target_ref",
                "relation_type",
            }:
                raise ValueError("invalid relation")
            _string(edge["source_ref"], "source_ref")
            _string(edge["target_ref"], "target_ref")
            validate_edge_type(edge["relation_type"])
    return operation


def _validate_patch(patch: Any) -> None:
    if not isinstance(patch, dict) or set(patch) - _PATCH:
        raise ValueError("unknown edit field")
    if "content_text" in patch:
        if not isinstance(patch["content_text"], str):
            raise ValueError("content_text must be text")
        validate_content(patch["content_text"])
    if "title" in patch:
        validate_title(patch["title"])
    if "emotion" in patch:
        validate_emotion(patch["emotion"])
    if "importance" in patch:
        _importance(patch["importance"])
    for name in ("event_time_start", "event_time_end"):
        if name in patch and patch[name] is not None:
            _number(patch[name], name)
    if "event_time_text" in patch and patch["event_time_text"] is not None:
        value = _string(patch["event_time_text"], "event_time_text", encoded_text_limit(500))
        validate_readable_text(value, 500, label="event_time_text")
    if "participants" in patch:
        _participants(patch["participants"])


def _audit_node(row: Any) -> dict[str, Any]:
    """Keep factual metadata and version references, never duplicate node text."""
    item = {
        key: row[key]
        for key in (
            "content_version",
            "title",
            "status",
            "importance",
            "event_time_start",
            "event_time_end",
            "event_time_text",
            "updated_at",
        )
    }
    item["manual_protected"] = bool(row["manual_protected"])
    return item


async def _one(connection: Any, sql: str, args: tuple = ()) -> Any:
    cursor = await connection.execute(sql, args)
    return await cursor.fetchone()


async def _all(connection: Any, sql: str, args: tuple = ()) -> list[Any]:
    cursor = await connection.execute(sql, args)
    return await cursor.fetchall()


class GovernanceService:
    def __init__(self, memory_store: Any):
        self.memory = memory_store

    async def apply(
        self,
        space_id: str,
        operation: dict[str, Any],
        *,
        actor: str = "webui",
        reason: str = "",
        job_id: str,
        lease_token: str,
    ) -> dict[str, Any]:
        operation = validate_operation(operation)
        _string(actor, "actor", 200)
        if not isinstance(reason, str) or len(reason) > 2000:
            raise ValueError("reason exceeds 2000 characters")
        request_digest = hashlib.sha256(
            _json(
                {
                    "space_id": space_id,
                    "operation": operation,
                    "actor": actor,
                    "reason": reason,
                }
            ).encode("utf-8")
        ).hexdigest()
        action = operation["action"]
        observations = {}
        if action == "split" and self.memory.source_observations:
            selected = tuple(dict.fromkeys(sid for part in operation["parts"] for sid in part["source_ids"]))
            observations = await self.memory.source_observations(selected)
        # 展示标题单独修改不改变正文语义、人工保护或任何召回状态。
        title_only = (action == "edit" and set(operation.get("patch", {})) == {"title"}
                      and "relations" not in operation)
        generation_id = operation["expected_generation_id"]
        now = time.time()
        async with self.memory.database.transaction() as connection:
            await assert_job_lease(connection, job_id, lease_token, scope_key=space_id)
            receipt = await _one(
                connection,
                "SELECT * FROM governance_receipts WHERE job_id=?",
                (job_id,),
            )
            if receipt:
                if receipt["input_digest"] != request_digest:
                    raise ValueError(
                        "job already committed a different governance operation"
                    )
                return {**json.loads(receipt["result_json"]), "replayed": True}
            generation = await _one(
                connection,
                "SELECT * FROM memory_generations WHERE space_id=? AND generation_id=? AND status='active'",
                (space_id, generation_id),
            )
            if generation is None:
                raise ValueError("stale or foreign generation")
            if kind := operation.get("entity_kind"):
                version = await apply_context_change(connection, space_id, operation, now)
                entity_id = operation["entity_id"]
                previous_version = operation["expected_version"]
                result = {"job_id": job_id, "space_id": space_id, "generation_id": generation_id,
                          "action": action, "entity_kind": kind, "entity_id": entity_id,
                          "before_version": previous_version, "content_version": version, "replayed": False}
                await self._receipt(connection, result, lease_token, actor, reason, request_digest, now)
                if kind != "short_term" and version is not None and version != previous_version:
                    await record_derivation(
                        connection, kind, entity_id, version, operation="edit", editor_kind="manual",
                        now=now, governance_job_id=job_id, base_versions=[version_reference(kind, entity_id, previous_version)],
                    )
                return result
            nodes = {}
            for node_id, version in operation["expected_versions"].items():
                row = await self.memory._node_row_in_connection(connection, node_id)
                if (
                    not row
                    or row["space_id"] != space_id
                    or row["generation_id"] != generation_id
                ):
                    raise ValueError("node belongs to another space or generation")
                if row["content_version"] != version:
                    raise ValueError("stale node version")
                if (node_id in operation.get("expected_titles", {})
                        and row["title"] != operation["expected_titles"][node_id]):
                    raise ValueError("stale node title")
                if (
                    node_id in operation.get("expected_updated_at", {})
                    and row["updated_at"] != operation["expected_updated_at"][node_id]
                ):
                    raise ValueError("stale node metadata")
                if row["status"] in {"deleted", "superseded"}:
                    raise ValueError("deleted or superseded nodes cannot be governed")
                nodes[node_id] = dict(row)
            affected = (
                operation["node_ids"] if action == "merge" else [operation["node_id"]]
            )
            now = max(now, *(math.nextafter(nodes[node_id]["updated_at"], math.inf) for node_id in affected))
            before_versions = {
                node_id: nodes[node_id]["content_version"] for node_id in affected
            }
            parents = await self._parents(connection, affected)
            refs = {}
            self_stale = set()
            if action == "edit":
                node = nodes[affected[0]]
                await self._edit(
                    connection,
                    node,
                    operation.get("patch", {}),
                    generation,
                    now,
                    reason,
                    title_only=title_only,
                )
                if "relations" in operation:
                    had_children = await _one(
                        connection,
                        "SELECT 1 FROM memory_edges WHERE source_node_id=? AND relation_type='contains' LIMIT 1",
                        (node["node_id"],),
                    )
                    await self._remove_edges(connection, affected)
                    await self._explicit_edges(
                        connection,
                        operation["relations"],
                        nodes,
                        {},
                        set(affected),
                        now,
                    )
                    has_children = await _one(
                        connection,
                        "SELECT 1 FROM memory_edges WHERE source_node_id=? AND relation_type='contains' LIMIT 1",
                        (node["node_id"],),
                    )
                    if (
                        had_children or has_children
                    ) and "content_text" not in operation.get("patch", {}):
                        self_stale.add(node["node_id"])
                if not title_only:
                    await self._invalidate_coverage(connection, affected)
            elif action == "split":
                refs = await self._split(
                    connection,
                    nodes[affected[0]],
                    operation,
                    nodes,
                    generation,
                    now,
                    reason,
                )
            elif action == "merge":
                await self._merge(connection, operation, nodes, generation, now, reason)
            else:
                await self._state(connection, nodes[affected[0]], action, now, reason)
            # Pure protection changes do not invalidate the parent's factual text.
            stale = []
            if action not in {"protect", "unprotect"} and not title_only:
                parents.update(
                    await self._parents(connection, affected + list(refs.values()))
                )
                stale = sorted(
                    (parents - set(affected) - set(refs.values())) | self_stale
                )
                await self._stale(connection, stale, job_id, now)
            result_nodes = affected + list(refs.values())
            after = {}
            for node_id in result_nodes:
                row = await _one(
                    connection,
                    "SELECT * FROM memory_nodes WHERE node_id=?",
                    (node_id,),
                )
                after[node_id] = _audit_node(row)
            result = {
                "job_id": job_id,
                "space_id": space_id,
                "generation_id": generation_id,
                "action": action,
                "before_versions": before_versions,
                "before_nodes": {
                    node_id: _audit_node(nodes[node_id]) for node_id in affected
                },
                "nodes": after,
                "node_refs": refs,
                "stale_parent_ids": stale,
                "replayed": False,
                "title_only": title_only,
            }
            if action == "delete":
                result["deletion_kind"] = "derived_tombstone"
            await self._receipt(connection, result, lease_token, actor, reason, request_digest, now)
            if action == "split":
                original_id = affected[0]
                source_rows = await _all(connection, "SELECT * FROM node_sources WHERE node_id=?", (original_id,))
                source_kinds = {row["source_id"]: row["source_kind"] for row in source_rows}
                for part in operation["parts"]:
                    await record_derivation(
                        connection, "node", refs[part["ref"]], 1, operation=action,
                        editor_kind="manual", now=now, governance_job_id=job_id,
                        base_versions=[version_reference("node", original_id, before_versions[original_id])],
                        sources=[(source_id, source_kinds[source_id]) for source_id in part["source_ids"]],
                        source_observations=observations,
                    )
            elif action in {"edit", "merge"}:
                target = operation["keep_id"] if action == "merge" else affected[0]
                version = after[target]["content_version"]
                if action == "merge" or version != before_versions[target]:
                    await record_derivation(
                        connection, "node", target, version, operation=action,
                        editor_kind="manual", now=now, governance_job_id=job_id,
                        base_versions=[version_reference("node", node_id, old_version)
                                       for node_id, old_version in before_versions.items()
                                       if node_id != target or old_version != version],
                    )
                elif after[target]["title"] != nodes[target]["title"]:
                    await record_derivation(
                        connection, "node", target, version, operation="title_update",
                        editor_kind="manual", now=now, governance_job_id=job_id,
                        semantic={"title_before": nodes[target]["title"],
                                  "title_after": after[target]["title"]},
                    )
            return result

    @staticmethod
    async def _receipt(connection, result, lease_token, actor, reason, request_digest, now):
        await assert_job_lease(connection, result["job_id"], lease_token, scope_key=result["space_id"])
        await connection.execute(
            """INSERT INTO governance_receipts(job_id,space_id,generation_id,action,actor,reason,input_digest,result_json,committed_at)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            (result["job_id"], result["space_id"], result["generation_id"], result["action"],
             actor, reason, request_digest, _json(result), now),
        )

    async def _edit(self, connection, node, patch, generation, now, reason, *, title_only=False):
        title = validate_title(patch["title"]) if "title" in patch else node["title"]
        if title_only:
            if title != node["title"]:
                await connection.execute("UPDATE memory_nodes SET title=? WHERE node_id=?",
                                         (title, node["node_id"]))
            return
        content = validate_content(patch.get("content_text", node["content_text"]))
        importance = _importance(patch.get("importance", node["importance"]))
        emotion_json = encode_emotion(patch["emotion"]) if "emotion" in patch else node["emotion_json"]
        changed = content != node["content_text"] or emotion_json != node["emotion_json"]
        reset = changed or importance != node["importance"]
        version = node["content_version"] + int(changed)
        times = [
            patch.get(key, node[key])
            for key in ("event_time_start", "event_time_end", "event_time_text")
        ]
        if times[0] is not None and times[1] is not None and times[1] < times[0]:
            raise ValueError("event_time_end cannot precede event_time_start")
        metadata = json.loads(node["metadata_json"])
        if "content_text" in patch:
            for key in ("summary_stale", "summary_stale_job_id"):
                metadata.pop(key, None)
        await connection.execute(
            """UPDATE memory_nodes SET content_version=?,importance=?,strength=?,event_time_start=?,event_time_end=?,event_time_text=?,
            manual_protected=1,manual_protection_reason=?,metadata_json=?,updated_at=?,strength_reset_at=?,strength_reset_round=?,reinforcement=?,title=?,emotion_json=?
            WHERE node_id=?""",
            (
                version,
                importance,
                importance if reset else node["strength"],
                *times,
                reason or "manual governance",
                _json(metadata),
                now,
                now if reset else node["strength_reset_at"],
                generation["activity_round"] if reset else node["strength_reset_round"],
                0 if reset else node["reinforcement"],
                title,
                emotion_json,
                node["node_id"],
            ),
        )
        if changed:
            await connection.execute(
                """INSERT INTO memory_node_versions(node_version_id,node_id,version_no,content_text,metadata_json,change_kind,editor_kind,created_at,title,emotion_json)
                VALUES (?,?,?,?,?,'manual_update','manual',?,?,?)""",
                (
                    _id("nodever"),
                    node["node_id"],
                    version,
                    content,
                    node["content_metadata_json"],
                    now,
                    title,
                    emotion_json,
                ),
            )
        if "participants" in patch:
            await connection.execute(
                "DELETE FROM node_participants WHERE node_id=?", (node["node_id"],)
            )
            await self._write_participants(
                connection, node["node_id"], _participants(patch["participants"])
            )

    @staticmethod
    async def _write_participants(connection, node_id, participants):
        await connection.executemany(
            "INSERT OR IGNORE INTO node_participants(node_id,participant_kind,participant_id,display_name) VALUES (?,?,?,?)",
            [(node_id, *item) for item in participants],
        )

    async def _split(
        self, connection, original, operation, nodes, generation, now, reason
    ):
        sources = {
            row["source_id"]: row
            for row in await _all(
                connection,
                "SELECT * FROM node_sources WHERE node_id=?",
                (original["node_id"],),
            )
        }
        refs = {}
        for part in operation["parts"]:
            if any(source not in sources for source in part["source_ids"]):
                raise ValueError("split provenance must belong to the original node")
            node_id = _id("node")
            title = validate_title(part["title"]) if "title" in part else None
            emotion_json = encode_emotion(part.get("emotion"))
            refs[part["ref"]] = node_id
            times = [
                part.get(key, original[key])
                for key in ("event_time_start", "event_time_end", "event_time_text")
            ]
            if times[0] is not None and times[1] is not None and times[1] < times[0]:
                raise ValueError("event_time_end cannot precede event_time_start")
            await connection.execute(
                """INSERT INTO memory_nodes(node_id,generation_id,space_id,node_type,status,importance,strength,
                event_time_start,event_time_end,event_time_text,manual_protected,manual_protection_reason,
                created_at,updated_at,strength_reset_at,strength_reset_round,title,emotion_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?,?,?,?,?,?)""",
                (
                    node_id,
                    original["generation_id"],
                    original["space_id"],
                    original["node_type"],
                    original["status"],
                    part["importance"],
                    part["importance"],
                    *times,
                    reason or "manual split",
                    now,
                    now,
                    now,
                    generation["activity_round"],
                    title,
                    emotion_json,
                ),
            )
            await connection.execute(
                """INSERT INTO memory_node_versions(node_version_id,node_id,version_no,content_text,change_kind,editor_kind,created_at,title,emotion_json)
                VALUES (?,?,1,?,'created','manual',?,?,?)""",
                (_id("nodever"), node_id, validate_content(part["content_text"]), now, title, emotion_json),
            )
            await connection.executemany(
                "INSERT INTO node_sources(node_id,source_id,source_kind,created_at) VALUES (?,?,?,?)",
                [
                    (node_id, source, sources[source]["source_kind"], now)
                    for source in dict.fromkeys(part["source_ids"])
                ],
            )
            # Participant attribution is explicitly assigned, not copied to every split.
            await self._write_participants(
                connection, node_id, _participants(part.get("participants", []))
            )
        await self._supersede(connection, [original["node_id"]], now, reason)
        await self._explicit_edges(
            connection,
            operation.get("relations", []),
            nodes,
            refs,
            set(refs.values()),
            now,
        )
        return refs

    async def _merge(self, connection, operation, nodes, generation, now, reason):
        ids = operation["node_ids"]
        keep_id = operation["keep_id"]
        if len({nodes[node_id]["node_type"] for node_id in ids}) != 1:
            raise ValueError("merge requires nodes of the same type")
        # Mixed lifecycle states must be resolved explicitly before merging.
        if len({nodes[node_id]["status"] for node_id in ids}) != 1:
            raise ValueError("merge requires the same node status")
        edges = await self._incident_edges(connection, ids)
        await self._edit(
            connection,
            nodes[keep_id],
            {
                "content_text": operation["content_text"],
                "importance": operation["importance"],
                **({"title": operation["title"]} if "title" in operation else {}),
                "emotion": operation.get("emotion"),
            },
            generation,
            now,
            reason,
        )
        for node_id in ids:
            if node_id == keep_id:
                continue
            await connection.execute(
                """INSERT OR IGNORE INTO node_sources(node_id,source_id,source_kind,created_at)
                SELECT ?,source_id,source_kind,created_at FROM node_sources WHERE node_id=?""",
                (keep_id, node_id),
            )
            await connection.execute(
                """INSERT OR IGNORE INTO node_participants(node_id,participant_kind,participant_id,display_name)
                SELECT ?,participant_kind,participant_id,display_name FROM node_participants WHERE node_id=?""",
                (keep_id, node_id),
            )
        await self._remove_edges(connection, ids)
        await self._supersede(
            connection, [node_id for node_id in ids if node_id != keep_id], now, reason
        )
        await self._invalidate_coverage(connection, ids)
        for edge in edges:
            source = (
                keep_id if edge["source_node_id"] in ids else edge["source_node_id"]
            )
            target = (
                keep_id if edge["target_node_id"] in ids else edge["target_node_id"]
            )
            if source != target:
                await self._edge(
                    connection,
                    source,
                    target,
                    edge["relation_type"],
                    nodes[keep_id]["space_id"],
                    nodes[keep_id]["generation_id"],
                    now,
                    metadata=edge["metadata_json"],
                )

    async def _state(self, connection, node, action, now, reason):
        node_id = node["node_id"]
        if action in {"protect", "unprotect"}:
            if action == "unprotect" and node["manual_protected"]:
                await invalidate_unlocked_reviews(connection, node_id)
            await connection.execute(
                "UPDATE memory_nodes SET manual_protected=?,manual_protection_reason=?,updated_at=? WHERE node_id=?",
                (
                    int(action == "protect"),
                    (reason or "manual governance") if action == "protect" else None,
                    now,
                    node_id,
                ),
            )
            return
        if action == "restore" and node["status"] != "disabled":
            raise ValueError("only disabled nodes can be restored")
        if action == "disable" and node["status"] not in {"active", "historical"}:
            raise ValueError("only active or historical nodes can be disabled")
        status = {"disable": "disabled", "restore": "active", "delete": "deleted"}[
            action
        ]
        await connection.execute(
            "UPDATE memory_nodes SET status=?,deleted_at=?,manual_protected=1,manual_protection_reason=?,updated_at=? WHERE node_id=?",
            (
                status,
                now if status == "deleted" else None,
                reason or "manual governance",
                now,
                node_id,
            ),
        )
        await self._invalidate_coverage(connection, [node_id])
        if action == "delete":
            await self._remove_edges(connection, [node_id])

    async def _supersede(self, connection, ids, now, reason):
        for node_id in ids:
            await connection.execute(
                "UPDATE memory_nodes SET status='superseded',manual_protected=1,manual_protection_reason=?,updated_at=? WHERE node_id=?",
                (reason or "manual governance", now, node_id),
            )
        await self._remove_edges(connection, ids)
        await self._invalidate_coverage(connection, ids)

    @staticmethod
    async def _incident_edges(connection, ids):
        placeholders = ",".join("?" for _ in ids)
        return await _all(
            connection,
            f"SELECT * FROM memory_edges WHERE source_node_id IN ({placeholders}) OR target_node_id IN ({placeholders})",
            (*ids, *ids),
        )

    @staticmethod
    async def _remove_edges(connection, ids):
        for node_id in ids:
            await connection.execute(
                "DELETE FROM memory_edges WHERE source_node_id=? OR target_node_id=?",
                (node_id, node_id),
            )

    @staticmethod
    async def _invalidate_coverage(connection, ids):
        for node_id in ids:
            await connection.execute(
                "DELETE FROM node_coverage WHERE event_node_id=? OR atomic_node_id=?",
                (node_id, node_id),
            )

    async def _explicit_edges(self, connection, edges, known, refs, changed_ids, now):
        for edge in edges:
            resolved = []
            for ref in (edge["source_ref"], edge["target_ref"]):
                if ref not in refs and ref not in known:
                    raise ValueError(
                        "relation endpoint requires an expected version or split ref"
                    )
                resolved.append(refs.get(ref, ref))
            if not set(resolved) & changed_ids:
                raise ValueError("relation must involve an edited or split node")
            sample = next(iter(known.values()))
            await self._edge(
                connection,
                *resolved,
                edge["relation_type"],
                sample["space_id"],
                sample["generation_id"],
                now,
            )

    @staticmethod
    async def _edge(
        connection,
        source,
        target,
        relation,
        space_id,
        generation_id,
        now,
        *,
        metadata="{}",
    ):
        relation = validate_edge_type(relation)
        if source == target:
            raise ValueError("relation cannot be a self-loop")
        rows = {
            row["node_id"]: row
            for row in await _all(
                connection,
                "SELECT * FROM memory_nodes WHERE node_id IN (?,?)",
                (source, target),
            )
        }
        if len(rows) != 2 or any(
            row["space_id"] != space_id
            or row["generation_id"] != generation_id
            or row["status"] not in {"active", "historical", "disabled"}
            for row in rows.values()
        ):
            raise ValueError(
                "relation endpoint is unavailable in this space and generation"
            )
        if relation == "contains" and (
            rows[source]["node_type"],
            rows[target]["node_type"],
        ) not in {("topic", "event"), ("topic", "atomic"), ("event", "atomic")}:
            raise ValueError("invalid contains relation types")
        if relation == "supersedes":
            # Check reachability after previous inserts in this same transaction.
            cycle = await _one(
                connection,
                """WITH RECURSIVE reachable(node_id) AS (
                    SELECT target_node_id FROM memory_edges WHERE source_node_id=? AND relation_type='supersedes'
                    UNION SELECT e.target_node_id FROM memory_edges e JOIN reachable r ON e.source_node_id=r.node_id WHERE e.relation_type='supersedes'
                ) SELECT 1 FROM reachable WHERE node_id=? LIMIT 1""",
                (target, source),
            )
            if cycle:
                raise ValueError("supersedes relation creates a cycle")
        await connection.execute(
            """INSERT OR IGNORE INTO memory_edges(edge_id,generation_id,space_id,source_node_id,target_node_id,relation_type,metadata_json,created_at)
            VALUES (?,?,?,?,?,?,?,?)""",
            (
                _id("edge"),
                generation_id,
                space_id,
                source,
                target,
                relation,
                metadata,
                now,
            ),
        )

    @staticmethod
    async def _parents(connection, ids):
        placeholders = ",".join("?" for _ in ids)
        rows = await _all(
            connection,
            f"""WITH RECURSIVE parents(node_id) AS (
                SELECT source_node_id FROM memory_edges WHERE relation_type='contains' AND target_node_id IN ({placeholders})
                UNION SELECT e.source_node_id FROM memory_edges e JOIN parents p ON e.target_node_id=p.node_id WHERE e.relation_type='contains'
            ) SELECT node_id FROM parents""",
            tuple(ids),
        )
        return {row["node_id"] for row in rows}

    @staticmethod
    async def _stale(connection, ids, job_id, now):
        for node_id in ids:
            row = await _one(
                connection,
                "SELECT metadata_json FROM memory_nodes WHERE node_id=?",
                (node_id,),
            )
            metadata = json.loads(row["metadata_json"])
            metadata.update(summary_stale=True, summary_stale_job_id=job_id)
            await connection.execute(
                "UPDATE memory_nodes SET metadata_json=?,updated_at=? WHERE node_id=?",
                (_json(metadata), now, node_id),
            )
        await GovernanceService._invalidate_coverage(connection, ids)

    async def list_audit(
        self, space_id: str, *, generation_id: str | None = None, limit: int = 100
    ):
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("audit limit must be between 1 and 1000")
        sql = "SELECT * FROM governance_receipts WHERE space_id=?"
        values = [space_id]
        if generation_id is not None:
            sql += " AND generation_id=?"
            values.append(generation_id)
        sql += " ORDER BY committed_at DESC,job_id DESC LIMIT ?"
        rows = await self.memory.database.fetchall(sql, (*values, limit))
        result = []
        for row in rows:
            item = dict(row)
            item["result"] = json.loads(item.pop("result_json"))
            result.append(item)
        return result

    @staticmethod
    async def purge_generation(connection: Any, generation_id: str) -> None:
        """Used inside a successful generation-switch transaction, never earlier."""
        await connection.execute(
            "DELETE FROM governance_receipts WHERE generation_id=?", (generation_id,)
        )
