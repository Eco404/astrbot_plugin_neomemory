"""Lease-fenced topic synthesis over versioned memory nodes, without source replay."""

from __future__ import annotations

import json
import time
import uuid

from .job_store import assert_job_lease
from .pipeline_store import assert_rebuild_target
from .provenance_store import record_derivation, version_reference
from .recall_store import _NODE_SELECT
from .topic_dependencies import write_member_dependencies

try:
    from ..core.candidate_feedback import CandidateValidationError
    from ..core.extraction_apply import digest
    from ..core.memory_emotions import encode_emotion
    from ..core.memory_nodes import validate_content, validate_title
    from ..core.memory_text import memory_texts, user_reference_ids
    from ..core.topic_schema import TOPIC_CREATE_MIN_MEMBERS
except ImportError:
    from core.candidate_feedback import CandidateValidationError
    from core.extraction_apply import digest
    from core.memory_emotions import encode_emotion
    from core.memory_nodes import validate_content, validate_title
    from core.memory_text import memory_texts, user_reference_ids
    from core.topic_schema import TOPIC_CREATE_MIN_MEMBERS


async def _rows(connection, sql, values=()):
    cursor = await connection.execute(sql, values)
    return await cursor.fetchall()


async def _one(connection, sql, values=()):
    rows = await _rows(connection, sql, values)
    return rows[0] if rows else None


def _id(prefix):
    return prefix + "_" + uuid.uuid4().hex


class TopicStore:
    def __init__(self, memory):
        self.memory = memory
        self.database = memory.database

    async def rebuild_preview(self, space_id):
        row = await self.database.fetchone(
            "SELECT COUNT(*) FILTER (WHERE eligible AND manual_protected=0) AS eligible_members, "
            "COUNT(*) FILTER (WHERE eligible AND manual_protected=1) AS protected_members, "
            "COUNT(*) FILTER (WHERE node_type='topic') AS old_topics FROM ("
            "SELECT n.node_type,n.manual_protected, n.node_type<>'topic' AND n.status='active' "
            "AND COALESCE(json_extract(n.metadata_json,'$.summary_stale'),0)=0 AS eligible "
            "FROM memory_nodes n JOIN memory_generations g USING(generation_id) "
            "WHERE n.space_id=? AND g.status='active')", (space_id,))
        return dict(row)

    @staticmethod
    def can_propose_changes(prepared):
        nodes = {n["node_id"]: n for n in prepared["nodes"]}
        writable = [n for n in nodes.values() if n["status"] == "active"
                    and not n["manual_protected"] and not n["metadata"].get("summary_stale")]
        if sum(n["node_type"] != "topic" for n in writable) >= TOPIC_CREATE_MIN_MEMBERS:
            return True
        for topic in writable:
            if topic["node_type"] == "topic":
                members = [nodes[e["target_node_id"]] for e in prepared["edges"]
                           if e["source_node_id"] == topic["node_id"] and e["relation_type"] == "contains"]
                if (members or any(n["node_type"] != "topic" for n in writable)) and all(
                    n["status"] in {"active", "historical"} and not n["metadata"].get("summary_stale")
                    for n in members
                ):
                    return True
        return False

    async def count_dirty(self, space_id, generation_id, *, before=None, review_before=None):
        row = await self.database.fetchone(
            "SELECT COUNT(*) AS count FROM memory_nodes n LEFT JOIN topic_reviews r ON r.node_id=n.node_id "
            "WHERE n.space_id=? AND n.generation_id=? AND n.node_type<>'topic' AND n.status='active' "
            "AND n.manual_protected=0 AND COALESCE(json_extract(n.metadata_json,'$.summary_stale'),0)=0 "
            "AND (r.node_id IS NULL OR r.content_version<>n.content_version OR r.processed_at<?) "
            "AND (? IS NULL OR n.updated_at<=?)", (space_id, generation_id, review_before, before, before))
        return row["count"]

    async def dirty_nodes(self, space_id, generation_id, *, limit=100, before=None, review_before=None):
        """Select new content or a fixed manual re-review range; unchanged automatic input stays idle."""
        rows = await self.database.fetchall(
            _NODE_SELECT.replace("SELECT n.*", "SELECT r.processed_at,n.*") + " LEFT JOIN topic_reviews r ON r.node_id=n.node_id "
            "WHERE n.space_id=? AND n.generation_id=? AND n.node_type<>'topic' "
            "AND n.status='active' AND n.manual_protected=0 "
            "AND COALESCE(json_extract(n.metadata_json,'$.summary_stale'),0)=0 "
            "AND (r.node_id IS NULL OR r.content_version<>n.content_version OR r.processed_at<?) "
            "AND (? IS NULL OR n.updated_at<=?) ORDER BY n.updated_at,n.node_id LIMIT ?",
            (space_id, generation_id, review_before, before, before, limit))
        return [self.memory._node_from_row(row) for row in rows]

    async def prepare(self, space_id, generation_id, node_ids, *, seed_ids=None):
        """Snapshot topic membership and event/atomic structure within visible nodes."""
        identifiers = list(dict.fromkeys(node_ids))
        seeds = list(dict.fromkeys(identifiers if seed_ids is None else seed_ids))
        async with self.database.transaction(write=False) as connection:
            generation = await _one(connection,
                "SELECT * FROM memory_generations WHERE space_id=? AND generation_id=? "
                "AND status IN ('active','building')", (space_id, generation_id))
            if generation is None:
                raise ValueError("topic generation is unavailable")
            nodes = {}
            edges = []
            for root in tuple(identifiers):
                parents = await _rows(connection,
                    "SELECT e.source_node_id FROM memory_edges e JOIN memory_nodes n ON n.node_id=e.source_node_id "
                    "WHERE e.target_node_id=? AND e.relation_type='contains' AND n.node_type='topic' "
                    "AND n.space_id=? AND n.generation_id=? AND n.status='active'",
                    (root, space_id, generation_id))
                for parent in parents:
                    if parent["source_node_id"] not in identifiers:
                        identifiers.append(parent["source_node_id"])
            for node_id in identifiers:
                row = await self.memory._node_row_in_connection(connection, node_id)
                if row is None or row["space_id"] != space_id or row["generation_id"] != generation_id:
                    raise ValueError("topic context node is outside generation")
                nodes[node_id] = self.memory._node_from_row(row)
            for node in list(nodes.values()):
                if node["node_type"] != "topic":
                    continue
                rows = await _rows(connection,
                    "SELECT source_node_id,target_node_id,relation_type FROM memory_edges "
                    "WHERE source_node_id=? AND relation_type='contains' ORDER BY target_node_id",
                    (node["node_id"],))
                edges.extend(dict(row) for row in rows)
                for edge in rows:
                    member_id = edge["target_node_id"]
                    if member_id not in nodes:
                        row = await self.memory._node_row_in_connection(connection, member_id)
                        nodes[member_id] = self.memory._node_from_row(row)
            if seed_ids is None:
                seeds = [seed for seed in seeds if nodes[seed]["node_type"] != "topic"]
            if any(seed not in nodes or nodes[seed]["node_type"] == "topic" for seed in seeds):
                raise ValueError("topic seeds must be supplied non-topic nodes")
            related = await self._related_topics(connection, space_id, generation_id, nodes)
            edges.extend(await self._member_structure(connection, nodes))
            prepared = {"space_id": space_id, "generation_id": generation_id,
                        "nodes": sorted(nodes.values(), key=lambda item: item["node_id"]),
                        "edges": sorted(edges, key=lambda edge: (edge["source_node_id"], edge["target_node_id"])),
                        "seed_ids": seeds, "related_topics": related, "member_structure_version": 1,
                        "allowed_user_ids": sorted(await self._allowed_user_ids(connection, space_id, nodes))}
            prepared["input_digest"] = digest(prepared)
            return prepared

    async def _member_structure(self, connection, nodes):
        """Expose only visible event-to-atomic containment without expanding context."""
        edges = []
        for node in nodes.values():
            if node["node_type"] != "event":
                continue
            rows = await _rows(connection,
                "SELECT source_node_id,target_node_id,relation_type FROM memory_edges "
                "WHERE source_node_id=? AND relation_type='contains' ORDER BY target_node_id",
                (node["node_id"],))
            edges.extend(dict(row) for row in rows if row["target_node_id"] in nodes
                         and nodes[row["target_node_id"]]["node_type"] == "atomic")
        return sorted(edges, key=lambda edge: (edge["source_node_id"], edge["target_node_id"]))

    async def _allowed_user_ids(self, connection, space_id, nodes):
        allowed = set(user_reference_ids(text for node in nodes.values() for text in memory_texts(node)))
        space = await _one(connection, "SELECT logical_user_id FROM memory_spaces WHERE space_id=?", (space_id,))
        if space["logical_user_id"]:
            allowed.add(space["logical_user_id"])
        for node_id in nodes:
            participants = await _rows(connection,
                "SELECT participant_id FROM node_participants WHERE node_id=? AND participant_kind='logical_user'",
                (node_id,))
            allowed.update(participant["participant_id"] for participant in participants)
        return allowed

    async def _related_topics(self, connection, space_id, generation_id, nodes):
        """Expose other direct owners without recursively expanding their members."""
        parent_ids = set()
        for node in nodes.values():
            if node["node_type"] == "topic":
                continue
            rows = await _rows(connection,
                "SELECT e.source_node_id FROM memory_edges e JOIN memory_nodes n ON n.node_id=e.source_node_id "
                "WHERE e.target_node_id=? AND e.relation_type='contains' AND n.node_type='topic' "
                "AND n.space_id=? AND n.generation_id=? AND n.status='active'",
                (node["node_id"], space_id, generation_id))
            parent_ids.update(row["source_node_id"] for row in rows if row["source_node_id"] not in nodes)
        related = []
        for parent_id in sorted(parent_ids):
            topic = self.memory._node_from_row(await self.memory._node_row_in_connection(connection, parent_id))
            memberships = await _rows(connection,
                "SELECT target_node_id FROM memory_edges WHERE source_node_id=? AND relation_type='contains' ORDER BY target_node_id",
                (parent_id,))
            related.append({key: topic[key] for key in (
                "node_id", "title", "content_text", "content_version", "status", "manual_protected",
                "metadata", "updated_at", "importance", "emotion")}
                | {"read_only": True, "member_ids": [row["target_node_id"] for row in memberships],
                   "summary_stale": bool(topic["metadata"].get("summary_stale"))})
        return related

    async def receipt(self, receipt_key):
        row = await self.database.fetchone("SELECT * FROM topic_receipts WHERE receipt_key=?", (receipt_key,))
        return json.loads(row["result_json"]) if row else None

    async def apply(self, job, prepared, output, *, receipt_key=None):
        """Commit a complete member set and its version basis in one transaction."""
        receipt_key = receipt_key or digest([job["job_id"], prepared["input_digest"]])
        output_digest = digest(output)
        now = time.time()
        async with self.database.transaction() as connection:
            lease = await assert_job_lease(connection, job["job_id"], job["lease_token"],
                                           scope_key=prepared["space_id"])
            if lease["kind"] not in {"topic_consolidate", "rebuild"}:
                raise PermissionError("task cannot synthesize topics")
            prior = await _one(connection, "SELECT * FROM topic_receipts WHERE receipt_key=?", (receipt_key,))
            if prior:
                if (prior["job_id"] != job["job_id"] or prior["input_digest"] != prepared["input_digest"]
                        or prior["output_digest"] != output_digest):
                    raise ValueError("topic receipt does not match this operation")
                return {**json.loads(prior["result_json"]), "replayed": True}
            generation = await _one(connection,
                "SELECT * FROM memory_generations WHERE generation_id=? AND space_id=?",
                (prepared["generation_id"], prepared["space_id"]))
            if generation is None or generation["status"] not in {"active", "building"}:
                raise ValueError("topic generation is unavailable")
            if generation["status"] == "building":
                await assert_rebuild_target(connection, lease, prepared["space_id"], prepared["generation_id"])
            known = {node["node_id"]: node for node in prepared["nodes"]}
            for node_id, node in known.items():
                current = await self.memory._node_row_in_connection(connection, node_id)
                if current is None:
                    raise ValueError("topic context changed")
                live = self.memory._node_from_row(current)
                fields = ("space_id", "generation_id", "node_type", "content_version", "status",
                          "manual_protected", "metadata", "title", "updated_at", "importance")
                if any(live[field] != node[field] for field in fields):
                    raise ValueError("topic context changed")
            for topic in (node for node in known.values() if node["node_type"] == "topic"):
                current = await _rows(connection,
                    "SELECT target_node_id FROM memory_edges WHERE source_node_id=? AND relation_type='contains'",
                    (topic["node_id"],))
                expected = {edge["target_node_id"] for edge in prepared["edges"]
                            if edge["source_node_id"] == topic["node_id"]}
                if {row["target_node_id"] for row in current} != expected:
                    raise ValueError("topic membership changed")
            if prepared.get("member_structure_version") == 1:
                expected_structure = [edge for edge in prepared["edges"]
                    if known[edge["source_node_id"]]["node_type"] == "event"]
                if await self._member_structure(connection, known) != expected_structure:
                    raise ValueError("topic member structure changed")
            if "related_topics" in prepared:
                related = await self._related_topics(connection, prepared["space_id"], prepared["generation_id"], known)
                if related != prepared["related_topics"]:
                    raise ValueError("topic context changed")
            changed = []
            topics = []
            handled = set()
            allowed_users = await self._allowed_user_ids(connection, prepared["space_id"], known)
            if "allowed_user_ids" in prepared and allowed_users != set(prepared["allowed_user_ids"]):
                raise ValueError("topic context changed")
            for change_index, change in enumerate(output["changes"]):
                texts = [change["title"], change["content"]]
                if change.get("emotion"):
                    texts.append(change["emotion"]["description"])
                if not set(user_reference_ids(texts)).issubset(allowed_users):
                    raise CandidateValidationError("topic_unknown_user", ("changes", change_index), "topic text references an unknown logical user")
                old = known.get(change["topic_id"]) if change["topic_id"] else None
                if change["operation"] == "update":
                    if (old is None or old["node_type"] != "topic"
                            or old["content_version"] != change["expected_version"]):
                        raise CandidateValidationError("topic_update_identity", ("changes", change_index), "topic update identity/version mismatch")
                    if old["node_id"] in handled:
                        raise CandidateValidationError("topic_duplicate_update", ("changes", change_index), "topic updated twice")
                    handled.add(old["node_id"])
                    if old["manual_protected"] or old["status"] != "active" or old["metadata"].get("summary_stale"):
                        raise CandidateValidationError("topic_update_identity", ("changes", change_index), "topic is read-only")
                elif change["operation"] != "create" or change["topic_id"] is not None or change["expected_version"] is not None:
                    raise CandidateValidationError("topic_create_identity", ("changes", change_index), "invalid topic creation")
                members = change["member_ids"]
                if len(set(members)) != len(members) or len(members) < (1 if old else TOPIC_CREATE_MIN_MEMBERS):
                    raise CandidateValidationError("topic_member_set", ("changes", change_index, "member_ids"), "topic requires distinct members")
                previous_members = {edge["target_node_id"] for edge in prepared["edges"]
                                    if old and edge["source_node_id"] == old["node_id"]}
                if any(known[member_id]["status"] not in {"active", "historical"}
                       or known[member_id]["metadata"].get("summary_stale")
                       for member_id in previous_members):
                    raise CandidateValidationError("topic_locked_membership", ("changes", change_index, "topic_id"),
                        "unavailable existing topic member requires manual revision")
                for member_index, member_id in enumerate(members):
                    member = known.get(member_id)
                    if (member is None or member["node_type"] == "topic"
                            or not (member["status"] == "active"
                                    or member["status"] == "historical" and member_id in previous_members)
                            or member["metadata"].get("summary_stale")):
                        raise CandidateValidationError("topic_member_unavailable", ("changes", change_index, "member_ids", member_index),
                            f"changes[{change_index}].member_ids[{member_index}]: topic member must be an available input event/atomic ID")
                if any(known[member_id]["manual_protected"]
                       for member_id in previous_members.symmetric_difference(members)):
                    raise CandidateValidationError("topic_protected_membership", ("changes", change_index, "member_ids"),
                        "protected member relation requires manual revision")
                node_id, content_changed = await self._write_topic(
                    connection, generation, known, old, change, receipt_key, now)
                topics.append(node_id)
                if content_changed:
                    changed.append(node_id)
            for seed_id in prepared["seed_ids"]:
                await connection.execute(
                    "INSERT INTO topic_reviews(node_id,content_version,processed_at) VALUES (?,?,?) "
                    "ON CONFLICT(node_id) DO UPDATE SET content_version=excluded.content_version,processed_at=excluded.processed_at",
                    (seed_id, known[seed_id]["content_version"], now))
            result = {"topic_ids": topics, "changed_node_ids": changed,
                      "reviewed_count": len(prepared["seed_ids"]), "receipt_key": receipt_key, "replayed": False}
            await connection.execute(
                "INSERT INTO topic_receipts(receipt_key,job_id,generation_id,input_digest,output_digest,result_json,created_at) "
                "VALUES (?,?,?,?,?,?,?)", (receipt_key, job["job_id"], prepared["generation_id"],
                    prepared["input_digest"], output_digest, json.dumps(result), now))
            return result

    async def _write_topic(self, connection, generation, known, old, change, receipt_key, now, *, new_node_id=None):
        content = validate_content(change["content"], max_chars=2000)
        title = validate_title(change["title"])
        emotion = encode_emotion(change["emotion"])
        importance = change["importance"]
        if not 0 <= importance <= 1:
            raise ValueError("topic importance must be between zero and one")
        node_id = old["node_id"] if old else (new_node_id or _id("node"))
        content_changed = old is None or content != old["content_text"] or emotion != encode_emotion(old["emotion"])
        version = old["content_version"] + int(content_changed) if old else 1
        if old is None:
            await connection.execute(
                "INSERT INTO memory_nodes(node_id,generation_id,space_id,node_type,status,content_version,importance,strength,"
                "created_at,updated_at,strength_reset_at,strength_reset_round,title,emotion_json) "
                "VALUES (?,?,?,'topic','active',?,?,?,?,?,?,?,?,?)",
                (node_id, generation["generation_id"], generation["space_id"], version, importance, importance,
                 now, now, now, generation["activity_round"], title, emotion))
        else:
            reset = content_changed or old["importance"] != importance
            await connection.execute(
                "UPDATE memory_nodes SET content_version=?,importance=?,title=?,emotion_json=?,"
                "updated_at=?,strength=?,strength_reset_at=?,strength_reset_round=?,reinforcement=? WHERE node_id=?",
                (version, importance, title, emotion, now if reset else old["updated_at"],
                 importance if reset else old["strength"], now if reset else old["strength_reset_at"],
                 generation["activity_round"] if reset else old["strength_reset_round"],
                 0 if reset else old["reinforcement"], node_id))
        if content_changed:
            await connection.execute(
                "INSERT INTO memory_node_versions(node_version_id,node_id,version_no,content_text,metadata_json,"
                "change_kind,editor_kind,created_at,title,emotion_json) VALUES (?,?,?,?,?,?,'model',?,?,?)",
                (_id("nodever"), node_id, version, content, json.dumps(old["content_metadata"] if old else {}),
                 "model_update" if old else "created", now, title, emotion))
        members = change["member_ids"]
        previous = await _rows(connection,
            "SELECT target_node_id FROM memory_edges WHERE source_node_id=? AND relation_type='contains'", (node_id,))
        for edge in previous:
            if edge["target_node_id"] not in members:
                await connection.execute(
                    "DELETE FROM memory_edges WHERE source_node_id=? AND target_node_id=? AND relation_type='contains'",
                    (node_id, edge["target_node_id"]))
        for member_id in members:
            await connection.execute(
                "INSERT OR IGNORE INTO memory_edges(edge_id,generation_id,space_id,source_node_id,target_node_id,relation_type,created_at) "
                "VALUES (?,?,?,?,?,'contains',?)",
                (_id("edge"), generation["generation_id"], generation["space_id"], node_id, member_id, now))
        await write_member_dependencies(connection, node_id,
            {member_id: known[member_id]["content_version"] for member_id in members}, now=now, replace=True)
        basis = [version_reference("node", member_id, known[member_id]["content_version"]) for member_id in members]
        if old and content_changed:
            basis.insert(0, version_reference("node", node_id, old["content_version"]))
        await record_derivation(connection, "node", node_id, version,
            operation="create" if old is None else "update" if content_changed else (
                "title_update" if title != old["title"] else "confirm"),
            editor_kind="model", now=now, base_versions=basis,
            semantic={"topic_receipt": receipt_key, "member_ids": members,
                      **({"title_before": old["title"], "title_after": title}
                         if old and not content_changed and title != old["title"] else {})})
        return node_id, content_changed
