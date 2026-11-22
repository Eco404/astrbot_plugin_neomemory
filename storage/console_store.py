"""Consistent console reads and durable, fenced manual note mutations."""

from __future__ import annotations

import hashlib
import json
import math
import time
import uuid

try:
    from ..core.jobs import ReviewRequired
    from ..core.memory_emotions import EMOTION_TAGS
    from ..core.memory_nodes import NODE_STATUSES, NODE_TYPES
    from ..core.memory_notes import validate_note_content
    from ..core.memory_text import memory_texts, render_display_text, render_memory_item, user_reference_ids
    from ..core.persona_policy import is_supported_persona
except ImportError:
    from core.jobs import ReviewRequired
    from core.memory_emotions import EMOTION_TAGS
    from core.memory_nodes import NODE_STATUSES, NODE_TYPES
    from core.memory_notes import validate_note_content
    from core.memory_text import memory_texts, render_display_text, render_memory_item, user_reference_ids
    from core.persona_policy import is_supported_persona

from .job_store import JobConflict, assert_job_lease
from .recall_store import _NODE_SELECT
from .source_scan import _record_spec

_CONTEXT = {
    "profiles": ("user_profiles", "profile_id", "description", {"active", "disabled", "historical"}),
    "relationships": ("relationship_states", "relationship_id", "state_text", {"active", "disabled", "historical"}),
    "short_term": ("short_term_states", "state_id", "summary_text", {"active", "disabled", "archived"}),
}

_NODE_ORDER = {
    "updated_desc": "n.content_updated_at DESC,n.node_id",
    "updated_asc": "n.content_updated_at ASC NULLS LAST,n.node_id",
    "importance_desc": "n.importance DESC,n.content_updated_at DESC,n.node_id",
    "importance_asc": "n.importance ASC,n.content_updated_at DESC,n.node_id",
}


async def _one(connection, query, values=()):
    cursor = await connection.execute(query, values)
    return await cursor.fetchone()


def _decode(row, pairs=()):
    result = dict(row)
    for stored, exposed in pairs:
        result[exposed] = json.loads(result.pop(stored))
    return result


def _page(limit, offset, q):
    if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or not 0 <= offset <= 1_000_000:
        raise ValueError("invalid page")
    if not isinstance(q, str) or len(q) > 1000:
        raise ValueError("invalid query")


def _text_search(field, query, user_ids):
    condition = f"instr(lower({field}),lower(?))>0"
    parameters = [query]
    if user_ids:
        condition = (
            f"({condition} OR EXISTS (SELECT 1 FROM json_each(?) AS matched_user "
            f"WHERE instr({field}, '{{{{user:' || matched_user.value || '}}}}')>0))"
        )
        parameters.append(json.dumps(user_ids))
    return condition, parameters


async def _space(connection, space_id):
    row = await _one(connection,
        "SELECT s.*,g.generation_id FROM memory_spaces s LEFT JOIN memory_generations g "
        "ON g.space_id=s.space_id AND g.status='active' WHERE s.space_id=?", (space_id,))
    if row is None:
        raise KeyError("space")
    return dict(row)


def _listing(space, rows, limit, offset, **extra):
    return {"space_id": space["space_id"], "generation_id": space["generation_id"],
            "items": rows[:limit], "limit": limit, "offset": offset, "has_more": len(rows) > limit, **extra}


async def _browse_page(connection, query, parameters, ordering, limit, offset):
    cursor = await connection.execute(
        f"{query} ORDER BY {ordering} LIMIT ? OFFSET ?", (*parameters, limit + 1, offset))
    rows = await cursor.fetchall()
    if len(rows) <= limit and (rows or offset == 0):
        total = offset + len(rows)
    else:
        total = (await _one(connection, f"SELECT COUNT(*) FROM ({query})", parameters))[0]
    return rows, total


def _display_fields(value, render_text):
    """Resolve typed user fields and embedded references in structured console details."""
    if isinstance(value, list):
        return [_display_fields(item, render_text) for item in value]
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            if key.endswith("logical_user_id") and isinstance(item, str):
                result[key.removesuffix("_id") + "_name"] = render_text("{{user:" + item + "}}")
            elif key == "participant_id" and value.get("participant_kind") == "logical_user":
                result["participant_name"] = render_text("{{user:" + item + "}}")
            elif key == "display_name" and value.get("participant_kind") == "logical_user":
                continue
            else:
                result[key] = _display_fields(item, render_text)
        return result
    return render_text(value) if isinstance(value, str) else value


class ConsoleStore:
    def __init__(self, memory, sources=None):
        self.memory = memory
        self.sources = sources

    async def spaces(self, limit, offset):
        rows = await self.memory.database.fetchall(
            "SELECT space_id,scope_key,space_type,persona_id,logical_user_id,platform_instance_id,group_id "
            "FROM memory_spaces ORDER BY created_at,space_id LIMIT ? OFFSET ?", (limit + 1, offset))
        items = [dict(row) for row in rows[:limit]]
        await self.label_spaces(items)
        return {"items": items, "limit": limit, "offset": offset, "has_more": len(rows) > limit}

    async def label_spaces(self, items):
        owners = {item["logical_user_id"] for item in items if item["space_type"] == "private"}
        groups = {(item["platform_instance_id"], item["group_id"]) for item in items if item["space_type"] == "group"}
        user_names = await self.sources.get_user_display_names(owners) if self.sources and owners else {}
        group_names = {}
        if self.sources and groups:
            marks = ",".join("(?,?)" for _ in groups)
            names = await self.sources.database.fetchall(
                "SELECT platform_instance_id,group_id,display_name FROM group_display_names "
                f"WHERE (platform_instance_id,group_id) IN (VALUES {marks})",
                (value for pair in groups for value in pair))
            group_names = {(row["platform_instance_id"], row["group_id"]): row["display_name"] for row in names}
        for item in items:
            if item["space_type"] == "private":
                name = user_names.get(item["logical_user_id"], "未命名用户")
            else:
                platform, group_id = item["platform_instance_id"], item["group_id"]
                name = "群 " + group_names.get((platform, group_id), group_id)
            item["display_label"] = f"{item['persona_id']} · {name}"
        return items

    async def display_details(self, value):
        texts = []
        _display_fields(value, texts.append)
        ids = user_reference_ids(texts)
        names = await self.sources.get_user_display_names(ids) if self.sources and ids else {}
        return _display_fields(value, lambda text: render_display_text(text, names))

    async def render_items(self, items):
        # 版本操作中的前后标题与当前标题共用昵称表，不改写保存的稳定引用。
        texts = [text for item in items for text in memory_texts(item)]
        for item in items:
            if item.get("state") and item["state"].get("format") != "relationship.slots.v1":
                _display_fields(item["state"], texts.append)
            for derivation in item.get("derivations", []):
                if derivation.get("semantic"):
                    _display_fields(derivation["semantic"], texts.append)
                change = derivation.get("title_change")
                if change is not None:
                    texts.extend(value for value in (change.get("before"), change.get("after")) if isinstance(value, str))
        references = tuple(dict.fromkeys([*user_reference_ids(texts),
            *(item["logical_user_id"] for item in items if item.get("logical_user_id"))]))
        names = await self.sources.get_user_display_names(references) if references and self.sources is not None else {}
        result = []
        for item in items:
            rendered = render_memory_item(item, names, render_text=render_display_text)
            if item.get("logical_user_id"):
                rendered["logical_user_name"] = names.get(item["logical_user_id"]) or "未命名用户"
            if item.get("state") and item["state"].get("format") != "relationship.slots.v1":
                rendered["display_state"] = _display_fields(item["state"], lambda value: render_display_text(value, names))
            if "derivations" in item:
                rendered["derivations"] = []
                for derivation in item["derivations"]:
                    entry = dict(derivation)
                    if entry.get("semantic"):
                        entry["display_semantic"] = _display_fields(entry["semantic"], lambda value: render_display_text(value, names))
                    if "title_change" in entry:
                        change = dict(entry["title_change"])
                        for field in ("before", "after"):
                            value = change.get(field)
                            change["display_" + field] = render_display_text(value, names) if isinstance(value, str) else None
                        entry["title_change"] = change
                    rendered["derivations"].append(entry)
            result.append(rendered)
        return result

    async def context(self, space_id, kind, *, status="all", q="", limit=50, offset=0):
        _page(limit, offset, q)
        if kind not in _CONTEXT:
            raise ValueError("unknown context kind")
        table, key, text_field, statuses = _CONTEXT[kind]
        if status != "all" and status not in statuses:
            raise ValueError("unknown context status")
        user_ids = await self.sources.find_user_ids_by_display_name(q) if q and self.sources is not None else []
        async with self.memory.database.transaction(write=False) as connection:
            space = await _space(connection, space_id)
            conditions = ["space_id=?", "generation_id=?"]
            parameters = [space_id, space["generation_id"]]
            if kind == "relationships" and space["space_type"] != "private":
                return _listing(space, [], limit, offset, kind=kind, total=0)
            if status != "all":
                conditions.append("status=?")
                parameters.append(status)
            if q:
                searchable = f"{text_field} || ' ' || tags_json" if kind == "profiles" else text_field
                condition, values = _text_search(searchable, q, user_ids)
                conditions.append(condition)
                parameters.extend(values)
            rows, total = await _browse_page(
                connection, f"SELECT * FROM {table} WHERE {' AND '.join(conditions)}",
                parameters, f"updated_at DESC,{key}", limit, offset)
            pairs = {"profiles": (("tags_json", "tags"),), "relationships": (("state_json", "state"),),
                     "short_term": (("coverage_json", "coverage"),)}[kind]
            items = [_decode(row, pairs) for row in rows]
            result = _listing(space, items, limit, offset, kind=kind, total=total)
        result["items"] = await self.render_items(result["items"])
        return result

    async def nodes(self, space_id, *, status="all", node_type="all", q="", limit=50, offset=0,
                    sort="updated_desc", emotion="all", updated_from=None, updated_before=None,
                    importance_min=None, importance_max=None):
        _page(limit, offset, q)
        if status != "all" and status not in NODE_STATUSES:
            raise ValueError("unknown node status")
        if node_type != "all" and node_type not in NODE_TYPES:
            raise ValueError("unknown node type")
        if sort not in _NODE_ORDER or emotion not in (*EMOTION_TAGS, "all", "none"):
            raise ValueError("unknown node ordering or emotion")
        if any(value is not None and not math.isfinite(value) for value in (updated_from, updated_before)):
            raise ValueError("invalid content time")
        if updated_from is not None and updated_before is not None and updated_from >= updated_before:
            raise ValueError("invalid content time range")
        if any(value is not None and not 0 <= value <= 1 for value in (importance_min, importance_max)):
            raise ValueError("invalid importance")
        if importance_min is not None and importance_max is not None and importance_min > importance_max:
            raise ValueError("invalid importance range")
        user_ids = await self.sources.find_user_ids_by_display_name(q) if q and self.sources is not None else []
        async with self.memory.database.transaction(write=False) as connection:
            space = await _space(connection, space_id)
            conditions, parameters = ["n.space_id=?", "n.generation_id=?"], [space_id, space["generation_id"]]
            for field, value in (("status", status), ("node_type", node_type)):
                if value != "all":
                    conditions.append(f"n.{field}=?")
                    parameters.append(value)
            if emotion == "none":
                conditions.append("n.emotion_json IS NULL")
            elif emotion != "all":
                conditions.append("EXISTS (SELECT 1 FROM json_each(n.emotion_json,'$.tags') AS tag "
                                  "WHERE json_extract(tag.value,'$.tag')=?)")
                parameters.append(emotion)
            for clause, value in (("n.content_updated_at>=?", updated_from), ("n.content_updated_at<?", updated_before),
                                  ("n.importance>=?", importance_min), ("n.importance<=?", importance_max)):
                if value is not None:
                    conditions.append(clause)
                    parameters.append(value)
            if q:
                condition, values = _text_search("COALESCE(n.title,'') || ' ' || v.content_text", q, user_ids)
                conditions.append(condition)
                parameters.extend(values)
            rows, total = await _browse_page(
                connection, _NODE_SELECT + f" WHERE {' AND '.join(conditions)}", parameters,
                _NODE_ORDER[sort], limit, offset)
            items = [self.memory._node_from_row(row) for row in rows]
            result = _listing(space, items, limit, offset, total=total)
        result["items"] = await self.render_items(result["items"])
        return result

    async def notes(self, space_id, *, status="active", q="", limit=50, offset=0):
        _page(limit, offset, q)
        if status not in {"active", "archived", "all"}:
            raise ValueError("unknown note status")
        user_ids = await self.sources.find_user_ids_by_display_name(q) if q and self.sources is not None else []
        condition, values = _text_search("content_text", q, user_ids)
        async with self.memory.database.transaction(write=False) as connection:
            space = await _space(connection, space_id)
            rows, total = await _browse_page(
                connection, "SELECT * FROM notes WHERE space_id=? AND status IN ('active','archived') "
                f"AND (?='all' OR status=?) AND {condition}",
                (space_id, status, status, *values), "updated_at DESC,note_id", limit, offset)
            items = [_decode(row, (("metadata_json", "metadata"),)) for row in rows]
            result = _listing(space, items, limit, offset, total=total)
        references = user_reference_ids(note["content_text"] for note in result["items"])
        result["user_names"] = await self.sources.get_user_display_names(references) if references and self.sources else {}
        return result

    async def node_relations(self, space_id, node_id, *, limit=30, offset=0):
        """Browse topic members or a node's parent topics in the active generation."""
        _page(limit, offset, "")
        async with self.memory.database.transaction(write=False) as connection:
            space = await _space(connection, space_id)
            node = await _one(connection,
                "SELECT node_type FROM memory_nodes WHERE node_id=? AND space_id=? AND generation_id=?",
                (node_id, space_id, space["generation_id"]))
            if node is None:
                raise KeyError("node")
            members = node["node_type"] == "topic"
            endpoint, other = ("source_node_id", "target_node_id") if members else ("target_node_id", "source_node_id")
            types = "n.node_type IN ('event','atomic')" if members else "n.node_type='topic'"
            cursor = await connection.execute(
                _NODE_SELECT + f" JOIN memory_edges e ON e.{other}=n.node_id "
                f"WHERE e.{endpoint}=? AND e.relation_type='contains' AND {types} "
                "AND n.space_id=? AND n.generation_id=? AND e.generation_id=n.generation_id "
                "ORDER BY n.content_updated_at DESC,n.node_id LIMIT ? OFFSET ?",
                (node_id, space_id, space["generation_id"], limit + 1, offset))
            items = [self.memory._node_from_row(row) for row in await cursor.fetchall()]
            result = _listing(space, items, limit, offset, node_id=node_id,
                              relation="members" if members else "topics")
        result["items"] = await self.render_items(result["items"])
        return result

    async def source(self, space_id, source_id):
        # Raw history belongs to its captured identity scope, independently of
        # derived generations. Failed, not-yet-consumed windows remain reviewable.
        async with self.memory.database.transaction(write=False) as connection:
            space = await _space(connection, space_id)
        if self.sources is None or not is_supported_persona(space["persona_id"]):
            raise KeyError("source")
        try:
            records = await self.sources.get_records((source_id,))
        except ValueError as exc:
            raise KeyError("source") from exc
        record = records[0]
        spec = _record_spec(record)
        if spec is None or spec.scope_key != space["scope_key"]:
            raise KeyError("source")
        source = _decode(record, (("metadata_json", "metadata"),))
        return {"space_id": space_id, "source": source, "display_source": await self.display_details(source)}


def validate_note_operation(operation):
    if not isinstance(operation, dict):
        raise ValueError("note operation must be an object")
    action = operation.get("action")
    if not isinstance(action, str) or action not in {"create", "update", "archive", "restore", "delete"}:
        raise ValueError("unknown note action")
    required = {"action", "content_text"} if action == "create" else {"action", "note_id", "expected_updated_at"}
    if action == "update":
        required.add("content_text")
    if set(operation) != required:
        raise ValueError("invalid note operation fields")
    result = dict(operation)
    if "content_text" in result:
        if not isinstance(result["content_text"], str):
            raise ValueError("note content must be text")
        result["content_text"] = validate_note_content(result["content_text"])
    if "note_id" in result:
        if not isinstance(result["note_id"], str) or not result["note_id"] or len(result["note_id"]) > 200:
            raise ValueError("invalid note ID")
        value = result["expected_updated_at"]
        try:
            valid = type(value) in {int, float} and math.isfinite(value)
        except OverflowError:
            valid = False
        if not valid:
            raise ValueError("invalid expected revision")
    return result


class NoteService:
    """Notes survive generation rebuilds; revisions and scope leases protect edits."""

    def __init__(self, memory, jobs):
        self.memory = memory
        self.jobs = jobs

    async def _validate_current(self, connection, space_id, operation):
        space = await _space(connection, space_id)
        if not is_supported_persona(space["persona_id"]):
            raise ValueError("unsupported persona")
        note = None
        if operation["action"] != "create":
            note = await _one(connection, "SELECT * FROM notes WHERE note_id=? AND space_id=? AND status<>'deleted'",
                              (operation["note_id"], space_id))
            if note is None:
                raise KeyError("note")
            if note["updated_at"] != operation["expected_updated_at"]:
                raise JobConflict("note revision changed")
            expected_status = {"archive": "active", "restore": "archived"}.get(operation["action"])
            if expected_status is not None and note["status"] != expected_status:
                raise JobConflict("note status changed")
        return space, note

    async def enqueue(self, space_id, key, operation):
        operation = validate_note_operation(operation)
        payload = {"operation": operation}
        # Preserve duplicate submit semantics even after the first job changes the
        # note revision. JobStore repeats the comparison under its writer lock.
        existing = await self.memory.database.fetchone(
            "SELECT job_id FROM memory_jobs WHERE idempotency_key=?", (key,))
        if existing is None:
            async with self.memory.database.transaction(write=False) as connection:
                await self._validate_current(connection, space_id, operation)
        return await self.jobs.submit("note", space_id, payload, key)

    async def run(self, job):
        operation = validate_note_operation(job["payload"]["operation"])
        encoded = json.dumps(operation, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        space_id = job["scope_key"]
        async with self.memory.database.transaction() as connection:
            current = await assert_job_lease(connection, job["job_id"], job["lease_token"], scope_key=space_id)
            if current["kind"] != "note" or current["payload"] != {"operation": operation}:
                raise ValueError("unexpected note task payload")
            receipt = await _one(connection, "SELECT * FROM note_receipts WHERE job_id=?", (job["job_id"],))
            if receipt is not None:
                if receipt["space_id"] != space_id or receipt["input_digest"] != digest:
                    raise ValueError("note receipt differs from request")
                return {**json.loads(receipt["result_json"]), "replayed": True}
            space = await _space(connection, space_id)
            if not is_supported_persona(space["persona_id"]):
                return {"skipped": True, "reason": "unsupported_persona"}
            try:
                space, note = await self._validate_current(connection, space_id, operation)
            except JobConflict as exc:
                raise ReviewRequired("note_revision_changed") from exc
            except KeyError as exc:
                raise ReviewRequired("note_unavailable") from exc
            action = operation["action"]
            now = time.time()
            if action == "create":
                note_id = f"note_{uuid.uuid4().hex}"
                await connection.execute(
                    "INSERT INTO notes(note_id,space_id,persona_id,logical_user_id,content_text,source_kind,created_at,updated_at) "
                    "VALUES (?,?,?,?,?,'manual',?,?)", (note_id, space_id, space["persona_id"], space["logical_user_id"],
                    operation["content_text"], now, now))
                status = "active"
            else:
                note_id = note["note_id"]
                now = max(now, math.nextafter(note["updated_at"], math.inf))
                status = {"archive": "archived", "restore": "active", "delete": "deleted"}.get(action, note["status"])
                await connection.execute("UPDATE notes SET content_text=?,status=?,updated_at=?,deleted_at=? WHERE note_id=?",
                    (operation.get("content_text", note["content_text"]), status, now,
                     now if status == "deleted" else None, note_id))
            result = {"note_id": note_id, "space_id": space_id, "action": action, "status": status, "updated_at": now}
            await connection.execute("INSERT INTO note_receipts(job_id,space_id,note_id,input_digest,result_json,committed_at) "
                "VALUES (?,?,?,?,?,?)", (job["job_id"], space_id, note_id, digest, json.dumps(result, sort_keys=True), now))
            return {**result, "replayed": False}
