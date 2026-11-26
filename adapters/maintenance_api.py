"""Authenticated AstrBot Web API for durable maintenance and manual governance."""

from __future__ import annotations

import json
import re
from functools import wraps
from typing import Any

from astrbot.api.web import error_response, json_response, request

try:
    from ..core.extraction_window import ExtractionWindowPending
    from ..core import node_reconstruction as reconstruction
    from ..core.model_routing import ModelSelectionRequired
    from ..storage.topic_store import TopicStore
    from ..storage.console_store import ConsoleStore
    from ..storage.version_retention import mark_missing_version_references
    from ..storage.governance_store import validate_operation
    from ..storage.job_store import JobConflict, inspection_checkpoint
    from ..core.space_data import SpaceDataService, PROJECTS
except ImportError:
    from core.extraction_window import ExtractionWindowPending
    from core import node_reconstruction as reconstruction
    from core.model_routing import ModelSelectionRequired
    from storage.topic_store import TopicStore
    from storage.console_store import ConsoleStore
    from storage.version_retention import mark_missing_version_references
    from storage.governance_store import validate_operation
    from storage.job_store import JobConflict, inspection_checkpoint
    from core.space_data import SpaceDataService, PROJECTS


PREFIX = "/astrbot_plugin_neomemory"
KINDS = frozenset({"node_draft", "node_apply", "extract", "rebuild", "reindex", "governance", "reject", "topic_consolidate", "topic_rebuild", "profile_rebuild", "relationship_rebuild", "section_rebuild"})
SUMMARY_FIELDS = (
    "job_id", "kind", "scope_key", "state", "step", "progress_done", "progress_total",
    "attempt", "max_attempts", "automatic_origin", "error_type", "error_code", "created_at", "updated_at", "available_at",
)


def _summary(job: dict[str, Any]) -> dict[str, Any]:
    result = {field: job.get(field) for field in SUMMARY_FIELDS}
    if (job.get("payload") or {}).get("include_topics"):
        lanes = (job.get("checkpoint") or {}).get("section_progress") or (job.get("result") or {}).get("section_progress") or {}
        result["section_progress"] = {lane: {key: lanes.get(lane, {}).get(key, default)
            for key, default in (("step", "queued"), ("done", 0), ("total", None), ("state", "queued"),
                                 ("attempt", None), ("available_at", None), ("error_code", None))}
            for lane in ("context", "topics")}
    checkpoint = inspection_checkpoint(job) if job.get("checkpoint") else {}
    if window := checkpoint.get("window_index"):
        result["committed_window"] = dict(window)
    return result


def _text(value: Any, *, maximum: int = 200, empty: bool = False) -> str:
    if not isinstance(value, str) or len(value) > maximum or (not empty and not value.strip()):
        raise ValueError("invalid text field")
    return value


def _integer(value: Any, *, minimum: int = 0, maximum: int = 1_000_000) -> int:
    if isinstance(value, str) and re.fullmatch(r"[0-9]{1,7}", value):
        value = int(value)
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("invalid pagination")
    return value


def _guard(handler):
    @wraps(handler)
    async def guarded(self, *args, **kwargs):
        if not self._ready():
            return error_response("维护服务尚未就绪。", status_code=503, data={"error_code": "maintenance_unavailable"})
        try:
            return await handler(self, *args, **kwargs)
        except reconstruction.ReconstructionUnavailable as exc:
            return error_response(str(exc), status_code=409, data={"error_code": "reconstruction_unavailable"})
        except JobConflict:
            return error_response("当前操作与运行任务或已有版本冲突，请刷新后重试。", status_code=409,
                                  data={"error_type": "JobConflict", "error_code": "maintenance_conflict"})
        except KeyError:
            return error_response("任务、空间或节点不存在。", status_code=404,
                                  data={"error_code": "maintenance_not_found"})
        except ModelSelectionRequired:
            return error_response("请选择本次任务使用的模型。", status_code=400, data={"error_code": "model_required"})
        except ExtractionWindowPending as waiting:
            return error_response(str(waiting), status_code=409, data={"error_code": "extraction_window_pending",
                                  "pending_count": waiting.pending, "min_messages": waiting.minimum})
        except (ValueError, TypeError):
            return error_response("请求字段无效，请检查必填项、范围和治理载荷格式。", status_code=400,
                                  data={"error_code": "invalid_maintenance_request"})
        except Exception:
            # Provider/SQLite exceptions may contain credentials or raw text.
            # The durable job's safe error identifiers remain inspectable.
            return error_response("维护请求暂时失败，请刷新任务状态后重试。", status_code=500,
                                  data={"error_code": "maintenance_request_failed"})
    return guarded


class MaintenanceAPI:
    def __init__(self, plugin: Any) -> None:
        self.plugin = plugin

    def register(self, context: Any) -> None:
        register = getattr(context, "register_web_api", None)
        if not callable(register):
            return
        for endpoint, method, handler in (
            ("tasks", "GET", self.tasks), ("task", "GET", self.task),
            ("extraction/backlog", "GET", self.extraction_backlog),
            ("recall/test", "POST", self.test_recall),
            ("recall/history", "GET", self.recall_history),
            ("recall/recording", "POST", self.recall_recording),
            ("topics/rebuild-preview", "GET", self.topic_rebuild_preview),
            ("tasks/model-preview", "POST", self.model_preview),
            ("availability", "GET", self.availability), ("tasks", "POST", self.submit),
            ("tasks/retry", "POST", self.retry), ("tasks/cancel", "POST", self.cancel),
            ("tasks/delete", "POST", self.delete),
            ("tasks/clear-completed", "POST", self.clear_completed),
            ("space/clear", "POST", self.clear_space), ("space/delete", "POST", self.delete_space),
            ("memory/spaces", "GET", self.spaces), ("memory/nodes", "GET", self.nodes),
            ("memory/versions", "GET", self.versions), ("memory/node-draft", "GET", self.node_draft),
            ("memory/relations", "GET", self.relations), ("memory/node-draft/discard", "POST", self.discard_node_draft),
        ):
            register(f"{PREFIX}/{endpoint}", handler, [method], f"NeoMemory maintenance {endpoint}")

    def _ready(self) -> bool:
        return bool(getattr(self.plugin, "ready", False) and not getattr(self.plugin, "terminating", False)
                    and getattr(self.plugin, "maintenance", None) is not None
                    and getattr(self.plugin, "jobs", None) is not None
                    and getattr(self.plugin, "memory_store", None) is not None)

    async def _body(self, allowed: set[str], required: set[str]) -> dict[str, Any]:
        body = await request.json(default=None)
        if not isinstance(body, dict) or set(body) - allowed or not required <= set(body):
            raise ValueError("invalid request body fields")
        if len(json.dumps(body, ensure_ascii=False, allow_nan=False).encode("utf-8")) > 300_000:
            raise ValueError("request body is too large")
        return body

    async def _space(self, value: Any) -> dict[str, Any]:
        space_id = _text(value, maximum=256)
        space = await self.plugin.memory_store.get_space(space_id)
        # These mutation endpoints accept a stable space_id, not a scope alias.
        if space is None or space["space_id"] != space_id:
            raise KeyError("space")
        return space

    @_guard
    async def test_recall(self):
        body = await self._body({"space_id", "query"}, {"space_id", "query"})
        space = await self._space(body["space_id"])
        query = _text(body["query"], maximum=4000).strip()
        snapshot = await self.plugin.runtime.test_recall(space["space_id"], query)
        if last_index := await self.plugin.jobs.latest_index(space["space_id"]):
            snapshot["last_index"] = last_index
        return json_response(snapshot)

    @_guard
    async def recall_history(self):
        space_id = request.query.get("space_id")
        history = self.plugin.runtime.recall_history
        records = []
        if space_id is not None:
            space = await self._space(space_id)
            records = await history.list(space["space_id"])
        return json_response({"enabled": history.enabled, "records": records})

    @_guard
    async def recall_recording(self):
        body = await self._body({"enabled"}, {"enabled"})
        if type(body["enabled"]) is not bool:
            raise ValueError("expected a recording switch")
        history = self.plugin.runtime.recall_history
        await history.set_enabled(body["enabled"])
        return json_response({"enabled": history.enabled})

    @_guard
    async def tasks(self):
        scope = request.query.get("scope_key")
        if scope is not None:
            scope = _text(scope, maximum=256)
        limit = _integer(request.query.get("limit", 50), minimum=1, maximum=100)
        offset = _integer(request.query.get("offset", 0))
        history_limit = _integer(request.query.get("history_limit", 50), minimum=1, maximum=499)
        history_offset = _integer(request.query.get("history_offset", 0))
        jobs = await self.plugin.jobs.list(scope_key=scope, limit=limit + 1, offset=offset, visible_only=True)
        items = []
        for job in jobs[:limit]:
            items.append({**_summary(job), **await self.plugin.jobs.recovery_actions(job)})
        history_jobs = await self.plugin.jobs.list_hidden_successes(
            scope_key=scope, limit=history_limit + 1, offset=history_offset,
        )
        history = []
        for job in history_jobs[:history_limit]:
            history.append({**_summary(job), **await self.plugin.jobs.recovery_actions(job)})
        return json_response({"items": items,
                              "limit": limit, "offset": offset, "has_more": len(jobs) > limit,
                              "history": history, "history_limit": history_limit,
                              "history_offset": history_offset, "history_has_more": len(history_jobs) > history_limit,
                              "completed_counts": await self.plugin.jobs.completed_counts(scope_key=scope)})

    @_guard
    async def task(self):
        job_id = _text(request.query.get("job_id"))
        result = await self.plugin.maintenance.describe(job_id)
        if result is None:
            raise KeyError("job")
        return json_response(result)

    @_guard
    async def extraction_backlog(self):
        space = await self._space(request.query.get("space_id"))
        backlog = await self.plugin.maintenance.extraction_backlog(space)
        return json_response({"pending_count": backlog.pending, "hint": backlog.hint})

    @_guard
    async def topic_rebuild_preview(self):
        space = await self._space(request.query.get("space_id"))
        preview = await TopicStore(self.plugin.memory_store).rebuild_preview(space["space_id"])
        return json_response(preview)

    @_guard
    async def availability(self):
        space = await self._space(request.query.get("space_id"))
        result = await self.plugin.jobs.availability(space["space_id"])
        result["automatic"] = await self.plugin.maintenance.automatic_status(space["space_id"])
        return json_response(result)

    @staticmethod
    def _sections(kind, body, space):
        if kind == "section_rebuild":
            sections = body.get("sections")
            if not isinstance(sections, list) or not sections or any(
                    section not in ("topics", "profiles", "relationship") for section in sections):
                raise ValueError("select at least one supported section")
            selected = set(sections)
            if "relationship" in selected and space["space_type"] != "private":
                raise ValueError("relationships require a private space")
            context = selected - {"topics"}
            kind = {frozenset(): "topic_rebuild", frozenset({"profiles"}): "profile_rebuild",
                    frozenset({"relationship"}): "relationship_rebuild",
                    frozenset({"profiles", "relationship"}): "context_rebuild"}[frozenset(context)]
            return kind, bool(context and "topics" in selected)
        elif "sections" in body:
            raise ValueError("sections require a section rebuild")
        return kind, False

    @_guard
    async def node_draft(self):
        space = await self._space(request.query.get("space_id"))
        node_id = _text(request.query.get("node_id"))
        row = await self.plugin.memory_store.database.fetchone(
            "SELECT job_id FROM memory_jobs WHERE scope_key=? AND kind='node_draft' "
            "AND json_extract(payload_json,'$.operation.node_id')=? ORDER BY created_at DESC,rowid DESC LIMIT 1",
            (space["space_id"], node_id))
        if not row:
            return json_response({"job": None})
        job = await self.plugin.jobs.get(row["job_id"])
        result = job and job["result"] or {}
        disposition = None
        if result.get("discarded"):
            disposition = "discarded"
        elif "content" in result:
            applied = await self.plugin.memory_store.database.fetchone(
                "SELECT a.state,r.job_id AS receipt FROM memory_jobs a LEFT JOIN governance_receipts r USING(job_id) "
                "WHERE a.scope_key=? AND a.kind='node_apply' AND json_extract(a.payload_json,'$.operation.draft_id')=? "
                "ORDER BY a.created_at DESC,a.rowid DESC LIMIT 1", (space["space_id"], row["job_id"]))
            if applied and applied["receipt"]:
                disposition = "applied"
            elif applied and applied["state"] in {"queued", "running", "retry_wait"}:
                disposition = "applying"
        return json_response({"job": _summary(job) if job else None, "disposition": disposition, "draft": {
            k: result[k] for k in ("original", "content", "source_count", "member_count", "missing_versions", "missing_sources")
            if k in result}, "version": result.get("snapshot", {}).get("content_version")})

    @_guard
    async def discard_node_draft(self):
        body = await self._body({"job_id", "space_id"}, {"job_id", "space_id"})
        space = await self._space(body["space_id"])
        await self.plugin.jobs.discard_node_draft(space["space_id"], _text(body["job_id"]))
        return json_response({"discarded": True})

    @_guard
    async def model_preview(self):
        body = await self._body({"kind", "space_id", "sections", "job_id", "node_id"}, set())
        if "job_id" in body:
            job = await self.plugin.jobs.get(_text(body["job_id"]))
            if not job:
                raise KeyError("unknown job")
            if job["kind"] == "short_term":
                return json_response(await self.plugin.maintenance.short_term.model_preview(job))
            space = await self._space(job["scope_key"])
            kind = job["kind"]
            include_topics = bool(job["payload"].get("include_topics"))
        else:
            space = await self._space(body.get("space_id"))
            kind, include_topics = self._sections(_text(body.get("kind")), body, space)
        if kind == "node_draft":
            node_id = job["payload"]["operation"]["node_id"] if "job_id" in body else _text(body.get("node_id"))
            engine = self.plugin.maintenance
            node, _, members, records, info = await reconstruction.reconstruction_input(engine, space, node_id)
            route = await reconstruction.model_route(engine, space, node, members, records, {})
            lane = "topic" if node["node_type"] == "topic" else "window"
            return json_response({lane: engine._route_summary([route]), "evidence": info})
        return json_response(await self.plugin.maintenance.model_preview(
            kind, space, include_topics=include_topics, retry="job_id" in body))

    @_guard
    async def submit(self):
        body = await self._body({"kind", "space_id", "idempotency_key", "operation", "reason", "failed_job_id", "sections", "model_selection"},
                                {"kind", "space_id", "idempotency_key"})
        kind = _text(body["kind"])
        if kind not in KINDS:
            raise ValueError("unsupported task kind")
        key = _text(body["idempotency_key"])
        space = await self._space(body["space_id"])
        reason = _text(body.get("reason", ""), maximum=2000, empty=True)
        operation = None
        failed_job_id = None
        if kind == "governance":
            if "failed_job_id" in body:
                raise ValueError("governance cannot reject a task")
            operation = validate_operation(body.get("operation"))
        elif kind in {"node_draft", "node_apply"}:
            operation = body.get("operation")
            fields = {"node_id"} if kind == "node_draft" else {"draft_id", "content"}
            if not isinstance(operation, dict) or set(operation) != fields:
                raise ValueError("invalid reconstruction operation")
            for field in fields:
                _text(operation[field], maximum=120_000 if field == "content" else 200)
        elif "operation" in body:
            raise ValueError("only governance accepts operation")
        if kind == "reject":
            failed_job_id = _text(body.get("failed_job_id"))
            if not reason.strip():
                raise ValueError("rejection needs a reason")
        elif "failed_job_id" in body:
            raise ValueError("only rejection accepts failed_job_id")
        options = {}
        kind, include_topics = self._sections(kind, body, space)
        if include_topics:
            options["include_topics"] = True
        if "model_selection" in body:
            options["model_selection"] = body["model_selection"]
        job = await self.plugin.maintenance.enqueue(kind, space["space_id"], key, **options,
                                                    operation=operation, reason=reason, failed_job_id=failed_job_id)
        return json_response(_summary(job))

    @_guard
    async def retry(self):
        body = await self._body({"job_id", "fresh", "regenerate_window", "model_selection"}, {"job_id"})
        job_id = _text(body["job_id"])
        fresh = body.get("fresh", False)
        regenerate = body.get("regenerate_window", False)
        if type(fresh) is not bool or type(regenerate) is not bool or (fresh and regenerate):
            raise ValueError("retry modes must be mutually exclusive booleans")
        kwargs = {"fresh": fresh}
        if "model_selection" in body:
            kwargs["model_selection"] = body["model_selection"]
        if regenerate:
            kwargs["regenerate_window"] = True
        return json_response(_summary(await self.plugin.maintenance.retry(job_id, **kwargs)))

    @_guard
    async def cancel(self):
        body = await self._body({"job_id"}, {"job_id"})
        return json_response(_summary(await self.plugin.jobs.cancel(_text(body["job_id"]))))

    @_guard
    async def delete(self):
        body = await self._body({"job_id"}, {"job_id"})
        return json_response(await self.plugin.maintenance.delete(_text(body["job_id"])))

    @_guard
    async def clear_completed(self):
        body = await self._body({"scope_key"}, set())
        scope = _text(body["scope_key"], maximum=256) if "scope_key" in body else None
        return json_response(await self.plugin.maintenance.clear_completed(scope_key=scope))

    def _space_data(self):
        runtime = getattr(self.plugin, "runtime", None)
        return SpaceDataService(
            self.plugin.source_store, self.plugin.memory_store,
            ingress_store=getattr(self.plugin, "ingress_store", None),
            cache=getattr(runtime, "cache", None), image_cache=getattr(self.plugin, "image_cache", None),
        )

    @_guard
    async def clear_space(self):
        body = await self._body({"space_id", "projects", "confirmation"}, {"space_id", "projects", "confirmation"})
        space_id = _text(body["space_id"], maximum=256)
        if body["confirmation"] != space_id:
            raise ValueError("space confirmation does not match")
        projects = body["projects"]
        if not isinstance(projects, list) or not projects or any(project not in PROJECTS for project in projects):
            raise ValueError("invalid cleanup projects")
        space = await self._space(space_id)
        return json_response(await self._space_data().clear(space, projects))

    @_guard
    async def delete_space(self):
        body = await self._body({"space_id", "confirmation"}, {"space_id", "confirmation"})
        space_id = _text(body["space_id"], maximum=256)
        if body["confirmation"] != space_id:
            raise ValueError("space confirmation does not match")
        space = await self._space(space_id)
        return json_response(await self._space_data().clear(space, (), destroy=True))

    @_guard
    async def spaces(self):
        limit = _integer(request.query.get("limit", 100), minimum=1, maximum=100)
        offset = _integer(request.query.get("offset", 0))
        console = ConsoleStore(self.plugin.memory_store, getattr(self.plugin, "source_store", None))
        return json_response(await console.spaces(limit, offset))

    @_guard
    async def nodes(self):
        space = await self._space(request.query.get("space_id"))
        limit = _integer(request.query.get("limit", 50), minimum=1, maximum=100)
        offset = _integer(request.query.get("offset", 0))
        bounds = {key: float(request.query[key])
                  for key in ("updated_from", "updated_before", "importance_min", "importance_max")
                  if request.query.get(key) not in (None, "")}
        result = await ConsoleStore(self.plugin.memory_store, getattr(self.plugin, "source_store", None)).nodes(
            space["space_id"], limit=limit, offset=offset,
            status=_text(request.query.get("status", "all")),
            node_type=_text(request.query.get("node_type", "all")),
            q=_text(request.query.get("q", ""), maximum=1000, empty=True),
            sort=_text(request.query.get("sort", "updated_desc")),
            emotion=_text(request.query.get("emotion", "all")), **bounds,
        )
        return json_response(result)

    @_guard
    async def relations(self):
        space = await self._space(request.query.get("space_id"))
        result = await ConsoleStore(self.plugin.memory_store, getattr(self.plugin, "source_store", None)).node_relations(
            space["space_id"], _text(request.query.get("node_id")),
            limit=_integer(request.query.get("limit", 30), minimum=1, maximum=100),
            offset=_integer(request.query.get("offset", 0)),
        )
        return json_response(result)

    @_guard
    async def versions(self):
        kind = _text(request.query.get("kind", "node"))
        if kind not in {"node", "profile", "relationship"}:
            raise ValueError("unsupported version kind")
        entity_id = _text(request.query.get("entity_id", request.query.get("node_id")))
        memory = self.plugin.memory_store
        if kind == "node":
            entity = await memory.get_node(entity_id)
        else:
            table, key, stored, decoded = {
                "profile": ("user_profiles", "profile_id", "tags_json", "tags"),
                "relationship": ("relationship_states", "relationship_id", "state_json", "state"),
            }[kind]
            row = await memory.database.fetchone(f"SELECT * FROM {table} WHERE {key}=?", (entity_id,))
            entity = dict(row) if row else None
            if entity:
                entity[decoded] = json.loads(entity.pop(stored))
        if entity is None:
            raise KeyError("memory entity")
        if "space_id" in request.query and _text(request.query["space_id"], maximum=256) != entity["space_id"]:
            raise KeyError("memory entity outside requested space")
        generation = await memory.get_active_generation(entity["space_id"])
        if generation is None or generation["generation_id"] != entity["generation_id"]:
            raise JobConflict("memory entity belongs to a retired generation")
        versions = await getattr(memory, f"list_{kind}_versions")(entity_id)
        version_no = request.query.get("version_no")
        if version_no is not None:
            version_no = _integer(version_no, minimum=1)
            shown = [item for item in versions if item["version_no"] == version_no]
            if not shown:
                raise KeyError("memory version")
        else:
            shown = list(reversed(versions))
        await mark_missing_version_references(memory.database, shown)
        console = ConsoleStore(memory, getattr(self.plugin, "source_store", None))
        rendered = await console.render_items([entity, *shown])
        result = {"kind": kind, "entity_id": entity_id, "space_id": entity["space_id"],
                  "entity": rendered[0], "items": rendered[1:],
                  "truncated": False,
                  "earliest_retained_version": versions[0]["version_no"] if versions else None}
        if kind == "node":
            result.update(node=rendered[0], sources=await memory.list_node_sources(entity_id),
                          sources_scope="entity_cumulative",
                          participants=await memory.list_node_participants(entity_id),
                          edges=await memory.list_edges(entity_id))
            result["display_relations"] = await console.display_details({
                "participants": result["participants"], "edges": result["edges"]})
        return json_response(result)
