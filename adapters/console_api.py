"""Authenticated console reads and durable manual note commands."""

from __future__ import annotations

import json
from functools import wraps
from typing import Any

from astrbot.api.web import error_response, json_response, request

try:
    from ..storage.job_store import JobConflict
except ImportError:
    from storage.job_store import JobConflict

from .maintenance_api import PREFIX, _integer, _summary, _text


def _guard(handler):
    @wraps(handler)
    async def guarded(self, *args, **kwargs):
        if not self._ready():
            return error_response("记忆查阅服务尚未就绪。", status_code=503,
                                  data={"error_code": "console_unavailable"})
        try:
            return await handler(self, *args, **kwargs)
        except JobConflict:
            return error_response("当前空间有运行任务，或内容版本已更新，请刷新后重试。", status_code=409,
                                  data={"error_code": "console_conflict"})
        except KeyError:
            return error_response("内容不在当前空间中、已不可用，或原文已清理。", status_code=404,
                                  data={"error_code": "console_not_found"})
        except (ValueError, TypeError):
            return error_response("请求字段无效，请检查参数和笔记内容。", status_code=400,
                                  data={"error_code": "invalid_console_request"})
        except Exception:
            return error_response("记忆查阅请求暂时失败，请稍后重试。", status_code=500,
                                  data={"error_code": "console_request_failed"})
    return guarded


class ConsoleAPI:
    def __init__(self, plugin: Any):
        self.plugin = plugin

    def register(self, context):
        register = getattr(context, "register_web_api", None)
        if not callable(register):
            return
        for endpoint, method, handler in (
            ("memory/context", "GET", self.context), ("memory/source", "GET", self.source),
            ("notes", "GET", self.notes), ("notes", "POST", self.submit_note),
        ):
            register(f"{PREFIX}/{endpoint}", handler, [method], f"NeoMemory console {endpoint}")

    def _ready(self):
        return bool(getattr(self.plugin, "ready", False) and not getattr(self.plugin, "terminating", False)
                    and getattr(self.plugin, "console_store", None) is not None
                    and getattr(self.plugin, "note_service", None) is not None)

    def _page(self, *, default_status):
        return {"status": _text(request.query.get("status", default_status)),
                "q": _text(request.query.get("q", ""), maximum=1000, empty=True),
                "limit": _integer(request.query.get("limit", 50), minimum=1, maximum=100),
                "offset": _integer(request.query.get("offset", 0))}

    @_guard
    async def context(self):
        space_id = _text(request.query.get("space_id"), maximum=256)
        kind = _text(request.query.get("kind"))
        return json_response(await self.plugin.console_store.context(space_id, kind, **self._page(default_status="all")))

    @_guard
    async def source(self):
        space_id = _text(request.query.get("space_id"), maximum=256)
        source_id = _text(request.query.get("source_id"), maximum=256)
        return json_response(await self.plugin.console_store.source(space_id, source_id))

    @_guard
    async def notes(self):
        space_id = _text(request.query.get("space_id"), maximum=256)
        return json_response(await self.plugin.console_store.notes(space_id, **self._page(default_status="active")))

    @_guard
    async def submit_note(self):
        body = await request.json(default=None)
        if not isinstance(body, dict) or not {"space_id", "idempotency_key", "action"} <= set(body):
            raise ValueError("invalid note body")
        if len(json.dumps(body, ensure_ascii=False, allow_nan=False).encode("utf-8")) > 100_000:
            raise ValueError("note request exceeds limit")
        if body["action"] == "create":
            return error_response("新建笔记仅供 Agent 使用。", status_code=400,
                                  data={"error_code": "invalid_console_request"})
        space_id = _text(body["space_id"], maximum=256)
        key = _text(body["idempotency_key"], maximum=200)
        operation = {name: value for name, value in body.items() if name not in {"space_id", "idempotency_key"}}
        job = await self.plugin.note_service.enqueue(space_id, key, operation)
        return json_response(_summary(job))
