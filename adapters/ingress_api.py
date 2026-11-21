"""Host-authenticated API for durable source admission diagnostics and retry."""

from __future__ import annotations

import re
from functools import wraps
from typing import Any

from astrbot.api.web import error_response, json_response, request

PREFIX = "/astrbot_plugin_neomemory"


def _integer(value: Any, *, minimum: int = 0, maximum: int = 1_000_000) -> int:
    if isinstance(value, str) and re.fullmatch(r"[0-9]{1,7}", value):
        value = int(value)
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("invalid pagination")
    return value


def _source_id(value: Any) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 200:
        raise ValueError("invalid source identity")
    return value


def _guard(handler):
    @wraps(handler)
    async def guarded(self, *args, **kwargs):
        if not self._ready():
            return error_response(
                "原始记录待写服务尚未就绪。",
                status_code=503,
                data={"error_code": "ingress_unavailable"},
            )
        try:
            return await handler(self, *args, **kwargs)
        except KeyError:
            return error_response(
                "待写记录不存在，可能已经成功入库。",
                status_code=404,
                data={"error_code": "ingress_record_not_found"},
            )
        except (ValueError, TypeError):
            return error_response(
                "请求字段无效，请检查记录 ID 和分页参数。",
                status_code=400,
                data={"error_code": "invalid_ingress_request"},
            )
        except Exception:
            # Storage errors may contain source text or paths. Safe queue codes
            # are separately returned by ingress.health/list, never exception text.
            return error_response(
                "原始记录待写请求暂时失败，请刷新后重试。",
                status_code=503,
                data={"error_code": "ingress_request_failed"},
            )

    return guarded


class IngressAPI:
    def __init__(self, plugin: Any) -> None:
        self.plugin = plugin

    def register(self, context: Any) -> None:
        register = getattr(context, "register_web_api", None)
        if not callable(register):
            return
        for endpoint, method, handler in (
            ("ingress", "GET", self.records),
            ("ingress/record", "GET", self.record),
            ("ingress/retry", "POST", self.retry),
        ):
            register(
                f"{PREFIX}/{endpoint}",
                handler,
                [method],
                f"NeoMemory source ingress {endpoint}",
            )

    def _ready(self) -> bool:
        return bool(
            getattr(self.plugin, "ready", False)
            and not getattr(self.plugin, "terminating", False)
            and getattr(self.plugin, "ingress", None) is not None
        )

    @_guard
    async def records(self):
        limit = _integer(request.query.get("limit", 50), minimum=1, maximum=100)
        offset = _integer(request.query.get("offset", 0))
        rows = await self.plugin.ingress.list(limit=limit + 1, offset=offset)
        return json_response(
            {
                "items": rows[:limit],
                "limit": limit,
                "offset": offset,
                "has_more": len(rows) > limit,
                "health": await self.plugin.ingress.health(),
            }
        )

    @_guard
    async def record(self):
        source_id = _source_id(request.query.get("source_id"))
        result = await self.plugin.ingress.get(source_id, include_content=True)
        if result is None:
            raise KeyError("source")
        return json_response(result)

    @_guard
    async def retry(self):
        body = await request.json(default=None)
        if not isinstance(body, dict) or set(body) - {"source_id"}:
            raise ValueError("invalid body")
        readmitted = 0
        source_id = None
        if "source_id" in body:
            source_id = _source_id(body["source_id"])
            if await self.plugin.ingress.get(source_id) is None:
                raise KeyError("source")
        else:
            # Volatile emergency data cannot be called durable until re-admitted.
            readmitted = await self.plugin.retry_failed_sources()
        retried = await self.plugin.ingress.retry(source_id)
        return json_response(
            {
                "retried": retried,
                "readmitted": readmitted,
                "health": await self.plugin.ingress.health(),
            }
        )
