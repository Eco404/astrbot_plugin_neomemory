"""Audit endpoints and operation capture at the authenticated WebUI boundary."""

import json
from functools import wraps

from astrbot.api.web import error_response, json_response, request

from .maintenance_api import PREFIX

_OPERATIONS = {
    "bindings": "修改账号绑定", "models/test": "测试模型连接", "models/probe": "测试模型连接",
    "settings": "保存插件设置", "space/policy": "保存空间设置", "appearance": "保存外观设置",
    "text-cleaning": "保存文本清理规则", "identities/rename": "重命名逻辑用户",
    "identities/apply": "变更账号绑定", "recall/recording": "设置召回记录", "recall/test": "测试召回",
    "ingress/retry": "重试待写记录", "notes": "治理笔记", "tasks": "提交后台任务",
    "tasks/retry": "重试任务", "tasks/cancel": "取消任务", "tasks/delete": "删除任务",
    "tasks/clear-completed": "清空已完成任务",
    "space/clear": "按项目清空记忆空间", "space/delete": "完整删除记忆空间",
}


class AuditAPI:
    def __init__(self, plugin):
        self.plugin = plugin

    def register_web_api(self, route, handler, methods, desc):
        endpoint = route.removeprefix(PREFIX + "/")
        if methods == ["POST"] and endpoint in _OPERATIONS:
            handler = self._observe(handler, endpoint)
        self.plugin.context.register_web_api(route, handler, methods, desc)

    def _observe(self, handler, endpoint):
        @wraps(handler)
        async def observed(*args, **kwargs):
            audit = self.plugin.audit
            if audit is None or not audit.enabled:
                return await handler(*args, **kwargs)
            actor = getattr(request, "username", None) or "WebUI"
            try:
                result = await handler(*args, **kwargs)
            except Exception as exc:
                await audit.record("error", "operation", _OPERATIONS[endpoint], actor=actor, error_type=type(exc).__name__)
                raise
            status = result.status_code
            details = {"actor": actor, "status": status}
            if status < 400:
                body = await request.json(default={})
                if isinstance(body, dict):
                    for key in ("job_id", "space_id", "scope_key", "logical_user_id", "action", "kind",
                                "fresh", "regenerate_window", "enabled", "node_id", "note_id", "projects"):
                        if key in body:
                            details[key] = body[key]
                    if endpoint == "identities/apply":
                        details.update({key: body[key] for key in ("account_id", "platform_instance_id")})
                    if isinstance(body.get("operation"), dict):
                        details.update({k: v for k, v in body["operation"].items()
                                        if k in ("action", "node_id", "profile_id", "relationship_id", "state_id", "entity_kind", "entity_id")})
                    if endpoint == "settings":
                        details["fields"] = sorted(body.get("changes", {}))
                data = json.loads(result.body)
                if isinstance(data, dict):
                    details.update({k: data[k] for k in ("job_id", "deleted_count", "automatic_count", "manual_count", "retried", "applied") if k in data})
            else:
                error = json.loads(result.body).get("data") or {}
                if "error_code" in error:
                    details["error_code"] = error["error_code"]
            level = "error" if status >= 500 else "warning" if status >= 400 else "info"
            await audit.record(level, "operation", _OPERATIONS[endpoint], **details)
            return result
        return observed

    def register(self):
        for path, method, handler in (("logs", "GET", self.listing), ("logs/settings", "POST", self.configure),
                                      ("logs/clear", "POST", self.clear)):
            self.register_web_api(f"{PREFIX}/{path}", handler, [method], f"NeoMemory {path}")

    async def listing(self):
        if not self.plugin.ready:
            return error_response("插件尚未就绪。", status_code=503)
        args = {key: request.query.get(key, "") for key in ("day", "level", "category", "keyword", "cursor")}
        try:
            return json_response(await self.plugin.audit.query(**args))
        except ValueError:
            return error_response("无效的日志筛选条件或分页位置。", status_code=400)

    async def configure(self):
        if not self.plugin.ready:
            return error_response("插件尚未就绪。", status_code=503)
        body = await request.json(default={})
        try:
            if not isinstance(body, dict):
                raise ValueError()
            return json_response(await self.plugin.audit.configure(body.get("enabled"), body.get("days")))
        except ValueError:
            return error_response("请选择记录开关和 1 至 30 天的保留期限。", status_code=400)

    async def clear(self):
        if not self.plugin.ready:
            return error_response("插件尚未就绪。", status_code=503)
        body = await request.json(default=None)
        if not isinstance(body, dict) or body.get("confirm") is not True:
            return error_response("请确认清空全部插件日志。", status_code=400)
        await self.plugin.audit.clear()
        return json_response({"cleared": True})
