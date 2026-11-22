"""Authenticated host Web routes for settings and model probes."""

from __future__ import annotations

import json
from functools import wraps
from urllib.parse import quote, unquote
from typing import Any

from astrbot.api.web import error_response, json_response, request

try:
    from ..core.jobs import ReviewRequired
    from ..core.settings import (
        SettingsApplyError,
        SettingsConflict,
        SettingsSaveError,
        SettingsUnavailable,
    )
    from ..storage.job_store import JobConflict
except ImportError:
    from core.jobs import ReviewRequired
    from core.settings import (
        SettingsApplyError,
        SettingsConflict,
        SettingsSaveError,
        SettingsUnavailable,
    )
    from storage.job_store import JobConflict

from .maintenance_api import PREFIX

try:
    from ..storage.space_policy import PERSONA_DEFAULTS, SPACE_DEFAULTS
except ImportError:
    from storage.space_policy import PERSONA_DEFAULTS, SPACE_DEFAULTS


def _guard(handler):
    @wraps(handler)
    async def guarded(self, *args, **kwargs):
        try:
            return await handler(self, *args, **kwargs)
        except (SettingsConflict, JobConflict):
            return error_response("配置已变化或已有相关任务，请刷新；若宿主配置被外部修改，请先在宿主重载插件。", status_code=409,
                                  data={"error_code": "settings_or_model_test_conflict"})
        except KeyError:
            return error_response("记忆空间不存在。", status_code=404)
        except SettingsUnavailable:
            return error_response("宿主配置当前无法保存。", status_code=503,
                                  data={"error_code": "settings_unavailable"})
        except SettingsSaveError:
            return error_response("配置保存失败，当前修改未生效。", status_code=500,
                                  data={"error_code": "settings_save_failed"})
        except SettingsApplyError:
            return error_response("配置已保存，但应用失败。请重新读取设置后重试应用。", status_code=500,
                                  data={"error_code": "settings_apply_failed", "applied": False})
        except (ValueError, TypeError):
            return error_response("请求字段、类型、范围或门限顺序无效。", status_code=400,
                                  data={"error_code": "invalid_settings_or_model_request"})
        except Exception:
            return error_response("操作暂时失败，请刷新后重试。", status_code=500,
                                  data={"error_code": "settings_or_model_request_failed"})
    return guarded


class SettingsAPI:
    def __init__(self, plugin: Any):
        self.plugin = plugin

    def register(self, context):
        register = getattr(context, "register_web_api", None)
        if not callable(register):
            return
        for path, method, handler in (("settings", "GET", self.get), ("settings", "POST", self.save),
                                      ("space/policy", "GET", self.get_space_policy), ("space/policy", "POST", self.save_space_policy),
                                      ("appearance", "GET", self.get_appearance), ("appearance", "POST", self.save_appearance),
                                      ("text-cleaning", "GET", self.get_cleaning), ("text-cleaning", "POST", self.save_cleaning),
                                      ("models/probe", "POST", self.probe)):
            register(f"{PREFIX}/{path}", handler, [method], f"NeoMemory {path}")

    async def _body(self, fields, *, optional=(), max_bytes=100_000):
        body = await request.json(default=None)
        if not isinstance(body, dict) or not fields <= set(body) <= fields.union(optional):
            raise ValueError("invalid request body")
        if max_bytes is not None and len(json.dumps(body, ensure_ascii=False, allow_nan=False).encode()) > max_bytes:
            raise ValueError("request exceeds size budget")
        return body

    @_guard
    async def get(self):
        return json_response(await self.plugin.settings.get())

    @_guard
    async def save(self):
        body = await self._body({"revision", "changes"})
        return json_response(await self.plugin.settings.save(**body))

    @_guard
    async def get_space_policy(self):
        if not self.plugin.ready or self.plugin.terminating:
            raise SettingsUnavailable()
        return json_response(await self.plugin.memory_store.get_space_policy(request.query.get("space_id")))

    @_guard
    async def save_space_policy(self):
        if not self.plugin.ready or self.plugin.terminating:
            raise SettingsUnavailable()
        body = await self._body({"space_id"}, optional=PERSONA_DEFAULTS | SPACE_DEFAULTS)
        space_id = body.pop("space_id")
        return json_response(await self.plugin.memory_store.set_space_policy(space_id, body))

    @_guard
    async def get_appearance(self):
        stored = request.cookies.get("neomemory_appearance", "")
        return json_response(json.loads(unquote(stored)) if stored else {}, headers={"Cache-Control": "no-store"})

    @_guard
    async def save_appearance(self):
        values = await self._body({"mode", "hue", "saturation", "lightness", "opacity"})
        response = json_response(values, headers={"Cache-Control": "no-store"})
        response.set_cookie("neomemory_appearance", quote(json.dumps(values, separators=(",", ":"))),
                            max_age=365 * 86400, httponly=True, samesite="lax")
        return response

    @_guard
    async def get_cleaning(self):
        if not self.plugin.ready or self.plugin.terminating:
            raise SettingsUnavailable()
        return json_response(self.plugin.cleaning_settings.config)

    @_guard
    async def save_cleaning(self):
        if not self.plugin.ready or self.plugin.terminating:
            raise SettingsUnavailable()
        body = await self._body({"groups", "rules", "revision"}, optional=("root_order",), max_bytes=None)
        try:
            return json_response(await self.plugin.cleaning_settings.save(body))
        except SettingsConflict:
            return error_response("文本清理规则已在其他页面更新，请刷新后再编辑。", status_code=409)
        except ValueError as exc:
            return error_response(str(exc), status_code=400)

    @_guard
    async def probe(self):
        if (not getattr(self.plugin, "ready", False) or getattr(self.plugin, "terminating", False)
                or getattr(self.plugin, "model_tests", None) is None):
            return error_response("模型测试服务尚未就绪。", status_code=503,
                                  data={"error_code": "model_test_unavailable"})
        body = await self._body({"kind", "provider_id"}, optional=("idempotency_key",))
        try:
            return json_response(await self.plugin.model_tests.probe(kind=body["kind"], provider_id=body["provider_id"]))
        except ReviewRequired as exc:
            message = {
                "model_test_configuration_changed": "测试期间模型配置已变化，请重新测试。",
                "model_test_provider_unavailable": "所选模型当前不可用。",
                "model_test_timeout": "模型连通性测试超时。",
                "model_test_provider_failed": "模型调用失败，请检查 Provider 配置。",
            }[exc.code]
            return error_response(message, status_code=502, data={"error_code": exc.code})
