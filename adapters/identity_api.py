"""Host-authenticated logical-user names and confirmed account changes."""
from functools import wraps

from astrbot.api.web import error_response, json_response, request

try:
    from ..core.identity_management import IdentityConflict
    from ..storage.job_store import JobConflict
except ImportError:
    from core.identity_management import IdentityConflict
    from storage.job_store import JobConflict
from .maintenance_api import PREFIX, _integer, _summary, _text
from .platforms import platform_name


def _guard(handler):
    @wraps(handler)
    async def guarded(self, *args, **kwargs):
        if not (self.plugin.ready and not self.plugin.terminating and self.plugin.identity_management is not None):
            return error_response("逻辑用户管理尚未就绪。", status_code=503,
                                  data={"error_code": "identity_unavailable"})
        try:
            return await handler(self, *args, **kwargs)
        except IdentityConflict as exc:
            return error_response(str(exc), status_code=409, data={"error_code": exc.code})
        except JobConflict:
            return error_response("已有后台任务正在执行，请稍后重新确认账号操作。", status_code=409,
                                  data={"error_code": "identity_task_conflict"})
        except KeyError:
            return error_response("空间或空间内的逻辑用户不存在，请刷新列表。", status_code=404,
                                  data={"error_code": "identity_not_found"})
        except (ValueError, TypeError):
            return error_response("请检查用户名称、平台实例和账号字段。", status_code=400,
                                  data={"error_code": "invalid_identity_request"})
        except Exception:
            return error_response("逻辑用户操作暂未完成，请刷新任务和账号列表。", status_code=500,
                                  data={"error_code": "identity_request_failed"})
    return guarded


class IdentityAPI:
    def __init__(self, plugin):
        self.plugin = plugin

    def register(self, context):
        for endpoint, method, handler in (
            ("identities", "GET", self.identities),
            ("identities/rename", "POST", self.rename),
            ("identities/preview", "POST", self.preview),
            ("identities/apply", "POST", self.apply),
        ):
            context.register_web_api(f"{PREFIX}/{endpoint}", handler, [method], f"NeoMemory {endpoint}")

    async def _body(self, fields):
        body = await request.json(default=None)
        if not isinstance(body, dict) or set(body) != set(fields):
            raise ValueError("invalid identity request fields")
        return {key: _text(body[key], maximum=500 if key == "idempotency_key" else 200) for key in fields}

    @_guard
    async def identities(self):
        space = await self.plugin.identity_management.get_space(_text(request.query.get("space_id")))
        result = await self.plugin.source_store.list_logical_users(
            space=space,
            q=_text(request.query.get("q", ""), maximum=200, empty=True),
            limit=_integer(request.query.get("limit", 50), minimum=1, maximum=100),
            offset=_integer(request.query.get("offset", 0)))
        result["conflicts"] = await self.plugin.identity_management.account_conflicts(result["items"])
        return json_response(result)

    @_guard
    async def rename(self):
        body = await self._body(("space_id", "logical_user_id", "display_name"))
        space = await self.plugin.identity_management.get_space(body.pop("space_id"))
        return json_response(await self.plugin.source_store.rename_logical_user(**body, space=space))

    async def _binding_body(self, *extra):
        body = await self._body(("space_id", "action", "logical_user_id", "platform_instance_id", "account_id", *extra))
        instance = body["platform_instance_id"]
        name = platform_name(self.plugin.context, instance)
        if name:
            await self.plugin.source_store.register_platform(instance, name)
        return body

    @_guard
    async def preview(self):
        body = await self._binding_body()
        return json_response(await self.plugin.identity_management.preview(**body))

    @_guard
    async def apply(self):
        body = await self._binding_body("confirmation_token", "idempotency_key")
        job = await self.plugin.identity_management.submit(**body)
        self.plugin.maintenance.runner._wake.set()
        return json_response(_summary(job))
