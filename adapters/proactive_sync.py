"""Consume the companion's durable, paginated delivered-message receipts."""

from __future__ import annotations

import asyncio
import time

try:
    from ..core.jobs import RetryableJobError
    from ..core.persona_policy import is_supported_persona
except ImportError:
    from core.jobs import RetryableJobError
    from core.persona_policy import is_supported_persona


class ReceiptsUnavailable(RetryableJobError):
    def __init__(self, message):
        super().__init__("companion_receipts_unavailable")
        self.message = message


class ProactiveSync:
    STATE_KEY = "companion_delivery_cursor"
    PAGE_SIZE = 100

    def __init__(self, plugin, *, clock=time.time):
        self.plugin = plugin
        self.pipeline = plugin.maintenance.pipeline
        self.clock = clock
        self._worker = None
        self._enabled = False
        self.last_issue = None

    async def configure(self, enabled):
        if not enabled:
            await self.stop()
            if await self.pipeline.get_state(self.STATE_KEY):
                await self.pipeline.set_state(self.STATE_KEY, None)
            return
        if self._enabled:
            return
        if not await self.pipeline.get_state(self.STATE_KEY):
            await self.pipeline.set_state(self.STATE_KEY, {"since": self.clock(), "after": 0})
        self._enabled = True
        self._worker = asyncio.create_task(self._poll(), name="neomemory-companion-receipts")

    async def stop(self):
        self._enabled = False
        self.last_issue = None
        if self._worker:
            self._worker.cancel()
            await asyncio.gather(self._worker, return_exceptions=True)
            self._worker = None

    async def page(self, *, after, since, until=None, highwater=None):
        adapter = self.plugin.external_memory
        if not adapter.enabled:
            raise ReceiptsUnavailable("请在功能设置中启用「接入陪伴插件外部记忆接口」。")
        if not adapter.connect():
            raise ReceiptsUnavailable(adapter.delivery_issue or "陪伴插件尚未连接。")
        if adapter.delivery_issue:
            raise ReceiptsUnavailable(adapter.delivery_issue)
        read = getattr(adapter.api, "list_delivered_proactive_messages", None)
        if not read:
            raise ReceiptsUnavailable("当前陪伴插件版本尚未提供主动消息投递回执接口。")
        try:
            return await read(adapter.registration_id, after=after, since=since, until=until,
                              highwater=highwater, limit=self.PAGE_SIZE)
        except PermissionError as exc:
            raise ReceiptsUnavailable("请确认 NeoMemory 为陪伴插件当前选中的记忆写入提供方。") from exc
        except RuntimeError as exc:
            raise ReceiptsUnavailable("陪伴插件的投递回执服务暂时不可用。") from exc

    async def sync_page(self):
        state = await self.pipeline.get_state(self.STATE_KEY)
        page = await self.page(since=state["since"], after=state["after"])
        if state.get("installation_id", page["installation_id"]) != page["installation_id"]:
            state = {"since": state["since"], "after": 0, "installation_id": page["installation_id"]}
            await self.pipeline.set_state(self.STATE_KEY, state)
            return True
        for record in page["items"]:
            if is_supported_persona(record["metadata"].get("host_persona_id")) and self.plugin.source_store.cleaner.clean(record["content"]).strip():
                if not await self.plugin.external_memory.accept_delivery(record):
                    raise RetryableJobError("companion_delivery_pending")
        new = {**state, "installation_id": page["installation_id"], "after": page["next_cursor"]}
        if state != new:
            await self.pipeline.set_state(self.STATE_KEY, new)
        return page["has_more"]

    async def _poll(self):
        while self._enabled:
            try:
                more = await self.sync_page()
                self.last_issue = None
                if more:
                    continue
            except ReceiptsUnavailable as exc:
                self.last_issue = exc.message
            except Exception as exc:
                self.last_issue = type(exc).__name__
            await asyncio.sleep(5)
