"""NeoMemory AstrBot plugin entry point and store lifecycle."""

from __future__ import annotations

import asyncio
import copy
import importlib.metadata
import weakref
from dataclasses import replace
from pathlib import Path
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.event.filter import CustomFilter
from astrbot.api.platform import MessageType
from astrbot.api.provider import LLMResponse, ProviderRequest
from astrbot.api.star import Context, Star, StarTools
from astrbot.api.web import error_response, json_response, request

try:
    from .adapters.console_api import ConsoleAPI
    from .adapters.external_memory import ExternalMemoryAdapter
    from .adapters.proactive_sync import ProactiveSync
    from .adapters.ingress_api import IngressAPI
    from .adapters.maintenance_api import MaintenanceAPI
    from .adapters.identity_api import IdentityAPI
    from .adapters.providers import ProviderAdapter
    from .adapters.runtime import MemoryRuntime
    from .adapters.settings_api import SettingsAPI
    from .core.cleaning_settings import CleaningSettings
    from .core.config import NeoMemoryConfig
    from .core.identity import extract_event_persona_id
    from .core.ingress import IngressService
    from .core.maintenance import MaintenanceEngine
    from .core.identity_management import IdentityManagement
    from .core.model_tests import ModelTestService
    from .core.persona_policy import is_supported_persona, is_unsupported_persona
    from .core.settings import SettingsService
    from .core.short_term import ShortTermService
    from .core.image_cache import PrivateImageCache
    from .core.image_capture import kind as component_kind, unambiguous_caption
    from .core.source_ingest import normalize_agent_response, normalize_event, normalize_sent_images
    from .storage.console_store import ConsoleStore, NoteService
    from .storage.ingress_store import IngressStore
    from .storage.memory_store import MemoryStore
    from .storage.pipeline_store import PipelineStore
    from .storage.source_store import SourceStore
except ImportError:  # pragma: no cover - supports direct local loading in probes.
    from adapters.console_api import ConsoleAPI
    from adapters.external_memory import ExternalMemoryAdapter
    from adapters.proactive_sync import ProactiveSync
    from adapters.ingress_api import IngressAPI
    from adapters.maintenance_api import MaintenanceAPI
    from adapters.identity_api import IdentityAPI
    from adapters.providers import ProviderAdapter
    from adapters.runtime import MemoryRuntime
    from adapters.settings_api import SettingsAPI
    from core.cleaning_settings import CleaningSettings
    from core.config import NeoMemoryConfig
    from core.identity import extract_event_persona_id
    from core.ingress import IngressService
    from core.maintenance import MaintenanceEngine
    from core.identity_management import IdentityManagement
    from core.model_tests import ModelTestService
    from core.persona_policy import is_supported_persona, is_unsupported_persona
    from core.settings import SettingsService
    from core.short_term import ShortTermService
    from core.image_cache import PrivateImageCache
    from core.image_capture import kind as component_kind, unambiguous_caption
    from core.source_ingest import normalize_agent_response, normalize_event, normalize_sent_images
    from storage.console_store import ConsoleStore, NoteService
    from storage.ingress_store import IngressStore
    from storage.memory_store import MemoryStore
    from storage.pipeline_store import PipelineStore
    from storage.source_store import SourceStore


PLUGIN_NAME = "astrbot_plugin_neomemory"
_ACTIVE_PLUGIN: weakref.ReferenceType[NeoMemoryPlugin] | None = None


def _set_active_plugin(plugin: NeoMemoryPlugin | None) -> None:
    global _ACTIVE_PLUGIN
    _ACTIVE_PLUGIN = weakref.ref(plugin) if plugin is not None else None


def _active_plugin() -> NeoMemoryPlugin | None:
    return _ACTIVE_PLUGIN() if _ACTIVE_PLUGIN is not None else None


class SourceCaptureFilter(CustomFilter):
    """Schedule source capture without waking the main Agent pipeline."""

    def filter(self, event: AstrMessageEvent, cfg: Any) -> bool:
        del cfg
        plugin = _active_plugin()
        if plugin is None or plugin.terminating or not plugin.ready:
            return False
        # AstrBot may initialize plugins in an order different from the
        # companion plugin. Re-check the public provider handle on ordinary
        # traffic so a later companion activation or reload can connect.
        plugin.external_memory.connect()
        try:
            message_type = event.get_message_type()
        except Exception:
            return False
        if message_type == MessageType.FRIEND_MESSAGE:
            if plugin.config.capture_agent_messages and plugin.config.private_image_limit:
                plugin.observe_private_sends(event)
            if not plugin.config.capture_private_messages:
                return False
        elif message_type == MessageType.GROUP_MESSAGE:
            if not plugin.config.capture_group_messages:
                return False
        else:
            return False
        try:
            sender_id = event.get_sender_id()
            self_id = event.get_self_id()
            if sender_id and self_id and sender_id == self_id:
                return False
        except Exception:
            pass
        plugin.schedule_user_event(event)
        return False


class NeoMemoryPlugin(Star):
    """Capture replayable sources and expose scoped recall through AstrBot."""

    def __init__(self, context: Context, config: dict[str, Any] | None = None):
        super().__init__(context)
        self.context = context
        self._host_config = config
        self.config = NeoMemoryConfig.from_mapping(config)
        self.settings = SettingsService(self)
        self.console_store = None
        self.note_service = None
        self.model_tests = None
        self.identity_management = None
        self.source_store: SourceStore | None = None
        self.memory_store: MemoryStore | None = None
        self.data_dir: Path | None = None
        self.runtime: MemoryRuntime | None = None
        self.maintenance: MaintenanceEngine | None = None
        self.jobs = None
        self.short_term: ShortTermService | None = None
        self.ingress: IngressService | None = None
        self.ingress_store: IngressStore | None = None
        self.proactive_sync = None
        self.image_cache = None
        self.external_memory = ExternalMemoryAdapter(self)
        self._unpersisted_overflow = 0
        self._admission_lock = asyncio.Lock()
        self._tasks: set[asyncio.Task[Any]] = set()
        self._failed_sources: dict[str, Any] = {}
        self._max_failed_sources = 1000
        self._ready = False
        self.terminating = False
        _set_active_plugin(self)
        self._register_web_api()

    @property
    def ready(self) -> bool:
        return (
            self._ready
            and self.source_store is not None
            and self.memory_store is not None
        )

    async def initialize(self) -> None:
        self.data_dir = StarTools.get_data_dir(PLUGIN_NAME)
        self.source_store = SourceStore(self.data_dir / "source.db")
        try:
            await self.source_store.open()
            await self.source_store.database.execute("PRAGMA synchronous = FULL")
            self.ingress_store = IngressStore(self.data_dir / "ingress.db",
                max_entries=self.config.ingress_max_entries, max_bytes=self.config.ingress_max_bytes)
            await self.ingress_store.open()
            self.ingress = IngressService(self.ingress_store, self.source_store, self.config)
            self.memory_store = MemoryStore(
                self.data_dir / "memory.db",
                source_exists=self.source_store.has_source,
                source_observations=self.source_store.get_observation_times,
            )
            await self.memory_store.open(version_limits=self.config.version_limits)
            self.cleaning_settings = CleaningSettings(PipelineStore(self.memory_store), self.source_store)
            await self.cleaning_settings.load()
            await self.ingress.start()
            self.runtime = MemoryRuntime(self)
            await self.runtime.start()
            self.maintenance = MaintenanceEngine(self.source_store, self.memory_store,
                self.runtime, self.config, self._load_persona)
            self.image_cache = PrivateImageCache(self.data_dir / "pending_images")
            await self.image_cache.open()
            self.maintenance.image_cache = self.image_cache
            self.jobs = self.maintenance.jobs
            self.identity_management = IdentityManagement(self.source_store, self.memory_store, self.jobs, self.runtime)
            self.maintenance.identity_management = self.identity_management
            await self.identity_management.cleanup_receipts()
            self.maintenance.runner.handlers["identity"] = self.identity_management.run
            self.console_store = ConsoleStore(self.memory_store, self.source_store)
            self.note_service = NoteService(self.memory_store, self.jobs)
            self.model_tests = ModelTestService(self)
            self.maintenance.model_tests = self.model_tests
            self.maintenance.runner.handlers["note"] = self.note_service.run
            self.maintenance.runner.handlers["model_test"] = self.model_tests.run
            self.short_term = ShortTermService(self.source_store, self.memory_store, self.jobs,
                self.runtime.adapter, self.config, self._load_persona)
            self.maintenance.short_term = self.short_term
            self.maintenance.runner.handlers["short_term"] = self.short_term.run
            self.proactive_sync = ProactiveSync(self)
            await self.maintenance.start()
        except (Exception, asyncio.CancelledError):
            await self._close_stores()
            raise
        self._ready = True
        self.external_memory.connect()
        await self.proactive_sync.configure(self.config.external_memory_enabled)
        logger.info("NeoMemory source and memory stores ready at %s", self.data_dir)

    async def apply_config(self, config: NeoMemoryConfig) -> None:
        """Apply saved settings to new work while active operations keep snapshots."""
        if not self._ready:
            previous = self.config
            self.config = config
            try:
                await self.initialize()
            except BaseException:
                self.config = previous
                raise
            return

        # Build bindings before publishing them. Stores and the shared Provider
        # concurrency budgets keep their identity across settings changes.
        prepared = copy.copy(self.runtime)
        prepared.configure(config)
        await self.runtime.adapter.reconfigure(
            llm_concurrency=config.llm_concurrency,
            embedding_concurrency=config.embedding_concurrency)
        if config.version_limits != self.config.version_limits:
            await self.memory_store.configure_version_limits(config.version_limits)
        external_changed = any(getattr(self.config, field) != getattr(config, field) for field in (
            "external_memory_enabled", "external_memory_mode",
            "external_memory_priority", "external_memory_timeout_seconds"))
        if external_changed:
            self.external_memory.disconnect()

        self.runtime.config, self.runtime.recall = config, prepared.recall
        if self.runtime.cache is not None:
            self.runtime.cache.limits = {"message": config.cache_message_limit, "node": config.cache_node_limit}
            self.runtime.cache.ttls = {"message": config.cache_message_ttl_seconds, "node": config.cache_node_ttl_seconds}
        self.maintenance.config = config
        self.short_term.config = config
        self.model_tests.config = config
        runner = self.maintenance.runner
        runner.concurrency = config.job_concurrency
        runner.lease_seconds = config.job_lease_seconds
        runner.poll_seconds = config.job_poll_seconds
        self.ingress_store.max_entries = config.ingress_max_entries
        self.ingress_store.max_bytes = config.ingress_max_bytes
        self.ingress.max_attempts = config.ingress_max_attempts
        self.ingress.retry_seconds = config.ingress_retry_seconds
        self.ingress.poll_seconds = config.ingress_poll_seconds
        self.config = config
        runner._wake.set()
        self.maintenance._scan_wake.set()
        self.ingress._wake.set()
        if self.ready:
            self.external_memory.connect()
            await self.proactive_sync.configure(config.external_memory_enabled)

    async def _load_persona(self, persona_id: str) -> str | None:
        if not is_supported_persona(persona_id):
            return None
        getter = getattr(getattr(self.context, "persona_manager", None), "get_persona_v3_by_id", None)
        persona = getter(persona_id) if callable(getter) else None
        return persona.get("prompt") if isinstance(persona, dict) else None

    async def _resolve_persona_id(self, event: AstrMessageEvent, *, conversation=None) -> str | None:
        """Resolve the host persona, explicitly excluding unsupported defaults."""
        event._neomemory_skip_persona = False

        def selected(value):
            if is_unsupported_persona(value):
                event._neomemory_skip_persona = True
                return None
            return str(value).strip() if value else None

        try:
            current_id = getattr(conversation, "cid", None)
            manager = getattr(self.context, "conversation_manager", None)
            if conversation is None and manager is not None:
                current_id = await manager.get_curr_conversation_id(event.unified_msg_origin)
                if current_id:
                    conversation = await manager.get_conversation(event.unified_msg_origin, current_id)
            event._neomemory_conversation_id = str(current_id) if current_id else None
            marker = object()
            getter = getattr(event, "get_extra", None)
            traced = getter("_trace_persona_id", marker) if callable(getter) else marker
            # AstrBot initially sets this to None while awaiting persona resolution.
            if traced is not marker and traced:
                return selected(traced)
            explicit = extract_event_persona_id(event)
            value = explicit or (getattr(conversation, "persona_id", None) if conversation else None)
            resolver = getattr(getattr(self.context, "persona_manager", None), "resolve_selected_persona", None)
            if callable(resolver):
                value, _, _, _ = await resolver(
                    umo=event.unified_msg_origin, conversation_persona_id=value,
                    platform_name=event.get_platform_name())
            return selected(value)
        except Exception as exc:
            logger.debug("NeoMemory persona resolution failed: %s", type(exc).__name__)
            return None

    async def terminate(self) -> None:
        self.terminating = True
        await self.settings.drain()
        self._ready = False
        self.external_memory.disconnect()
        tasks = list(self._tasks)
        if tasks:
            done, pending = await asyncio.wait(
                tasks,
                timeout=self.config.write_flush_timeout_ms / 1000,
            )
            del done
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        try:
            await self._close_stores()
        finally:
            _set_active_plugin(None)

    async def _close_stores(self) -> None:
        """Release the dependent memory store before its source validator."""

        self._ready = False
        if self.proactive_sync is not None:
            await self.proactive_sync.stop()
            self.proactive_sync = None
        maintenance, self.maintenance = self.maintenance, None
        self.jobs = None
        self.short_term = None
        self.console_store = None
        self.note_service = None
        self.model_tests = None
        self.identity_management = None
        ingress, self.ingress = self.ingress, None
        ingress_store, self.ingress_store = self.ingress_store, None
        runtime, self.runtime = self.runtime, None
        memory_store, self.memory_store = self.memory_store, None
        source_store, self.source_store = self.source_store, None
        try:
            try:
                try:
                    if maintenance is not None:
                        await maintenance.stop()
                finally:
                    if runtime is not None:
                        await runtime.close()
            finally:
                if self.image_cache is not None:
                    await self.image_cache.close()
                    self.image_cache = None
                if memory_store is not None:
                    await memory_store.close()
        finally:
            try:
                if ingress is not None:
                    await ingress.stop()
            finally:
                try:
                    if ingress_store is not None:
                        await ingress_store.close()
                finally:
                    if source_store is not None:
                        await source_store.close()

    def schedule_user_event(self, event: AstrMessageEvent) -> None:
        if not self.ready or self.terminating:
            return
        event._neomemory_source_capture = self._schedule(self._capture_user_event(event))

    async def _capture_user_event(self, event: AstrMessageEvent) -> None:
        config = self.config
        persona = await self._resolve_persona_id(event)
        if getattr(event, "_neomemory_skip_persona", False):
            return
        references = {}
        source = normalize_event(
            event,
            actor_kind="user",
            image_references=references,
            max_content_chars=config.max_content_chars,
            persona_id=persona,
            resolve_event_persona=False,
            host_conversation_id=getattr(event, "_neomemory_conversation_id", None),
        )
        if source is not None:
            event._neomemory_source_id = source.source_id
            slots = source.metadata.get("image_attachments", ())
            if (len(slots) == 1 and not slots[0].get("author_path") and config.private_image_limit
                    and self.image_cache is not None):
                self.image_cache.expect_caption(source.source_id, event)
            self._stage_images(source, references)
            self._cache_event_caption(event)
            await self._persist_source(source)

    def _stage_images(self, source, references):
        if references and self.image_cache is not None:
            limit = self.config.private_image_limit
            self.image_cache.stage(source.source_id, {slot: ref for slot, ref in references.items() if slot <= limit})

    def _cache_event_caption(self, event):
        caption = getattr(event, "_neomemory_image_caption", None)
        source_id = getattr(event, "_neomemory_source_id", None)
        if self.image_cache is not None and source_id and caption:
            self.image_cache.set_caption(source_id, event, caption)
            event._neomemory_image_caption = None

    def observe_private_sends(self, event):
        """Observe successful sends on this event, including direct plugin sends."""
        send = getattr(event, "send", None)
        if not callable(send) or getattr(event, "_neomemory_observes_sends", False):
            return
        event._neomemory_observes_sends = True
        send_number = 0

        def captured(message):
            nonlocal send_number
            if not self.ready or self.terminating or not self.config.capture_agent_messages or not self.config.private_image_limit:
                return
            chain = tuple(getattr(message, "chain", ()))
            if any(component_kind(part) in {"image", "node", "nodes", "forward"} for part in chain):
                send_number += 1
                previous = getattr(event, "_neomemory_source_capture", None)
                event._neomemory_source_capture = self._schedule(
                    self._capture_sent_images(event, chain, send_number, previous))

        # QQ splits a chain into individual media deliveries. Its normal and
        # native streaming paths share this method and return a platform receipt.
        deliver = getattr(event, "_post_send_one", None)
        if event.get_platform_name() in {"qq_official", "qq_official_webhook"} and callable(deliver):
            async def observe_qq_delivery(message_to_send, stream=None):
                receipt = await deliver(message_to_send, stream)
                if receipt is not None:
                    captured(message_to_send)
                return receipt
            event._post_send_one = observe_qq_delivery
        else:
            async def observe_send(message):
                result = await send(message)
                captured(message)
                return result
            event.send = observe_send

    async def _capture_sent_images(self, event, chain, send_number, previous):
        if previous is not None:
            await asyncio.gather(previous, return_exceptions=True)
        persona = await self._resolve_persona_id(event)
        if getattr(event, "_neomemory_skip_persona", False):
            return
        references = {}
        source = normalize_sent_images(event, chain, persona_id=persona, send_number=send_number,
                                       cleaner=self.source_store.cleaner,
                                       image_references=references, max_content_chars=self.config.max_content_chars,
                                       host_conversation_id=getattr(event, "_neomemory_conversation_id", None))
        if source:
            generated = getattr(event, "_neomemory_generated_text", None)
            if generated and source.content_text and source.content_text == generated.content_text:
                source = replace(source, content_text="", metadata={**source.metadata,
                    "accompanying_response_source_id": generated.source_id,
                    "accompanying_text_chars": len(source.content_text)})
            self._stage_images(source, references)
            await self._persist_source(source)

    @filter.custom_filter(SourceCaptureFilter, False)
    async def capture_message(self, event: AstrMessageEvent) -> None:
        """Capture private and group input without waking AstrBot."""

        del event

    @filter.on_llm_response()
    async def capture_agent_response(
        self, event: AstrMessageEvent, resp: LLMResponse
    ) -> None:
        if (
            not self.ready
            or self.terminating
            or not self.config.capture_agent_messages
            or str(getattr(resp, "role", "assistant") or "assistant")
            not in {"assistant", ""}
        ):
            return
        self.external_memory.connect()
        previous = getattr(event, "_neomemory_source_capture", None)
        event._neomemory_source_capture = self._schedule(self._capture_agent_response(event, resp, previous))

    async def _capture_agent_response(
        self, event: AstrMessageEvent, resp: LLMResponse, previous=None
    ) -> None:
        if previous is not None:
            await asyncio.gather(previous, return_exceptions=True)
        config = self.config
        persona = await self._resolve_persona_id(event)
        if getattr(event, "_neomemory_skip_persona", False):
            return
        source = normalize_agent_response(
            event,
            resp,
            cleaner=self.source_store.cleaner,
            max_content_chars=config.max_content_chars,
            persona_id=persona,
            resolve_event_persona=False,
            host_conversation_id=getattr(event, "_neomemory_conversation_id", None),
        )
        if source is not None:
            event._neomemory_generated_text = source
            await self._persist_source(source)

    @filter.on_llm_request()
    async def inject_memory(self, event: AstrMessageEvent, req: ProviderRequest) -> None:
        """Attach bounded temporary recall without changing permanent history."""
        if self.ready and not self.terminating and self.runtime is not None:
            self.external_memory.connect()
            if self.config.private_image_limit and self.config.capture_private_messages and event.get_message_type() == MessageType.FRIEND_MESSAGE:
                components = getattr(getattr(event, "message_obj", None), "message", ()) or ()
                event._neomemory_image_caption = unambiguous_caption(req, components)
                self._cache_event_caption(event)
            await self.runtime.passive(event, req)

    @filter.llm_tool(name="neomemory_recall")
    async def recall_tool(self, event: AstrMessageEvent, query: str, kind: str = "all",
                          limit: int = 0, event_after: str = "", event_before: str = "") -> str:
        """Recall memories and notes for the current persona and conversation.

        Args:
            query(string): A question about experiences or a literal keyword for notes, up to 4000 characters.
            kind(string): all (default), memory, atomic, event, topic, or note. Notes are plans/thoughts, not evidence of events.
            limit(number): Integer maximum returned entries, 1–30; 0 uses the configured default.
            event_after(string): Optional inclusive event-time start, ISO 8601 with timezone or UTC date. Unknown-time memories and notes are excluded when time filters apply.
            event_before(string): Optional inclusive event-time end, ISO 8601 with timezone or UTC date.
        """
        if not self.ready or self.terminating or self.runtime is None:
            return "NeoMemory 尚未就绪。"
        return await self.runtime.active_recall(event, query, kind, limit, event_after, event_before)

    @filter.llm_tool(name="neomemory_notes")
    async def notes_tool(self, event: AstrMessageEvent, action: str, content: str = "",
                         note_id: str = "", query: str = "", status: str = "active") -> str:
        """Manage independent notes within the current persona and conversation.

        Args:
            action(string): One of create, list, update, archive, restore, delete. Delete removes a note from queries; archive can be restored.
            content(string): Note text for create/update, up to 2000 characters.
            note_id(string): Existing note ID for update/archive/restore/delete.
            query(string): Optional literal text search for list, up to 200 characters.
            status(string): For list, active (default), archived, or all.
        """
        if not self.ready or self.terminating or self.runtime is None:
            return "NeoMemory 尚未就绪。"
        return await self.runtime.notes(event, action, content, note_id, query, status)

    def _remember_unpersisted(self, source, exc):
        if source.source_id in self._failed_sources or len(self._failed_sources) < self._max_failed_sources:
            self._failed_sources[source.source_id] = source
        else:
            self._unpersisted_overflow += 1
        logger.warning("NeoMemory ingress admission failed: %s", type(exc).__name__)

    async def _persist_source(self, source) -> None:
        if self.ingress is None or is_unsupported_persona(source.persona_id):
            return
        async with self._admission_lock:
            # A previously unpersisted record must not be overtaken when queue
            # space recovers. The queue itself orders only admitted records.
            predecessors = [item for item in self._failed_sources.values()
                            if item.identity.stream_id == source.identity.stream_id]
            candidates = predecessors + ([source] if source.source_id not in self._failed_sources else [])
            for candidate in candidates:
                try:
                    await self.ingress.submit(candidate)
                    self._failed_sources.pop(candidate.source_id, None)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._remember_unpersisted(candidate, exc)
                    if candidate.source_id != source.source_id:
                        self._remember_unpersisted(source, exc)
                    return

    async def retry_failed_sources(self, limit: int | None = None) -> int:
        """Try to durably admit emergency records; durable source retries run separately."""
        if self.ingress is None:
            return 0
        pending = list(self._failed_sources.values())
        if limit is not None:
            pending = pending[:max(0, int(limit))]
        for source in pending:
            await self._persist_source(source)
        # New arrivals may be admitted concurrently between awaits; they must
        # not turn this batch's restored count negative.
        return sum(source.source_id not in self._failed_sources for source in pending)

    def _schedule(self, coroutine) -> asyncio.Task:
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)

        def finish(completed: asyncio.Task[Any]) -> None:
            self._tasks.discard(completed)
            if completed.cancelled():
                return
            try:
                completed.result()
            except Exception:
                logger.exception("NeoMemory background source task failed")

        task.add_done_callback(finish)
        return task

    def _register_web_api(self) -> None:
        register = getattr(self.context, "register_web_api", None)
        if not callable(register):
            return
        register(
            f"/{PLUGIN_NAME}/health", self.api_health, ["GET"], "NeoMemory health"
        )
        register(
            f"/{PLUGIN_NAME}/streams",
            self.api_streams,
            ["GET"],
            "NeoMemory source streams",
        )
        register(
            f"/{PLUGIN_NAME}/records",
            self.api_records,
            ["GET"],
            "NeoMemory source records",
        )
        register(
            f"/{PLUGIN_NAME}/bindings",
            self.api_bind_account,
            ["POST"],
            "NeoMemory account binding",
        )

        register(f"/{PLUGIN_NAME}/models", self.api_models, ["GET"], "NeoMemory providers")
        register(f"/{PLUGIN_NAME}/models/test", self.api_test_model, ["POST"], "NeoMemory provider connectivity")
        MaintenanceAPI(self).register(self.context)
        IdentityAPI(self).register(self.context)
        IngressAPI(self).register(self.context)
        ConsoleAPI(self).register(self.context)
        SettingsAPI(self).register(self.context)

    async def api_models(self):
        try:
            adapter = self.runtime.adapter if self.runtime is not None else ProviderAdapter(self.context)
            return json_response(adapter.list_models())
        except Exception as exc:
            if self.runtime is not None:
                self.runtime.error("models", exc)
            return error_response("host providers unavailable", status_code=503)

    async def api_test_model(self):
        """Run the connectivity check exposed by models/probe."""
        return await SettingsAPI(self).probe()

    @staticmethod
    def _host_version() -> str | None:
        try:
            return importlib.metadata.version("AstrBot")
        except importlib.metadata.PackageNotFoundError:
            try:
                return importlib.metadata.version("astrbot")
            except importlib.metadata.PackageNotFoundError:
                return None

    def _storage_bytes(self) -> dict[str, int]:
        sizes = {}
        for name in ("source.db", "memory.db", "cache.db", "ingress.db"):
            size = 0
            for suffix in ("", "-wal", "-shm"):
                try:
                    size += (self.data_dir / (name + suffix)).stat().st_size
                except FileNotFoundError:
                    continue
            sizes[name] = size
        return sizes

    async def api_health(self):
        if self.source_store is None or self.memory_store is None:
            return json_response(
                {
                    "plugin": PLUGIN_NAME,
                    "ready": False,
                    "host_version": self._host_version(),
                }
            )
        result = await self.source_store.health()
        result["memory"] = await self.memory_store.health()
        if self.proactive_sync and self.proactive_sync.last_issue:
            result["companion_delivery_sync"] = self.proactive_sync.last_issue
        result["storage_bytes"] = await asyncio.to_thread(self._storage_bytes)
        if self.ingress is not None:
            result["ingress"] = {**await self.ingress.health(),
                "unpersisted_count": len(self._failed_sources), "unpersisted_overflow": self._unpersisted_overflow}
        if self.maintenance is not None:
            result["maintenance"] = {**self.maintenance.runner.health(),
                **await self.jobs.health(), "scanner_error_type": self.maintenance.last_error_type,
                "extraction_prompt_version": self.maintenance.extraction.prompt_version,
                "topic_prompt_version": self.maintenance.topic_prompt_version,
                "automatic_topics": self.config.topic_consolidation_enabled,
                "automatic_extraction": self.config.automatic_extraction_enabled,
                "llm_configured": bool(self.config.llm_provider_id)}
        if self.runtime is not None:
            result["recall"] = {"configured": self.runtime.recall is not None,
                "enabled": self.config.recall_enabled, "tools_enabled": self.config.tools_enabled,
                "background_tasks": len(self.runtime.tasks),
                "diagnostics": dict(self.runtime.diagnostics)}
            try:
                result["cache"] = await self.runtime.cache.health() if self.runtime.cache else {"ready": False}
            except Exception as exc:
                result["cache"] = {"ready": False, "error_type": type(exc).__name__}
        result.update(
            {
                "plugin": PLUGIN_NAME,
                "ready": self.ready,
                "host_version": self._host_version(),
                "background_tasks": len(self._tasks),
                "failed_source_count": len(self._failed_sources),
            }
        )
        return json_response(result)

    async def api_streams(self):
        if self.source_store is None:
            return error_response("source store is not ready", status_code=503)
        limit = request.query.get("limit", 100, type=int)
        return json_response({"items": await self.source_store.list_streams(limit)})

    async def api_records(self):
        if self.source_store is None:
            return error_response("source store is not ready", status_code=503)
        stream_id = request.query.get("stream_id", None)
        limit = request.query.get("limit", 50, type=int)
        offset = request.query.get("offset", 0, type=int)
        return json_response(
            {
                "items": await self.source_store.list_records(
                    stream_id=stream_id,
                    limit=limit,
                    offset=offset,
                )
            }
        )

    async def api_bind_account(self):
        return error_response("请在维护页预览账号变更并二次确认；旧绑定入口不再直接修改身份。",
                              status_code=409, data={"error_code": "identity_confirmation_required"})
