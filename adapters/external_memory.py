"""Adapter for AstrBot's companion.external-memory.v1 provider contract.

The companion plugin owns the request authorization.  NeoMemory only accepts
the serialized scope supplied by that contract and resolves it to its own
explicit memory space; it never infers identity from the query text.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from .platforms import platform_name

try:
    from ..core.persona_policy import is_supported_persona
except ImportError:
    from core.persona_policy import is_supported_persona

try:
    from ..core.text_cleaning import DEFAULT_CLEANER, receipt_fingerprints, receipt_text_matches
    from ..core.identity import EventIdentity
    from ..core.memory_spaces import group_space, private_space
    from ..core.recall_history import recall_snapshot
    from ..core.source_ingest import CapturedSource
except ImportError:  # pragma: no cover - direct script imports
    from core.text_cleaning import DEFAULT_CLEANER, receipt_fingerprints, receipt_text_matches
    from core.identity import EventIdentity
    from core.memory_spaces import group_space, private_space
    from core.recall_history import recall_snapshot
    from core.source_ingest import CapturedSource


logger = logging.getLogger(__name__)

PROVIDER_API_VERSION = "companion.external-memory.v1"
PROVIDER_ID = "astrbot_plugin_neomemory.memory"
SUPPORTED_MODES = frozenset({"supplement", "owner"})
COMPANION_PLUGIN_ID = "astrbot_plugin_private_companion"


class ExternalMemoryContractError(ValueError):
    """The host request does not provide a safe, supported identity."""


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ExternalMemoryContractError(f"{field} must be an object")
    return value


def _json_value(value: Any, field: str, *, max_bytes: int = 131_072) -> Any:
    """Return a JSON-normalized value before it enters the durable ingress."""

    try:
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        if len(encoded.encode("utf-8")) > max_bytes:
            raise ExternalMemoryContractError(f"{field} exceeds the JSON budget")
        return json.loads(encoded)
    except ExternalMemoryContractError:
        raise
    except (TypeError, ValueError, OverflowError) as exc:
        raise ExternalMemoryContractError(f"{field} must be JSON-serializable") from exc


def _subject_id(value: Any, field: str) -> str:
    subject = _mapping(value, field)
    result = _text(subject.get("subject_id"))
    if not result or len(result) > 512:
        raise ExternalMemoryContractError(f"{field}.subject_id is invalid")
    return result


@dataclass(frozen=True, slots=True)
class ExternalMemoryScope:
    """The subset of the companion context needed by NeoMemory."""

    installation_id: str
    persona_id: str
    platform_instance_id: str
    account_id: str
    native_scope_id: str
    group_id: str = ""
    session_id: str = ""
    scope_kind: str = "private"

    @property
    def is_private(self) -> bool:
        return self.scope_kind == "private"


def parse_external_scope(context: Any, *, require_private: bool = False, delivered: bool = False) -> ExternalMemoryScope:
    """Validate a serialized companion context without guessing identifiers."""

    root = _mapping(context, "context")
    scope = _mapping(root.get("scope"), "context.scope")
    kind = _text(scope.get("kind"))
    if kind not in {"private", "group"}:
        raise ExternalMemoryContractError("unsupported companion scope kind")
    installation_id = _text(scope.get("installation_id"))
    if not installation_id:
        raise ExternalMemoryContractError("context.scope.installation_id is required")
    platform = _text(scope.get("platform"))
    if not platform:
        raise ExternalMemoryContractError("context.scope.platform is required")
    role = _mapping(root.get("role"), "context.role")
    role_id = _subject_id(role, "context.role")
    principal = _mapping(root.get("principal"), "context.principal")
    for field, value in (
        ("context.role.installation_id", role.get("installation_id")),
        ("context.principal.installation_id", principal.get("installation_id")),
    ):
        value = _text(value)
        if value and value != installation_id:
            raise ExternalMemoryContractError(f"{field} crosses installations")
    user = root.get("user")
    if isinstance(user, Mapping):
        user_installation = _text(user.get("installation_id"))
        if user_installation and user_installation != installation_id:
            raise ExternalMemoryContractError("context.user crosses installations")
    installations = {
        value for value in (
            _text(scope.get("installation_id")),
            _text(role.get("installation_id")),
            _text(principal.get("installation_id")),
        ) if value
    }
    if len(installations) > 1:
        raise ExternalMemoryContractError("companion context crosses installations")
    account_id = _text(principal.get("native_account_id"))
    if kind == "private" and not delivered and not account_id:
        raise ExternalMemoryContractError("private context requires principal.native_account_id")
    if require_private and kind != "private":
        raise ExternalMemoryContractError("private companion context required")
    native_scope = _text(scope.get("native_scope_id"))
    if not native_scope:
        raise ExternalMemoryContractError("context.scope.native_scope_id is required")
    if delivered and kind == "private":
        account_id = native_scope
    if kind == "private" and native_scope != account_id:
        raise ExternalMemoryContractError("private scope and principal account do not match")
    return ExternalMemoryScope(
        installation_id=installation_id,
        persona_id=role_id,
        platform_instance_id=platform,
        account_id=account_id,
        native_scope_id=native_scope,
        group_id=native_scope if kind == "group" else "",
        session_id=_text(scope.get("session_id")) or native_scope,
        scope_kind=kind,
    )


def discover_companion_api(context: Any) -> Any | None:
    """Find the live public extension API, without importing companion internals."""

    getter = getattr(context, "get_all_stars", None)
    if not callable(getter):
        return None
    try:
        stars = getter()
    except Exception:
        return None
    for star in stars or ():
        if not getattr(star, "activated", False):
            continue
        names = {getattr(star, "root_dir_name", ""), getattr(star, "name", "")}
        if COMPANION_PLUGIN_ID not in names:
            continue
        api = getattr(getattr(star, "star_cls", None), "extension_api", None)
        if getattr(api, "memory_provider_api_version", None) == PROVIDER_API_VERSION:
            return api
    return None


class ExternalMemoryAdapter:
    """Lifecycle and callback implementation for the companion provider API."""

    def __init__(self, plugin: Any):
        self.plugin = plugin
        self.api: Any | None = None
        self.registration_id: str | None = None
        self.delivery_issue: str | None = None

    @property
    def mode(self) -> str:
        value = str(getattr(self.plugin.config, "external_memory_mode", "supplement"))
        return value if value in SUPPORTED_MODES else "supplement"

    @property
    def enabled(self) -> bool:
        return bool(getattr(self.plugin.config, "external_memory_enabled", False))

    def is_active(self) -> bool:
        return bool(self.enabled and getattr(self.plugin, "ready", False)
                    and not getattr(self.plugin, "terminating", False))

    def handles_model_memory(self) -> bool:
        return self.mode == "owner"

    def connect(self) -> bool:
        """Register once for the current host lifetime; return whether connected."""

        if not self.enabled:
            self.disconnect()
            return False
        api = discover_companion_api(self.plugin.context)
        if api is None:
            self.disconnect()
            return False
        if self.registration_id is not None and api is self.api:
            return True
        if self.registration_id is not None:
            self.disconnect()
        options = dict(plugin=self.plugin, display_name="NeoMemory", read=self.read, write=self.write,
            summary=self.summary, is_active=self.is_active,
            priority=int(getattr(self.plugin.config, "external_memory_priority", 0)),
            handles_model_memory=self.handles_model_memory(),
            timeout_seconds=float(getattr(self.plugin.config, "external_memory_timeout_seconds", 5.0)))
        has_receipts = callable(getattr(api, "list_delivered_proactive_messages", None))
        if has_receipts:
            options["delivered_proactive"] = True
        issue = None
        try:
            try:
                registration = api.register_memory_provider(PROVIDER_ID, **options)
            except TypeError as exc:
                if not has_receipts or "unexpected keyword argument 'delivered_proactive'" not in str(exc):
                    raise
                issue = "陪伴插件的回执接口与注册接口版本不一致。请在方便时重启 AstrBot 以重新加载接口。"
                del options["delivered_proactive"]
                registration = api.register_memory_provider(PROVIDER_ID, **options)
        except Exception as exc:
            issue = issue or f"陪伴插件注册记忆提供方失败（{type(exc).__name__}）。"
            if self.delivery_issue != issue:
                logger.info("NeoMemory companion memory provider unavailable: %s", issue)
            self.delivery_issue = issue
            return False
        self.delivery_issue = f"基础记忆联动已连接。{issue}" if issue else None
        self.api = api
        self.registration_id = registration
        return True

    def disconnect(self) -> None:
        api, registration = self.api, self.registration_id
        self.api = None
        self.registration_id = None
        self.delivery_issue = None
        if api is not None and registration is not None:
            try:
                api.unregister_memory_provider(registration)
            except Exception:
                logger.debug("NeoMemory companion provider unregister failed", exc_info=True)

    async def _private_space(self, context: Any) -> tuple[dict[str, Any] | None, ExternalMemoryScope]:
        scope = parse_external_scope(context, require_private=True)
        logical_user_id = await self.plugin.source_store.resolve_account(
            scope.platform_instance_id, scope.account_id, platform_name(self.plugin.context, scope.platform_instance_id)
        )
        if logical_user_id is None:
            return None, scope
        spec = private_space(scope.persona_id, logical_user_id)
        return await self.plugin.memory_store.get_space(spec.scope_key), scope

    async def read(self, request: Any) -> dict[str, list[dict[str, str]]]:
        """Return bounded, whole-node entries for one authorized private scope."""

        root = _mapping(request, "request")
        if root.get("schema") != PROVIDER_API_VERSION or root.get("purpose") != "recall":
            raise ExternalMemoryContractError("unsupported external memory read request")
        # Scope resolution can overlap a model switch. Keep this callback's
        # admitted model bindings while later callbacks use the new settings.
        started = time.monotonic()
        runtime = self.plugin.runtime
        service = getattr(runtime, "recall", None)
        history = getattr(runtime, "recall_history", None)
        ticket = history.ticket() if history is not None else None
        space, scope = await self._private_space(root.get("context"))
        if space is None or service is None:
            return {"items": []}
        query = _text(root.get("query"))[:4000]
        if not query:
            return {"items": []}
        limit = root.get("limit", 8)
        max_chars = root.get("max_chars", 2400)
        if type(limit) is not int or not 0 <= limit <= 32:
            raise ExternalMemoryContractError("invalid read limit")
        if type(max_chars) is not int or not 0 <= max_chars <= 100_000:
            raise ExternalMemoryContractError("invalid read character budget")
        result = await service.recall(space["space_id"], query)
        items: list[dict[str, str]] = []
        returned = []
        remaining = max_chars
        for hit in result.hits[:limit]:
            content = _text(hit.get("content_text"))
            if not content or len(content) > remaining:
                continue
            item = {"id": _text(hit.get("node_id")), "content": content, "source": "neomemory"}
            occurred = hit.get("event_time_text") or hit.get("event_time_start")
            if occurred is not None:
                item["occurred_at"] = str(occurred)
            encoded = len(json.dumps(item, ensure_ascii=False, separators=(",", ":")))
            if encoded > remaining:
                continue
            items.append(item)
            if ticket is not None:
                returned.append(hit)
            remaining -= encoded
        current_user = await self.plugin.source_store.resolve_account(
            scope.platform_instance_id, scope.account_id, platform_name(self.plugin.context, scope.platform_instance_id))
        if current_user != space["logical_user_id"]:
            return {"items": []}
        if result.diagnostics.get("missing_node_vectors") and runtime.recall is service:
            runtime.schedule_index(space["space_id"])
        if ticket is not None:
            text = "\n".join(item["content"] for item in items)
            await history.append(ticket, space["space_id"], recall_snapshot(
                "external", query, started, result, text=text, hits=returned))
        return {"items": items}

    @staticmethod
    def _external_source(record: Mapping[str, Any], scope: ExternalMemoryScope, cleaner=DEFAULT_CLEANER) -> CapturedSource:
        source_plugin = _text(record.get("source_plugin"))
        memory_id = _text(record.get("memory_id"))
        content = _text(record.get("content"))
        kind = _text(record.get("kind"))
        if not source_plugin or not memory_id or not content:
            raise ExternalMemoryContractError("external write requires source_plugin, memory_id and content")
        if len(source_plugin) > 512 or len(memory_id) > 512:
            raise ExternalMemoryContractError("external source identifiers are too long")
        if kind not in {"persona_life", "proactive_message"}:
            raise ExternalMemoryContractError("unsupported external memory kind")
        tags = record.get("tags", [])
        if not isinstance(tags, (list, tuple)) or any(
            not isinstance(tag, str) or not tag.strip() for tag in tags
        ):
            raise ExternalMemoryContractError("external tags must be non-empty strings")
        tags = list(tags)
        metadata = record.get("metadata", {})
        if not isinstance(metadata, Mapping):
            raise ExternalMemoryContractError("external metadata must be an object")
        tags = _json_value(tags, "external tags", max_bytes=16_384)
        metadata = _json_value(dict(metadata), "external metadata")
        optional = _json_value(
            {
                "occurred_at": record.get("occurred_at"),
                "confidence": record.get("confidence"),
                "importance": record.get("importance"),
            },
            "external record fields",
            max_bytes=16_384,
        )
        # A source_event is deliberately kept out of the conversation extractor
        # until that pipeline gains an explicit autonomous-activity contract.
        platform = "companion:" + scope.platform_instance_id
        identity = EventIdentity(
            platform_instance_id=platform,
            platform_name=scope.platform_instance_id,
            message_type="FriendMessage" if scope.is_private else "GroupMessage",
            # External records share one replay stream per source plugin and
            # host scope. The stable memory ID remains the deduplication key.
            unified_msg_origin=(
                f"external:{source_plugin}:{scope.installation_id}:"
                f"{scope.platform_instance_id}:{scope.native_scope_id}"
            ),
            session_id=scope.session_id,
            group_id=scope.group_id,
            account_id=scope.account_id,
            account_name="",
            self_id="",
            message_id=f"{source_plugin}:{memory_id}",
        )
        metadata = {
            "external_source_plugin": source_plugin,
            "external_memory_id": memory_id,
            "external_kind": kind,
            "external_installation_id": scope.installation_id,
            "external_scope_kind": scope.scope_kind,
            "external_native_scope_id": scope.group_id or scope.account_id,
            "external_account_id": scope.account_id,
            "external_session_id": scope.session_id,
            "external_tags": tags,
            "external_metadata": metadata,
            "external_occurred_at": optional["occurred_at"],
            "external_confidence": optional["confidence"],
            "external_importance": optional["importance"],
        }
        content_json = json.dumps(metadata, ensure_ascii=False, separators=(",", ":"))
        if source_plugin == COMPANION_PLUGIN_ID:
            metadata.update(receipt_fingerprints(content), text_cleaning_revision=cleaner.revision)
            content = cleaner.clean(content)
        return CapturedSource(
            identity=identity,
            actor_kind="external",
            content_text=content,
            observed_at=time.time(),
            source_type="source_event",
            message_id=identity.message_id,
            content_json=content_json,
            metadata=metadata,
            actor_account_id=scope.account_id,
            participant_ids=(),
            persona_id=scope.persona_id,
            identity_pending=True,
        )

    @staticmethod
    def _delivery(record, *, historical=False, cleaner=DEFAULT_CLEANER):
        """Translate a confirmed segment into the original AstrBot conversation."""
        scope = parse_external_scope(record["context"], delivered=True)
        meta = record["metadata"]
        if (record.get("schema") != PROVIDER_API_VERSION or record.get("source_plugin") != COMPANION_PLUGIN_ID
                or record.get("kind") != "proactive_message" or meta.get("delivery_status") != "delivered"):
            raise ExternalMemoryContractError("expected a confirmed companion delivery")
        delivered_at = datetime.fromisoformat(record["occurred_at"])
        if delivered_at.tzinfo is None:
            raise ExternalMemoryContractError("delivery time requires a timezone")
        message_type = "FriendMessage" if scope.is_private else "GroupMessage"
        message_id = f"companion:{scope.installation_id}:{record['memory_id']}"
        identity = EventIdentity(
            platform_instance_id=scope.platform_instance_id, platform_name="", message_type=message_type,
            unified_msg_origin=f"{scope.platform_instance_id}:{message_type}:{scope.session_id}",
            session_id=scope.session_id, group_id=scope.group_id, account_id=scope.account_id,
            account_name="", self_id=_text(record["context"]["scope"].get("bot_id")), message_id=message_id)
        metadata = {
            "delivered_proactive": True, "proactive_contact_id": meta["contact_id"],
            "delivery_id": meta["delivery_id"], "delivery_sequence": meta["sequence"],
            "external_memory_id": record["memory_id"], "external_source_plugin": COMPANION_PLUGIN_ID,
            "delivery_target_id": scope.native_scope_id,
            "host_conversation_id": meta.get("host_conversation_id"),
            "platform_message_id": meta.get("platform_message_id"), "replay_only": historical,
        }
        metadata.update(receipt_fingerprints(record["content"]), text_cleaning_revision=cleaner.revision)
        return CapturedSource(identity=identity, actor_kind="agent", message_id=message_id,
                              content_text=cleaner.clean(record["content"]), observed_at=delivered_at.timestamp(),
                              actor_account_id=identity.self_id, metadata=metadata,
                              persona_id=_text(meta.get("host_persona_id")) or None)

    def delivered_source_id(self, record):
        return self._delivery(record).source_id

    async def delivery_space(self, record):
        source = self._delivery(record, cleaner=self.plugin.source_store.cleaner)
        if not source.content_text.strip() or not is_supported_persona(source.persona_id):
            return None
        identity = source.identity
        if identity.is_group:
            return group_space(source.persona_id, identity.platform_instance_id, identity.group_id)
        owner = await self.plugin.source_store.delivery_recipient(identity.platform_instance_id, identity.account_id,
            platform_name(self.plugin.context, identity.platform_instance_id), source.observed_at)
        return private_space(source.persona_id, owner) if owner else None

    async def accept_delivery(self, record, *, historical=False):
        source = self._delivery(record, historical=historical, cleaner=self.plugin.source_store.cleaner)
        if not is_supported_persona(source.persona_id):
            return None
        adapter = platform_name(self.plugin.context, source.identity.platform_instance_id) or ""
        source = replace(source, identity=replace(source.identity, platform_name=adapter))
        if await self.plugin.source_store.has_delivery(source):
            return source.source_id
        if not source.content_text.strip():
            return None
        submitted = await self.plugin.ingress.submit(source)
        return source.source_id if submitted.durable and submitted.state != "failed" else None

    async def write(self, record: Any) -> str | None:
        """Persist a companion life/proactive record through durable ingress."""

        root = _mapping(record, "record")
        if root.get("schema") != PROVIDER_API_VERSION:
            raise ExternalMemoryContractError("unsupported external memory write schema")
        if root.get("kind") == "proactive_message" and _mapping(root.get("metadata", {}), "metadata").get("delivery_status") == "delivered":
            return await self.accept_delivery(root)
        scope = parse_external_scope(root.get("context"))
        source = self._external_source(root, scope, cleaner=self.plugin.source_store.cleaner)
        try:
            existing = await self.plugin.source_store.get_records((source.source_id,))
        except ValueError:
            existing = []
        if existing:
            row = existing[0]
            try:
                existing_metadata = json.loads(row.get("metadata_json") or "{}")
            except (TypeError, ValueError):
                existing_metadata = {}
            expected_metadata = source.metadata
            scope_fields = (
                "external_source_plugin", "external_kind", "external_installation_id",
                "external_scope_kind", "external_native_scope_id", "external_account_id",
                "external_session_id",
            )
            metadata_conflict = any(
                key in existing_metadata and existing_metadata.get(key) != expected_metadata.get(key)
                for key in scope_fields
            )
            same_text = row.get("source_hash") == hashlib.sha256(source.content_text.encode()).hexdigest()
            if source.metadata["external_source_plugin"] == COMPANION_PLUGIN_ID:
                same_text = receipt_text_matches(row.get("content_text") or "", existing_metadata,
                                                 source.content_text, source.metadata)
            if (not same_text
                    or row.get("source_type") not in {None, "source_event"}
                    or row.get("actor_kind") not in {None, "external"}
                    or row.get("persona_id") != source.persona_id
                    or row.get("actor_account_id", "") != source.actor_account_id
                    or row.get("platform_instance_id") not in {None, source.identity.platform_instance_id}
                    or (row.get("group_id") or "") != source.identity.group_id
                    or metadata_conflict):
                raise ExternalMemoryContractError("external memory id conflicts with existing content")
            return source.source_id
        if not source.content_text.strip():
            return None
        submission = await self.plugin.ingress.submit(source)
        return source.source_id if submission.state in {"queued", "captured"} else None

    async def summary(self, request: Any) -> dict[str, dict[str, int]]:
        root = _mapping(request, "request")
        if root.get("schema") != PROVIDER_API_VERSION or root.get("purpose") != "summary":
            raise ExternalMemoryContractError("unsupported external memory summary request")
        space, _ = await self._private_space(root.get("context"))
        if space is None:
            return {"counts": {"profile": 0, "preference": 0, "relationship": 0,
                                "private_conversation": 0, "other": 0, "total": 0}}
        generation = await self.plugin.memory_store.get_active_generation(space["space_id"])
        if generation is None:
            return {"counts": {"profile": 0, "preference": 0, "relationship": 0,
                                "private_conversation": 0, "other": 0, "total": 0}}
        params = (space["space_id"], generation["generation_id"])
        profiles = await self.plugin.memory_store.database.fetchone(
            "SELECT COUNT(*) AS count FROM user_profiles WHERE space_id=? AND generation_id=? AND status='active'", params)
        relationships = await self.plugin.memory_store.database.fetchone(
            "SELECT COUNT(*) AS count FROM relationship_states WHERE space_id=? AND generation_id=? AND status='active'", params)
        nodes = await self.plugin.memory_store.database.fetchone(
            "SELECT COUNT(*) AS count FROM memory_nodes WHERE space_id=? AND generation_id=? AND status IN ('active','historical')", params)
        profile_count = int(profiles["count"] if profiles else 0)
        relationship_count = int(relationships["count"] if relationships else 0)
        node_count = int(nodes["count"] if nodes else 0)
        counts = {"profile": profile_count, "preference": 0, "relationship": relationship_count,
                  "private_conversation": node_count, "other": 0}
        counts["total"] = sum(counts.values())
        return {"counts": counts}


__all__ = [
    "COMPANION_PLUGIN_ID",
    "PROVIDER_API_VERSION",
    "PROVIDER_ID",
    "ExternalMemoryAdapter",
    "ExternalMemoryContractError",
    "ExternalMemoryScope",
    "discover_companion_api",
    "parse_external_scope",
]
