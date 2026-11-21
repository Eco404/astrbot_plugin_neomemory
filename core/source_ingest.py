"""Normalize AstrBot events into source records without model calls."""

from __future__ import annotations

import hashlib
import inspect
import json
import time
from dataclasses import dataclass, field
from typing import Any

from .text_cleaning import DEFAULT_CLEANER
from .image_capture import kind, project_images, without_image_data
from .identity import (
    EventIdentity,
    extract_event_identity,
    extract_event_persona_id,
    stable_source_id,
)


def _component_dump(component: Any) -> Any:
    if hasattr(component, "model_dump"):
        try:
            return component.model_dump(mode="json")
        except TypeError:
            try:
                return component.model_dump()
            except Exception:
                pass
        except Exception:
            pass
    if isinstance(component, dict):
        return component
    # Some AstrBot versions use Pydantic v1. Its dict() retains Reply.sender_id
    # and At.name, which the platform wire serializers can discard.
    for name in ("dict", "toDict", "to_dict"):
        dump = getattr(component, name, None)
        if callable(dump) and not inspect.iscoroutinefunction(dump):
            try:
                output = dump()
            except Exception:
                continue
            if isinstance(output, dict):
                return output
            if inspect.iscoroutine(output):
                output.close()
    return {"type": type(component).__name__, "text": str(component)}


def _components(event: Any) -> list[dict] | None:
    items = getattr(getattr(event, "message_obj", None), "message", None)
    if not isinstance(items, (list, tuple)):
        return None
    return [_component_dump(item) for item in items]


def _serialize_components(components: list[dict] | None) -> str | None:
    if components is None:
        return None
    try:
        return json.dumps(components, ensure_ascii=False, separators=(",", ":"), default=str)
    except (TypeError, ValueError):
        return None


def serialize_message_components(event: Any) -> str | None:
    parts = _components(event)
    return _serialize_components(project_images(parts, private=False)[0]) if parts is not None else None


def _field(item: Any, key: str) -> Any:
    return item.get(key) if isinstance(item, dict) else getattr(item, key, None)


def _qq_mentions(raw: Any) -> list[dict]:
    # The SDK can omit member_openid/is_you from its projected user objects.
    # Prefer the actual QQ payload when AstrBot exposes it.
    payload = getattr(raw, "raw_data", raw)
    if isinstance(payload, dict) and "mentions" in payload:
        mentions = payload["mentions"]
    else:
        mentions = getattr(raw, "mentions", ())
    targets = []
    for mention in mentions or ():
        mention_id = str(_field(mention, "id") or "")
        account_id = str(_field(mention, "member_openid") or mention_id)
        if account_id:
            target = {"kind": "mention", "account_id": account_id}
            if mention_id and mention_id != account_id:
                target["mention_id"] = mention_id
            if name := _field(mention, "username"):
                target["display_name"] = str(name)
            if _field(mention, "is_you") is True:
                target["is_self"] = True
            targets.append(target)
    return targets


def _group_addressing(event: Any, identity: EventIdentity, components: list[dict]) -> list[dict]:
    raw = getattr(getattr(event, "message_obj", None), "raw_message", None)
    official = identity.platform_name == "qq_official"
    targets = _qq_mentions(raw) if official else []
    qq_ids = {target[key] for target in targets for key in ("account_id", "mention_id") if key in target}
    for item in components:
        component_type = item.get("type", "")
        kind = str(getattr(component_type, "value", component_type)).casefold()
        data = item.get("data", item)
        if kind == "at":
            account = str(data.get("qq") or "")
            if account == "all":
                targets.append({"kind": "mention_all"})
                continue
            if not account or account in qq_ids:
                continue
            # QQ channel messages can include a synthetic At("qq_official")
            # even when the raw message does not address this bot.
            if official and account == "qq_official" and _field(raw, "channel_id") and not _field(raw, "group_openid"):
                continue
            target = {"kind": "mention", "account_id": account}
            name = data.get("name")
        elif kind == "reply":
            account = str(data.get("sender_id") or "")
            target = {"kind": "reply"}
            if account:
                target["account_id"] = account
            if data.get("id"):
                target["message_id"] = str(data["id"])
            name = data.get("sender_nickname")
            if not account and "message_id" not in target:
                continue
        else:
            continue
        if name:
            target["display_name"] = str(name)
        if account and account == identity.self_id:
            target["is_self"] = True
        targets.append(target)
    return targets


def _event_text(event: Any) -> str:
    getter = getattr(event, "get_message_str", None)
    if callable(getter):
        try:
            return str(getter() or "")
        except Exception:
            pass
    return str(getattr(event, "message_str", "") or "")


def _bounded_text(text: str, max_chars: int) -> tuple[str, bool, int]:
    length = len(text)
    if max_chars > 0 and length > max_chars:
        return text[:max_chars], True, length
    return text, False, length


@dataclass(frozen=True, slots=True)
class CapturedSource:
    """A validated, I/O-free source record candidate."""

    identity: EventIdentity
    actor_kind: str
    content_text: str
    observed_at: float
    source_type: str = "conversation"
    message_id: str = ""
    response_id: str = ""
    content_json: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    actor_account_id: str = ""
    actor_account_name: str = ""
    participant_ids: tuple[str, ...] = ()
    persona_id: str | None = None
    identity_pending: bool = False

    @property
    def dedup_key(self) -> str:
        if self.metadata.get("delivered_proactive"):
            return f"delivery:{self.message_id}"
        external_id = self.response_id or self.message_id or self.identity.message_id
        if external_id:
            return f"external:{self.identity.stream_id}:{self.actor_kind}:{external_id}"
        digest = hashlib.sha256(
            "\x1f".join(
                (
                    self.identity.stream_id,
                    self.actor_kind,
                    self.content_text,
                    str(self.observed_at),
                )
            ).encode("utf-8")
        ).hexdigest()
        return f"fallback:{self.identity.stream_id}:{self.actor_kind}:{digest}"

    @property
    def source_id(self) -> str:
        return stable_source_id(self.dedup_key)


def normalize_event(
    event: Any,
    *,
    actor_kind: str = "user",
    max_content_chars: int = 0,
    observed_at: float | None = None,
    response_id: str = "",
    persona_id: str | None = None,
    resolve_event_persona: bool = True,
    host_conversation_id: str | None = None,
    image_references: dict | None = None,
) -> CapturedSource | None:
    """Normalize a user/platform event; return None for empty content."""

    identity = extract_event_identity(event)
    resolved_persona_id = (persona_id or extract_event_persona_id(event)) if resolve_event_persona else persona_id
    text, truncated, original_length = _bounded_text(
        without_image_data(_event_text(event)), max_content_chars
    )
    components = _components(event)
    slots = []
    if components is not None:
        raw = getattr(getattr(event, "message_obj", None), "raw_message", None)
        components, slots = project_images(components, private=identity.is_private, raw=raw,
                                           references=image_references)
    component_json = _serialize_components(components)
    if not text.strip() and not component_json:
        return None
    metadata: dict[str, Any] = {
        "content_truncated": truncated,
        "original_content_length": original_length,
    }
    if slots:
        metadata["image_attachments"] = slots
    if identity.is_group and actor_kind == "user":
        addressing = _group_addressing(event, identity, components or [])
        if addressing:
            metadata["group_addressing"] = addressing
    if response_id:
        metadata["response_id"] = response_id
    if host_conversation_id:
        metadata["host_conversation_id"] = host_conversation_id
    return CapturedSource(
        identity=identity,
        actor_kind=actor_kind,
        content_text=text,
        observed_at=time.time() if observed_at is None else float(observed_at),
        message_id=identity.message_id,
        response_id=response_id,
        content_json=component_json,
        metadata=metadata,
        actor_account_id=identity.account_id,
        actor_account_name=identity.account_name,
        participant_ids=tuple(filter(None, (identity.account_id,))),
        persona_id=resolved_persona_id,
        identity_pending=(
            resolved_persona_id is None
            or identity.platform_instance_id in {"", "unknown"}
            or identity.unified_msg_origin
            in {"", "unknown", "unknown:OtherMessage:unknown"}
            or not identity.account_id
        ),
    )


def normalize_agent_response(
    event: Any,
    response: Any,
    *,
    max_content_chars: int = 0,
    observed_at: float | None = None,
    persona_id: str | None = None,
    resolve_event_persona: bool = True,
    host_conversation_id: str | None = None,
    cleaner=DEFAULT_CLEANER,
) -> CapturedSource | None:
    """Normalize a final LLM response as an agent source record."""

    if bool(getattr(response, "is_chunk", False)):
        return None
    content = cleaner.clean(str(getattr(response, "completion_text", "") or ""))
    if not content.strip():
        return None
    identity = extract_event_identity(event)
    resolved_persona_id = (persona_id or extract_event_persona_id(event)) if resolve_event_persona else persona_id
    response_id = str(getattr(response, "id", "") or "")
    text, truncated, original_length = _bounded_text(without_image_data(content), max_content_chars)
    metadata = {
        "text_cleaning_revision": cleaner.revision,
        "content_truncated": truncated,
        "original_content_length": original_length,
        "response_role": str(getattr(response, "role", "assistant") or "assistant"),
        "trigger_message_id": identity.message_id or None,
    }
    if host_conversation_id:
        metadata["host_conversation_id"] = host_conversation_id
    return CapturedSource(
        identity=identity,
        actor_kind="agent",
        content_text=text,
        observed_at=time.time() if observed_at is None else float(observed_at),
        message_id=identity.message_id,
        response_id=response_id,
        metadata=metadata,
        actor_account_id=identity.self_id,
        actor_account_name="Agent",
        participant_ids=tuple(filter(None, (identity.self_id,))),
        persona_id=resolved_persona_id,
        identity_pending=(
            resolved_persona_id is None
            or identity.platform_instance_id in {"", "unknown"}
            or identity.unified_msg_origin
            in {"", "unknown", "unknown:OtherMessage:unknown"}
        ),
    )


def clean_agent_component_text(components, cleaner=DEFAULT_CLEANER):
    # Project only the outer sender's words. Quoted replies and forwarded authors own theirs.
    plain = [part.get("data", part) for part in components if kind(part) in {"plain", "text"}]
    words = cleaner.clean_parts([part.get("text", "") for part in plain])
    for part, word in zip(plain, words):
        part["text"] = word
    return "".join(words)


def normalize_sent_images(event, chain, *, persona_id, send_number, image_references,
                          max_content_chars=0, host_conversation_id=None, cleaner=DEFAULT_CLEANER):
    """Capture only attachments delivered on this outgoing private message chain."""
    identity = extract_event_identity(event)
    parts = [_component_dump(component) for component in chain]
    components, slots = project_images(parts, private=identity.is_private, references=image_references)
    if not slots:
        return None
    text = clean_agent_component_text(components, cleaner)
    text, truncated, original_length = _bounded_text(text, max_content_chars)
    metadata = {"text_cleaning_revision": cleaner.revision, "image_attachments": slots, "trigger_message_id": identity.message_id or None,
                "content_truncated": truncated, "original_content_length": original_length}
    if host_conversation_id:
        metadata["host_conversation_id"] = host_conversation_id
    # Separate media sends associated with one inbound trigger have distinct source identities.
    delivery_id = f"sent-image:{identity.message_id}:{send_number}" if identity.message_id else ""
    return CapturedSource(identity=identity, actor_kind="agent", actor_account_id=identity.self_id,
                          actor_account_name="Agent", persona_id=persona_id, observed_at=time.time(),
                          message_id=identity.message_id, response_id=delivery_id, metadata=metadata,
                          content_text=text, content_json=_serialize_components(components),
                          participant_ids=tuple(filter(None, (identity.self_id,))),
                          identity_pending=persona_id is None)
