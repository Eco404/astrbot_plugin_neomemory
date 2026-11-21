"""Pure identity and scope helpers for AstrBot message events."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _call(event: Any, method: str, attribute: str = "") -> str:
    callback = getattr(event, method, None)
    if callable(callback):
        try:
            return _text(callback())
        except Exception:
            return ""
    return _text(getattr(event, attribute or method, ""))


def _extra(event: Any, key: str) -> str:
    """Read an event extra without depending on a concrete AstrBot event class."""

    getter = getattr(event, "get_extra", None)
    if callable(getter):
        try:
            return _text(getter(key, None))
        except (TypeError, AttributeError):
            return ""
    extras = getattr(event, "_extras", None)
    if isinstance(extras, dict):
        return _text(extras.get(key))
    extras = getattr(event, "extras", None)
    if isinstance(extras, dict):
        return _text(extras.get(key))
    return ""


def message_type_value(event: Any) -> str:
    value = getattr(event, "get_message_type", None)
    if callable(value):
        try:
            message_type = value()
            return _text(getattr(message_type, "value", message_type))
        except Exception:
            pass
    return _text(getattr(event, "message_type", "OtherMessage")) or "OtherMessage"


def is_group_event(event: Any) -> bool:
    return message_type_value(event).casefold() == "groupmessage"


def is_private_event(event: Any) -> bool:
    return message_type_value(event).casefold() == "friendmessage"


@dataclass(frozen=True, slots=True)
class EventIdentity:
    """Stable fields extracted from an AstrBot event without I/O."""

    platform_instance_id: str
    platform_name: str
    message_type: str
    unified_msg_origin: str
    session_id: str
    group_id: str
    account_id: str
    account_name: str
    self_id: str
    message_id: str
    group_name: str = ""

    @property
    def is_group(self) -> bool:
        return self.message_type.casefold() == "groupmessage"

    @property
    def is_private(self) -> bool:
        return self.message_type.casefold() == "friendmessage"

    @property
    def stream_id(self) -> str:
        raw = "\x1f".join(
            (
                self.platform_instance_id,
                self.message_type,
                self.unified_msg_origin,
            )
        )
        return "stream_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def extract_event_identity(event: Any) -> EventIdentity:
    """Read only public event properties and tolerate incomplete test doubles."""

    message_obj = getattr(event, "message_obj", None)
    message_id = _text(getattr(message_obj, "message_id", ""))
    unified = _call(event, "unified_msg_origin", "unified_msg_origin")
    if not unified:
        unified = _call(event, "get_session_id", "session_id")
    platform_id = _call(event, "get_platform_id", "platform_id") or "unknown"
    platform_name = _call(event, "get_platform_name", "platform_name")
    return EventIdentity(
        platform_instance_id=platform_id,
        platform_name=platform_name,
        message_type=message_type_value(event),
        unified_msg_origin=unified or "unknown:OtherMessage:unknown",
        session_id=_call(event, "get_session_id", "session_id") or unified,
        group_id=_call(event, "get_group_id", "group_id"),
        account_id=_call(event, "get_sender_id", "sender_id"),
        account_name=_call(event, "get_sender_name", "sender_name"),
        self_id=_call(event, "get_self_id", "self_id"),
        message_id=message_id,
        group_name=_text(getattr(getattr(message_obj, "group", None), "group_name", "")),
    )


def extract_event_persona_id(event: Any) -> str | None:
    """Best-effort persona snapshot from explicit event metadata.

    AstrBot resolves the active persona asynchronously from the conversation
    manager. This helper only accepts values already attached to the event, so
    a missing value is represented as ``None`` rather than guessed.
    """

    for key in ("persona_id", "conversation_persona_id", "neomemory_persona_id"):
        value = _extra(event, key)
        if value:
            return value
    for attribute in ("persona_id", "conversation_persona_id"):
        value = _text(getattr(event, attribute, ""))
        if value:
            return value
    return None


def default_logical_user_id(platform_instance_id: str, account_id: str) -> str:
    """Seed an unseen identity from its instance or shared platform scope."""

    raw = f"{platform_instance_id}\x1f{account_id}"
    return "lu_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def private_memory_space_id(persona_id: str, logical_user_id: str) -> str:
    return f"persona:{persona_id}:user:{logical_user_id}"


def group_memory_space_id(
    persona_id: str, platform_instance_id: str, group_id: str
) -> str:
    return f"persona:{persona_id}:platform:{platform_instance_id}:group:{group_id}"


def stable_source_id(dedup_key: str) -> str:
    return "src_" + hashlib.sha256(dedup_key.encode("utf-8")).hexdigest()[:32]
