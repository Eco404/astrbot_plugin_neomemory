"""Memory is enabled only for explicit supported persona identities."""

from __future__ import annotations

UNSUPPORTED_PERSONAS = frozenset({"default", "_chatui_default_", "[%None]"})


def is_unsupported_persona(persona_id: str | None) -> bool:
    return isinstance(persona_id, str) and persona_id.strip() in UNSUPPORTED_PERSONAS


def is_supported_persona(persona_id: str | None) -> bool:
    return isinstance(persona_id, str) and bool(persona_id.strip()) and not is_unsupported_persona(persona_id)
