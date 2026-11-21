"""Validation helpers for autonomous notes."""

from __future__ import annotations

NOTE_STATUSES = frozenset({"active", "archived", "deleted"})


def validate_note_status(status: str) -> str:
    value = str(status).strip().lower()
    if value not in NOTE_STATUSES:
        raise ValueError(f"unsupported note status: {status}")
    return value


def validate_note_content(content: str, *, max_chars: int = 20000) -> str:
    value = str(content).strip()
    if not value:
        raise ValueError("note content must not be empty")
    if max_chars > 0 and len(value) > max_chars:
        raise ValueError(f"note content exceeds {max_chars} characters")
    return value
