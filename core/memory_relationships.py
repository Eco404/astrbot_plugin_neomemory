"""Validation helpers for persona relationship state."""

from __future__ import annotations

from .text_lengths import validate_readable_text


def validate_relationship_text(value: str, *, max_chars: int = 4000) -> str:
    result = str(value).strip()
    return validate_readable_text(result, max_chars, label="relationship text")
