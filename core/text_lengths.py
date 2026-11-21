"""Readable prose limits, independent from encoded identifiers and payload size."""

from __future__ import annotations

import re

USER_REFERENCE_MAX_CHARS = 200
USER_REFERENCE_OVERHEAD = len("{{user:}}")
_USER_REFERENCE = re.compile(r"\{\{user:([^{}\s]+)\}\}")


def encoded_text_limit(readable_limit: int) -> int:
    """Capacity for a field made entirely of maximum-length user references."""
    return readable_limit * (USER_REFERENCE_MAX_CHARS + USER_REFERENCE_OVERHEAD)


def readable_text_length(text: str) -> int:
    """Count each complete, syntactically valid user reference as one unit.

    Reference ownership remains the responsibility of the reference/apply layer.
    Text is never normalized or truncated by this function.
    """
    if any(len(match.group(1)) > USER_REFERENCE_MAX_CHARS
           for match in _USER_REFERENCE.finditer(text)):
        raise ValueError("user reference exceeds character limit")
    readable = _USER_REFERENCE.sub("人", text)
    if "{{user:" in readable:
        raise ValueError("contains an invalid user reference")
    return len(readable)


def validate_readable_text(text: str, limit: int, *, label: str) -> str:
    if len(text) > encoded_text_limit(limit):
        raise ValueError(f"{label} exceeds encoded character limit")
    try:
        length = readable_text_length(text)
    except ValueError as exc:
        raise ValueError(f"{label} {exc}") from exc
    if length > limit:
        raise ValueError(f"{label} exceeds {limit} readable characters")
    return text
