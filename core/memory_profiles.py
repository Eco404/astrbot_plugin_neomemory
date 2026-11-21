"""Small validation helpers for user profiles."""

from __future__ import annotations

from .text_lengths import validate_readable_text


def validate_profile_tags(tags: list[str] | tuple[str, ...], *, limit: int = 32) -> list[str]:
    result: list[str] = []
    for tag in tags:
        value = str(tag).strip()
        if value and value not in result:
            result.append(value)
    if len(result) > limit:
        raise ValueError(f"profile has more than {limit} tags")
    return result


def validate_profile_description(description: str, *, max_chars: int = 2000) -> str:
    value = str(description).strip()
    return validate_readable_text(value, max_chars, label="profile description")
