"""Small, optional emotional context carried by memory node content versions."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from .text_lengths import USER_REFERENCE_MAX_CHARS, encoded_text_limit, validate_readable_text

EMOTION_TAGS = (
    "joy", "sadness", "anger", "fear", "surprise", "trust", "concern", "affection", "neutral",
)
EMOTION_DESCRIPTION_MAX_CHARS = 160
EMOTION_REFERENCE_MAX_CHARS = USER_REFERENCE_MAX_CHARS
EMOTION_DESCRIPTION_MAX_ENCODED_CHARS = encoded_text_limit(EMOTION_DESCRIPTION_MAX_CHARS)
EMOTION_UNSET = object()


def validate_emotion(value: Any) -> dict[str, Any] | None:
    """Validate and normalize each tag and its individual score."""
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ValueError("emotion requires tags and description, or null")
    if set(value) != {"tags", "description"}:
        raise ValueError("emotion requires tags and description, or null")
    tags = value["tags"]
    if not isinstance(tags, list) or not 1 <= len(tags) <= 3:
        raise ValueError("emotion tags require one to three unique supported values")
    names = []
    normalized = []
    for item in tags:
        if not isinstance(item, Mapping) or set(item) != {"tag", "intensity"}:
            raise ValueError("each emotion tag requires its own tag and intensity")
        tag, intensity = item["tag"], item["intensity"]
        if not isinstance(tag, str) or tag not in EMOTION_TAGS or tag in names:
            raise ValueError("emotion tags require one to three unique supported values")
        if isinstance(intensity, bool) or not isinstance(intensity, (int, float)) or not 0 <= intensity <= 1:
            raise ValueError("emotion intensity must be between zero and one")
        names.append(tag)
        normalized.append({"tag": tag, "intensity": float(intensity)})
    if "neutral" in names and len(names) != 1:
        raise ValueError("neutral cannot be combined with another emotion")
    description = value["description"]
    if not isinstance(description, str) or not description.strip():
        raise ValueError("emotion description must contain text")
    description = description.strip()
    validate_readable_text(description, EMOTION_DESCRIPTION_MAX_CHARS, label="emotion description")
    return {"tags": sorted(normalized, key=lambda item: EMOTION_TAGS.index(item["tag"])), "description": description}


def encode_emotion(value: Any) -> str | None:
    emotion = validate_emotion(value)
    return None if emotion is None else json.dumps(emotion, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def decode_emotion(value: str | None) -> dict[str, Any] | None:
    return None if value is None else validate_emotion(json.loads(value))
