"""Programmatic fragmentation for oversized text-only conversation sources.

Fragments preserve the complete source text and receive stable source IDs. The
model sees the fragment relationship as input metadata, while derived records
can cite each fragment independently. Structured attachments and forwards stay
atomic until they have a dedicated fragment representation.
"""

from __future__ import annotations

import json
import re
from dataclasses import replace
from typing import Any


_TRANSCRIPT_MARKER = re.compile(r"(?m)^===\s*消息\s+\d+\s*===\s*$")
_PLAIN_TYPES = {"plain", "text"}


def _kind(part: Any) -> str:
    return str(part.get("type", "")).casefold() if isinstance(part, dict) else ""


def _plain_components(content_json: str | None) -> bool:
    if not content_json:
        return True
    try:
        parts = json.loads(content_json)
    except (TypeError, ValueError):
        return False
    return isinstance(parts, list) and all(_kind(part) in _PLAIN_TYPES for part in parts)


def _fragment_components(content_json: str | None, text: str) -> str | None:
    if not content_json:
        return None
    return json.dumps([{"type": "Plain", "text": text}], ensure_ascii=False, separators=(",", ":"))


def _fits(text: str, *, max_chars: int, max_tokens: int) -> bool:
    return len(text) <= max_chars and len(text.encode("utf-8")) + 8 <= max_tokens


def _hard_chunks(text: str, *, max_chars: int, max_tokens: int) -> list[str]:
    chunks: list[str] = []
    remaining = text
    while remaining:
        low, high = 1, len(remaining)
        best = 0
        while low <= high:
            middle = (low + high) // 2
            if _fits(remaining[:middle], max_chars=max_chars, max_tokens=max_tokens):
                best = middle
                low = middle + 1
            else:
                high = middle - 1
        if best == 0:
            raise ValueError("fragment budget cannot contain one character")
        chunks.append(remaining[:best])
        remaining = remaining[best:]
    return chunks


def _units(text: str) -> list[str]:
    markers = list(_TRANSCRIPT_MARKER.finditer(text))
    if markers:
        prefix = text[: markers[0].start()]
        units = []
        if prefix:
            units.append(prefix)
        units.extend(text[match.start():next_match.start() if next_match else len(text)]
                     for match, next_match in zip(markers, markers[1:] + [None]))
        return units
    return text.splitlines(keepends=True) or [text]


def _pack(text: str, *, max_chars: int, max_tokens: int) -> list[str]:
    units = _units(text)
    chunks: list[str] = []
    current = ""
    for unit in units:
        if not unit:
            continue
        if _fits(current + unit, max_chars=max_chars, max_tokens=max_tokens):
            current += unit
            continue
        if current:
            chunks.append(current)
            current = ""
        if _fits(unit, max_chars=max_chars, max_tokens=max_tokens):
            current = unit
        else:
            chunks.extend(_hard_chunks(unit, max_chars=max_chars, max_tokens=max_tokens))
    if current:
        chunks.append(current)
    return chunks


def fragment_source(source: Any, *, max_chars: int, max_tokens: int) -> tuple[Any, ...]:
    """Split an oversized text-only conversation source without dropping bytes.

    The first fragment keeps the original message/response identity. Subsequent
    fragments use deterministic suffixes so ingress deduplication and source
    citations remain stable across repeated deliveries.
    """

    if (
        getattr(source, "source_type", "conversation") != "conversation"
        or not isinstance(getattr(source, "content_text", None), str)
        or not isinstance(max_chars, int)
        or not isinstance(max_tokens, int)
        or max_chars <= 0
        or max_tokens <= 8
        or _fits(source.content_text, max_chars=max_chars, max_tokens=max_tokens)
        or not _plain_components(getattr(source, "content_json", None))
        or getattr(source, "metadata", {}).get("image_attachments")
    ):
        return (source,)

    chunks = _pack(source.content_text, max_chars=max_chars, max_tokens=max_tokens)
    if len(chunks) <= 1:
        return (source,)
    parent_source_id = source.source_id
    total = len(chunks)
    fragments = []
    for index, chunk in enumerate(chunks):
        metadata = dict(source.metadata)
        metadata["source_fragment"] = {
            "parent_source_id": parent_source_id,
            "index": index + 1,
            "count": total,
        }
        metadata["fragment_original_content_length"] = len(source.content_text)
        updates = {
            "content_text": chunk,
            "content_json": _fragment_components(source.content_json, chunk),
            "metadata": metadata,
        }
        if index:
            suffix = f":fragment:{index + 1}"
            updates["message_id"] = (source.message_id or source.identity.message_id or parent_source_id) + suffix
            if source.response_id:
                updates["response_id"] = source.response_id + suffix
        fragments.append(replace(source, **updates))
    return tuple(fragments)


def fragment_info(record: dict[str, Any]) -> dict[str, int | str] | None:
    """Return validated prompt metadata for a persisted source fragment."""

    metadata = record.get("metadata")
    if not isinstance(metadata, dict):
        try:
            metadata = json.loads(record.get("metadata_json") or "{}")
        except (TypeError, ValueError):
            return None
    info = metadata.get("source_fragment")
    if not isinstance(info, dict):
        return None
    parent = info.get("parent_source_id")
    index = info.get("index")
    count = info.get("count")
    if not isinstance(parent, str) or not parent or type(index) is not int or type(count) is not int:
        return None
    if not 1 <= index <= count:
        return None
    return {"parent_source_id": parent, "index": index, "count": count}
