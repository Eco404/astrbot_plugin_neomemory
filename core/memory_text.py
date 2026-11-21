"""Stable person references in derived prose, resolved only for presentation."""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from .relationship_state import FORMAT as RELATIONSHIP_FORMAT, relationship_text

_USER_REFERENCE = re.compile(r"\{\{user:([^{}\s]+)\}\}")
_TEXT_FIELDS = ("content_text", "description", "state_text", "event_time_text", "title", "summary_text")


def user_reference_ids(texts: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(match.group(1) for text in texts
                               for match in _USER_REFERENCE.finditer(text)))


def map_user_references(text: str, resolve: Callable[[str], str]) -> str:
    """Replace explicit references once; never rewrite names or ordinary words."""
    return _USER_REFERENCE.sub(lambda match: "{{user:" + resolve(match.group(1)) + "}}", text)


def render_memory_text(text: str, names: Mapping[str, str] | None = None) -> str:
    """Disambiguate shared nicknames across the supplied presentation context."""
    names = names or {}
    counts = Counter(names.values())

    def display(match):
        identity = match.group(1)
        name = names.get(identity)
        if not name:
            return f"未命名成员（{identity}）"
        return f"{name}（{identity}）" if counts[name] > 1 else name

    return _USER_REFERENCE.sub(display, text)


def render_display_text(text: str, names: Mapping[str, str]) -> str:
    """Render explicit person references by current name on the WebUI."""
    return _USER_REFERENCE.sub(lambda match: names.get(match.group(1)) or "未命名用户", text)


def memory_texts(item: Mapping[str, Any]) -> list[str]:
    texts = [item[field] for field in _TEXT_FIELDS if isinstance(item.get(field), str)] + list(item.get("tags", []))
    for container in (item, item.get("state") or {}):
        for slot in ("long_term", "aftereffect"):
            value = container.get(slot)
            if isinstance(value, Mapping) and isinstance(value.get("body"), str):
                texts.append(value["body"])
    if item.get("emotion"):
        texts.append(item["emotion"]["description"])
    return texts


def render_memory_item(item: Mapping[str, Any], names: Mapping[str, str], *, render_text=render_memory_text) -> dict[str, Any]:
    result = dict(item)
    for field in _TEXT_FIELDS:
        if isinstance(item.get(field), str):
            result["display_" + field] = render_text(item[field], names)
    state = item.get("state")
    if isinstance(state, Mapping) and state.get("format") == RELATIONSHIP_FORMAT:
        if "state_text" in item:
            result["display_state_text"] = render_text(relationship_text(state), names)
        result["display_state"] = {**state, **{slot: {**state[slot],
            "body": render_text(state[slot]["body"], names)} for slot in ("long_term", "aftereffect")}}
    if "tags" in item:
        result["display_tags"] = [render_text(tag, names) for tag in item["tags"]]
    if item.get("emotion"):
        result["display_emotion"] = {**item["emotion"],
            "description": render_text(item["emotion"]["description"], names)}
    return result
