"""Expose forwarded text and its reported author separately from the sender's comment."""

import json
import re

_FORWARD_TYPES = {"forward", "node", "nodes"}
_FORWARD_TAG = re.compile(r'"type"\s*:\s*"(?:forward|nodes?)"', re.IGNORECASE)


def _kind(part):
    return str(part.get("type", "")).lower() if isinstance(part, dict) else ""


def _text(parts):
    if isinstance(parts, str):
        return parts
    pieces = []
    for part in parts:
        kind = _kind(part)
        if kind in {"plain", "text"}:
            pieces.append(part.get("data", part).get("text", ""))
        elif kind == "at":
            account = part.get("data", part).get("qq")
            if account:
                pieces.append(f"<@{account}>")
    return "".join(pieces)


def _multimsg_preview(card):
    # AstrBot Json components carry the decoded QQ card in data.
    if not isinstance(card, dict) or card.get("app") != "com.tencent.multimsg":
        return
    if (card.get("config") or {}).get("forward") != 1:
        return
    items = (card.get("meta") or {}).get("detail", {}).get("news", [])
    lines = [item["text"] for item in items if isinstance(item, dict) and item.get("text")]
    if lines:
        yield {"author": None, "text": "\n".join(lines), "preview_only": True}
    else:
        yield {"content_available": False}


def _forward_entries(parts):
    for part in parts:
        kind = _kind(part)
        if kind == "json":
            yield from _multimsg_preview(part.get("data"))
            continue
        if kind not in _FORWARD_TYPES:
            continue
        data = part.get("data", part)
        if kind == "nodes":
            entries = list(_forward_entries(data.get("nodes") or data.get("messages") or ()))
            yield from entries or [{"content_available": False}]
        elif kind == "forward":
            entries = list(_forward_entries(data.get("content") or ()))
            yield from entries or [{"content_available": False}]
        else:
            content = data.get("content") or ()
            text = _text(content)
            nested = list(_forward_entries(content)) if isinstance(content, list) else []
            if text or nested:
                entry = {"author": data.get("name") or data.get("nickname") or None, "text": text}
                if nested:
                    entry["forwarded_messages"] = nested
                yield entry
            else:
                yield {"content_available": False}


def project_forward(source):
    """Project only structured forwards, including those in historical component JSON."""
    raw = source.get("content_json")
    if not raw or ("com.tencent.multimsg" not in raw and not _FORWARD_TAG.search(raw)):
        return None
    parts = json.loads(raw)
    if not isinstance(parts, list):
        return None
    forwarded = list(_forward_entries(parts))
    if not forwarded:
        return None
    comment = _text(parts)
    if json.loads(source.get("metadata_json") or "{}").get("content_truncated"):
        comment = comment[:len(source["content_text"])]
    return {"sender_comment": comment, "forwarded_messages": forwarded}


def window_text(source):
    """Count the structured material which can actually enter an extraction window."""
    forward = project_forward(source)
    if forward is None:
        return source["content_text"]
    return json.dumps(forward, ensure_ascii=False, separators=(",", ":"))
