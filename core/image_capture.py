"""Project ordered image attachments from host components without persisting media references."""

import re
from collections.abc import Mapping

_INLINE_IMAGE = re.compile(r'(?:data:image/[\w.+-]+;base64,|base64://)[A-Za-z0-9+/=]+', re.I)
_CAPTION = re.compile(r"<image_caption>(.+?)</image_caption>", re.S)


def without_image_data(text):
    return _INLINE_IMAGE.sub("[图片]", text)


def kind(part):
    value = part.get("type", "") if isinstance(part, Mapping) else getattr(part, "type", "")
    return str(getattr(value, "value", value)).casefold()


def _onebot_image_hints(raw):
    message = raw.get("message", ()) if isinstance(raw, Mapping) else ()
    hints = []
    if isinstance(message, list):
        for part in message:
            if kind(part) == "image":
                hints.append(part.get("data", part))
    return hints


def _sticker(data):
    subtype = data.get("sub_type") if isinstance(data, Mapping) else getattr(data, "sub_type", None)
    summary = data.get("summary") if isinstance(data, Mapping) else getattr(data, "summary", None)
    return subtype in (1, "1") or summary in {"[动画表情]", "[商城表情]"}


def media_placeholder(message):
    """Label direct attachments when a recorded message has no query text."""
    hints = iter(_onebot_image_hints(getattr(message, "raw_message", None)))
    labels = []
    for part in getattr(message, "message", ()) or ():
        tag = kind(part)
        if tag == "image":
            data = part.get("data", part) if isinstance(part, Mapping) else part
            sticker = _sticker(next(hints, {})) or _sticker(data)
            labels.append("[表情]" if sticker else "[图片]")
        elif tag == "face":
            labels.append("[表情]")
    return " ".join(labels)


def project_images(components, *, private, raw=None, references=None):
    """Return image-free source components plus ordered, replayable attachment labels."""
    slots = []
    hints = iter(_onebot_image_hints(raw))
    ordinal = 0

    def visit(item, authors, *, collect=True, top=False):
        nonlocal ordinal
        if isinstance(item, str):
            return without_image_data(item)
        if isinstance(item, list):
            return [visit(part, authors, collect=collect) for part in item]
        if not isinstance(item, dict):
            return item
        tag = kind(item)
        data = item.get("data", item)
        if tag == "image":
            hint = next(hints, {}) if top else {}
            if _sticker(hint) or _sticker(data):
                return {"type": "Face"}
            if not private or not collect:
                return {"type": "Image"}
            ordinal += 1
            descriptor = {"slot": ordinal}
            if authors:
                descriptor["author_path"] = list(authors)
            slots.append(descriptor)
            ref = data.get("url") or hint.get("url") or data.get("file") or data.get("path")
            if references is not None and isinstance(ref, str) and ref:
                references[ordinal] = ref
            return {"type": "Image", "slot": ordinal}
        if tag == "node":
            authors = (*authors, data.get("name") or data.get("nickname") or None)
        # Quoted replies carry historical chains. Only forwarded nodes and the
        # outer live message can supply independently attributed attachments.
        if tag == "reply":
            collect = False
        return {k: visit(v, authors, collect=collect) for k, v in item.items()}

    return [visit(part, (), top=True) for part in components], slots


def unambiguous_caption(request, components):
    """The host can caption all current and quoted images in one string."""
    if sum(kind(part) == "image" for part in components) != 1:
        return None
    if any(kind(part) in {"reply", "node", "nodes", "forward"} for part in components):
        return None
    values = [match.strip() for part in getattr(request, "extra_user_content_parts", ()) or ()
              for match in _CAPTION.findall(getattr(part, "text", "") or "") if match.strip()]
    return values[0] if len(values) == 1 else None
