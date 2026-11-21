"""Fixed private image slots for a durable extraction window. Image bytes stay in the cache."""

import hashlib
import json


async def prepare_slots(sources, cache, records, *, limit, evidence_only=False):
    slots = []
    aliases = {record["source_id"]: f"s{index}" for index, record in enumerate(records, 1)}
    for index, record in enumerate(records, 1):
        metadata = json.loads(record.get("metadata_json") or "{}")
        accompanying = {}
        if linked := metadata.get("accompanying_response_source_id"):
            if linked in aliases:
                accompanying["sent_with_source"] = aliases[linked]
            else:
                chain = json.loads(record.get("content_json") or "[]")
                accompanying["sent_with_text"] = "".join(
                    part.get("data", part).get("text", "") for part in chain
                    if isinstance(part, dict) and str(part.get("type", "")).lower() in {"plain", "text"})
                accompanying["sent_with_text"] = accompanying["sent_with_text"][:metadata["accompanying_text_chars"]]
        for item in metadata.get("image_attachments", []):
            slots.append({"source_id": record["source_id"], "source_alias": f"s{index}",
                          **accompanying, "slot": item["slot"], "authors": item.get("author_path", []),
                          "caption": item.get("caption")})
    if not slots:
        return []
    evidence = {(e["source_id"], e["slot"]): e for e in await sources.list_image_evidence(
        tuple(dict.fromkeys(a["source_id"] for a in slots)))}
    selected = []
    for index, item in enumerate(slots):
        key = item["source_id"], item["slot"]
        if key in evidence:
            item["evidence"] = evidence[key]["description"]
            item["evidence_origin"] = evidence[key]["origin"]
        elif evidence_only or index >= limit:
            continue
        elif cache:
            item["caption"] = item["caption"] or await cache.caption(*key)
        selected.append(item)
    return selected


async def load_slots(manifest, cache):
    """Freeze the compressed content hash on first use. Never substitute another image on retry."""
    attachments = []
    lost_image = False
    for item in manifest:
        attachment = item.copy()
        if not (item.get("evidence") or item.get("caption") or item.get("reason")):
            data = await cache.read(item["source_id"], item["slot"]) if cache else None
            fingerprint = hashlib.sha256(data).hexdigest() if data else None
            if fingerprint and item.get("sha256", fingerprint) == fingerprint:
                item["sha256"] = fingerprint
                attachment.update(sha256=fingerprint, jpeg=data)
            else:
                lost_image |= bool(item.pop("sha256", None))
                item["reason"] = "image_unavailable"
                attachment = item.copy()
        attachments.append(attachment)
    return attachments, lost_image


def text_budget(messages):
    return sum(len(text.encode()) for message in messages for text in (
        [message["content"]] if isinstance(message["content"], str) else
        [part["text"] for part in message["content"] if part["type"] == "text"]))


def collects_evidence(attachments):
    return any(not item.get("evidence") and (item.get("sha256") or item.get("caption"))
               for item in attachments)


def image_counts(window):
    """Counts survive indexing checkpoints without retaining source text."""
    if "image_counts" in window:
        return window["image_counts"]
    counts = {}
    for slot in window.get("image_slots", []):
        kind = slot.get("reason")
        if not kind:
            kind = "saved_evidence" if slot.get("evidence") else "host_caption" if slot.get("caption") else "image"
        counts[kind] = counts.get(kind, 0) + 1
    return counts
