"""One conversation source per proactive contact and actual delivery destination."""

import hashlib
import json
from dataclasses import replace

from .image_capture import without_image_data


# A contact can end with an unexported voice, poke or unsuccessful delivery.
PART_WAIT_SECONDS = 60


def contact_source(segment, installation_id, part_count):
    meta = segment.metadata
    scope = [installation_id, meta["proactive_contact_id"], segment.persona_id,
             segment.identity.stream_id, segment.actor_account_id, meta["delivery_target_id"],
             meta["host_conversation_id"]]
    scope_hash = hashlib.sha256(json.dumps(scope, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
    message_id = f"companion-contact:{scope_hash}"
    text = without_image_data(segment.content_text)
    part = {key: meta[key] for key in ("external_memory_id", "delivery_id", "delivery_sequence",
                                        "platform_message_id", "receipt_input_hash", "text_cleaning_revision")}
    part.update(receipt_source_id=segment.source_id, observed_at=segment.observed_at, length=len(text))
    metadata = {key: meta[key] for key in ("delivered_proactive", "proactive_contact_id", "external_source_plugin",
                                           "delivery_target_id", "host_conversation_id", "replay_only")}
    metadata.update(delivery_parts=[part], part_count=part_count)
    return replace(segment, message_id=message_id, identity=replace(segment.identity, message_id=message_id),
                   content_text=text, metadata=metadata)


def part_texts(text, parts):
    """Use stored boundaries so a newline inside a delivered part remains intact."""
    offset = 0
    result = []
    for part in parts:
        end = offset + part["length"]
        result.append(text[offset:end])
        offset = end + 1
    return result


def merge_deliveries(text, metadata, incoming):
    """Merge ordered, confirmed parts while retaining the first accepted text projection."""
    parts = metadata["delivery_parts"]
    by_sequence = {part["delivery_sequence"]: (part, body)
                   for part, body in zip(parts, part_texts(text, parts))}
    incoming_parts = incoming.metadata["delivery_parts"]
    sequences = {part["receipt_source_id"]: part["delivery_sequence"] for part in parts}
    for part, body in zip(incoming_parts, part_texts(incoming.content_text, incoming_parts)):
        seq = part["delivery_sequence"]
        if sequences.get(part["receipt_source_id"], seq) != seq:
            raise ValueError("delivery sequence conflicts with existing source")
        if seq in by_sequence:
            original = by_sequence[seq][0]
            if any(original[key] != part[key] for key in
                   ("receipt_source_id", "delivery_id", "observed_at", "receipt_input_hash", "platform_message_id")):
                raise ValueError("delivery id conflicts with existing source")
        else:
            by_sequence[seq] = (part, body)
    if len(by_sequence) == len(parts):
        return None
    ordered = [by_sequence[seq] for seq in sorted(by_sequence)]
    merged = {**metadata, "delivery_parts": [part for part, _ in ordered]}
    return replace(incoming, content_text="\n".join(body for _, body in ordered), metadata=merged,
                   observed_at=max(part["observed_at"] for part, _ in ordered))
