"""Independent persona understanding and interaction aftereffect slots."""

from __future__ import annotations

from .text_lengths import validate_readable_text

FORMAT = "relationship.slots.v1"
LONG_TERM_MAX_CHARS = 500
AFTEREFFECT_MAX_CHARS = 150
SLOT_LIMITS = {"long_term": LONG_TERM_MAX_CHARS, "aftereffect": AFTEREFFECT_MAX_CHARS}
SLOT_LABELS = {"long_term": "长期理解", "aftereffect": "近期余韵"}
SLOT_OPERATIONS = ("keep", "replace", "clear")


def is_slot_operation(body):
    return body.strip().casefold() in SLOT_OPERATIONS


def relationship_slots(state_text="", state=None):
    """Initialize a relationship or copy its two slots before editing."""
    if state is not None:
        return {"format": FORMAT, **{slot: dict(state[slot]) for slot in SLOT_LIMITS}}
    return {"format": FORMAT, "long_term": {"body": state_text or "", "observed_at": None},
            "aftereffect": {"body": "", "observed_at": None}}


def validate_relationship_slots(state):
    result = relationship_slots(state=state)
    for slot, limit in SLOT_LIMITS.items():
        result[slot]["body"] = validate_readable_text(result[slot]["body"].strip(), limit,
                                                    label=f"relationship {slot}")
        if is_slot_operation(result[slot]["body"]):
            raise ValueError("relationship slot body cannot be an operation")
        if not result[slot]["body"]:
            result[slot]["observed_at"] = None
    return result


def relationship_text(state):
    """Render both slots into searchable text."""
    return "\n\n".join(f"{SLOT_LABELS[slot]}\n{state[slot]['body']}" for slot in SLOT_LIMITS
                        if state[slot]["body"])


def transition_relationship(previous, operations, observed_at):
    """Only a replacement binds a slot to this batch's original observation time."""
    result = relationship_slots(state=previous)
    for slot, limit in SLOT_LIMITS.items():
        operation = operations[slot]
        if operation["op"] == "replace":
            body = validate_readable_text(operation["body"].strip(), limit, label=f"relationship {slot}")
            result[slot] = {"body": body, "observed_at": observed_at}
        elif operation["op"] == "clear":
            result[slot] = {"body": "", "observed_at": None}
    return result


def visible_relationship_slots(state, *, now, aftereffect_seconds):
    """Expiry affects only reading, never the stored state or its observation clocks."""
    visible = {slot: state[slot] for slot in SLOT_LIMITS if state[slot]["body"]}
    after = visible.get("aftereffect")
    if (after and aftereffect_seconds > 0 and after["observed_at"] is not None
            and now - after["observed_at"] >= aftereffect_seconds):
        del visible["aftereffect"]
    return visible
