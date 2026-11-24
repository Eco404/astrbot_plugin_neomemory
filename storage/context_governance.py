"""Manual changes to profile, relationship, and rolling summary content."""

import json

try:
    from ..core.memory_profiles import validate_profile_description, validate_profile_tags
    from ..core.relationship_state import SLOT_LIMITS, relationship_slots, relationship_text, validate_relationship_slots
    from ..core.text_lengths import validate_readable_text
except ImportError:
    from core.memory_profiles import validate_profile_description, validate_profile_tags
    from core.relationship_state import SLOT_LIMITS, relationship_slots, relationship_text, validate_relationship_slots
    from core.text_lengths import validate_readable_text

from .provenance_store import snapshot_context_version

_TABLES = {
    "profile": ("user_profiles", "profile_id"),
    "relationship": ("relationship_states", "relationship_id"),
    "short_term": ("short_term_states", "state_id"),
}
_PATCH_FIELDS = {
    "profile": {"description", "tags"},
    "relationship": set(SLOT_LIMITS),
    "short_term": {"summary_text"},
}


def validate_context_operation(operation):
    kind = operation.get("entity_kind")
    action = operation.get("action")
    if kind not in _TABLES or action not in {"edit", "delete"}:
        raise ValueError("unsupported context governance operation")
    required = {"entity_kind", "entity_id", "action", "expected_generation_id", "expected_version"}
    if action == "edit":
        required.add("patch")
    if set(operation) != required:
        raise ValueError("invalid context governance fields")
    for field in ("expected_generation_id", "entity_id"):
        value = operation[field]
        if not isinstance(value, str) or not value.strip() or len(value) > 200:
            raise ValueError(f"invalid {field}")
    if type(operation["expected_version"]) is not int or operation["expected_version"] < 1:
        raise ValueError("invalid expected context version")
    if action == "delete":
        return operation
    patch = operation["patch"]
    if not isinstance(patch, dict) or set(patch) != _PATCH_FIELDS[kind]:
        raise ValueError("invalid context edit fields")
    for field in _PATCH_FIELDS[kind] - {"tags"}:
        if not isinstance(patch[field], str):
            raise ValueError(f"invalid {field}")
    if kind == "profile":
        if not isinstance(patch["tags"], list) or not all(isinstance(tag, str) for tag in patch["tags"]):
            raise ValueError("invalid profile tags")
        patch["description"] = validate_profile_description(patch["description"])
        patch["tags"] = validate_profile_tags(patch["tags"])
    elif kind == "relationship":
        state = relationship_slots()
        for slot in SLOT_LIMITS:
            state[slot]["body"] = patch[slot]
        validated = validate_relationship_slots(state)
        for slot in SLOT_LIMITS:
            patch[slot] = validated[slot]["body"]
    else:
        patch["summary_text"] = validate_readable_text(patch["summary_text"].strip(), 4000, label="short-term summary")
    return operation


async def apply_context_change(connection, space_id, operation, now):
    kind = operation["entity_kind"]
    table, key = _TABLES[kind]
    entity_id = operation["entity_id"]
    cursor = await connection.execute(
        f"SELECT * FROM {table} WHERE {key}=? AND space_id=? AND generation_id=?",
        (entity_id, space_id, operation["expected_generation_id"]),
    )
    row = await cursor.fetchone()
    if row is None or row["content_version"] != operation["expected_version"]:
        raise ValueError("stale or foreign context entry")
    if kind == "short_term" and json.loads(row["coverage_json"]).get("summary_deleted"):
        raise ValueError("short-term summary was deleted")

    version = row["content_version"]
    if operation["action"] == "delete":
        if kind == "short_term":
            # Retain the rolling cursor so the next summary starts at new messages.
            await connection.execute(
                """UPDATE short_term_states SET summary_text='', status='active', content_version=content_version+1,
                coverage_json=json_set(coverage_json,'$.summary_deleted',1), updated_at=? WHERE state_id=?""",
                (now, entity_id),
            )
        else:
            await connection.execute(f"DELETE FROM {table} WHERE {key}=?", (entity_id,))
        return None

    patch = operation["patch"]
    if kind == "profile":
        if (patch["tags"], patch["description"]) == (json.loads(row["tags_json"]), row["description"]):
            return version
        fields = {"tags_json": json.dumps(patch["tags"], ensure_ascii=False), "description": patch["description"]}
    elif kind == "relationship":
        state = json.loads(row["state_json"])
        if all(state[slot]["body"] == patch[slot] for slot in SLOT_LIMITS):
            return version
        for slot in SLOT_LIMITS:
            if patch[slot] and not state[slot]["body"]:
                state[slot]["observed_at"] = now
            if not patch[slot]:
                state[slot]["observed_at"] = None
            state[slot]["body"] = patch[slot]
        fields = {"state_json": json.dumps(state, ensure_ascii=False), "state_text": relationship_text(state)}
    else:
        if patch["summary_text"] == row["summary_text"]:
            return version
        fields = {"summary_text": patch["summary_text"]}
    assignments = ", ".join(f"{column}=?" for column in fields)
    await connection.execute(
        f"UPDATE {table} SET {assignments}, content_version=?, updated_at=? WHERE {key}=?",
        (*fields.values(), version + 1, now, entity_id),
    )
    if kind != "short_term":
        await snapshot_context_version(connection, kind, entity_id)
    return version + 1
