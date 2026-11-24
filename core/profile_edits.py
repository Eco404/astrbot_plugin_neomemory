"""Apply model-selected edits to the frozen profile of one logical user."""
from __future__ import annotations

import re
from copy import deepcopy

from .candidate_feedback import CandidateValidationError
from .profile_memory_prompt import GROUP_PROFILE_MAX_DESCRIPTION_CHARS, GROUP_PROFILE_MAX_TAGS
from .text_lengths import readable_text_length

# Consume references as a unit. A period within a decimal or domain is part of the text.
_SENTENCE_END = re.compile(
    r"""(?P<reference>\{\{user:[^{}\s]+\}\})|(?:[。！？!?；;…]|\.(?![A-Za-z0-9_]))+["’”'」』）)\]]*\s*|[\r\n]+[ \t]*"""
)


def profile_parts(text: str) -> list[str]:
    """Split a frozen description into addressable sentences with their original separators."""
    parts = []
    start = 0
    for match in _SENTENCE_END.finditer(text):
        if match.lastgroup != "reference" and text[start:match.end()].strip():
            parts.append(text[start:match.end()])
            start = match.end()
    if start < len(text):
        parts.append(text[start:])
    return parts


def model_profile_schema(schema: dict) -> dict:
    result = deepcopy(schema)
    profile = result.get("$defs", {}).get("ProfileChange")
    if profile is not None:
        for name in ("remove_sentences", "remove_tags"):
            profile["properties"][name] = {"type": "array", "items": {"type": "integer", "minimum": 1}}
            if name not in profile["required"]:
                profile["required"].append(name)
    return result


def _edit_error(code, location, message, **constraints):
    exc = CandidateValidationError(code, location, message)
    exc.constraints = constraints
    return exc


def _indexes(change, field, count, location):
    numbers = change.get(field, [])
    if not isinstance(numbers, list):
        raise CandidateValidationError("list_type", (*location, field), "use an array of one-based indexes")
    selected = set()
    for index, number in enumerate(numbers):
        if type(number) is not int or not 1 <= number <= count or number in selected:
            constraints = {"minimum": 1, "maximum": count} if count else {"maxItems": 0}
            raise _edit_error("profile_edit_index", (*location, field, index),
                "select distinct existing indexes from the same user's frozen profile", **constraints)
        selected.add(number)
    return selected


def _splice(parts, selected, additions):
    """Insert at the first removed item, or append to all retained items."""
    first = min(selected, default=len(parts) + 1) - 1
    return parts[:first] + additions + [
        part for index, part in enumerate(parts[first:], first + 1) if index not in selected
    ]


def _merge_description(parts, selected, additions):
    existing = {part.strip() for index, part in enumerate(parts, 1) if index not in selected}
    novel = []
    for part in profile_parts(additions):
        key = part.strip()
        if key and key not in existing:
            novel.append(part)
            existing.add(key)
    if not novel:
        return "".join(_splice(parts, selected, []))

    # Keep the boundary of the replaced sentence, including paragraph breaks. An
    # unterminated old sentence needs a separator when adding a new full thought.
    first = min(selected, default=len(parts) + 1) - 1
    inserted = "".join(novel)
    if first and not parts[first - 1][-1].isspace() and not inserted[0].isspace():
        previous = parts[first - 1][-1]
        if previous not in "。！？；…」』”）":
            inserted = (" " if previous in ".!?;" else "\n") + inserted
    if selected and any(index not in selected for index in range(first + 2, len(parts) + 1)):
        replaced = parts[first]
        separator = replaced[len(replaced.rstrip()):]
        if not inserted[-1].isspace():
            inserted += separator or ("" if inserted[-1] in "。！？；…」』”）" else "\n")
    return "".join(_splice(parts, selected, [inserted]))


def apply_profile_edits(changes, known_profiles, *, group=False):
    """Resolve edit indexes against the same user and wire IDs as the model input."""
    if not isinstance(changes, list):
        return
    profiles = {profile["logical_user_id"]: profile for profile in known_profiles}
    tag_limit = GROUP_PROFILE_MAX_TAGS if group else 12
    char_limit = GROUP_PROFILE_MAX_DESCRIPTION_CHARS if group else 600
    merged = []
    for index, change in enumerate(changes):
        if not isinstance(change, dict) or not isinstance(change.get("logical_user_id"), str):
            continue
        location = ("profile_changes", index)
        profile = profiles.get(change["logical_user_id"], {})
        parts = profile.get("description_sentences")
        if parts is None:
            parts = profile_parts(profile.get("description", ""))
        tags = profile.get("tags", [])
        remove_tags = _indexes(change, "remove_tags", len(tags), location)
        remove_parts = _indexes(change, "remove_sentences", len(parts), location)
        fields = {}
        added = change.get("tags")
        if len(tags) > len(remove_tags) and isinstance(added, list) and all(isinstance(tag, str) for tag in added):
            if len(set(added)) != len(added):
                raise CandidateValidationError("duplicate_values", (*location, "tags"), "tags must be unique")
            kept = {tag for number, tag in enumerate(tags, 1) if number not in remove_tags}
            fields["tags"] = _splice(tags, remove_tags, [tag for tag in added if tag not in kept])
            if len(fields["tags"]) > tag_limit:
                raise _edit_error("profile_edit_capacity", (*location, "tags"),
                    "merged profile exceeds the tag limit", maxItems=tag_limit, actualItems=len(fields["tags"]))
        if len(parts) > len(remove_parts) and isinstance(change.get("description"), str):
            description = _merge_description(parts, remove_parts, change["description"])
            try:
                length = readable_text_length(description)
            except ValueError as exc:
                raise CandidateValidationError("invalid_user_reference", (*location, "description"), str(exc)) from exc
            if length > char_limit:
                raise _edit_error("profile_edit_capacity", (*location, "description"),
                    "merged profile exceeds the readable length limit",
                    **{"x-readable-maxLength": char_limit, "actualReadableChars": length})
            fields["description"] = description
        merged.append((change, fields))
    for change, fields in merged:
        change.update(fields)
        change.pop("remove_sentences", None)
        change.pop("remove_tags", None)
