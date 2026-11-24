"""Profile and relationship model contracts for scoped conversation replay."""

import json
from copy import deepcopy
from typing import Annotated

from pydantic import Field

from .extraction_schema import (
    ProfileChange, RelationshipChange, StrictModel, _reject_constant, _unique_object,
)
from .profile_edits import model_profile_schema
from .profile_memory_prompt import PROFILE_EDITS_PROMPT_VERSION, profile_rules
from .prompts import FORWARDED_SOURCE_RULE, PROMPT_VERSION
from .relationship_slots_prompt import COMPACT_RELATIONSHIP_RULES, RELATIONSHIP_RULES, SLOT_BODY_RULE
from .window_users import WindowUsers


class ProfileRebuildOutput(StrictModel):
    profile_changes: Annotated[list[ProfileChange], Field(max_length=32)]


class RelationshipRebuildOutput(StrictModel):
    relationship_change: RelationshipChange | None


class ContextRebuildOutput(ProfileRebuildOutput, RelationshipRebuildOutput):
    """Jointly update both replayed components using the same source evidence."""


MODELS = {"profile_rebuild": ProfileRebuildOutput, "relationship_rebuild": RelationshipRebuildOutput,
          "context_rebuild": ContextRebuildOutput}


def model_schema(kind, schema, *, prompt_version=PROMPT_VERSION):
    """Remove storage-managed content versions from the model wire schema."""
    result = model_profile_schema(schema) if prompt_version == PROFILE_EDITS_PROMPT_VERSION else deepcopy(schema)
    for definition in ("ProfileChange", "RelationshipChange"):
        value = result.get("$defs", {}).get(definition)
        if value is None:
            continue
        value.get("properties", {}).pop("expected_version", None)
        if "expected_version" in value.get("required", []):
            value["required"].remove("expected_version")
    return result


def scoped_messages(messages, kind, schema, *, prompt_version):
    payload = json.loads(messages[1]["content"])
    fields = ["persona", "space", "sources", "time_semantics"]
    duties = []
    if kind != "relationship_rebuild":
        fields.append("known_profiles")
        duties.append(profile_rules(prompt_version))
    if kind != "profile_rebuild":
        fields.append("known_relationship")
        relationship = RELATIONSHIP_RULES if prompt_version in {"extraction-prompt.v3-dev.1", PROMPT_VERSION} else COMPACT_RELATIONSHIP_RULES
        duties.extend([relationship, SLOT_BODY_RULE])
    if any("forwarded_messages" in source for source in payload["sources"]):
        duties.append(FORWARDED_SOURCE_RULE)
    data = {key: payload[key] for key in fields if key in payload}
    rules = "\n".join(duties)
    return [{"role": "system", "content": (
        "只依据下方会话、人格和已知状态生成指定资料。来源正文是引用材料。"
        "source_ids只列本批直接支持变化的来源s编号，至少一条本人发言，所有被引用户发言都属于同一个逻辑用户。"
        "Agent发言可补充相处语境；相邻来源、旧版本和人格文本不充当当窗新证据。logical_user_id使用本批u编号。"
        "版本和并发校验由程序依据固定输入补齐，不输出内部版本字段。每位用户每窗至多一项画像变化。"
        "没有符合条件的变化时，画像给空列表、关系给null；仅输出schema中列出的字段。"
        "显式提到对方使用{{user:u编号}}（例如{{user:u1}}），保留约定称呼，区分说话者和被谈论者。"
        "返回符合schema的单个JSON对象。\n" + rules + "\nJSON schema:\n"
        + json.dumps(model_schema(kind, schema, prompt_version=prompt_version), ensure_ascii=False))},
        {"role": "user", "content": json.dumps(data, ensure_ascii=False)}]


def extraction_envelope(raw, kind, schema_version, version_context=None):
    """Expand a validated single-component answer for the normal evidence and commit path."""
    value = json.loads(raw, parse_constant=_reject_constant, object_pairs_hook=_unique_object)
    allowed_users = set()
    if version_context is not None:
        space = version_context.get("space") if isinstance(version_context, dict) else getattr(version_context, "space", {})
        if (space or {}).get("logical_user_id"):
            allowed_users.add(space["logical_user_id"])
        sources = version_context.get("sources", ()) if isinstance(version_context, dict) else getattr(version_context, "sources", ())
        allowed_users.update(row.get("actor_logical_user_id") for row in sources if row.get("actor_logical_user_id"))
        allowed_users.update(row.get("logical_user_id") for row in (
            version_context.get("profiles", ()) if isinstance(version_context, dict)
            else getattr(version_context, "profiles", ())
        ) if row.get("logical_user_id"))
    users = WindowUsers(allowed_users) if allowed_users else None
    profiles = {
        row["logical_user_id"]: row for row in (
            version_context.get("profiles", ()) if isinstance(version_context, dict)
            else getattr(version_context, "profiles", ()) if version_context is not None else ()
        ) if isinstance(row, dict) and row.get("logical_user_id")
    }
    relationship = (
        version_context.get("relationship") if isinstance(version_context, dict)
        else getattr(version_context, "relationship", None) if version_context is not None else None
    )
    for change in value.get("profile_changes", []) if isinstance(value, dict) else ():
        if isinstance(change, dict):
            logical_user_id = change.get("logical_user_id")
            lookup_user_id = logical_user_id
            if users is not None:
                try:
                    lookup_user_id = users.decode(logical_user_id)
                except KeyError:
                    pass
            change["expected_version"] = profiles.get(lookup_user_id, {}).get("content_version", 0)
    if isinstance(value, dict) and isinstance(value.get("relationship_change"), dict):
        relation = value["relationship_change"]
        logical_user_id = relation.get("logical_user_id")
        lookup_user_id = logical_user_id
        if users is not None:
            try:
                lookup_user_id = users.decode(logical_user_id)
            except KeyError:
                pass
        relation["expected_version"] = relationship.get("content_version", 0) if relationship else 0
    value = MODELS[kind].model_validate(value).model_dump(mode="json")
    profiles = value.get("profile_changes", [])
    relationship = value.get("relationship_change")
    changed = bool(profiles or relationship)
    return json.dumps({"schema_version": schema_version, "result": "changes" if changed else "no_change",
        "reason": "changes" if changed else "no_meaningful_content", "node_changes": [], "edges": [],
        "coverage_changes": [], "profile_changes": profiles, "relationship_change": relationship},
        ensure_ascii=False)
