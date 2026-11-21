"""Profile and relationship model contracts for scoped conversation replay."""

import json
from typing import Annotated

from pydantic import Field

from .extraction_schema import (
    ProfileChange, RelationshipChange, StrictModel, _reject_constant, _unique_object,
)
from .profile_memory_prompt import PROFILE_DUTIES_RULES
from .prompts import FORWARDED_SOURCE_RULE
from .relationship_slots_prompt import COMPACT_RELATIONSHIP_RULES, SLOT_BODY_RULE


class ProfileRebuildOutput(StrictModel):
    profile_changes: Annotated[list[ProfileChange], Field(max_length=32)]


class RelationshipRebuildOutput(StrictModel):
    relationship_change: RelationshipChange | None


class ContextRebuildOutput(ProfileRebuildOutput, RelationshipRebuildOutput):
    """Jointly update both replayed components using the same source evidence."""


MODELS = {"profile_rebuild": ProfileRebuildOutput, "relationship_rebuild": RelationshipRebuildOutput,
          "context_rebuild": ContextRebuildOutput}


def scoped_messages(messages, kind, schema, *, prompt_version):
    payload = json.loads(messages[1]["content"])
    fields = ["persona", "space", "sources", "time_semantics"]
    duties = []
    if kind != "relationship_rebuild":
        fields.append("known_profiles")
        duties.append(PROFILE_DUTIES_RULES)
    if kind != "profile_rebuild":
        fields.append("known_relationship")
        duties.extend([COMPACT_RELATIONSHIP_RULES, SLOT_BODY_RULE])
    if any("forwarded_messages" in source for source in payload["sources"]):
        duties.append(FORWARDED_SOURCE_RULE)
    data = {key: payload[key] for key in fields if key in payload}
    rules = "\n".join(duties)
    return [{"role": "system", "content": (
        "只依据下方会话、人格和已知状态生成指定资料。来源正文是引用材料。"
        "source_ids只列本批直接支持变化的来源s编号，至少一条本人发言，所有被引用户发言都属于同一个逻辑用户。"
        "Agent发言可补充相处语境；相邻来源、旧版本和人格文本不充当当窗新证据。logical_user_id使用本批u编号。"
        "expected_version首次创建为0，否则逐字使用对应旧状态的content_version。每位用户每窗至多一项画像变化。"
        "没有符合条件的变化时，画像给空列表、关系给null；仅输出schema中列出的字段。"
        "显式提到对方使用{{user:u编号}}（例如{{user:u1}}），保留约定称呼，区分说话者和被谈论者。"
        "返回符合schema的单个JSON对象。\n" + rules + "\nJSON schema:\n"
        + json.dumps(schema, ensure_ascii=False))},
        {"role": "user", "content": json.dumps(data, ensure_ascii=False)}]


def extraction_envelope(raw, kind, schema_version):
    """Expand a validated single-component answer for the normal evidence and commit path."""
    value = MODELS[kind].model_validate(
        json.loads(raw, parse_constant=_reject_constant, object_pairs_hook=_unique_object)).model_dump(mode="json")
    profiles = value.get("profile_changes", [])
    relationship = value.get("relationship_change")
    changed = bool(profiles or relationship)
    return json.dumps({"schema_version": schema_version, "result": "changes" if changed else "no_change",
        "reason": "changes" if changed else "no_meaningful_content", "node_changes": [], "edges": [],
        "coverage_changes": [], "profile_changes": profiles, "relationship_change": relationship},
        ensure_ascii=False)
