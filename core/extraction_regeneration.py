"""Bounded candidate regeneration using fixed validation feedback."""

from __future__ import annotations

import json
import re

from .extraction_schema import SCHEMA_VERSION
from .profile_memory_prompt import GROUP_PROFILE_MAX_TAGS, GROUP_PROFILE_MAX_DESCRIPTION_CHARS, PROFILE_EDITS_PROMPT_VERSION
from .profile_edits import apply_profile_edits
from .candidate_feedback import FIELD_RULES as _FIELD_RULES, field_feedback

VERSION = "candidate-regeneration.v2"
_FIELDS = frozenset({
    "schema_version", "result", "reason", "node_changes", "local_ref", "node_type", "operation",
    "node_id", "base_node_ids", "expected_version", "content", "title", "importance", "source_ids", "event_time", "start", "end", "text",
    "edges", "from_ref", "to_ref", "relation_type", "coverage_changes", "event_ref", "atomic_ref",
    "expected_event_version", "expected_atomic_version", "coverage_kind", "profile_changes", "logical_user_id",
    "tags", "description", "remove_sentences", "remove_tags", "relationship_change", "state_text", "emotion_tags", "emotion_intensity", "emotion", "intensity", "tag", "long_term", "aftereffect", "op", "body",
})
_EXPLANATIONS = {
    "schema_mismatch": "按本次输入提供的schema返回业务变化对象，关系使用独立long_term/aftereffect操作，节点情绪每个标签独立强度；不得退回单段或共享强度协议。",
    "relationship_slot_body": "关系各段op=replace时body填写该段完整的内心想法，不能只填keep、replace或clear（含大小写变化）；keep或clear时body必须为空字符串。只修正对应字段，保持来源和原有语义。",
    "create_identity_invalid": "新建节点 operation=create 时 node_id 必须为 null；若实际补充输入中的旧节点，应选择 update 并逐字使用该节点ID，不能仅为通过检查改成新建。",
    "update_identity_required": "更新节点 operation=update 时，node_id 必须来自输入中同一个已有节点；不能用用户ID、画像或人格关系版本代替。若确为新经历则 create，不为通过检查重复新建旧事。",
    "unknown_local_reference": "关系边和覆盖中的 local_* 端点必须在本次 node_changes 中实际声明；已有节点使用输入提供的节点引用。只保留来源支持的关联，不为补端点虚构节点。",
    "unknown_event_time": "事件发生时间未知时，event_time 必须为 JSON null；不得使用 start、end、text 全为 null 的对象，也不得补造时间。",
    "event_time_interval": "事件时间的 end 不得早于 start；仅使用来源支持的时间信息。",
    "unknown_reference": "只能逐字使用当前输入提供的对应种类短引用，或本次候选实际声明的 local_* 引用；不能猜测、修补或引入其他引用。",
    "source_outside_window": "source_ids 只能引用当前固定窗口提供的来源。",
    "title_invalid": "title 必须是1～60个可读字符的完整简短标题；一个完整合法的用户占位符按一个字符计，不能按稳定ID的编码长度判断可读长度。只用输入已有身份，不拆断占位符，不增加正文没有的事实。",
    "event_identity_mismatch": "同一事件的有依据补充才允许 update；另一独立经历用 create，身份不确定可不作变化，不能只改标签来通过检查。",
    "profile_edit_index": "remove_sentences 与 remove_tags 分别使用该逻辑用户在本窗 description_sentences、tags 中的序号，均从 1 开始且不可重复。没有旧项或只新增时填空数组。",
    "profile_edit_capacity": "上限按保留的旧资料与本次新增合并后计算。减少本次低价值细节，按序号合并同一事项或同义标签，保留其他仍有效的资料及其条件。",
    "profile_evidence_insufficient": "本批画像新增或修正必须来自本人明确自述；观察行为和人格推断不能当作稳定画像。",
    "profile_repeated_evidence_missing": "repeated_self_statement 至少需要本批两条本人直接自述；只有一条明确自述时使用 explicit_self_statement。",
    "interaction_confirmation_required": "accepted/completed 需要明确用户确认的直接来源子集；其他状态和纯主观感受的确认列表应为空。",
    "interaction_confirmation_invalid": "确认来源必须是直接来源中的当前逻辑用户发言；上下文、Agent 提议和其他用户发言不能替代确认。",
    "readonly_node": "该字段引用的节点在固定窗口中只读。保留只读节点的正文、状态、关系和覆盖，只提交窗口内其他有依据的变化；没有变化时返回 no_change。",
    "node_outside_context": "只能更新或连接当前输入明确提供的已有节点，或本次候选实际声明的 local_* 节点。",
    "node_type_mismatch": "更新已有节点时 node_type 必须等于该节点在当前输入中的 node_type。",
    "expected_node_version_mismatch": "更新已有节点时只引用输入中确定的同一node_id；内容版本和并发校验由程序依据固定输入自动计算。",
    "expected_profile_version_mismatch": "画像只引用输入中确定的逻辑用户；内容版本和并发校验由程序自动计算。",
    "expected_relationship_version_mismatch": "关系只引用输入中确定的逻辑用户；内容版本和并发校验由程序自动计算。",
    "coverage_event_version_mismatch": "覆盖只声明输入中确定的event_ref和atomic_ref；最终内容版本由程序依据候选内容自动计算。",
    "coverage_atomic_version_mismatch": "覆盖只声明输入中确定的event_ref和atomic_ref；最终内容版本由程序依据候选内容自动计算。",
    "evidence_user_mismatch": "画像或关系来源必须包含被描述用户本人的发言；Agent 的猜测不能作为该用户陈述的替代证据。",
    "profile_user_mismatch": "私聊画像只能属于当前空间提供的逻辑用户。",
    "relationship_user_mismatch": "关系变化只能属于当前私聊空间提供的逻辑用户。",
    "group_profile_limit": "群成员画像最多 8 个标签，描述最多 240 字符。",
    "coverage_endpoint_type_mismatch": "覆盖只能从事件节点连接到原子节点。",
    "duplicate_coverage_pair": "同一候选中每个事件与原子组合只能声明一次覆盖。",
    "edge_self_reference": "关系边的两个端点必须解析为不同节点。",
    "invalid_contains_hierarchy": "contains 只允许事件包含原子、主题包含事件或主题包含原子。",
    "ordinary_topic_create_members": "新建topic必须通过contains关联至少2个不同event/atomic节点；同一节点的local_ref与已有node_id只计一次。数量只是结构下限，仍须存在独立成员的持续关联；同一事件与其细节不能仅因ID不同证明独立。依据固定输入补充真实关联，缺乏依据则不创建主题，不拆分或虚构节点凑数。",
    "supersedes_node_type_mismatch": "supersedes 两端必须为相同节点类型。",
    "supersedes_cycle": "supersedes 关系不得构成循环。",
    "missing": "补齐 schema 要求的字段；无值时只能在 schema 允许的位置使用 null。",
    "extra_forbidden": "移除 schema 未定义的字段。",
    "json_invalid": "返回一个完整有效的 JSON 对象，不使用代码围栏、重复键、非有限数值或额外说明。",
    "string_too_long": "字段文本过长；长度按还原稳定用户引用后的完整表述检查。请缩短完整表述，保留主体、否定和条件，保持引用归属；不要截断片段或返回补丁。",
    "schema_invalid": "按当前 schema 检查字段类型、允许值、长度、唯一性与跨字段约束，返回完整候选。",
    "candidate_rejected": "上一份候选未通过校验。依据当前固定输入和 schema 重新生成，不补造事实、引用或版本。",
    "invalid_user_reference": "使用输入中合法完整的 {{user:ID}} 引用；不要拆开括号、加入空白或编造身份。",
    "readable_too_long": "文本超过可读字符上限，完整合法用户引用计一个字符。保留主体、条件和否定，返回完整表述。",
    "duplicate_values": "每个来源、标签、局部引用、更新节点及画像用户列表都必须唯一。只合并同一对象的重复声明，不合并不同经历，不丢失必要变化。",
    "duplicate_edges": "相同起点、终点和关系类型只能声明一条边，保留真正支持该关系的来源。",
    "coverage_self_reference": "覆盖的事件与原子端点必须不同，且类型分别为event与atomic。",
    "neutral_emotion_conflict": "neutral只能单独使用；其他情绪可组合，不为通过检查改变实际感受归属。",
    "result_changes_mismatch": "result=no_change时所有变化集合为空、relationship_change=null且reason不是changes；result=changes时至少有一项实际变化且reason=changes。按来源保留必要变化，不伪造无变化。",
    "event_metadata_mismatch": "event节点需要event_status及event_identity；其他节点这两个字段为null。按来源判断身份和状态，不为了通过检查改换经历。",
    "source_sets_overlap": "直接来源source_ids与背景context_source_ids不得重叠；依据来源实际职责选择，不删除必要证据。",
    "node_content_too_long": "正文遵守当前schema的节点类型长度限制，保留主体、条件和否定，不能直接截断。",
}


_NAMED_SCHEMA_ERRORS = frozenset({
    "duplicate_values", "duplicate_edges", "coverage_self_reference", "neutral_emotion_conflict",
    "result_changes_mismatch", "event_metadata_mismatch", "source_sets_overlap", "node_content_too_long",
    "duplicate_coverage_pair", "relationship_slot_body",
})

_JSON_SYNTAX_HINTS = {
    "Expecting ',' delimiter": "检查错误位置前的逗号和括号：对象以 } 闭合，数组以 ] 闭合，不能相互替代。",
    "Expecting ':' delimiter": "对象的字段名与值之间必须有冒号。",
    "Expecting property name enclosed in double quotes": "对象字段名使用双引号，末项后不要保留逗号。",
    "Expecting value": "错误位置缺少合法 JSON 值，检查空值、末尾逗号及未完成的输出。",
    "Unterminated string starting at": "字符串缺少结束双引号，检查字符串闭合与转义。",
    "Extra data": "完整 JSON 对象之外存在额外内容，只返回一个对象。",
}


class RegenerationContextError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _path(location):
    fields = _FIELDS
    if isinstance(location, str):
        if not re.fullmatch(r"\$(?:\.[a-z_]+|\[[0-9]+\])*", location):
            return "$"
        if any(field not in fields for field in re.findall(r"\.([a-z_]+)", location)):
            return "$"
        return location
    result = "$"
    for component in location:
        if type(component) is int and component >= 0:
            result += f"[{component}]"
        elif component in fields:
            result += "." + component
        else:
            return result
    return result


def validation_feedback(archived, *, version=VERSION, schema_version=SCHEMA_VERSION, context=None):
    """Re-validate the previous raw draft; never forward exception text or input."""
    from .vision_request import split_vision_output, VisionOutputError
    from .window_images import collects_evidence

    window = archived["window"]
    slots = window.get("image_slots", ())
    if version not in {"candidate-regeneration.v1", VERSION}:
        raise RegenerationContextError("regeneration_version_unsupported")
    if not collects_evidence(slots):
        return _extraction_feedback(archived, version, schema_version, context)

    try:
        extraction_raw, _ = split_vision_output(window["raw"], slots)
    except VisionOutputError as exc:
        return {
            "protocol": version, "status": "previous_candidate_rejected",
            "errors": [{"path": exc.path, "type": exc.code, "instruction": exc.instruction}],
            "instruction": "依据原始固定输入返回完整 extraction 与 image_evidence 对象。",
        }
    except (ValueError, RecursionError):
        # Syntax feedback must use positions in the original, unmodified response.
        return _extraction_feedback(archived, version, schema_version, context)

    inner = dict(archived, window=dict(window, raw=extraction_raw))
    feedback = _extraction_feedback(inner, version, schema_version, context)
    for error in feedback["errors"]:
        error["path"] = "$.extraction" + error["path"][1:]
    return feedback


def _extraction_feedback(archived, version, schema_version, context):
    from pydantic import ValidationError

    from .extraction_schema import _decode_output, extraction_model_json_schema, parse_extraction_output_for_context
    from .extraction_errors import DuplicateJSONKey, ExtractionCapacityError
    from .candidate_feedback import CandidateValidationError

    raw = archived["window"]["raw"]
    errors = []
    try:
        decoded = json.loads(raw)
    except (ValueError, RecursionError):
        decoded = None
    target_schema = schema_version
    special = {}
    if isinstance(decoded, dict) and isinstance(decoded.get("node_changes"), list):
        for index, node in enumerate(decoded["node_changes"]):
            event_time = node.get("event_time") if isinstance(node, dict) else None
            if not isinstance(event_time, dict):
                continue
            path = f"$.node_changes[{index}].event_time"
            if set(event_time) == {"start", "end", "text"} and all(value is None for value in event_time.values()):
                special[path] = "unknown_event_time"
            elif (type(event_time.get("start")) in (int, float) and type(event_time.get("end")) in (int, float)
                  and event_time["end"] < event_time["start"]):
                special[path] = "event_time_interval"
    scoped_kind = archived["window"].get("context_rebuild_kind")
    prompt_version = archived["window"].get("prompt_version")
    try:
        validation_raw = raw
        if prompt_version == PROFILE_EDITS_PROMPT_VERSION:
            value = _decode_output(raw)
            known = context or {}
            apply_profile_edits(value.get("profile_changes", []), known.get("known_profiles", ()),
                                group=(known.get("space") or {}).get("space_type") == "group")
            validation_raw = json.dumps(value, ensure_ascii=False)
        if scoped_kind:
            from .context_rebuild import extraction_envelope
            envelope = extraction_envelope(validation_raw, scoped_kind, target_schema)
            parse_extraction_output_for_context(envelope, {
                "known_nodes": (context or {}).get("known_nodes", ()),
                "profiles": (context or {}).get("known_profiles", ()),
                "relationship": (context or {}).get("known_relationship"),
            }, schema_version=target_schema, validate_supplied_versions=False)
        else:
            parse_extraction_output_for_context(validation_raw, {
                "known_nodes": (context or {}).get("known_nodes", ()),
                "profiles": (context or {}).get("known_profiles", ()),
                "relationship": (context or {}).get("known_relationship"),
            }, schema_version=target_schema, validate_supplied_versions=False)
    except CandidateValidationError as exc:
        path = _path(exc.location)
        error = {"path": path, "type": exc.code,
                 "instruction": _EXPLANATIONS.get(exc.code, "按当前固定输入重新生成完整候选。")}
        if constraints := getattr(exc, "constraints", None):
            error["constraints"] = constraints
        errors.append(error)
    except ValidationError as exc:
        issues = exc.errors(include_input=False, include_context=False, include_url=False)
        # Validation diagnostics may describe a legacy candidate that still
        # contains storage-managed fields. The active request schema omits
        # those fields, while the feedback schema must still locate their
        # concrete failure when a provider returns them.
        schema = extraction_model_json_schema(
            target_schema or (decoded.get("schema_version") if isinstance(decoded, dict) else None)
            or SCHEMA_VERSION, prompt_version=prompt_version
        )
        if scoped_kind:
            from .context_rebuild import MODELS, model_schema
            schema = model_schema(scoped_kind, MODELS[scoped_kind].model_json_schema(), prompt_version=prompt_version)
        for item in issues[:12]:
            path = _path(item["loc"])
            if path not in special:
                precise = field_feedback(item, schema, path, decoded, standards=True)
                if precise:
                    errors.append(precise)
                    continue
            kind = item["type"]
            code = special.get(path, kind if kind in _EXPLANATIONS else "schema_invalid")
            if path.endswith(".title") and item["type"] != "missing":
                code = "title_invalid"
            errors.append({"path": path, "type": code, "instruction": _EXPLANATIONS[code]})
    except ExtractionCapacityError as exc:
        errors.append(_capacity_feedback("raw", exc.actual_chars, exc.limit_chars))
    except json.JSONDecodeError as exc:
        instruction = _EXPLANATIONS["json_invalid"]
        error = {"path": "$", "type": "json_invalid", "instruction": instruction}
        if hint := _JSON_SYNTAX_HINTS.get(exc.msg):
            instruction += hint
        instruction += "lineno/colno 从 1 开始，pos 从 0 开始，位置按字符计数；根据解析位置检查并修正完整候选。"
        error.update(lineno=exc.lineno, colno=exc.colno, pos=exc.pos, instruction=instruction)
        errors.append(error)
    except DuplicateJSONKey as exc:
        field = exc.key if exc.key in _FIELDS else None
        error = {"path": "$", "type": "duplicate_json_key",
            "instruction": "同一JSON对象中字段名只能出现一次。删除重复字段声明，返回完整候选；不能合并或覆盖不同值来猜测含义。"}
        if field:
            error["field"] = field
        errors.append(error)
    except ValueError:
        errors.append({"path": "$", "type": "json_invalid", "instruction": _EXPLANATIONS["json_invalid"]})
    else:
        # Valid short references may still expand beyond a canonical field limit.
        for item in archived["window"].get("validation_errors", []):
            if item.get("type") == "output_capacity_exceeded":
                canonical = archived["window"].get("canonical_raw")
                if isinstance(canonical, str):
                    from .extraction_schema import MAX_OUTPUT_CHARS
                    errors.append(_capacity_feedback("canonical", len(canonical), MAX_OUTPUT_CHARS))
                    continue
            if item.get("type") in _FIELD_RULES:
                from .candidate_feedback import _location
                canonical = json.loads(archived["window"].get("canonical_raw", raw))
                location = item.get("path", "$")
                location = _location(location) if isinstance(location, str) else location
                precise = field_feedback({"loc": location, "type": item["type"]},
                        extraction_model_json_schema(target_schema), _path(location), canonical, standards=True)
                if precise is not None:
                    precise["stage"] = "canonical"
                    if item["type"] == "string_too_long":
                        precise["instruction"] = "按稳定用户引用还原后的文本计算编码字符，缩短至constraints.maxLength以内；保留主体、否定、条件，不截断引用。"
                    errors.append(precise)
                    continue
            code = item.get("type") if item.get("type") in _EXPLANATIONS else "candidate_rejected"
            path = _path(item.get("path", "$"))
            if path.endswith(".title") and code in {"schema_invalid", "string_too_long"}:
                code = "title_invalid"
            if code == "group_profile_limit":
                canonical = archived["window"].get("canonical_raw", raw)
                errors.append(_group_profile_feedback(path, json.loads(canonical), target_schema))
                continue
            errors.append({"path": path, "type": code, "instruction": _EXPLANATIONS[code]})
    for error in errors:
        if error["type"] == "schema_mismatch":
            error["instruction"] = "按本次原始系统提示词中的JSON schema完整返回业务变化对象；不能依据被拒草稿猜测目标协议，也不能删去必需的业务字段来规避校验。"
        if error["type"] in {"unknown_reference", "node_outside_context"}:
            error["instruction"] = _reference_instruction(error["path"])
        if error["type"] == "unknown_reference":
            error["instruction"] += "s*是来源，u*是逻辑用户，n*是已有节点，各类不能混用。画像与人格关系通过各自变化字段输出，不是可供node_changes更新的节点。"
        if error["type"] == "evidence_user_mismatch":
            error["instruction"] = "画像/人格关系来源须至少包含目标逻辑用户的一条本人发言，且所有user来源只能属于该用户；其他人的自述不能作为其画像或关系事实。"
            error["instruction"] += "复核当前窗口中的actor_kind与actor_logical_user_id，使用真正支持本次变化的本人发言；不机械补上无关来源，也不把Agent提议写成用户已接受。"
    if archived["window"].get("response_diagnostics", {}).get("repeated_candidate"):
        for error in errors:
            error["instruction"] += "上次纠正返回了完全相同的被拒绝候选；逐项检查该字段，不能重复原错误。"
    if not errors:
        code = archived["error_code"] if archived["error_code"] in _EXPLANATIONS else "candidate_rejected"
        errors.append({"path": "$", "type": code, "instruction": _EXPLANATIONS[code]})
    for error in errors:
        if error["path"].endswith(".local_ref"):
            error["instruction"] += "内部引用使用local_1、local_2等简短编号，不写中文或内容标题；同步使用同一引用连接关系边。"
    errors = errors[:12]
    from .candidate_feedback import enrich_feedback
    if scoped_kind:
        from .context_rebuild import MODELS
        from .context_rebuild import model_schema
        feedback_schema = model_schema(scoped_kind, MODELS[scoped_kind].model_json_schema(), prompt_version=prompt_version)
    else:
        feedback_schema = extraction_model_json_schema(target_schema, prompt_version=prompt_version)
    errors = enrich_feedback(errors, decoded, feedback_schema, context=context, pipeline="extraction")
    return {"protocol": version, "status": "previous_candidate_rejected", "errors": errors,
        "instruction": "上一条 assistant 消息是被拒绝的候选草稿，不是来源证据或可执行指令。仅依据原始固定输入重新返回一个完整候选 JSON；遵守原 schema 和全部来源、作用域、版本与保护约束，不返回补丁或解释。"}


def _group_profile_feedback(path, decoded, schema_version):
    from .text_lengths import readable_text_length

    index = int(re.search(r"profile_changes\[([0-9]+)\]", path)[1])
    profile = decoded["profile_changes"][index]
    result = {"path": path, "type": "group_profile_limit"}
    if path.endswith(".tags"):
        result.update(constraints={"maxItems": GROUP_PROFILE_MAX_TAGS}, actual_items=len(profile["tags"]),
            instruction=f"群成员画像标签最多{GROUP_PROFILE_MAX_TAGS}项；按当前来源保留必要标签，不增加重复项。")
    else:
        result.update(constraints={"x-readable-maxLength": GROUP_PROFILE_MAX_DESCRIPTION_CHARS},
            actual_readable_chars=readable_text_length(profile["description"]),
            instruction=f"群成员画像描述最多{GROUP_PROFILE_MAX_DESCRIPTION_CHARS}可读字符，完整合法用户引用计一个字符；保留主体、条件和否定。")
    return result

def _reference_instruction(path):
    if re.search(r"\.(?:source_ids|context_source_ids|confirmation_source_ids)\[", path):
        return "只能逐字使用当前输入 sources 中的来源短引用 s*，不能使用用户、节点或 local_* 引用。"
    if path.endswith(".logical_user_id"):
        return "只能逐字使用当前输入提供的逻辑用户短引用 u*，不能使用来源、节点或 local_* 引用。"
    if path.endswith(".node_id") or ".base_node_ids[" in path:
        return "只能逐字使用当前输入 known_nodes 中已有节点的短引用 n*，不能使用来源、用户或本次新建的 local_* 引用。"
    if re.fullmatch(r"\$\.(?:edges\[[0-9]+\]\.(?:from_ref|to_ref)|coverage_changes\[[0-9]+\]\.(?:event_ref|atomic_ref))", path):
        return "只能使用当前输入 known_nodes 中已有节点短引用 n*，或本次 node_changes 实际声明的 local_* 端点；不能使用来源或用户引用。"
    return "引用必须属于当前字段规定的种类与当前输入范围；local_* 只允许在关系边和覆盖端点引用本次实际声明的节点，不能用于来源、用户、更新身份或 base_node_ids。"

def _capacity_feedback(stage, actual_chars, limit_chars):
    return {"path": "$", "type": "output_capacity_exceeded", "stage": stage,
        "actual_chars": actual_chars, "limit_chars": limit_chars,
        "instruction": "整份候选超过独立编码容量，不是JSON语法、来源或事件同一性错误。canonical表示稳定引用展开后的大小。减少冗余叙述和不必要的重复身份引用，主体必须明确，保留事实、否定、条件和完整引用；不能删除必要变化、伪造no_change或改用旧schema来规避容量，无法完整表达时留待人工处理。"}


def build_messages(base_messages, window, archived, *, schema_version=None):
    """Append one rejected draft and one feedback message to the base request."""
    regeneration = window.get("regeneration", {})
    version = regeneration.get("version")
    if version not in {"candidate-regeneration.v1", VERSION}:
        raise RegenerationContextError("regeneration_version_unsupported")
    if archived is None or archived["candidate_id"] != regeneration.get("candidate_id"):
        raise RegenerationContextError("regeneration_candidate_unavailable")
    if schema_version is None:
        raise RegenerationContextError("regeneration_schema_unavailable")
    previous = archived["window"]
    fields = ("input_digest", "signature", "source_ids", "source_cursor", "known_node_ids", "reference_protocol")
    if any(field not in window or previous.get(field) != window[field] for field in fields):
        raise RegenerationContextError("regeneration_context_changed")
    if window.get("image_slots") != previous.get("image_slots"):
        raise RegenerationContextError("regeneration_context_changed")
    if not isinstance(previous.get("raw"), str):
        raise RegenerationContextError("regeneration_candidate_unavailable")
    return [*base_messages, {"role": "assistant", "content": previous["raw"]},
        {"role": "user", "content": json.dumps(validation_feedback(archived, version=version, schema_version=schema_version,
            context=json.loads(base_messages[1]["content"])), ensure_ascii=False, sort_keys=True)}]


# Candidate mistakes are eligible only against the unchanged prepared input.
# Runtime/configuration/lease/protection failures are deliberately not here.
_AUTOMATIC_ERRORS = frozenset({
    "invalid_image_evidence", "json_invalid", "duplicate_json_key", "literal_error", "string_pattern_mismatch",
    "missing", "extra_forbidden", "unknown_event_time", "event_time_interval",
    "create_identity_invalid", "update_identity_required", "unknown_local_reference",
    "readonly_node", "unknown_reference", "node_outside_context", "node_type_mismatch",
    "expected_node_version_mismatch", "expected_profile_version_mismatch",
    "expected_relationship_version_mismatch", "coverage_event_version_mismatch",
    "coverage_atomic_version_mismatch", "evidence_user_mismatch",
    "profile_user_mismatch", "relationship_user_mismatch", "source_outside_window",
    "coverage_endpoint_type_mismatch", "duplicate_coverage_pair", "edge_self_reference",
    "invalid_contains_hierarchy", "ordinary_topic_create_members", "supersedes_node_type_mismatch", "supersedes_cycle",
    "invalid_user_reference", "group_profile_limit", "profile_edit_capacity", "profile_edit_index",
}) | _FIELD_RULES.keys() | _NAMED_SCHEMA_ERRORS | {"schema_mismatch"}


def automatic_correction_allowed(error_code, window):
    """Do not reinterpret runtime conflicts or generic value errors as drafts."""
    errors = window.get("validation_errors", [])
    return (error_code in _AUTOMATIC_ERRORS | {"invalid_output"} and bool(errors)
            and all(error["type"] in _AUTOMATIC_ERRORS for error in errors))
