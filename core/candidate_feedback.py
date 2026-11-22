"""Schema-derived candidate diagnostics shared by bounded memory pipelines."""
from __future__ import annotations

import json
from pydantic import ValidationError
from .extraction_errors import DuplicateJSONKey, ExtractionCapacityError
from .relationship_state import SLOT_OPERATIONS

TOPIC_RECOVERY_VERSION = "topic-regeneration.v2-dev.1"
SHORT_TERM_RECOVERY_VERSION = "short-term-regeneration.v1"

# Schema field rules share error classification and feedback. Limits themselves
# are read from the same JSON schema supplied in the initial request.
FIELD_RULES = {
    "readable_too_long": "文本不得超过constraints.x-readable-maxLength个可读字符；合法完整用户引用计一个字符，保留主体、条件和否定。",
    "string_too_long": "文本不得超过constraints.maxLength个编码字符；保留主体、条件和否定，不截断引用。",
    "string_too_short": "文本不得少于constraints.minLength个编码字符；根据来源提供完整值，不填充或补造事实。",
    "too_long": "值的长度不得超过constraints中的上限；字符串按maxLength，列表按maxItems。保留必要内容，不增加重复项。",
    "too_short": "值的长度不得少于constraints中的下限；字符串按minLength，列表按minItems。只用输入支持的值，不填充或补造来源。",
    "greater_than_equal": "数值不得小于constraints.minimum。",
    "less_than_equal": "数值不得大于constraints.maximum。",
    "greater_than": "数值必须大于constraints.exclusiveMinimum。",
    "less_than": "数值必须小于constraints.exclusiveMaximum。",
    "multiple_of": "数值必须符合constraints.multipleOf。",
    "literal_error": "从constraints.enum或const中逐字选择合法值，不翻译或自创标签。",
    "string_pattern_mismatch": "字符串必须符合constraints.pattern，保留引用归属，不生成其他格式。",
    "string_type": "值必须是schema指定的字符串，不把列表或对象序列化来代替正文。",
    "int_type": "值必须是JSON整数，不能用字符串、小数或布尔值替代。版本必须来自当前输入。",
    "float_type": "值必须是JSON数值，不能用字符串或布尔值替代。",
    "bool_type": "值必须是JSON布尔值true或false，不能用字符串或数字替代。",
    "list_type": "值必须是JSON数组，元素符合当前schema；不得用字符串或对象代替。",
    "dict_type": "值必须是JSON对象，字段符合当前schema。",
    "model_type": "值必须是当前schema定义的对象，补齐其必需字段；仅在nullable允许时用null。",
    "finite_number": "数值必须有限并符合schema范围，不使用NaN或Infinity。",
    "missing": "补齐schema要求的字段；仅在nullable允许时用null，不虚构来源。",
    "extra_forbidden": "移除schema未定义的字段，必要内容放在协议已有字段中。",
}


def field_constraints(schema, location, *, standards=False):
    """Read public constraints from the declared schema, never exception values."""
    field = schema

    def resolve(value):
        if "$ref" in value:
            value = schema["$defs"][value["$ref"].rsplit("/", 1)[1]]
        if "anyOf" in value:
            value = next((option for option in value["anyOf"] if option.get("type") != "null"), {})
            return resolve(value)
        return value

    for component in location:
        field = resolve(field)
        field = field.get("items", {}) if type(component) is int else field.get("properties", {}).get(component, {})
    nullable = any(value.get("type") == "null" for value in field.get("anyOf", []))
    field = resolve(field)
    result = {key: field[key] for key in (
        "type", "enum", "const", "pattern", "minItems", "maxItems", "minLength", "maxLength",
        "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf",
        "x-readable-maxLength", "x-readable-limitsByNodeType",
    ) if key in field}
    if standards:
        if field:
            result["nullable"] = nullable
        for key in ("uniqueItems", "required", "additionalProperties"):
            if key in field:
                result[key] = field[key]
        if field.get("additionalProperties") is False:
            result["allowed_fields"] = list(field.get("properties", {}))
    return result


def field_feedback(error, schema, path, decoded, *, standards=False):
    kind = error["type"]
    if kind not in FIELD_RULES:
        return None
    constraints = field_constraints(schema, error["loc"], standards=standards)
    if standards and kind == "extra_forbidden":
        constraints = field_constraints(schema, error["loc"][:-1], standards=True) if error["loc"] else constraints
    limits = constraints.pop("x-readable-limitsByNodeType", None)
    if limits:
        owner = decoded
        for component in error["loc"][:-1]:
            owner = owner[component]
        node_type = owner.get("node_type")
        if isinstance(node_type, str) and node_type in limits:
            constraints["x-readable-maxLength"] = limits[node_type]
    result = {"path": path, "type": kind, "instruction": FIELD_RULES[kind], "constraints": constraints}
    if kind in {"readable_too_long", "string_too_long", "string_too_short", "too_long", "too_short"}:
        value = decoded
        for component in error["loc"]:
            value = value[component]
        if kind == "readable_too_long":
            from .text_lengths import readable_text_length
            result["actual_readable_chars"] = readable_text_length(value)
        elif isinstance(value, str):
            result["actual_encoded_chars"] = len(value)
        else:
            result["actual_items"] = len(value)
    if standards and kind in {"greater_than_equal", "less_than_equal", "greater_than", "less_than", "multiple_of"}:
        value = decoded
        for component in error["loc"]:
            value = value[component]
        result["actual_value"] = value
    return result


class CandidateValidationError(ValueError):
    """A fixed candidate rule failed; the message is for local diagnostics."""
    def __init__(self, code, location, message):
        self.code, self.location = code, tuple(location)
        super().__init__(message)


def _path(location, schema):
    result = "$"
    field = schema
    for component in location:
        if "$ref" in field:
            field = schema["$defs"][field["$ref"].rsplit("/", 1)[1]]
        if "anyOf" in field:
            field = next(value for value in field["anyOf"] if value.get("type") != "null")
            if "$ref" in field:
                field = schema["$defs"][field["$ref"].rsplit("/", 1)[1]]
        if type(component) is int:
            result += f"[{component}]"
            field = field.get("items", {})
        elif component in field.get("properties", {}):
            result += "." + component
            field = field["properties"][component]
        else:
            break
    return result


_COMMON = {
    "json_invalid": "返回完整有效JSON对象，检查所示解析位置，不使用Markdown或额外文字。",
    "duplicate_json_key": "同一JSON对象的字段名只能出现一次，不猜测或静默合并冲突值。",
    "invalid_user_reference": "使用输入中合法完整的{{user:ID}}，不得截断、编造或更换归属。",
    "duplicate_values": "列表中的值必须唯一，保留必要信息，不合并不同经历或删除必要成员。",
    "neutral_emotion_conflict": "neutral只能单独使用；按实际有依据的感受选择情绪，不改变主体。",
}


def validation_errors(exc, raw, schema, explanations, *, standards=True, context=None, pipeline=None):
    """Return bounded structured diagnostics without forwarding exception text."""
    if isinstance(exc, ExtractionCapacityError):
        return [{"path": "$", "type": "output_capacity_exceeded", "actual_encoded_chars": exc.actual_chars,
                 "limit_chars": exc.limit_chars}]
    if isinstance(exc, json.JSONDecodeError):
        return [{"path": "$", "type": "json_invalid", "instruction": _COMMON["json_invalid"],
                 "lineno": exc.lineno, "colno": exc.colno, "pos": exc.pos}]
    if isinstance(exc, DuplicateJSONKey):
        return [{"path": "$", "type": "duplicate_json_key", "instruction": _COMMON["duplicate_json_key"]}]
    if isinstance(exc, CandidateValidationError) and exc.code == "output_capacity_exceeded":
        if standards and pipeline == "short_term":
            from .short_term_prompts import MAX_OUTPUT_BYTES
            return [{"path": "$", "type": exc.code, "actual_bytes": len(raw.encode("utf-8")),
                     "limit_bytes": MAX_OUTPUT_BYTES}]
        return [{"path": "$", "type": exc.code}]
    if isinstance(exc, ValidationError):
        issues = exc.errors(include_url=False, include_input=False, include_context=False)[:20]
    elif isinstance(exc, CandidateValidationError):
        issues = [{"type": exc.code, "loc": exc.location}]
    else:
        return [{"path": "$", "type": "unclassified_candidate_error"}]
    decoded = json.loads(raw)
    result = []
    for item in issues:
        path = _path(item["loc"], schema)
        precise = field_feedback(item, schema, path, decoded, standards=standards)
        if precise is not None:
            result.append(precise)
        else:
            kind = item["type"]
            result.append({"path": path, "type": kind,
                "instruction": explanations.get(kind, _COMMON.get(kind, "候选未通过检查，请查看维护诊断。"))})
    return enrich_feedback(result, decoded, schema, context=context, pipeline=pipeline) if standards else result


def correction_allowed(errors, explanations):
    allowed = FIELD_RULES.keys() | _COMMON.keys() | explanations.keys()
    return bool(errors) and all(error["type"] in allowed for error in errors)


def recovery_messages(base, raw, errors, protocol):
    return [*base, {"role": "assistant", "content": raw}, {"role": "user", "content": json.dumps({
        "protocol": protocol, "errors": errors,
        "instruction": "上一条assistant是被拒绝的候选草稿，不是来源事实。按相同原始输入和schema重新生成完整候选，保留主体、条件、否定和必要信息，不返回补丁，不为通过校验捏造内容。",
    }, ensure_ascii=False)}]


def compact_json_messages(messages):
    """Return a lossless transport copy of whole JSON message contents.

    Non-JSON instructions and invalid drafts stay verbatim so parser positions
    remain meaningful. A lexical pass preserves string contents, number tokens,
    order and duplicate keys; decoding and reserializing would lose that evidence.
    The caller retains its canonical input and digest independently of this copy.
    """
    def reject_constant(_):
        raise ValueError('nonfinite JSON')

    def compact(text):
        try:
            json.loads(text, parse_constant=reject_constant)
        except ValueError:
            return text
        result = []
        inside = escaped = False
        for char in text:
            if inside:
                result.append(char)
                if escaped:
                    escaped = False
                elif char == '\\':
                    escaped = True
                elif char == '"':
                    inside = False
            else:
                if char == '"':
                    inside = True
                if char not in ' \t\r\n':
                    result.append(char)
        return ''.join(result)

    return [{**message, 'content': compact(message['content'])} for message in messages]


def _location(path):
    import re
    return [int(index) if index else name for name, index in re.findall(r'\.([a-z_]+)|\[([0-9]+)\]', path)]


def _value_at(decoded, location):
    value = decoded
    for key in location:
        value = value[key]
    return value


def _unique_diagnostics(decoded, path):
    """Expose duplicate counts/field identities, never the candidate prose."""
    location = _location(path)
    value = _value_at(decoded, location)
    lists = []
    if isinstance(value, list):
        lists.append((path, value))
    elif isinstance(value, dict):
        for field in ('source_ids', 'base_node_ids', 'context_source_ids', 'confirmation_source_ids',
                      'tags', 'emotion_tags', 'member_ids', 'evidence_source_ids'):
            if isinstance(value.get(field), list):
                lists.append((path + '.' + field, value[field]))
        if path == '$':
            for field, keys in (('node_changes', ('local_ref',)), ('node_changes', ('node_id',)),
                                ('profile_changes', ('logical_user_id',)),
                                ('edges', ('from_ref', 'to_ref', 'relation_type')),
                                ('coverage_changes', ('event_ref', 'atomic_ref')),
                                ('changes', ('topic_id',))):
                values = [tuple(row[key] for key in keys) for row in value.get(field, [])
                          if all(row.get(key) is not None for key in keys)]
                lists.append((f'$.{field}', values))
    result = []
    for field_path, values in lists:
        per_tag = field_path.endswith('.tags') and values and all(isinstance(value, dict) and 'tag' in value for value in values)
        identities = [value['tag'] for value in values] if per_tag else values
        distinct = len({json.dumps(value, ensure_ascii=False, sort_keys=True) for value in identities})
        if distinct < len(values):
            result.append({'path': field_path, 'actual_items': len(values), 'distinct_items': distinct,
                           'constraints': {'unique_by': 'tag'} if per_tag else {'uniqueItems': True}})
    return result


def enrich_feedback(errors, decoded, schema, *, context=None, pipeline=None):
    """Attach existing cross-field standards and fixed-input metrics to new protocols.

    This changes diagnostics only. Parsers/storage remain the authoritative
    validators; prior protocol callers retain their original feedback bytes.
    """
    if not isinstance(decoded, dict):
        return errors
    context = context or {}
    known = {row['node_id']: row for row in context.get('known_nodes', context.get('nodes', []))}
    sources = {row['source_id']: row for row in context.get('sources', context.get('new_sources', []))}
    for error in errors:
        code, path = error['type'], error['path']
        loc = _location(path)
        item = decoded
        if loc and loc[0] in {'node_changes', 'profile_changes', 'coverage_changes', 'changes', 'edges'} and len(loc) > 1:
            rows = decoded.get(loc[0], [])
            item = rows[loc[1]] if loc[1] < len(rows) else None
        elif loc and loc[0] == 'relationship_change':
            item = decoded.get('relationship_change')
        # Saved application errors may name a field absent from a reparsed
        # historical draft. Its established explanation remains usable, but
        # measurements require the actual candidate object.
        if item is None:
            continue
        if code == 'string_too_long' and path == '$.summary_text' and 'stored_reference_chars' in context:
            text = decoded['summary_text']
            sizes = context['stored_reference_chars']
            error['actual_encoded_chars'] = len(text) + sum(
                text.count('{{user:' + alias + '}}') * (size - len('{{user:' + alias + '}}'))
                for alias, size in sizes.items())
            error['instruction'] = '按stored_reference_chars中的还原长度缩短摘要，保留主体、条件和否定。'
        if code in {'duplicate_values', 'duplicate_edges', 'duplicate_coverage_pair',
                    'topic_duplicate_members', 'topic_duplicate_update', 'topic_member_set'}:
            duplicates = _unique_diagnostics(decoded, '$' if code == 'topic_duplicate_update' else path)
            if duplicates:
                error['violations'] = duplicates
        if code == 'ordinary_topic_create_members':
            from .topic_schema import TOPIC_CREATE_MIN_MEMBERS
            aliases = {row['local_ref']: row['node_id'] if row['operation'] == 'update' else row['local_ref']
                       for row in decoded['node_changes']}
            types = {node_id: row['node_type'] for node_id, row in known.items()}
            types.update({aliases[row['local_ref']]: row['node_type'] for row in decoded['node_changes']})
            topic_id = aliases[item['local_ref']]
            members = [aliases.get(edge['to_ref'], edge['to_ref']) for edge in decoded['edges']
                       if edge['relation_type'] == 'contains'
                       and aliases.get(edge['from_ref'], edge['from_ref']) == topic_id
                       and types.get(aliases.get(edge['to_ref'], edge['to_ref'])) in {'event', 'atomic'}]
            error.update(constraints={'minItems': TOPIC_CREATE_MIN_MEMBERS,
                                      'member_node_types': ['event', 'atomic'],
                                      'identity': 'resolved_node_id'},
                         actual_items=len(members), distinct_items=len(set(members)))
        elif code in {'topic_create_members', 'topic_member_set'}:
            from .topic_schema import TOPIC_CREATE_MIN_MEMBERS
            members = item['member_ids']
            minimum = TOPIC_CREATE_MIN_MEMBERS if item['operation'] == 'create' else 1
            error.update(constraints={'minItems': minimum, 'uniqueItems': True},
                         actual_items=len(members), distinct_items=len(set(members)))
        elif code == 'initial_summary_evidence_required':
            minimum = schema['allOf'][0]['then']['properties']['evidence_source_ids']['minItems']
            error.update(constraints={'minItems': minimum}, actual_items=len(decoded['evidence_source_ids']))
        elif code in {'create_identity_invalid', 'topic_create_identity'}:
            field = 'topic_id' if code.startswith('topic_') else 'node_id'
            error['constraints'] = {field: {'const': None}, 'expected_version': {'const': None}}
        elif code in {'update_identity_required', 'topic_update_identity'}:
            field = 'topic_id' if code.startswith('topic_') else 'node_id'
            error['constraints'] = {field: {'nullable': False}, 'expected_version': {'nullable': False, 'minimum': 1}}
            if code == 'topic_update_identity' and context:
                error['constraints'][field]['enum'] = [key for key, node in known.items()
                    if node['node_type'] == 'topic' and node.get('status') == 'active'
                    and not any(node.get(flag) for flag in ('manual_protected', 'read_only', 'summary_stale'))]
            old = known.get(item.get(field))
            if old and (field != 'topic_id' or old['node_type'] == 'topic'):
                error['constraints']['expected_version'] = {'const': old['content_version']}
                error['actual_version'] = item.get('expected_version')
        elif code == 'event_time_interval':
            value = item['event_time']
            error.update(constraints={'end_greater_than_or_equal_to_start': True},
                         actual_start=value['start'], actual_end=value['end'])
        elif code == 'unknown_event_time':
            error['constraints'] = {'const': None}
        elif code == 'neutral_emotion_conflict':
            value = _value_at(decoded, loc)
            error.update(constraints={'when_neutral_present_maxItems': 1}, actual_items=len(value['emotion_tags' if 'emotion_tags' in value else 'tags']))
        elif code == 'relationship_slot_body':
            value = _value_at(decoded, loc)
            replace = value['op'] == 'replace'
            error.update(constraints={'op': value['op'], 'body': {'min_non_whitespace_chars': 1, 'not_standalone_casefolded': list(SLOT_OPERATIONS)} if replace else {'const': ''}},
                         actual_encoded_chars=len(value['body']), actual_non_whitespace_chars=len(value['body'].strip()))
        elif code == 'subjective_schema_mismatch':
            error['constraints'] = field_constraints(schema, ('schema_version',), standards=True)
        elif code == 'interaction_confirmation_required':
            confirmed = item['interaction_status'] in {'accepted', 'completed'}
            error.update(constraints={'minItems': 1} if confirmed else {'maxItems': 0},
                         actual_items=len(item['confirmation_source_ids']))
        elif code == 'source_sets_overlap':
            overlap = set(item['source_ids']) & set(item['context_source_ids'])
            error.update(constraints={'max_overlapping_source_ids': 0}, actual_overlapping_source_ids=len(overlap))
        elif code == 'result_changes_mismatch':
            count = sum(len(decoded.get(field, [])) for field in ('node_changes', 'edges', 'coverage_changes', 'profile_changes'))
            count += int(decoded.get('relationship_change') is not None)
            error.update(actual_changes=count, constraints={'result': decoded['result'],
                'change_count': {'const': 0} if decoded['result'] == 'no_change' else {'minimum': 1},
                'reason': {'not': 'changes'} if decoded['result'] == 'no_change' else {'const': 'changes'}})
        elif code == 'node_content_too_long':
            from .extraction_schema import NODE_CONTENT_LIMITS
            error.update(constraints={'maxLength': NODE_CONTENT_LIMITS[item['node_type']]},
                         actual_encoded_chars=len(item['content']))
        elif code in {'edge_self_reference', 'coverage_self_reference'}:
            error['constraints'] = {'endpoints_must_differ': True}
        elif code == 'invalid_contains_hierarchy':
            error['constraints'] = {'allowed_type_pairs': [['event', 'atomic'], ['topic', 'event'], ['topic', 'atomic']]}
        elif code == 'coverage_endpoint_type_mismatch':
            error['constraints'] = {'event_ref_node_type': 'event', 'atomic_ref_node_type': 'atomic'}
        elif code == 'profile_evidence_insufficient':
            error['constraints'] = {'enum': ['explicit_self_statement', 'repeated_self_statement']}
        elif code == 'event_identity_mismatch':
            error['constraints'] = ({'const': 'same_event'} if item['operation'] == 'update'
                                    else {'enum': ['new_event', 'uncertain']})
        elif code == 'supersedes_node_type_mismatch':
            error['constraints'] = {'endpoint_node_types_must_match': True}
        elif code == 'supersedes_cycle':
            error['constraints'] = {'acyclic': True}
        elif code == 'event_metadata_mismatch':
            error['constraints'] = {field: ({'nullable': False} if item['node_type'] == 'event' else {'const': None})
                                    for field in ('event_status', 'event_identity')}
        if context:
            _fixed_context_feedback(error, item, decoded, known, sources, context)
    return errors


def _fixed_context_feedback(error, item, decoded, known, sources, context):
    code, path = error['type'], error['path']
    if code == 'readonly_node':
        error['actual_value'] = _value_at(decoded, _location(path))
        error['writable_nodes'] = [key for key, node in known.items()
                                   if node.get('status') == 'active' and not node.get('manual_protected')]
    elif code == 'topic_locked_membership':
        error['actual_value'] = item['topic_id']
        error['unavailable_members'] = [edge['target_node_id'] for edge in context.get('edges', [])
            if edge['source_node_id'] == item['topic_id'] and edge['relation_type'] == 'contains'
            and (known[edge['target_node_id']]['status'] not in {'active', 'historical'}
                 or known[edge['target_node_id']].get('summary_stale'))]
    elif code == 'topic_protected_membership':
        previous = {edge['target_node_id'] for edge in context.get('edges', [])
                    if edge['source_node_id'] == item.get('topic_id') and edge['relation_type'] == 'contains'}
        protected = {key for key, node in known.items() if node.get('manual_protected')}
        chosen = set(item.get('member_ids', []))
        error['remove_from_proposal'] = sorted((chosen - previous) & protected)
        error['restore_in_proposal'] = sorted((previous - chosen) & protected)
    elif code == 'topic_update_identity':
        available = {node_id: node for node_id, node in known.items()
            if node['node_type'] == 'topic' and node.get('status') == 'active'
            and not any(node.get(flag) for flag in ('manual_protected', 'read_only', 'summary_stale'))}
        if item.get('topic_id') not in available:
            error['violations'] = [{
                'path': path + '.topic_id', 'actual_value': item.get('topic_id'),
                'constraints': {'enum': list(available), 'nullable': False},
                'allowed_topics': [{'topic_id': node_id, 'expected_version': node['content_version']}
                                  for node_id, node in available.items()],
                'instruction': '该topic_id不属于本次可编辑主题集合。按内容确定目标后，从allowed_topics逐字复制完整topic_id及配套expected_version；不猜测或补写ID。',
            }]
        elif item.get('expected_version') != available[item['topic_id']]['content_version']:
            error['violations'] = [{
                'path': path + '.expected_version', 'actual_value': item.get('expected_version'),
                'constraints': {'const': available[item['topic_id']]['content_version']},
                'instruction': '保持有效topic_id，expected_version使用该主题输入中提供的当前content_version。',
            }]
    elif (code == 'missing' and path.endswith('.expected_version') and path.startswith('$.changes[')
          and item.get('operation') == 'update' and item.get('topic_id') in known
          and known[item['topic_id']]['node_type'] == 'topic'):
        error['constraints'] = {'const': known[item['topic_id']]['content_version'], 'nullable': False}
        error['actual_value'] = None
    elif code in {'expected_node_version_mismatch', 'node_type_mismatch'}:
        old = known[item['node_id']]
        field = 'expected_version' if code.startswith('expected_') else 'node_type'
        expected = old['content_version'] if field == 'expected_version' else old[field]
        error.update(constraints={'const': expected})
        if field == 'expected_version':
            error['actual_value'] = item[field]
    elif code in {'expected_profile_version_mismatch', 'expected_relationship_version_mismatch'}:
        previous = (next((row for row in context.get('known_profiles', [])
                          if row['logical_user_id'] == item['logical_user_id']), None)
                    if code == 'expected_profile_version_mismatch' else context.get('known_relationship'))
        error.update(constraints={'const': previous['content_version'] if previous else 0}, actual_value=item['expected_version'])
    elif code in {'coverage_event_version_mismatch', 'coverage_atomic_version_mismatch'}:
        from .memory_emotions import encode_emotion
        field = 'event' if code == 'coverage_event_version_mismatch' else 'atomic'
        ref = item[field + '_ref']
        change = next((row for row in decoded['node_changes'] if row.get('node_id') == ref or row['local_ref'] == ref), None)
        old = known.get(change['node_id'] if change else ref)
        if old is None:
            expected = 1
        else:
            changed = bool(change and (change['content'] != old['content_text'] or
                ('emotion' in change and encode_emotion(change['emotion']) != encode_emotion(old.get('emotion')))))
            expected = old['content_version'] + int(changed)
        error.update(constraints={'const': expected}, actual_value=item['expected_' + field + '_version'],
                     version_basis='after_candidate_content_and_normalized_emotion')
    elif code in {'evidence_user_mismatch', 'profile_repeated_evidence_missing'}:
        statements = [sources[source_id] for source_id in item['source_ids']]
        own = sum(row.get('actor_kind') == 'user' and row.get('actor_logical_user_id') == item['logical_user_id'] for row in statements)
        other = sum(row.get('actor_kind') == 'user' and row.get('actor_logical_user_id') != item['logical_user_id'] for row in statements)
        error.update(constraints={'minimum_own_user_sources': 2 if code == 'profile_repeated_evidence_missing' else 1,
                                  'maximum_other_user_sources': 0}, actual_own_user_sources=own, actual_other_user_sources=other)
    elif code == 'interaction_confirmation_invalid':
        error['constraints'] = {'enum': [source_id for source_id in item['source_ids']
            if sources[source_id].get('actor_kind') == 'user' and
            sources[source_id].get('actor_logical_user_id') == item['logical_user_id']]}
    elif code in {'profile_user_mismatch', 'relationship_user_mismatch'}:
        error['constraints'] = {'const': context['space']['logical_user_id']}
    elif code in {'unknown_reference', 'source_outside_window', 'node_outside_context'}:
        if any(field in path for field in ('.source_ids', '.context_source_ids', '.confirmation_source_ids')):
            error['constraints'] = {'enum': list(sources)}
        elif path.endswith('.logical_user_id'):
            error['constraints'] = {'enum': _known_user_ids(context)}
        elif path.endswith('.node_id') or '.base_node_ids' in path:
            error['constraints'] = {'enum': list(known)}
        elif path.endswith(('.from_ref', '.to_ref', '.event_ref', '.atomic_ref')):
            error['constraints'] = {'enum': list(known) + [row['local_ref'] for row in decoded['node_changes']]}
        elif code == 'unknown_reference':
            error['constraints'] = {'allowed_user_ids': _known_user_ids(context)}
    elif code == 'unknown_local_reference':
        error['constraints'] = {'allowed_local_references': [row['local_ref'] for row in decoded['node_changes']]}
    elif code in {'summary_unknown_user', 'topic_unknown_user'}:
        key = 'allowed_user_ids' if code == 'topic_unknown_user' else 'known_user_ids'
        if key in context:
            error['constraints'] = {'allowed_user_ids': context[key]}
    elif code == 'topic_member_unavailable':
        previous = {edge['target_node_id'] for edge in context.get('edges', [])
                    if edge['source_node_id'] == item.get('topic_id') and edge['relation_type'] == 'contains'}
        error['constraints'] = {'enum': [node_id for node_id, node in known.items()
            if node['node_type'] in {'event', 'atomic'} and not node.get('summary_stale') and
            (node['status'] == 'active' or node['status'] == 'historical' and node_id in previous) and
            (not node.get('manual_protected') or node_id in previous)]}
        error['actual_value'] = _value_at(decoded, _location(path))


def _known_user_ids(context):
    """Read the same identity fields/prose as ExtractionReferences from fixed input."""
    from .memory_text import memory_texts, user_reference_ids
    records = [context.get('space', {}), *context.get('sources', []),
               *context.get('known_profiles', [])]
    if context.get('known_relationship') is not None:
        records.append(context['known_relationship'])
    users = {record[field] for record in records
             for field in ('logical_user_id', 'actor_logical_user_id', 'conversation_logical_user_id')
             if record.get(field)}
    users.update(target["logical_user_id"] for source in context.get("sources", [])
                 for target in source.get("group_addressing", []) if target.get("logical_user_id"))
    for record in [*context.get('known_nodes', []), *context.get('known_profiles', []),
                   *([context['known_relationship']] if context.get('known_relationship') else [])]:
        users.update(user_reference_ids(memory_texts(record)))
    return sorted(users)
