"""Topic candidate rules supplement the shared schema-derived field feedback."""

from .candidate_feedback import TOPIC_RECOVERY_VERSION as VERSION

EXPLANATIONS = {
    "topic_create_identity": "create时topic_id和expected_version均为null；延续旧主题必须update原主题，不能为通过检查新建同义主题。",
    "topic_create_members": "新主题至少包含两个不同的输入event/atomic；不能为凑数加入无关成员，没有合适主题可不新建。",
    "topic_update_identity": "update使用输入中可编辑topic的topic_id及当前content_version，不能使用成员ID、只读目录ID或猜测版本。",
    "topic_duplicate_members": "member_ids must be unique：完整最终成员集合中每个ID只出现一次，保留仍有意义的成员。",
    "topic_duplicate_update": "同一已有主题每批最多更新一次；整合其有依据的变化，不重复输出冲突更新。",
    "topic_locked_membership": "该主题含失效或待复核成员，略过该主题，保留其他合法变化。",
    "topic_protected_membership": "保留原有受保护成员，不添加新的受保护成员。只返回有依据的合法变化，否则返回空 changes。",
    "topic_member_set": "member_ids必须唯一；新建至少两个成员，更新至少一个成员；只使用输入支持的关联。",
    "topic_member_unavailable": "新增成员只能来自当前输入nodes中可用的active event/atomic；更新原主题可保留其已有historical成员，按历史含义表述，不恢复成当前事实。不能引用topic、只读目录未提供的节点、失效节点或编造ID。",
    "topic_unknown_user": "正文、标题和情绪只使用当前成员及空间支持的逻辑用户身份，不猜测、替换或新增用户ID。",
}


def refresh_feedback(raw, errors, *, schema_version, context):
    """Refresh an explicitly regenerated draft against its frozen input only."""
    from copy import deepcopy
    import json
    from .candidate_feedback import enrich_feedback, validation_errors
    from .topic_schema import output_schema, parse_output

    schema = output_schema(schema_version)
    try:
        parse_output(raw, expected_schema_version=schema_version)
    except ValueError as exc:
        return validation_errors(exc, raw, schema, EXPLANATIONS, context=context, pipeline='topic')
    return enrich_feedback(deepcopy(errors), json.loads(raw), schema, context=context, pipeline='topic')
