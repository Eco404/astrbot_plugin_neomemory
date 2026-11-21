"""Topic creation feedback measures resolved members, not edge count."""

import json
from types import SimpleNamespace

import pytest

from core.extraction_regeneration import automatic_correction_allowed, validation_feedback
from tests.test_extraction_apply import node
from tests.test_ordinary_topic_members import proposal
from tests.test_topic_extraction_dependencies import edge


@pytest.mark.parametrize("case,actual,distinct", [("empty", 0, 0), ("single", 1, 1), ("alias", 2, 1)])
def test_creation_feedback_reports_structural_minimum_without_inventing_semantics(case, actual, distinct):
    changes = [node("s1", ref="local_topic", kind="topic", content="社区绘画计划。")]
    edges = []
    context = {"known_nodes": [{"node_id": "n1", "node_type": "event", "content_version": 1}]}
    if case == "single":
        edges = [edge("s1", "local_topic", "n1")]
    elif case == "alias":
        changes.append(node("s1", ref="local_event", old=context["known_nodes"][0], content="绘画活动结束。"))
        edges = [edge("s1", "local_topic", ref) for ref in ("n1", "local_event")]
    archive = {"error_code": "ordinary_topic_create_members", "window": {
        "raw": json.dumps(proposal(SimpleNamespace(prompt_version="extraction-prompt.v1"), changes, edges)),
        "validation_errors": [{"type": "ordinary_topic_create_members", "path": "$.node_changes[0]"}],
    }}
    assert automatic_correction_allowed(archive["error_code"], archive["window"])
    error, = validation_feedback(archive, schema_version="extraction.v1", context=context)["errors"]
    assert error["type"] == "ordinary_topic_create_members"
    assert error["constraints"]["minItems"] == 2
    assert error["constraints"]["identity"] == "resolved_node_id"
    assert error["actual_items"] == actual
    assert error["distinct_items"] == distinct
    assert "数量只是结构下限" in error["instruction"]
    assert "不拆分或虚构节点凑数" in error["instruction"]
