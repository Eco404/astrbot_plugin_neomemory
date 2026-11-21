"""Structured forwards have separate speakers in extraction and scoped replay."""

import json

import pytest

from core.context_rebuild import MODELS, scoped_messages
from core.extraction_apply import PreparedExtraction
from core.extraction_references import ExtractionReferences
from core.prompts import FORWARDED_SOURCE_RULE


def batch(parts):
    source = {
        "source_id": "source", "actor_kind": "user", "actor_logical_user_id": "forwarder",
        "actor_account_id": "account", "stream_id": "group-stream", "observed_at": 123,
        "content_text": "原始平台摘要", "content_json": json.dumps(parts),
        "metadata_json": json.dumps({"group_addressing": [
            {"kind": "mention", "account_id": "peer", "logical_user_id": "other"}]}),
    }
    return PreparedExtraction("batch", {"space_type": "group"}, "gen", 0, (source,),
                              (), (), None, "合成人格")


@pytest.mark.parametrize("kind", [None, *MODELS])
def test_forward_authorship_and_sender_comment_reach_each_extractor(kind):
    prepared = batch([
        {"type": "At", "qq": "peer"},
        {"type": "Plain", "text": "我已经寄出。"},
        {"type": "Node", "name": "笔友", "content": [{"type": "Plain", "text": "我正在休假。"}]},
        {"type": "Forward", "id": "opaque"},
    ])

    def request(prepared):
        messages = ExtractionReferences(prepared).messages()
        if kind:
            messages = scoped_messages(messages, kind, MODELS[kind].model_json_schema(),
                                       prompt_version=prepared.prompt_version)
        return messages[0]["content"], json.loads(messages[1]["content"])["sources"][0]

    rules, source = request(prepared)
    assert FORWARDED_SOURCE_RULE in rules
    assert source["content_text"] == "[@1]我已经寄出。"
    assert source["actor_logical_user_id"] == "u1"
    assert source["group_addressing"][0]["logical_user_id"] == "u2"
    assert source["forwarded_messages"] == [
        {"author": "笔友", "text": "我正在休假。"}, {"content_available": False}]
    assert prepared.sources[0]["content_text"] == "原始平台摘要"


def test_ordinary_message_does_not_receive_forwarded_source_rule():
    messages = batch([{"type": "Plain", "text": "转发是我的工作。"}]).messages()
    assert FORWARDED_SOURCE_RULE not in messages[0]["content"]
    assert "forwarded_messages" not in json.loads(messages[1]["content"])["sources"][0]


def test_forward_tree_retains_authors_and_qq_card_preview():
    from core.forwarded_content import project_forward

    parts = [
        {"type": "Nodes", "nodes": [{"type": "Node", "name": "甲", "content": [
            {"type": "Plain", "text": "转载如下。"},
            {"type": "node", "data": {"nickname": "乙", "content": "下周见。"}},
        ]}]},
        {"type": "Json", "data": {"app": "com.tencent.multimsg", "config": {"forward": 1},
            "meta": {"detail": {"news": [{"text": "丙：我会参加"}]}}}},
        {"type": "Plain", "text": "我的附言。"},
    ]
    assert project_forward(batch(parts).sources[0]) == {
        "sender_comment": "我的附言。",
        "forwarded_messages": [
            {"author": "甲", "text": "转载如下。", "forwarded_messages": [{"author": "乙", "text": "下周见。"}]},
            {"author": None, "text": "丙：我会参加", "preview_only": True},
        ],
    }
    assert project_forward(batch([{"type": "Json", "data": {"app": "other"}}]).sources[0]) is None
