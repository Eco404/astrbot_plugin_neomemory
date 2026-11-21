"""Speaker excerpts follow version provenance, not all accumulated citations."""

import json

import pytest

from core.topic_evidence import collect_topic_evidence
from tests.test_maintenance_engine import environment
from tests.test_extraction_apply import capture
from storage.provenance_store import record_derivation, version_reference


@pytest.mark.asyncio
async def test_version_sources_preserve_roles_and_do_not_use_later_cumulative_sources(environment):
    env = environment
    source = await capture(env.sources, "我独自参加了两次湿地观察。", number=3)
    node = await env.memory.create_node(space_id=env.space["space_id"], node_type="event",
        content_text="参加了两次湿地观察。", source_ids=[source.source_id])
    later = await capture(env.sources, "我还没参与这些活动，只听你介绍。", number=4, actor="agent")
    await env.memory.database.execute(
        "INSERT INTO node_sources(node_id,source_id,source_kind,created_at) VALUES (?,?,'source_record',0)",
        (node["node_id"], later.source_id))
    evidence = await collect_topic_evidence(env.memory, env.sources, [node])
    assert len(evidence) == 1
    records = evidence[0]["sources"]
    assert len(records) == 1 and records[0]["source_id"] == source.source_id
    assert records[0]["actor_kind"] == "user"
    assert records[0]["actor_logical_user_id"] == source.logical_user_id
    assert records[0]["content_text"] == "我独自参加了两次湿地观察。"
    assert records[0]["basis_version"] == 1


@pytest.mark.asyncio
async def test_bounded_excerpts_are_explicit_and_missing_source_is_not_invented(environment, monkeypatch):
    env = environment
    source = await capture(env.sources, "一段很长的原始说明。" * 100, number=3)
    node = await env.memory.create_node(space_id=env.space["space_id"], node_type="event",
        content_text="一项经历", source_ids=[source.source_id])
    evidence = await collect_topic_evidence(env.memory, env.sources, [node], char_budget=30)
    assert len(evidence[0]["sources"][0]["content_text"]) == 30
    assert evidence[0]["sources"][0]["truncated"]
    assert evidence[0]["complete"] is False
    async def absent(_):
        return False
    monkeypatch.setattr(env.sources, "has_source", absent)
    assert (await collect_topic_evidence(env.memory, env.sources, [node]))[0]["sources"] == []


@pytest.mark.asyncio
async def test_cross_type_confirmation_chain_terminates_without_using_membership_as_content_evidence(environment):
    env = environment
    source = await capture(env.sources, "我参加了社区绘画活动。", number=3)
    association = await capture(env.sources, "把这项活动放进那个主题里。", number=4)
    event = await env.memory.create_node(space_id=env.space["space_id"], node_type="event",
        content_text="参加社区绘画活动。", source_ids=[source.source_id])
    topic = await env.memory.create_node(space_id=env.space["space_id"], node_type="topic",
        content_text="社区绘画计划。", source_ids=[source.source_id])
    async with env.memory.database.transaction() as connection:
        await record_derivation(connection, "node", event["node_id"], 1, operation="confirm", editor_kind="model", now=1000,
            base_versions=[version_reference("node", topic["node_id"], 1)])
        await record_derivation(connection, "node", topic["node_id"], 1, operation="confirm", editor_kind="model", now=1001,
            base_versions=[version_reference("node", event["node_id"], 1)])
        await record_derivation(connection, "node", topic["node_id"], 1, operation="membership", editor_kind="model", now=1002,
            sources=[(association.source_id, "source_record")],
            semantic={"member_versions": [version_reference("node", event["node_id"], 1)]})
    evidence = await collect_topic_evidence(env.memory, env.sources, [event])
    assert [item["source_id"] for item in evidence[0]["sources"]] == [source.source_id]


@pytest.mark.asyncio
async def test_group_citations_resolve_targets_within_member_version(environment):
    from core.memory_spaces import group_space
    from core.topic_prompt import build_messages
    from core.topic_schema import parse_output
    from core.window_users import WindowUsers
    from storage.topic_store import TopicStore

    env = environment
    space = await env.memory.get_or_create_space(group_space("p", "platform", "club"))
    sender = await capture(env.sources, "<@editor> 你的校样我已签收，<@nobody> 的还没送到。", group="club", user="desk", number=10)
    editor = await capture(env.sources, "下一版还在修改。", group="club", user="editor", number=11)
    node = await env.memory.create_node(space_id=space["space_id"], node_type="event", content_text=(
        "{{user:" + sender.logical_user_id + "}}收下了{{user:" + editor.logical_user_id + "}}的校样。"),
        source_ids=[sender.source_id, editor.source_id])
    options = await TopicStore(env.memory).prepare(space["space_id"], node["generation_id"], [node["node_id"]])
    ids = options["allowed_user_ids"]
    users = WindowUsers(ids)
    evidence = await collect_topic_evidence(env.memory, env.sources, [node])
    messages = build_messages("人格", [node], [], evidence=evidence, allowed_user_ids=ids)
    rows = json.loads(messages[1]["content"])["evidence"][0]["sources"]
    row = next(source for source in rows if source["source_id"] == sender.source_id)
    assert row["actor_logical_user_id"] == users.encode(sender.logical_user_id)
    assert row["group_addressing"] == [
        {"kind": "mention", "actor_kind": "user", "logical_user_id": users.encode(editor.logical_user_id), "marker": "[@1]"},
        {"kind": "mention", "actor_kind": "unknown", "marker": "[@2]"}]
    assert row["content_text"].startswith("[@1]") and "conversation_logical_user_id" not in row
    assert "group_addressing" in messages[0]["content"]
    assert evidence[0]["sources"] != rows
    from tests.test_topic_schema import proposal
    data = json.loads(messages[1]["content"])
    candidate = {"schema_version": data["schema_version"], "changes": [proposal(
        content="{{user:" + users.encode(editor.logical_user_id) + "}}仍在修订校样。")]}
    restored = parse_output(json.dumps(candidate), expected_schema_version=data["schema_version"], window_user_ids=ids)
    assert editor.logical_user_id in restored["changes"][0]["content"]


@pytest.mark.asyncio
async def test_topic_evidence_wraps_nested_quotes_in_group_input(environment):
    from core.memory_spaces import group_space
    from core.topic_prompt import build_messages

    env = environment
    space = await env.memory.get_or_create_space(group_space("p", "platform", "club"))
    parts = [
        {"type": "At", "qq": "desk"}, {"type": "Plain", "text": " 转给你。"},
        {"type": "Node", "name": "记录员", "content": [
            {"type": "Plain", "text": "收到这段安排。"},
            {"type": "Node", "name": "船长", "content": "我下月要出海。<@quoted>"}]},
        {"type": "Forward", "id": "opaque-card"}]
    from core.source_ingest import normalize_event
    from tests.test_source_ingest import FakeEvent, MessageObject

    class GroupEvent(FakeEvent):
        def get_message_type(self):
            return "GroupMessage"

        def get_group_id(self):
            return "club"

    event = GroupEvent()
    event.message_str = " @值班员(desk)  转给你。"
    event.message_obj = MessageObject(message_id="forward-15", message=parts)
    source = await env.sources.capture(normalize_event(event, persona_id="p"))
    node = await env.memory.create_node(space_id=space["space_id"], node_type="event",
        content_text="群里收到一份安排。", source_ids=[source.source_id])
    evidence = await collect_topic_evidence(env.memory, env.sources, [node])
    prompt = build_messages("人格", [node], [], evidence=evidence, allowed_user_ids=[source.logical_user_id])
    row = json.loads(prompt[1]["content"])["evidence"][0]["sources"][0]
    assert "content_text" not in row and row["sender_comment"] == "[@1] 转给你。"
    assert row["forwarded_messages"] == [
        {"author": "记录员", "text": "收到这段安排。", "forwarded_messages": [
            {"author": "船长", "text": "我下月要出海。<@quoted>"}]}, {"content_available": False}]
    assert len(row["group_addressing"]) == 1 and row["actor_logical_user_id"] == "u1"
    assert "forwarded_messages" in prompt[0]["content"]
