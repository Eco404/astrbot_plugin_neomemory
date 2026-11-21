"""宿主工具主动查询：真实存储、确定性向量和无外网故障验证。"""

import asyncio
import json
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from astrbot.core.provider.register import llm_tools

from tests.test_active_recall import entries
from core.retrieval import render_nodes
from tests.test_recall_runtime import RuntimeEvent, plugin_factory, seed, warm


def test_astrbot_registers_extended_tool_parameters():
    recall = llm_tools.get_func("neomemory_recall")
    properties = recall.parameters["properties"]
    assert set(properties) == {"query", "kind", "limit", "event_after", "event_before"}
    assert properties["limit"]["type"] == "number"  # 宿主注册器不支持 integer。
    assert "Integer" in properties["limit"]["description"]
    assert "UTC" in properties["event_after"]["description"]
    assert "delete" in llm_tools.get_func("neomemory_notes").parameters["properties"]["action"]["description"]


@pytest.mark.asyncio
async def test_note_query_without_embedding_obeys_scope_status_and_literal_search(plugin_factory):
    plugin = await plugin_factory({"embedding_provider_id": ""})
    own, other = RuntimeEvent(), RuntimeEvent(persona="another-persona", message_id="other")
    own_space, _ = await seed(plugin, own)
    await seed(plugin, other)
    wanted = json.loads(await plugin.notes_tool(own, "create", "计划 100%_complete"))
    archived = json.loads(await plugin.notes_tool(own, "create", "旧计划 100%_complete"))
    deleted = json.loads(await plugin.notes_tool(own, "create", "废弃计划 100%_complete"))
    await plugin.notes_tool(own, "archive", note_id=archived["note_id"])
    await plugin.notes_tool(own, "delete", note_id=deleted["note_id"])
    await plugin.notes_tool(other, "create", "他人计划 100%_complete")
    text = await plugin.recall_tool(own, "%_", kind="all")
    assert [(item["source"], item["id"]) for item in entries(text)] == [("note", wanted["note_id"])]
    assert plugin.runtime.recall is None
    assert plugin.context.embedding.calls == []
    assert await plugin.memory_store.database.fetchall("SELECT * FROM recall_reinforcements") == []
    assert "找不到" in await plugin.notes_tool(own, "restore", note_id=deleted["note_id"])
    for action in ("delete", "restore"):
        assert "找不到" in await plugin.notes_tool(other, action, note_id=wanted["note_id"])
    assert (await plugin.memory_store.list_notes(space_id=own_space["space_id"]))[0]["note_id"] == wanted["note_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["timeout", "exception"])
async def test_embedding_failure_does_not_prevent_independent_note_query(plugin_factory, failure):
    plugin = await plugin_factory({"active_deadline_seconds": .2})
    event = RuntimeEvent()
    space, _ = await seed(plugin, event)
    await warm(plugin, space)
    note = json.loads(await plugin.notes_tool(event, "create", "合成关键词的待办计划"))
    if failure == "timeout":
        plugin.context.embedding.gate = asyncio.Event()
    else:
        plugin.context.embedding.failure = ConnectionError("synthetic private provider detail")
    text = await asyncio.wait_for(plugin.recall_tool(event, "合成关键词"), timeout=1)
    assert [item["id"] for item in entries(text)] == [note["note_id"]]
    assert plugin.context.embedding.active == 0
    if failure == "timeout":
        assert plugin.context.embedding.cancelled >= 1
    assert "synthetic private" not in text
    assert await plugin.memory_store.database.fetchall("SELECT * FROM recall_reinforcements") == []


@pytest.mark.asyncio
async def test_mixed_result_only_reinforces_memory_entries_actually_returned(plugin_factory):
    plugin = await plugin_factory({"recall_max_nodes": 4})
    event = RuntimeEvent()
    space, first = await seed(plugin, event, content="合成事项一。")
    _, second = await seed(plugin, RuntimeEvent(message_id="second"), content="合成事项二。")
    await warm(plugin, space)
    note = json.loads(await plugin.notes_tool(event, "create", "合成事项的未来计划"))
    text = await plugin.recall_tool(event, "合成事项", limit=2)
    payload = entries(text)
    assert [item["source"] for item in payload] == ["memory", "note"]
    assert payload[1]["id"] == note["note_id"]
    assert len(text.encode("utf-8")) <= plugin.config.recall_budget_tokens
    await asyncio.gather(*list(plugin.runtime.tasks))
    rows = await plugin.memory_store.database.fetchall("SELECT node_id FROM recall_reinforcements")
    assert [row["node_id"] for row in rows] == [payload[0]["id"]]
    assert payload[0]["id"] in {first["node_id"], second["node_id"]}
    assert plugin.runtime.recall.policy.max_nodes == 4


@pytest.mark.asyncio
async def test_type_and_event_time_filters_apply_before_candidate_ranking(plugin_factory):
    plugin = await plugin_factory()
    event = RuntimeEvent()
    space, unknown = await seed(plugin, event, content="最高相似但未知时间。")
    source_ids = [item["source_id"] for item in await plugin.memory_store.list_node_sources(unknown["node_id"])]
    start = datetime(2026, 11, 13, tzinfo=timezone.utc).timestamp()
    target = await plugin.memory_store.create_node(space_id=space["space_id"], node_type="event",
        content_text="目标时间内的事件。", source_ids=source_ids, event_time_start=start + 1)
    await plugin.memory_store.create_node(space_id=space["space_id"], node_type="topic",
        content_text="相似主题。", source_ids=source_ids, event_time_start=start + 1)
    await plugin.memory_store.create_node(space_id=space["space_id"], node_type="event",
        content_text="时间范围外事件。", source_ids=source_ids, event_time_start=start - 10)
    plugin.context.embedding.response = lambda texts: [[.8, .6] if text == target["content_text"] else [1., 0.] for text in texts]
    await warm(plugin, space)
    plugin.runtime.recall.policy = replace(plugin.runtime.recall.policy, candidate_per_query=1, max_neighbors=0)
    await plugin.notes_tool(event, "create", "检索关键词的未来计划")
    text = await plugin.recall_tool(event, "检索关键词", kind="event", event_after="2026-11-13", event_before="2026-11-13")
    assert [item["id"] for item in entries(text)] == [target["node_id"]]
    assert "没有可返回" in await plugin.recall_tool(event, "检索关键词", kind="note", event_after="2026-11-13")


@pytest.mark.asyncio
async def test_note_only_query_never_enters_embedding_pipeline(plugin_factory):
    plugin = await plugin_factory()
    event = RuntimeEvent()
    space, _ = await seed(plugin, event)
    await warm(plugin, space)
    note = json.loads(await plugin.notes_tool(event, "create", "单查笔记关键词"))
    before = len(plugin.context.embedding.calls)
    plugin.context.embedding.failure = AssertionError("notes must not invoke embeddings")
    result = await plugin.recall_tool(event, "关键词", kind="note", limit=1)
    assert [item["id"] for item in entries(result)] == [note["note_id"]]
    assert len(plugin.context.embedding.calls) == before


@pytest.mark.asyncio
async def test_note_deleted_while_embedding_is_pending_is_not_returned(plugin_factory):
    plugin = await plugin_factory()
    event = RuntimeEvent()
    space, _ = await seed(plugin, event)
    await warm(plugin, space)
    note = json.loads(await plugin.notes_tool(event, "create", "删除竞态关键词"))
    plugin.context.embedding.started.clear()
    plugin.context.embedding.gate = asyncio.Event()
    task = asyncio.create_task(plugin.recall_tool(event, "删除竞态关键词"))
    try:
        await asyncio.wait_for(plugin.context.embedding.started.wait(), timeout=1)
        assert json.loads(await plugin.notes_tool(event, "delete", note_id=note["note_id"]))["status"] == "deleted"
        plugin.context.embedding.gate.set()
        response = await task
        assert all(item["source"] == "memory" for item in entries(response))
        assert note["note_id"] not in response
    finally:
        plugin.context.embedding.gate.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_final_active_budget_restores_atomic_and_reinforces_only_returned_node(plugin_factory):
    plugin = await plugin_factory()
    event = RuntimeEvent()
    space, parent = await seed(plugin, event, content="事件整体叙述。" * 20)
    sources = await plugin.memory_store.list_node_sources(parent["node_id"])
    atomic = await plugin.memory_store.create_node(space_id=space["space_id"], node_type="atomic",
        content_text="已确定的细节。", source_ids=[sources[0]["source_id"]])
    await plugin.memory_store.add_coverage(parent["node_id"], atomic["node_id"])
    await warm(plugin, space)
    budget = len(render_nodes([parent]).encode("utf-8"))
    plugin.runtime.config = replace(plugin.runtime.config, recall_budget_tokens=budget)
    plugin.runtime.recall.policy = replace(plugin.runtime.recall.policy, budget_tokens=budget)
    before = len(plugin.context.embedding.calls)
    response = await plugin.recall_tool(event, "预算测试查询", kind="memory")
    assert [item["id"] for item in entries(response)] == [atomic["node_id"]]
    assert len(response.encode("utf-8")) <= budget
    assert len(plugin.context.embedding.calls) - before == 1
    await asyncio.gather(*list(plugin.runtime.tasks))
    rows = await plugin.memory_store.database.fetchall("SELECT node_id FROM recall_reinforcements")
    assert [row["node_id"] for row in rows] == [atomic["node_id"]]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["disable_event", "remove_coverage"])
async def test_final_active_candidates_and_coverage_are_revalidated(plugin_factory, monkeypatch, change):
    plugin = await plugin_factory()
    event = RuntimeEvent()
    space, parent = await seed(plugin, event, content="包含具体细节的事件。")
    sources = await plugin.memory_store.list_node_sources(parent["node_id"])
    atomic = await plugin.memory_store.create_node(space_id=space["space_id"], node_type="atomic",
        content_text="独立的具体细节。", source_ids=[sources[0]["source_id"]])
    await plugin.memory_store.add_coverage(parent["node_id"], atomic["node_id"])
    await warm(plugin, space)
    original = plugin.runtime.recall.recall

    async def mutate_after_ranking(*args, **kwargs):
        result = await original(*args, **kwargs)
        assert [item["node_id"] for item in result.hits] == [parent["node_id"]]
        if change == "disable_event":
            await plugin.memory_store.update_node_status(parent["node_id"], "disabled")
        else:
            await plugin.memory_store.database.execute("DELETE FROM node_coverage WHERE event_node_id=?",
                                                       (parent["node_id"],))
        return result

    monkeypatch.setattr(plugin.runtime.recall, "recall", mutate_after_ranking)
    response = await plugin.recall_tool(event, "最终校验", kind="memory")
    returned = {item["id"] for item in entries(response)}
    assert returned == ({atomic["node_id"]} if change == "disable_event"
                        else {parent["node_id"], atomic["node_id"]})


@pytest.mark.asyncio
async def test_final_active_validation_still_allows_historical_memory(plugin_factory):
    plugin = await plugin_factory()
    event = RuntimeEvent()
    space, historical = await seed(plugin, event, status="historical")
    await warm(plugin, space)
    response = await plugin.recall_tool(event, "历史经历", kind="memory")
    assert [item["id"] for item in entries(response)] == [historical["node_id"]]
    assert entries(response)[0]["status"] == "historical"
