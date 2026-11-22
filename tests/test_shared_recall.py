"""Cross-space grants and attribution through the three recall entry points."""

import asyncio
import time

import pytest

pytest.importorskip("astrbot.api")
from astrbot.api.provider import ProviderRequest

from core.retrieval import RecallResult
from core.shared_recall import deliver_shared, search_shared
from tests.test_recall_runtime import RuntimeEvent, plugin_factory, seed, warm


@pytest.mark.asyncio
async def test_shared_recall_delivers_only_authorized_long_term_memories(plugin_factory):
    plugin = await plugin_factory()
    query = "天文台的望远镜"
    event = RuntimeEvent(text=query, message_id="local")
    home, local = await seed(plugin, event, content=query + "在本地使用")
    group, foreign = await seed(plugin, RuntimeEvent(group_id="观星群", message_id="foreign"), content=query + "是在群中讨论的")
    other, forbidden = await seed(plugin, RuntimeEvent(persona="另一个人格", message_id="other"), content=query + "是另一个人格的经历")
    await plugin.memory_store.create_note(space_id=group["space_id"], content_text=query + "笔记")
    for space in (home, group, other):
        await warm(plugin, space)

    async def preview(space=home):
        return await plugin.runtime.test_recall(space["space_id"], query)

    disabled = await preview()
    assert [n["node_id"] for n in disabled["items"]] == [local["node_id"]]
    await plugin.memory_store.set_space_policy(home["space_id"], {"shared_recall_enabled": True})
    plugin.context.embedding.calls.clear()
    enabled = await preview()
    assert len(plugin.context.embedding.calls) == 1
    assert local["content_text"] in enabled["text"] and foreign["content_text"] in enabled["text"]
    assert forbidden["content_text"] not in enabled["text"] and query + "笔记" not in enabled["text"]
    assert enabled["items"][-1]["source_space_id"] == group["space_id"]
    assert enabled["items"][-1]["source_scene"] == "群聊「观星群」"
    assert "其他对话中的记忆" in enabled["text"] and "Alice" in enabled["text"]
    assert local["content_text"] not in (await preview(group))["text"]
    await plugin.memory_store.set_space_policy(group["space_id"], {"allow_group_read_private": True})
    assert local["content_text"] in (await preview(group))["text"]

    history = plugin.runtime.recall_history
    await history.set_enabled(True)
    request = ProviderRequest()
    await plugin.inject_memory(event, request)
    assert foreign["content_text"] in request.extra_user_content_parts[0].text
    assert request.extra_user_content_parts[0]._no_save
    response = await plugin.recall_tool(event, query)
    assert foreign["content_text"] in response
    for entry in await history.list(home["space_id"]):
        assert entry["items"][-1]["source_space_id"] == group["space_id"]
    if plugin.runtime.tasks:
        await asyncio.gather(*tuple(plugin.runtime.tasks))
    assert (await plugin.memory_store.get_node(foreign["node_id"]))["reinforcement"] == foreign["reinforcement"]
    await plugin.memory_store.set_space_policy(group["space_id"], {"exclude_from_shared_recall": True})
    assert foreign["content_text"] not in (await preview())["text"]


@pytest.mark.asyncio
async def test_cold_source_and_final_admission_after_revocation_or_version_change(plugin_factory):
    plugin = await plugin_factory()
    query = "共同种下的山茶花"
    group, node = await seed(plugin, RuntimeEvent(group_id="shared", message_id="foreign"), content=query)
    home, _ = await seed(plugin, RuntimeEvent(message_id="local"), content="其他记忆")
    memory = plugin.memory_store
    await memory.set_space_policy(home["space_id"], {"shared_recall_enabled": True})
    service = plugin.runtime.recall

    async def search():
        return await search_shared(memory, plugin.source_store, service, RecallResult(home["space_id"]),
                                   home, query, time.monotonic() + 5, preview=True)

    async def deliver(prepared, budget=15000):
        return await deliver_shared(memory, "当前记忆", prepared, space_id=home["space_id"], budget=budget)

    assert await search() == [] and plugin.context.embedding.calls == []
    await warm(plugin, group)
    prepared = await search()
    assert len(prepared) == 1
    assert await deliver(prepared, 200) == ("当前记忆", [])
    text, hits = await deliver(prepared)
    assert query in text and [n["node_id"] for n in hits] == [node["node_id"]]
    await memory.set_space_policy(group["space_id"], {"exclude_from_shared_recall": True})
    assert await deliver(prepared) == ("当前记忆", [])
    await memory.set_space_policy(group["space_id"], {"exclude_from_shared_recall": False})
    await memory.update_node_status(node["node_id"], "disabled")
    assert await deliver(prepared) == ("当前记忆", [])
