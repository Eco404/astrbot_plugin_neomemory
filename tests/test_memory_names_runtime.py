"""Identity rendering through recall and the real plugin's request assembly."""

import copy
import json

import pytest

from core.context_bundle import assemble_context
from core.identity import default_logical_user_id, extract_event_identity
from core.retrieval import RecallPolicy, RecallService, render_nodes, select_context
from tests.test_context_bundle import config, context
from tests.test_retrieval import NOW, FakeEmbeddings, FakeMemory, memory_node, snapshot
from tests.test_recall_runtime import RuntimeEvent, seed, warm
from tests.test_recall_runtime import plugin_factory as plugin_factory
from astrbot.api.provider import ProviderRequest


@pytest.mark.asyncio
@pytest.mark.parametrize("active", [False, True])
async def test_recall_renders_current_names_without_changing_vector_input_or_evidence(active):
    item = memory_node("n", text='我记得 {{user:alice}} 与 {{user:bob}} 讨论过合唱。')
    original = copy.deepcopy(item)
    memory = FakeMemory(snapshot(item))
    embeddings = FakeEmbeddings({item["content_text"]: [1, 0], "合唱": [1, 0]},
                                cached={item["content_text"]: [1, 0]})
    lookups = []

    async def names(ids):
        lookups.append(ids)
        return {"alice": "旧昵称", "bob": "同名"}

    service = RecallService(memory, embeddings, name_resolver=names)
    result = await service.recall("space", "合唱", active=active, now=NOW,
                                  user_names={"alice": '新昵称"甲'})
    payload = json.loads(result.text.split("\n", 1)[1])
    assert payload[0]["memory"] == '我记得 新昵称"甲 与 同名 讨论过合唱。'
    assert lookups == [("alice", "bob")]
    assert result.hits[0]["content_text"] == original["content_text"]
    assert memory.data["nodes"][0] == original
    assert embeddings.calls[0][1] == [original["content_text"]]
    assert [call[1] for call in embeddings.calls if call[0] == "embed"] == [["合唱"]]


def test_both_budget_stages_measure_expanded_names_and_never_cut_a_qualifier():
    item = memory_node("n", text="我记得 {{user:alice}} 仅在周末参加合唱。")
    short_names = {"alice": "阿禾"}
    long_names = {"alice": "很长的昵称" * 90}
    policy = RecallPolicy(budget_tokens=len(render_nodes([item], short_names).encode()))
    assert select_context([item], [], policy, user_names=short_names) == [item]
    assert select_context([item], [], policy, user_names=long_names) == []
    cfg = config(recall_budget_tokens=policy.budget_tokens)
    bundle = assemble_context(context(), [item], cfg, user_names=long_names)
    assert bundle.hits == [] and bundle.text == ""
    assert bundle.diagnostics["long_term_omitted"] == [{"node_id": "n", "reason": "budget"}]
    assert item["content_text"].endswith("仅在周末参加合唱。")


@pytest.mark.asyncio
async def test_passive_and_tool_use_event_nickname_keep_endearment_and_stored_version(plugin_factory):
    plugin = await plugin_factory({"recall_budget_tokens": 6000})
    event = RuntimeEvent()
    identity = extract_event_identity(event)
    logical = default_logical_user_id(identity.platform_instance_id, identity.account_id)
    text = "我记得 {{user:" + logical + "}} 请我叫他“队长”，我们讨论过合唱。"
    space, node = await seed(plugin, event, content=text)
    await plugin.memory_store.upsert_profile(space_id=space["space_id"], logical_user_id=logical,
        tags=["合唱"], description="{{user:" + logical + "}} 喜欢合唱。")
    await plugin.memory_store.upsert_relationship(space_id=space["space_id"], logical_user_id=logical,
        state_text="我有点期待和 {{user:" + logical + "}} 再聊聊。队长这个称呼让我觉得亲近。")
    await warm(plugin, space)
    event.get_sender_name = lambda: "新账号昵称"
    request = ProviderRequest(prompt="合唱")
    await plugin.inject_memory(event, request)
    injected = request.extra_user_content_parts[0].text
    assert "新账号昵称" in injected and "{{user:" not in injected
    assert "队长" in injected and "我有点期待" in injected and "喜欢合唱" in injected
    assert request.extra_user_content_parts[0]._no_save
    active = await plugin.runtime.active_recall(event, "合唱")
    assert "新账号昵称" in active and "队长" in active and "{{user:" not in active
    stored = await plugin.memory_store.get_node(node["node_id"])
    assert stored["content_text"] == text
    assert stored["content_version"] == node["content_version"]
    assert plugin.context.calls == []


@pytest.mark.asyncio
async def test_profile_names_render_even_without_embeddings(plugin_factory):
    plugin = await plugin_factory({"embedding_provider_id": ""})
    event = RuntimeEvent()
    space, _ = await seed(plugin, event)
    logical = space["logical_user_id"]
    await plugin.memory_store.upsert_profile(space_id=space["space_id"], logical_user_id=logical,
        tags=["合唱"], description="{{user:" + logical + "}} 喜欢合唱。")
    event.get_sender_name = lambda: "阿禾"
    request = ProviderRequest()
    await plugin.inject_memory(event, request)
    assert "阿禾 喜欢合唱" in request.extra_user_content_parts[0].text
    assert plugin.context.calls == [] and plugin.context.embedding.calls == []
