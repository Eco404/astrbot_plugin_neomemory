"""Context continuity independent of online retrieval and host persistence."""

import asyncio
import json
from types import SimpleNamespace

import pytest

pytest.importorskip("astrbot.api")

from astrbot.api.provider import ProviderRequest  # noqa: E402
from astrbot.core.agent.message import (  # noqa: E402
    Message,
    dump_messages_with_checkpoints,
)

from core.identity import extract_event_identity  # noqa: E402
from core.short_term import conversation_key  # noqa: E402
from storage.cache_store import CacheStore  # noqa: E402
from tests.test_recall_runtime import (  # noqa: E402
    RuntimeEvent,
    seed,
    warm,
)
from tests.test_recall_runtime import (
    plugin_factory as _plugin_factory,
)
from tests.test_settings import HostConfig  # noqa: E402

plugin_factory = _plugin_factory


async def profile(plugin, space, description="用户喜欢绘画。"):
    await plugin.memory_store.upsert_profile(space_id=space["space_id"],
        logical_user_id=space["logical_user_id"], tags=["绘画"], description=description)


@pytest.mark.asyncio
async def test_profile_relationship_and_active_note_hint_work_without_models_and_are_temporary(plugin_factory):
    plugin = await plugin_factory({"embedding_provider_id": ""})
    event = RuntimeEvent()
    space, _ = await seed(plugin, event)
    await profile(plugin, space)
    await plugin.memory_store.upsert_relationship(space_id=space["space_id"],
        logical_user_id=space["logical_user_id"], state_text="我感到交流很愉快。")
    await plugin.memory_store.create_note(space_id=space["space_id"], content_text="不能自动泄漏的笔记正文")
    request = ProviderRequest(prompt="继续聊聊")
    await plugin.inject_memory(event, request)
    part = request.extra_user_content_parts[0]
    assert "用户喜欢绘画" in part.text and "我感到交流很愉快" in part.text
    assert "neomemory_notes" in part.text and "不能自动泄漏的笔记正文" not in part.text
    assert part._no_save
    persisted = dump_messages_with_checkpoints([Message.model_validate(await request.assemble_context())])
    assert "用户喜欢绘画" not in json.dumps(persisted, ensure_ascii=False)
    assert plugin.context.calls == [] and plugin.context.embedding.calls == []


@pytest.mark.asyncio
async def test_embedding_timeout_keeps_completed_profile_context(plugin_factory):
    plugin = await plugin_factory({"recall_deadline_seconds": .1})
    event = RuntimeEvent()
    space, node = await seed(plugin, event)
    await profile(plugin, space)
    await warm(plugin, space)
    plugin.context.embedding.gate = asyncio.Event()
    request = ProviderRequest()
    await asyncio.wait_for(plugin.inject_memory(event, request), timeout=.5)
    assert "用户喜欢绘画" in request.extra_user_content_parts[0].text
    assert node["content_text"] not in request.extra_user_content_parts[0].text
    assert plugin.context.embedding.active == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["cache_failure", "recall_disabled", "provider_failure"])
async def test_independent_failures_and_recall_switch_do_not_disable_profile(plugin_factory, monkeypatch, mode):
    if mode == "cache_failure":
        async def fail(_store):
            raise OSError("synthetic unavailable cache")

        monkeypatch.setattr(CacheStore, "open", fail)
    plugin = await plugin_factory({"recall_enabled": mode != "recall_disabled"})
    event = RuntimeEvent()
    space, _ = await seed(plugin, event)
    await profile(plugin, space)
    if mode == "provider_failure":
        await warm(plugin, space)
        plugin.context.embedding.failure = ConnectionError("synthetic unavailable provider")
    request = ProviderRequest()
    await plugin.inject_memory(event, request)
    assert "用户喜欢绘画" in request.extra_user_content_parts[0].text
    assert plugin.ready


@pytest.mark.asyncio
async def test_failed_local_context_does_not_discard_valid_long_term_recall(plugin_factory, monkeypatch):
    plugin = await plugin_factory()
    event = RuntimeEvent()
    space, node = await seed(plugin, event)
    await warm(plugin, space)

    async def fail(*args, **kwargs):
        raise ValueError("synthetic malformed local context")

    monkeypatch.setattr(plugin.runtime.context_store, "snapshot", fail)
    request = ProviderRequest()
    await plugin.inject_memory(event, request)
    assert node["content_text"] in request.extra_user_content_parts[0].text


@pytest.mark.asyncio
async def test_group_only_current_speaker_group_profile_is_injected(plugin_factory):
    plugin = await plugin_factory({"embedding_provider_id": ""})
    event = RuntimeEvent(group_id="group-a")
    group, _ = await seed(plugin, event)
    private, _ = await seed(plugin, RuntimeEvent(message_id="private"))
    await profile(plugin, private, "私聊敏感画像")
    user = await plugin.source_store.resolve_account("test-platform", "u-1")
    await plugin.memory_store.upsert_profile(space_id=group["space_id"], logical_user_id=user,
        tags=[], description="群中发言人的公开画像")
    await plugin.memory_store.upsert_profile(space_id=group["space_id"], logical_user_id="other-user",
        tags=[], description="另一成员的画像")
    request = ProviderRequest()
    await plugin.inject_memory(event, request)
    text = request.extra_user_content_parts[0].text
    assert "群中发言人的公开画像" in text
    assert "私聊敏感画像" not in text and "另一成员的画像" not in text


@pytest.mark.asyncio
async def test_short_summary_is_scoped_and_suppressed_when_host_summary_exists(plugin_factory):
    plugin = await plugin_factory({"embedding_provider_id": ""})
    event = RuntimeEvent()
    space, _ = await seed(plugin, event)
    stream = extract_event_identity(event).stream_id
    await plugin.memory_store.upsert_short_term(space_id=space["space_id"],
        conversation_id=conversation_key(stream, "conversation-one"),
        summary_text="还在讨论旅行日期，尚未决定。",
        coverage={"stream_id": stream, "host_conversation_id": "conversation-one"})

    def request(cid, contexts=None):
        req = ProviderRequest(contexts=contexts or [])
        req.conversation = SimpleNamespace(cid=cid, persona_id="persona-test")
        return req

    own = request("conversation-one")
    await plugin.inject_memory(event, own)
    assert "还在讨论旅行日期" in own.extra_user_content_parts[0].text
    other = request("conversation-two")
    await plugin.inject_memory(event, other)
    assert other.extra_user_content_parts == []
    host_summary = request("conversation-one", [{"role": "user",
        "content": "Our previous history conversation summary:宿主已总结"}])
    await plugin.inject_memory(event, host_summary)
    assert host_summary.extra_user_content_parts == []


@pytest.mark.asyncio
async def test_snapshot_change_before_injection_drops_both_old_generation_context_and_recall(plugin_factory, monkeypatch):
    plugin = await plugin_factory()
    event = RuntimeEvent()
    space, _ = await seed(plugin, event)
    await profile(plugin, space)
    await warm(plugin, space)
    original = plugin.runtime.context_store.revalidate

    async def switch(snapshot, **kwargs):
        target = await plugin.memory_store.create_generation(space["space_id"])
        await plugin.memory_store.activate_generation(space["space_id"], target["generation_id"])
        return await original(snapshot, **kwargs)

    monkeypatch.setattr(plugin.runtime.context_store, "revalidate", switch)
    request = ProviderRequest()
    await plugin.inject_memory(event, request)
    assert request.extra_user_content_parts == []


@pytest.mark.asyncio
async def test_shared_budget_only_reinforces_returned_long_term_hits(plugin_factory, monkeypatch):
    plugin = await plugin_factory({"recall_budget_tokens": 700})
    event = RuntimeEvent()
    space, _ = await seed(plugin, event)
    await profile(plugin, space, "用户喜欢绘画，也在学习摄影。")
    await plugin.memory_store.create_note(space_id=space["space_id"], content_text="一个笔记")
    await warm(plugin, space)
    returned = []
    monkeypatch.setattr(plugin.runtime, "reinforce_returned", lambda result, event: returned.extend(result.hits))
    request = ProviderRequest()
    await plugin.inject_memory(event, request)
    text = request.extra_user_content_parts[0].text
    assert len(text.encode()) <= 700
    assert all(item["content_text"] in text for item in returned)


@pytest.mark.asyncio
async def test_saved_budget_priority_changes_next_injection_without_reload(plugin_factory, monkeypatch):
    plugin = await plugin_factory({"recall_budget_tokens": 700, "automatic_extraction_enabled": False})
    plugin._host_config = HostConfig(plugin._host_config)
    runtime, source_store, memory_store = plugin.runtime, plugin.source_store, plugin.memory_store
    event = RuntimeEvent()
    space, node = await seed(plugin, event,
        content="我记得对方在周末学习陶艺，只有提前预约时才去工作室。" * 4)
    description = "对方喜欢尝试新材料，需要事先预约工作室。"
    await profile(plugin, space, description)
    await warm(plugin, space)
    returned = []
    monkeypatch.setattr(runtime, "reinforce_returned", lambda result, event: returned.extend(result.hits))

    async def inject():
        returned.clear()
        request = ProviderRequest(prompt="聊聊工作室")
        await plugin.inject_memory(event, request)
        part = request.extra_user_content_parts[0]
        assert part._no_save and len(part.text.encode("utf-8")) <= 700
        return part.text, [item["node_id"] for item in returned]

    text, reinforced = await inject()
    assert node["content_text"] in text and description not in text
    assert reinforced == [node["node_id"]]
    before = await plugin.settings.get()
    saved = await plugin.settings.save(before["revision"], {"context_budget_priority": "context_first"})
    assert saved["applied"] and not saved["reload_required"]
    text, reinforced = await inject()
    assert description in text and node["content_text"] not in text
    assert reinforced == []

    reset = await plugin.settings.save(saved["revision"], {
        "context_budget_priority": saved["schema"]["context_budget_priority"]["default"]})
    assert reset["applied"] and reset["values"]["recall_budget_tokens"] == 700
    text, reinforced = await inject()
    assert node["content_text"] in text and description not in text
    assert reinforced == [node["node_id"]]
    assert plugin.runtime is runtime and plugin.source_store is source_store and plugin.memory_store is memory_store
    assert plugin.context.calls == []


@pytest.mark.asyncio
async def test_evidence_filter_hot_apply_affects_preview_and_injection(plugin_factory):
    plugin = await plugin_factory({"automatic_extraction_enabled": False})
    plugin._host_config = HostConfig(plugin._host_config)
    event = RuntimeEvent(text="鲸鱼如何换气？")
    space, node = await seed(plugin, event, content="练习书法前要先铺好宣纸。")
    plugin.context.embedding.response = lambda texts: [
        [1., 0.] if text == event.get_message_str() else [.4, .9165] for text in texts]
    await warm(plugin, space)

    async def returned():
        preview = await plugin.runtime.test_recall(space["space_id"], event.get_message_str())
        request = ProviderRequest(prompt=event.get_message_str())
        await plugin.inject_memory(event, request)
        text = "".join(part.text for part in request.extra_user_content_parts)
        return [item["node_id"] for item in preview["items"]], node["content_text"] in text

    assert plugin.config.recall_selection_mode == "adaptive"
    assert await returned() == ([], False)
    settings = await plugin.settings.get()
    saved = await plugin.settings.save(settings["revision"], {"recall_selection_mode": "gated"})
    assert saved["applied"] and not saved["reload_required"]
    assert await returned() == ([], False)
    await plugin.settings.save(saved["revision"], {"recall_selection_mode": "baseline"})
    assert await returned() == ([node["node_id"]], True)


@pytest.mark.asyncio
async def test_short_term_enqueue_is_background_and_cannot_hold_up_context(plugin_factory):
    plugin = await plugin_factory({"embedding_provider_id": ""})
    event = RuntimeEvent()
    space, _ = await seed(plugin, event)
    await profile(plugin, space)
    gate, entered = asyncio.Event(), asyncio.Event()

    async def pending(*args, **kwargs):
        entered.set()
        await gate.wait()

    plugin.short_term = SimpleNamespace(maybe_enqueue=pending)
    request = ProviderRequest()
    request.conversation = SimpleNamespace(cid="conversation-one", persona_id="persona-test")
    await asyncio.wait_for(plugin.inject_memory(event, request), timeout=.3)
    assert "用户喜欢绘画" in request.extra_user_content_parts[0].text
    assert entered.is_set()
    gate.set()


@pytest.mark.asyncio
async def test_context_order_live_save_and_scoped_diagnostics(plugin_factory):
    plugin = await plugin_factory({"embedding_provider_id": "", "recall_budget_tokens": 1000,
        "automatic_extraction_enabled": False})
    plugin._host_config = HostConfig(plugin._host_config)
    event = RuntimeEvent()
    space, _ = await seed(plugin, event)
    description = ("Only visits when the route is open. " * 20).strip()
    relationship = ("我愿意同对方仔细讨论这些选择。" * 17).strip()
    await profile(plugin, space, description)
    await plugin.memory_store.upsert_relationship(space_id=space["space_id"],
        logical_user_id=space["logical_user_id"], state_text=relationship)

    async def inject():
        request = ProviderRequest(prompt="Discuss the options")
        await plugin.inject_memory(event, request)
        return request.extra_user_content_parts[0].text

    assert description in await inject()
    before = await plugin.settings.get()
    saved = await plugin.settings.save(before["revision"], {"context_entry_order": "relationship_first"})
    assert saved["applied"]
    text = await inject()
    assert relationship in text and description not in text
    diagnostic = plugin.runtime.diagnostics["last_context"]
    assert diagnostic["space_id"] == space["space_id"]
    assert diagnostic["request_id"] and diagnostic["recorded_at"] > 0
    assert diagnostic["injected_bytes"] == len(text.encode())
    await plugin.settings.save(saved["revision"], {"context_entry_order": "short_term_first"})
    assert description in await inject()
    assert plugin.runtime.diagnostics["last_context"]["request_id"] != diagnostic["request_id"]
    assert plugin.context.calls == []
