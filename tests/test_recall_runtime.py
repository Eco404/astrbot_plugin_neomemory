"""AstrBot request/tool lifecycle against real source, memory and cache stores."""

from __future__ import annotations

import asyncio
import copy
import json
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import pytest_asyncio

pytest.importorskip("astrbot.api")

from astrbot.api.provider import ProviderRequest  # noqa: E402
from astrbot.core.agent.message import (  # noqa: E402
    Message,
    TextPart,
    dump_messages_with_checkpoints,
)

import adapters.settings_api as settings_module  # noqa: E402
import main as plugin_module  # noqa: E402
from core.identity import extract_event_identity  # noqa: E402
from core.retrieval import RecallResult  # noqa: E402
from core.source_ingest import normalize_event  # noqa: E402
from main import NeoMemoryPlugin  # noqa: E402
from storage.cache_store import CacheStore  # noqa: E402
from tests.test_host_probe import FakeContext as HostContext  # noqa: E402
from tests.test_host_probe import FakeEvent as HostEvent  # noqa: E402
from tests.test_providers import FakeContext as ProviderContext  # noqa: E402


class RuntimeContext(HostContext, ProviderContext):
    def __init__(self):
        HostContext.__init__(self)
        ProviderContext.__init__(self)


class RuntimeEvent(HostEvent):
    def __init__(self, *, message_id="query", text="回忆过去", persona="persona-test", extras=None, group_id=""):
        super().__init__(message_id=message_id, text=text, group_id=group_id)
        self.extras = {"persona_id": persona} if persona is not None else {}
        self.extras.update(extras or {})

    def get_extra(self, key, default=None):
        return self.extras.get(key, default)


@pytest_asyncio.fixture
async def plugin_factory(tmp_path, monkeypatch):
    plugins = []
    monkeypatch.setattr(plugin_module, "json_response", lambda value: value)

    async def make(config=None, context=None, directory=None):
        directory = directory or tmp_path / str(len(plugins))
        monkeypatch.setattr(plugin_module.StarTools, "get_data_dir", staticmethod(lambda _name: directory))
        plugin = NeoMemoryPlugin(context or RuntimeContext(), {"embedding_provider_id": "embedding-test", **(config or {})})
        plugins.append(plugin)
        await plugin.initialize()
        return plugin

    yield make
    for plugin in reversed(plugins):
        await plugin.terminate()


async def record(plugin, event, *, observed_at=None):
    source = normalize_event(
        event, persona_id=await plugin._resolve_persona_id(event),
        observed_at=observed_at, resolve_event_persona=False,
    )
    assert source is not None
    await plugin.source_store.capture(source)
    return source


async def seed(plugin, event, *, content="一起准备过旅行。", status="active"):
    source = await record(plugin, event, observed_at=time.time() - 10_000)
    space, _ = await plugin.runtime.scope(event, create=True)
    assert space is not None
    node = await plugin.memory_store.create_node(
        space_id=space["space_id"], node_type="event", content_text=content,
        source_ids=[source.source_id], importance=0.8, status=status,
    )
    return space, node


async def warm(plugin, space):
    await plugin.runtime.recall.index_space(space["space_id"])


def note_items(result):
    return json.loads(result.split("\n", 1)[1])


@pytest.mark.asyncio
async def test_cold_index_does_not_wait_for_model_then_next_request_recalls(plugin_factory):
    plugin = await plugin_factory()
    event = RuntimeEvent()
    space, node = await seed(plugin, event)
    plugin.context.embedding.gate = asyncio.Event()
    request = ProviderRequest(prompt="原始问题")
    # The blocked provider is only touched by a separately scheduled index.
    await asyncio.wait_for(plugin.inject_memory(event, request), timeout=0.5)
    assert request.extra_user_content_parts == []
    await asyncio.wait_for(plugin.context.embedding.started.wait(), timeout=1)
    assert space["space_id"] in plugin.runtime.index_tasks
    assert not plugin.runtime.schedule_index(space["space_id"])
    pending, = await plugin.jobs.list(scope_key=space["space_id"], visible_only=True)
    assert pending["state"] == "running"
    plugin.context.embedding.gate.set()
    await asyncio.gather(*list(plugin.runtime.index_tasks.values()))
    jobs = await plugin.jobs.list(scope_key=space["space_id"])
    assert len(jobs) == 1 and jobs[0]["kind"] == "reindex" and jobs[0]["state"] == "succeeded"
    assert jobs[0]["result"]["indexed"] == 1
    assert jobs[0]["payload"]["request"]["automatic"] is True
    assert not await plugin.jobs.list(scope_key=space["space_id"], visible_only=True)
    await plugin.inject_memory(event, request)
    assert node["content_text"] in request.extra_user_content_parts[0].text
    assert request.extra_user_content_parts[0]._no_save is True


@pytest.mark.asyncio
@pytest.mark.parametrize("group_id", ["", "group-1"])
async def test_manual_user_name_overrides_current_platform_nickname_in_injection(plugin_factory, group_id):
    plugin = await plugin_factory()
    event = RuntimeEvent(group_id=group_id)
    identity = extract_event_identity(event)
    logical = await plugin.source_store.resolve_account(identity.platform_instance_id, identity.account_id)
    space, _ = await seed(plugin, event, content=f"{{{{user:{logical}}}}}准备过旅行。")
    await warm(plugin, space)
    assert await plugin.runtime._current_user_name(space, identity) == {logical: "Alice"}
    await plugin.source_store.rename_logical_user(logical, "共同称呼")
    request = ProviderRequest(prompt="过去的旅行")
    await plugin.inject_memory(event, request)
    text = "\n".join(part.text for part in request.extra_user_content_parts)
    assert "共同称呼" in text
    assert "Alice" not in text


@pytest.mark.asyncio
async def test_recall_is_provider_visible_but_filtered_by_host_history_serializer(plugin_factory):
    plugin = await plugin_factory()
    event = RuntimeEvent()
    space, node = await seed(plugin, event)
    await warm(plugin, space)
    contexts = [{"role": "user", "content": "已有历史"}]
    request = ProviderRequest(prompt="本轮问题", system_prompt="人格设定", contexts=copy.deepcopy(contexts))
    ordinary = TextPart(text="需要正常持久化的内容")
    request.extra_user_content_parts.append(ordinary)
    await plugin.inject_memory(event, request)
    assembled = await request.assemble_context()
    assert node["content_text"] in json.dumps(assembled, ensure_ascii=False)
    persisted = dump_messages_with_checkpoints([Message.model_validate(assembled)])
    serialized = json.dumps(persisted, ensure_ascii=False)
    assert node["content_text"] not in serialized
    assert "需要正常持久化的内容" in serialized and "本轮问题" in serialized
    assert request.contexts == contexts
    assert request.system_prompt == "人格设定" and request.prompt == "本轮问题"


@pytest.mark.asyncio
async def test_repeated_hook_replaces_only_its_own_content_part(plugin_factory):
    plugin = await plugin_factory()
    event = RuntimeEvent()
    space, _ = await seed(plugin, event)
    await warm(plugin, space)
    foreign = TextPart(text="其他插件资料").mark_as_temp()
    request = ProviderRequest(extra_user_content_parts=[foreign])
    await plugin.inject_memory(event, request)
    old = request._neomemory_part
    # Identical content in another plugin's distinct object must survive too.
    same_text_foreign = TextPart(text=old.text).mark_as_temp()
    request.extra_user_content_parts.append(same_text_foreign)
    await plugin.inject_memory(event, request)
    assert len(request.extra_user_content_parts) == 3
    assert request.extra_user_content_parts[0] is foreign
    assert request.extra_user_content_parts[1] is same_text_foreign
    assert request._neomemory_part is not old
    assert all(part is not old for part in request.extra_user_content_parts)
    # A later call with no matching scope removes the prior injection.
    await plugin.inject_memory(RuntimeEvent(persona="another-persona"), request)
    assert request.extra_user_content_parts == [foreign, same_text_foreign]


@pytest.mark.asyncio
async def test_history_queries_use_same_persona_stream_and_exclude_current(plugin_factory, monkeypatch):
    plugin = await plugin_factory()
    event = RuntimeEvent(message_id="current", text="本轮问题")
    space, _ = await seed(plugin, RuntimeEvent(message_id="seed"))
    await warm(plugin, space)
    now = time.time()
    for historical, timestamp in (
        (RuntimeEvent(message_id="right", text="同人格历史"), now - 10),
        (RuntimeEvent(message_id="wrong", text="其他人格秘密", persona="other"), now - 9),
        (RuntimeEvent(message_id="group", text="其他群聊资料", group_id="group-a"), now - 8),
        (RuntimeEvent(message_id="old", text="过期历史"), now - 8000),
        (event, now - 1),
    ):
        await record(plugin, historical, observed_at=timestamp)
    observed = []
    original = plugin.runtime.recall.recall

    async def spy(space_id, query, **kwargs):
        observed.extend(kwargs.get("history", []))
        return await original(space_id, query, **kwargs)

    monkeypatch.setattr(plugin.runtime.recall, "recall", spy)
    await plugin.inject_memory(event, ProviderRequest())
    assert [item["content_text"] for item in observed] == ["同人格历史"]


@pytest.mark.asyncio
async def test_note_tools_reject_cross_space_ids_and_support_archive_restore_literal_search(plugin_factory):
    plugin = await plugin_factory()
    own = RuntimeEvent(message_id="own")
    other = RuntimeEvent(message_id="other", persona="other-persona")
    own_space, _ = await seed(plugin, own)
    await seed(plugin, other)
    created = json.loads(await plugin.notes_tool(own, "create", "Project 100%_complete"))
    note_id = created["note_id"]
    await plugin.notes_tool(own, "create", "普通笔记")
    assert created["status"] == "active"
    for action in ("update", "archive", "restore"):
        response = await plugin.notes_tool(other, action, content="恶意覆盖", note_id=note_id)
        assert "找不到" in response
    assert note_items(await plugin.notes_tool(other, "list")) == []
    found = note_items(await plugin.notes_tool(own, "list", query="%_"))
    assert [item["note_id"] for item in found] == [note_id]
    assert json.loads(await plugin.notes_tool(own, "archive", note_id=note_id))["status"] == "archived"
    assert note_items(await plugin.notes_tool(own, "list", query="project")) == []
    assert note_items(await plugin.notes_tool(own, "list", query="project", status="archived"))[0]["status"] == "archived"
    assert len(note_items(await plugin.notes_tool(own, "list", status="all"))) == 2
    assert json.loads(await plugin.notes_tool(own, "restore", note_id=note_id))["status"] == "active"
    await plugin.notes_tool(own, "update", content="已修正笔记", note_id=note_id)
    stored = await plugin.memory_store.list_notes(space_id=own_space["space_id"])
    assert next(item for item in stored if item["note_id"] == note_id)["content_text"] == "已修正笔记"


@pytest.mark.asyncio
async def test_active_tool_only_returns_current_scope_and_can_include_historical(plugin_factory):
    plugin = await plugin_factory()
    own = RuntimeEvent(message_id="own")
    other = RuntimeEvent(message_id="other", persona="other-persona")
    own_space, own_node = await seed(plugin, own, content="本人格历史经历", status="historical")
    other_space, other_node = await seed(plugin, other, content="其他人格私密经历")
    await warm(plugin, own_space)
    await warm(plugin, other_space)
    result = await plugin.recall_tool(own, "经历")
    assert own_node["content_text"] in result
    assert other_node["content_text"] not in result
    request = ProviderRequest()
    await plugin.inject_memory(own, request)
    assert request.extra_user_content_parts == []


@pytest.mark.asyncio
async def test_passive_deadline_and_provider_failure_leave_original_request_usable(plugin_factory):
    plugin = await plugin_factory({"recall_deadline_seconds": 0.1})
    event = RuntimeEvent()
    space, _ = await seed(plugin, event)
    await warm(plugin, space)
    foreign = TextPart(text="other context")
    request = ProviderRequest(prompt="question", extra_user_content_parts=[foreign])
    plugin.context.embedding.gate = asyncio.Event()
    await asyncio.wait_for(plugin.inject_memory(event, request), timeout=0.5)
    assert request.extra_user_content_parts == [foreign]
    assert request.prompt == "question"
    assert plugin.context.embedding.active == 0
    plugin.context.embedding.gate.set()
    plugin.context.embedding.failure = ConnectionError("private provider response must not escape")
    await plugin.inject_memory(event, request)
    assert request.extra_user_content_parts == [foreign]
    assert "private provider response" not in json.dumps(plugin.runtime.diagnostics)
    assert plugin.ready


@pytest.mark.asyncio
async def test_cancelling_request_propagates_and_does_not_break_later_recall(plugin_factory):
    plugin = await plugin_factory()
    event = RuntimeEvent()
    space, node = await seed(plugin, event)
    await warm(plugin, space)
    plugin.context.embedding.started.clear()
    plugin.context.embedding.gate = asyncio.Event()
    task = asyncio.create_task(plugin.inject_memory(event, ProviderRequest()))
    await asyncio.wait_for(plugin.context.embedding.started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert plugin.context.embedding.active == 0
    plugin.context.embedding.gate.set()
    request = ProviderRequest()
    await plugin.inject_memory(event, request)
    assert node["content_text"] in request.extra_user_content_parts[0].text


@pytest.mark.asyncio
async def test_cache_initialization_failure_preserves_source_capture(plugin_factory, monkeypatch):
    async def fail_cache(_self):
        raise OSError("synthetic cache failure with sensitive detail")

    monkeypatch.setattr(CacheStore, "open", fail_cache)
    plugin = await plugin_factory()
    assert plugin.ready and plugin.runtime.cache is None and plugin.runtime.recall is None
    await plugin._capture_user_event(RuntimeEvent(message_id="survives", text="原始内容仍记录"))
    assert (await plugin.source_store.health())["source_count"] == 1
    health = await plugin.api_health()
    assert health["recall"]["diagnostics"]["cache"] == "OSError"
    assert "sensitive detail" not in json.dumps(health)
    request = ProviderRequest(prompt="still usable")
    await plugin.inject_memory(RuntimeEvent(), request)
    assert request.extra_user_content_parts == []


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["min", "max"])
async def test_all_exposed_numeric_boundaries_can_initialize_runtime(plugin_factory, boundary):
    schema = json.loads((Path(__file__).resolve().parents[1] / "_conf_schema.json").read_text())
    values = {name: metadata["slider"][boundary] for name, metadata in schema.items() if "slider" in metadata}
    plugin = await plugin_factory(values)
    assert plugin.ready
    assert plugin.runtime.recall is not None and plugin.runtime.cache is not None
    assert (await plugin.runtime.cache.health())["schema_version"] == 1


@pytest.mark.asyncio
async def test_persona_resolver_applies_host_default_and_conversation_override(plugin_factory):
    context = RuntimeContext()
    calls = []

    async def resolve(**kwargs):
        calls.append(kwargs)
        return kwargs["conversation_persona_id"] or "host-default", None, None, None

    context.persona_manager = SimpleNamespace(resolve_selected_persona=resolve)
    plugin = await plugin_factory(context=context)
    event = RuntimeEvent(persona=None)
    assert await plugin._resolve_persona_id(event) == "host-default"
    assert calls[-1]["conversation_persona_id"] is None
    assert await plugin._resolve_persona_id(event, conversation=SimpleNamespace(persona_id="session-persona")) == "session-persona"
    assert calls[-1]["conversation_persona_id"] == "session-persona"
    assert calls[-1]["platform_name"] == "test"


@pytest.mark.asyncio
@pytest.mark.parametrize("trace,expected", [("traced-persona", "traced-persona"), ("[%None]", None)])
async def test_trace_persona_is_authoritative_even_when_default_or_explicit_exists(plugin_factory, trace, expected):
    context = RuntimeContext()

    async def must_not_resolve(**kwargs):
        pytest.fail("host trace must not be replaced by a fresh persona selection")

    context.persona_manager = SimpleNamespace(resolve_selected_persona=must_not_resolve)
    plugin = await plugin_factory(context=context)
    event = RuntimeEvent(persona="explicit-other", extras={"_trace_persona_id": trace})
    assert await plugin._resolve_persona_id(event) == expected


@pytest.mark.asyncio
async def test_intermediate_trace_none_uses_resolver_and_keeps_source_eligible_for_history(plugin_factory):
    context = RuntimeContext()
    calls = []

    async def resolve(**kwargs):
        calls.append(kwargs)
        return "resolved-host-default", None, None, None

    context.persona_manager = SimpleNamespace(resolve_selected_persona=resolve)
    plugin = await plugin_factory(context=context)
    # AstrBot first writes None, awaits persona resolution, and only then writes
    # the effective trace ID. Capture may run during that await.
    event = RuntimeEvent(persona=None, extras={"_trace_persona_id": None})
    await plugin._capture_user_event(event)
    rows = await plugin.source_store.list_records()
    assert calls and len(rows) == 1
    assert rows[0]["persona_id"] == "resolved-host-default"
    assert rows[0]["identity_pending"] == 0
    history = await plugin.source_store.recent_conversation(
        rows[0]["stream_id"], "resolved-host-default",
        since=time.time() - 60, until=time.time() + 1,
    )
    assert [item["source_id"] for item in history] == [rows[0]["source_id"]]


@pytest.mark.asyncio
@pytest.mark.parametrize("persona", [None, "[%None]", "default", "_chatui_default_"])
async def test_unresolved_persona_stays_pending_but_unsupported_is_skipped(plugin_factory, persona):
    plugin = await plugin_factory()
    event = RuntimeEvent(persona=persona)
    assert await plugin._resolve_persona_id(event) is None
    await plugin._capture_user_event(event)
    records = await plugin.source_store.list_records()
    if persona is None:
        assert len(records) == 1 and records[0]["identity_pending"] == 1
        assert records[0]["persona_id"] is None
    else:
        assert records == []
    assert (await plugin.runtime.scope(event, create=True))[0] is None
    assert "尚无可用记忆空间" in await plugin.notes_tool(event, "create", "不能绑定未知人格")


@pytest.mark.asyncio
async def test_note_source_ref_points_to_the_stable_original_user_record(plugin_factory):
    plugin = await plugin_factory()
    event = RuntimeEvent(message_id="stable-note-source", text="请记下这个约定")
    source = await record(plugin, event)
    response = json.loads(await plugin.notes_tool(event, "create", "稍后继续讨论计划"))
    space, _ = await plugin.runtime.scope(event)
    notes = await plugin.memory_store.list_notes(space_id=space["space_id"])
    note = next(item for item in notes if item["note_id"] == response["note_id"])
    assert note["source_ref"] == source.source_id
    assert await plugin.source_store.has_source(note["source_ref"])


@pytest.mark.asyncio
async def test_note_without_message_id_uses_stream_and_turn_metadata_without_false_source_ref(plugin_factory):
    plugin = await plugin_factory()
    event = RuntimeEvent(message_id="", text="没有平台消息 ID 的请求")
    await record(plugin, event)
    response = json.loads(await plugin.notes_tool(event, "create", "保留本轮备注"))
    space, _ = await plugin.runtime.scope(event)
    note = next(item for item in await plugin.memory_store.list_notes(space_id=space["space_id"])
                if item["note_id"] == response["note_id"])
    assert note["source_ref"] is None
    assert note["metadata"]["stream_id"] == extract_event_identity(event).stream_id
    assert note["metadata"]["turn_id"] == plugin.runtime.turn_id(event)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["passive", "active", "notes"])
async def test_runtime_close_cancels_its_foreground_work_without_cancelling_host_call(
    plugin_factory, monkeypatch, operation
):
    plugin = await plugin_factory()
    event = RuntimeEvent()
    space, _ = await seed(plugin, event)
    await warm(plugin, space)
    runtime = plugin.runtime
    started = asyncio.Event()
    gate = asyncio.Event()
    cancelled = asyncio.Event()
    request = ProviderRequest(prompt="宿主正常继续")
    if operation == "notes":
        original = plugin.memory_store.create_note

        async def pending_note(**kwargs):
            started.set()
            try:
                await gate.wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise
            return await original(**kwargs)

        monkeypatch.setattr(plugin.memory_store, "create_note", pending_note)
        coroutine = plugin.notes_tool(event, "create", "关闭后不能写入")
    else:
        plugin.context.embedding.started.clear()
        plugin.context.embedding.gate = gate
        started = plugin.context.embedding.started
        coroutine = (plugin.inject_memory(event, request) if operation == "passive"
                     else plugin.recall_tool(event, "等待中的主动问题"))
    host_call = asyncio.create_task(coroutine)
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        await asyncio.wait_for(runtime.close(), timeout=1)
        await asyncio.wait_for(host_call, timeout=0.5)
        assert not host_call.cancelled()
        assert runtime.closing and runtime.cache is None
        assert not runtime.tasks
        assert not runtime.foreground_tasks
        assert request.extra_user_content_parts == []
        if operation == "notes":
            assert cancelled.is_set()
        else:
            assert plugin.context.embedding.active == 0
        gate.set()
        await asyncio.sleep(0)
        assert await plugin.memory_store.list_notes(space_id=space["space_id"]) == []
        assert request.extra_user_content_parts == []
    finally:
        host_call.cancel()
        await asyncio.gather(host_call, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["active", "notes"])
async def test_host_cancelling_a_tool_still_propagates_cancellation(plugin_factory, monkeypatch, operation):
    plugin = await plugin_factory()
    event = RuntimeEvent()
    space, _ = await seed(plugin, event)
    await warm(plugin, space)
    started = asyncio.Event()
    cancelled = asyncio.Event()
    if operation == "notes":
        async def pending_note(**kwargs):
            started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        monkeypatch.setattr(plugin.memory_store, "create_note", pending_note)
        coroutine = plugin.notes_tool(event, "create", "宿主已取消不能写入")
    else:
        plugin.context.embedding.started.clear()
        plugin.context.embedding.gate = asyncio.Event()
        started = plugin.context.embedding.started
        coroutine = plugin.recall_tool(event, "等待中的主动查询")
    host_call = asyncio.create_task(coroutine)
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        host_call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await host_call
        assert plugin.runtime.closing is False
        assert not plugin.runtime.foreground_tasks
        assert plugin.context.embedding.active == 0
        if operation == "notes":
            assert cancelled.is_set()
        assert await plugin.memory_store.list_notes(space_id=space["space_id"]) == []
    finally:
        host_call.cancel()
        await asyncio.gather(host_call, return_exceptions=True)


@pytest.mark.asyncio
async def test_model_api_lists_safe_metadata_and_probes_without_writing_memories(plugin_factory, monkeypatch):
    plugin = await plugin_factory()
    await plugin.maintenance.stop()
    models = await plugin.api_models()
    assert {item['provider_id'] for item in models['embedding']} == {'embedding-test'}
    assert all(set(item) <= {'provider_id', 'model', 'provider_class', 'dimension'}
               for items in models.values() for item in items)

    async def payload(**kwargs):
        return {'kind': 'embedding', 'provider_id': 'embedding-test', 'idempotency_key': 'probe'}

    monkeypatch.setattr(settings_module, 'request', SimpleNamespace(json=payload))
    response = await plugin.api_test_model()
    result = json.loads(response.body)
    assert await plugin.jobs.list() == []
    assert result['ok'] is True and result['dimension'] > 0
    assert (await plugin.memory_store.health())['node_count'] == 0
    assert (await plugin.source_store.health())['source_count'] == 0


@pytest.mark.asyncio
async def test_model_probe_does_not_return_provider_error_bodies(plugin_factory, monkeypatch):
    plugin = await plugin_factory()
    await plugin.maintenance.stop()

    async def payload(**kwargs):
        return {'kind': 'embedding', 'provider_id': 'embedding-test', 'idempotency_key': 'probe-failure'}

    async def fail(*args, **kwargs):
        raise RuntimeError('sensitive-provider-response-must-stay-private')

    monkeypatch.setattr(settings_module, 'request', SimpleNamespace(json=payload))
    monkeypatch.setattr(plugin.runtime.adapter, 'embed_batch', fail)
    response = await plugin.api_test_model()
    assert response.status_code == 502
    assert await plugin.jobs.list() == []
    detail = json.loads(response.body)
    assert detail['data']['error_code'] == 'model_test_provider_failed'
    assert 'sensitive-provider-response' not in json.dumps(detail)
    assert 'sensitive-provider-response' not in json.dumps(await plugin.api_health())


@pytest.mark.asyncio
async def test_cache_close_failure_still_releases_authoritative_stores(plugin_factory, monkeypatch):
    plugin = await plugin_factory()
    runtime, source, memory = plugin.runtime, plugin.source_store, plugin.memory_store
    original = runtime.close

    async def fail_close():
        await original()
        raise RuntimeError('synthetic cache close failure')

    monkeypatch.setattr(runtime, 'close', fail_close)
    with pytest.raises(RuntimeError, match='synthetic cache close failure'):
        await plugin.terminate()
    assert plugin.runtime is None and plugin.memory_store is None and plugin.source_store is None
    for store in (source, memory):
        with pytest.raises(RuntimeError, match='not open'):
            _ = store.database.connection


@pytest.mark.asyncio
async def test_strength_configuration_applies_to_new_work_without_rewriting_memory(plugin_factory):
    plugin = await plugin_factory()
    space, item = await seed(plugin, RuntimeEvent())
    old_recall = plugin.runtime.recall
    stores = (plugin.source_store, plugin.memory_store, plugin.jobs, plugin.runtime.cache)
    await plugin.apply_config(replace(plugin.config, strength_importance_decay_weight=2,
        strength_time_rate=0, strength_activity_loss_per_round=0, strength_wake_margin=0.02,
        reinforcement_gain=0.1, reinforcement_cooldown_seconds=0,
        reinforcement_max_per_turn=0.08, reinforcement_max_total=0.1))
    new_recall = plugin.runtime.recall
    assert new_recall is not old_recall
    assert new_recall.policy.strength_policy.importance_decay_weight == 2
    assert new_recall.policy.strength_policy.wake_margin == 0.02
    assert old_recall.policy.strength_policy.reinforcement_gain == 0.02
    assert (plugin.source_store, plugin.memory_store, plugin.jobs, plugin.runtime.cache) == stores
    assert await plugin.memory_store.get_node(item["node_id"]) == item
    result = RecallResult(space_id=space["space_id"], generation_id=item["generation_id"],
                          hits=[{**item, "direct": True}])
    # A retained request uses its original policy; a new request uses the live values.
    await old_recall.reinforce(result, "old-work")
    first = await plugin.memory_store.get_node(item["node_id"])
    assert first["reinforcement"] == pytest.approx(0.004)
    await new_recall.reinforce(result, "new-work")
    second = await plugin.memory_store.get_node(item["node_id"])
    assert second["reinforcement"] == pytest.approx(0.0236)
    assert second["strength_reset_at"] == item["strength_reset_at"]


@pytest.mark.asyncio
async def test_recall_history_tracks_final_injection_and_active_result_while_preview_is_read_only(plugin_factory):
    plugin = await plugin_factory()
    event = RuntimeEvent(text="和旅行有关的事")
    space, node = await seed(plugin, event)
    space_id = space["space_id"]
    history = plugin.runtime.recall_history
    assert not history.enabled
    await history.set_enabled(True)

    cold = await plugin.runtime.test_recall(space_id, event.get_message_str())
    assert not cold["items"] and cold["reason"]
    assert cold["index_coverage"] == {"total": 1, "usable": 0, "truncated": False}
    assert await plugin.jobs.list() == []
    await warm(plugin, space)
    before = await plugin.memory_store.get_node(node["node_id"])
    preview = await plugin.runtime.test_recall(space_id, event.get_message_str())
    assert preview["mode"] == "test" and [row["node_id"] for row in preview["items"]] == [node["node_id"]]
    assert await plugin.memory_store.get_node(node["node_id"]) == before
    assert await history.list(space_id) == []

    req = ProviderRequest()
    await plugin.inject_memory(event, req)
    passive, = await history.list(space_id)
    assert passive["query"] == event.get_message_str() and passive["mode"] == "passive"
    assert passive["text"] == req.extra_user_content_parts[0].text
    assert [row["node_id"] for row in passive["items"]] == [node["node_id"]]
    response = await plugin.recall_tool(event, "旅行")
    active, previous = await history.list(space_id)
    assert active["mode"] == "active" and active["text"] == response and previous == passive
    assert [row["node_id"] for row in active["items"]] == [node["node_id"]]
    await history.set_enabled(False)
    await plugin.recall_tool(event, "旅行")
    assert await history.list(space_id) == []

@pytest.mark.asyncio
async def test_recall_recording_survives_plugin_restart_and_disable_clears_on_disk(plugin_factory, tmp_path):
    directory = tmp_path / "persistent"
    first = await plugin_factory(directory=directory)
    space, _ = await seed(first, RuntimeEvent())
    space_id = space["space_id"]
    history = first.runtime.recall_history
    await history.set_enabled(True)
    ticket = history.ticket()
    snapshot = {"mode": "passive", "query": "周末", "text": "同一次召回的完整内容"}
    await history.append(ticket, space_id, snapshot)
    await first.terminate()

    second = await plugin_factory(directory=directory)
    restored = second.runtime.recall_history
    assert restored.enabled and await restored.list(space_id) == [snapshot]
    await restored.set_enabled(True)
    assert restored.ticket() == ticket
    await restored.set_enabled(False)
    assert not restored.enabled and await restored.list(space_id) == []
    await second.terminate()

    third = await plugin_factory(directory=directory)
    after_clear = third.runtime.recall_history
    assert not after_clear.enabled and await after_clear.list(space_id) == []
    await after_clear.set_enabled(True)
    await after_clear.append(ticket, space_id, snapshot)
    assert await after_clear.list(space_id) == []
