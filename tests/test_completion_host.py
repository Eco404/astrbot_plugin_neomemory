"""Completion-phase behavior at the real AstrBot plugin boundary."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

pytest.importorskip("astrbot.api")

from astrbot.api.provider import LLMResponse, ProviderRequest  # noqa: E402

from core.memory_spaces import private_space  # noqa: E402
from tests.test_delivered_extraction import receipt  # noqa: E402
from tests.test_recall_runtime import RuntimeContext, RuntimeEvent  # noqa: E402
from tests.test_recall_runtime import plugin_factory as _plugin_factory  # noqa: E402
from tests.test_source_store import make_source  # noqa: E402

plugin_factory = _plugin_factory


@pytest.mark.asyncio
@pytest.mark.parametrize("persona", ["default", "_chatui_default_", "[%None]"])
async def test_unsupported_persona_trace_event_and_conversation_skip_all_capture_and_scope(plugin_factory, persona):
    context = RuntimeContext()
    plugin = await plugin_factory({"embedding_provider_id": "", "automatic_extraction_enabled": False}, context=context)
    for origin in ("trace", "event", "conversation"):
        event = RuntimeEvent(message_id=f"skip-{origin}", persona=persona if origin == "event" else None,
                             extras={"_trace_persona_id": persona} if origin == "trace" else {})
        if origin == "conversation":
            async def current_id(*args):
                return "conversation"

            async def conversation(*args):
                return SimpleNamespace(cid="conversation", persona_id=persona)

            context.conversation_manager = SimpleNamespace(
                get_curr_conversation_id=current_id, get_conversation=conversation)
        await plugin._capture_user_event(event)
        await plugin._capture_agent_response(event, LLMResponse(role="assistant", completion_text="不会被记忆", id=origin))
        assert getattr(event, "_neomemory_skip_persona", False)
        assert not hasattr(event, "_neomemory_source_id")
        assert await plugin.runtime.scope(event, create=True) == (None, None)
        request = ProviderRequest(prompt="本轮问题")
        await plugin.inject_memory(event, request)
        assert not request.extra_user_content_parts
    source = replace(make_source("direct-default"), persona_id=persona)
    await plugin._persist_source(source)  # A direct internal caller must also skip.
    assert (await plugin.source_store.health())["source_count"] == 0
    assert (await plugin.ingress.health())["pending_count"] == 0
    assert (await plugin.memory_store.health())["space_count"] == 0
    assert not plugin._failed_sources
    assert not context.calls
    assert not context.embedding.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("persona", ["default", "_chatui_default_", "[%None]"])
async def test_all_old_default_background_kinds_finish_as_skipped_without_model_calls(plugin_factory, persona):
    plugin = await plugin_factory({"embedding_provider_id": "", "automatic_extraction_enabled": False})
    await plugin.maintenance.stop()
    space = await plugin.memory_store.get_or_create_space(private_space(persona, "legacy-logical-user"))
    for kind in ("extract", "rebuild", "reindex", "governance", "reject", "short_term"):
        job = await plugin.jobs.submit(kind, space["space_id"], {}, f"legacy-{kind}")
        claimed = await plugin.jobs.claim("legacy-worker", lease_seconds=60)
        assert claimed["job_id"] == job["job_id"]
        await plugin.maintenance.runner._execute(claimed)
        result = await plugin.jobs.get(job["job_id"])
        assert result["state"] == "succeeded", (kind, result["error_code"])
        assert result["result"] == {"skipped": True, "reason": "unsupported_persona"}
    assert not plugin.context.calls
    assert not plugin.context.embedding.calls
    assert (await plugin.memory_store.health())["node_count"] == 0
    assert (await plugin.maintenance.automatic_status(space["space_id"]))["state"] == "skipped"


async def _controlled_ingress(plugin):
    """Keep the real writer API while advancing its queue explicitly in tests."""
    poller = plugin.ingress._task
    if poller is not None:
        poller.cancel()
        await asyncio.gather(poller, return_exceptions=True)
        plugin.ingress._task = None
    await plugin.maintenance.stop()


@pytest.mark.asyncio
async def test_full_durable_queue_emergency_records_do_not_get_overtaken_when_capacity_recovers(plugin_factory, monkeypatch):
    plugin = await plugin_factory({"ingress_max_entries": 1, "embedding_provider_id": "", "automatic_extraction_enabled": False})
    await _controlled_ingress(plugin)
    original = plugin.source_store.capture

    async def temporary_failure(source):
        raise OSError("synthetic unavailable storage")

    monkeypatch.setattr(plugin.source_store, "capture", temporary_failure)
    first, second, third, newest = [make_source(f"ordered-{number}", content=f"message {number}") for number in range(4)]
    await plugin._persist_source(first)
    await plugin._persist_source(second)
    await plugin._persist_source(third)
    assert [row["source_id"] for row in await plugin.ingress.list()] == [first.source_id]
    assert list(plugin._failed_sources) == [second.source_id, third.source_id]
    assert await plugin.source_store.list_records() == []
    monkeypatch.setattr(plugin.source_store, "capture", original)
    await plugin.ingress.retry(first.source_id)
    await plugin.ingress.process_once(source_id=first.source_id)
    # A new live message triggers admission of the older volatile predecessors.
    await plugin._persist_source(newest)
    records = await plugin.source_store.list_records(stream_id=first.identity.stream_id)
    assert [row["source_id"] for row in reversed(records)] == [first.source_id, second.source_id, third.source_id, newest.source_id]
    assert [row["sequence_no"] for row in reversed(records)] == [1, 2, 3, 4]
    assert not plugin._failed_sources
    assert (await plugin.ingress.health())["pending_count"] == 0
    assert plugin._unpersisted_overflow == 0


@pytest.mark.asyncio
async def test_emergency_backlog_is_readmitted_without_new_host_message(plugin_factory, monkeypatch):
    plugin = await plugin_factory({"embedding_provider_id": "", "automatic_extraction_enabled": False})
    await _controlled_ingress(plugin)
    first, second = make_source("emergency-first"), make_source("emergency-second")
    enqueue = plugin.ingress_store.enqueue

    async def offline(*args, **kwargs):
        raise OSError("queue offline")

    monkeypatch.setattr(plugin.ingress_store, "enqueue", offline)
    for source in (first, second):
        await plugin._persist_source(source)
    assert len(plugin._failed_sources) == 2
    monkeypatch.setattr(plugin.ingress_store, "enqueue", enqueue)
    await plugin.ingress.start()
    async with asyncio.timeout(2):
        while plugin._failed_sources:
            await asyncio.sleep(.01)
    records = await plugin.source_store.list_records(stream_id=first.identity.stream_id)
    assert [record["source_id"] for record in reversed(records)] == [first.source_id, second.source_id]


@pytest.mark.asyncio
async def test_delivery_does_not_overtake_unpersisted_chat_in_same_stream(plugin_factory, monkeypatch):
    plugin = await plugin_factory({"automatic_extraction_enabled": False})
    await _controlled_ingress(plugin)
    record = receipt(1, 150)
    delivery = plugin.external_memory._delivery(record)
    earlier = replace(make_source("original"), identity=replace(delivery.identity, message_id="original"))
    with monkeypatch.context() as patch:
        async def offline(*args, **kwargs):
            raise OSError("offline")

        patch.setattr(plugin.ingress_store, "enqueue", offline)
        await plugin._persist_source(earlier)
        with pytest.raises(RuntimeError, match="ingress_persistence_unavailable"):
            await plugin.external_memory.write(record)
    assert [*plugin._failed_sources] == [earlier.source_id]
    assert await plugin.external_memory.write(record) == delivery.source_id
    assert await plugin.external_memory.write(record) == delivery.source_id
    history = await plugin.source_store.list_records(stream_id=earlier.identity.stream_id)
    assert [row["source_id"] for row in reversed(history)] == [earlier.source_id, delivery.source_id]
    assert not plugin._failed_sources


@pytest.mark.asyncio
async def test_reload_persists_emergency_source_if_inbox_recovered(plugin_factory, monkeypatch, tmp_path):
    config = {"automatic_extraction_enabled": False, "ingress_max_entries": 1}
    plugin = await plugin_factory(config, directory=tmp_path)
    await _controlled_ingress(plugin)
    sources = [make_source(f"survives-reload-{number}") for number in range(2)]
    with monkeypatch.context() as patch:
        async def offline(*args, **kwargs):
            raise OSError("offline")

        patch.setattr(plugin.ingress_store, "enqueue", offline)
        for source in sources:
            await plugin._persist_source(source)
    assert [*plugin._failed_sources] == [source.source_id for source in sources]
    await plugin.terminate()
    assert not plugin._failed_sources
    restarted = await plugin_factory(config, directory=tmp_path)
    async with asyncio.timeout(2):
        while not await restarted.source_store.has_source(sources[-1].source_id):
            await asyncio.sleep(.01)
    records = await restarted.source_store.list_records(stream_id=sources[0].identity.stream_id)
    assert [row["source_id"] for row in reversed(records)] == [source.source_id for source in sources]


@pytest.mark.asyncio
async def test_emergency_overflow_is_visible_and_never_evicts_earliest_unpersisted_record(plugin_factory, monkeypatch):
    plugin = await plugin_factory({"ingress_max_entries": 1, "embedding_provider_id": "", "automatic_extraction_enabled": False})
    await _controlled_ingress(plugin)
    plugin._max_failed_sources = 1
    original = plugin.source_store.capture

    async def temporary_failure(source):
        raise OSError("synthetic unavailable storage")

    monkeypatch.setattr(plugin.source_store, "capture", temporary_failure)
    first, second, overflow = [make_source(f"overflow-{number}") for number in range(3)]
    for source in (first, second, overflow):
        await plugin._persist_source(source)
    assert list(plugin._failed_sources) == [second.source_id]
    assert plugin._unpersisted_overflow == 1
    assert await plugin.ingress.get(overflow.source_id) is None
    assert not await plugin.source_store.has_source(overflow.source_id)
    health = await plugin.api_health()
    assert health["ingress"]["unpersisted_count"] == 1
    assert health["ingress"]["unpersisted_overflow"] == 1
    monkeypatch.setattr(plugin.source_store, "capture", original)
    await plugin.ingress.retry(first.source_id)
    await plugin.ingress.process_once(source_id=first.source_id)
    assert await plugin.retry_failed_sources() == 1
    assert await plugin.source_store.has_source(second.source_id)
    assert not await plugin.source_store.has_source(overflow.source_id)
    assert plugin._unpersisted_overflow == 1
