"""Delivered companion segments through ingress, replay, and real SQLite stores."""
import asyncio
import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from adapters.external_memory import COMPANION_PLUGIN_ID, ExternalMemoryAdapter, PROVIDER_API_VERSION
from storage.source_scan import SourceScanner
from tests.test_external_memory import companion_context
from tests.test_maintenance_engine import environment, enqueue_and_claim, execute
from tests.test_recall_runtime import plugin_factory


def receipt(sequence, timestamp, text="周末去看展。"):
    return {"schema": PROVIDER_API_VERSION, "source_plugin": COMPANION_PLUGIN_ID,
            "kind": "proactive_message", "memory_id": f"segment-{sequence}", "context": companion_context(),
            "content": text, "occurred_at": datetime.fromtimestamp(timestamp, timezone.utc).isoformat(),
            "metadata": {"delivery_status": "delivered", "host_persona_id": "p", "host_conversation_id": "cid",
                         "contact_id": "contact", "sequence": sequence, "delivery_id": f"d-{sequence}"}}


@pytest.mark.asyncio
async def test_first_message_can_be_delivered_and_duplicate_ids_require_identical_scope_and_content(plugin_factory):
    plugin = await plugin_factory()
    record = receipt(0, 120)
    adapter = plugin.external_memory
    record["context"]["principal"]["native_account_id"] = "different-trigger-account"
    source_id = await adapter.write(record)
    assert source_id == adapter.delivered_source_id(record)
    assert await adapter.accept_delivery(record, historical=True) == source_id
    for key, value in (("content", "另一条"), ("occurred_at", receipt(0, 121)["occurred_at"])):
        with pytest.raises(ValueError, match="conflicts"):
            await adapter.write({**record, key: value})
    bad_meta = {**record["metadata"], "host_persona_id": "other"}
    with pytest.raises(ValueError, match="conflicts"):
        await adapter.write({**record, "metadata": bad_meta})
    spec = await adapter.delivery_space(record)
    store = plugin.source_store
    source, = await store.get_records((source_id,))
    assert spec.logical_user_id == source["conversation_logical_user_id"]
    assert (source["actor_kind"], source["actor_logical_user_id"], source["observed_at"]) == ("agent", None, 120)
    assert [item["source_id"] for item in (await SourceScanner(store).page(spec)).records] == [source_id]
    kwargs = dict(since=0, until=150, host_conversation_id="cid", logical_user_id=spec.logical_user_id)
    history = await store.recent_conversation(source["stream_id"], "p", **kwargs)
    assert history[0]["delivered_proactive"] and history[0]["proactive_contact_id"] == "contact"
    assert not await store.recent_conversation(source["stream_id"], "other", **kwargs)
    await store.bind_account(platform_instance_id="qq", account_id="user-1", logical_user_id="new-person")
    assert (await adapter.delivery_space(record)).logical_user_id == spec.logical_user_id
    later = receipt(1, datetime.now(timezone.utc).timestamp())
    assert (await adapter.delivery_space(later)).logical_user_id == "new-person"
    grouped = {**record, "memory_id": "group-segment", "context": companion_context(kind="group")}
    await adapter.write(grouped)
    assert (await adapter.delivery_space(grouped)).scope_key == "persona:p:platform:qq:group:group-1"
    assert (await store.health())["source_count"] == 2


@pytest.mark.asyncio
async def test_rebuild_interleaves_backfill_and_retains_live_cursor(environment):
    env = environment
    # Fixture has ordinary observations at 100 and 200. Place older confirmed
    # segments on both sides to exercise multiple windows and a frozen range.
    adapter = ExternalMemoryAdapter(SimpleNamespace(context=SimpleNamespace(), source_store=env.sources))
    for index, stamp in enumerate((175, 90, 150, 110)):
        src = adapter._delivery(receipt(index, stamp), historical=True)
        identity = replace(src.identity, platform_instance_id="platform", account_id="u",
                           unified_msg_origin="platform:u", session_id="u")
        await env.sources.capture(replace(src, identity=identity))
    spec = env.space
    normal = await env.engine.scanner.page(spec)
    assert [row["observed_at"] for row in normal.records] == [100, 200]
    job = await enqueue_and_claim(env, "rebuild")
    assert job["payload"]["total"] == 6
    pages = []
    for offset in (0, 2, 4):
        page = await env.engine.scanner.replay_page(spec, job["payload"]["highwater"], offset, limit=2)
        pages.extend(page.records)
    assert [r["observed_at"] for r in pages] == [90, 100, 110, 150, 175, 200]
    result = await execute(env, job)
    assert result["state"] == "succeeded", result
    assert result["result"]["source_count"] == 6
    generation = await env.memory.get_active_generation(spec["space_id"])
    assert await env.engine.pipeline.get_cursor(generation["generation_id"]) == 6
    assert await env.engine.pending_source_count(spec) == 0


def contact_part(sequence, text, *, count=4, contact="visit"):
    item = receipt(sequence, 100 + sequence, text)
    item["memory_id"] = f"{contact}-part-{sequence}"
    item["metadata"].update(contact_id=contact, part_count=count, delivery_id=item["memory_id"],
                            platform_message_id=f"platform-{sequence}")
    return item


@pytest.mark.asyncio
async def test_contact_is_one_source_across_restart_concurrent_push_and_replay(plugin_factory, tmp_path):
    from adapters.proactive_context import missing_deliveries

    config = {"automatic_extraction_enabled": False}
    plugin = await plugin_factory(config, directory=tmp_path)
    parts = [contact_part(i, text) for i, text in enumerate(("甲", "乙\n乙", "丙", "丁"))]
    source_id = await plugin.external_memory.write(parts[2])
    assert await plugin.external_memory.write(parts[0]) == source_id
    assert (await plugin.source_store.health())["source_count"] == 0
    await plugin.terminate()

    plugin = await plugin_factory(config, directory=tmp_path)
    adapter = plugin.external_memory
    assert await asyncio.gather(*(adapter.write(parts[i]) for i in (1, 2, 3))) == [source_id] * 3
    assert [await adapter.write(item) for item in parts] == [source_id] * 4
    row, = await plugin.source_store.get_records((source_id,))
    assert row["content_text"] == "甲\n乙\n乙\n丙\n丁"
    assert row["observed_at"] == 103
    metadata = json.loads(row["metadata_json"])
    assert [(part["delivery_sequence"], part["platform_message_id"], part["observed_at"])
            for part in metadata["delivery_parts"]] == [(i, f"platform-{i}", 100 + i) for i in range(4)]
    spec = await adapter.delivery_space(parts[0])
    space = await plugin.memory_store.get_or_create_space(spec)
    assert await plugin.maintenance.pending_source_count(space) == 1
    scanner = SourceScanner(plugin.source_store)
    replay = await scanner.replay_page(space, await scanner.highwater(space), 0)
    assert [item["content_text"] for item in replay.records] == [row["content_text"]]
    history = await plugin.source_store.recent_conversation(row["stream_id"], "p", since=0, until=200)
    assert missing_deliveries(history, [{"role": "assistant", "content": item["content"]} for item in parts]) == []
    assert missing_deliveries(history, [{"role": "assistant", "content": row["content_text"]}]) == []


@pytest.mark.asyncio
async def test_partial_contact_publishes_only_confirmed_text_and_merges_late_receipts(plugin_factory):
    from core.proactive_delivery import PART_WAIT_SECONDS
    from tests.test_completion_host import _controlled_ingress

    plugin = await plugin_factory()
    await _controlled_ingress(plugin)
    clock = [100.]
    plugin.ingress_store.clock = lambda: clock[0]
    adapter = plugin.external_memory
    second = contact_part(1, "已送达")
    source_id = await adapter.write(second)
    clock[0] += PART_WAIT_SECONDS - 1
    assert await adapter.write(second) == source_id
    assert await plugin.ingress.process_once() is None
    clock[0] += 1
    assert (await plugin.ingress.process_once()).source_id == source_id
    row, = await plugin.source_store.get_records((source_id,))
    assert row["content_text"] == "已送达"
    assert await adapter.write(contact_part(0, "补到的前段")) == source_id
    clock[0] += PART_WAIT_SECONDS
    await plugin.ingress.process_once()
    row, = await plugin.source_store.get_records((source_id,))
    assert row["content_text"] == "补到的前段\n已送达"
    assert (await plugin.source_store.health())["source_count"] == 1
    assert await adapter.write(contact_part(0, "另一轮", count=1, contact="next")) != source_id
    assert (await plugin.source_store.health())["source_count"] == 2


@pytest.mark.asyncio
async def test_existing_per_segment_source_survives_source_format_upgrade_and_replay(plugin_factory, tmp_path):
    plugin = await plugin_factory(directory=tmp_path)
    item = contact_part(0, "原有分段")
    legacy = plugin.external_memory._delivery(item)
    await plugin.source_store.capture(legacy)
    await plugin.terminate()
    with sqlite3.connect(tmp_path / "source.db") as database:
        database.execute("DROP TABLE proactive_receipt_sources")
        database.execute("PRAGMA user_version=1")

    plugin = await plugin_factory(directory=tmp_path)
    assert await plugin.external_memory.write(item) == legacy.source_id
    assert (await plugin.source_store.health())["source_count"] == 1
    assert (await plugin.source_store.get_records((legacy.source_id,)))[0]["content_text"] == "原有分段"


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["private", "group"])
async def test_standard_companion_write_reaches_extraction_once(plugin_factory, kind):
    from unittest.mock import AsyncMock
    from core.memory_spaces import group_space, private_space

    plugin = await plugin_factory({"external_memory_enabled": True, "automatic_extraction_enabled": False})
    ctx = companion_context(kind=kind, role="companion-internal-role")
    ctx["scope"]["conversation_id"] = "astrbot-conversation:original:v2-role:opaque"
    ctx["principal"]["native_account_id"] = "scheduler"
    message_type = "FriendMessage" if kind == "private" else "GroupMessage"
    umo = f"qq:{message_type}:session-a"
    conversation = SimpleNamespace(user_id=umo, persona_id=None)
    lookup = AsyncMock(return_value=conversation)
    plugin.context.conversation_manager.get_conversation = lookup
    resolver = AsyncMock(return_value=("host-persona", None, None, False))
    plugin.context.persona_manager = SimpleNamespace(resolve_selected_persona=resolver)
    body = "第一行\n第二行<<PRIVATE_COMPANION_SPLIT>>第三行\n第四行"
    item = {"schema": PROVIDER_API_VERSION, "source_plugin": COMPANION_PLUGIN_ID,
            "kind": "proactive_message", "context": ctx, "memory_id": "proactive:whole",
            "occurred_at": "2026-11-24T10:00:00+08:00", "content": f"我主动说过：{body}",
            "metadata": {"contact_id": "contact", "clean_visible_text": body}}
    writer = plugin.external_memory.write
    source_id = await writer(item)
    assert source_id
    lookup.assert_awaited_once_with(umo, "original")
    resolver.assert_awaited_once_with(umo=umo, conversation_persona_id=None, platform_name="")
    row, = await plugin.source_store.get_records((source_id,))
    assert (row["source_type"], row["actor_kind"], row["persona_id"], row["actor_account_id"]) == (
        "conversation", "agent", "host-persona", "bot-account")
    assert row["content_text"] == "第一行\n第二行\n第三行\n第四行"
    assert row["observed_at"] == datetime.fromisoformat(item["occurred_at"]).timestamp()
    meta = json.loads(row["metadata_json"])
    assert meta["host_conversation_id"] == "original" and meta["delivery_basis"] == "provider_write"
    if kind == "private":
        owner = await plugin.source_store.resolve_account("qq", "user-1")
        assert owner and meta["recipient_logical_user_id"] == owner
        spec = private_space("host-persona", owner)
    else:
        spec = group_space("host-persona", "qq", "group-1")
    discovered = await SourceScanner(plugin.source_store).discover_spaces()
    assert discovered.spaces == (spec,)
    space = await plugin.memory_store.get_or_create_space(spec)
    assert await plugin.maintenance.pending_source_count(space) == 1
    assert [x["source_id"] for x in (await SourceScanner(plugin.source_store).page(spec)).records] == [source_id]
    resolver.return_value = ("another-persona", None, None, False)
    assert await writer(item) == source_id
    assert (await plugin.source_store.health())["source_count"] == 1
    assert await writer({**item, "memory_id": "empty", "metadata": {"clean_visible_text": "&&angry&&"}}) is None

    # A removed host window cannot send new content to the now-selected persona.
    lookup.return_value = None
    unresolved_id = await writer({**item, "memory_id": "another-contact"})
    unresolved, = await plugin.source_store.get_records((unresolved_id,))
    assert unresolved["source_type"] == "source_event"
    assert await plugin.maintenance.pending_source_count(space) == 1


@pytest.mark.asyncio
async def test_standard_write_replay_preserves_first_queued_message(plugin_factory, monkeypatch):
    from unittest.mock import AsyncMock
    from tests.test_completion_host import _controlled_ingress

    plugin = await plugin_factory({"automatic_extraction_enabled": False})
    await _controlled_ingress(plugin)
    plugin.context.persona_manager = SimpleNamespace(
        resolve_selected_persona=AsyncMock(return_value=("first", None, None, False)))
    item = {"schema": PROVIDER_API_VERSION, "source_plugin": COMPANION_PLUGIN_ID, "kind": "proactive_message",
            "memory_id": "same", "content": "一条推送", "context": companion_context()}
    with monkeypatch.context() as patch:
        patch.setattr(plugin.ingress, "process_once", AsyncMock(return_value=None))
        source_id = await plugin.external_memory.write(item)
        plugin.context.persona_manager.resolve_selected_persona.return_value = ("second", None, None, False)
        assert await plugin.external_memory.write(item) == source_id
        assert (await plugin.ingress_store.health())["pending_count"] == 1
    await plugin.ingress.process_once()
    stored, = await plugin.source_store.get_records((source_id,))
    assert stored["persona_id"] == "first" and stored["content_text"] == item["content"]
