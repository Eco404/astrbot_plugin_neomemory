"""Delivered companion segments through ingress, replay, and real SQLite stores."""
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
