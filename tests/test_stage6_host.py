"""Console services remain coherent across real host configuration and reloads."""

import asyncio
import json

import pytest

pytest.importorskip("astrbot.api")

from astrbot.core.config.astrbot_config import AstrBotConfig  # noqa: E402

import main as plugin_module  # noqa: E402
from core.config import NeoMemoryConfig  # noqa: E402
from storage.job_store import JobConflict  # noqa: E402
from storage.schema import CURRENT_SCHEMA_VERSION  # noqa: E402
from tests.test_recall_runtime import RuntimeContext, RuntimeEvent, seed  # noqa: E402
from tests.test_recall_runtime import plugin_factory as _plugin_factory  # noqa: E402

plugin_factory = _plugin_factory


@pytest.mark.asyncio
async def test_recall_can_enable_through_real_host_config_without_reload(tmp_path, monkeypatch):
    config_path = tmp_path / "host-config.json"
    defaults = {**NeoMemoryConfig().as_dict(), "recall_enabled": False, "host_unknown_field": "preserved"}
    host_config = AstrBotConfig(str(config_path), default_config=defaults)
    monkeypatch.setattr(plugin_module.StarTools, "get_data_dir", staticmethod(lambda _: tmp_path / "data"))
    plugin = plugin_module.NeoMemoryPlugin(RuntimeContext(), host_config)
    try:
        await plugin.initialize()
        assert plugin.ready and not plugin.config.recall_enabled
        registered = {(path, tuple(methods)) for path, _, methods, _ in plugin.context.registered_web_apis}
        assert ("/astrbot_plugin_neomemory/settings", ("POST",)) in registered
        state = await plugin.settings.get()
        saved = await plugin.settings.save(state["revision"], {"recall_enabled": True, "history_limit": 8})
        assert not saved["reload_required"] and saved["values"]["recall_enabled"]
        assert saved["applied"] and plugin.ready and plugin.config.history_limit == 8
        stored = json.loads(config_path.read_text(encoding="utf-8-sig"))
        assert stored["history_limit"] == 8 and stored["host_unknown_field"] == "preserved"
    finally:
        await plugin.terminate()
    reloaded = plugin_module.NeoMemoryPlugin(RuntimeContext(), AstrBotConfig(str(config_path), default_config=defaults))
    try:
        await reloaded.initialize()
        assert reloaded.ready and reloaded.config.history_limit == 8
        assert not (await reloaded.settings.get())["reload_required"]
        assert reloaded.console_store is not None and reloaded.note_service is not None
        assert {"note", "model_test"} <= set(reloaded.maintenance.runner.handlers)
        assert reloaded.memory_store.schema_version == CURRENT_SCHEMA_VERSION
    finally:
        await reloaded.terminate()


@pytest.mark.asyncio
async def test_shutdown_drains_live_save_before_changing_runtime_lifecycle(tmp_path, monkeypatch):
    host_config = AstrBotConfig(str(tmp_path / "host-config.json"),
        default_config={**NeoMemoryConfig().as_dict(), "automatic_extraction_enabled": False})
    monkeypatch.setattr(plugin_module.StarTools, "get_data_dir", staticmethod(lambda _: tmp_path / "data"))
    plugin = plugin_module.NeoMemoryPlugin(RuntimeContext(), host_config)
    await plugin.initialize()
    entered, proceed = asyncio.Event(), asyncio.Event()
    original_save = host_config.save_config_async
    original_initialize = plugin.initialize
    initializations = []

    async def slow_save(self, changes):
        entered.set()
        await proceed.wait()
        return await original_save(changes)

    async def track_initialize():
        initializations.append(True)
        await original_initialize()

    monkeypatch.setattr(AstrBotConfig, "save_config_async", slow_save)
    monkeypatch.setattr(plugin, "initialize", track_initialize)
    state = await plugin.settings.get()
    saving = asyncio.create_task(plugin.settings.save(state["revision"], {"history_limit": 8}))
    stopping = None
    try:
        await asyncio.wait_for(entered.wait(), 1)
        stopping = asyncio.create_task(plugin.terminate())
        await asyncio.sleep(0)
        assert plugin.terminating and not stopping.done()
        proceed.set()
        saved = await asyncio.wait_for(saving, 2)
        await asyncio.wait_for(stopping, 2)
        assert saved["applied"] and plugin.config.history_limit == 8
        assert not initializations
        assert plugin.source_store is None and plugin.runtime is None
    finally:
        proceed.set()
        await asyncio.gather(saving, return_exceptions=True)
        if stopping is not None:
            await stopping
        else:
            await plugin.terminate()


@pytest.mark.asyncio
async def test_agent_note_write_respects_queued_manual_note_and_releases_after_completion(plugin_factory):
    plugin = await plugin_factory({"automatic_extraction_enabled": False})
    await plugin.maintenance.stop()
    event = RuntimeEvent()
    space, _ = await seed(plugin, event)
    note = await plugin.memory_store.create_note(space_id=space["space_id"], content_text="旧笔记")
    operation = {"action": "update", "note_id": note["note_id"],
                 "expected_updated_at": note["updated_at"], "content_text": "人工修正"}
    queued = await plugin.note_service.enqueue(space["space_id"], "manual-note", operation)
    detail = await plugin.maintenance.describe(queued["job_id"])
    assert detail["candidate"] == operation
    assert "payload" not in detail and "lease_token" not in detail
    for action in ("create", "update"):
        result = await plugin.runtime.notes(event, action, "Agent同时写入", note["note_id"])
        assert "暂时不可用" in result
    unchanged = (await plugin.memory_store.list_notes(space_id=space["space_id"]))[0]
    assert unchanged["content_text"] == "旧笔记"
    job = await plugin.jobs.claim("manual-worker")
    await plugin.maintenance.runner._execute(job)
    assert (await plugin.jobs.get(queued["job_id"]))["state"] == "succeeded"
    corrected = (await plugin.memory_store.list_notes(space_id=space["space_id"]))[0]
    assert corrected["content_text"] == "人工修正"
    assert corrected["updated_at"] > note["updated_at"]
    result = await plugin.runtime.notes(event, "create", "后续独立笔记")
    assert json.loads(result)["status"] == "active"
    with pytest.raises(JobConflict):
        await plugin.note_service.enqueue(space["space_id"], "stale-page", operation)


@pytest.mark.asyncio
async def test_model_probe_legacy_route_and_new_route_share_handler_and_safe_details(plugin_factory):
    plugin = await plugin_factory()
    await plugin.maintenance.stop()
    job = await plugin.model_tests.enqueue(kind="embedding", provider_id="embedding-test", idempotency_key="same")
    again = await plugin.model_tests.enqueue(kind="embedding", provider_id="embedding-test", idempotency_key="same")
    assert job["job_id"] == again["job_id"]
    claimed = await plugin.jobs.claim("worker")
    await plugin.maintenance.runner._execute(claimed)
    detail = await plugin.maintenance.describe(job["job_id"])
    assert detail["state"] == "succeeded" and detail["result"]["ok"]
    assert not {"checkpoint", "payload", "lease_token"} & set(detail)
    assert "vectors" not in detail["result"]
