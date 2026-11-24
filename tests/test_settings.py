"""Strict edits, safe persistence and concurrency with the real host config."""

import asyncio
import json
from types import SimpleNamespace

import pytest

from core.config import NeoMemoryConfig
from core.settings import (
    SettingsApplyError,
    SettingsConflict,
    SettingsSaveError,
    SettingsService,
    SettingsUnavailable,
)
from core.short_term import ShortTermService


class HostConfig(dict):
    def __init__(self, values=None):
        super().__init__(values or {})
        self.saves = []
        self.entered = asyncio.Event()
        self.proceed = None
        self.fail = False

    async def save_config_async(self, changes):
        self.update(changes)
        self.entered.set()
        if self.proceed:
            await self.proceed.wait()
        if self.fail:
            raise OSError("secret-upstream-error")
        self.saves.append(dict(self))
        return True


def service(host=None):
    host = host if host is not None else HostConfig()
    plugin = SimpleNamespace(_host_config=host, config=NeoMemoryConfig.from_mapping(host),
                             ready=False, terminating=False, applications=[])

    async def apply_config(config):
        plugin.applications.append(config)
        plugin.config = config

    plugin.apply_config = apply_config
    return SettingsService(plugin)


@pytest.mark.asyncio
async def test_save_applies_runtime_and_unknown_values_stay_private():
    host = HostConfig({"future_secret": "do-not-expose"})
    item = service(host)
    state = await item.get()
    assert state["can_save"] and state["applied"] and not state["reload_required"]
    assert "do-not-expose" not in json.dumps(state)
    assert state["schema"]["recall_enabled"]["group"] == "功能"
    assert state["schema"]["history_limit"]["group"] == "高级"
    assert state["schema"]["short_term_provider_id"]["group"] == "model"
    saved = await item.save(state["revision"], {"recall_enabled": False, "history_limit": 12})
    assert saved["applied"] and not saved["reload_required"] and saved["values"]["recall_enabled"] is False
    assert saved["runtime_values"]["recall_enabled"] is False
    assert saved["values"] == saved["runtime_values"]
    assert host["future_secret"] == "do-not-expose"
    assert saved["revision"] != state["revision"]
    # Resetting exactly one field preserves every other saved edit.
    reset = await item.save(saved["revision"], {"history_limit": saved["schema"]["history_limit"]["default"]})
    assert reset["values"]["recall_enabled"] is False and reset["values"]["history_limit"] == 10
    assert reset["applied"] and len(item.plugin.applications) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [
    {}, {"unknown": 1}, {"recall_enabled": 1}, {"recall_enabled": "true"}, {"history_limit": True},
    {"history_limit": 1.0}, {"history_limit": -1}, {"history_limit": 51},
    {"user_weight": float("inf")}, {"user_weight": float("nan")}, {"user_weight": True},
    {"short_term_provider_id": 3}, {"short_term_provider_id": " x "}, {"embedding_revision": "x" * 257},
    {"private_min_messages": 25}, {"group_topic_messages": 201}, {"short_term_max_messages": 2},
    {"external_memory_mode": "invalid"}, {"external_memory_mode": ""}, {"external_memory_mode": "OWNER"},
    {"context_budget_priority": "invalid"}, {"context_entry_order": "invalid"},
])
async def test_rejects_invalid_values_instead_of_silently_clamping(changes):
    item = service()
    before = await item.get()
    with pytest.raises(ValueError):
        await item.save(before["revision"], changes)
    assert await item.get() == before
    assert not item._host().saves
    assert not item.plugin.applications


@pytest.mark.asyncio
async def test_external_memory_owner_mode_applies_without_normalizing_saved_value():
    item = service()
    before = await item.get()
    assert before["schema"]["external_memory_mode"]["options"] == ["supplement", "owner"]
    after = await item.save(before["revision"], {"external_memory_mode": "owner"})
    assert after["applied"] and not after["reload_required"]
    assert after["values"]["external_memory_mode"] == "owner"
    assert after["values"] == after["runtime_values"]
    assert item.plugin.config.external_memory_mode == "owner"
    assert item._host().saves[-1]["external_memory_mode"] == "owner"


@pytest.mark.asyncio
async def test_strength_parameters_are_advanced_and_can_reset_one_field_live():
    item = service()
    before = await item.get()
    names = [name for name in before["schema"] if name.startswith(("strength_", "reinforcement_"))]
    assert len(names) == 14
    assert all(before["schema"][name]["group"] == "高级" for name in names)
    saved = await item.save(before["revision"], {
        "strength_importance_decay_weight": 2.0, "strength_wake_margin": 0.02,
        "reinforcement_gain": 0.05, "reinforcement_max_per_turn": 0.08,
    })
    assert saved["applied"] and not saved["reload_required"]
    reset = await item.save(saved["revision"], {
        "strength_importance_decay_weight": saved["schema"]["strength_importance_decay_weight"]["default"],
    })
    assert reset["values"]["strength_importance_decay_weight"] == 1
    assert reset["values"]["strength_wake_margin"] == 0.02
    assert reset["values"]["reinforcement_gain"] == 0.05
    assert reset["values"] == reset["runtime_values"]
    with pytest.raises(ValueError):
        await item.save(reset["revision"], {"reinforcement_max_total": 0.21})


@pytest.mark.asyncio
async def test_combined_threshold_changes_validated_as_one_atomic_patch():
    item = service()
    before = await item.get()
    after = await item.save(before["revision"], {"private_min_messages": 2, "private_topic_messages": 3,
        "private_max_messages": 4, "short_term_min_messages": 1, "short_term_max_messages": 2})
    assert after["values"]["private_max_messages"] == 4
    assert len(item._host().saves) == 1


@pytest.mark.asyncio
async def test_two_clients_with_same_revision_cannot_overwrite_each_other():
    item = service()
    state = await item.get()
    results = await asyncio.gather(item.save(state["revision"], {"history_limit": 11}),
                                   item.save(state["revision"], {"history_limit": 12}), return_exceptions=True)
    assert sum(isinstance(result, SettingsConflict) for result in results) == 1
    assert len(item._host().saves) == 1


@pytest.mark.asyncio
async def test_external_host_changes_invalidate_revision_even_for_unknown_keys():
    item = service()
    state = await item.get()
    item._host()["future_setting"] = 1
    with pytest.raises(SettingsConflict):
        await item.save(state["revision"], {"history_limit": 11})


@pytest.mark.asyncio
async def test_failure_rolls_back_only_our_fields_and_never_leaks_error():
    host = HostConfig({"recall_enabled": True, "history_limit": 10, "unknown": "preserved"})
    host.fail, host.proceed = True, asyncio.Event()
    item = service(host)
    state = await item.get()
    task = asyncio.create_task(item.save(state["revision"], {"recall_enabled": False, "history_limit": 11}))
    await host.entered.wait()
    host["history_limit"] = 13  # An external host edit must survive our failure.
    host["new"] = "also-preserved"
    host.proceed.set()
    with pytest.raises(SettingsSaveError) as error:
        await task
    assert "secret" not in str(error.value)
    assert host == {"recall_enabled": True, "history_limit": 13, "unknown": "preserved", "new": "also-preserved"}
    assert item.plugin.config.recall_enabled and item.plugin.config.history_limit == 10
    assert not item.plugin.applications


@pytest.mark.asyncio
async def test_cancellation_finishes_save_and_application_before_unlocking():
    host = HostConfig()
    host.proceed = asyncio.Event()
    item = service(host)
    state = await item.get()
    task = asyncio.create_task(item.save(state["revision"], {"recall_enabled": False}))
    await host.entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    reader = asyncio.create_task(item.get())
    await asyncio.sleep(0)
    assert not task.done() and not reader.done()
    task.cancel()  # Repeated shutdown/disconnect signals must not cancel the host write.
    await asyncio.sleep(0)
    assert not task.done()
    host.proceed.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    read = await reader
    assert read["values"]["recall_enabled"] is False and read["runtime_values"]["recall_enabled"] is False
    assert read["applied"] and len(item.plugin.applications) == 1
    assert len(host.saves) == 1


@pytest.mark.asyncio
async def test_newer_host_commit_is_not_rolled_back():
    class Superseded(HostConfig):
        async def save_config_async(self, changes):
            self.update(changes)
            self["history_limit"] = 15
            return False
    item = service(Superseded())
    with pytest.raises(SettingsConflict):
        await item.save((await item.get())["revision"], {"history_limit": 11})
    assert item._host()["history_limit"] == 15


@pytest.mark.asyncio
async def test_plain_mapping_is_read_only_and_termination_disables_saves():
    for host in ({}, HostConfig()):
        item = service(host)
        if isinstance(host, HostConfig):
            item.plugin.terminating = True
        state = await item.get()
        assert state["can_save"] is False
        with pytest.raises(SettingsUnavailable):
            await item.save(state["revision"], {"recall_enabled": False})


@pytest.mark.asyncio
async def test_real_astrbot_config_persists_with_async_host_api(tmp_path):
    from astrbot.core.config.astrbot_config import AstrBotConfig
    path = tmp_path / "config.json"
    host = AstrBotConfig(str(path), default_config={**NeoMemoryConfig().as_dict(), "unknown": "preserve"})
    item = service(host)
    result = await item.save((await item.get())["revision"], {"short_term_provider_id": "summary-provider"})
    disk = json.loads(path.read_text(encoding="utf-8-sig"))
    assert disk["short_term_provider_id"] == "summary-provider" and disk["unknown"] == "preserve"
    assert result["runtime_values"]["short_term_provider_id"] == "summary-provider"
    assert result["applied"] and not result["reload_required"]


@pytest.mark.asyncio
async def test_sync_host_fallback_runs_outside_event_loop():
    import threading
    loop_thread = threading.get_ident()
    class SyncConfig(dict):
        def save_config(self, changes):
            assert threading.get_ident() != loop_thread
            self.update(changes)
    item = service(SyncConfig())
    result = await item.save((await item.get())["revision"], {"recall_enabled": False})
    assert result["values"]["recall_enabled"] is False


@pytest.mark.asyncio
async def test_short_term_provider_override_and_fallback_are_part_of_execution_signature():
    seen = []
    adapter = SimpleNamespace(llm_fingerprint=lambda provider_id, revision: seen.append(provider_id) or provider_id)
    config = NeoMemoryConfig.from_mapping({"llm_provider_id": "main", "short_term_provider_id": "summary"})
    item = ShortTermService(None, SimpleNamespace(database=None), None, adapter, config, lambda _: "persona")
    assert item._provider_id() == "summary"
    first = await item._execution({"persona_id": "p"})
    item.config = NeoMemoryConfig.from_mapping({"llm_provider_id": "main"})
    second = await item._execution({"persona_id": "p"})
    assert first[1] != second[1] and seen == ["summary", "main"]


@pytest.mark.asyncio
async def test_failed_save_restores_absent_fields_and_cancelled_save_rolls_back():
    host = HostConfig()
    host.fail, host.proceed = True, asyncio.Event()
    item = service(host)
    task = asyncio.create_task(item.save((await item.get())["revision"], {"recall_enabled": False}))
    await host.entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    host.proceed.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert dict(host) == {}


@pytest.mark.asyncio
async def test_numeric_float_controls_accept_integers_but_do_not_accept_booleans():
    item = service()
    saved = await item.save((await item.get())["revision"], {"user_weight": 0, "ingress_retry_seconds": 1})
    assert saved["values"]["user_weight"] == 0 and saved["values"]["ingress_retry_seconds"] == 1


@pytest.mark.asyncio
async def test_second_host_object_disk_edit_requires_reload_instead_of_overwriting(tmp_path):
    from astrbot.core.config.astrbot_config import AstrBotConfig
    path = tmp_path / "config.json"
    defaults = {**NeoMemoryConfig().as_dict(), "unknown": "preserve"}
    first = AstrBotConfig(str(path), default_config=defaults)
    item = service(first)
    before = await item.get()
    assert not before["external_change"] and before["can_save"]
    second = AstrBotConfig(str(path), default_config=defaults)
    await second.save_config_async({"history_limit": 15, "unknown": "external-edit"})
    # Separate instances do not share in-memory state, so only checking the
    # dict revision would silently restore history_limit=10 on the next save.
    assert first["history_limit"] == 10
    changed = await item.get()
    assert changed["external_change"] and changed["reload_required"] and not changed["can_save"]
    assert changed["revision"] == before["revision"]
    assert "external-edit" not in json.dumps(changed) and str(path) not in json.dumps(changed)
    with pytest.raises(SettingsConflict):
        await item.save(before["revision"], {"recall_enabled": False})
    assert json.loads(path.read_text(encoding="utf-8-sig"))["history_limit"] == 15
    reloaded = service(AstrBotConfig(str(path), default_config=defaults))
    refreshed = await reloaded.get()
    assert not refreshed["external_change"] and refreshed["can_save"]
    assert refreshed["values"]["history_limit"] == 15


@pytest.mark.asyncio
async def test_own_saves_refresh_disk_baseline_and_missing_disk_is_read_only(tmp_path):
    from astrbot.core.config.astrbot_config import AstrBotConfig
    path = tmp_path / "config.json"
    host = AstrBotConfig(str(path), default_config=NeoMemoryConfig().as_dict())
    item = service(host)
    first = await item.save((await item.get())["revision"], {"history_limit": 11})
    assert first["can_save"] and not first["external_change"]
    second = await item.save(first["revision"], {"history_limit": 12})
    assert second["can_save"] and not second["external_change"]
    path.unlink()
    missing = await item.get()
    assert missing["external_change"] and not missing["can_save"]
    with pytest.raises(SettingsConflict):
        await item.save(second["revision"], {"history_limit": 13})
    assert not path.exists()


@pytest.mark.asyncio
async def test_basic_host_schema_preserves_hidden_settings_across_loads(tmp_path):
    from pathlib import Path

    from astrbot.core.config.astrbot_config import AstrBotConfig

    schema = json.loads((Path(__file__).resolve().parents[1] / "_conf_schema.json").read_text())
    assert {key for key, meta in schema.items() if not meta.get("invisible")} == {
        key for key, meta in schema.items() if meta["type"] == "bool"
    } | {"llm_provider_id", "topic_provider_id", "short_term_provider_id", "embedding_provider_id", "rerank_provider_id",
         "llm_concurrency", "embedding_concurrency", "embedding_batch_size", "embedding_revision"}
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"llm_provider_id": "saved-llm", "embedding_provider_id": "saved-embedding",
                               "recall_deadline_seconds": 7.0}))
    host = AstrBotConfig(str(path), schema=schema)
    item = service(host)
    state = await item.get()
    assert state["values"]["llm_provider_id"] == "saved-llm"
    assert state["values"]["embedding_provider_id"] == "saved-embedding"
    assert state["values"]["recall_deadline_seconds"] == 7.0
    assert set(state["schema"]) == set(schema)
    assert all("invisible" not in meta for meta in state["schema"].values())
    changed = await item.save(state["revision"], {"capture_group_messages": False})
    reloaded = AstrBotConfig(str(path), schema=schema)
    assert reloaded["llm_provider_id"] == "saved-llm"
    assert reloaded["embedding_provider_id"] == "saved-embedding"
    assert reloaded["recall_deadline_seconds"] == 7.0
    assert not reloaded["capture_group_messages"] and changed["applied"]


@pytest.mark.asyncio
async def test_saved_configuration_survives_apply_failure_and_empty_patch_retries(tmp_path):
    from astrbot.core.config.astrbot_config import AstrBotConfig

    path = tmp_path / "config.json"
    host = AstrBotConfig(str(path), default_config=NeoMemoryConfig().as_dict())
    item = service(host)
    original_apply = item.plugin.apply_config

    async def fail(config):
        raise RuntimeError("private-upstream-detail")

    item.plugin.apply_config = fail
    before = await item.get()
    with pytest.raises(SettingsApplyError) as error:
        await item.save(before["revision"], {"llm_provider_id": "replacement-provider"})
    assert "private-upstream" not in str(error.value)
    state = await item.get()
    assert state["values"]["llm_provider_id"] == "replacement-provider"
    assert state["runtime_values"]["llm_provider_id"] == ""
    assert state["apply_error"] == "settings_apply_failed" and not state["applied"]
    assert state["can_save"] and not state["external_change"] and not state["reload_required"]
    assert json.loads(path.read_text(encoding="utf-8-sig"))["llm_provider_id"] == "replacement-provider"
    disk_before_retry = path.stat().st_mtime_ns
    item.plugin.apply_config = original_apply
    recovered = await item.save(state["revision"], {})
    assert recovered["applied"] and recovered["apply_error"] is None
    assert recovered["runtime_values"]["llm_provider_id"] == "replacement-provider"
    assert recovered["revision"] == state["revision"]
    assert path.stat().st_mtime_ns == disk_before_retry


@pytest.mark.asyncio
async def test_cancellation_during_application_keeps_lock_until_runtime_switches():
    item = service()
    entered, proceed = asyncio.Event(), asyncio.Event()
    original_apply = item.plugin.apply_config

    async def apply(config):
        entered.set()
        await proceed.wait()
        await original_apply(config)

    item.plugin.apply_config = apply
    state = await item.get()
    saving = asyncio.create_task(item.save(state["revision"], {"llm_concurrency": 4}))
    await entered.wait()
    saving.cancel()
    reader = asyncio.create_task(item.get())
    drain = asyncio.create_task(item.drain())
    await asyncio.sleep(0)
    assert not saving.done() and not reader.done() and not drain.done()
    proceed.set()
    with pytest.raises(asyncio.CancelledError):
        await saving
    result = await reader
    await drain
    assert result["applied"] and result["runtime_values"]["llm_concurrency"] == 4


@pytest.mark.asyncio
async def test_apply_failure_remains_visible_after_request_cancellation():
    item = service()
    entered, proceed = asyncio.Event(), asyncio.Event()

    async def fail(config):
        entered.set()
        await proceed.wait()
        raise RuntimeError("unavailable")

    item.plugin.apply_config = fail
    saving = asyncio.create_task(item.save((await item.get())["revision"], {"history_limit": 11}))
    await entered.wait()
    saving.cancel()
    await asyncio.sleep(0)
    proceed.set()
    with pytest.raises(asyncio.CancelledError):
        await saving
    state = await item.get()
    assert state["values"]["history_limit"] == 11 and state["runtime_values"]["history_limit"] == 10
    assert not state["applied"] and state["apply_error"] == "settings_apply_failed"
