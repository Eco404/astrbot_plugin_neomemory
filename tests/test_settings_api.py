"""Host bridge endpoints preserve strict contracts and safe error responses."""

import asyncio
import json
from types import SimpleNamespace

import pytest

import adapters.settings_api as module
from adapters.settings_api import SettingsAPI
from core.config import NeoMemoryConfig
from core.settings import SettingsService


class HostConfig(dict):
    async def save_config_async(self, changes):
        self.update(changes)
        return True


@pytest.fixture
def api(monkeypatch):
    plugin = SimpleNamespace(_host_config=HostConfig(), config=NeoMemoryConfig(), ready=False, terminating=False)
    async def apply_config(config):
        plugin.config = config
    plugin.apply_config = apply_config
    plugin.settings = SettingsService(plugin)
    result = SettingsAPI(plugin)
    result.test_body = None
    async def body(**kwargs):
        return result.test_body
    monkeypatch.setattr(module, "request", SimpleNamespace(json=body))
    return result


def payload(response, status=200):
    assert response.status_code == status, response.body
    return json.loads(response.body)


@pytest.mark.asyncio
async def test_routes_register_without_new_http_server(api):
    registrations = []
    api.register(SimpleNamespace(register_web_api=lambda *args: registrations.append(args)))
    assert {(row[0], tuple(row[2])) for row in registrations} == {
        ("/astrbot_plugin_neomemory/settings", ("GET",)),
        ("/astrbot_plugin_neomemory/settings", ("POST",)),
        ("/astrbot_plugin_neomemory/appearance", ("GET",)),
        ("/astrbot_plugin_neomemory/appearance", ("POST",)),
        ("/astrbot_plugin_neomemory/text-cleaning", ("GET",)),
        ("/astrbot_plugin_neomemory/text-cleaning", ("POST",)),
        ("/astrbot_plugin_neomemory/models/probe", ("POST",)),
        ("/astrbot_plugin_neomemory/space/policy", ("GET",)),
        ("/astrbot_plugin_neomemory/space/policy", ("POST",)),
    }
    api.register(SimpleNamespace())


@pytest.mark.asyncio
async def test_settings_remain_available_when_plugin_is_disabled(api):
    state = payload(await api.get())
    api.test_body = {"revision": state["revision"], "changes": {"recall_enabled": False}}
    saved = payload(await api.save())
    assert saved["applied"] and not saved["reload_required"] and saved["values"]["recall_enabled"] is False
    assert saved["runtime_values"]["recall_enabled"] is False
    assert payload(await api.save(), 409)["data"]["error_code"] == "settings_or_model_test_conflict"
    assert payload(await api.probe(), 503)["data"]["error_code"] == "model_test_unavailable"


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [None, [], {}, {"revision": "x", "changes": {}, "extra": 1},
    {"revision": "x", "changes": {}}, {"revision": "x" * 64, "changes": {"user_weight": float("nan")}}])
async def test_invalid_requests_return_safe_400(api, body):
    api.test_body = body
    assert payload(await api.save(), 400)["data"]["error_code"] == "invalid_settings_or_model_request"


@pytest.mark.asyncio
async def test_persistence_error_has_no_exception_body(api):
    async def fail(changes):
        raise OSError("Bearer never-expose-this")
    api.plugin._host_config.save_config_async = fail
    state = payload(await api.get())
    api.test_body = {"revision": state["revision"], "changes": {"recall_enabled": False}}
    result = payload(await api.save(), 500)
    assert result["data"]["error_code"] == "settings_save_failed"
    assert "never-expose-this" not in json.dumps(result)


@pytest.mark.asyncio
async def test_apply_error_exposes_saved_state_and_allows_retry(api):
    original_apply = api.plugin.apply_config

    async def fail(config):
        raise RuntimeError("private-model-details")

    api.plugin.apply_config = fail
    state = payload(await api.get())
    api.test_body = {"revision": state["revision"], "changes": {"llm_provider_id": "new-provider"}}
    failed = payload(await api.save(), 500)
    assert failed["data"]["error_code"] == "settings_apply_failed" and not failed["data"]["applied"]
    assert "private-model-details" not in json.dumps(failed)
    pending = payload(await api.get())
    assert pending["values"]["llm_provider_id"] == "new-provider"
    assert pending["runtime_values"]["llm_provider_id"] == ""
    assert not pending["applied"] and pending["apply_error"] == "settings_apply_failed"
    api.plugin.apply_config = original_apply
    api.test_body = {"revision": pending["revision"], "changes": {}}
    retried = payload(await api.save())
    assert retried["applied"] and not retried["reload_required"]
    assert retried["runtime_values"]["llm_provider_id"] == "new-provider"


@pytest.mark.asyncio
async def test_probe_returns_connectivity_result(api):
    api.plugin.ready = True
    async def probe(*, kind, provider_id):
        return {"ok": True, "kind": kind, "provider_id": provider_id}
    api.plugin.model_tests = SimpleNamespace(probe=probe)
    api.test_body = {"kind": "llm", "provider_id": "test"}
    assert payload(await api.probe()) == {"ok": True, **api.test_body}


@pytest.mark.asyncio
async def test_cancellation_is_not_hidden_in_error_response(api):
    async def cancelled():
        raise asyncio.CancelledError()
    api.plugin.settings.get = cancelled
    with pytest.raises(asyncio.CancelledError):
        await api.get()


@pytest.mark.asyncio
async def test_text_cleaning_revision_validation_and_restart(api, tmp_path):
    from core.cleaning_settings import CleaningSettings
    from core.text_cleaning import default_cleaning_config
    from storage.memory_store import MemoryStore
    from storage.pipeline_store import PipelineStore

    store = MemoryStore(tmp_path / "memory.db")
    await store.open()
    try:
        plugin = api.plugin
        sources = SimpleNamespace()
        plugin.ready = True
        plugin.cleaning_settings = CleaningSettings(PipelineStore(store), sources)
        await plugin.cleaning_settings.load()
        initial = payload(await api.get_cleaning())
        assert initial == default_cleaning_config()
        rule = dict(id="tag", name="去标记", kind="literal", pattern="[隐藏]", replacement="",
                    enabled=True, group_id=None)
        api.test_body = dict(revision=initial["revision"], rules=[rule], groups=[])
        saved = payload(await api.save_cleaning())
        assert saved["revision"] == initial["revision"] + 1
        payload(await api.save_cleaning(), 409)
        for invalid in (dict(rule, group_id="missing"), dict(rule, kind="companion"),
                        dict(rule, kind="regex", pattern="("),
                        dict(rule, kind="regex", pattern="(a)", replacement=r"\2")):
            api.test_body = dict(saved, rules=[invalid])
            payload(await api.save_cleaning(), 400)
        assert payload(await api.get_cleaning()) == saved
        await store.close()
        await store.open()
        plugin.cleaning_settings = CleaningSettings(PipelineStore(store), sources)
        await plugin.cleaning_settings.load()
        assert payload(await api.get_cleaning()) == saved
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_space_policy_returns_only_visible_sources_with_console_labels(api, tmp_path):
    from core.memory_spaces import group_space, private_space
    from storage.console_store import ConsoleStore
    from storage.memory_store import MemoryStore

    memory = MemoryStore(tmp_path / "memory.db")
    await memory.open()
    try:
        api.plugin.ready = True
        api.plugin.memory_store = memory
        api.plugin.console_store = ConsoleStore(memory)
        private = await memory.get_or_create_space(private_space("p", "u"))
        group = await memory.get_or_create_space(group_space("p", "adapter", "77"))
        await memory.get_or_create_space(group_space("another", "adapter", "77"))
        module.request.query = {"space_id": private["space_id"]}
        assert payload(await api.get_space_policy())["shared_spaces"] == []
        api.test_body = {"space_id": private["space_id"], "shared_recall_enabled": True}
        sources = payload(await api.save_space_policy())["shared_spaces"]
        assert sources == [{"space_id": group["space_id"], "display_label": "p · 群 77"}]
        assert payload(await api.get_space_policy())["shared_spaces"] == sources
        module.request.query = {"space_id": group["space_id"]}
        assert payload(await api.get_space_policy())["shared_spaces"] == []
    finally:
        await memory.close()
