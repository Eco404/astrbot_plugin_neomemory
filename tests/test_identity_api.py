import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

pytest.importorskip("astrbot.api.web")

import adapters.identity_api as module
from core.memory_spaces import group_space, private_space
from tests.test_identity_management import source
from tests.test_recall_runtime import plugin_factory


def payload(response, status=200):
    assert response.status_code == status, response.body
    return json.loads(response.body)


@pytest.mark.asyncio
async def test_initialized_plugin_identity_routes_complete_background_binding(plugin_factory, monkeypatch):
    plugin = await plugin_factory()
    await plugin.source_store.bind_account(platform_instance_id="test", account_id="a", logical_user_id="A")
    await plugin.source_store.bind_account(platform_instance_id="test", account_id="b", logical_user_id="B")
    space = await plugin.memory_store.get_or_create_space(group_space("p", "test", "group"))
    for account, persona, group in (("a", "p", "group"), ("b", "p", "group"),
                                    ("c", "other", "group"), ("d", "p", "elsewhere")):
        raw = source(account, account)
        await plugin.source_store.capture(replace(raw, persona_id=persona, identity=replace(raw.identity,
            message_type="GroupMessage", group_id=group, unified_msg_origin="test:GroupMessage:" + group)))
    handlers = {route.rsplit("/astrbot_plugin_neomemory/", 1)[-1]: handler
                for route, handler, _, _ in plugin.context.registered_web_apis}
    query, body = {}, {}

    async def request_body(**kwargs):
        return body

    monkeypatch.setattr(module, "request", SimpleNamespace(query=query, json=request_body))
    assert (await handlers["identities"]()).status_code == 400
    query["space_id"] = space["space_id"]
    assert {u["logical_user_id"] for u in payload(await handlers["identities"]())["items"]} == {"A", "B"}
    body.update(space_id=query["space_id"], logical_user_id="A", display_name="Named user")
    renamed = payload(await handlers["identities/rename"]())
    assert renamed["display_name"] == "Named user" and renamed["display_name_source"] == "manual"
    body.clear()
    body.update(space_id=query["space_id"], action="bind", logical_user_id="A", platform_instance_id="test", account_id="b")
    preview = payload(await handlers["identities/preview"]())
    assert preview["delete_previous_user"]
    assert "confirmation_token" in preview
    body.update(confirmation_token=preview["confirmation_token"], idempotency_key="api-bind")
    submitted = payload(await handlers["identities/apply"]())
    assert submitted["kind"] == "identity" and submitted["scope_key"] == "*"
    for _ in range(100):
        job = await plugin.jobs.get(submitted["job_id"])
        if job["state"] in {"succeeded", "failed"}:
            break
        await asyncio.sleep(0.01)
    assert job["state"] == "succeeded", job
    assert await plugin.source_store.resolve_account("test", "b") == "A"
    assert len(payload(await handlers["identities"]())["items"]) == 1
    assert payload(await handlers["identities/apply"]())["job_id"] == submitted["job_id"]
    assert (await plugin.api_bind_account()).status_code == 409
    await plugin.maintenance.clear_completed(scope_key='*')
    assert await plugin.source_store.database.fetchall('SELECT * FROM identity_operation_receipts') == []
    assert await plugin.source_store.resolve_account('test', 'b') == 'A'


@pytest.mark.asyncio
async def test_identity_api_last_account_confirmation_and_missing_fields(plugin_factory, monkeypatch):
    plugin = await plugin_factory()
    await plugin.source_store.bind_account(platform_instance_id="test", account_id="a", logical_user_id="A")
    api = module.IdentityAPI(plugin)
    space = await plugin.memory_store.get_or_create_space(private_space("p", "A"))
    body = {"space_id": space["space_id"], "action": "unbind", "logical_user_id": "A", "platform_instance_id": "test", "account_id": "a"}

    async def request_body(**kwargs):
        return body

    monkeypatch.setattr(module, "request", SimpleNamespace(query={}, json=request_body))
    error = payload(await api.preview(), 409)
    assert error["data"]["error_code"] == "last_account"
    body["account_id"] = "new"
    body["action"] = "bind"
    assert "confirmation_token" in payload(await api.preview())
    error = payload(await api.apply(), 400)
    assert error["data"]["error_code"] == "invalid_identity_request"
    assert await plugin.jobs.list() == []


@pytest.mark.asyncio
async def test_private_space_limits_target_and_allows_explicit_transfer(plugin_factory, monkeypatch):
    plugin = await plugin_factory()
    for user in ("A", "B"):
        await plugin.source_store.bind_account(platform_instance_id="test", account_id=user.lower(), logical_user_id=user)
    first = await plugin.memory_store.get_or_create_space(private_space("one", "A"))
    second = await plugin.memory_store.get_or_create_space(private_space("two", "A"))
    previous = await plugin.memory_store.get_or_create_space(private_space("other", "B"))
    api = module.IdentityAPI(plugin)
    query, body = {"space_id": first["space_id"]}, {}

    async def request_body(**kwargs):
        return body

    monkeypatch.setattr(module, "request", SimpleNamespace(query=query, json=request_body))
    assert [row["logical_user_id"] for row in payload(await api.identities())["items"]] == ["A"]
    body.update(**query, logical_user_id="B", display_name="outside")
    payload(await api.rename(), 404)
    del body["display_name"]
    body.update(action="bind", platform_instance_id="test", account_id="b")
    assert payload(await api.preview(), 409)["data"]["error_code"] == "identity_outside_space"
    body["logical_user_id"] = "A"
    preview = payload(await api.preview())
    assert preview["delete_previous_user"]
    assert preview["previous_private_spaces"] == [{"space_id": previous["space_id"], "persona_id": "other"}]
    body.update(confirmation_token=preview["confirmation_token"], idempotency_key="transfer", space_id=second["space_id"])
    assert payload(await api.apply(), 409)["data"]["error_code"] == "identity_confirmation_changed"
    assert await plugin.source_store.resolve_account("test", "b") == "B"
    body["space_id"] = first["space_id"]
    job = payload(await api.apply())
    for _ in range(100):
        job = await plugin.jobs.get(job["job_id"])
        if job["state"] in {"succeeded", "failed"}:
            break
        await asyncio.sleep(0.01)
    assert job["state"] == "succeeded"
    assert await plugin.source_store.resolve_account("test", "b") == "A"
    assert await plugin.memory_store.get_space(previous["space_id"]) is None


@pytest.mark.asyncio
async def test_preview_new_configured_instance_finds_owner_before_any_messages(plugin_factory, monkeypatch):
    from tests.test_identity_management import qq_source
    plugin = await plugin_factory()
    store = plugin.source_store
    existing = await store.capture(qq_source("first", "10005", "msg"))
    await store.capture(qq_source("second", "10005", "another"))
    await store.bind_account(platform_instance_id="telegram-bot", account_id="42", logical_user_id="A")
    space = await plugin.memory_store.get_or_create_space(private_space("p", "A"))
    old_space = await plugin.memory_store.get_or_create_space(private_space("p", existing.logical_user_id))
    monkeypatch.setattr(plugin.context, "get_platform_inst",
                        lambda instance: SimpleNamespace(meta=lambda: SimpleNamespace(name="aiocqhttp"))
                        if instance == "new-bot" else None, raising=False)
    body = {"space_id": space["space_id"], "logical_user_id": "A", "platform_instance_id": "new-bot",
            "account_id": "10005", "action": "bind"}

    async def request_body(**kwargs):
        return body

    api = module.IdentityAPI(plugin)
    monkeypatch.setattr(module, "request", SimpleNamespace(json=request_body))
    preview = payload(await api.preview())
    assert preview["previous_user"]["logical_user_id"] == existing.logical_user_id
    assert preview["account"]["platform_instance_ids"] == ["first", "new-bot", "second"]
    assert preview["previous_private_spaces"] == [{"space_id": old_space["space_id"], "persona_id": "p"}]
    assert preview["delete_previous_user"]
    assert not await store.database.fetchone("SELECT 1 FROM account_bindings WHERE platform_instance_id='new-bot'")
    body.update(confirmation_token=preview["confirmation_token"], idempotency_key="confirm-new")
    job = payload(await api.apply())
    for _ in range(100):
        job = await plugin.jobs.get(job["job_id"])
        if job["state"] in {"succeeded", "failed"}:
            break
        await asyncio.sleep(0.01)
    assert job["state"] == "succeeded"
    for instance in ("first", "second", "new-bot"):
        assert await store.resolve_account(instance, "10005") == "A"
