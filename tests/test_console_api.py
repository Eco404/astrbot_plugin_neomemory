import asyncio
import json
from types import SimpleNamespace

import pytest
import pytest_asyncio

pytest.importorskip("astrbot.api.web")

import adapters.console_api as module  # noqa: E402
from adapters.console_api import ConsoleAPI  # noqa: E402
from core.memory_spaces import private_space  # noqa: E402
from storage.console_store import ConsoleStore, NoteService  # noqa: E402
from storage.job_store import JobStore  # noqa: E402
from storage.memory_store import MemoryStore  # noqa: E402


@pytest_asyncio.fixture
async def api(tmp_path, monkeypatch):
    memory = MemoryStore(tmp_path / "memory.db")
    await memory.open()
    jobs = JobStore(memory.database)
    result = ConsoleAPI(SimpleNamespace(ready=True, terminating=False,
        console_store=ConsoleStore(memory), note_service=NoteService(memory, jobs)))
    result.test_space = await memory.get_or_create_space(private_space("p", "u"))
    result.test_query = {"space_id": result.test_space["space_id"]}
    result.test_body = None

    async def body(**kwargs):
        return result.test_body

    monkeypatch.setattr(module, "request", SimpleNamespace(json=body, query=result.test_query))
    try:
        yield result
    finally:
        await memory.close()


def payload(response, status=200):
    assert response.status_code == status, response.body
    return json.loads(response.body)


def test_registration_uses_authenticated_host_bridge(api):
    routes = []
    api.register(SimpleNamespace(register_web_api=lambda *args: routes.append(args)))
    assert len(routes) == 4
    assert {(row[0], tuple(row[2])) for row in routes} == {
        ("/astrbot_plugin_neomemory/memory/context", ("GET",)),
        ("/astrbot_plugin_neomemory/memory/source", ("GET",)),
        ("/astrbot_plugin_neomemory/notes", ("GET",)),
        ("/astrbot_plugin_neomemory/notes", ("POST",)),
    }
    api.register(SimpleNamespace())


@pytest.mark.asyncio
async def test_profile_context_and_empty_note_reads(api):
    store = api.plugin.console_store.memory
    await store.upsert_profile(space_id=api.test_space["space_id"], logical_user_id="u", tags=["coder"], description="Hello")
    api.test_query["kind"] = "profiles"
    result = payload(await api.context())
    assert result["items"][0]["tags"] == ["coder"]
    assert payload(await api.notes())["items"] == []


@pytest.mark.asyncio
async def test_note_submit_is_durable_and_duplicate_is_idempotent(api):
    api.test_body = {"space_id": api.test_space["space_id"], "idempotency_key": "same-click", "action": "create",
                     "content_text": "A note"}
    payload(await api.submit_note(), 400)
    assert await api.plugin.note_service.jobs.list(scope_key=api.test_space["space_id"]) == []
    note = await api.plugin.note_service.memory.create_note(space_id=api.test_space["space_id"], content_text="Initial note")
    api.test_body.update(action="update", note_id=note["note_id"], expected_updated_at=note["updated_at"])
    first = payload(await api.submit_note())
    second = payload(await api.submit_note())
    assert first["job_id"] == second["job_id"]
    assert first["kind"] == "note" and first["state"] == "queued"
    assert "payload" not in first and "content_text" not in json.dumps(first)
    api.test_body["idempotency_key"] = "other-click"
    assert payload(await api.submit_note(), 409)["data"]["error_code"] == "console_conflict"


@pytest.mark.asyncio
async def test_delete_api_runs_as_task_and_repeat_request_keeps_committed_result(api):
    service = api.plugin.note_service
    note = await service.memory.create_note(space_id=api.test_space["space_id"], content_text="合成待删除笔记")
    api.test_body = {"space_id": api.test_space["space_id"], "idempotency_key": "delete-click", "action": "delete",
                     "note_id": note["note_id"], "expected_updated_at": note["updated_at"]}
    queued = payload(await api.submit_note())
    assert queued["kind"] == "note" and queued["state"] == "queued"
    claimed = await service.jobs.claim("api-worker")
    result = await service.run(claimed)
    await service.jobs.finish(claimed["job_id"], claimed["lease_token"], result=result)
    assert result["status"] == "deleted"
    assert payload(await api.notes())["items"] == []
    repeated = payload(await api.submit_note())
    assert repeated["job_id"] == queued["job_id"] and repeated["state"] == "succeeded"
    api.test_body.update(idempotency_key="restore-deleted", action="restore", expected_updated_at=result["updated_at"])
    payload(await api.submit_note(), 404)


@pytest.mark.asyncio
@pytest.mark.parametrize("attribute,value", [("ready", False), ("terminating", True), ("console_store", None), ("note_service", None)])
async def test_unready_endpoints_fail_safely(api, attribute, value):
    setattr(api.plugin, attribute, value)
    for endpoint in (api.context, api.source, api.notes, api.submit_note):
        assert payload(await endpoint(), 503)["data"]["error_code"] == "console_unavailable"


@pytest.mark.asyncio
@pytest.mark.parametrize("query", [{"limit": "101"}, {"limit": True}, {"offset": "-1"}, {"q": "q" * 1001}, {"status": "bad"}])
async def test_listing_rejects_invalid_parameters(api, query):
    api.test_query.update(kind="profiles", **query)
    payload(await api.context(), 400)
    payload(await api.notes(), 400)


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [None, [], {}, {"space_id": "s", "idempotency_key": "k", "action": "create", "content_text": "yes", "sql": "injected"},
    {"space_id": "s", "idempotency_key": "k", "action": "update", "note_id": "n", "content_text": "yes"},
    {"space_id": "s", "idempotency_key": "k", "action": "create", "content_text": "x" * 20001}])
async def test_note_rejects_malformed_commands(api, body):
    api.test_body = body
    payload(await api.submit_note(), 400)


@pytest.mark.asyncio
async def test_unknown_space_and_source_return_404(api):
    api.test_query.update(space_id="unknown", kind="profiles", source_id="not-here")
    payload(await api.context(), 404)
    payload(await api.notes(), 404)
    payload(await api.source(), 404)
    api.test_query["space_id"] = api.test_space["space_id"]
    payload(await api.source(), 404)


@pytest.mark.asyncio
async def test_safe_error_codes_and_cancellation(api, monkeypatch):
    async def fail(*args, **kwargs):
        raise RuntimeError("secret raw body never expose")

    monkeypatch.setattr(api.plugin.console_store, "notes", fail)
    result = payload(await api.notes(), 500)
    assert "secret raw body" not in json.dumps(result)
    assert result["data"]["error_code"] == "console_request_failed"

    async def cancel(*args, **kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(api.plugin.console_store, "notes", cancel)
    with pytest.raises(asyncio.CancelledError):
        await api.notes()
