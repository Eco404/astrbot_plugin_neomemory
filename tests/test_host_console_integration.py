"""Real AstrBot auth/router, request context and persistent console APIs."""

import asyncio
import json

import httpx
import pytest
import pytest_asyncio

pytest.importorskip("astrbot.api")

from tests.host_console_harness import API_PREFIX, HostConsoleHarness  # noqa: E402


@pytest_asyncio.fixture
async def host_console(tmp_path):
    harness = HostConsoleHarness(tmp_path)
    await harness.start()
    try:
        yield harness
    finally:
        await harness.stop()


@pytest.mark.asyncio
async def test_host_scoped_assets_bridge_auth_and_private_api(host_console):
    harness = host_console
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=harness.app), base_url="http://host") as client:
        denied = await client.get(f"{API_PREFIX}/memory/spaces")
        assert denied.status_code == 401
        denied_write = await client.post(f"{API_PREFIX}/notes", json={
            "space_id": harness.space["space_id"], "idempotency_key": "unauthorized-note",
            "action": "create", "content_text": "Must not be stored",
        })
        assert denied_write.status_code == 401
        assert await harness.plugin.memory_store.list_notes(space_id=harness.space["space_id"]) == []
        invalid = await client.get(harness.asset_prefix.replace(harness.asset_token, "invalid") + "app.js")
        assert invalid.status_code == 401
        wrong_page = await client.get(harness.asset_prefix.replace("/views/neomemory/", "/views/other/") + "app.js")
        assert wrong_page.status_code == 401
        for filename in ("", "app.js", "ui.js", "memory.js", "notes.js", "settings.js", "maintenance.js", "task-progress.js", "identities.js", "style.css"):
            response = await client.get(harness.asset_prefix + filename)
            assert response.status_code == 200
            assert response.headers["access-control-allow-origin"] == "*"
            assert response.headers["cache-control"] == "no-store"
        bridge = await client.get("/api/plugin/page/bridge-sdk.js", params={"asset_token": harness.asset_token})
        assert bridge.status_code == 200 and "__setInitialContext" in bridge.text
        assert (await client.get("/api/plugin/page/bridge-sdk.js")).status_code == 401
        denied_api = await client.get(
            "/api/plug/astrbot_plugin_neomemory/memory/spaces", params={"asset_token": harness.asset_token},
        )
        assert denied_api.status_code == 401
        spaces = await client.get(f"{API_PREFIX}/memory/spaces", headers=harness.headers)
        assert spaces.status_code == 200 and len(spaces.json()["items"]) == 2
        versions = await client.get(f"{API_PREFIX}/memory/versions", headers=harness.headers,
                                    params={"space_id": harness.space["space_id"], "node_id": harness.node["node_id"]})
        source_id = versions.json()["sources"][0]["source_id"]
        source = await client.get(f"{API_PREFIX}/memory/source", headers=harness.headers,
                                  params={"space_id": harness.space["space_id"], "source_id": source_id})
        assert source.status_code == 200
        wrong_scope = await client.get(f"{API_PREFIX}/memory/source", headers=harness.headers,
                                       params={"space_id": harness.other_space["space_id"], "source_id": source_id})
        assert wrong_scope.status_code == 404


@pytest.mark.asyncio
async def test_host_http_settings_and_queued_task_survive_runtime_reload(host_console):
    harness = host_console
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=harness.app), base_url="http://host", headers=harness.headers) as client:
        original = (await client.get(f"{API_PREFIX}/settings")).json()
        changed = await client.post(f"{API_PREFIX}/settings", json={"revision": original["revision"], "changes": {"history_limit": 8}})
        assert changed.status_code == 200 and not changed.json()["reload_required"]
        assert changed.json()["applied"] is True
        assert harness.plugin.config.history_limit == 8
        assert json.loads((harness.directory / "config.json").read_text(encoding="utf-8-sig"))["history_limit"] == 8
        stale = await client.post(f"{API_PREFIX}/settings", json={"revision": original["revision"], "changes": {"history_limit": 9}})
        assert stale.status_code == 409
        await harness.plugin.maintenance.runner.stop()
        body = {"space_id": harness.space["space_id"], "idempotency_key": "host-persistent-note", "action": "create", "content_text": "Synthetic persistent note"}
        assert (await client.post(f"{API_PREFIX}/notes", json=body)).status_code == 400
        note = await harness.plugin.memory_store.create_note(space_id=body["space_id"], content_text="Initial note")
        body.update(action="update", note_id=note["note_id"], expected_updated_at=note["updated_at"])
        submitted = await client.post(f"{API_PREFIX}/notes", json=body)
        assert submitted.status_code == 200
        job_id = submitted.json()["job_id"]
        assert (await client.post(f"{API_PREFIX}/notes", json=body)).json()["job_id"] == job_id
        await harness.reload()
        assert harness.plugin.config.history_limit == 8
        for _ in range(100):
            job = await harness.plugin.jobs.get(job_id)
            if job["state"] == "succeeded":
                break
            await asyncio.sleep(0.02)
        assert job["state"] == "succeeded"
        notes = await client.get(f"{API_PREFIX}/notes", params={"space_id": harness.space["space_id"]})
        assert [note["content_text"] for note in notes.json()["items"]] == ["Synthetic persistent note"]
        details = (await client.get(f"{API_PREFIX}/task", params={"job_id": job_id})).json()
        assert not {"lease_token", "payload", "checkpoint"} & details.keys()
