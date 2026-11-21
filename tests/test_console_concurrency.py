"""Settings, note writes and model probes across concurrent host operations."""

import asyncio
import json
from types import SimpleNamespace

import pytest

import main as plugin_module
from core.memory_spaces import private_space
from main import NeoMemoryPlugin
from storage.console_store import ConsoleStore, NoteService
from storage.job_store import JobConflict, JobStore, LeaseLost
from storage.memory_store import MemoryStore
from tests.test_host_probe import FakeContext


@pytest.mark.asyncio
async def test_plugin_reload_waits_for_inflight_settings_persistence(tmp_path, monkeypatch):
    monkeypatch.setattr(plugin_module.StarTools, "get_data_dir", staticmethod(lambda _name: tmp_path))
    entered, proceed = asyncio.Event(), asyncio.Event()
    class HostConfig(dict):
        async def save_config_async(self, changes):
            self.update(changes)
            entered.set()
            await proceed.wait()
            return True
    plugin = NeoMemoryPlugin(FakeContext(), HostConfig(enabled=False))
    state = await plugin.settings.get()
    saving = asyncio.create_task(plugin.settings.save(state["revision"], {"history_limit": 11}))
    await entered.wait()
    stopping = asyncio.create_task(plugin.terminate())
    try:
        await asyncio.sleep(0.01)
        assert not stopping.done(), "plugin reload returned before the host configuration was persisted"
    finally:
        proceed.set()
        await asyncio.gather(saving, stopping)


@pytest.mark.asyncio
async def test_agent_note_writes_cannot_pass_web_task_scope_lease(tmp_path):
    memory = MemoryStore(tmp_path / "memory.db")
    external = MemoryStore(tmp_path / "memory.db")
    await memory.open()
    await external.open()
    try:
        space = await memory.get_or_create_space(private_space("persona", "user"))
        jobs = JobStore(memory.database)
        service = NoteService(memory, jobs)
        note = await external.create_note(space_id=space["space_id"], content_text="agent-created")
        await service.enqueue(space["space_id"], "edit-click", {"action": "update", "note_id": note["note_id"],
            "expected_updated_at": note["updated_at"], "content_text": "web-edit"})
        for mutate in (
            external.create_note(space_id=space["space_id"], content_text="must-not-create"),
            external.update_note(note["note_id"], space_id=space["space_id"], content_text="must-not-overwrite"),
        ):
            with pytest.raises(JobConflict):
                await mutate
        job = await jobs.claim("web-worker")
        result = await service.run(job)
        # A completed DB mutation remains exclusive until the durable task
        # receipt has been accounted for by the runner.
        with pytest.raises(JobConflict):
            await external.update_note(note["note_id"], space_id=space["space_id"], status="archived")
        await jobs.finish(job["job_id"], job["lease_token"], result=result)
        updated = await external.update_note(note["note_id"], space_id=space["space_id"], content_text="new-agent-edit")
        assert updated["content_text"] == "new-agent-edit"
    finally:
        await external.close()
        await memory.close()


@pytest.mark.asyncio
async def test_old_note_worker_cannot_replay_receipt_after_lease_is_replaced(tmp_path):
    memory = MemoryStore(tmp_path / "memory.db")
    await memory.open()
    try:
        space = await memory.get_or_create_space(private_space("persona", "user"))
        jobs = JobStore(memory.database)
        service = NoteService(memory, jobs)
        await service.enqueue(space["space_id"], "create-click", {"action": "create", "content_text": "one"})
        first = await jobs.claim("first-worker")
        await service.run(first)
        await memory.database.execute("UPDATE memory_jobs SET lease_until=0 WHERE job_id=?", (first["job_id"],))
        second = await jobs.claim("second-worker")
        with pytest.raises(LeaseLost):
            await service.run(first)
        replay = await service.run(second)
        assert replay["replayed"] is True
        await jobs.finish(second["job_id"], second["lease_token"], result=replay)
        assert len((await ConsoleStore(memory).notes(space["space_id"]))["items"]) == 1
    finally:
        await memory.close()


@pytest.mark.asyncio
async def test_shutdown_during_probe_preserves_outcome_without_artificial_provider_failure(tmp_path, monkeypatch):
    class Provider:
        provider_config = {"id": "llm"}
        def get_model(self):
            return "test-model"
    context = FakeContext()
    provider = Provider()
    context.get_provider_by_id = lambda _: provider
    context.get_all_providers = lambda: [provider]
    context.get_all_embedding_providers = lambda: []
    entered, release = asyncio.Event(), asyncio.Event()
    async def generate(**kwargs):
        entered.set()
        await release.wait()
        return SimpleNamespace(completion_text="OK")
    context.llm_generate = generate
    monkeypatch.setattr(plugin_module.StarTools, "get_data_dir", staticmethod(lambda _: tmp_path))
    plugin = NeoMemoryPlugin(context, {"automatic_extraction_enabled": False})
    await plugin.initialize()
    engine = plugin.maintenance
    await engine.stop()
    queued = await plugin.model_tests.enqueue(kind="llm", provider_id="llm", idempotency_key="shutdown-click")
    job = await plugin.jobs.claim("worker")
    work = asyncio.create_task(engine.runner._execute(job))
    engine.runner._workers[job["job_id"]] = work
    await entered.wait()
    scanner_cancelled, scanner_release = asyncio.Event(), asyncio.Event()
    async def scanner():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            scanner_cancelled.set()
            await scanner_release.wait()
            raise
    engine._scanner_task = asyncio.create_task(scanner())
    await asyncio.sleep(0)
    stopping = asyncio.create_task(plugin.terminate())
    try:
        await scanner_cancelled.wait()
        release.set()
        await asyncio.wait_for(work, 1)
        # Runtime detachment during teardown must not turn an otherwise good
        # provider response into a provider failure. Cancellation may instead
        # release the job for restart; both are valid outcomes.
        result = await engine.jobs.get(queued["job_id"])
        assert result["state"] in {"queued", "succeeded"}, json.dumps({key: result[key] for key in ("state", "error_code")})
    finally:
        scanner_release.set()
        release.set()
        await stopping
