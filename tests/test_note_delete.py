"""笔记删除与归档分离：作用域、版本冲突及提交凭据恢复。"""

import pytest

from core.memory_spaces import private_space
from storage.job_store import JobConflict
from tests.test_console_store import execute, stores


@pytest.mark.asyncio
@pytest.mark.parametrize("archived", [False, True])
async def test_deleted_note_disappears_from_queries_and_cannot_be_restored(stores, archived):
    memory, _, space, console, service = stores
    note = await execute(service, space, "create", {"action": "create", "content_text": "合成待办"})
    if archived:
        note = await execute(service, space, "archive", {"action": "archive", "note_id": note["note_id"],
            "expected_updated_at": note["updated_at"]})
    deleted = await execute(service, space, "delete", {"action": "delete", "note_id": note["note_id"],
        "expected_updated_at": note["updated_at"]})
    assert deleted["status"] == "deleted"
    for status in ("active", "archived", "all"):
        assert (await console.notes(space["space_id"], status=status))["items"] == []
    assert await memory.list_notes(space_id=space["space_id"], query="合成") == []
    row = await memory.database.fetchone("SELECT * FROM notes WHERE note_id=?", (note["note_id"],))
    assert row["deleted_at"] == deleted["updated_at"]
    with pytest.raises(KeyError):
        await service.enqueue(space["space_id"], "restore-deleted", {"action": "restore", "note_id": note["note_id"],
            "expected_updated_at": deleted["updated_at"]})
    for scope in ({}, {"space_id": space["space_id"]}):
        with pytest.raises(KeyError):
            await memory.update_note(note["note_id"], status="active", **scope)


@pytest.mark.asyncio
async def test_delete_checks_scope_and_revision_before_mutating(stores):
    memory, _, space, _, service = stores
    note = await execute(service, space, "create", {"action": "create", "content_text": "初稿"})
    operation = {"action": "delete", "note_id": note["note_id"], "expected_updated_at": note["updated_at"]}
    other = await memory.get_or_create_space(private_space("p", "another-user"))
    with pytest.raises(KeyError):
        await service.enqueue(other["space_id"], "cross-delete", operation)
    await memory.update_note(note["note_id"], content_text="后续修改")
    with pytest.raises(JobConflict):
        await service.enqueue(space["space_id"], "stale-delete", operation)
    current = (await memory.list_notes(space_id=space["space_id"]))[0]
    assert current["content_text"] == "后续修改" and current["status"] == "active"


@pytest.mark.asyncio
async def test_duplicate_delete_and_restart_replay_exactly_one_receipt(stores):
    memory, _, space, _, service = stores
    note = await execute(service, space, "create", {"action": "create", "content_text": "测试笔记"})
    operation = {"action": "delete", "note_id": note["note_id"], "expected_updated_at": note["updated_at"]}
    queued = await service.enqueue(space["space_id"], "delete-once", operation)
    assert (await service.enqueue(space["space_id"], "delete-once", operation))["job_id"] == queued["job_id"]
    job = await service.jobs.claim("before-restart")
    deleted = await service.run(job)
    await memory.database.execute("UPDATE memory_jobs SET lease_until=0 WHERE job_id=?", (job["job_id"],))
    await memory.close()
    await memory.open()
    resumed = await service.jobs.claim("after-restart")
    replay = await service.run(resumed)
    assert replay["replayed"] and replay["updated_at"] == deleted["updated_at"]
    await service.jobs.finish(resumed["job_id"], resumed["lease_token"], result=replay)
    assert (await service.enqueue(space["space_id"], "delete-once", operation))["state"] == "succeeded"
    row = await memory.database.fetchone("SELECT COUNT(*) AS n FROM note_receipts WHERE job_id=?", (job["job_id"],))
    assert row["n"] == 1
    with pytest.raises(JobConflict):
        await service.enqueue(space["space_id"], "delete-once", {**operation, "action": "archive"})
