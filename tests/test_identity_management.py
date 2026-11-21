import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
import pytest_asyncio

from core.identity_management import IdentityConflict, IdentityManagement
from core.memory_spaces import group_space, private_space
from core.jobs import ReviewRequired
from storage.cache_store import CacheStore, text_hash
from storage.job_store import JobConflict, JobStore
from storage.memory_store import MemoryStore
from storage.pipeline_store import PipelineStore
from storage.source_store import SourceStore
from tests.test_source_store import make_source


def source(account="b", message="m1"):
    value = make_source(message)
    return replace(value, actor_account_id=account, actor_account_name="Nickname " + account,
        persona_id="p", identity=replace(value.identity, platform_instance_id="test", account_id=account,
            unified_msg_origin="test:FriendMessage:" + account, session_id=account))


@pytest_asyncio.fixture
async def env(tmp_path):
    sources = SourceStore(tmp_path / "source.db")
    await sources.open()
    memory = MemoryStore(tmp_path / "memory.db", source_exists=sources.has_source)
    await memory.open()
    cache = CacheStore(tmp_path / "cache.db")
    await cache.open()
    jobs = JobStore(memory.database)
    service = IdentityManagement(sources, memory, jobs, SimpleNamespace(cache=cache))
    for account, user in (("a", "A"), ("b", "B")):
        await sources.bind_account(platform_instance_id="test", account_id=account,
                                   logical_user_id=user, display_name="Name " + user)
    captured = await sources.capture(source())
    try:
        yield SimpleNamespace(sources=sources, memory=memory, cache=cache, jobs=jobs,
                              service=service, captured=captured)
    finally:
        await cache.close()
        await memory.close()
        await sources.close()


def request(action="bind", account="b"):
    return {"action": action, "logical_user_id": "A", "platform_instance_id": "test", "account_id": account}


async def submit(env, action="bind", account="b", key="binding"):
    body = request(action, account)
    preview = await env.service.preview(**body)
    job = await env.service.submit(**body, confirmation_token=preview["confirmation_token"], idempotency_key=key)
    return preview, job


async def execute(env):
    job = await env.jobs.claim("identity-test", lease_seconds=60)
    result = await env.service.run(job)
    await env.jobs.finish(job["job_id"], job["lease_token"], result=result)
    return job, result


async def seed_private(env, user="B", persona="p"):
    space = await env.memory.get_or_create_space(private_space(persona, user))
    node = await env.memory.create_node(space_id=space["space_id"], node_type="event", content_text="Synthetic event",
        source_ids=[env.captured.source_id], manual_protected=True)
    await env.cache.put_vectors("model", space["space_id"], [text_hash(node["content_text"])], [[1, 0]],
                                kind="node", identities=[(node["node_id"], node["content_version"])])
    await env.memory.upsert_profile(space_id=space["space_id"], logical_user_id=user, tags=["synthetic"])
    await env.memory.upsert_relationship(space_id=space["space_id"], logical_user_id=user, state_text="Synthetic feeling")
    await env.memory.create_note(space_id=space["space_id"], content_text="Synthetic note")
    return space, node


@pytest.mark.asyncio
async def test_bind_clears_single_account_old_private_memories_all_personas(env):
    old_space, old_node = await seed_private(env)
    await seed_private(env, persona="another")
    target, target_node = await seed_private(env, user="A")
    group = await env.memory.get_or_create_space(group_space("p", "test", "group"))
    group_node = await env.memory.create_node(space_id=group["space_id"], node_type="event", content_text="Shared group",
                                            source_ids=[env.captured.source_id])
    old_job = await env.jobs.submit("rebuild", old_space["space_id"], {}, "old-rebuild")
    running = await env.jobs.claim("old", lease_seconds=60)
    generation = await env.memory.get_active_generation(old_space["space_id"])
    await PipelineStore(env.memory).begin_rebuild(running, old_space["space_id"], generation["generation_id"])
    await env.jobs.fail(old_job["job_id"], running["lease_token"], error_type="Test", error_code="test", retryable=False)
    before_raw = await env.sources.get_records((env.captured.source_id,))
    preview, queued = await submit(env)
    assert preview["delete_previous_user"]
    assert preview["counts"]["nodes"] == preview["counts"]["private_spaces"] == 2
    assert preview["counts"]["notes"] == 2
    assert queued["scope_key"] == "*"
    _, result = await execute(env)
    assert result["deleted_logical_user_id"] == "B"
    assert await env.sources.resolve_account("test", "b") == "A"
    assert (await env.sources.database.fetchone("SELECT status FROM logical_users WHERE logical_user_id='B'"))["status"] == "deleted"
    assert await env.sources.get_records((env.captured.source_id,)) == before_raw
    assert not await env.memory.get_node(old_node["node_id"])
    assert await env.memory.get_node(target_node["node_id"])
    assert await env.memory.get_node(group_node["node_id"])
    assert await env.jobs.get(old_job["job_id"]) is None
    assert not await env.cache.database.fetchone("SELECT 1 FROM embedding_cache WHERE namespace=?", (old_space["space_id"],))
    assert (await env.cache.health())["counts"]["node"] == 1
    assert not await env.memory.database.fetchall("PRAGMA foreign_key_check")
    assert not await env.sources.database.fetchall("PRAGMA foreign_key_check")
    again = await env.service.submit(**request(), confirmation_token=preview["confirmation_token"], idempotency_key="binding")
    assert again["job_id"] == queued["job_id"] and again["state"] == "succeeded"


@pytest.mark.asyncio
async def test_shared_old_user_retains_memories_and_other_accounts(env):
    await env.sources.bind_account(platform_instance_id="test", account_id="c", logical_user_id="B")
    old_space, old_node = await seed_private(env)
    preview, _ = await submit(env)
    assert not preview["delete_previous_user"]
    assert not any(preview["counts"].values())
    await execute(env)
    assert await env.sources.resolve_account("test", "b") == "A"
    assert await env.sources.resolve_account("test", "c") == "B"
    assert (await env.sources.get_logical_user("B"))["status"] == "active"
    assert (await env.memory.get_node(old_node["node_id"]))["space_id"] == old_space["space_id"]


@pytest.mark.asyncio
async def test_unbind_assigns_fresh_identity_keeps_memory_and_historical_owner(env):
    await env.sources.bind_account(platform_instance_id="test", account_id="extra", logical_user_id="A")
    original = await env.sources.capture(source("a", "old-a"))
    _, node = await seed_private(env, user="A")
    preview, _ = await submit(env, "unbind", "a")
    assert not preview["delete_previous_user"]
    _, result = await execute(env)
    new_user = result["logical_user_id"]
    assert new_user != "A"
    assert await env.sources.resolve_account("test", "a") == new_user
    assert await env.sources.resolve_account("test", "extra") == "A"
    assert (await env.sources.get_records((original.source_id,)))[0]["actor_logical_user_id"] == "A"
    new_source = await env.sources.capture(source("a", "new-a"))
    assert new_source.logical_user_id == new_user
    assert await env.memory.get_node(node["node_id"])
    with pytest.raises(IdentityConflict, match="唯一账号"):
        await env.service.preview(**request("unbind", "extra"))


@pytest.mark.asyncio
async def test_preview_confirmation_and_global_admission(env):
    preview = await env.service.preview(**request())
    await env.sources.rename_logical_user("A", "Renamed")
    with pytest.raises(IdentityConflict) as changed:
        await env.service.submit(**request(), confirmation_token=preview["confirmation_token"], idempotency_key="stale")
    assert changed.value.code == "identity_confirmation_changed"
    await env.jobs.submit("extract", "another-space", {}, "busy")
    with pytest.raises(JobConflict):
        await submit(env)


@pytest.mark.asyncio
async def test_preview_does_not_hold_source_lock_while_waiting_for_memory(env):
    async with env.memory.database.transaction():
        preview_task = asyncio.create_task(env.service.preview(**request()))
        await asyncio.sleep(0.02)
        await asyncio.wait_for(env.sources.get_logical_user("A"), timeout=0.5)
    await asyncio.wait_for(preview_task, timeout=1)


@pytest.mark.asyncio
async def test_resume_after_memory_commit_source_rollback_uses_checkpoint_and_ignores_name_changes(env, monkeypatch):
    _, node = await seed_private(env)
    await submit(env)
    job = await env.jobs.claim("first", lease_seconds=60)
    execute_sql = env.sources.database.connection.execute
    failed = False

    async def fail_receipt(sql, parameters=()):
        nonlocal failed
        if "INSERT INTO identity_operation_receipts" in sql and not failed:
            failed = True
            raise OSError("synthetic storage interruption")
        return await execute_sql(sql, parameters)

    monkeypatch.setattr(env.sources.database.connection, "execute", fail_receipt)
    with pytest.raises(OSError):
        await env.service.run(job)
    assert await env.sources.resolve_account("test", "b") == "B"
    assert not await env.memory.get_node(node["node_id"])
    checkpoint = (await env.jobs.get(job["job_id"]))["checkpoint"]
    assert checkpoint["identity_purge_complete"]
    await env.sources.rename_logical_user("A", "Changed display only")
    await env.sources.capture(replace(source("b", "nickname-change"), actor_account_name="Changed nickname"))
    await env.jobs.release(job["job_id"], job["lease_token"])
    resumed = await env.jobs.claim("second", lease_seconds=60)
    result = await env.service.run(resumed)
    assert result["removed"]["nodes"] == 1
    assert await env.sources.resolve_account("test", "b") == "A"
    assert await env.service.run(resumed) == result
    assert (await env.sources.database.fetchone("SELECT count(*) AS n FROM identity_operation_receipts"))["n"] == 1


@pytest.mark.asyncio
async def test_memory_change_between_confirmation_and_global_admission_needs_review(env, monkeypatch):
    old_space, _ = await seed_private(env)
    original = env.jobs.submit

    async def concurrent_submit(*args, **kwargs):
        await env.memory.create_note(space_id=old_space["space_id"], content_text="Concurrent note")
        return await original(*args, **kwargs)

    monkeypatch.setattr(env.jobs, "submit", concurrent_submit)
    await submit(env)
    running = await env.jobs.claim("worker", lease_seconds=60)
    with pytest.raises(ReviewRequired):
        await env.service.run(running)
    assert await env.sources.resolve_account("test", "b") == "B"


@pytest.mark.asyncio
async def test_source_receipt_retained_until_task_deleted(env):
    await submit(env)
    job, _ = await execute(env)
    assert await env.service.cleanup_receipts() == 0
    await env.jobs.delete(job["job_id"])
    assert await env.service.cleanup_receipts() == 1
    assert await env.service.cleanup_receipts() == 0


def qq_source(instance, account, message):
    raw = source(account, message)
    return replace(raw, identity=replace(raw.identity, platform_name="aiocqhttp",
                                         platform_instance_id=instance,
                                         unified_msg_origin=f"{instance}:FriendMessage:{account}"))


@pytest.mark.asyncio
async def test_multiple_routes_move_and_unbind_as_one_social_account(env):
    first = await env.sources.capture(qq_source("onebot-1", "10001", "1"))
    await env.sources.capture(qq_source("onebot-2", "10001", "2"))
    bind = {**request(), "platform_instance_id": "onebot-2", "account_id": "10001"}
    preview = await env.service.preview(**bind)
    assert preview["account"]["platform_instance_ids"] == ["onebot-1", "onebot-2"]
    assert preview["previous_user"]["logical_user_id"] == first.logical_user_id
    assert preview["delete_previous_user"]
    await env.service.submit(**bind, confirmation_token=preview["confirmation_token"], idempotency_key="move-both")
    await execute(env)
    for instance in ("onebot-1", "onebot-2", "onebot-3"):
        assert (await env.sources.capture(qq_source(instance, "10001", "3"))).logical_user_id == "A"
    unbind = {**bind, "action": "unbind"}
    preview = await env.service.preview(**unbind)
    assert len(preview["target_user"]["accounts"]) == 2
    await env.service.submit(**unbind, confirmation_token=preview["confirmation_token"], idempotency_key="detach-both")
    _, result = await execute(env)
    new_id = result["logical_user_id"]
    assert new_id != "A"
    for instance in ("onebot-1", "onebot-2", "onebot-3"):
        assert await env.sources.resolve_account(instance, "10001") == new_id
    with pytest.raises(IdentityConflict, match="唯一账号"):
        await env.service.preview(**{**unbind, "logical_user_id": new_id})


@pytest.mark.asyncio
async def test_split_legacy_ownership_lists_impacts_and_requires_one_confirmed_transfer(env):
    for instance in ("qq-1", "qq-2"):
        await env.sources.capture(qq_source(instance, "10002", instance))
    # Simulate two owners left by the instance-keyed schema.
    for instance, owner in (("qq-1", "A"), ("qq-2", "B")):
        await env.sources.database.execute(
            "UPDATE account_bindings SET logical_user_id=? WHERE platform_instance_id=? AND account_id=?",
            (owner, instance, "10002"))
    old_space, old_node = await seed_private(env, "B", "other-persona")
    conflicts = await env.service.account_conflicts([{"logical_user_id": "A"}])
    assert len(conflicts) == 1
    assert conflicts[0]["account_id"] == "10002"
    owners = {o["logical_user_id"]: o for o in conflicts[0]["owners"]}
    assert set(owners) == {"A", "B"}
    assert owners["B"]["platform_instance_ids"] == ["qq-2"]
    assert owners["B"]["private_spaces"] == [{"persona_id": "other-persona", "space_id": old_space["space_id"]}]
    assert owners["B"]["counts"]["nodes"] == 1
    body = {**request(), "platform_instance_id": "qq-2", "account_id": "10002"}
    preview = await env.service.preview(**body)
    assert not preview["delete_previous_user"]
    await env.service.submit(**body, confirmation_token=preview["confirmation_token"], idempotency_key="resolve-conflict")
    await execute(env)
    assert await env.service.account_conflicts([{"logical_user_id": "A"}]) == []
    for instance in ("qq-1", "qq-2", "qq-3"):
        assert (await env.sources.capture(qq_source(instance, "10002", "after"))).logical_user_id == "A"
    assert await env.memory.get_node(old_node["node_id"])
