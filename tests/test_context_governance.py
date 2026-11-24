"""Exercise manual context edits through durable jobs and subsequent summary rolls."""

import json

import pytest

from core.relationship_state import relationship_slots
from storage.console_store import ConsoleStore
from storage.context_store import ContextStore
from storage.governance_store import GovernanceService, validate_operation
from storage.job_store import JobStore
from tests.test_governance import apply, lease, setup_nodes, store
from tests.test_short_term import add, claim, current, env, execute


def operation(kind, row, action="edit", **patch):
    field = {"profile": "profile_id", "relationship": "relationship_id", "short_term": "state_id"}[kind]
    request = {"entity_kind": kind, "entity_id": row[field], "action": action,
               "expected_generation_id": row["generation_id"], "expected_version": row["content_version"]}
    if action == "edit":
        request["patch"] = patch
    return request


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["profile", "relationship"])
async def test_context_edits_are_scoped_versioned_and_replayable(store, kind):
    space, _ = await setup_nodes(store, kinds=())
    other, _ = await setup_nodes(store, kinds=(), user="other")
    sid = space["space_id"]
    if kind == "profile":
        row = await store.upsert_profile(space_id=sid, logical_user_id="u", description="旧描述", tags=["旧标签"])
        patch = {"description": "新描述", "tags": ["新标签"]}
    else:
        slots = relationship_slots()
        slots["aftereffect"] = {"body": "旧余韵", "observed_at": 25}
        row = await store.upsert_relationship(space_id=sid, logical_user_id="u", state=slots)
        patch = {"long_term": "长期理解", "aftereffect": "新余韵"}
    edit = operation(kind, row, **patch)
    edited, job = await apply(store, sid, edit, finish=False)
    assert edited["content_version"] == 2
    assert (await GovernanceService(store).apply(
        sid, edit, job_id=job["job_id"], lease_token=job["lease_token"],
    ))["replayed"]
    await JobStore(store.database).finish(job["job_id"], job["lease_token"], result=edited)
    versions = getattr(store, f"list_{kind}_versions")
    history = await versions(edit["entity_id"])
    assert [version["version_no"] for version in history] == [1, 2]
    manual, = history[-1]["derivations"]
    assert manual["editor_kind"] == "manual"
    assert manual["governance_job_id"] == job["job_id"]
    assert manual["base_versions"] == [{"kind": kind, "entity_id": edit["entity_id"], "version_no": 1}]
    if kind == "relationship":
        assert history[-1]["state"]["aftereffect"] == {"body": patch["aftereffect"], "observed_at": 25}
        assert history[-1]["state"]["long_term"]["body"] == patch["long_term"]
    else:
        assert history[-1]["description"] == patch["description"] and history[-1]["tags"] == patch["tags"]
    same, _ = await apply(store, sid, {**edit, "expected_version": 2})
    assert same["content_version"] == 2

    # Simulate a request which selected this entry before the correction.
    for target, generation, version in ((sid, edit["expected_generation_id"], 1),
                                        (other["space_id"], (await store.get_active_generation(other["space_id"]))["generation_id"], 2)):
        request = {**operation(kind, row, action="delete"), "expected_generation_id": generation, "expected_version": version}
        jobs, stale = await lease(store, target, request)
        with pytest.raises(ValueError, match="stale or foreign"):
            await GovernanceService(store).apply(target, request, job_id=stale["job_id"], lease_token=stale["lease_token"])
        await jobs.finish(stale["job_id"], stale["lease_token"], result={})
    assert len(await versions(edit["entity_id"])) == 2
    await apply(store, sid, {**operation(kind, row, action="delete"), "expected_version": 2})
    with pytest.raises(KeyError):
        await versions(edit["entity_id"])


@pytest.mark.asyncio
async def test_deleted_summary_keeps_cursor_and_resumes_with_only_new_messages(env):
    sid = env.space["space_id"]
    await execute(env, await claim(env))
    old = await current(env)
    correction = "我需要等对方答复。"
    await apply(env.memory, sid, operation("short_term", old, summary_text=correction))
    edited = await current(env)
    assert edited["summary_text"] == correction and edited["coverage"] == old["coverage"]
    await apply(env.memory, sid, operation("short_term", edited, action="delete"))
    console = ConsoleStore(env.memory, env.sources)
    assert (await console.context(sid, "short_term"))["items"] == []
    assert await current(env) is None
    scope = {"logical_user_id": env.records[0].logical_user_id, "stream_id": env.stream_id,
             "host_conversation_id": "chat", "conversation_key": old["conversation_id"]}
    assert (await ContextStore(env.memory).snapshot(sid, **scope))["short_term"] is None
    await add(env, start=9, count=2)
    job = await claim(env)
    assert job["payload"]["source_ids"] == [r.source_id for r in env.records[4:8]]
    assert (await execute(env, job))["state"] == "succeeded"
    assert json.loads(env.adapter.calls[-1][-1]["content"])["previous_summary"] == ""
    updated = await current(env)
    assert updated["content_version"] == 4 and updated["coverage"]["source_count"] == 8
    assert "summary_deleted" not in updated["coverage"]
    assert len((await console.context(sid, "short_term"))["items"]) == 1


def test_validate_context_operation_limits_patch_to_editable_fields():
    base = {"entity_kind": "short_term", "entity_id": "s", "expected_version": 1,
            "expected_generation_id": "g", "action": "edit"}
    assert validate_operation({**base, "patch": {"summary_text": "摘要"}})["patch"]["summary_text"] == "摘要"
    with pytest.raises(ValueError):
        validate_operation({**base, "patch": {"summary_text": "摘要", "covered_to_source_id": "s"}})
    with pytest.raises(ValueError):
        validate_operation({**base, "entity_kind": "relationship", "patch": {"long_term": "", "aftereffect": "replace"}})
