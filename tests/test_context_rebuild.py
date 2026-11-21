"""Scoped source replay preserves other components and resumes without applying windows twice."""

import json

import pytest

from core.memory_spaces import group_space
from core.profile_memory_prompt import PROFILE_DUTIES_RULES
from tests.test_extraction_apply import capture
from tests.test_maintenance_engine import environment, enqueue_and_claim, execute, restart


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["profile_rebuild", "relationship_rebuild"])
async def test_scoped_rebuild_restart_publishes_only_target(environment, kind):
    env = environment
    sid, uid = env.space["space_id"], env.space["logical_user_id"]
    for i in range(3, 7):
        await capture(env.sources, number=i)
    old_profile = await env.memory.upsert_profile(space_id=sid, logical_user_id=uid, description="旧画像", tags=[])
    old_rel = await env.memory.upsert_relationship(space_id=sid, logical_user_id=uid, state_text="旧关系")
    event = await env.memory.create_node(space_id=sid, node_type="event", content_text="旧经历",
                                         source_ids=[env.records[0].source_id])
    job = await enqueue_and_claim(env, kind)
    field = "profile_changes" if kind == "profile_rebuild" else "relationship_change"
    observed = []

    async def model(messages):
        data = json.loads(messages[1]["content"])
        assert "known_nodes" not in data and "known_edges" not in data
        assert field in json.loads(messages[0]["content"].split("JSON schema:\n")[1])["properties"]
        assert (PROFILE_DUTIES_RULES in messages[0]["content"]) == (kind == "profile_rebuild")
        assert data["persona"]
        observed.append(data["sources"][0]["content_text"])
        if len(observed) == 2:
            raise ConnectionError("synthetic timeout")
        known = (data.get("known_profiles") or [data.get("known_relationship")])[0]
        version = known["content_version"] if known else 0
        change = {"logical_user_id": "u1", "expected_version": version, "source_ids": ["s1"]}
        if field == "profile_changes":
            assert "known_relationship" not in data
            change.update(tags=["早班"], description=f"习惯早班，版本{version + 1}。")
            env.adapter.output = json.dumps({field: [change]})
        else:
            assert "known_profiles" not in data
            change.update(long_term={"op": "replace", "body": f"我更愿意理解这种选择。{version + 1}"},
                          aftereffect={"op": "keep", "body": ""})
            env.adapter.output = json.dumps({field: change})

    env.adapter.on_generate = model
    failed = await execute(env, job)
    assert failed["state"] == "retry_wait", failed
    assert (await env.memory.list_profiles(space_id=sid))[0]["profile_id"] == old_profile["profile_id"]
    assert (await env.memory.get_relationship(space_id=sid, logical_user_id=uid))["relationship_id"] == old_rel["relationship_id"]
    restart(env)
    await env.memory.database.execute("UPDATE memory_jobs SET available_at=0 WHERE job_id=?", (job["job_id"],))
    completed = await execute(env, await env.engine.jobs.claim("retry", lease_seconds=60))
    assert completed["state"] == "succeeded", (completed["error_code"], completed["error_type"], completed["checkpoint"].get("window", {}).get("validation_errors"))
    assert observed[1] == observed[2] and len(observed) >= 3
    assert completed["result"]["source_count"] == 6
    assert await env.memory.get_node(event["node_id"]) == event
    profile = (await env.memory.list_profiles(space_id=sid))[0]
    rel = await env.memory.get_relationship(space_id=sid, logical_user_id=uid)
    if field == "profile_changes":
        assert rel == old_rel and profile["profile_id"] != old_profile["profile_id"]
        entity_kind, identifier = "profile", profile["profile_id"]
    else:
        assert profile == old_profile and rel["relationship_id"] != old_rel["relationship_id"]
        entity_kind, identifier = "relationship", rel["relationship_id"]
    versions = await getattr(env.memory, f"list_{entity_kind}_versions")(identifier)
    assert sorted(item["version_no"] for item in versions) == list(range(1, len(observed)))
    assert all(item["derivations"][0]["sources"] for item in versions)
    assert not await env.memory.database.fetchall("SELECT * FROM extraction_batch_sources")
    assert (await env.memory.get_active_generation(sid))["activity_round"] == 0


@pytest.mark.asyncio
async def test_group_rejects_relationship_rebuild(environment):
    env = environment
    group = await env.memory.get_or_create_space(group_space("p", "platform", "group"))
    with pytest.raises(ValueError, match="private"):
        await env.engine.enqueue("relationship_rebuild", group["space_id"], "group-rebuild")


@pytest.mark.asyncio
async def test_joint_rebuild_uses_one_call_and_one_publication(environment):
    env = environment
    sid = env.space["space_id"]
    def answer(data):
        uid = data["sources"][0]["actor_logical_user_id"]
        source = data["sources"][0]["source_id"]
        identity = {"logical_user_id": uid, "source_ids": [source], "expected_version": 0}
        return {"profile_changes": [{**identity, "description": "喜欢固定的工作时间。", "tags": []}],
                "relationship_change": {**identity, "long_term": {"op": "keep", "body": ""},
                    "aftereffect": {"op": "replace", "body": "我还有点好奇。"}}}

    async def model(messages, **options):
        env.adapter.calls.append(messages)
        data = json.loads(messages[1]["content"])
        assert PROFILE_DUTIES_RULES in messages[0]["content"]
        assert "known_profiles" in data and "known_relationship" in data
        return json.dumps(answer(data))

    env.adapter.generate = model
    result = await execute(env, await enqueue_and_claim(env, "context_rebuild"))
    assert result["state"] == "succeeded", result
    assert result["result"]["counts"] == {"user_profiles": 1, "relationship_states": 1}
    assert len(env.adapter.calls) == 1
    assert len(await env.memory.list_profiles(space_id=sid)) == 1
    relation = await env.memory.get_relationship(space_id=sid, logical_user_id=env.space["logical_user_id"])
    assert relation["state"]["aftereffect"]["body"] == "我还有点好奇。"


@pytest.mark.asyncio
async def test_scoped_retry_reports_only_target_slot_error(environment):
    from dataclasses import replace
    env = environment
    env.engine.config = replace(env.config, extraction_auto_regeneration=True)

    async def model(messages, **kwargs):
        env.adapter.calls.append(messages)
        data = json.loads(messages[1]["content"])
        failed = len(env.adapter.calls) == 1
        if not failed:
            errors = json.loads(messages[-1]["content"])["errors"]
            assert [error["path"] for error in errors] == ["$.relationship_change.aftereffect"]
            assert errors[0]["type"] == "relationship_slot_body"
        change = {"logical_user_id": "u1", "expected_version": 0,
                  "long_term": {"op": "keep", "body": ""},
                  "aftereffect": {"op": "replace", "body": "replace" if failed else "我还有一点期待。"},
                  "source_ids": [data["sources"][0]["source_id"]]}
        return json.dumps({"relationship_change": change})

    env.adapter.generate = model
    result = await execute(env, await enqueue_and_claim(env, "relationship_rebuild"))
    assert result["state"] == "succeeded", (result["error_code"], result["checkpoint"])
    assert len(env.adapter.calls) == 2
