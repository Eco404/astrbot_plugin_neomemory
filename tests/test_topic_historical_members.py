"""Existing historical membership survives synthesis without reviving old facts."""

import pytest

from core.candidate_feedback import CandidateValidationError
from tests.test_topic_store import env, begin, initial_topic, output, prepared

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("content_changed", [False, True])
async def test_retains_historical_member_and_its_version_without_mutating_it(env, content_changed):
    memory, store, _, _, nodes = env
    topic = await initial_topic(env)
    historical = await memory.update_node_status(nodes[0]["node_id"], "historical")
    batch = await prepared(env, ids=[nodes[1]["node_id"]], seeds=[nodes[1]["node_id"]])
    assert next(n for n in batch["nodes"] if n["node_id"] == historical["node_id"])["status"] == "historical"
    content = "共同计划经历了早期尝试，目前已进入持续实践。" if content_changed else topic["content_text"]
    result = await store.apply(await begin(env), batch, output(nodes, topic, content=content))
    assert result["topic_ids"] == [topic["node_id"]]
    assert await memory.get_node(historical["node_id"]) == historical
    dependency = await memory.database.fetchone(
        "SELECT member_version FROM topic_dependencies WHERE topic_id=? AND member_id=?",
        (topic["node_id"], historical["node_id"]))
    assert dependency["member_version"] == historical["content_version"]
    current = await memory.get_node(topic["node_id"])
    assert current["content_version"] == topic["content_version"] + int(content_changed)
    if not content_changed:
        for field in ("strength_reset_at", "strength_reset_round", "reinforcement", "updated_at"):
            assert current[field] == topic[field]


@pytest.mark.parametrize("operation", ["create", "update_other"])
async def test_historical_member_cannot_be_added_to_another_topic(env, operation):
    memory, store, _, _, nodes = env
    topic = await initial_topic(env) if operation == "update_other" else None
    historical = await memory.create_node(
        space_id=nodes[0]["space_id"], node_type="event", status="historical",
        content_text="已被新安排替代的早期实践。", source_ids=["independent-history"])
    batch = await prepared(env, ids=[n["node_id"] for n in [*nodes, historical]], seeds=[])
    with pytest.raises(CandidateValidationError) as error:
        await store.apply(await begin(env), batch, output([*nodes, historical], topic))
    assert error.value.code == "topic_member_unavailable"


@pytest.mark.parametrize("state", ["disabled", "deleted", "superseded", "stale"])
@pytest.mark.parametrize("retain", [False, True])
async def test_unavailable_existing_member_requires_revision_even_if_omitted(env, state, retain):
    memory, store, _, _, nodes = env
    topic = await initial_topic(env)
    if state == "stale":
        await memory.database.execute(
            "UPDATE memory_nodes SET metadata_json=? WHERE node_id=?",
            ('{"summary_stale":true}', nodes[0]["node_id"]))
    else:
        await memory.update_node_status(nodes[0]["node_id"], state)
    candidate = output(nodes if retain else nodes[1:], topic)
    with pytest.raises(PermissionError, match="existing topic member"):
        await store.apply(await begin(env), await prepared(env), candidate)
    assert (await memory.get_node(topic["node_id"]))["content_version"] == topic["content_version"]


async def test_status_change_after_prepare_is_still_a_snapshot_conflict(env):
    memory, store, _, _, nodes = env
    topic = await initial_topic(env)
    batch = await prepared(env)
    await memory.update_node_status(nodes[0]["node_id"], "historical")
    with pytest.raises(ValueError, match="context changed"):
        await store.apply(await begin(env), batch, output(nodes, topic))


@pytest.mark.parametrize("retain", [False, True])
async def test_protected_historical_member_can_be_retained_but_not_removed(env, retain):
    memory, store, _, _, nodes = env
    topic = await initial_topic(env)
    await memory.update_node_status(nodes[0]["node_id"], "historical")
    await memory.set_manual_protection(nodes[0]["node_id"], True)
    batch = await prepared(env)
    job = await begin(env)
    candidate = output(nodes if retain else nodes[1:], topic)
    if retain:
        assert (await store.apply(job, batch, candidate))["topic_ids"] == [topic["node_id"]]
    else:
        with pytest.raises(PermissionError, match="protected member"):
            await store.apply(job, batch, candidate)
