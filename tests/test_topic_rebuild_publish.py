"""Topic-only publication replaces the old graph once and preserves its members."""

import uuid

import pytest

from core.memory_spaces import private_space
from storage.topic_rebuild_publish import publish_topics, published_topics, topic_rebuild_basis
from tests.test_topic_store import env, initial_topic, output

pytestmark = pytest.mark.asyncio


async def publication(env):
    memory, _, jobs, space, nodes = env
    generation = nodes[0]["generation_id"]
    await jobs.submit("topic_rebuild", space["space_id"],
                      {"expected_generation_id": generation}, uuid.uuid4().hex)
    job = await jobs.claim("test", lease_seconds=60)
    draft = output(nodes)["changes"][0] | {"node_id": "node_replacement"}
    return job, generation, [draft], {node["node_id"]: node["content_version"] for node in nodes}, (
        await topic_rebuild_basis(memory, space["space_id"], generation))


async def test_publication_scopes_deletion_and_starts_new_history(env):
    memory, _, _, space, nodes = env
    old = await initial_topic(env)
    await memory.update_node_content(old["node_id"], content_text="另一个旧版本")
    await memory.database.execute("UPDATE memory_nodes SET status='disabled',manual_protected=1 WHERE node_id=?",
                                  (old["node_id"],))
    other_space = await memory.get_or_create_space(private_space("p", "other"))
    other = await memory.create_node(space_id=other_space["space_id"], node_type="topic",
                                     content_text="另一个空间的主题", source_ids=["other-source"])
    args = await publication(env)
    # Foreground reinforcement is permitted while synthesis runs.
    await memory.database.execute("UPDATE memory_nodes SET reinforcement=0.1 WHERE node_id=?", (nodes[0]["node_id"],))
    result = await publish_topics(memory, *args)
    assert await published_topics(memory, args[0]["job_id"]) == result == await publish_topics(memory, *args)
    assert result["removed_node_ids"] == [old["node_id"]]
    assert result["reviewed_count"] == len(nodes)
    assert await memory.get_node(other["node_id"]) == other
    assert await memory.get_node(old["node_id"]) is None
    assert not await memory.database.fetchall("SELECT * FROM memory_node_versions WHERE node_id=?", (old["node_id"],))
    new_id, = result["topic_ids"]
    assert [v["version_no"] for v in await memory.list_node_versions(new_id)] == [1]
    derivation, = await memory.list_version_derivations("node", new_id)
    assert {base["entity_id"] for base in derivation["base_versions"]} == {n["node_id"] for n in nodes}
    assert (await memory.get_node(nodes[0]["node_id"]))["reinforcement"] == .1
    assert (await memory.get_active_generation(space["space_id"]))["generation_id"] == args[1]


async def test_publication_rolls_back_partial_write(env):
    memory = env[0]
    old = await initial_topic(env)
    job, generation, drafts, reviewed, basis = await publication(env)
    bad_draft = drafts[0] | {"node_id": "node_invalid", "importance": 1.5}
    with pytest.raises(ValueError, match="importance"):
        await publish_topics(memory, job, generation, drafts + [bad_draft], reviewed, basis)
    async def cache_unavailable(scope, removed):
        assert scope == old["space_id"] and removed == [old["node_id"]]
        raise OSError("cache unavailable")

    with pytest.raises(OSError, match="cache unavailable"):
        await publish_topics(memory, job, generation, drafts, reviewed, basis, invalidate_nodes=cache_unavailable)
    assert await memory.get_node(old["node_id"]) == old
    assert await memory.get_node(drafts[0]["node_id"]) is None
    assert await published_topics(memory, job["job_id"]) is None


async def test_publication_refuses_stale_inputs_and_allows_empty_result(env):
    memory, _, _, space, _ = env
    old = await initial_topic(env)
    job, generation, _, reviewed, basis = await publication(env)
    await memory.database.execute("UPDATE memory_nodes SET title='人工修订' WHERE node_id=?", (old["node_id"],))
    with pytest.raises(ValueError, match="topic context changed"):
        await publish_topics(memory, job, generation, [], reviewed, basis)
    assert (await memory.get_node(old["node_id"]))["title"] == "人工修订"
    fresh_basis = await topic_rebuild_basis(memory, space["space_id"], generation)
    result = await publish_topics(memory, job, generation, [], reviewed, fresh_basis)
    assert result["topic_ids"] == [] and result["removed_node_ids"] == [old["node_id"]]


async def test_replaced_topic_input_stops_blocking_and_cannot_resume(env):
    from types import SimpleNamespace

    from core.jobs import ReviewRequired
    from core.topic_maintenance import TopicMaintenanceMixin

    memory, _, jobs, space, nodes = env
    worker = SimpleNamespace(memory=memory)
    scope, generation = space["space_id"], nodes[0]["generation_id"]
    await jobs.submit("topic_consolidate", scope, {"expected_generation_id": generation}, "old-topics")
    old = await jobs.claim("test", lease_seconds=60)
    await jobs.fail(old["job_id"], old["lease_token"], error_type="review", error_code="invalid_topic_output")
    assert await TopicMaintenanceMixin._topic_blockers(worker, scope, generation)
    args = await publication(env)
    await publish_topics(memory, *args)
    assert not await TopicMaintenanceMixin._topic_blockers(worker, scope, generation)
    stale = await jobs.get(old["job_id"])
    assert stale["state"] == "failed" and stale["error_code"] == "invalid_topic_output"
    with pytest.raises(ReviewRequired, match="context_changed"):
        await TopicMaintenanceMixin._consolidate_topics(worker, stale)
