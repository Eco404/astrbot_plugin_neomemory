"""Content recency follows source observations, independently of storage writes."""

import json
import uuid

import pytest
import pytest_asyncio

from core.extraction_apply import ExtractionService
from core.memory_spaces import private_space
from storage.job_store import JobStore
from storage.memory_store import MemoryStore
from storage.source_store import SourceStore
from storage.topic_store import TopicStore
from tests.test_extraction_apply import capture, node, prepare
from tests.test_governance import apply, operation
from tests.test_extraction_apply import output
from tests.test_topic_store import output as topic_output

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def observed_store(tmp_path):
    observations = {"early": 100.0, "middle": 200.0, "late": 600.0, "epoch": 0.0}
    calls = []

    async def lookup(source_ids):
        calls.append(tuple(source_ids))
        return {sid: observations[sid] for sid in source_ids if sid in observations}

    memory = MemoryStore(tmp_path / "memory.db", source_observations=lookup)
    await memory.open()
    try:
        yield memory, observations, calls
    finally:
        await memory.close()


@pytest_asyncio.fixture
async def extraction_env(tmp_path):
    sources = SourceStore(tmp_path / "source.db")
    await sources.open()
    memory = MemoryStore(
        tmp_path / "memory.db", source_exists=sources.has_source,
        source_observations=sources.get_observation_times,
    )
    await memory.open()
    try:
        yield sources, memory, ExtractionService(sources, memory)
    finally:
        await memory.close()
        await sources.close()


async def create(memory, source_ids, *, kind="event", text="A synthetic activity."):
    space = await memory.get_or_create_space(private_space("p", "u"))
    return await memory.create_node(
        space_id=space["space_id"], node_type=kind, content_text=text,
        source_ids=source_ids,
    )


async def extract(env, number, old=None, *, text="A synthetic activity.", title="合成活动"):
    batch, source = await prepare(env, number=number, text=text,
                                  known=(old["node_id"],) if old else ())
    change = {**node(source.source_id, old=old, content=text), "title": title}
    candidate = output([change])
    candidate["node_changes"][0]["title"] = title
    receipt = await env[2].apply(batch, json.dumps(candidate), model_id="synthetic")
    current = await env[1].get_node(receipt["result"]["node_refs"]["local_interview"])
    return current, source


async def test_create_uses_observation_not_rebuild_or_event_time(observed_store):
    memory, _, _ = observed_store
    current = await create(memory, ["early", "middle"])
    assert current["content_updated_at"] == 200
    assert current["created_at"] > 200 and current["updated_at"] > 200
    version = (await memory.list_node_versions(current["node_id"]))[0]
    assert version["content_updated_at"] == 200
    assert version["created_at"] > 200
    changed = await memory.update_node_content(current["node_id"], current["content_text"],
                                              event_time_start=900, event_time_end=950)
    assert changed["content_updated_at"] == 200
    epoch = await create(memory, ["epoch"])
    unknown = await create(memory, ["removed-source"])
    assert epoch["content_updated_at"] == 0
    assert unknown["content_updated_at"] is None


async def test_confirmation_old_source_does_not_regress_new_source_advances(extraction_env):
    memory = extraction_env[1]
    current, _ = await extract(extraction_env, 2)
    original_version = current["content_version"]
    assert current["content_updated_at"] == 200
    current, _ = await extract(extraction_env, 1, current)
    assert current["content_updated_at"] == 200
    current, _ = await extract(extraction_env, 3, current)
    assert current["content_updated_at"] == 300
    assert current["content_version"] == original_version
    versions = await memory.list_node_versions(current["node_id"])
    assert len(versions) == 1 and versions[0]["content_updated_at"] == 300
    assert [d["operation"] for d in versions[0]["derivations"]] == ["create", "confirm", "confirm"]
    current, _ = await extract(extraction_env, 4, current, text="The activity is now complete.")
    assert current["content_updated_at"] == 400
    assert [v["content_updated_at"] for v in await memory.list_node_versions(current["node_id"])] == [300, 400]


async def test_title_only_does_not_treat_new_citation_as_content_confirmation(extraction_env):
    current, _ = await extract(extraction_env, 1)
    revised, _ = await extract(extraction_env, 9, current, title="展示标题")
    assert revised["title"] == "展示标题"
    assert revised["content_updated_at"] == 100
    assert revised["content_version"] == current["content_version"]
    assert revised["updated_at"] == current["updated_at"]
    versions = await extraction_env[1].list_node_versions(current["node_id"])
    assert versions[0]["content_updated_at"] == 100
    assert versions[0]["derivations"][-1]["operation"] == "title_update"


async def test_manual_edit_status_and_lock_keep_source_recency(observed_store):
    memory, _, _ = observed_store
    current = await create(memory, ["early"])
    stale_request = operation("edit", [current], patch={"content_text": "A correction."})
    protected = await memory.set_manual_protection(current["node_id"], True)
    assert protected["updated_at"] > current["updated_at"]
    assert protected["content_updated_at"] == 100
    with pytest.raises(ValueError, match="stale|changed|snapshot"):
        await apply(memory, current["space_id"], stale_request)
    # The failed lease must finish before a fresh independent governance job.
    running = await memory.database.fetchall("SELECT job_id,lease_token FROM memory_jobs WHERE state='running'")
    jobs = JobStore(memory.database)
    for row in running:
        await jobs.cancel(row["job_id"])
    await apply(memory, current["space_id"], operation("edit", [protected], patch={"content_text": "A correction."}))
    edited = await memory.get_node(current["node_id"])
    assert edited["content_updated_at"] == 100
    assert [v["content_updated_at"] for v in await memory.list_node_versions(current["node_id"])] == [100, 100]
    disabled = await memory.update_node_status(current["node_id"], "disabled")
    assert disabled["content_updated_at"] == 100
    assert disabled["updated_at"] >= edited["updated_at"]


async def test_merge_and_split_use_selected_source_snapshots_after_raw_cleanup(observed_store):
    memory, observations, _ = observed_store
    first = await create(memory, ["early"])
    second = await create(memory, ["late"], text="Another activity.")
    observations.clear()
    await apply(memory, first["space_id"], operation("merge", [first, second]))
    merged = await memory.get_node(first["node_id"])
    assert merged["content_updated_at"] == 600
    request = operation("split", [merged], parts=[
        {"ref": "earlier", "content_text": "Earlier activity.", "importance": 0.5, "source_ids": ["early"]},
        {"ref": "later", "content_text": "Later activity.", "importance": 0.5, "source_ids": ["late"]},
    ])
    result, _ = await apply(memory, first["space_id"], request)
    for ref, expected in (("earlier", 100), ("later", 600)):
        child = await memory.get_node(result["node_refs"][ref])
        assert child["content_updated_at"] == expected
        assert (await memory.list_node_versions(child["node_id"]))[0]["content_updated_at"] == expected


async def synthesize(memory, nodes, old=None, *, text="A shared synthetic activity programme."):
    jobs, topics = JobStore(memory.database), TopicStore(memory)
    space_id, generation_id = nodes[0]["space_id"], nodes[0]["generation_id"]
    await jobs.submit("topic_consolidate", space_id, {}, uuid.uuid4().hex)
    job = await jobs.claim("test", lease_seconds=300)
    batch = await topics.prepare(space_id, generation_id, [n["node_id"] for n in nodes] + ([old["node_id"]] if old else []))
    result = await topics.apply(job, batch, topic_output(nodes, old, content=text))
    await jobs.finish(job["job_id"], job["lease_token"], result=result)
    return await memory.get_node(result["topic_ids"][0])


async def test_topic_inherits_members_and_explicit_confirmation_advances(observed_store):
    memory, _, _ = observed_store
    nodes = [await create(memory, [sid], text=f"Activity {sid}.") for sid in ("early", "middle", "late")]
    topic = await synthesize(memory, nodes[:2])
    assert topic["content_updated_at"] == 200
    same = await synthesize(memory, nodes, topic, text=topic["content_text"])
    assert same["content_updated_at"] == 600
    for field in ("content_version", "updated_at", "strength_reset_at", "strength_reset_round"):
        assert same[field] == topic[field]
    updated = await synthesize(memory, nodes, same, text="The programme now includes a further activity.")
    assert updated["content_updated_at"] == 600
    assert [v["content_updated_at"] for v in await memory.list_node_versions(topic["node_id"])] == [600, 600]
    # Scheduling continues to use processing time rather than old source time.
    assert await TopicStore(memory).dirty_nodes(nodes[0]["space_id"], nodes[0]["generation_id"], before=600) == []


async def test_membership_only_ignores_new_sources_and_later_member_confirmation(extraction_env):
    memory, service = extraction_env[1:]
    first, _ = await extract(extraction_env, 1)
    second, _ = await extract(extraction_env, 2, text="A second synthetic activity.")
    topic = await synthesize(memory, [first, second])
    first, _ = await extract(extraction_env, 5, first)
    assert first["content_updated_at"] == 500
    third, _ = await extract(extraction_env, 3, text="A third synthetic activity.")
    batch, source = await prepare(extraction_env, number=9,
                                  known=(topic["node_id"], third["node_id"]))
    candidate = output(edges=[{
        "from_ref": topic["node_id"], "to_ref": third["node_id"],
        "relation_type": "contains", "source_ids": [source.source_id],
    }])
    await service.apply(batch, json.dumps(candidate), model_id="synthetic")
    current = await memory.get_node(topic["node_id"])
    assert current["content_updated_at"] == 200
    assert current["updated_at"] == topic["updated_at"]
    versions = await memory.list_node_versions(topic["node_id"])
    assert versions[0]["content_updated_at"] == 200
    assert versions[0]["derivations"][-1]["operation"] == "membership"


async def test_reads_and_reopen_keep_snapshot_without_raw_records(observed_store):
    memory, observations, calls = observed_store
    current = await create(memory, ["early", "middle"])
    observations.clear()
    calls.clear()
    assert (await memory.get_node(current["node_id"]))["content_updated_at"] == 200
    assert (await memory.list_nodes(space_id=current["space_id"]))[0]["content_updated_at"] == 200
    assert (await memory.list_node_versions(current["node_id"]))[0]["content_updated_at"] == 200
    assert calls == []
    await memory.close()
    await memory.open()
    assert (await memory.get_node(current["node_id"]))["content_updated_at"] == 200
    snapshots = await memory.database.fetchall("SELECT observed_at FROM memory_derivation_sources ORDER BY observed_at")
    assert [row["observed_at"] for row in snapshots] == [100, 200]


async def test_manual_topic_edit_inherits_frozen_topic_not_later_child_confirmation(extraction_env):
    memory = extraction_env[1]
    first, _ = await extract(extraction_env, 1)
    second, _ = await extract(extraction_env, 2, text="A second synthetic activity.")
    topic = await synthesize(memory, [first, second])
    confirmed, _ = await extract(extraction_env, 3, first)
    assert confirmed["content_version"] == first["content_version"]
    assert confirmed["content_updated_at"] == 300
    assert (await memory.get_node(topic["node_id"]))["content_updated_at"] == 200
    await apply(memory, topic["space_id"], operation(
        "edit", [topic], patch={"content_text": "A corrected description of the same programme."}))
    current = await memory.get_node(topic["node_id"])
    assert current["content_updated_at"] == 200
    assert [v["content_updated_at"] for v in await memory.list_node_versions(topic["node_id"])] == [200, 200]


async def test_ordinary_parent_before_child_uses_final_member_observation(extraction_env):
    sources, memory, service = extraction_env
    early = await capture(sources, "A programme was announced.", number=2)
    later = await capture(sources, "An activity in that programme was completed.", number=10)
    space = await memory.get_or_create_space(private_space("p", early.logical_user_id))
    batch = await service.prepare(
        space_id=space["space_id"], source_ids=(early.source_id, later.source_id),
        persona="A synthetic companion.", known_node_ids=(),
    )
    candidate = output([
        node(early.source_id, ref="local_topic", kind="topic", content="A programme includes independent activities."),
        node(later.source_id, ref="local_member", content="An independent activity was completed."),
        node(early.source_id, ref="local_other", content="A separate opening activity was arranged."),
    ], edges=[{
        "from_ref": "local_topic", "to_ref": "local_member", "relation_type": "contains",
        "source_ids": [later.source_id],
    }, {
        "from_ref": "local_topic", "to_ref": "local_other", "relation_type": "contains",
        "source_ids": [early.source_id],
    }])
    result = await service.apply(batch, json.dumps(candidate), model_id="synthetic")
    refs = result["result"]["node_refs"]
    topic = await memory.get_node(refs["local_topic"])
    member = await memory.get_node(refs["local_member"])
    assert member["content_updated_at"] == 1000
    assert topic["content_updated_at"] == 1000
    version = (await memory.list_node_versions(topic["node_id"]))[0]
    assert version["content_updated_at"] == 1000
    assert {"kind": "node", "entity_id": member["node_id"], "version_no": 1} in version["derivations"][0]["base_versions"]


async def test_topic_confirmation_without_member_basis_does_not_refresh_old_members(extraction_env):
    memory, service = extraction_env[1:]
    first, _ = await extract(extraction_env, 1)
    second, _ = await extract(extraction_env, 2, text="A second synthetic activity.")
    topic = await synthesize(memory, [first, second])
    confirmed, _ = await extract(extraction_env, 5, first)
    assert confirmed["content_version"] == first["content_version"]
    assert confirmed["content_updated_at"] == 500
    batch, source = await prepare(extraction_env, number=3, known=(topic["node_id"],))
    candidate = output([node(source.source_id, old=topic, kind="topic", content=topic["content_text"])])
    candidate["node_changes"][0]["title"] = topic["title"]
    await service.apply(batch, json.dumps(candidate), model_id="synthetic")
    current = await memory.get_node(topic["node_id"])
    assert current["content_version"] == topic["content_version"]
    assert current["content_updated_at"] == 300
    versions = await memory.list_node_versions(topic["node_id"])
    assert versions[0]["content_updated_at"] == 300
    assert versions[0]["derivations"][-1]["operation"] == "confirm"
    assert versions[0]["derivations"][-1]["base_versions"] == []
