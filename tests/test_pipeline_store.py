"""Lease-fenced replay, durable cursors and atomic generation switching."""

import json
import sqlite3
import time
from dataclasses import replace

import pytest
import pytest_asyncio

from core.extraction_apply import ExtractionService, digest
from core.memory_spaces import private_space
from storage.job_store import JobConflict, JobStore, LeaseLost
from storage.memory_store import MemoryStore
from storage.pipeline_store import PipelineStore
from storage.source_store import SourceStore
from tests.test_extraction_apply import capture
from tests.test_maintenance_engine import node, output


@pytest_asyncio.fixture
async def pipeline(tmp_path):
    sources = SourceStore(tmp_path / "source.db")
    await sources.open()
    memory = MemoryStore(tmp_path / "memory.db", source_exists=sources.has_source)
    await memory.open()
    source = await capture(sources)
    space = await memory.get_or_create_space(private_space("p", source.logical_user_id))
    generation = await memory.get_active_generation(space["space_id"])
    try:
        yield (sources, memory, ExtractionService(sources, memory), JobStore(memory.database),
               PipelineStore(memory), space["space_id"], generation["generation_id"], source.source_id)
    finally:
        await memory.close()
        await sources.close()


async def claim(jobs, space_id, *, kind="rebuild", key="task"):
    await jobs.submit(kind, space_id, {}, key)
    return await jobs.claim("worker", lease_seconds=60)


async def switch_phase(jobs, job):
    await jobs.save_checkpoint(job["job_id"], job["lease_token"], checkpoint={"phase": "switch"},
                               step="switch", progress_done=1, progress_total=1)


@pytest.mark.asyncio
async def test_pipeline_state_and_empty_cursor(pipeline):
    _, _, _, _, store, _, generation_id, _ = pipeline
    assert await store.get_cursor(generation_id) == 0
    assert await store.consumed(generation_id, ()) == set()
    assert await store.get_state("discovery", 0) == 0
    await store.set_state("discovery", {"cursor": 8})
    assert await store.get_state("discovery") == {"cursor": 8}
    await store.set_state("discovery", 12)
    assert await store.get_state("discovery") == 12
    with pytest.raises(ValueError):
        await store.set_state("discovery", float("nan"))
    with pytest.raises(ValueError):
        await store.consumed(generation_id, "one-id")


@pytest.mark.asyncio
async def test_begin_rebuild_is_empty_durable_and_resumable(pipeline):
    _, memory, _, jobs, store, space_id, old_generation, source_id = pipeline
    await memory.create_node(space_id=space_id, node_type="event", content_text="旧记忆",
                             source_ids=[source_id], manual_protected=True)
    job = await claim(jobs, space_id)
    target = await store.begin_rebuild(job, space_id, old_generation)
    assert target["status"] == "building"
    assert target["parent_generation_id"] == old_generation
    assert await memory.list_nodes(space_id=space_id, generation_id=target["generation_id"]) == []
    assert (await memory.get_active_generation(space_id))["generation_id"] == old_generation
    assert (await store.begin_rebuild(job, space_id, old_generation))["generation_id"] == target["generation_id"]
    await jobs.release(job["job_id"], job["lease_token"])
    resumed = await jobs.claim("new")
    assert (await store.begin_rebuild(resumed, space_id, old_generation))["generation_id"] == target["generation_id"]
    with pytest.raises(LeaseLost):
        await store.begin_rebuild(job, space_id, old_generation)


@pytest.mark.asyncio
async def test_rebuild_creation_rejects_wrong_kind_scope_and_parent(pipeline):
    _, memory, _, jobs, store, space_id, old_generation, _ = pipeline
    job = await claim(jobs, space_id, kind="extract")
    with pytest.raises(PermissionError):
        await store.begin_rebuild(job, space_id, old_generation)
    await jobs.cancel(job["job_id"])
    rebuild = await claim(jobs, space_id, key="rebuild")
    with pytest.raises(ValueError):
        await store.begin_rebuild(rebuild, space_id, "wrong-parent")
    other = await memory.get_or_create_space(private_space("p", "other"))
    with pytest.raises(LeaseLost):
        await store.begin_rebuild(rebuild, other["space_id"], old_generation)


@pytest.mark.asyncio
async def test_atomic_activation_receipt_and_governance_cleanup(pipeline):
    _, memory, _, jobs, store, space_id, old_generation, _ = pipeline
    await memory.database.execute(
        "INSERT INTO governance_receipts(job_id,space_id,generation_id,action,actor,input_digest,result_json,committed_at) "
        "VALUES ('governed',?,?,'edit','user','digest','{}',?)", (space_id, old_generation, time.time()),
    )
    job = await claim(jobs, space_id)
    target = await store.begin_rebuild(job, space_id, old_generation)
    with pytest.raises(ValueError, match="switch phase"):
        await store.activate_rebuild(job, space_id, target["generation_id"], old_generation, result={"count": 4})
    assert (await memory.get_active_generation(space_id))["generation_id"] == old_generation
    await switch_phase(jobs, job)
    receipt = await store.activate_rebuild(job, space_id, target["generation_id"], old_generation, result={"count": 4})
    assert receipt["replayed"] is False
    assert receipt["result"] == {"count": 4}
    assert (await memory.get_active_generation(space_id))["generation_id"] == target["generation_id"]
    assert (await memory.database.fetchone("SELECT status FROM memory_generations WHERE generation_id=?", (old_generation,)))["status"] == "retired"
    assert await memory.database.fetchone("SELECT job_id FROM governance_receipts WHERE generation_id=?", (old_generation,)) is None
    assert (await store.get_rebuild_receipt(job["job_id"]))["target_generation_id"] == target["generation_id"]
    # Simulate interruption after the commit but before task completion/checkpoint.
    await jobs.release(job["job_id"], job["lease_token"])
    resumed = await jobs.claim("new")
    replay = await store.activate_rebuild(resumed, space_id, target["generation_id"], old_generation, result={"count": 4})
    assert replay["replayed"] is True
    assert replay["committed_at"] == receipt["committed_at"]
    with pytest.raises(LeaseLost):
        await store.activate_rebuild(job, space_id, target["generation_id"], old_generation, result={})


@pytest.mark.asyncio
async def test_cancel_and_expiry_cannot_create_or_activate_generation(pipeline):
    _, memory, _, jobs, store, space_id, old_generation, _ = pipeline
    cancelled = await claim(jobs, space_id, key="cancel")
    await jobs.cancel(cancelled["job_id"])
    with pytest.raises(LeaseLost):
        await store.begin_rebuild(cancelled, space_id, old_generation)
    job = await claim(jobs, space_id, key="expire")
    target = await store.begin_rebuild(job, space_id, old_generation)
    await switch_phase(jobs, job)
    await memory.database.execute("UPDATE memory_jobs SET lease_until=? WHERE job_id=?", (time.time() - 1, job["job_id"]))
    with pytest.raises(LeaseLost):
        await store.activate_rebuild(job, space_id, target["generation_id"], old_generation, result={})
    assert (await memory.get_active_generation(space_id))["generation_id"] == old_generation
    resumed = await jobs.claim("new")
    await store.activate_rebuild(resumed, space_id, target["generation_id"], old_generation, result={})


@pytest.mark.asyncio
async def test_activation_failure_rolls_back_generation_switch(pipeline):
    _, memory, _, jobs, store, space_id, old_generation, _ = pipeline
    job = await claim(jobs, space_id)
    target = await store.begin_rebuild(job, space_id, old_generation)
    await switch_phase(jobs, job)
    await memory.database.execute(
        "CREATE TRIGGER fail_rebuild_receipt BEFORE INSERT ON rebuild_receipts "
        "BEGIN SELECT RAISE(ABORT, 'injected failure'); END",
    )
    with pytest.raises(sqlite3.IntegrityError, match="injected failure"):
        await store.activate_rebuild(job, space_id, target["generation_id"], old_generation, result={})
    assert (await memory.get_active_generation(space_id))["generation_id"] == old_generation
    assert (await memory.database.fetchone("SELECT status FROM memory_generations WHERE generation_id=?", (target["generation_id"],)))["status"] == "building"
    assert await store.get_rebuild_receipt(job["job_id"]) is None


@pytest.mark.asyncio
async def test_building_extraction_cursor_and_receipt_avoid_repeat_llm(pipeline):
    _, memory, service, jobs, store, space_id, old_generation, source_id = pipeline
    job = await claim(jobs, space_id)
    target = await store.begin_rebuild(job, space_id, old_generation)
    batch = await service.prepare(space_id=space_id, source_ids=(source_id,), persona="测试人格",
                                  generation_id=target["generation_id"], job_id=job["job_id"],
                                  lease_token=job["lease_token"], allow_building=True, source_cursor=10)
    calls = []

    async def model(messages):
        calls.append(messages)
        return json.dumps(output([node(source_id)]))

    receipt = await service.run(batch, model, model_id="fake")
    assert receipt["activity_round"] == 1
    assert await store.get_cursor(target["generation_id"]) == 10
    assert await store.get_cursor(old_generation) == 0
    assert await store.consumed(target["generation_id"], [source_id, "absent"]) == {source_id}
    assert await memory.list_nodes(space_id=space_id) == []  # Old active generation remains usable.
    await jobs.release(job["job_id"], job["lease_token"])
    resumed = await jobs.claim("resumed")
    replay_batch = replace(batch, lease_token=resumed["lease_token"])
    assert replay_batch.input_digest == batch.input_digest
    assert (await service.run(replay_batch, model, model_id="fake"))["replayed"] is True
    assert len(calls) == 1
    assert await store.get_cursor(target["generation_id"]) == 10


@pytest.mark.asyncio
async def test_invalid_extraction_cannot_advance_cursor(pipeline):
    _, memory, service, jobs, store, space_id, old_generation, source_id = pipeline
    job = await claim(jobs, space_id, kind="extract")
    batch = await service.prepare(space_id=space_id, source_ids=(source_id,), persona="测试",
                                  job_id=job["job_id"], lease_token=job["lease_token"], source_cursor=10)
    invalid = node("wrong-source")
    with pytest.raises(ValueError):
        await service.apply(batch, json.dumps(output([invalid])), model_id="fake")
    assert await store.get_cursor(old_generation) == 0
    assert await store.consumed(old_generation, [source_id]) == set()
    assert (await memory.get_active_generation(space_id))["activity_round"] == 0
    await service.apply(batch, json.dumps(output()), model_id="fake")
    assert await store.get_cursor(old_generation) == 10
    assert await memory.list_nodes(space_id=space_id) == []


@pytest.mark.asyncio
async def test_cursor_failure_rolls_back_nodes_batch_and_activity_round(pipeline):
    _, memory, service, jobs, store, space_id, generation_id, source_id = pipeline
    job = await claim(jobs, space_id, kind="extract")
    batch = await service.prepare(space_id=space_id, source_ids=(source_id,), persona="测试",
                                  job_id=job["job_id"], lease_token=job["lease_token"], source_cursor=10)
    await memory.database.execute(
        "CREATE TRIGGER fail_source_cursor BEFORE INSERT ON derived_cursors "
        "BEGIN SELECT RAISE(ABORT, 'cursor write failed'); END",
    )
    with pytest.raises(sqlite3.IntegrityError, match="cursor write failed"):
        await service.apply(batch, json.dumps(output([node(source_id)])), model_id="fake")
    assert await memory.list_nodes(space_id=space_id) == []
    assert await memory.get_extraction_batch(batch.batch_id) is None
    assert await store.consumed(generation_id, [source_id]) == set()
    assert (await memory.get_active_generation(space_id))["activity_round"] == 0


@pytest.mark.asyncio
async def test_cursor_never_regresses_on_a_later_batch(pipeline):
    sources, _, service, jobs, store, space_id, generation_id, source_id = pipeline
    job = await claim(jobs, space_id, kind="extract")
    first = await service.prepare(space_id=space_id, source_ids=(source_id,), persona="测试",
                                  job_id=job["job_id"], lease_token=job["lease_token"], source_cursor=10)
    await service.apply(first, json.dumps(output()), model_id="fake")
    second_source = await capture(sources, number=2)
    second = await service.prepare(space_id=space_id, source_ids=(second_source.source_id,), persona="测试",
                                   job_id=job["job_id"], lease_token=job["lease_token"], source_cursor=5)
    await service.apply(second, json.dumps(output()), model_id="fake")
    assert await store.get_cursor(generation_id) == 10


@pytest.mark.asyncio
async def test_explicit_extraction_cannot_bypass_running_job(pipeline):
    _, _, service, jobs, _, space_id, _, source_id = pipeline
    batch = await service.prepare(space_id=space_id, source_ids=(source_id,), persona="测试")
    await claim(jobs, space_id)
    with pytest.raises(JobConflict):
        await service.apply(batch, json.dumps(output()), model_id="fake")
    with pytest.raises(JobConflict):
        await service.prepare(space_id=space_id, source_ids=(source_id,), persona="测试")


@pytest.mark.asyncio
async def test_lease_revalidation_after_model_rejects_cancelled_writer(pipeline):
    _, memory, service, jobs, store, space_id, generation_id, source_id = pipeline
    job = await claim(jobs, space_id, kind="extract")
    batch = await service.prepare(space_id=space_id, source_ids=(source_id,), persona="测试",
                                  job_id=job["job_id"], lease_token=job["lease_token"], source_cursor=4)

    async def model(messages):
        await jobs.cancel(job["job_id"])
        return json.dumps(output([node(source_id)]))

    with pytest.raises(LeaseLost):
        await service.run(batch, model, model_id="fake")
    assert await store.get_cursor(generation_id) == 0
    assert await memory.list_nodes(space_id=space_id) == []


@pytest.mark.asyncio
async def test_only_owned_building_generation_is_extractable(pipeline):
    _, memory, service, jobs, store, space_id, old_generation, source_id = pipeline
    unowned = await memory.create_generation(space_id)
    job = await claim(jobs, space_id)
    await store.begin_rebuild(job, space_id, old_generation)
    kwargs = dict(space_id=space_id, source_ids=(source_id,), persona="测试", job_id=job["job_id"],
                  lease_token=job["lease_token"])
    with pytest.raises(PermissionError, match="does not belong"):
        await service.prepare(**kwargs, generation_id=unowned["generation_id"], allow_building=True)
    with pytest.raises(PermissionError, match="must target"):
        await service.prepare(**kwargs)
    await jobs.cancel(job["job_id"])
    with pytest.raises(PermissionError):
        await service.prepare(space_id=space_id, source_ids=(source_id,), persona="测试",
                              generation_id=unowned["generation_id"], allow_building=True)
    wrong = await claim(jobs, space_id, kind="governance", key="wrong-kind")
    with pytest.raises(PermissionError, match="task kind"):
        await service.prepare(**{**kwargs, "job_id": wrong["job_id"], "lease_token": wrong["lease_token"]})


@pytest.mark.asyncio
async def test_digest_contains_frozen_input_and_excludes_runtime_lease(pipeline):
    sources, memory, _, _, _, space_id, _, source_id = pipeline
    service = ExtractionService(sources, memory)
    batch = await service.prepare(space_id=space_id, source_ids=(source_id,), persona="测试")
    expected = digest({"space": batch.space, "generation": batch.generation_id, "round": batch.expected_round,
                     "sources": batch.sources, "nodes": batch.known_nodes, "profiles": batch.profiles,
                     "relationship": batch.relationship, "persona": batch.persona, "prompt": batch.prompt_version, "schema": "extraction.v1"})
    assert batch.input_digest == expected
    assert replace(batch, job_id="one", lease_token="first").input_digest == expected
    assert replace(batch, job_id="two", lease_token="next").input_digest == expected
    assert replace(batch, source_cursor=1).input_digest != expected
    assert replace(batch, allow_building=True).input_digest != expected


async def seed_generation_content(memory, space_id, generation_id, source_id):
    """Build real version/provenance rows in an independently selected generation."""
    space = await memory.get_space(space_id)
    item = await memory.create_node(
        space_id=space_id, generation_id=generation_id, node_type="event",
        content_text="第一次一起讨论项目。", source_ids=[source_id],
    )
    await memory.update_node_content(item["node_id"], "又一起讨论了项目安排。")
    for text in ("喜欢摄影", "喜欢摄影和徒步"):
        profile = await memory.upsert_profile(
            space_id=space_id, generation_id=generation_id,
            logical_user_id=space["logical_user_id"], tags=[text], description=text,
        )
        relation = await memory.upsert_relationship(
            space_id=space_id, generation_id=generation_id,
            logical_user_id=space["logical_user_id"], state_text=f"我对相处的感觉是：{text}。",
        )
    short = await memory.upsert_short_term(
        space_id=space_id, generation_id=generation_id,
        conversation_id="conversation", summary_text="还在讨论项目安排。",
    )
    await memory.database.execute(
        "INSERT INTO short_term_sources(state_id,source_id,sequence_no) VALUES (?,?,1)",
        (short["state_id"], source_id),
    )
    await memory.database.execute(
        "INSERT INTO derived_cursors(generation_id,scan_cursor) VALUES (?,1) "
        "ON CONFLICT(generation_id) DO UPDATE SET scan_cursor=1", (generation_id,),
    )
    return item, profile, relation, short


@pytest.mark.asyncio
async def test_rebuild_discards_old_content_and_versions_but_preserves_sources_notes_and_other_space(pipeline):
    sources, memory, service, jobs, store, space_id, old_generation, source_id = pipeline
    old_node, old_profile, old_relation, old_short = await seed_generation_content(
        memory, space_id, old_generation, source_id,
    )
    old_batch = await service.prepare(space_id=space_id, source_ids=(source_id,), persona="测试人格")
    await service.apply(old_batch, json.dumps(output()), model_id="synthetic")
    note = await memory.create_note(space_id=space_id, content_text="下次记得询问项目进度。")
    other = await memory.get_or_create_space(private_space("p", "another-user"))
    other_generation = (await memory.get_active_generation(other["space_id"]))["generation_id"]
    other_node, _, _, _ = await seed_generation_content(memory, other["space_id"], other_generation, source_id)
    old_versions = await memory.list_node_versions(old_node["node_id"])
    assert [item["version_no"] for item in old_versions] == [1, 2]
    job = await claim(jobs, space_id)
    target = await store.begin_rebuild(job, space_id, old_generation)
    batch = await service.prepare(
        space_id=space_id, source_ids=(source_id,), persona="测试人格",
        generation_id=target["generation_id"], job_id=job["job_id"], lease_token=job["lease_token"],
        allow_building=True, source_cursor=1,
    )
    receipt = await service.apply(batch, json.dumps(output([node(source_id)])), model_id="synthetic")
    new_id = receipt["result"]["node_refs"]["local_interview"]
    space = await memory.get_space(space_id)
    fresh_profile = await memory.upsert_profile(
        space_id=space_id, generation_id=target["generation_id"],
        logical_user_id=space["logical_user_id"], tags=["摄影"], description="喜欢摄影。",
    )
    fresh_relation = await memory.upsert_relationship(
        space_id=space_id, generation_id=target["generation_id"],
        logical_user_id=space["logical_user_id"], state_text="我很珍惜一起讨论项目的时光。",
    )
    await memory.database.execute(
        "INSERT INTO topic_receipts(receipt_key,job_id,generation_id,input_digest,output_digest,result_json,created_at) "
        "VALUES ('retired-topic',?,?,'input','output','{}',?)", (job["job_id"], old_generation, time.time()),
    )
    invalidated = []

    async def invalidate(namespace, node_ids):
        invalidated.append((namespace, set(node_ids)))

    await switch_phase(jobs, job)
    await store.activate_rebuild(
        job, space_id, target["generation_id"], old_generation, result={}, invalidate_nodes=invalidate,
    )
    assert invalidated == [(space_id, {old_node["node_id"]})]
    assert await memory.get_node(old_node["node_id"]) is None
    for table, key, value in (
        ("memory_node_versions", "node_id", old_node["node_id"]),
        ("node_sources", "node_id", old_node["node_id"]),
        ("profile_versions", "profile_id", old_profile["profile_id"]),
        ("relationship_versions", "relationship_id", old_relation["relationship_id"]),
        ("short_term_sources", "state_id", old_short["state_id"]),
    ):
        assert await memory.database.fetchone(f"SELECT 1 FROM {table} WHERE {key}=?", (value,)) is None
    for table in ("user_profiles", "relationship_states", "short_term_states", "derived_cursors",
                  "extraction_batches", "extraction_batch_sources", "topic_receipts"):
        assert await memory.database.fetchone(f"SELECT 1 FROM {table} WHERE generation_id=?", (old_generation,)) is None
    old_version_ids = [item["node_version_id"] for item in old_versions]
    for version_id in old_version_ids:
        assert await memory.database.fetchone(
            "SELECT 1 FROM memory_derivations WHERE node_version_id=?", (version_id,),
        ) is None
    assert (await memory.get_node(new_id))["content_version"] == 1
    assert [v["version_no"] for v in await memory.list_node_versions(new_id)] == [1]
    assert [v["version_no"] for v in await memory.list_profile_versions(fresh_profile["profile_id"])] == [1]
    assert [v["version_no"] for v in await memory.list_relationship_versions(fresh_relation["relationship_id"])] == [1]
    assert (await memory.get_node(other_node["node_id"]))["content_version"] == 2
    assert await sources.has_source(source_id)
    assert (await memory.list_notes(space_id=space_id))[0]["note_id"] == note["note_id"]
    assert await memory.database.fetchall("PRAGMA foreign_key_check") == []
    # A repeated switch uses the durable receipt without deleting current content again.
    assert (await store.activate_rebuild(job, space_id, target["generation_id"], old_generation, result={}))[
        "replayed"
    ] is True
    assert (await memory.get_node(new_id))["content_version"] == 1


@pytest.mark.asyncio
async def test_rebuild_cleans_preexisting_retired_content_and_restarts_versions_on_each_rebuild(pipeline):
    _, memory, _, jobs, store, space_id, old_generation, source_id = pipeline
    older = await memory.create_generation(space_id)
    old_item, _, _, _ = await seed_generation_content(memory, space_id, older["generation_id"], source_id)
    await memory.database.execute(
        "UPDATE memory_generations SET status='retired' WHERE generation_id=?", (older["generation_id"],),
    )
    previous_item = old_item
    for index in range(2):
        job = await claim(jobs, space_id, key=f"rebuild-{index}")
        target = await store.begin_rebuild(job, space_id, old_generation)
        fresh = await memory.create_node(
            space_id=space_id, generation_id=target["generation_id"], node_type="event",
            content_text="重新构建的记忆。", source_ids=[source_id],
        )
        assert fresh["content_version"] == 1
        await memory.update_node_content(fresh["node_id"], "本次重构期间新增的经历。")
        await switch_phase(jobs, job)
        await store.activate_rebuild(job, space_id, target["generation_id"], old_generation, result={})
        assert await memory.get_node(previous_item["node_id"]) is None
        assert [v["version_no"] for v in await memory.list_node_versions(fresh["node_id"])] == [1, 2]
        await jobs.finish(job["job_id"], job["lease_token"])
        previous_item = fresh
        old_generation = target["generation_id"]
    assert await memory.database.fetchall("PRAGMA foreign_key_check") == []


@pytest.mark.asyncio
async def test_rebuild_publication_failure_restores_deleted_content_and_versions(pipeline):
    _, memory, _, jobs, store, space_id, old_generation, source_id = pipeline
    old_node, old_profile, old_relation, old_short = await seed_generation_content(
        memory, space_id, old_generation, source_id,
    )
    before = {}
    tables = ("memory_nodes", "memory_node_versions", "node_sources", "user_profiles", "profile_versions",
              "relationship_states", "relationship_versions", "memory_derivations", "memory_derivation_sources",
              "short_term_states", "short_term_sources")
    for table in tables:
        before[table] = [dict(row) for row in await memory.database.fetchall(f"SELECT * FROM {table}")]
    job = await claim(jobs, space_id)
    target = await store.begin_rebuild(job, space_id, old_generation)
    await switch_phase(jobs, job)
    await memory.database.execute(
        "CREATE TRIGGER fail_rebuild_receipt BEFORE INSERT ON rebuild_receipts "
        "BEGIN SELECT RAISE(ABORT, 'publication failed'); END",
    )
    with pytest.raises(sqlite3.IntegrityError, match="publication failed"):
        await store.activate_rebuild(job, space_id, target["generation_id"], old_generation, result={})
    for table in tables:
        assert [dict(row) for row in await memory.database.fetchall(f"SELECT * FROM {table}")] == before[table]
    assert (await memory.get_active_generation(space_id))["generation_id"] == old_generation
    assert await memory.database.fetchall("PRAGMA foreign_key_check") == []
