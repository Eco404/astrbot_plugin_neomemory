"""Persist direct citations and real version baselines without inventing evidence."""

import asyncio
import json

import pytest
import pytest_asyncio

from core.extraction_apply import ExtractionService
from core.memory_spaces import private_space
from storage.governance_store import GovernanceService
from storage.job_store import JobStore
from storage.memory_store import MemoryStore
from storage.source_store import SourceStore
from tests.test_extraction_apply import node, prepare, profile
from tests.test_extraction_apply import output
from tests.test_governance import apply, operation, setup_nodes
from tests.test_maintenance_engine import enqueue_and_claim, execute, restart
from tests.test_maintenance_engine import environment as _maintenance_fixture
from tests.test_maintenance_engine import output as maintenance_output, node as maintenance_node

maintenance_environment = _maintenance_fixture


@pytest_asyncio.fixture
async def stores(tmp_path):
    sources = SourceStore(tmp_path / "source.db")
    memory = MemoryStore(tmp_path / "memory.db", source_exists=sources.has_source)
    await sources.open()
    await memory.open()
    try:
        yield sources, memory, ExtractionService(sources, memory)
    finally:
        await memory.close()
        await sources.close()


def relationship(source_id, user, version, text):
    return {
        "logical_user_id": user, "expected_version": version, "long_term": {"op": "replace", "body": text},
        "aftereffect": {"op": "keep", "body": ""}, "source_ids": [source_id],
    }


@pytest.mark.asyncio
async def test_each_update_keeps_its_direct_citations_and_previous_snapshot(stores):
    _, memory, service = stores
    old = None
    source_ids = []
    batches = []
    for index, text in enumerate(("我喜欢徒步。", "我改为游泳，仍喜欢摄影。", "我现在主要骑车，仍喜欢摄影。"), 1):
        batch, source = await prepare(stores, number=index, text=text, known=(old["node_id"],) if old else ())
        source_ids.append(source.source_id)
        batches.append(batch.batch_id)
        candidate = output(
            [node(source.source_id, content=text, old=old)],
            profiles=[profile(source.source_id, source.logical_user_id, version=index - 1, description=text)],
            relationship=relationship(source.source_id, source.logical_user_id, index - 1, f"我理解了新的选择 {index}。"),
        )
        raw = json.dumps(candidate)
        receipt = await service.apply(batch, raw, model_id="synthetic")
        await service.apply(batch, raw, model_id="synthetic")
        old = await memory.get_node(receipt["result"]["node_refs"]["local_interview"])
    profile_row = (await memory.list_profiles(space_id=batch.space["space_id"]))[0]
    relation = await memory.get_relationship(space_id=batch.space["space_id"], logical_user_id=source.logical_user_id)
    kinds = (
        ("node", old["node_id"], await memory.list_node_versions(old["node_id"])),
        ("profile", profile_row["profile_id"], await memory.list_profile_versions(profile_row["profile_id"])),
        ("relationship", relation["relationship_id"], await memory.list_relationship_versions(relation["relationship_id"])),
    )
    for kind, entity_id, versions in kinds:
        assert [v["version_no"] for v in versions] == [1, 2, 3]
        for index, version in enumerate(versions):
            assert version["provenance_status"] == "recorded"
            assert len(version["derivations"]) == 1
            derivation = version["derivations"][0]
            assert derivation["sources"] == [{"source_id": source_ids[index], "source_kind": "source_record"}]
            assert derivation["batch_id"] == batches[index]
            assert derivation["base_versions"] == ([{"kind": kind, "entity_id": entity_id, "version_no": index}] if index else [])
        assert await memory.list_version_derivations(kind, entity_id, 2) == versions[1]["derivations"]
    assert (await memory.database.fetchone("SELECT COUNT(*) AS n FROM memory_derivations"))["n"] == 9
    assert len(await memory.list_node_sources(old["node_id"])) == 3
    assert await memory.database.fetchall("PRAGMA foreign_key_check") == []


@pytest.mark.asyncio
async def test_identical_content_new_citations_do_not_create_false_versions(stores):
    _, memory, service = stores
    old = None
    source_ids = []
    for number in (1, 2):
        batch, source = await prepare(stores, number=number, known=(old["node_id"],) if old else ())
        source_ids.append(source.source_id)
        raw = json.dumps(output(
            [node(source.source_id, old=old, content="我记得对方正在准备面试。")],
            profiles=[profile(source.source_id, source.logical_user_id, version=number - 1)],
            relationship=relationship(source.source_id, source.logical_user_id, number - 1, "我愿意支持对方。"),
        ))
        receipt = await service.apply(batch, raw, model_id="synthetic")
        old = await memory.get_node(receipt["result"]["node_refs"]["local_interview"])
    profile_row = (await memory.list_profiles(space_id=batch.space["space_id"]))[0]
    relation = await memory.get_relationship(space_id=batch.space["space_id"], logical_user_id=source.logical_user_id)
    for versions in (
        await memory.list_node_versions(old["node_id"]),
        await memory.list_profile_versions(profile_row["profile_id"]),
    ):
        assert len(versions) == 1
        assert [d["operation"] for d in versions[0]["derivations"]] == ["create", "confirm"]
        assert [d["sources"][0]["source_id"] for d in versions[0]["derivations"]] == source_ids
        assert versions[0]["derivations"][1]["base_versions"] == []


    versions = await memory.list_relationship_versions(relation["relationship_id"])
    assert [v["state"]["long_term"]["observed_at"] for v in versions] == [100, 200]
    assert [v["derivations"][0]["sources"][0]["source_id"] for v in versions] == source_ids


@pytest.mark.asyncio
async def test_program_replaces_model_versions_before_commit(stores):
    _, memory, service = stores
    batch, source = await prepare(stores)
    bad = output([node(source.source_id)], profiles=[profile(source.source_id, source.logical_user_id, version=7)])
    await service.apply(batch, json.dumps(bad), model_id="synthetic")
    profile_row = (await memory.list_profiles(space_id=batch.space["space_id"]))[0]
    assert profile_row["content_version"] == 1
    assert (await memory.get_active_generation(batch.space["space_id"]))["activity_round"] == 1


@pytest.mark.asyncio
async def test_receipt_and_provenance_failure_are_atomic(stores, monkeypatch):
    _, memory, service = stores
    batch, source = await prepare(stores)

    async def fail(*args, **kwargs):
        raise RuntimeError("synthetic write failure")

    monkeypatch.setattr("storage.extraction_store.record_derivation", fail)
    with pytest.raises(RuntimeError, match="synthetic write failure"):
        await service.apply(batch, json.dumps(output([node(source.source_id)])), model_id="synthetic")
    assert await memory.get_extraction_batch(batch.batch_id) is None
    assert await memory.database.fetchall("SELECT * FROM memory_node_versions") == []


@pytest.mark.asyncio
async def test_manual_edit_split_merge_and_source_changes_do_not_rewrite_history(tmp_path):
    memory = MemoryStore(tmp_path / "memory.db")
    await memory.open()
    try:
        space, (original,) = await setup_nodes(memory)
        request = operation("edit", [original], patch={"content_text": "人工确认的新表述。"})
        result, job = await apply(memory, space["space_id"], request, finish=False)
        await GovernanceService(memory).apply(space["space_id"], request, job_id=job["job_id"], lease_token=job["lease_token"])
        await JobStore(memory.database).finish(job["job_id"], job["lease_token"], result=result)
        current = await memory.get_node(original["node_id"])
        versions = await memory.list_node_versions(current["node_id"])
        assert len(versions[1]["derivations"]) == 1
        edited = versions[1]["derivations"][0]
        assert edited["editor_kind"] == "manual" and edited["sources"] == []
        assert edited["governance_job_id"] == job["job_id"]
        assert edited["base_versions"] == [{"kind": "node", "entity_id": original["node_id"], "version_no": 1}]
        await memory.add_node_source(current["node_id"], "later-linked-source")
        await memory.remove_node_source(current["node_id"], "later-linked-source")
        assert await memory.list_node_versions(current["node_id"]) == versions
        split, _ = await apply(memory, space["space_id"], operation("split", [current], parts=[
            {"ref": "left", "content_text": "第一项。", "importance": 0.4, "source_ids": ["source_0"]},
            {"ref": "right", "content_text": "第二项。", "importance": 0.4, "source_ids": ["source_0"]},
        ]))
        parts = [await memory.get_node(value) for value in split["node_refs"].values()]
        for part in parts:
            derived = (await memory.list_node_versions(part["node_id"]))[0]["derivations"][0]
            assert derived["operation"] == "split"
            assert derived["sources"] == [{"source_id": "source_0", "source_kind": "source_record"}]
            assert derived["base_versions"][0]["version_no"] == 2
        await apply(memory, space["space_id"], operation("merge", parts, content_text=parts[0]["content_text"]))
        merged = await memory.list_node_versions(parts[0]["node_id"])
        assert len(merged) == 1
        assert merged[0]["derivations"][-1]["operation"] == "merge"
        assert merged[0]["derivations"][-1]["sources"] == []
        assert merged[0]["derivations"][-1]["base_versions"] == [{"kind": "node", "entity_id": parts[1]["node_id"], "version_no": 1}]
        assert await memory.database.fetchall("PRAGMA foreign_key_check") == []
    finally:
        await memory.close()


@pytest.mark.asyncio
async def test_rebuild_resume_keeps_one_provenance_set_in_the_new_generation(maintenance_environment, monkeypatch):
    env = maintenance_environment
    old = await env.memory.create_node(
        space_id=env.space["space_id"], node_type="event", content_text="旧构建代的事件。",
        source_ids=[env.records[0].source_id],
    )
    env.adapter.output = json.dumps(maintenance_output(
        [maintenance_node(env.records[0].source_id)],
        profiles=[profile(env.records[0].source_id, env.records[0].logical_user_id)],
        relationship={"logical_user_id": env.records[0].logical_user_id, "expected_version": 0,
                      "source_ids": [env.records[0].source_id],
                      "long_term": {"op": "replace", "body": "我愿意支持对方。"},
                      "aftereffect": {"op": "keep", "body": ""}},
    ))
    job = await enqueue_and_claim(env, "rebuild")
    original = env.engine.extraction.apply

    async def commit_then_interrupt(*args, **kwargs):
        await original(*args, **kwargs)
        raise asyncio.CancelledError()

    monkeypatch.setattr(env.engine.extraction, "apply", commit_then_interrupt)
    with pytest.raises(asyncio.CancelledError):
        await env.engine.runner._execute(job)
    restart(env)
    completed = await execute(env, await env.engine.jobs.claim("resumed"))
    assert completed["state"] == "succeeded"
    assert len(env.adapter.calls) == 1
    active = await env.memory.get_active_generation(env.space["space_id"])
    current = (await env.memory.list_nodes(space_id=env.space["space_id"]))[0]
    assert active["generation_id"] != old["generation_id"]
    assert current["generation_id"] == active["generation_id"]
    assert current["node_id"] != old["node_id"]
    assert len(await env.memory.list_version_derivations("node", current["node_id"])) == 1
    assert await env.memory.get_node(old["node_id"]) is None
    assert await env.memory.database.fetchone(
        "SELECT 1 FROM memory_node_versions WHERE node_id=?", (old["node_id"],),
    ) is None
    assert (await env.memory.database.fetchone("SELECT COUNT(*) AS n FROM memory_derivations"))["n"] == 3


@pytest.mark.asyncio
async def test_public_context_writes_are_versioned_without_fabricated_citations(tmp_path):
    memory = MemoryStore(tmp_path / "memory.db")
    await memory.open()
    try:
        space = await memory.get_or_create_space(private_space("p", "u"))
        for text, status in (("第一版", "active"), ("第二版", "active"), ("第二版", "disabled")):
            prof = await memory.upsert_profile(space_id=space["space_id"], logical_user_id="u", tags=[text], description=text, status=status)
            rel = await memory.upsert_relationship(space_id=space["space_id"], logical_user_id="u", state_text=text, status=status)
        for versions in (await memory.list_profile_versions(prof["profile_id"]), await memory.list_relationship_versions(rel["relationship_id"])):
            assert [v["version_no"] for v in versions] == [1, 2, 3]
            assert [v["status"] for v in versions] == ["active", "active", "disabled"]
            assert all(v["derivations"][0]["sources"] == [] for v in versions)
            assert all(v["derivations"][0]["editor_kind"] == "system" for v in versions)
        for kind in ("node", "profile", "relationship"):
            with pytest.raises(KeyError):
                await memory.list_version_derivations(kind, "missing")
    finally:
        await memory.close()
