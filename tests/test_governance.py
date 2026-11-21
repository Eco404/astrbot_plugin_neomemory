"""Manual operations must remain atomic, scoped, versioned and recoverable."""

import json
import uuid

import pytest
import pytest_asyncio

from core.memory_spaces import private_space
from storage.governance_store import GovernanceService, validate_operation
from storage.job_store import JobStore, LeaseLost
from storage.memory_store import MemoryStore


@pytest_asyncio.fixture
async def store(tmp_path):
    memory = MemoryStore(tmp_path / "memory.db")
    await memory.open()
    try:
        yield memory
    finally:
        await memory.close()


async def setup_nodes(store, kinds=("event",), *, user="u"):
    space = await store.get_or_create_space(private_space("p", user))
    nodes = []
    for index, kind in enumerate(kinds):
        nodes.append(
            await store.create_node(
                space_id=space["space_id"],
                node_type=kind,
                content_text=f"original {kind} {index}",
                source_ids=[f"source_{index}"],
                importance=0.5,
                participants=[
                    {"participant_kind": "logical_user", "participant_id": user}
                ],
            )
        )
    return space, nodes


def operation(action, nodes, **kwargs):
    base = {
        "action": action,
        "expected_generation_id": nodes[0]["generation_id"],
        "expected_versions": {
            node["node_id"]: node["content_version"] for node in nodes
        },
        "expected_updated_at": {node["node_id"]: node["updated_at"] for node in nodes},
    }
    if action == "merge":
        base.update(
            node_ids=[node["node_id"] for node in nodes],
            keep_id=nodes[0]["node_id"],
            content_text="merged narrative",
            importance=0.8,
        )
    else:
        base["node_id"] = nodes[0]["node_id"]
    return {**base, **kwargs}


async def lease(store, space_id, payload):
    jobs = JobStore(store.database)
    job = await jobs.submit("governance", space_id, payload, str(uuid.uuid4()))
    claimed = await jobs.claim("test", lease_seconds=30)
    assert claimed["job_id"] == job["job_id"]
    return jobs, claimed


async def apply(store, space_id, payload, *, finish=True):
    jobs, claimed = await lease(store, space_id, payload)
    result = await GovernanceService(store).apply(
        space_id, payload, job_id=claimed["job_id"], lease_token=claimed["lease_token"]
    )
    if finish:
        await jobs.finish(claimed["job_id"], claimed["lease_token"], result=result)
    return result, claimed


@pytest.mark.asyncio
async def test_edit_is_protected_versioned_strength_reset_and_idempotent(store):
    space, (node,) = await setup_nodes(store)
    await store.database.execute(
        "UPDATE memory_nodes SET reinforcement=.15,strength_reset_at=1 WHERE node_id=?",
        (node["node_id"],),
    )
    request = operation(
        "edit",
        [node],
        patch={
            "content_text": "corrected fact",
            "importance": 0.7,
            "event_time_start": 10,
            "event_time_end": 20,
        },
    )
    result, claimed = await apply(store, space["space_id"], request, finish=False)
    current = await store.get_node(node["node_id"])
    assert current["content_text"] == "corrected fact"
    assert current["content_version"] == 2
    assert current["manual_protected"]
    assert current["reinforcement"] == 0
    assert current["strength"] == 0.7
    assert current["strength_reset_at"] > 1
    replay = await GovernanceService(store).apply(
        space["space_id"],
        request,
        job_id=claimed["job_id"],
        lease_token=claimed["lease_token"],
    )
    assert replay == {**result, "replayed": True}
    versions = await store.list_node_versions(node["node_id"])
    assert len(versions) == 2
    audit = await GovernanceService(store).list_audit(space["space_id"])
    assert len(audit) == 1
    assert "corrected fact" not in json.dumps(audit)
    assert audit[0]["result"]["before_versions"] == {node["node_id"]: 1}
    changed_request = operation(
        "edit", [node], patch={"content_text": "different fact"}
    )
    with pytest.raises(ValueError, match="different governance"):
        await GovernanceService(store).apply(
            space["space_id"],
            changed_request,
            job_id=claimed["job_id"],
            lease_token=claimed["lease_token"],
        )


@pytest.mark.asyncio
async def test_edit_clears_nullable_fields_and_replaces_participants(store):
    space, (node,) = await setup_nodes(store)
    await apply(
        store,
        space["space_id"],
        operation(
            "edit",
            [node],
            patch={
                "event_time_start": 5,
                "event_time_end": 7,
                "event_time_text": "last week",
            },
        ),
    )
    node = await store.get_node(node["node_id"])
    original_version = node["content_version"]
    await apply(
        store,
        space["space_id"],
        operation(
            "edit",
            [node],
            patch={
                "event_time_start": None,
                "event_time_end": None,
                "event_time_text": None,
                "participants": [],
            },
        ),
    )
    current = await store.get_node(node["node_id"])
    assert current["event_time_start"] is None
    assert current["event_time_end"] is None
    assert current["event_time_text"] is None
    assert current["content_version"] == original_version
    assert await store.list_node_participants(node["node_id"]) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", ["space", "generation", "version", "metadata", "lease"]
)
async def test_scope_snapshot_and_lease_guards_leave_no_mutation(store, case):
    space, (node,) = await setup_nodes(store)
    request = operation("edit", [node], patch={"content_text": "must not apply"})
    target_space = space["space_id"]
    if case == "space":
        other, _ = await setup_nodes(store, user="other")
        target_space = other["space_id"]
        request["expected_generation_id"] = (
            await store.get_active_generation(target_space)
        )["generation_id"]
    if case == "generation":
        generation = await store.create_generation(target_space)
        await store.activate_generation(target_space, generation["generation_id"])
    if case == "version":
        request["expected_versions"][node["node_id"]] += 1
    if case == "metadata":
        await store.set_manual_protection(node["node_id"], True)
    _, claimed = await lease(store, target_space, request)
    with pytest.raises((ValueError, LeaseLost)):
        await GovernanceService(store).apply(
            target_space,
            request,
            job_id=claimed["job_id"],
            lease_token="wrong" if case == "lease" else claimed["lease_token"],
        )
    assert (await store.get_node(node["node_id"]))["content_text"] == node[
        "content_text"
    ]
    assert await GovernanceService(store).list_audit(target_space) == []


@pytest.mark.asyncio
async def test_metadata_snapshot_required_even_when_content_version_unchanged(store):
    _, (node,) = await setup_nodes(store)
    request = operation("protect", [node])
    del request["expected_updated_at"]
    with pytest.raises(ValueError, match="expected_updated_at"):
        validate_operation(request)


@pytest.mark.asyncio
async def test_delete_tombstone_preserves_provenance_and_marks_ancestors_stale(store):
    space, (topic, event, atomic) = await setup_nodes(
        store, ("topic", "event", "atomic")
    )
    await store.add_edge(topic["node_id"], event["node_id"], "contains")
    await store.add_edge(event["node_id"], atomic["node_id"], "contains")
    await store.add_coverage(event["node_id"], atomic["node_id"])
    result, _ = await apply(store, space["space_id"], operation("delete", [atomic]))
    assert result["deletion_kind"] == "derived_tombstone"
    assert set(result["stale_parent_ids"]) == {event["node_id"], topic["node_id"]}
    for parent in (topic, event):
        current = await store.get_node(parent["node_id"])
        assert current["metadata"]["summary_stale"] is True
        assert current["content_text"] == parent["content_text"]
        assert current["content_version"] == 1
    current = await store.get_node(atomic["node_id"])
    assert current["status"] == "deleted" and current["deleted_at"]
    assert len(await store.list_node_versions(atomic["node_id"])) == 1
    assert len(await store.list_node_sources(atomic["node_id"])) == 1
    assert await store.list_edges(atomic["node_id"]) == []
    assert await store.database.fetchall("SELECT * FROM node_coverage") == []


@pytest.mark.asyncio
async def test_protection_changes_leave_parent_factual_summary_and_strength_alone(
    store,
):
    space, (event, atomic) = await setup_nodes(store, ("event", "atomic"))
    await store.add_edge(event["node_id"], atomic["node_id"], "contains")
    await store.add_coverage(event["node_id"], atomic["node_id"])
    for action, flag in (("protect", True), ("unprotect", False)):
        old = await store.get_node(atomic["node_id"])
        await apply(store, space["space_id"], operation(action, [old]))
        current = await store.get_node(atomic["node_id"])
        assert current["manual_protected"] is flag
        assert current["strength_reset_at"] == old["strength_reset_at"]
        assert current["content_version"] == old["content_version"]
        assert (
            "summary_stale" not in (await store.get_node(event["node_id"]))["metadata"]
        )
        assert len(await store.database.fetchall("SELECT * FROM node_coverage")) == 1


@pytest.mark.asyncio
async def test_disable_restore_preserves_manual_protection_and_cannot_resurrect_deleted(
    store,
):
    space, (node,) = await setup_nodes(store)
    await apply(store, space["space_id"], operation("disable", [node]))
    node = await store.get_node(node["node_id"])
    assert node["status"] == "disabled" and node["manual_protected"]
    await apply(store, space["space_id"], operation("restore", [node]))
    node = await store.get_node(node["node_id"])
    assert node["status"] == "active" and node["manual_protected"]
    await apply(store, space["space_id"], operation("delete", [node]))
    node = await store.get_node(node["node_id"])
    with pytest.raises(ValueError, match="deleted or superseded"):
        await apply(store, space["space_id"], operation("restore", [node]))


def split_request(node, **kwargs):
    return operation(
        "split",
        [node],
        parts=[
            {
                "ref": "first",
                "content_text": "first event",
                "importance": 0.5,
                "source_ids": ["source_0"],
            },
            {
                "ref": "second",
                "content_text": "second event",
                "importance": 0.8,
                "source_ids": ["source_0"],
            },
        ],
        **kwargs,
    )


@pytest.mark.asyncio
async def test_split_has_explicit_provenance_and_no_guessed_relationships(store):
    space, (node, topic) = await setup_nodes(store, ("event", "topic"))
    await store.add_edge(topic["node_id"], node["node_id"], "contains")
    result, _ = await apply(store, space["space_id"], split_request(node))
    assert len(result["node_refs"]) == 2
    assert (await store.get_node(node["node_id"]))["status"] == "superseded"
    for node_id in result["node_refs"].values():
        current = await store.get_node(node_id)
        assert current["node_type"] == "event"
        assert current["manual_protected"]
        assert [row["source_id"] for row in await store.list_node_sources(node_id)] == [
            "source_0"
        ]
        assert await store.list_node_participants(node_id) == []
        assert await store.list_edges(node_id) == []
    assert (await store.get_node(topic["node_id"]))["metadata"]["summary_stale"]


@pytest.mark.asyncio
async def test_split_explicit_relation_resolves_new_refs(store):
    space, (node,) = await setup_nodes(store)
    request = split_request(
        node,
        relations=[
            {"source_ref": "first", "target_ref": "second", "relation_type": "follows"}
        ],
    )
    result, _ = await apply(store, space["space_id"], request)
    (edge,) = await store.list_edges()
    assert edge["source_node_id"] == result["node_refs"]["first"]
    assert edge["target_node_id"] == result["node_refs"]["second"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["source", "relation", "time"])
async def test_split_failure_rolls_back_even_after_first_child_insert(store, failure):
    space, (node,) = await setup_nodes(store)
    request = split_request(node)
    if failure == "source":
        request["parts"][1]["source_ids"] = ["not-original"]
    elif failure == "time":
        request["parts"][1].update(event_time_start=20, event_time_end=10)
    else:
        request["relations"] = [
            {"source_ref": "first", "target_ref": "second", "relation_type": "contains"}
        ]
    with pytest.raises(ValueError):
        await apply(store, space["space_id"], request)
    assert len(await store.list_nodes(space_id=space["space_id"])) == 1
    assert (await store.get_node(node["node_id"]))["status"] == "active"
    assert await GovernanceService(store).list_audit(space["space_id"]) == []


@pytest.mark.asyncio
async def test_merge_unions_sources_participants_edges_without_self_edges(store):
    space, (first, second, topic, atomic) = await setup_nodes(
        store, ("event", "event", "topic", "atomic")
    )
    await store.add_node_participant(second["node_id"], "external", "friend")
    for node in (first, second):
        await store.add_edge(topic["node_id"], node["node_id"], "contains")
        await store.add_edge(node["node_id"], atomic["node_id"], "contains")
        await store.add_coverage(node["node_id"], atomic["node_id"])
    await store.add_edge(first["node_id"], second["node_id"], "related")
    await apply(store, space["space_id"], operation("merge", [first, second]))
    current = await store.get_node(first["node_id"])
    assert current["content_text"] == "merged narrative"
    assert current["content_version"] == 2
    assert current["manual_protected"]
    assert (await store.get_node(second["node_id"]))["status"] == "superseded"
    assert {
        row["source_id"] for row in await store.list_node_sources(first["node_id"])
    } == {"source_0", "source_1"}
    assert len(await store.list_node_participants(first["node_id"])) == 2
    edges = await store.list_edges()
    assert len(edges) == 2
    assert all(edge["source_node_id"] != edge["target_node_id"] for edge in edges)
    assert await store.database.fetchall("SELECT * FROM node_coverage") == []


@pytest.mark.asyncio
async def test_merge_rejects_cycle_after_redirect_and_rolls_back_content_and_sources(
    store,
):
    space, (first, second, middle) = await setup_nodes(
        store, ("event", "event", "event")
    )
    await store.add_edge(first["node_id"], middle["node_id"], "supersedes")
    await store.add_edge(middle["node_id"], second["node_id"], "supersedes")
    with pytest.raises(ValueError, match="cycle"):
        await apply(store, space["space_id"], operation("merge", [first, second]))
    assert (await store.get_node(first["node_id"]))["content_version"] == 1
    assert (await store.get_node(second["node_id"]))["status"] == "active"
    assert len(await store.list_node_sources(first["node_id"])) == 1
    assert len(await store.list_edges()) == 2
    assert await GovernanceService(store).list_audit(space["space_id"]) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["type", "status", "foreign"])
async def test_merge_rejects_incompatible_targets(store, failure):
    space, nodes = await setup_nodes(
        store, ("event", "topic" if failure == "type" else "event")
    )
    if failure == "status":
        nodes[1] = await store.update_node_status(nodes[1]["node_id"], "historical")
    elif failure == "foreign":
        _, (nodes[1],) = await setup_nodes(store, user="foreign")
    with pytest.raises(ValueError):
        await apply(store, space["space_id"], operation("merge", nodes))
    assert (await store.get_node(nodes[0]["node_id"]))["content_version"] == 1


@pytest.mark.asyncio
async def test_explicit_edit_relations_replaces_edges_and_clears_stale_after_review(
    store,
):
    space, (event, atomic, other) = await setup_nodes(
        store, ("event", "atomic", "atomic")
    )
    await store.add_edge(event["node_id"], atomic["node_id"], "contains")
    await apply(
        store,
        space["space_id"],
        operation("edit", [atomic], patch={"content_text": "corrected detail"}),
    )
    event = await store.get_node(event["node_id"])
    assert event["metadata"]["summary_stale"]
    request = operation(
        "edit",
        [event, other],
        patch={"content_text": event["content_text"]},
        relations=[
            {
                "source_ref": event["node_id"],
                "target_ref": other["node_id"],
                "relation_type": "contains",
            }
        ],
    )
    await apply(store, space["space_id"], request)
    current = await store.get_node(event["node_id"])
    assert "summary_stale" not in current["metadata"]
    assert current["content_version"] == 1
    (edge,) = await store.list_edges(event["node_id"])
    assert edge["target_node_id"] == other["node_id"]


@pytest.mark.asyncio
async def test_edit_relation_failure_rolls_back_body_and_old_edges(store):
    space, (event, atomic) = await setup_nodes(store, ("event", "atomic"))
    await store.add_edge(event["node_id"], atomic["node_id"], "contains")
    request = operation(
        "edit",
        [event, atomic],
        patch={"content_text": "must roll back"},
        relations=[
            {
                "source_ref": atomic["node_id"],
                "target_ref": event["node_id"],
                "relation_type": "contains",
            }
        ],
    )
    with pytest.raises(ValueError, match="contains"):
        await apply(store, space["space_id"], request)
    assert (await store.get_node(event["node_id"]))["content_text"] == event[
        "content_text"
    ]
    (edge,) = await store.list_edges()
    assert edge["source_node_id"] == event["node_id"]


@pytest.mark.asyncio
async def test_generation_audit_purge_is_transactional_and_keeps_other_spaces(store):
    space, (node,) = await setup_nodes(store)
    other, (other_node,) = await setup_nodes(store, user="other")
    for location, item in ((space, node), (other, other_node)):
        await apply(store, location["space_id"], operation("protect", [item]))
    with pytest.raises(RuntimeError):
        async with store.database.transaction() as connection:
            await GovernanceService.purge_generation(connection, node["generation_id"])
            raise RuntimeError("activation failed")
    assert len(await GovernanceService(store).list_audit(space["space_id"])) == 1
    async with store.database.transaction() as connection:
        await GovernanceService.purge_generation(connection, node["generation_id"])
    assert await GovernanceService(store).list_audit(space["space_id"]) == []
    assert len(await GovernanceService(store).list_audit(other["space_id"])) == 1


@pytest.mark.asyncio
async def test_deleting_derived_memory_does_not_modify_original_database(
    store, tmp_path
):
    from core.identity import EventIdentity
    from core.source_ingest import CapturedSource
    from storage.source_store import SourceStore

    sources = SourceStore(tmp_path / "source.db")
    await sources.open()
    try:
        captured = await sources.capture(
            CapturedSource(
                identity=EventIdentity(
                    "test",
                    "test",
                    "FriendMessage",
                    "test:u",
                    "u",
                    "",
                    "u",
                    "User",
                    "bot",
                    "m1",
                ),
                actor_kind="user",
                actor_account_id="u",
                content_text="真实来源结构中的合成测试内容",
                observed_at=100,
                persona_id="p",
            )
        )
        space = await store.get_or_create_space(
            private_space("p", captured.logical_user_id)
        )
        node = await store.create_node(
            space_id=space["space_id"],
            node_type="event",
            content_text="错误的派生解释",
            source_ids=[captured.source_id],
        )
        before = await sources.get_records((captured.source_id,))
        await apply(store, space["space_id"], operation("delete", [node]))
        assert await sources.get_records((captured.source_id,)) == before
        assert await sources.has_source(captured.source_id)
    finally:
        await sources.close()


@pytest.mark.asyncio
async def test_expired_lease_before_commit_rolls_back_entire_governance(
    store, monkeypatch
):
    import storage.governance_store as module

    space, (node,) = await setup_nodes(store)
    original = module.assert_job_lease
    calls = 0

    async def expires_on_commit(connection, *args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise LeaseLost("lease expired before commit")
        return await original(connection, *args, **kwargs)

    monkeypatch.setattr(module, "assert_job_lease", expires_on_commit)
    with pytest.raises(LeaseLost, match="expired"):
        await apply(
            store,
            space["space_id"],
            operation("edit", [node], patch={"content_text": "not committed"}),
        )
    assert (await store.get_node(node["node_id"]))["content_text"] == node[
        "content_text"
    ]
    assert len(await store.list_node_versions(node["node_id"])) == 1
    assert await GovernanceService(store).list_audit(space["space_id"]) == []


@pytest.mark.asyncio
async def test_relation_only_edit_marks_unchanged_own_summary_stale(store):
    space, (event, atomic) = await setup_nodes(store, ("event", "atomic"))
    await store.add_edge(event["node_id"], atomic["node_id"], "contains")
    result, _ = await apply(
        store, space["space_id"], operation("edit", [event], relations=[])
    )
    assert result["stale_parent_ids"] == [event["node_id"]]
    current = await store.get_node(event["node_id"])
    assert current["metadata"]["summary_stale"]
    assert current["content_text"] == event["content_text"]
