"""Replay real source records through fake model output and atomic memory writes."""

import asyncio
import json
from dataclasses import replace

import pytest
import pytest_asyncio

from core.extraction_apply import ExtractionService
from core.identity import EventIdentity
from core.memory_spaces import group_space, private_space
from core.source_ingest import CapturedSource
from storage.memory_store import MemoryStore
from storage.source_store import SourceStore


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


async def capture(
    sources,
    text="我正在准备面试",
    *,
    number=1,
    user="u",
    group="",
    actor="user",
    persona="p",
):
    identity = EventIdentity(
        "platform",
        "test",
        "GroupMessage" if group else "FriendMessage",
        f"platform:{group or user}",
        group or user,
        group,
        user,
        user,
        "bot",
        f"m-{number}",
    )
    result = await sources.capture(
        CapturedSource(
            identity=identity,
            actor_kind=actor,
            content_text=text,
            observed_at=number * 100.0,
            actor_account_id="bot" if actor == "agent" else user,
            persona_id=persona,
        )
    )
    return result


def output(nodes=(), *, profiles=(), edges=(), relationship=None, coverage_changes=()):
    has_changes = bool(nodes or profiles or edges or relationship or coverage_changes)
    return {
        "schema_version": "extraction.v1",
        "result": "changes" if has_changes else "no_change",
        "reason": "changes" if has_changes else "no_meaningful_content",
        "node_changes": list(nodes),
        "profile_changes": list(profiles),
        "edges": list(edges),
        "relationship_change": relationship,
        "coverage_changes": list(coverage_changes),
    }


def node(
    source_id,
    *,
    content="用户在准备面试。",
    ref="local_interview",
    kind="event",
    old=None,
):
    return {
        "local_ref": ref,
        "node_type": kind,
        "operation": "update" if old else "create",
        "node_id": old["node_id"] if old else None,
        "expected_version": old["content_version"] if old else None,
        "content": content,
        "importance": 0.6,
        "source_ids": [source_id],
        "event_time": None,
        "title": "合成记忆条目",
        "emotion": None,
        "base_node_ids": [],
    }


def profile(source_id, user, *, version=0, description="已戒咖啡"):
    return {
        "logical_user_id": user,
        "expected_version": version,
        "description": description,
        "tags": ["不喝咖啡"],
        "source_ids": [source_id],
    }


async def prepare(stores, *, number=1, text="我在准备面试", known=(), actor="user"):
    sources, memory, service = stores
    source = await capture(sources, text, number=number, actor=actor)
    # Agent records intentionally do not create a logical user.
    user = source.logical_user_id
    if user is None:
        user = (await sources.get_records((source.source_id,)))[0][
            "conversation_logical_user_id"
        ]
    space = await memory.get_or_create_space(private_space("p", user))
    batch = await service.prepare(
        space_id=space["space_id"],
        source_ids=(source.source_id,),
        persona="耐心且谨慎的陪伴者",
        known_node_ids=tuple(known),
    )
    return batch, source


@pytest.mark.asyncio
async def test_fake_replay_event_continuation_and_separate_similar_event(stores):
    _, memory, service = stores
    batch, source = await prepare(stores)
    calls = []

    async def fake_model(messages):
        calls.append(messages)
        assert (
            json.loads(messages[1]["content"])["sources"][0]["content_text"]
            == "我在准备面试"
        )
        return json.dumps(output([node(source.source_id)]))

    first = await service.run(batch, fake_model, model_id="fake")
    assert first["activity_round"] == 1
    old = await memory.get_node(first["result"]["node_refs"]["local_interview"])
    assert old["strength_reset_round"] == 1
    assert old["strength_reset_at"] > 0
    second_batch, second_source = await prepare(
        stores, number=2, text="上次面试通过了", known=(old["node_id"],)
    )
    revised = "用户先准备了面试，随后通过了这次面试。"
    await service.apply(
        second_batch,
        json.dumps(output([node(second_source.source_id, old=old, content=revised)])),
        model_id="fake",
    )
    new = await memory.get_node(old["node_id"])
    assert new["content_version"] == 2
    assert new["content_text"] == revised
    assert new["strength_reset_round"] == 2
    assert len(await memory.list_node_sources(old["node_id"])) == 2
    assert [
        v["content_text"] for v in await memory.list_node_versions(old["node_id"])
    ] == [old["content_text"], revised]
    third_batch, third_source = await prepare(
        stores, number=3, text="今天又参加另一场面试", known=(old["node_id"],)
    )
    await service.apply(
        third_batch,
        json.dumps(
            output(
                [node(third_source.source_id, content="用户另一次参加了新面试。")],
                edges=[
                    {
                        "from_ref": "local_interview",
                        "to_ref": old["node_id"],
                        "relation_type": "related",
                        "source_ids": [third_source.source_id],
                    }
                ],
            )
        ),
        model_id="fake",
    )
    assert len(await memory.list_nodes(space_id=batch.space["space_id"])) == 2
    assert len(await memory.list_edges()) == 1
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_no_change_idempotent_after_restart_and_no_second_model_call(stores):
    _, memory, service = stores
    batch, _ = await prepare(stores, text="早安")
    raw = json.dumps(output())
    results = await asyncio.gather(
        *(service.apply(batch, raw, model_id="fake") for _ in range(2))
    )
    assert sorted(r["replayed"] for r in results) == [False, True]
    await memory.close()
    await memory.open()

    async def must_not_call(_):
        raise AssertionError("completed batch should not invoke a model")

    resumed = await service.run(batch, must_not_call, model_id="fake")
    assert resumed["replayed"] is True
    assert (await memory.get_active_generation(batch.space["space_id"]))[
        "activity_round"
    ] == 1
    assert await memory.list_nodes(space_id=batch.space["space_id"]) == []


@pytest.mark.asyncio
async def test_invalid_late_profile_rolls_back_nodes_sources_round_and_receipt(stores):
    sources, memory, service = stores
    batch, source = await prepare(stores)
    bad = output(
        [node(source.source_id)], profiles=[profile(source.source_id, "other")]
    )
    with pytest.raises(ValueError, match="another private"):
        await service.apply(batch, json.dumps(bad), model_id="fake")
    assert await memory.list_nodes(space_id=batch.space["space_id"]) == []
    assert await memory.get_extraction_batch(batch.batch_id) is None
    assert (await memory.get_active_generation(batch.space["space_id"]))[
        "activity_round"
    ] == 0
    assert (await sources.health())["source_count"] == 1
    good = output(
        [node(source.source_id)],
        profiles=[profile(source.source_id, source.logical_user_id)],
    )
    assert (await service.apply(batch, json.dumps(good), model_id="fake"))[
        "activity_round"
    ] == 1


@pytest.mark.asyncio
async def test_preference_history_merges_but_profile_replaces(stores):
    _, memory, service = stores
    batch, source = await prepare(stores, text="我习惯早晨喝咖啡")
    first = await service.apply(
        batch,
        json.dumps(
            output(
                [
                    node(
                        source.source_id,
                        content="用户过去习惯早晨喝咖啡。",
                        kind="atomic",
                    )
                ],
                profiles=[
                    {
                        **profile(
                            source.source_id,
                            source.logical_user_id,
                            description="爱喝咖啡",
                        ),
                        "tags": ["咖啡"],
                    }
                ],
            )
        ),
        model_id="fake",
    )
    old = await memory.get_node(first["result"]["node_refs"]["local_interview"])
    batch2, source2 = await prepare(
        stores, number=2, text="为了睡眠现在不喝咖啡了", known=(old["node_id"],)
    )
    history = "用户过去习惯早晨喝咖啡，后来为了睡眠停止喝咖啡。"
    relation = {
        "logical_user_id": source.logical_user_id,
        "expected_version": 0,
        "long_term": {"op": "replace", "body": "我愿意听他讲近况。"},
        "aftereffect": {"op": "keep", "body": ""},
        "source_ids": [source2.source_id],
    }
    await service.apply(
        batch2,
        json.dumps(
            output(
                [node(source2.source_id, old=old, content=history, kind="atomic")],
                profiles=[
                    profile(source2.source_id, source.logical_user_id, version=1)
                ],
                relationship=relation,
            )
        ),
        model_id="fake",
    )
    assert (await memory.get_node(old["node_id"]))["content_text"] == history
    profiles = await memory.list_profiles(space_id=batch.space["space_id"])
    assert profiles[0]["description"] == "已戒咖啡"
    assert profiles[0]["tags"] == ["不喝咖啡"]
    assert profiles[0]["content_version"] == 2
    assert (
        await memory.get_relationship(
            space_id=batch.space["space_id"], logical_user_id=source.logical_user_id
        )
    )["state"]["long_term"]["body"] == "我愿意听他讲近况。"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "unknown_source",
        "unknown_node",
        "cross_space",
        "protected",
        "disabled",
        "stale_version",
        "retired_generation",
    ],
)
async def test_reject_invalid_or_stale_candidates(stores, fault):
    _, memory, service = stores
    first, source = await prepare(stores)
    base = await memory.create_node(
        space_id=first.space["space_id"],
        node_type="event",
        content_text="已有经历",
        source_ids=[source.source_id],
    )
    batch, source2 = await prepare(stores, number=2, known=(base["node_id"],))
    candidate = node(source2.source_id, old=base)
    if fault == "unknown_source":
        candidate["source_ids"] = ["made-up"]
    elif fault == "unknown_node":
        candidate["node_id"] = "node_unknown"
    elif fault == "cross_space":
        other = await memory.get_or_create_space(private_space("p", "other"))
        alien = await memory.create_node(
            space_id=other["space_id"],
            node_type="event",
            content_text="别人的事",
            source_ids=[source.source_id],
        )
        candidate["node_id"] = alien["node_id"]
    elif fault == "protected":
        await memory.set_manual_protection(base["node_id"], True)
    elif fault == "disabled":
        await memory.update_node_status(base["node_id"], "disabled")
    elif fault == "stale_version":
        await memory.update_node_content(base["node_id"], "人工修正经历")
    else:
        replacement = await memory.create_generation(batch.space["space_id"])
        await memory.activate_generation(
            batch.space["space_id"], replacement["generation_id"]
        )
    with pytest.raises((ValueError, PermissionError)):
        await service.apply(batch, json.dumps(output([candidate])), model_id="fake")
    assert await memory.get_extraction_batch(batch.batch_id) is None


@pytest.mark.asyncio
async def test_source_scope_and_persona_are_checked_at_prepare(stores):
    sources, memory, service = stores
    batch, _ = await prepare(stores)
    alien = await capture(sources, number=2, user="other")
    with pytest.raises(ValueError, match="private space"):
        await service.prepare(
            space_id=batch.space["space_id"], source_ids=(alien.source_id,), persona="p"
        )
    wrong_persona = await capture(sources, number=3, persona="q")
    with pytest.raises(ValueError, match="persona"):
        await service.prepare(
            space_id=batch.space["space_id"],
            source_ids=(wrong_persona.source_id,),
            persona="p",
        )
    agent_batch, agent = await prepare(
        stores, number=4, actor="agent", text="我猜你爱喝咖啡"
    )
    with pytest.raises(ValueError, match="own words"):
        await service.apply(
            agent_batch,
            json.dumps(
                output(
                    profiles=[profile(agent.source_id, batch.space["logical_user_id"])]
                )
            ),
            model_id="fake",
        )


@pytest.mark.asyncio
async def test_group_profiles_lightweight_and_no_relationship(stores):
    sources, memory, service = stores
    source = await capture(sources, group="g")
    space = await memory.get_or_create_space(group_space("p", "platform", "g"))
    batch = await service.prepare(
        space_id=space["space_id"], source_ids=(source.source_id,), persona="p"
    )
    with pytest.raises(ValueError, match="lightweight"):
        await service.apply(
            batch,
            json.dumps(
                output(
                    profiles=[
                        profile(
                            source.source_id,
                            source.logical_user_id,
                            description="长" * 241,
                        )
                    ]
                )
            ),
            model_id="fake",
        )
    await service.apply(
        batch,
        json.dumps(
            output(profiles=[profile(source.source_id, source.logical_user_id)])
        ),
        model_id="fake",
    )
    assert len(await memory.list_profiles(space_id=space["space_id"])) == 1


@pytest.mark.asyncio
async def test_overlapping_batches_and_stale_round_rejected(stores):
    _, memory, service = stores
    batch1, _ = await prepare(stores)
    batch2, _ = await prepare(stores, number=2)
    await service.apply(batch1, json.dumps(output()), model_id="fake")
    with pytest.raises(ValueError, match="stale"):
        await service.apply(batch2, json.dumps(output()), model_id="fake")
    # Even with a fresh round, consuming the same source using a different batch
    # key is not permitted.
    overlap = replace(batch1, batch_id="other-batch", expected_round=1)
    with pytest.raises(ValueError, match="already consumed"):
        await service.apply(overlap, json.dumps(output()), model_id="fake")
    assert (await memory.get_active_generation(batch1.space["space_id"]))[
        "activity_round"
    ] == 1


@pytest.mark.asyncio
async def test_model_failure_or_bad_json_leaves_source_for_retry(stores):
    sources, memory, service = stores
    batch, _ = await prepare(stores)

    async def failure(_):
        raise TimeoutError("fake provider timeout")

    with pytest.raises(TimeoutError):
        await service.run(batch, failure, model_id="fake")
    with pytest.raises(ValueError):
        await service.apply(batch, '{"schema_version": "unknown"}', model_id="fake")
    assert await memory.get_extraction_batch(batch.batch_id) is None
    assert (await sources.health())["source_count"] == 1
    assert (await service.apply(batch, json.dumps(output()), model_id="fake"))[
        "activity_round"
    ] == 1


@pytest.mark.asyncio
async def test_out_of_order_old_sources_cannot_restore_old_profile(stores):
    sources, memory, service = stores
    older = await capture(sources, text="喜欢咖啡", number=1)
    newer = await capture(sources, text="不再喝咖啡", number=2)
    space = await memory.get_or_create_space(private_space("p", older.logical_user_id))
    new_batch = await service.prepare(
        space_id=space["space_id"], source_ids=(newer.source_id,), persona="p"
    )
    await service.apply(
        new_batch,
        json.dumps(output(profiles=[profile(newer.source_id, newer.logical_user_id)])),
        model_id="fake",
    )
    old_batch = await service.prepare(
        space_id=space["space_id"], source_ids=(older.source_id,), persona="p"
    )
    with pytest.raises(ValueError, match="out-of-order"):
        await service.apply(
            old_batch,
            json.dumps(
                output(
                    profiles=[
                        profile(
                            older.source_id,
                            older.logical_user_id,
                            version=1,
                            description="喜欢咖啡",
                        )
                    ]
                )
            ),
            model_id="fake",
        )
    assert (await memory.list_profiles(space_id=space["space_id"]))[0][
        "description"
    ] == "已戒咖啡"


@pytest.mark.asyncio
async def test_memory_apply_enforces_authoritative_checker(stores):
    _, memory, service = stores
    batch, source = await prepare(stores)
    memory.source_exists = lambda _: False
    with pytest.raises(ValueError, match="unknown source"):
        await service.apply(
            batch, json.dumps(output([node(source.source_id)])), model_id="fake"
        )
    memory.source_exists = None
    with pytest.raises(RuntimeError, match="authoritative"):
        await service.apply(batch, json.dumps(output()), model_id="fake")


@pytest.mark.asyncio
async def test_completed_run_checks_original_context_and_active_generation(stores):
    _, memory, service = stores
    batch, _ = await prepare(stores)
    await service.apply(batch, json.dumps(output()), model_id="fake")

    async def never(_):
        raise AssertionError("no model call expected")

    with pytest.raises(ValueError, match="different input"):
        await service.run(replace(batch, persona="changed"), never, model_id="fake")
    new_generation = await memory.create_generation(batch.space["space_id"])
    await memory.activate_generation(
        batch.space["space_id"], new_generation["generation_id"]
    )
    with pytest.raises(ValueError, match="no longer active"):
        await service.run(batch, never, model_id="fake")


@pytest.mark.asyncio
async def test_complete_prompt_budget_is_checked_before_model_call(stores):
    _, _, service = stores
    batch, _ = await prepare(stores)

    async def never(_):
        raise AssertionError("oversize prompt must not invoke model")

    with pytest.raises(ValueError, match="prompt exceeds budget"):
        await service.run(batch, never, model_id="fake", max_prompt_bytes=10)


@pytest.mark.asyncio
async def test_delayed_user_capture_can_resolve_agent_trigger_owner(stores):
    sources, memory, service = stores
    identity = EventIdentity(
        "platform",
        "test",
        "FriendMessage",
        "platform:u",
        "u",
        "",
        "u",
        "u",
        "bot",
        "trigger",
    )
    agent = await sources.capture(
        CapturedSource(
            identity=identity,
            actor_kind="agent",
            content_text="我记住了",
            observed_at=101.0,
            persona_id="p",
            actor_account_id="bot",
            response_id="response",
            metadata={"trigger_message_id": "trigger"},
        )
    )
    assert (await sources.get_records((agent.source_id,)))[0][
        "conversation_logical_user_id"
    ] is None
    user = await sources.capture(
        CapturedSource(
            identity=identity,
            actor_kind="user",
            content_text="我已戒咖啡",
            observed_at=100.0,
            persona_id="p",
            actor_account_id="u",
        )
    )
    space = await memory.get_or_create_space(private_space("p", user.logical_user_id))
    batch = await service.prepare(
        space_id=space["space_id"],
        source_ids=(agent.source_id, user.source_id),
        persona="p",
    )
    assert batch.sources[0]["conversation_logical_user_id"] == user.logical_user_id
    await service.apply(
        batch,
        json.dumps(output(profiles=[profile(user.source_id, user.logical_user_id)])),
        model_id="fake",
    )


@pytest.mark.asyncio
async def test_supersedes_cycle_rolls_back_and_valid_replacement_is_historical(stores):
    _, memory, service = stores
    batch, source = await prepare(stores)
    changes = [
        node(source.source_id, ref="local_a"),
        node(source.source_id, ref="local_b"),
    ]
    edge = {
        "from_ref": "local_a",
        "to_ref": "local_b",
        "relation_type": "supersedes",
        "source_ids": [source.source_id],
    }
    reverse = {**edge, "from_ref": "local_b", "to_ref": "local_a"}
    with pytest.raises(ValueError, match="cycle"):
        await service.apply(
            batch, json.dumps(output(changes, edges=[edge, reverse])), model_id="fake"
        )
    assert await memory.list_nodes(space_id=batch.space["space_id"]) == []
    result = await service.apply(
        batch, json.dumps(output(changes, edges=[edge])), model_id="fake"
    )
    assert (await memory.get_node(result["result"]["node_refs"]["local_b"]))[
        "status"
    ] == "historical"


@pytest.mark.asyncio
async def test_strength_resets_and_rounds_are_scoped(stores):
    _, memory, service = stores
    batch, source = await prepare(stores)
    other = await memory.get_or_create_space(private_space("p", "another"))
    result = await service.apply(
        batch, json.dumps(output([node(source.source_id)])), model_id="fake"
    )
    node_id = result["result"]["node_refs"]["local_interview"]
    await memory.database.execute(
        "UPDATE memory_nodes SET reinforcement=0.1, strength_reset_at=10 WHERE node_id=?",
        (node_id,),
    )
    batch2, source2 = await prepare(stores, number=2, known=(node_id,))
    old = await memory.get_node(node_id)
    await service.apply(
        batch2,
        json.dumps(output([node(source2.source_id, content="面试已通过", old=old)])),
        model_id="fake",
    )
    new = await memory.get_node(node_id)
    assert new["reinforcement"] == 0
    assert new["strength_reset_at"] > 10
    assert new["strength_reset_round"] == 2
    assert (await memory.get_active_generation(other["space_id"]))[
        "activity_round"
    ] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_context", ["node", "profile"])
async def test_no_change_cannot_consume_sources_using_stale_context(
    stores, changed_context
):
    _, memory, service = stores
    batch1, source = await prepare(stores)
    existing = await memory.create_node(
        space_id=batch1.space["space_id"],
        node_type="atomic",
        content_text="用户喝咖啡",
        source_ids=[source.source_id],
    )
    batch, _ = await prepare(stores, number=2, known=(existing["node_id"],))
    if changed_context == "node":
        await memory.update_node_content(existing["node_id"], "用户已戒咖啡")
    else:
        await memory.upsert_profile(
            space_id=batch.space["space_id"],
            logical_user_id=source.logical_user_id,
            tags=["不喝咖啡"],
            description="人工纠正",
        )
    with pytest.raises(ValueError, match="stale"):
        await service.apply(batch, json.dumps(output()), model_id="fake")
    assert await memory.get_extraction_batch(batch.batch_id) is None


def coverage(source_id, event_ref="local_event", atomic_ref="local_atomic", **overrides):
    return {
        "event_ref": event_ref,
        "atomic_ref": atomic_ref,
        "expected_event_version": 1,
        "expected_atomic_version": 1,
        "coverage_kind": "full",
        "source_ids": [source_id],
        **overrides,
    }


def covered_output(nodes=(), *, coverages=(), **kwargs):
    result = output(nodes, **kwargs)
    result.update(schema_version="extraction.v1", coverage_changes=list(coverages))
    if coverages:
        result.update(result="changes", reason="changes")
    return result


def covered_nodes(source_id):
    return [
        node(source_id, ref="local_event", content="用户准备面试，并确认面试安排在周五。"),
        node(source_id, ref="local_atomic", kind="atomic", content="用户的面试安排在周五。"),
    ]


async def coverage_rows(memory):
    return [dict(row) for row in await memory.database.fetchall("SELECT * FROM node_coverage")]


@pytest.mark.asyncio
async def test_creation_coverage_is_atomic_and_replay_does_not_duplicate(stores):
    _, memory, service = stores
    batch, source = await prepare(stores)
    candidate = covered_output(
        covered_nodes(source.source_id), coverages=[coverage(source.source_id)]
    )
    first = await service.apply(batch, json.dumps(candidate), model_id="fake")
    second = await service.apply(batch, json.dumps(candidate), model_id="fake")
    assert second["replayed"] is True
    records = await coverage_rows(memory)
    assert len(records) == 1
    assert records[0]["event_node_id"] == first["result"]["node_refs"]["local_event"]
    assert records[0]["atomic_node_id"] == first["result"]["node_refs"]["local_atomic"]
    assert records[0]["event_content_version"] == records[0]["atomic_content_version"] == 1
    assert records[0]["coverage_kind"] == "full"
    assert first["schema_version"] == "extraction.v1"


@pytest.mark.asyncio
async def test_updates_rebuild_coverage_using_both_final_versions(stores):
    _, memory, service = stores
    first, source = await prepare(stores)
    committed = await service.apply(
        first,
        json.dumps(covered_output(covered_nodes(source.source_id), coverages=[coverage(source.source_id)])),
        model_id="fake",
    )
    event_id = committed["result"]["node_refs"]["local_event"]
    atomic_id = committed["result"]["node_refs"]["local_atomic"]
    event, atomic = await memory.get_node(event_id), await memory.get_node(atomic_id)
    second, source2 = await prepare(stores, number=2, known=(event_id, atomic_id))
    changes = [
        node(source2.source_id, old=event, ref="local_event", content="用户的面试原定周五，现改在周六。"),
        node(source2.source_id, old=atomic, ref="local_atomic", kind="atomic", content="用户的面试已改在周六。"),
    ]
    candidate = covered_output(changes, coverages=[coverage(
        source2.source_id, expected_event_version=2, expected_atomic_version=2,
    )])
    await service.apply(second, json.dumps(candidate), model_id="fake")
    record = (await coverage_rows(memory))[0]
    assert record["event_content_version"] == record["atomic_content_version"] == 2

    # The same event may be addressed by its existing node ID while an atomic
    # node is addressed by a local alias. Importance-only changes keep version 2.
    event, atomic = await memory.get_node(event_id), await memory.get_node(atomic_id)
    third, source3 = await prepare(stores, number=3, known=(event_id, atomic_id))
    changed_importance = {
        **node(source3.source_id, old=atomic, ref="local_atomic", kind="atomic", content=atomic["content_text"]),
        "importance": 0.7,
    }
    await service.apply(third, json.dumps(covered_output(
        [changed_importance], coverages=[coverage(
            source3.source_id, event_ref=event_id, atomic_ref="local_atomic",
            expected_event_version=2, expected_atomic_version=2, coverage_kind="partial",
        )],
    )), model_id="fake")
    record = (await coverage_rows(memory))[0]
    assert record["event_content_version"] == record["atomic_content_version"] == 2
    assert record["coverage_kind"] == "partial"


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [
    "event_version", "atomic_version", "unknown_source", "unknown_node",
    "cross_space", "protected", "disabled", "wrong_type", "duplicate_alias",
])
async def test_invalid_coverage_rejected_before_memory_changes(stores, fault):
    _, memory, service = stores
    first, source = await prepare(stores)
    event = await memory.create_node(
        space_id=first.space["space_id"], node_type="event", content_text="已有事件。",
        source_ids=[source.source_id],
    )
    if fault == "protected":
        await memory.set_manual_protection(event["node_id"], True)
    if fault == "disabled":
        await memory.update_node_status(event["node_id"], "disabled")
    batch, source2 = await prepare(stores, number=2, known=(event["node_id"],))
    atomic_change = node(source2.source_id, ref="local_atomic", kind="atomic")
    proposed = coverage(source2.source_id, event_ref=event["node_id"])
    nodes = [atomic_change]
    if fault == "event_version":
        proposed["expected_event_version"] = 2
    elif fault == "atomic_version":
        proposed["expected_atomic_version"] = 2
    elif fault == "unknown_source":
        proposed["source_ids"] = ["source_unknown"]
    elif fault == "unknown_node":
        proposed["event_ref"] = "node_unknown"
    elif fault == "cross_space":
        other = await memory.get_or_create_space(private_space("p", "other"))
        alien = await memory.create_node(
            space_id=other["space_id"], node_type="event", content_text="其他用户的事件。",
            source_ids=[source.source_id],
        )
        proposed["event_ref"] = alien["node_id"]
        # Even tampering with the trusted-context container cannot cross scope.
        batch = replace(batch, known_nodes=(*batch.known_nodes, alien))
    elif fault == "wrong_type":
        atomic_change["node_type"] = "topic"
    elif fault == "duplicate_alias":
        nodes.append(node(source2.source_id, old=event, ref="local_event", content=event["content_text"]))
    coverages = [proposed]
    if fault == "duplicate_alias":
        coverages.append({**proposed, "event_ref": "local_event"})
    with pytest.raises((ValueError, PermissionError)):
        await service.apply(
            batch, json.dumps(covered_output(nodes, coverages=coverages)), model_id="fake"
        )
    assert await coverage_rows(memory) == []
    assert await memory.get_extraction_batch(batch.batch_id) is None
    assert (await memory.get_node(event["node_id"]))["content_version"] == 1
    rows = await memory.database.fetchall("SELECT node_type FROM memory_nodes WHERE space_id=?", (batch.space["space_id"],))
    assert [row["node_type"] for row in rows] == ["event"]
    assert (await memory.get_active_generation(batch.space["space_id"]))["activity_round"] == 0


@pytest.mark.asyncio
async def test_late_failure_rolls_back_coverage_and_valid_retry_commits(stores):
    _, memory, service = stores
    batch, source = await prepare(stores)
    candidate = covered_output(
        covered_nodes(source.source_id), coverages=[coverage(source.source_id)],
        profiles=[profile(source.source_id, "wrong-user")],
    )
    with pytest.raises(ValueError, match="another private"):
        await service.apply(batch, json.dumps(candidate), model_id="fake")
    assert await coverage_rows(memory) == []
    assert await memory.list_nodes(space_id=batch.space["space_id"]) == []
    assert await memory.get_extraction_batch(batch.batch_id) is None
    candidate["profile_changes"] = []
    await service.apply(batch, json.dumps(candidate), model_id="fake")
    assert len(await coverage_rows(memory)) == 1


@pytest.mark.asyncio
async def test_contains_does_not_infer_coverage(stores):
    _, memory, service = stores
    batch, source = await prepare(stores)
    await service.apply(batch, json.dumps(output(
        covered_nodes(source.source_id), edges=[{
            "from_ref": "local_event", "to_ref": "local_atomic", "relation_type": "contains",
            "source_ids": [source.source_id],
        }],
    )), model_id="fake")
    assert await coverage_rows(memory) == []


@pytest.mark.asyncio
async def test_coverage_only_does_not_rewrite_or_restrengthen_nodes(stores):
    _, memory, service = stores
    batch, source = await prepare(stores)
    result = await service.apply(
        batch, json.dumps(output(covered_nodes(source.source_id))), model_id="fake"
    )
    event_id, atomic_id = (
        result["result"]["node_refs"]["local_event"],
        result["result"]["node_refs"]["local_atomic"],
    )
    before_event, before_atomic = await memory.get_node(event_id), await memory.get_node(atomic_id)
    later, source2 = await prepare(stores, number=2, known=(event_id, atomic_id))
    await service.apply(later, json.dumps(covered_output(coverages=[coverage(
        source2.source_id, event_ref=event_id, atomic_ref=atomic_id,
    )])), model_id="fake")
    assert await memory.get_node(event_id) == before_event
    assert await memory.get_node(atomic_id) == before_atomic
    assert len(await coverage_rows(memory)) == 1
    assert await memory.list_edges() == []


@pytest.mark.asyncio
async def test_silent_group_recipient_survives_transfer_replay_but_cannot_supply_profile(stores):
    from core.extraction_references import ExtractionReferences
    from core.short_term_prompts import messages, parse_output, PROMPT_VERSION as SHORT_VERSION
    from core.window_users import WindowUsers

    sources, memory, _ = stores
    service = ExtractionService(sources, memory)
    old = await capture(sources, "到场", user="recipient", group="g", number=1)
    sent = await capture(sources, "<@recipient> 明天请来领奖。", user="sender", group="g", number=2)
    await sources.bind_account(platform_instance_id="platform", account_id="recipient",
                               logical_user_id="transferred")
    # Delayed arrival with a past observation time must still use the old binding.
    identity = EventIdentity("platform", "test", "GroupMessage", "platform:g", "g", "g", "sender",
                             "sender", "bot", "late")
    delayed = await sources.capture(CapturedSource(identity, "user", "<@recipient> 附上颁奖时间。",
        210., actor_account_id="sender", persona_id="p", metadata={"group_addressing": [
            {"kind": "reply", "message_id": "m-1", "account_id": "recipient"}]}))
    space = await memory.get_or_create_space(group_space("p", "platform", "g"))
    batch = await service.prepare(space_id=space["space_id"], source_ids=(sent.source_id, delayed.source_id), persona="P")
    refs = ExtractionReferences(batch)
    wire = json.loads(refs.messages()[1]["content"])
    assert wire["sources"][0]["group_addressing"] == [
        {"kind": "mention", "actor_kind": "user", "logical_user_id": "u2", "marker": "[@1]"}]
    assert [t["logical_user_id"] for t in wire["sources"][1]["group_addressing"]] == ["u2", "u2"]

    short_payload = json.loads(messages(persona="P", previous_summary="", records=batch.sources, summary_chars=400)[1]["content"])
    short_user = short_payload["new_sources"][0]["group_addressing"][0]["logical_user_id"]
    assert short_user != short_payload["new_sources"][0]["logical_user_id"]
    users = WindowUsers([sent.logical_user_id, old.logical_user_id])
    assert users.encode(old.logical_user_id) == short_user
    summary = parse_output(json.dumps({"schema_version": SHORT_VERSION, "summary_text": "{{user:"+short_user+"}}明天领奖。",
        "evidence_source_ids": [sent.source_id]}), source_ids=[sent.source_id, delayed.source_id],
        summary_chars=400, allowed_user_ids=[sent.logical_user_id, old.logical_user_id])
    assert "{{user:"+old.logical_user_id+"}}" in summary["summary_text"]

    candidate = output(nodes=[node("s1", content="{{user:u2}}获邀领奖。")])
    candidate["node_changes"][0].update(title="颁奖", emotion=None, base_node_ids=[])
    candidate["profile_changes"] = [profile("s1", "u2")]
    with pytest.raises(ValueError, match="own words"):
        await service.apply(batch, refs.decode(json.dumps(candidate)), model_id="synthetic")
    candidate["profile_changes"] = []
    await service.apply(batch, refs.decode(json.dumps(candidate)), model_id="synthetic")
    people = await memory.database.fetchall("SELECT participant_id FROM node_participants")
    assert {row["participant_id"] for row in people} == {sent.logical_user_id, old.logical_user_id}


@pytest.mark.asyncio
async def test_image_evidence_survives_reopen_and_replays_in_attachment_order(stores):
    sources, memory, service = stores
    batch, source = await prepare(stores)
    first = {"source_id": source.source_id, "slot": 1, "description": "门牌写着 417。", "origin": "image"}
    second = {"source_id": source.source_id, "slot": 3, "description": "奖牌呈银色。", "origin": "host_caption"}
    evidence = [second, first]
    raw = json.dumps(output([node(source.source_id)]))
    receipt = await service.apply(batch, raw, model_id="test", image_evidence=evidence)
    assert not receipt["replayed"]
    assert not await memory.pending_image_evidence(None)
    await sources.close()
    await sources.open()
    replay = await service.apply(batch, raw, model_id="test", image_evidence=evidence)
    assert replay["replayed"]
    assert await sources.list_image_evidence((source.source_id,)) == [first, second]
    with pytest.raises(ValueError, match="different input or output"):
        await service.apply(batch, raw, model_id="test", image_evidence=[first])
    await sources.database.execute("DELETE FROM source_records WHERE source_id=?", (source.source_id,))
    assert not await sources.list_image_evidence((source.source_id,))


@pytest.mark.asyncio
@pytest.mark.parametrize("interrupt", ["before_source_write", "before_ack"])
async def test_image_evidence_transfer_resumes_after_interruption(stores, monkeypatch, interrupt):
    sources, memory, service = stores
    batch, source = await prepare(stores)
    evidence = [{"source_id": source.source_id, "slot": 2, "description": "图片上预定九点开场。", "origin": "image"}]
    raw = json.dumps(output([node(source.source_id)]))

    async def fail(*_):
        raise OSError("interrupted transfer")

    with monkeypatch.context() as patch:
        if interrupt == "before_source_write":
            patch.setattr(sources, "save_image_evidence", fail)
        else:
            patch.setattr(memory, "acknowledge_image_evidence", fail)
        with pytest.raises(OSError, match="interrupted transfer"):
            await service.apply(batch, raw, model_id="test", image_evidence=evidence)

    assert await memory.get_extraction_batch(batch.batch_id)
    assert len(await memory.pending_image_evidence(None)) == 1
    await memory.close()
    await sources.close()
    await sources.open()
    await memory.open()
    await service.flush_image_evidence()
    assert not await memory.pending_image_evidence(None)
    assert await sources.list_image_evidence((source.source_id,)) == evidence


@pytest.mark.asyncio
async def test_rejected_image_memory_cannot_publish_visual_evidence(stores):
    sources, memory, service = stores
    batch, source = await prepare(stores)
    raw = json.dumps(output([node("unknown")]))
    with pytest.raises(ValueError):
        await service.apply(batch, raw, model_id="test", image_evidence=[
            {"source_id": source.source_id, "slot": 1, "description": "红色行李箱。", "origin": "image"}
        ])
    assert not await sources.list_image_evidence((source.source_id,))
    assert not await memory.pending_image_evidence(None)
