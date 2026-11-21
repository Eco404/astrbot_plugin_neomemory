"""Original message time survives replay and bounded provenance presentation."""

import json
from datetime import datetime, timezone

import pytest

from core.extraction_observations import (
    TEMPORAL_SEMANTICS,
    build_observation_anchors,
    source_observation_fields,
    utc_iso,
)
from storage.provenance_store import record_derivation
from tests.test_extraction_apply import capture, node, prepare, profile
from tests.test_provenance import output, relationship, stores


def test_source_fields_preserve_message_anchor_and_actor_across_midnight():
    before = datetime(2024, 4, 7, 23, 59, tzinfo=timezone.utc).timestamp()
    source = {"actor_kind": "user", "observed_at": before, "event_time_start": before - 86400}
    first = source_observation_fields(source)
    second = source_observation_fields({**source, "actor_kind": "agent", "observed_at": before + 120})
    assert first["observed_at_iso_utc"] == "2024-04-07T23:59:00+00:00"
    assert first["observed_weekday_utc"] == "Sunday"
    assert second["observed_at_iso_utc"] == "2024-04-08T00:01:00+00:00"
    assert second["observed_weekday_utc"] == "Monday"
    assert first["speaker_perspective"] == "conversation_participant"
    assert second["speaker_perspective"] == "persona_self"
    assert first["event_time_start_iso_utc"] == "2024-04-06T23:59:00+00:00"
    assert source["observed_at"] == before
    assert TEMPORAL_SEMANTICS["speaker_local_timezone"] == "unknown"
    assert "event_time_end_iso_utc" not in first


@pytest.mark.asyncio
async def test_version_anchors_use_direct_original_time_not_rebuild_time(stores):
    sources, memory, service = stores
    old = None
    source_ids = []
    for number in range(1, 6):
        batch, source = await prepare(stores, number=number, known=(old["node_id"],) if old else ())
        source_ids.append(source.source_id)
        candidate = output(
            [node(source.source_id, old=old, content=f"合成活动进展 {number}。")],
            profiles=[profile(source.source_id, source.logical_user_id, version=number - 1, description=f"合成偏好 {number}。")],
            relationship=relationship(source.source_id, source.logical_user_id, number - 1, f"我对相处的感受 {number}。"),
        )
        receipt = await service.apply(batch, json.dumps(candidate), model_id="synthetic")
        old = await memory.get_node(receipt["result"]["node_refs"]["local_interview"])
    profile_row = (await memory.list_profiles(space_id=batch.space["space_id"]))[0]
    relation = await memory.get_relationship(space_id=batch.space["space_id"], logical_user_id=source.logical_user_id)
    entities = [
        ("node", old["node_id"], 5),
        ("profile", profile_row["profile_id"], 5),
        ("relationship", relation["relationship_id"], 5),
    ]
    late = await capture(sources, number=99)
    async with memory.database.transaction() as connection:
        await record_derivation(connection, "node", old["node_id"], 5,
                                operation="title_update", editor_kind="manual", now=99999999,
                                sources=((late.source_id, "source_record"),))
    anchors = await build_observation_anchors(memory, entities)
    for kind, entity_id, _ in entities:
        items = anchors[kind, entity_id]
        assert [item["version_no"] for item in items] == [1, 4, 5]
        assert [item["observed_from_iso_utc"] for item in items] == [utc_iso(100), utc_iso(400), utc_iso(500)]
        assert all(item["observed_from_iso_utc"] == item["observed_through_iso_utc"] for item in items)
        assert all(item["direct_source_count"] == item["available_source_count"] == 1 for item in items)
    prior = await build_observation_anchors(memory, [("node", old["node_id"], 2)])
    assert [item["version_no"] for item in prior["node", old["node_id"]]] == [1, 2]
    # Persisted direct observations survive cleanup without using write dates.
    await sources.database.execute("DELETE FROM source_records WHERE source_id=?", (source_ids[0],))
    after = await build_observation_anchors(memory, entities)
    for kind, entity_id, _ in entities:
        first = after[kind, entity_id][0]
        assert first["direct_source_count"] == 1
        assert first["available_source_count"] == 1
        assert first["observed_from_iso_utc"] == utc_iso(100)
        assert first["observed_through_iso_utc"] == utc_iso(100)


@pytest.mark.asyncio
async def test_confirmations_share_version_without_pretending_one_fact_date(stores):
    sources, memory, service = stores
    batch, source = await prepare(stores)
    receipt = await service.apply(batch, json.dumps(output([node(source.source_id)])), model_id="synthetic")
    node_id = receipt["result"]["node_refs"]["local_interview"]
    later = await capture(sources, number=2)
    async with memory.database.transaction() as connection:
        await record_derivation(connection, "node", node_id, 1,
                                operation="confirm", editor_kind="model", now=99999999,
                                sources=((later.source_id, "source_record"), ("unrelated", "external")),
                                source_observations={later.source_id: 200.0})
    anchors = await build_observation_anchors(memory, [("node", node_id, 1)])
    assert anchors["node", node_id] == [{
        "version_no": 1, "direct_source_count": 2, "available_source_count": 2,
        "observed_from_iso_utc": utc_iso(100), "observed_through_iso_utc": utc_iso(200),
    }]
    assert await sources.get_observation_times(()) == {}
    assert await sources.get_observation_times((source.source_id,) * 250 + (later.source_id, "missing")) == {
        source.source_id: 100.0, later.source_id: 200.0,
    }


@pytest.mark.asyncio
async def test_split_freezes_times_for_sources_attached_after_creation(stores):
    from tests.test_governance import apply, operation

    sources, memory, service = stores
    batch, first = await prepare(stores)
    receipt = await service.apply(batch, json.dumps(output([node(first.source_id)])), model_id="synthetic")
    original = await memory.get_node(receipt["result"]["node_refs"]["local_interview"])
    later = await capture(sources, number=2)
    await memory.add_node_source(original["node_id"], later.source_id)
    memory.source_observations = sources.get_observation_times
    split = operation("split", [original], parts=[
        {"ref": ref, "content_text": f"独立分项 {ref}", "importance": .5, "source_ids": [sid]}
        for ref, sid in [("part_a", first.source_id), ("part_b", later.source_id)]
    ])
    result, _ = await apply(memory, batch.space["space_id"], split)
    later_part = result["node_refs"]["part_b"]
    await sources.database.execute("DELETE FROM source_records")
    anchors = await build_observation_anchors(memory, [("node", later_part, 1)])
    assert anchors["node", later_part] == [{
        "version_no": 1, "direct_source_count": 1, "available_source_count": 1,
        "observed_from_iso_utc": utc_iso(200), "observed_through_iso_utc": utc_iso(200),
    }]


@pytest.mark.asyncio
async def test_relationship_replacement_null_and_source_cleanup_preserve_copied_anchors(stores, monkeypatch):
    sources, memory, service = stores
    for number in (1, 2):
        batch, source = await prepare(stores, number=number)
        await service.apply(batch, json.dumps(output(relationship=relationship(
            source.source_id, source.logical_user_id, number - 1, "我愿意慢慢交流。"))), model_id="synthetic")
    row = await memory.get_relationship(space_id=batch.space["space_id"], logical_user_id=source.logical_user_id)
    assert row["content_version"] == 2
    entities = [("relationship", row["relationship_id"], 2)]
    expected = [{"version_no": version, "direct_source_count": 1, "available_source_count": 1,
                 "observed_from_iso_utc": utc_iso(stamp), "observed_through_iso_utc": utc_iso(stamp)}
                for version, stamp in ((1, 100), (2, 200))]
    assert (await build_observation_anchors(memory, entities))["relationship", row["relationship_id"]] == expected
    batch, _ = await prepare(stores, number=3)
    await service.apply(batch, json.dumps(output()), model_id="synthetic")
    await sources.database.execute("DELETE FROM source_records")

    async def no_raw_read(_ids):
        raise AssertionError("Copied timestamps must not require the source store")

    monkeypatch.setattr(sources, "get_observation_times", no_raw_read)
    assert (await build_observation_anchors(memory, entities))["relationship", row["relationship_id"]] == expected
    current = await memory.get_relationship(space_id=batch.space["space_id"], logical_user_id=row["logical_user_id"])
    assert current["content_version"] == row["content_version"]
    assert current["updated_at"] == row["updated_at"]


@pytest.mark.asyncio
async def test_relationship_without_direct_sources_does_not_inherit_previous_version_time(stores):
    sources, memory, service = stores
    batch, source = await prepare(stores)
    await service.apply(batch, json.dumps(output(relationship=relationship(
        source.source_id, source.logical_user_id, 0, "我愿意听听。"))), model_id="synthetic")
    changed = await memory.upsert_relationship(
        space_id=batch.space["space_id"], logical_user_id=source.logical_user_id, state_text="我想按自己的节奏交流。")
    anchors = await build_observation_anchors(memory, [("relationship", changed["relationship_id"], 2)])
    assert anchors["relationship", changed["relationship_id"]] == [
        {"version_no": 1, "direct_source_count": 1, "available_source_count": 1,
         "observed_from_iso_utc": utc_iso(100), "observed_through_iso_utc": utc_iso(100)},
        {"version_no": 2, "direct_source_count": 0, "available_source_count": 0,
         "observed_from_iso_utc": None, "observed_through_iso_utc": None},
    ]
