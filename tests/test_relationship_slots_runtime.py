"""Two independently maintained subjective slots survive replay and budgeted reads."""
import copy
import json

import pytest
import pytest_asyncio

from core.context_bundle import assemble_context
from core.extraction_apply import ExtractionService
from core.relationship_state import (
    FORMAT, relationship_slots, relationship_text, transition_relationship, visible_relationship_slots,
)
from storage.context_store import ContextStore
from storage.memory_store import MemoryStore
from storage.source_store import SourceStore
from tests.test_context_bundle import config, context
from tests.test_extraction_apply import prepare


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


def slot(op="keep", body=""):
    return {"op": op, "body": body}


def output(source, version, *, long_term=None, aftereffect=None):
    return {"schema_version": "extraction.v1", "result": "changes", "reason": "changes",
            "node_changes": [], "profile_changes": [], "edges": [], "coverage_changes": [],
            "relationship_change": {"logical_user_id": source.logical_user_id, "expected_version": version,
                "source_ids": [source.source_id], "long_term": long_term or slot(), "aftereffect": aftereffect or slot()}}


@pytest.mark.asyncio
async def test_partial_updates_keep_original_clock_and_noop_does_not_add_versions(stores):
    sources, memory, service = stores
    batch, source = await prepare(stores)
    await service.apply(batch, json.dumps(output(source, 0,
        long_term=slot("replace", "我愿意慢慢了解这份坦诚。"), aftereffect=slot("replace", "刚才的回应让我轻松了一点。"))), model_id="synthetic")
    first = await memory.get_relationship(space_id=batch.space["space_id"], logical_user_id=source.logical_user_id)
    assert first["state"]["format"] == FORMAT
    assert first["state"]["long_term"]["observed_at"] == 100
    batch, source = await prepare(stores, number=2)
    await service.apply(batch, json.dumps(output(source, 1, aftereffect=slot("replace", "这回我有些不快，也想缓一缓。"))), model_id="synthetic")
    second = await memory.get_relationship(space_id=batch.space["space_id"], logical_user_id=source.logical_user_id)
    assert second["state"]["long_term"] == first["state"]["long_term"]
    assert second["state"]["aftereffect"]["observed_at"] == 200
    assert second["content_version"] == 2
    batch, source = await prepare(stores, number=3)
    await service.apply(batch, json.dumps(output(source, 2)), model_id="synthetic")
    assert await memory.get_relationship(space_id=batch.space["space_id"], logical_user_id=source.logical_user_id) == second
    assert len(await memory.list_relationship_versions(second["relationship_id"])) == 2
    batch, source = await prepare(stores, number=4)
    await service.apply(batch, json.dumps(output(source, 2, aftereffect=slot("clear"))), model_id="synthetic")
    third = await memory.get_relationship(space_id=batch.space["space_id"], logical_user_id=source.logical_user_id)
    assert third["state"]["long_term"] == first["state"]["long_term"]
    assert third["state"]["aftereffect"] == {"body": "", "observed_at": None}
    batch, source = await prepare(stores, number=5)
    await service.apply(batch, json.dumps(output(source, 3, long_term=slot("clear"))), model_id="synthetic")
    empty = await memory.get_relationship(space_id=batch.space["space_id"], logical_user_id=source.logical_user_id)
    assert empty["state_text"] == "" and empty["content_version"] == 4
    assert assemble_context(context(relationship=empty), [], config(), now=500).text == ""
    versions = await memory.list_relationship_versions(second["relationship_id"])
    assert versions[0]["state"] == first["state"]
    assert versions[1]["state"] == second["state"]


@pytest.mark.asyncio
async def test_understanding_is_preserved_when_only_aftereffect_changes(stores):
    _, memory, service = stores
    batch, source = await prepare(stores)
    old = await memory.upsert_relationship(space_id=batch.space["space_id"], logical_user_id=source.logical_user_id,
                                           state_text="我在意这份不催促的相处。")
    batch, source = await prepare(stores, number=2)
    await service.apply(batch, json.dumps(output(source, 1, aftereffect=slot("replace", "这次被催得有一点烦。"))), model_id="synthetic")
    current = await memory.get_relationship(space_id=batch.space["space_id"], logical_user_id=source.logical_user_id)
    assert current["state"]["long_term"] == old["state"]["long_term"]
    assert current["state"]["aftereffect"]["observed_at"] == 200
    ctx = ContextStore(memory)
    snapshot = await ctx.snapshot(batch.space["space_id"], logical_user_id=source.logical_user_id,
                                  stream_id="stream", host_conversation_id=None)
    assert snapshot["relationship"]["state"] == current["state"]
    assert "长期理解" in assemble_context(snapshot, [], config(), now=200).text
    assert "近期余韵" in assemble_context(snapshot, [], config(), now=200).text


def test_expiry_is_read_only_independent_and_unknown_time_does_not_expire():
    state = relationship_slots("长期的信任")
    state["aftereffect"] = {"body": "我还有点烦", "observed_at": 100}
    before = copy.deepcopy(state)
    assert set(visible_relationship_slots(state, now=199, aftereffect_seconds=100)) == {"long_term", "aftereffect"}
    assert set(visible_relationship_slots(state, now=200, aftereffect_seconds=100)) == {"long_term"}
    assert "aftereffect" in visible_relationship_slots(state, now=999999, aftereffect_seconds=0)
    assert state == before
    state["aftereffect"]["observed_at"] = None
    assert "aftereffect" in visible_relationship_slots(state, now=999999, aftereffect_seconds=100)
    state["long_term"]["body"] = ""
    visible = visible_relationship_slots(state, now=999999, aftereffect_seconds=100)
    assert set(visible) == {"aftereffect"}


def test_replay_clock_and_slot_budget_do_not_clip_body_or_displace_understanding():
    old = relationship_slots()
    operations = {"long_term": slot("replace", "熟悉且自在。" * 80), "aftereffect": slot("replace", "短暂的不快。" * 20)}
    state = transition_relationship(old, operations, 100)
    assert state == transition_relationship(old, operations, 100)
    row = {"state": state, "state_text": relationship_text(state)}
    full = assemble_context(context(relationship=row), [], config(recall_budget_tokens=20000), now=100)
    first = next(item for item in full.diagnostics["entries"] if item["section"] == "relationship")
    limited = assemble_context(context(relationship=row), [], config(recall_budget_tokens=first["standalone_bytes"]), now=100)
    assert state["long_term"]["body"] in limited.text
    assert state["aftereffect"]["body"] not in limited.text
    assert "relationship_aftereffect:budget" in limited.diagnostics["context_omitted"]
    expired = assemble_context(context(relationship=row), [], config(recall_budget_tokens=20000,
                               relationship_aftereffect_seconds=100), now=200)
    assert state["long_term"]["body"] in expired.text
    assert state["aftereffect"]["body"] not in expired.text


@pytest.mark.asyncio
async def test_existing_programmatic_text_edit_keeps_other_slot_and_does_not_create_a_source_time(stores):
    _, memory, _ = stores
    batch, source = await prepare(stores)
    state = relationship_slots("长期理解")
    state["aftereffect"] = {"body": "旧余韵", "observed_at": 100}
    row = await memory.upsert_relationship(space_id=batch.space["space_id"], logical_user_id=source.logical_user_id, state=state)
    edited = await memory.upsert_relationship(space_id=batch.space["space_id"], logical_user_id=source.logical_user_id,
                                              state_text="我重新想了想这段相处。")
    assert edited["state"]["long_term"] == {"body": "我重新想了想这段相处。", "observed_at": None}
    assert edited["state"]["aftereffect"] == row["state"]["aftereffect"]
    disabled = await memory.upsert_relationship(space_id=batch.space["space_id"], logical_user_id=source.logical_user_id,
                                                status="disabled")
    assert disabled["state"] == edited["state"]
    assert disabled["state_text"] == edited["state_text"]


@pytest.mark.asyncio
async def test_control_word_cannot_enter_slot_body(stores):
    _, memory, _ = stores
    batch, source = await prepare(stores)
    state = relationship_slots("我很喜欢一起修整园子的节奏。")
    state["aftereffect"] = {"body": "replace", "observed_at": 100}
    with pytest.raises(ValueError, match="cannot be an operation"):
        await memory.upsert_relationship(space_id=batch.space["space_id"],
                                          logical_user_id=source.logical_user_id, state=state)
    replacement = "I feel ready to replace that old habit."
    current = transition_relationship(relationship_slots(), {"long_term": slot(),
        "aftereffect": slot("replace", replacement)}, 101)
    assert current["aftereffect"] == {"body": replacement, "observed_at": 101}
