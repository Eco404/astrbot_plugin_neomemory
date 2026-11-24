"""Candidate diagnostics preserve transactional and stale-context semantics."""

import json

import pytest

from core.extraction_errors import InvalidExtractionCandidate
from tests.test_extraction_apply import (
    capture, coverage, covered_nodes, covered_output, node, output, prepare, profile, stores,
)
from tests.test_maintenance_engine import environment, enqueue_and_claim, execute
from tests.test_maintenance_engine import node as production_node, output as production_output


async def snapshot(memory):
    tables = await memory.database.fetchall("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    return {row["name"]: [tuple(item) for item in await memory.database.fetchall(
        f'SELECT * FROM "{row["name"]}" ORDER BY rowid')] for row in tables}


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value,code", [
    ("node_type", "atomic", "node_type_mismatch"),
])
async def test_node_preconditions_have_distinct_field_diagnostics_and_no_writes(stores, field, value, code):
    _, memory, service = stores
    initial, source = await prepare(stores, text="Synthetic calendar appointment.")
    stored = await memory.create_node(space_id=initial.space["space_id"], node_type="event",
        content_text="Synthetic event.", source_ids=[source.source_id])
    batch, later = await prepare(stores, number=2, text="Synthetic calendar detail.", known=[stored["node_id"]])
    candidate = node(later.source_id, old=stored, content="private-sentinel-do-not-log")
    candidate[field] = value
    before = await snapshot(memory)
    with pytest.raises(InvalidExtractionCandidate, match="node type or expected_version does not match") as caught:
        await service.apply(batch, json.dumps(output([candidate])), model_id="fake")
    assert (caught.value.code, caught.value.path) == (code, "$.node_changes[0]." + field)
    assert await snapshot(memory) == before
    diagnostic = str(caught.value) + str(vars(caught.value))
    for sensitive in (candidate["content"], stored["node_id"], later.source_id):
        assert sensitive not in diagnostic


@pytest.mark.asyncio
@pytest.mark.parametrize("existing_outside_window", [False, True])
async def test_unknown_and_outside_window_sources_are_not_silently_repaired(stores, existing_outside_window):
    sources, memory, service = stores
    batch, _ = await prepare(stores, text="Synthetic in-window message.")
    outside = (await capture(sources, "Synthetic excluded message.", number=2)).source_id if existing_outside_window else "private-unknown-source"
    before = await snapshot(memory)
    with pytest.raises(InvalidExtractionCandidate, match="outside the fixed") as caught:
        await service.apply(batch, json.dumps(output([node(outside)])), model_id="fake")
    assert (caught.value.code, caught.value.path) == ("source_outside_window", "$.node_changes[0].source_ids[0]")
    assert outside not in str(caught.value) + str(vars(caught.value))
    assert await snapshot(memory) == before


@pytest.mark.asyncio
async def test_late_attribution_error_rolls_back_earlier_node_writes(stores):
    _, memory, service = stores
    initial, _ = await prepare(stores, text="Synthetic user statement.")
    batch, agent = await prepare(stores, number=2, actor="agent", text="Synthetic agent conjecture.")
    candidate = output([node(agent.source_id, content="Synthetic candidate to roll back.")],
        profiles=[profile(agent.source_id, initial.space["logical_user_id"], description="private-attribution-sentinel")])
    before = await snapshot(memory)
    with pytest.raises(InvalidExtractionCandidate, match="own words") as caught:
        await service.apply(batch, json.dumps(candidate), model_id="fake")
    assert (caught.value.code, caught.value.path) == ("evidence_user_mismatch", "$.profile_changes[0].source_ids")
    assert "private-attribution-sentinel" not in str(caught.value) + str(vars(caught.value))
    assert await snapshot(memory) == before


@pytest.mark.asyncio
async def test_actual_context_change_remains_a_context_error(stores):
    _, memory, service = stores
    batch, source = await prepare(stores, text="Synthetic initial record.")
    stored = await memory.create_node(space_id=batch.space["space_id"], node_type="event",
        content_text="Synthetic original.", source_ids=[source.source_id])
    prepared, later = await prepare(stores, number=2, text="Synthetic continuation.", known=[stored["node_id"]])
    await memory.update_node_content(stored["node_id"], "Synthetic concurrent edit.")
    before = await snapshot(memory)
    with pytest.raises(ValueError, match="stale node snapshot") as caught:
        await service.apply(prepared, json.dumps(output([node(later.source_id, old=stored)])), model_id="fake")
    assert not isinstance(caught.value, InvalidExtractionCandidate)
    assert await snapshot(memory) == before


@pytest.mark.asyncio
async def test_maintenance_persists_precise_diagnostics_without_model_text(environment):
    env = environment
    stored = await env.memory.create_node(space_id=env.space["space_id"], node_type="event",
        content_text="Synthetic original.", source_ids=[env.records[0].source_id])
    candidate = production_node(env.records[1].source_id, old=stored, content="private-sentinel-diagnostic")
    candidate["node_type"] = "atomic"
    env.adapter.output = json.dumps(production_output([candidate]))
    failed = await execute(env, await enqueue_and_claim(env))
    assert failed["state"] == "failed"
    assert failed["error_code"] == "node_type_mismatch"
    diagnostics = failed["checkpoint"]["window"]["validation_errors"]
    assert diagnostics == [{"path": "$.node_changes[0].node_type", "type": "node_type_mismatch"}]
    assert "private-sentinel" not in json.dumps(diagnostics)
    assert "canonical_raw" in failed["checkpoint"]["window"]
    detail = await env.engine.describe(failed["job_id"])
    assert detail["validation_errors"] == diagnostics
    assert detail["regenerate_window_available"]
    assert (await env.memory.get_node(stored["node_id"]))["content_version"] == stored["content_version"]


@pytest.mark.asyncio
async def test_maintenance_does_not_relabel_changed_context_as_bad_candidate(environment):
    env = environment
    stored = await env.memory.create_node(space_id=env.space["space_id"], node_type="event",
        content_text="Synthetic original.", source_ids=[env.records[0].source_id])
    env.adapter.output = json.dumps(production_output([production_node(env.records[1].source_id, old=stored)]))

    async def concurrent_change(messages):
        await env.memory.update_node_content(stored["node_id"], "Synthetic concurrent edit.")

    env.adapter.on_generate = concurrent_change
    failed = await execute(env, await enqueue_and_claim(env))
    assert failed["state"] == "retry_wait"
    assert failed["error_code"] == "window_context_changed"
    assert not failed["checkpoint"]["window"].get("validation_errors")
