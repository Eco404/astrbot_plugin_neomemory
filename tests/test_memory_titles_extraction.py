"""独立合成样例验证标题协议、引用及正文版本边界。"""

import copy
import json
from dataclasses import replace

import pytest

from core.extraction_apply import ExtractionService
from core.extraction_errors import InvalidExtractionCandidate
from core.extraction_references import ExtractionReferences, ReferenceValidationError
from core.extraction_schema import extraction_json_schema, parse_extraction_output
from core.memory_text import render_memory_item
from core.prompts import PROMPT_VERSION, build_extraction_prompt
from tests.test_extraction_apply import (
    coverage, coverage_rows, node, output, prepare, stores,
)


def titled_output(nodes=(), *, coverages=()):
    return {**output(nodes), "schema_version": "extraction.v1", "coverage_changes": list(coverages)}


def titled_node(source, *, title="社区展出准备", **kwargs):
    return {**node(source, **kwargs), "title": title}


def current_stores(stores):
    sources, memory, _ = stores
    return sources, memory, ExtractionService(sources, memory)


@pytest.mark.parametrize("title", [None, "", "  ", "甲" * 61, "{{user:bad id}}", "{{user:unfinished"])
def test_title_schema_rejects_invalid_readable_titles(title):
    with pytest.raises(ValueError):
        parse_extraction_output(json.dumps(titled_output([titled_node("s1", title=title)])))


def test_title_is_required_and_has_separate_limit():
    missing = output([node("s1")])
    del missing["node_changes"][0]["title"]
    with pytest.raises(ValueError, match="title"):
        parse_extraction_output(json.dumps(missing))
    user_id = "member_" + "x" * 190
    title = "{{user:" + user_id + "}}" + "甲" * 59
    parsed = parse_extraction_output(json.dumps(titled_output([titled_node("s1", title=title)])))
    assert parsed.node_changes[0].title == title
    schema = extraction_json_schema("extraction.v1")
    assert schema["properties"]["schema_version"]["const"] == "extraction.v1"
    assert "title" in schema["$defs"]["NodeChange"]["required"]


@pytest.mark.asyncio
async def test_titleless_output_does_not_consume_window(stores):
    env = stores
    batch, source = await prepare(env, text="我计划参加社区展出。")
    candidate = output([node(source.source_id)])
    del candidate["node_changes"][0]["title"]
    with pytest.raises(ValueError, match="title"):
        await env[2].apply(batch, json.dumps(candidate), model_id="synthetic")
    assert await env[1].get_extraction_batch(batch.batch_id) is None
    result = await env[2].apply(batch, json.dumps(titled_output()), model_id="synthetic")
    assert result["schema_version"] == "extraction.v1"


@pytest.mark.asyncio
async def test_title_reference_roundtrip_and_display_are_identity_stable(stores):
    env = current_stores(stores)
    batch, source = await prepare(env, text="我计划参加社区展出。")
    identity = source.logical_user_id
    title = "{{user:" + identity + "}}的展出计划"
    candidate = titled_output([titled_node(source.source_id, title=title)])
    refs = ExtractionReferences(batch)
    wire = refs.encode_output(json.dumps(candidate))
    encoded = json.loads(wire)
    assert encoded["node_changes"][0]["title"] == "{{user:u1}}的展出计划"
    assert json.loads(refs.decode(wire)) == candidate
    encoded["node_changes"][0]["title"] = "{{user:unknown}}的展出计划"
    with pytest.raises(ReferenceValidationError) as error:
        refs.decode(json.dumps(encoded))
    assert error.value.path == "$.node_changes[0].title"
    item = {"title": title, "content_text": "计划参加社区展出。"}
    assert render_memory_item(item, {identity: "小舟"})["display_title"] == "小舟的展出计划"
    assert render_memory_item(item, {identity: "新昵称"})["display_title"] == "新昵称的展出计划"
    assert item["title"] == title


@pytest.mark.asyncio
async def test_titles_snapshot_with_body_versions_but_title_only_keeps_memory_semantics(stores):
    env = current_stores(stores)
    _, memory, service = env
    first, source = await prepare(env, text="我计划周五参加社区展出。")
    created = await service.apply(first, json.dumps(titled_output([
        titled_node(source.source_id, ref="local_event", title="社区展出计划", content="计划周五参加社区展出。"),
        titled_node(source.source_id, ref="local_atomic", kind="atomic", title="展出时间", content="计划周五参加社区展出。"),
    ], coverages=[coverage(source.source_id)])), model_id="synthetic")
    event_id = created["result"]["node_refs"]["local_event"]
    atomic_id = created["result"]["node_refs"]["local_atomic"]
    await memory.database.execute("UPDATE memory_nodes SET reinforcement=0.1 WHERE node_id=?", (event_id,))
    old = await memory.get_node(event_id)
    before_coverage = await coverage_rows(memory)
    second, next_source = await prepare(env, number=2, text="还是原来的社区展出安排。", known=(event_id, atomic_id))
    payload = json.loads(second.messages()[1]["content"])
    assert payload["known_nodes"][0]["title"] == "社区展出计划"
    await service.apply(second, json.dumps(titled_output([
        titled_node(next_source.source_id, old=old, title="周五社区展出计划", content=old["content_text"]),
    ])), model_id="synthetic")
    after = await memory.get_node(event_id)
    assert after["title"] == "周五社区展出计划"
    for field in ("content_version", "content_text", "importance", "strength", "strength_reset_at",
                  "strength_reset_round", "reinforcement", "updated_at"):
        assert after[field] == old[field], field
    assert await coverage_rows(memory) == before_coverage
    versions = await memory.list_node_versions(event_id)
    assert len(versions) == 1
    assert versions[0]["title"] == "社区展出计划"
    title_edits = [item for item in versions[0]["derivations"] if item["operation"] == "title_update"]
    assert title_edits[0]["title_change"] == {"before": "社区展出计划", "after": "周五社区展出计划"}
    third, newer_source = await prepare(env, number=3, text="展出已改到周六。", known=(event_id,))
    await service.apply(third, json.dumps(titled_output([
        titled_node(newer_source.source_id, old=after, title="社区展出改期", content="原计划周五参加社区展出，现改为周六。"),
    ])), model_id="synthetic")
    versions = await memory.list_node_versions(event_id)
    assert [(item["version_no"], item["title"]) for item in versions] == [(1, "社区展出计划"), (2, "社区展出改期")]
    assert await coverage_rows(memory) == []


@pytest.mark.asyncio
async def test_model_preparation_detects_a_concurrent_title_edit_even_without_body_change(stores):
    env = current_stores(stores)
    batch, source = await prepare(env, text="我计划参加社区展出。")
    result = await env[2].apply(batch, json.dumps(titled_output([titled_node(source.source_id)])), model_id="synthetic")
    node_id = result["result"]["node_refs"]["local_interview"]
    old = await env[1].get_node(node_id)
    next_batch, next_source = await prepare(env, number=2, known=(node_id,))
    await env[1].database.execute("UPDATE memory_nodes SET title=? WHERE node_id=?", ("人工改名", node_id))
    assert (await env[1].get_node(node_id))["updated_at"] == old["updated_at"]
    with pytest.raises(ValueError, match="stale node snapshot"):
        await env[2].apply(next_batch, json.dumps(titled_output([
            titled_node(next_source.source_id, old=old, title="迟到的模型标题", content=old["content_text"]),
        ])), model_id="synthetic")


@pytest.mark.asyncio
async def test_title_only_identity_in_known_nodes_is_projected_and_not_mutated(stores):
    env = current_stores(stores)
    batch, _ = await prepare(env)
    identity = "member_only_in_title"
    known = ({"node_id": "node_known", "node_type": "event", "content_version": 1,
              "content_text": "合成展出计划。", "title": "{{user:" + identity + "}}的展出计划"},)
    batch = replace(batch, known_nodes=known)
    original = copy.deepcopy(batch)
    projected = json.loads(ExtractionReferences(batch).messages()[1]["content"])
    assert projected["known_nodes"][0]["title"] == "{{user:u2}}的展出计划"
    assert batch == original
