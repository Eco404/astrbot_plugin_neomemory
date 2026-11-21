"""Derived nodes cite supplied old versions without inventing direct sources."""

import json

import pytest

from core.extraction_references import ExtractionReferences, ReferenceValidationError
from core.extraction_schema import extraction_json_schema
from tests.test_extraction_apply import node, prepare
from tests.test_extraction_apply import output, stores  # noqa: F401


async def seed(stores):
    batch, source = await prepare(stores, number=1, text="我负责校对一份古籍，限定馆内查阅。")
    candidate = output([node(source.source_id, content="对方校对的古籍仅限馆内查阅。")])
    receipt = await stores[2].apply(batch, json.dumps(candidate), model_id="synthetic")
    return await stores[1].get_node(receipt["result"]["node_refs"]["local_interview"]), source


@pytest.mark.asyncio
async def test_new_atomic_keeps_explicit_old_basis_and_current_source_separate(stores):
    _, memory, service = stores
    old, first = await seed(stores)
    await memory.database.execute("UPDATE memory_nodes SET manual_protected=1 WHERE node_id=?", (old["node_id"],))
    batch, second = await prepare(stores, number=2, text="我今天增加了索引页。", known=(old["node_id"],))
    change = {**node(second.source_id, kind="atomic", content="对方在仅限馆内查阅的古籍中增加了索引页。"),
              "base_node_ids": [old["node_id"]]}
    refs = ExtractionReferences(batch)
    raw = json.dumps(output([change]))
    wire = refs.encode_output(raw)
    assert json.loads(wire)["node_changes"][0]["base_node_ids"] == ["n1"]
    receipt = await service.apply(batch, refs.decode(wire), model_id="synthetic")
    replay = await service.apply(batch, refs.decode(wire), model_id="synthetic")
    assert replay["replayed"]
    node_id = receipt["result"]["node_refs"]["local_interview"]
    versions = await memory.list_node_versions(node_id)
    assert len(versions) == 1 and len(versions[0]["derivations"]) == 1
    derivation = versions[0]["derivations"][0]
    assert derivation["base_versions"] == [{"kind": "node", "entity_id": old["node_id"], "version_no": 1}]
    assert derivation["sources"] == [{"source_id": second.source_id, "source_kind": "source_record"}]
    assert [item["source_id"] for item in await memory.list_node_sources(node_id)] == [second.source_id]
    assert (await memory.list_node_versions(old["node_id"]))[0]["derivations"][0]["sources"][0]["source_id"] == first.source_id


@pytest.mark.asyncio
async def test_same_window_update_does_not_shift_new_nodes_basis_to_future_version(stores):
    _, memory, service = stores
    old, _ = await seed(stores)
    batch, source = await prepare(stores, number=2, text="索引完成了，借阅限制不变。", known=(old["node_id"],))
    update = node(source.source_id, old=old, content="对方古籍索引已经完成，仍仅限馆内查阅。")
    derived = {**node(source.source_id, ref="local_index", kind="atomic", content="对方已完成馆内古籍的索引。"),
               "base_node_ids": [old["node_id"]]}
    receipt = await service.apply(batch, json.dumps(output([update, derived])), model_id="synthetic")
    assert (await memory.get_node(old["node_id"]))["content_version"] == 2
    version = (await memory.list_node_versions(receipt["result"]["node_refs"]["local_index"]))[0]
    assert version["derivations"][0]["base_versions"][0]["version_no"] == 1


@pytest.mark.asyncio
async def test_out_of_context_basis_rejects_without_consuming_sources(stores):
    _, memory, service = stores
    old, _ = await seed(stores)
    batch, source = await prepare(stores, number=2)
    candidate = output([{**node(source.source_id), "base_node_ids": [old["node_id"]]}])
    with pytest.raises(ValueError, match="fixed extraction context"):
        await service.apply(batch, json.dumps(candidate), model_id="synthetic")
    assert await memory.get_extraction_batch(batch.batch_id) is None
    assert len(await memory.list_nodes(space_id=batch.space["space_id"])) == 1


@pytest.mark.asyncio
async def test_basis_reference_kind_is_checked_in_short_reference_protocol(stores):
    old, _ = await seed(stores)
    batch, source = await prepare(stores, number=2, known=(old["node_id"],))
    refs = ExtractionReferences(batch)
    raw = output([{**node(source.source_id), "base_node_ids": [old["node_id"]]}])
    wire = json.loads(refs.encode_output(json.dumps(raw)))
    wire["node_changes"][0]["base_node_ids"] = ["s1"]
    with pytest.raises(ReferenceValidationError) as caught:
        refs.decode(json.dumps(wire))
    assert caught.value.path == "$.node_changes[0].base_node_ids[0]"


def test_basis_schema_is_closed_and_requires_explicit_empty_list():
    schema = extraction_json_schema("extraction.v1")
    for definition in [schema, *schema["$defs"].values()]:
        assert definition["additionalProperties"] is False
        assert set(definition["required"]) == set(definition["properties"])
    assert "base_node_ids" in schema["$defs"]["NodeChange"]["required"]


@pytest.mark.asyncio
async def test_missing_basis_field_rejects_without_commit(stores):
    batch, source = await prepare(stores)
    candidate = output([node(source.source_id)])
    del candidate["node_changes"][0]["base_node_ids"]
    with pytest.raises(ValueError):
        await stores[2].apply(batch, json.dumps(candidate), model_id="synthetic")
    assert await stores[1].get_extraction_batch(batch.batch_id) is None
