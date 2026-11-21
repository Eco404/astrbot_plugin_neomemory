"""Slot prose travels through actual local references and scoped extraction writes."""
import copy
import json

import pytest

from core.extraction_errors import InvalidExtractionCandidate
from core.extraction_references import ExtractionReferences, ReferenceValidationError
from tests.test_extraction_apply import capture, prepare
from tests.test_relationship_slots_runtime import output, slot, stores


@pytest.mark.asyncio
async def test_slot_user_references_roundtrip_and_predecessor_is_encoded(stores):
    _, memory, service = stores
    batch, source = await prepare(stores)
    user = source.logical_user_id
    original = output(source, 0,
        long_term=slot("replace", "我愿意听{{user:" + user + "}}慢慢说。"),
        aftereffect=slot("replace", "{{user:" + user + "}}刚才的回应让我有些开心。"))
    refs = ExtractionReferences(batch)
    encoded = json.loads(refs.encode_output(json.dumps(original)))
    relation = encoded["relationship_change"]
    assert relation["logical_user_id"] == "u1" and relation["source_ids"] == ["s1"]
    assert "{{user:u1}}" in relation["long_term"]["body"]
    assert "{{user:u1}}" in relation["aftereffect"]["body"]
    restored = refs.decode(json.dumps(encoded))
    assert json.loads(restored) == original
    receipt = await service.apply(batch, restored, model_id="synthetic")
    assert (await service.apply(batch, restored, model_id="synthetic"))["replayed"]
    saved = await memory.get_relationship(space_id=batch.space["space_id"], logical_user_id=user)
    assert saved["content_version"] == 1
    assert saved["state"]["long_term"]["body"] == original["relationship_change"]["long_term"]["body"]
    next_batch, _ = await prepare(stores, number=2)
    before = copy.deepcopy(next_batch.relationship)
    data = json.loads(ExtractionReferences(next_batch).messages()[1]["content"])
    assert "state_text" not in data["known_relationship"]
    assert "{{user:u1}}" in data["known_relationship"]["state"]["long_term"]["body"]
    assert "{{user:u1}}" in data["known_relationship"]["state"]["aftereffect"]["body"]
    assert next_batch.relationship == before
    assert len(await memory.list_relationship_versions(saved["relationship_id"])) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["long_term", "aftereffect"])
async def test_unknown_user_in_either_slot_is_rejected_before_apply(stores, name):
    _, memory, _ = stores
    batch, source = await prepare(stores)
    refs = ExtractionReferences(batch)
    candidate = output(source, 0, **{name: slot("replace", "我想继续听听。")})
    wire = json.loads(refs.encode_output(json.dumps(candidate)))
    wire["relationship_change"][name]["body"] = "我想听听{{user:u999}}的想法。"
    with pytest.raises(ReferenceValidationError) as exc:
        refs.decode(json.dumps(wire))
    assert exc.value.path == f"$.relationship_change.{name}.body"
    assert await memory.get_relationship(space_id=batch.space["space_id"], logical_user_id=source.logical_user_id) is None


@pytest.mark.asyncio
async def test_cross_user_source_and_target_cannot_be_committed(stores):
    sources, memory, service = stores
    batch, source = await prepare(stores)
    outsider = await capture(sources, user="another-person", number=20)
    candidate = output(source, 0, long_term=slot("replace", "我想认真听完。"))
    candidate["relationship_change"]["source_ids"] = [outsider.source_id]
    with pytest.raises(InvalidExtractionCandidate) as exc:
        await service.apply(batch, json.dumps(candidate), model_id="synthetic")
    assert exc.value.code == "source_outside_window"
    candidate["relationship_change"]["source_ids"] = [source.source_id]
    candidate["relationship_change"]["logical_user_id"] = outsider.logical_user_id
    with pytest.raises(InvalidExtractionCandidate) as exc:
        await service.apply(batch, json.dumps(candidate), model_id="synthetic")
    assert exc.value.code == "relationship_user_mismatch"
    assert await memory.get_relationship(space_id=batch.space["space_id"], logical_user_id=source.logical_user_id) is None


@pytest.mark.asyncio
async def test_explicit_identical_replacement_uses_new_evidence_but_keep_does_not(stores):
    _, memory, service = stores
    batch, source = await prepare(stores)
    text = "我愿意按自己的节奏继续了解。"
    await service.apply(batch, json.dumps(output(source, 0,
        long_term=slot("replace", text), aftereffect=slot("replace", "还留着一点不快。"))), model_id="synthetic")
    first = await memory.get_relationship(space_id=batch.space["space_id"], logical_user_id=source.logical_user_id)
    batch, source = await prepare(stores, number=2)
    await service.apply(batch, json.dumps(output(source, 1, long_term=slot("replace", text))), model_id="synthetic")
    second = await memory.get_relationship(space_id=batch.space["space_id"], logical_user_id=source.logical_user_id)
    assert second["state"]["long_term"] == {"body": text, "observed_at": 200}
    assert second["state"]["aftereffect"] == first["state"]["aftereffect"]
    assert second["content_version"] == 2
