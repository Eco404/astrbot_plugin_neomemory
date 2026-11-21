"""Independent identity-length transformations across extraction and governance."""

import json

import pytest
from pydantic import ValidationError

from core.extraction_apply import ExtractionService
from core.extraction_references import ExtractionReferences
from core.extraction_schema import MAX_OUTPUT_CHARS, extraction_json_schema, parse_extraction_output
from core.memory_spaces import group_space, private_space
from core.text_lengths import encoded_text_limit, readable_text_length
from storage.governance_store import validate_operation
from tests.test_extraction_apply import capture, node, output, profile
from tests.test_extraction_apply import stores as stores
from tests.test_governance import apply, operation, setup_nodes
from tests.test_governance import store as store


def prose(limit, user="u1"):
    return "{{user:" + user + "}}" + "字" * (limit - 1)


def candidate(kind, text):
    value = output()
    value.update(schema_version="extraction.v1", result="changes", reason="changes", coverage_changes=[])
    if kind in {"atomic", "event", "topic", "title", "emotion", "time"}:
        change = node("s1", kind=kind if kind in {"atomic", "event", "topic"} else "event", content="合成说明")
        change.update(title="标题", emotion=None)
        if kind == "title":
            change["title"] = text
        elif kind == "emotion":
            change["emotion"] = {"tags": [{"tag": "trust", "intensity": .5}], "description": text}
        elif kind == "time":
            change["event_time"] = {"start": None, "end": None, "text": text}
        else:
            change["content"] = text
        value["node_changes"] = [change]
    elif kind in {"profile", "tag"}:
        change = profile("s1", "u1", description=text if kind == "profile" else "合成画像")
        if kind == "tag":
            change["tags"] = [text]
        value["profile_changes"] = [change]
    else:
        value["relationship_change"] = {"logical_user_id": "u1", "expected_version": 0,
            "long_term": {"op": "keep", "body": ""}, "aftereffect": {"op": "keep", "body": ""},
            "source_ids": ["s1"]}
        value["relationship_change"][kind] = {"op": "replace", "body": text}
    return value


FIELDS = [
    ("atomic", 800, ("node_changes", 0, "content")),
    ("event", 3000, ("node_changes", 0, "content")),
    ("topic", 2000, ("node_changes", 0, "content")),
    ("title", 60, ("node_changes", 0, "title")),
    ("emotion", 160, ("node_changes", 0, "emotion", "description")),
    ("time", 120, ("node_changes", 0, "event_time", "text")),
    ("profile", 600, ("profile_changes", 0, "description")),
    ("tag", 24, ("profile_changes", 0, "tags", 0)),
    ("long_term", 500, ("relationship_change", "long_term", "body")),
    ("aftereffect", 150, ("relationship_change", "aftereffect", "body")),
]


@pytest.mark.parametrize("kind,limit,path", FIELDS)
@pytest.mark.parametrize("user", ["u1", "界" * 200])
def test_identity_length_does_not_change_readable_boundary(kind, limit, path, user):
    text = prose(limit, user)
    raw = json.dumps(candidate(kind, text), ensure_ascii=False)
    assert parse_extraction_output(raw).model_dump() == candidate(kind, text)
    assert readable_text_length(text) == limit and len(text) > limit
    with pytest.raises(ValidationError) as caught:
        parse_extraction_output(json.dumps(candidate(kind, text + "字"), ensure_ascii=False))
    errors = caught.value.errors()
    assert errors[0]["loc"] == path
    assert errors[0]["type"] == "readable_too_long"
    assert errors[0]["ctx"]["max_length"] == limit


@pytest.mark.parametrize("text", ["{{user:}}", "{{user:a b}}", "{{user:unfinished", "{{user:" + "a" * 201 + "}}"])
def test_invalid_reference_cannot_discount_content_length(text):
    with pytest.raises(ValidationError) as caught:
        parse_extraction_output(json.dumps(candidate("event", text)))
    assert caught.value.errors()[0]["loc"] == ("node_changes", 0, "content")
    assert caught.value.errors()[0]["type"] == "invalid_user_reference"


def test_schema_exposes_readable_and_encoded_limits_separately():
    schema = extraction_json_schema()
    for model, field, limit in [("NodeChange", "content", 3000), ("NodeChange", "title", 60),
                               ("ProfileChange", "description", 600), ("LongTermRelationshipUpdate", "body", 500),
                               ("AftereffectRelationshipUpdate", "body", 150)]:
        spec = schema["$defs"][model]["properties"][field]
        assert spec["maxLength"] == encoded_text_limit(limit)
        assert spec["x-readable-maxLength"] == limit
    assert schema["$defs"]["NodeChange"]["properties"]["content"]["x-readable-limitsByNodeType"] == {
        "atomic": 800, "event": 3000, "topic": 2000}
    # Encoded output remains independently bounded, even when readable content fits.
    raw = json.dumps(candidate("event", "{{user:" + "a" * 200 + "}}"), ensure_ascii=False)
    with pytest.raises(ValueError, match="output exceeds"):
        parse_extraction_output(raw + " " * MAX_OUTPUT_CHARS)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,limit,path", FIELDS)
async def test_short_reference_decode_apply_and_history_preserve_full_text(stores, kind, limit, path):
    sources, memory, _ = stores
    source = await capture(sources, text="独立协议验证来源")
    space = await memory.get_or_create_space(private_space("p", source.logical_user_id))
    service = ExtractionService(sources, memory)
    batch = await service.prepare(space_id=space["space_id"], source_ids=(source.source_id,), persona="独立合成人格")
    short = prose(limit)
    wire = candidate(kind, short)
    if kind == "topic":
        for ref in ("local_first", "local_second"):
            wire["node_changes"].append(node("s1", ref=ref))
            wire["edges"].append({"from_ref": wire["node_changes"][0]["local_ref"], "to_ref": ref,
                                  "relation_type": "contains", "source_ids": ["s1"]})
    canonical = ExtractionReferences(batch).decode(json.dumps(wire, ensure_ascii=False))
    result = await service.apply(batch, canonical, model_id="controlled-validation")
    restored = short.replace("{{user:u1}}", "{{user:" + source.logical_user_id + "}}")
    assert result["activity_round"] == 1
    if kind in {"profile", "tag"}:
        saved = (await memory.list_profiles(space_id=space["space_id"]))[0]
        actual = saved["description"] if kind == "profile" else saved["tags"][0]
    elif kind in {"long_term", "aftereffect"}:
        actual = (await memory.get_relationship(space_id=space["space_id"], logical_user_id=source.logical_user_id))["state"][kind]["body"]
    else:
        saved = next(n for n in await memory.list_nodes(space_id=space["space_id"])
                     if kind != "topic" or n["node_type"] == "topic")
        actual = (saved["title"] if kind == "title" else saved["emotion"]["description"] if kind == "emotion"
                  else saved["event_time_text"] if kind == "time" else saved["content_text"])
        if kind in {"atomic", "event", "topic"}:
            assert (await memory.list_node_versions(saved["node_id"]))[0]["content_text"] == restored
    assert actual == restored
    assert readable_text_length(actual) == limit


@pytest.mark.asyncio
@pytest.mark.parametrize("over", [0, 1])
async def test_group_profile_uses_readable_lightweight_boundary(stores, over):
    sources, memory, _ = stores
    source = await capture(sources, text="群聊独立来源", group="group-length")
    space = await memory.get_or_create_space(group_space("p", "platform", "group-length"))
    service = ExtractionService(sources, memory)
    batch = await service.prepare(space_id=space["space_id"], source_ids=(source.source_id,), persona="独立合成人格")
    raw = ExtractionReferences(batch).decode(json.dumps(candidate("profile", prose(240 + over)), ensure_ascii=False))
    if over:
        with pytest.raises(ValueError, match="lightweight limit"):
            await service.apply(batch, raw, model_id="controlled-validation")
        assert await memory.list_profiles(space_id=space["space_id"]) == []
    else:
        await service.apply(batch, raw, model_id="controlled-validation")
        assert readable_text_length((await memory.list_profiles(space_id=space["space_id"]))[0]["description"]) == 240


@pytest.mark.asyncio
async def test_wrong_schema_rejected_before_storage(stores):
    sources, memory, _ = stores
    source = await capture(sources, text="独立协议版本来源")
    space = await memory.get_or_create_space(private_space("p", source.logical_user_id))
    service = ExtractionService(sources, memory)
    batch = await service.prepare(space_id=space["space_id"], source_ids=(source.source_id,), persona="独立合成人格")
    value = candidate("event", "独立事件正文")
    value["schema_version"] = "extraction.v99"
    value["node_changes"][0]["source_ids"] = [source.source_id]
    with pytest.raises(ValueError):
        await service.apply(batch, json.dumps(value, ensure_ascii=False), model_id="controlled-validation")
    assert await memory.list_nodes(space_id=space["space_id"]) == []
    assert (await memory.get_active_generation(space["space_id"]))["activity_round"] == 0


@pytest.mark.asyncio
async def test_manual_store_and_governance_share_readable_lengths(store):
    identity = "a" * 200
    space, (node,) = await setup_nodes(store, user=identity)
    text = prose(20000, identity)
    saved = await store.update_node_content(node["node_id"], text)
    assert saved["content_text"] == text
    # Keep the placeholder complete while changing an ordinary character.
    updated = text[:-1] + "改"
    await apply(store, space["space_id"], operation("edit", [saved], patch={"content_text": updated}))
    assert (await store.get_node(node["node_id"]))["content_text"] == updated
    with pytest.raises(ValueError, match="readable characters"):
        await store.update_node_content(node["node_id"], text + "字")
    current = await store.get_node(node["node_id"])
    with pytest.raises(ValueError, match="readable characters"):
        validate_operation(operation("edit", [current], patch={"content_text": text + "字"}))
    description = prose(2000, identity)
    relation = prose(500, identity)
    assert (await store.upsert_profile(space_id=space["space_id"], logical_user_id=identity,
                                      tags=[], description=description))["description"] == description
    assert (await store.upsert_relationship(space_id=space["space_id"], logical_user_id=identity,
                                           state_text=relation))["state"]["long_term"]["body"] == relation
    for action, field in (("merge", "content_text"), ("split", "parts")):
        if action == "merge":
            other = await store.create_node(space_id=space["space_id"], node_type="event",
                                            content_text="合成其他事件", source_ids=["source_0"])
            request = operation(action, [current, other], content_text=updated)
        else:
            request = operation(action, [current], parts=[{"ref": "part_a", "content_text": updated,
                "importance": .5, "source_ids": ["source_0"]}, {"ref": "part_b", "content_text": "独立分项",
                "importance": .5, "source_ids": ["source_0"]}])
        assert validate_operation(request)[field] == request[field]


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["split", "merge"])
async def test_split_and_merge_commit_complete_long_reference_prose(store, action):
    identity = "界" * 200
    space, nodes = await setup_nodes(store, kinds=("event", "event"), user=identity)
    text = prose(20000, identity)
    if action == "merge":
        request = operation("merge", nodes, content_text=text)
    else:
        request = operation("split", [nodes[0]], parts=[
            {"ref": "part_a", "content_text": text, "importance": .5, "source_ids": ["source_0"]},
            {"ref": "part_b", "content_text": "独立分项", "importance": .4, "source_ids": ["source_0"]}])
    await apply(store, space["space_id"], request)
    active = await store.list_nodes(space_id=space["space_id"])
    assert any(item["content_text"] == text for item in active)
