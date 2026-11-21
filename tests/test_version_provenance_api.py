"""Version browsing keeps direct evidence distinct from inherited and cumulative links."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("astrbot.api.web")

from core.memory_spaces import private_space  # noqa: E402
from storage.provenance_store import record_derivation  # noqa: E402
from tests.test_maintenance_api import api as _api  # noqa: E402
from tests.test_maintenance_api import payload  # noqa: E402

api = _api


async def create_pair(api, kind):
    memory, space = api.plugin.memory_store, api.test_space
    first, second = "{{user:u}}喜欢晴天散步。", "{{user:u}}只在周末晴天散步。"
    if kind == "node":
        entity = await memory.create_node(space_id=space["space_id"], node_type="event",
            content_text=first, source_ids=["original-source"])
        current = await memory.update_node_content(entity["node_id"], second)
        text_field = "content_text"
    elif kind == "profile":
        entity = await memory.upsert_profile(space_id=space["space_id"], logical_user_id="u",
            description=first, tags=["{{user:u}}的爱好"])
        current = await memory.upsert_profile(space_id=space["space_id"], logical_user_id="u",
            description=second, tags=["{{user:u}}的爱好"])
        text_field = "description"
    else:
        entity = await memory.upsert_relationship(space_id=space["space_id"], logical_user_id="u",
            state_text=first)
        current = await memory.upsert_relationship(space_id=space["space_id"], logical_user_id="u",
            state_text=second)
        first, second = entity["state_text"], current["state_text"]
        text_field = "state_text"
    return entity[f"{kind}_id"], current, text_field, first, second


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["node", "profile", "relationship"])
async def test_all_version_types_render_names_and_preserve_direct_citations_and_baselines(api, kind):
    memory = api.plugin.memory_store
    entity_id, current, text_field, first, second = await create_pair(api, kind)
    base = {"kind": kind, "entity_id": entity_id, "version_no": 1}
    async with memory.database.transaction() as connection:
        await record_derivation(connection, kind, entity_id, current["content_version"],
            operation="confirm", editor_kind="model", now=1000,
            sources=[("new-direct-source", "source_record")], base_versions=[base],
            context_sources=["new-context-source"], semantic={"subject_kind": "user"})
    resolved = []

    async def names(ids):
        assert not memory.database._lock.locked()
        resolved.append(tuple(ids))
        return {"u": "清和"}

    api.plugin.source_store = SimpleNamespace(get_user_display_names=names)
    api.test_query.update(kind=kind, entity_id=entity_id, space_id=api.test_space["space_id"])
    result = payload(await api.versions())

    assert result["kind"] == kind and result["entity_id"] == entity_id
    assert result["entity"][text_field] == second
    assert [item["version_no"] for item in result["items"]] == [2, 1]
    assert [item[text_field] for item in result["items"]] == [second, first]
    assert [item["display_" + text_field] for item in result["items"]] == [
        value.replace("{{user:u}}", "清和") for value in (second, first)]
    assert resolved == [("u",)]
    addition = next(item for item in result["items"][0]["derivations"] if item["operation"] == "confirm")
    assert addition["sources"] == [{"source_id": "new-direct-source", "source_kind": "source_record"}]
    assert addition["base_versions"] == [base] and addition["editor_kind"] == "model"
    assert addition["context_source_ids"] == ["new-context-source"]
    assert addition["semantic"] == {"subject_kind": "user"}
    if kind == "node":
        assert result["sources_scope"] == "entity_cumulative"
        assert [item["source_id"] for item in result["sources"]] == ["original-source"]
    else:
        assert "sources" not in result
    if kind == "profile":
        assert result["items"][1]["display_tags"] == ["清和的爱好"]
    if kind == "relationship":
        assert result["items"][1]["state"]["long_term"]["body"] == "{{user:u}}喜欢晴天散步。"
        assert result["items"][0]["state"]["long_term"]["body"] == "{{user:u}}只在周末晴天散步。"


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["node", "profile", "relationship"])
async def test_missing_provenance_is_explicit_and_never_inferred_from_other_versions(api, kind):
    memory = api.plugin.memory_store
    entity_id, _, _, _, _ = await create_pair(api, kind)
    versions = await getattr(memory, f"list_{kind}_versions")(entity_id)
    version_key = f"{kind}_version_id"
    await memory.database.execute(f"DELETE FROM memory_derivations WHERE {version_key}=?", (versions[0][version_key],))
    api.test_query.update(kind=kind, entity_id=entity_id)

    result = payload(await api.versions())

    assert result["items"][1]["provenance_status"] == "unrecorded"
    assert result["items"][1]["derivations"] == []
    assert result["items"][0]["provenance_status"] == "recorded"
    if kind == "node":
        assert result["sources"][0]["source_id"] == "original-source"


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["node", "profile", "relationship"])
async def test_version_lookup_enforces_requested_space_and_active_generation(api, kind):
    memory = api.plugin.memory_store
    entity_id, _, _, _, _ = await create_pair(api, kind)
    other = await memory.get_or_create_space(private_space("other-persona", "u"))
    api.test_query.update(kind=kind, entity_id=entity_id, space_id=other["space_id"])
    payload(await api.versions(), 404)
    api.test_query["space_id"] = api.test_space["space_id"]
    api.test_query["version_no"] = "1"
    assert [item["version_no"] for item in payload(await api.versions())["items"]] == [1]
    api.test_query["version_no"] = "99"
    payload(await api.versions(), 404)
    api.test_query["version_no"] = "0"
    payload(await api.versions(), 400)
    api.test_query.pop("version_no")
    generation = await memory.create_generation(api.test_space["space_id"])
    await memory.activate_generation(api.test_space["space_id"], generation["generation_id"])
    payload(await api.versions(), 409)


@pytest.mark.asyncio
async def test_pruned_baseline_is_labelled_and_cannot_be_opened(api):
    memory = api.plugin.memory_store
    entity_id, _, _, _, _ = await create_pair(api, "node")
    for index in range(3, 13):
        await memory.update_node_content(entity_id, f"Synthetic version {index}.")
    api.test_query.update(node_id=entity_id)
    recent = payload(await api.versions())
    assert [item["version_no"] for item in recent["items"]] == list(range(12, 2, -1))
    oldest = recent["items"][-1]
    assert recent["earliest_retained_version"] == 3
    assert oldest["derivations"][0]["base_versions"][0]["available"] is False
    api.test_query["version_no"] = "1"
    payload(await api.versions(), 404)


@pytest.mark.asyncio
async def test_retention_prunes_on_reopen_and_live_change_with_independent_limits(api):
    memory = api.plugin.memory_store
    sid = api.test_space["space_id"]
    keys = {kind: (await create_pair(api, kind))[0] for kind in ("node", "profile", "relationship")}
    await memory.configure_version_limits(dict.fromkeys(keys, 20))

    async def write_all(number):
        text = f"第 {number} 版"
        await memory.update_node_content(keys["node"], text)
        await memory.upsert_profile(space_id=sid, logical_user_id="u", description=text, tags=[])
        await memory.upsert_relationship(space_id=sid, logical_user_id="u", state_text=text)

    async def retained(kind):
        versions = await getattr(memory, f"list_{kind}_versions")(keys[kind])
        return [v["version_no"] for v in versions]

    for i in range(3, 13):
        await write_all(i)
    assert await retained("relationship") == list(range(1, 13))
    await memory.close()
    await memory.open(version_limits={"node": 3, "profile": 4, "relationship": 5})
    for kind, count in (("node", 3), ("profile", 4), ("relationship", 5)):
        assert await retained(kind) == list(range(13-count, 13))
    await memory.configure_version_limits({"node": 1, "profile": 2, "relationship": 3})
    await write_all(13)
    assert await retained("node") == [13]
    assert await retained("profile") == [12, 13]
    assert await retained("relationship") == [11, 12, 13]
    await memory.configure_version_limits(dict.fromkeys(keys, 10))
    assert await retained("node") == [13]
    assert [source["source_id"] for source in await memory.list_node_sources(keys["node"])] == ["original-source"]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["short_term", "notes", "unknown"])
async def test_version_endpoint_rejects_unsupported_entity_types(api, kind):
    api.test_query.update(kind=kind, entity_id="not-relevant")
    payload(await api.versions(), 400)
