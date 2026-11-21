"""标题治理只修改展示信息，正文及召回依据继续保持原有状态。"""

import pytest

from storage.governance_store import validate_operation
from tests.test_governance import apply, operation, setup_nodes, store  # noqa: F401


@pytest.mark.asyncio
async def test_title_only_preserves_parent_coverage_strength_and_original_snapshot(store):
    space, (parent, child) = await setup_nodes(store, ("event", "atomic"))
    await store.add_edge(parent["node_id"], child["node_id"], "contains")
    await store.add_coverage(parent["node_id"], child["node_id"])
    await store.database.execute(
        "UPDATE memory_nodes SET reinforcement=.12,strength=.2,last_recalled_at=123 WHERE node_id=?",
        (child["node_id"],),
    )
    before = await store.get_node(child["node_id"])
    provenance_before = (await store.list_node_versions(child["node_id"]))[0]["provenance_status"]
    coverage = [tuple(row) for row in await store.database.fetchall("SELECT * FROM node_coverage")]
    request = operation("edit", [before], patch={"title": "出发时间"},
                        expected_titles={child["node_id"]: None})
    result, _ = await apply(store, space["space_id"], request)
    after = await store.get_node(child["node_id"])
    assert after == {**before, "title": "出发时间"}
    assert result["title_only"]
    assert result["stale_parent_ids"] == []
    assert await store.get_node(parent["node_id"]) == parent
    assert [tuple(row) for row in await store.database.fetchall("SELECT * FROM node_coverage")] == coverage
    versions = await store.list_node_versions(child["node_id"])
    assert len(versions) == 1 and versions[0]["title"] is None
    assert versions[0]["provenance_status"] == provenance_before
    assert versions[0]["derivations"][-1]["title_change"] == {"before": None, "after": "出发时间"}
    # 同正文版本的过期表单仍不能覆盖一次已提交的标题修改。
    with pytest.raises(ValueError, match="stale node title"):
        await apply(store, space["space_id"], request)


def test_title_edit_requires_original_title_snapshot():
    node = {"node_id": "n", "generation_id": "g", "content_version": 1, "updated_at": 100}
    with pytest.raises(ValueError, match="expected_titles"):
        validate_operation(operation("edit", [node], patch={"title": "新标题"}))


@pytest.mark.asyncio
async def test_title_and_body_edit_snapshots_and_resets_as_body_edit(store):
    space, (node,) = await setup_nodes(store)
    result, _ = await apply(store, space["space_id"], operation(
        "edit", [node], patch={"content_text": "约好的行程已经完成。", "title": "完成行程"},
        expected_titles={node["node_id"]: None},
    ))
    changed = await store.get_node(node["node_id"])
    assert changed["content_version"] == 2
    assert changed["manual_protected"] and not result["title_only"]
    assert changed["title"] == "完成行程"
    versions = await store.list_node_versions(node["node_id"])
    assert [v["title"] for v in versions] == [None, "完成行程"]


@pytest.mark.asyncio
async def test_split_and_merge_titles_follow_the_new_bodies(store):
    space, (node,) = await setup_nodes(store)
    result, _ = await apply(store, space["space_id"], operation("split", [node], parts=[
        {"ref": "a", "title": "首次约定", "content_text": "约好周末看展。", "importance": .5,
         "source_ids": ["source_0"]},
        {"ref": "b", "title": "行程调整", "content_text": "出发时间改到下午。", "importance": .5,
         "source_ids": ["source_0"]},
    ]))
    parts = [await store.get_node(node_id) for node_id in result["node_refs"].values()]
    assert {part["title"] for part in parts} == {"首次约定", "行程调整"}
    merged, _ = await apply(store, space["space_id"], operation(
        "merge", parts, title="看展约定与调整", expected_titles={n["node_id"]: n["title"] for n in parts},
    ))
    current = await store.get_node(parts[0]["node_id"])
    assert current["title"] == "看展约定与调整"
    assert not merged["title_only"]
    assert (await store.list_node_versions(current["node_id"]))[-1]["title"] == current["title"]
