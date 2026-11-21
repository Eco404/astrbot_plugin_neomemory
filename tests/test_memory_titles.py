"""标题存储、兼容迁移和条目查阅的独立合成验证。"""


import pytest
import pytest_asyncio

from core.memory_nodes import validate_title
from core.memory_spaces import private_space
from storage.console_store import ConsoleStore
from storage.memory_store import MemoryStore


@pytest_asyncio.fixture
async def store(tmp_path):
    memory = MemoryStore(tmp_path / "memory.db", source_exists=lambda _: True)
    await memory.open()
    try:
        yield memory
    finally:
        await memory.close()


async def make_node(store, *, title=None, node_type="event", content_text="一起约好周末看展。"):
    space = await store.get_or_create_space(private_space("p", "u"))
    return await store.create_node(
        space_id=space["space_id"], node_type=node_type, content_text=content_text,
        source_ids=["synthetic-source"], title=title,
    )


def test_title_limit_counts_complete_user_references_once():
    short = "{{user:u1}}"
    long = "{{user:" + "界" * 200 + "}}"
    assert validate_title("  周末看展  ") == "周末看展"
    assert validate_title("题" * 59 + short) == "题" * 59 + short
    assert validate_title("题" * 59 + long) == "题" * 59 + long
    assert validate_title(long * 60) == long * 60
    for value in ("题" * 60 + short, "题" * 60 + long, short * 61):
        with pytest.raises(ValueError, match="readable characters"):
            validate_title(value)


@pytest.mark.parametrize("value", [None, 5, "", "  ", "{{user:}}", "{{user:a b}}", "{{user:unfinished"])
def test_invalid_title_is_not_accepted_as_display_text(value):
    with pytest.raises(ValueError):
        validate_title(value)


def test_title_bounds_encoded_identity_length():
    with pytest.raises(ValueError, match="reference exceeds"):
        validate_title("{{user:" + "u" * 201 + "}}")
    with pytest.raises(ValueError, match="encoded character"):
        validate_title(("{{user:" + "u" * 200 + "}}") * 2000)


@pytest.mark.asyncio
async def test_title_changes_preserve_content_history_strength_and_coverage(store):
    event = await make_node(store, title="周末看展")
    atomic = await make_node(store, title="出发约定", node_type="atomic", content_text="约好周末出发。")
    await store.add_coverage(event["node_id"], atomic["node_id"])
    await store.database.execute(
        "UPDATE memory_nodes SET strength=0.42,reinforcement=0.12,last_recalled_at=123 WHERE node_id=?",
        (event["node_id"],),
    )
    before = await store.get_node(event["node_id"])
    coverage_before = [tuple(row) for row in await store.database.fetchall("SELECT * FROM node_coverage")]
    changed = await store.update_node_content(event["node_id"], event["content_text"], title="看展计划")
    unchanged_fields = (
        "content_version", "node_version_id", "importance", "strength", "strength_reset_at",
        "strength_reset_round", "reinforcement", "last_recalled_at", "manual_protected", "updated_at",
    )
    assert {key: changed[key] for key in unchanged_fields} == {key: before[key] for key in unchanged_fields}
    assert changed["title"] == "看展计划"
    assert [tuple(row) for row in await store.database.fetchall("SELECT * FROM node_coverage")] == coverage_before
    versions = await store.list_node_versions(event["node_id"])
    assert len(versions) == 1
    assert versions[0]["title"] == "周末看展"
    operation = versions[0]["derivations"][-1]
    assert operation["operation"] == "title_update"
    assert operation["semantic"] == {}
    assert operation["title_change"] == {"before": "周末看展", "after": "看展计划"}

    next_edit = await store.update_node_content(event["node_id"], event["content_text"], title="周末活动")
    assert next_edit["updated_at"] == changed["updated_at"]
    noop = await store.update_node_content(event["node_id"], event["content_text"], title="周末活动")
    assert noop["updated_at"] == next_edit["updated_at"]


@pytest.mark.asyncio
async def test_body_versions_snapshot_effective_titles_without_backfill(store):
    node = await make_node(store)
    assert node["title"] is None
    first_edit = await store.update_node_content(node["node_id"], node["content_text"], title="周末计划")
    assert first_edit["content_version"] == 1
    await store.update_node_content(node["node_id"], "周末去看了展览。", title="参观展览")
    await store.update_node_content(node["node_id"], "看完展览后又一起散步。")
    versions = await store.list_node_versions(node["node_id"])
    assert [version["title"] for version in versions] == [None, "参观展览", "参观展览"]
    assert (await store.get_node(node["node_id"]))["title"] == "参观展览"
    assert (await store.list_nodes(space_id=node["space_id"]))[0]["title"] == "参观展览"


@pytest.mark.asyncio
async def test_title_operation_does_not_invent_legacy_content_provenance(store):
    node = await make_node(store)
    await store.database.execute("DELETE FROM memory_derivations WHERE node_version_id=?", (node["node_version_id"],))
    await store.update_node_content(node["node_id"], node["content_text"], title="周末计划")
    versions = await store.list_node_versions(node["node_id"])
    assert versions[0]["provenance_status"] == "unrecorded"
    assert versions[0]["title"] is None
    assert [item["operation"] for item in versions[0]["derivations"]] == ["title_update"]


@pytest.mark.asyncio
async def test_title_search_uses_current_names_without_rewriting_history(store):
    class Names:
        name = "清禾"

        async def get_user_display_names(self, ids):
            return {uid: self.name for uid in ids}

        async def find_user_ids_by_display_name(self, query):
            return ["u"] if query == self.name else []

    names = Names()
    console = ConsoleStore(store, names)
    node = await make_node(store, title="与{{user:u}}的看展约定")
    await make_node(store, content_text="普通条目")
    assert [item["node_id"] for item in (await console.nodes(node["space_id"], q="看展约定"))["items"]] == [node["node_id"]]
    assert [item["node_id"] for item in (await console.nodes(node["space_id"], q="清禾"))["items"]] == [node["node_id"]]
    await store.update_node_content(node["node_id"], node["content_text"], title="与{{user:u}}的周末计划")
    names.name = "小禾"
    rendered = await console.render_items(await store.list_node_versions(node["node_id"]))
    change = rendered[0]["derivations"][-1]["title_change"]
    assert change == {
        "before": "与{{user:u}}的看展约定", "after": "与{{user:u}}的周末计划",
        "display_before": "与小禾的看展约定", "display_after": "与小禾的周末计划",
    }
    assert (await console.nodes(node["space_id"], q="清禾"))["items"] == []
    assert len((await console.nodes(node["space_id"], q="小禾"))["items"]) == 1
    assert rendered[0]["title"] == "与{{user:u}}的看展约定"
