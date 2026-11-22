import asyncio

import pytest

from core.identity import EventIdentity
from core.memory_spaces import group_space, private_space
from core.source_ingest import CapturedSource
from storage.memory_schema import MEMORY_FORMAT
from storage.memory_store import MemoryStore
from storage.source_store import SourceStore


@pytest.mark.asyncio
async def test_memory_store_initializes_spaces_and_generations(tmp_path):
    store = MemoryStore(tmp_path / "memory.db")
    await store.open()

    private = await store.get_or_create_space(private_space("p", "u"))
    same = await store.get_or_create_space(private_space("p", "u"))
    group = await store.get_or_create_space(group_space("p", "adapter", "g"))

    assert store.schema_version == MEMORY_FORMAT
    assert private["space_id"] == same["space_id"]
    assert private["space_id"] != group["space_id"]
    active = await store.get_active_generation(private["space_id"])
    assert active is not None
    assert active["status"] == "active"
    health = await store.health()
    assert health["space_count"] == 2
    assert health["generation_count"] == 2
    await store.close()


@pytest.mark.asyncio
async def test_nodes_versions_sources_and_statuses(tmp_path):
    known = {"src-1", "src-2"}
    store = MemoryStore(tmp_path / "memory.db", source_exists=lambda source_id: source_id in known)
    await store.open()
    space = await store.get_or_create_space(private_space("p", "u"))

    node = await store.create_node(
        space_id=space["space_id"],
        node_type="atomic",
        content_text="用户喜欢上午喝咖啡。",
        source_ids=["src-1"],
        importance=0.8,
        metadata={"confidence": 0.9},
        participants=[{"participant_kind": "logical_user", "participant_id": "u"}],
    )
    assert node["content_version"] == 1
    assert node["content_text"] == "用户喜欢上午喝咖啡。"
    assert node["metadata"] == {"confidence": 0.9}
    assert await store.list_node_sources(node["node_id"]) == [
        {
            "node_id": node["node_id"],
            "source_id": "src-1",
            "source_kind": "source_record",
            "created_at": pytest.approx(node["created_at"], abs=2),
        }
    ]
    with pytest.raises(ValueError, match="unknown source_id"):
        await store.add_node_source(node["node_id"], "missing")

    updated = await store.update_node_content(
        node["node_id"],
        "用户现在不再喝咖啡。",
        importance=0.9,
    )
    assert updated["content_version"] == 2
    assert updated["strength"] == pytest.approx(0.9)
    assert len(await store.list_node_sources(node["node_id"])) == 1
    assert await store.add_node_source(node["node_id"], "src-2") is True
    assert await store.add_node_source(node["node_id"], "src-2") is False
    assert len(await store.list_node_sources(node["node_id"])) == 2

    metadata_only = await store.update_node_content(
        node["node_id"],
        "用户现在不再喝咖啡。",
        metadata={"reviewed": True},
    )
    assert metadata_only["content_version"] == 2
    assert metadata_only["metadata"] == {"reviewed": True}
    importance_only = await store.update_node_content(
        node["node_id"],
        "用户现在不再喝咖啡。",
        importance=0.7,
    )
    assert importance_only["content_version"] == 2
    assert importance_only["importance"] == pytest.approx(0.7)
    assert importance_only["strength"] == pytest.approx(0.7)

    protected = await store.set_manual_protection(
        node["node_id"], True, reason="人工确认"
    )
    assert protected["manual_protected"] is True
    assert protected["manual_protection_reason"] == "人工确认"
    assert protected["status"] == "active"
    with pytest.raises(PermissionError):
        await store.update_node_content(node["node_id"], "自动覆盖不应发生。", editor_kind="model")
    disabled = await store.update_node_status(node["node_id"], "disabled")
    assert disabled["status"] == "disabled"
    assert disabled["manual_protected"] is True
    restored = await store.set_manual_protection(node["node_id"], False)
    assert restored["manual_protected"] is False
    assert restored["manual_protection_reason"] is None
    deleted = await store.update_node_status(node["node_id"], "deleted")
    assert deleted["status"] == "deleted"
    assert await store.list_nodes(space_id=space["space_id"]) == []
    assert len(await store.list_nodes(space_id=space["space_id"], statuses=["deleted"])) == 1
    await store.close()


@pytest.mark.asyncio
async def test_node_versions_are_queryable_and_protection_is_independent(tmp_path):
    store = MemoryStore(tmp_path / "memory.db", source_exists=lambda _: True)
    await store.open()
    space = await store.get_or_create_space(private_space("p", "u"))
    node = await store.create_node(
        space_id=space["space_id"],
        node_type="atomic",
        content_text="第一版",
        source_ids=["source"],
    )
    await store.update_node_content(node["node_id"], "第二版", editor_kind="manual")
    versions = await store.list_node_versions(node["node_id"])
    assert [item["version_no"] for item in versions] == [1, 2]
    assert [item["content_text"] for item in versions] == ["第一版", "第二版"]
    await store.set_manual_protection(node["node_id"], True)
    assert (await store.update_node_status(node["node_id"], "historical"))["status"] == "historical"
    assert (await store.get_node(node["node_id"]))["manual_protected"] is True
    await store.close()


@pytest.mark.asyncio
async def test_generation_switch_and_scope_isolation(tmp_path):
    store = MemoryStore(tmp_path / "memory.db", source_exists=lambda _: True)
    await store.open()
    first = await store.get_or_create_space(private_space("p", "u1"))
    second = await store.get_or_create_space(private_space("p", "u2"))
    first_active = await store.get_active_generation(first["space_id"])
    generation = await store.create_generation(
        first["space_id"], parent_generation_id=first_active["generation_id"], reason="test"
    )
    await store.activate_generation(first["space_id"], generation["generation_id"])
    assert (await store.get_active_generation(first["space_id"]))["generation_id"] == generation[
        "generation_id"
    ]
    assert (await store.get_active_generation(second["space_id"]))["generation_id"] != generation[
        "generation_id"
    ]
    node = await store.create_node(
        space_id=first["space_id"],
        generation_id=generation["generation_id"],
        node_type="event",
        content_text="一次经历",
        source_ids=["source"],
    )
    with pytest.raises(ValueError, match="share space and generation"):
        other = await store.create_node(
            space_id=second["space_id"],
            node_type="atomic",
            content_text="另一空间",
            source_ids=["source-2"],
        )
        await store.add_edge(node["node_id"], other["node_id"], "related")
    await store.close()


@pytest.mark.asyncio
async def test_edges_coverage_profiles_relationship_notes_are_idempotent(tmp_path):
    store = MemoryStore(tmp_path / "memory.db", source_exists=lambda _: True)
    await store.open()
    space = await store.get_or_create_space(private_space("p", "u"))
    atomic = await store.create_node(
        space_id=space["space_id"], node_type="atomic", content_text="细节", source_ids=["a"]
    )
    event = await store.create_node(
        space_id=space["space_id"], node_type="event", content_text="事件过程", source_ids=["e"]
    )
    edge = await store.add_edge(event["node_id"], atomic["node_id"], "contains")
    assert edge["relation_type"] == "contains"
    assert len(await store.list_edges()) == 1
    assert await store.add_coverage(event["node_id"], atomic["node_id"]) is True
    assert await store.add_coverage(event["node_id"], atomic["node_id"]) is False
    with pytest.raises(ValueError, match="content version"):
        await store.add_coverage(
            event["node_id"], atomic["node_id"], atomic_content_version=99
        )

    profile = await store.upsert_profile(
        space_id=space["space_id"], logical_user_id="u", tags=["咖啡", "咖啡"], description="偏好"
    )
    same_profile = await store.upsert_profile(
        space_id=space["space_id"], logical_user_id="u", tags=["咖啡"], description="偏好"
    )
    assert profile["profile_id"] == same_profile["profile_id"]
    assert same_profile["content_version"] == 1
    with pytest.raises(ValueError, match="logical_user_id"):
        await store.upsert_profile(
            space_id=space["space_id"], logical_user_id="other", tags=[]
        )
    await store.upsert_relationship(
        space_id=space["space_id"], logical_user_id="u", state_text="熟悉"
    )
    assert (await store.get_relationship(space_id=space["space_id"], logical_user_id="u"))["state"]["long_term"]["body"] == "熟悉"
    with pytest.raises(ValueError, match="logical_user_id"):
        await store.upsert_relationship(
            space_id=space["space_id"], logical_user_id="other", state_text="错误空间"
        )
    short = await store.upsert_short_term(
        space_id=space["space_id"], conversation_id="conv", summary_text="正在讨论记忆"
    )
    assert (await store.get_short_term(space_id=space["space_id"], conversation_id="conv"))["state_id"] == short[
        "state_id"
    ]
    note = await store.create_note(space_id=space["space_id"], content_text="记得下次询问项目进展。")
    await store.update_note(note["note_id"], status="archived")
    assert await store.list_notes(space_id=space["space_id"], statuses=["archived"])
    await store.close()


@pytest.mark.asyncio
async def test_parallel_writes_keep_spaces_separate(tmp_path):
    store = MemoryStore(tmp_path / "memory.db", source_exists=lambda _: True)
    await store.open()
    spaces = [
        await store.get_or_create_space(private_space("p", f"u-{index}"))
        for index in range(2)
    ]
    nodes = await asyncio.gather(
        *(
            store.create_node(
                space_id=space["space_id"],
                node_type="atomic",
                content_text=f"内容-{index}",
                source_ids=[f"src-{index}"],
            )
            for index, space in enumerate(spaces)
        )
    )
    assert {node["space_id"] for node in nodes} == {space["space_id"] for space in spaces}
    await store.close()


@pytest.mark.asyncio
async def test_node_source_reference_can_be_checked_against_source_db(tmp_path):
    source_store = SourceStore(tmp_path / "source.db")
    await source_store.open()
    source = CapturedSource(
        identity=EventIdentity(
            platform_instance_id="adapter",
            platform_name="test",
            message_type="FriendMessage",
            unified_msg_origin="adapter:FriendMessage:u",
            session_id="u",
            group_id="",
            account_id="u",
            account_name="Alice",
            self_id="bot",
            message_id="m-1",
        ),
        actor_kind="user",
        content_text="原始事实",
        observed_at=100,
        message_id="m-1",
        actor_account_id="u",
        actor_account_name="Alice",
        persona_id="p",
    )
    captured = await source_store.capture(source)
    memory_store = MemoryStore(
        tmp_path / "memory.db", source_exists=source_store.has_source
    )
    await memory_store.open()
    space = await memory_store.get_or_create_space(private_space("p", "u"))
    node = await memory_store.create_node(
        space_id=space["space_id"],
        node_type="atomic",
        content_text="派生记忆",
        source_ids=[captured.source_id],
    )
    assert [row["source_id"] for row in await memory_store.list_node_sources(node["node_id"])] == [
        captured.source_id
    ]
    with pytest.raises(ValueError, match="unknown source_id"):
        await memory_store.add_node_source(node["node_id"], "src-missing")
    await memory_store.close()
    await source_store.close()
