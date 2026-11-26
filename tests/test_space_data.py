import pytest

from core.memory_spaces import private_space
from core.space_data import SpaceDataService
from storage.memory_store import MemoryStore
from storage.source_store import SourceStore
from tests.test_source_store import make_source


@pytest.mark.asyncio
async def test_project_cleanup_preserves_unselected_context_and_other_spaces(tmp_path):
    sources = SourceStore(tmp_path / "source.db")
    memory = MemoryStore(tmp_path / "memory.db")
    await sources.open()
    await memory.open()
    try:
        space = await memory.get_or_create_space(private_space("persona", "user"))
        other = await memory.get_or_create_space(private_space("persona", "other"))
        node = await memory.create_node(
            space_id=space["space_id"], node_type="event", content_text="event", source_ids=["source"],
        )
        other_node = await memory.create_node(
            space_id=other["space_id"], node_type="event", content_text="other", source_ids=["other"],
        )
        generation = await memory.get_active_generation(space["space_id"])
        async with memory.database.transaction() as connection:
            await connection.execute(
                "INSERT INTO user_profiles(profile_id,generation_id,space_id,logical_user_id,description,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?)", ("profile", generation["generation_id"], space["space_id"], "user", "profile", 0, 0),
            )
        result = await SpaceDataService(sources, memory).clear(space, ["long_term"])
        assert result["counts"]["long_term"] == 1
        assert await memory.get_node(node["node_id"]) is None
        assert await memory.get_node(other_node["node_id"]) is not None
        assert await memory.database.fetchone("SELECT 1 FROM user_profiles WHERE profile_id='profile'")
    finally:
        await memory.close()
        await sources.close()


@pytest.mark.asyncio
async def test_clear_sources_returns_count_and_can_be_repeated(tmp_path):
    sources = SourceStore(tmp_path / "source.db")
    memory = MemoryStore(tmp_path / "memory.db")
    await sources.open()
    await memory.open()
    try:
        record = await sources.capture(make_source())
        space = await memory.get_or_create_space(private_space("persona-default", record.logical_user_id))
        service = SpaceDataService(sources, memory)
        for expected in (1, 0):
            result = await service.clear(space, ["sources"])
            assert result["source_count"] == result["counts"]["sources"] == expected
        assert not await sources.has_source(record.source_id)
    finally:
        await memory.close()
        await sources.close()


@pytest.mark.asyncio
async def test_full_cleanup_destroys_only_selected_space(tmp_path):
    sources = SourceStore(tmp_path / "source.db")
    memory = MemoryStore(tmp_path / "memory.db")
    await sources.open()
    await memory.open()
    try:
        space = await memory.get_or_create_space(private_space("persona", "user"))
        other = await memory.get_or_create_space(private_space("persona", "other"))
        result = await SpaceDataService(sources, memory).clear(space, (), destroy=True)
        assert result["deleted"] is True
        assert await memory.get_space(space["space_id"]) is None
        assert await memory.get_space(other["space_id"]) is not None
    finally:
        await memory.close()
        await sources.close()
