import pytest

from core.memory_spaces import private_space, group_space
from storage.memory_store import MemoryStore
from storage.memory_schema import MEMORY_FORMAT, MEMORY_SCHEMA
from storage.schema import initialize_schema
from storage.sqlite_executor import SQLiteExecutor


@pytest.mark.asyncio
async def test_upgrade_from_release_preserves_spaces_and_persists_policies(tmp_path):
    path = tmp_path / "memory.db"
    db = SQLiteExecutor(path)
    await db.open()
    await initialize_schema(db, MEMORY_SCHEMA)
    await db.execute("INSERT INTO memory_spaces(space_id,scope_key,space_type,persona_id,logical_user_id,created_at,updated_at) "
                     "VALUES('s1','persona:p:user:u','private','p','u',1,1)")
    await db.close()
    memory = MemoryStore(path)
    await memory.open()
    assert memory.schema_version == MEMORY_FORMAT == 2
    assert (await memory.database.fetchone("PRAGMA user_version"))[0] == 2
    assert (await memory.get_space_policy('s1'))['model_mode'] == 'inherit'
    await memory.set_space_policy('s1', {'model_mode': 'fixed', 'provider_id': 'test-chat'})
    await memory.close()
    await memory.open()
    assert (await memory.get_space_policy('s1'))['provider_id'] == 'test-chat'
    await memory.close()


@pytest.mark.asyncio
async def test_shared_spaces_respect_sender_visibility_and_destination_type(tmp_path):
    memory = MemoryStore(tmp_path / "memory.db")
    await memory.open()
    try:
        a,b,g,other = [(await memory.get_or_create_space(spec))['space_id'] for spec in
                       (private_space("p","a"), private_space("p","b"),
                        group_space("p","i","g"), private_space("q","a"))]
        async def visible(space):
            return {row['space_id'] for row in await memory.list_shared_spaces(space)}
        assert not await visible(a)
        await memory.set_space_policy(a, {"shared_recall_enabled": True})
        assert await visible(a) == {b,g}
        assert not await visible(g)
        await memory.set_space_policy(g, {"allow_group_read_private": True})
        await memory.set_space_policy(b, {"exclude_from_shared_recall": True})
        assert await visible(g) == {a}
        assert await visible(b) == {a,g}
        assert not await visible(other)
    finally:
        await memory.close()
