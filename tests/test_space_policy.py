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
    assert memory.schema_version == MEMORY_FORMAT == 3
    assert (await memory.database.fetchone("PRAGMA user_version"))[0] == 3
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


@pytest.mark.asyncio
async def test_release_jobs_keep_origin_and_due_time_on_upgrade(tmp_path):
    from storage.space_policy import POLICY_SCHEMA
    from storage.job_store import JobStore

    path = tmp_path / "memory.db"
    db = SQLiteExecutor(path)
    await db.open()
    await initialize_schema(db, MEMORY_SCHEMA + POLICY_SCHEMA)
    await db.execute("PRAGMA user_version=2")
    for key in ("short_term:automatic", "short_term_fresh:manual"):
        await db.execute(
            "INSERT INTO memory_jobs(job_id,kind,scope_key,idempotency_key,state,payload_json,"
            "available_at,created_at,updated_at) VALUES (?,'short_term','s',?,'failed','{}',10,5,10)",
            (key, key))
    await db.close()
    memory = MemoryStore(path)
    await memory.open()
    try:
        store = JobStore(memory.database)
        automatic = await store.get("short_term:automatic")
        assert automatic["automatic_origin"] and automatic["available_at"] == 21610
        manual = await store.get("short_term_fresh:manual")
        assert not manual["automatic_origin"]
        await memory.close()
        await memory.open()
        assert (await store.get("short_term:automatic")) == automatic
        assert (await memory.health())["schema_revision"] == "v3"
    finally:
        await memory.close()


@pytest.mark.asyncio
async def test_upgrade_release_database_with_existing_checkpoints(tmp_path):
    from storage.space_policy import POLICY_SCHEMA
    path = tmp_path / "memory.db"
    old = SQLiteExecutor(path)
    await old.open()
    await initialize_schema(old, MEMORY_SCHEMA + POLICY_SCHEMA)
    await old.execute("PRAGMA user_version=2")
    await old.execute("""INSERT INTO memory_jobs(job_id,kind,scope_key,idempotency_key,state,payload_json,
        checkpoint_json,available_at,created_at,updated_at)
        VALUES ('old','extract','s','k','failed','{}','{"done":2}',0,0,0)""")
    await old.close()
    memory = MemoryStore(path)
    await memory.open()
    try:
        row = await memory.database.fetchone("SELECT checkpoint_json,attempts_json FROM memory_jobs WHERE job_id='old'")
        assert tuple(row) == ('{"done":2}', '[]')
    finally:
        await memory.close()
