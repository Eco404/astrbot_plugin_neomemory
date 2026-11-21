"""The first-install schemas are atomic and reopening preserves stored data."""

import sqlite3

import pytest

from storage.memory_schema import MEMORY_SCHEMA
from storage.schema import initialize_schema
from storage.source_schema import SOURCE_SCHEMA
from storage.sqlite_executor import SQLiteExecutor


@pytest.mark.asyncio
@pytest.mark.parametrize("schema", [MEMORY_SCHEMA, SOURCE_SCHEMA], ids=["memory", "source"])
async def test_first_install_retries_after_interrupted_ddl_and_reopens(tmp_path, schema):
    database = SQLiteExecutor(tmp_path / "store.db")
    await database.open()
    try:
        with pytest.raises(sqlite3.OperationalError):
            await initialize_schema(database, (*schema, "CREATE TABLE"))
        assert (await database.fetchone("PRAGMA user_version"))[0] == 0
        assert await database.fetchall("SELECT name FROM sqlite_schema") == []
        assert await initialize_schema(database, schema) == 1
        assert (await database.fetchone("PRAGMA user_version"))[0] == 1
        assert not await database.fetchone("SELECT name FROM sqlite_schema WHERE name='schema_migrations'")
        assert await initialize_schema(database, schema) == 1
        assert await database.fetchall("PRAGMA foreign_key_check") == []
    finally:
        await database.close()
