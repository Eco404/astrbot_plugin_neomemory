"""Create a complete database schema atomically on first open."""

from .sqlite_executor import SQLiteExecutor

CURRENT_SCHEMA_VERSION = 1


async def initialize_schema(database: SQLiteExecutor, statements: tuple[str, ...]) -> int:
    async with database.transaction() as connection:
        version, = await (await connection.execute("PRAGMA user_version")).fetchone()
        if version == CURRENT_SCHEMA_VERSION:
            return version
        if version != 0:
            raise RuntimeError(f"Unsupported {database.path.name} schema version: {version}")
        for statement in statements:
            await connection.execute(statement)
        await connection.execute(f"PRAGMA user_version = {CURRENT_SCHEMA_VERSION}")
    return CURRENT_SCHEMA_VERSION
