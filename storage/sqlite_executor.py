"""Small async SQLite wrapper with explicit transaction boundaries."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import aiosqlite


class SQLiteExecutor:
    """Serialize writes per database while keeping the public API async."""

    def __init__(self, path: str | Path, *, busy_timeout_ms: int = 5000) -> None:
        self.path = Path(path)
        self.busy_timeout_ms = max(100, int(busy_timeout_ms))
        self._connection: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    @property
    def connection(self) -> aiosqlite.Connection:
        if self._connection is None:
            raise RuntimeError("SQLite database is not open")
        return self._connection

    async def open(self) -> None:
        if self._connection is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = await aiosqlite.connect(self.path)
        try:
            connection.row_factory = aiosqlite.Row
            await connection.execute("PRAGMA foreign_keys = ON")
            await connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
            await connection.execute("PRAGMA journal_mode = WAL")
            await connection.execute("PRAGMA synchronous = NORMAL")
            await connection.commit()
        except BaseException:
            await connection.close()
            raise
        self._connection = connection

    async def close(self) -> None:
        connection = self._connection
        self._connection = None
        if connection is not None:
            await connection.close()

    async def execute(
        self,
        sql: str,
        parameters: Iterable[Any] = (),
        *,
        commit: bool = True,
    ) -> aiosqlite.Cursor:
        async with self._lock:
            cursor = await self.connection.execute(sql, tuple(parameters))
            if commit:
                await self.connection.commit()
            return cursor

    async def executemany(
        self,
        sql: str,
        parameters: Iterable[Iterable[Any]],
        *,
        commit: bool = True,
    ) -> aiosqlite.Cursor:
        async with self._lock:
            cursor = await self.connection.executemany(sql, parameters)
            if commit:
                await self.connection.commit()
            return cursor

    async def fetchone(
        self, sql: str, parameters: Iterable[Any] = ()
    ) -> aiosqlite.Row | None:
        async with self._lock:
            cursor = await self.connection.execute(sql, tuple(parameters))
            return await cursor.fetchone()

    async def fetchall(
        self, sql: str, parameters: Iterable[Any] = ()
    ) -> list[aiosqlite.Row]:
        async with self._lock:
            cursor = await self.connection.execute(sql, tuple(parameters))
            return await cursor.fetchall()

    def transaction(self, *, write: bool = True) -> SQLiteTransaction:
        return SQLiteTransaction(self, write=write)


class SQLiteTransaction:
    """Hold the executor lock for one explicit SQLite transaction."""

    def __init__(self, executor: SQLiteExecutor, *, write: bool = True) -> None:
        self.executor = executor
        self.write = write
        self.connection: aiosqlite.Connection | None = None

    async def __aenter__(self) -> aiosqlite.Connection:
        await self.executor._lock.acquire()
        try:
            self.connection = self.executor.connection
            # A WAL read snapshot must not reserve the writer lock. Callers
            # that mutate keep IMMEDIATE so read-modify-write stays atomic.
            await self.connection.execute("BEGIN IMMEDIATE" if self.write else "BEGIN")
            return self.connection
        except BaseException:
            try:
                if self.connection is not None:
                    await self.connection.rollback()
            finally:
                self.connection = None
                self.executor._lock.release()
            raise

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        try:
            if self.connection is None:
                return
            if exc_type is None:
                try:
                    await self.connection.commit()
                except BaseException:
                    # A cancelled await does not cancel aiosqlite's worker.
                    # Queue cleanup after the commit and finish it before
                    # another transaction can acquire this connection.
                    await self.connection.rollback()
                    raise
            else:
                await self.connection.rollback()
        finally:
            self.connection = None
            self.executor._lock.release()
