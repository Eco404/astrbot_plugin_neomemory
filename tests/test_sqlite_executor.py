"""Cancellation and failure boundaries must leave reusable SQLite connections."""

import asyncio
import sqlite3

import aiosqlite
import pytest

from storage.sqlite_executor import SQLiteExecutor


@pytest.mark.asyncio
async def test_cancelled_begin_rolls_back_before_releasing_connection(tmp_path, monkeypatch):
    database = SQLiteExecutor(tmp_path / "cancel-begin.db")
    await database.open()
    transaction = database.transaction()
    connection = database.connection
    execute = connection.execute
    entered = asyncio.Event()

    async def pause_after_begin(sql, *args, **kwargs):
        cursor = await execute(sql, *args, **kwargs)
        if sql == "BEGIN IMMEDIATE":
            entered.set()
            await asyncio.Future()
        return cursor

    monkeypatch.setattr(connection, "execute", pause_after_begin)

    async def cancelled_transaction():
        async with transaction:
            pytest.fail("cancelled BEGIN must not enter the transaction body")

    task = asyncio.create_task(cancelled_transaction())
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        assert connection.in_transaction is True
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert connection.in_transaction is False
        assert transaction.connection is None
        assert database._lock.locked() is False
        monkeypatch.setattr(connection, "execute", execute)
        async with database.transaction() as usable:
            await usable.execute("CREATE TABLE recovered(value INTEGER)")
        assert connection.in_transaction is False
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await database.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_kind", ["error", "cancel"])
async def test_failed_pragma_closes_unregistered_connection(
    tmp_path, monkeypatch, failure_kind
):
    database = SQLiteExecutor(tmp_path / "cancel-open.db")
    connect = aiosqlite.connect
    opened = []
    entered = asyncio.Event()

    async def connect_with_failing_pragma(*args, **kwargs):
        connection = await connect(*args, **kwargs)
        opened.append(connection)
        execute = connection.execute

        async def fail_pragma(sql, *args, **kwargs):
            result = await execute(sql, *args, **kwargs)
            if sql == "PRAGMA journal_mode = WAL":
                # Do not let this injected failure's traceback retain an
                # unfinished PRAGMA result cursor and an artificial file lock.
                await result.close()
                entered.set()
                if failure_kind == "cancel":
                    await asyncio.Future()
                raise sqlite3.OperationalError("synthetic PRAGMA failure")
            return result

        monkeypatch.setattr(connection, "execute", fail_pragma)
        return connection

    monkeypatch.setattr(aiosqlite, "connect", connect_with_failing_pragma)
    task = asyncio.create_task(database.open())
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        if failure_kind == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(sqlite3.OperationalError, match="synthetic PRAGMA failure"):
                await task
        assert len(opened) == 1
        with pytest.raises(RuntimeError, match="not open"):
            _ = database.connection
        with pytest.raises(ValueError, match="no active connection"):
            await opened[0].execute("SELECT 1")

        monkeypatch.setattr(aiosqlite, "connect", connect)
        await database.open()
        row = await database.fetchone("PRAGMA foreign_keys")
        assert row[0] == 1
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await database.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_kind", ["error", "cancel_before", "cancel_after"])
async def test_interrupted_commit_finishes_transaction_before_unlocking(
    tmp_path, monkeypatch, failure_kind
):
    database = SQLiteExecutor(tmp_path / "cancel-commit.db")
    await database.open()
    await database.execute("CREATE TABLE records(value TEXT)")
    connection = database.connection
    commit = connection.commit
    rollback = connection.rollback
    entered = asyncio.Event()
    rollback_lock_states = []

    async def interrupted_commit():
        if failure_kind == "cancel_after":
            await commit()
        entered.set()
        if failure_kind.startswith("cancel"):
            await asyncio.Future()
        raise sqlite3.OperationalError("synthetic COMMIT failure")

    async def observe_rollback():
        rollback_lock_states.append(database._lock.locked())
        await rollback()

    monkeypatch.setattr(connection, "commit", interrupted_commit)
    monkeypatch.setattr(connection, "rollback", observe_rollback)

    async def write_record():
        async with database.transaction() as transaction:
            await transaction.execute("INSERT INTO records VALUES ('first')")

    task = asyncio.create_task(write_record())
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        if failure_kind.startswith("cancel"):
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(sqlite3.OperationalError, match="synthetic COMMIT failure"):
                await task
        assert rollback_lock_states == [True]
        assert connection.in_transaction is False
        assert database._lock.locked() is False
        rows = await database.fetchall("SELECT value FROM records")
        # Cancellation after SQLite commits cannot undo that commit. Higher
        # layers use the atomically written batch receipt to identify retries.
        assert [row[0] for row in rows] == (
            ["first"] if failure_kind == "cancel_after" else []
        )
        monkeypatch.setattr(connection, "commit", commit)
        async with database.transaction() as transaction:
            await transaction.execute("INSERT INTO records VALUES ('second')")
        row = await database.fetchone("SELECT COUNT(*) FROM records WHERE value = 'second'")
        assert row[0] == 1
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await database.close()
