"""Durable request slots for one frozen, isolated evaluation (not a cache)."""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path


class BudgetError(RuntimeError):
    """Content-free diagnostic; never contains a URL or response body."""


class RequestLedger:
    def __init__(self, path: Path, manifest_hash: str):
        self.path = Path(path)
        self.manifest_hash = manifest_hash

    @classmethod
    def create(cls, path, manifest_hash, arms, cases):
        path = Path(path)
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.fchmod(fd, 0o600)
        os.close(fd)
        ledger = cls(path, manifest_hash)
        with sqlite3.connect(path) as db:
            db.executescript("""
                CREATE TABLE binding (manifest_hash TEXT NOT NULL);
                CREATE TABLE arms (arm TEXT PRIMARY KEY, claimed INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE slots (arm TEXT NOT NULL, case_id TEXT NOT NULL,
                    sent INTEGER NOT NULL DEFAULT 0 CHECK(sent IN (0,1)),
                    PRIMARY KEY(arm,case_id));
            """)
            db.execute("INSERT INTO binding VALUES (?)", (manifest_hash,))
            db.executemany("INSERT INTO arms(arm) VALUES (?)", [(a,) for a in arms])
            db.executemany("INSERT INTO slots(arm,case_id) VALUES (?,?)", [(a, c) for a in arms for c in cases])
        return ledger

    @contextmanager
    def _transaction(self):
        if self.path.is_symlink() or not self.path.is_file():
            raise BudgetError("missing_or_unsafe_budget_ledger")
        db = sqlite3.connect(self.path.as_uri() + "?mode=rw", uri=True, timeout=5)
        try:
            db.execute("BEGIN IMMEDIATE")
            binding = db.execute("SELECT manifest_hash FROM binding").fetchall()
            if binding != [(self.manifest_hash,)]:
                raise BudgetError("budget_manifest_mismatch")
            yield db
            db.commit()
        finally:
            db.close()

    def claim(self, arm):
        with self._transaction() as db:
            cursor = db.execute("UPDATE arms SET claimed=1 WHERE arm=? AND claimed=0", (arm,))
            if cursor.rowcount != 1:
                raise BudgetError("unknown_or_already_started_arm")

    def reserve_request(self, arm, case):
        # Durable before dispatch: a crash in the narrow interval before send
        # conservatively spends the slot. It can never grant another request.
        with self._transaction() as db:
            claimed = db.execute("SELECT claimed FROM arms WHERE arm=?", (arm,)).fetchone()
            if claimed != (1,):
                raise BudgetError("arm_not_claimed")
            cursor = db.execute("UPDATE slots SET sent=1 WHERE arm=? AND case_id=? AND sent=0", (arm, case))
            if cursor.rowcount != 1:
                raise BudgetError("request_slot_unavailable")

    def counts(self):
        with self._transaction() as db:
            return dict(db.execute("SELECT arm,SUM(sent) FROM slots GROUP BY arm").fetchall())


class SlotGuard:
    """HTTPX pre-send hook; limits host fallbacks as well as network retries."""

    def __init__(self, ledger, arm):
        self.ledger, self.arm = ledger, arm
        self.case = None

    async def hook(self, _request):
        if self.case is None:
            raise BudgetError("request_outside_frozen_window")
        self.ledger.reserve_request(self.arm, self.case)
