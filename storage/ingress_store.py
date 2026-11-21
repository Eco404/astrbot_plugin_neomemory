"""A bounded durable write-ahead inbox, separate from disposable caches.

The byte cap applies to serialized pending payloads, not SQLite page/WAL size.
Rows remain until source.db acknowledges the same stable source identity. A
failed stream head blocks later rows in that stream; unrelated streams continue.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any

try:
    from ..core.text_cleaning import RECEIPT_PROJECTION_FIELDS, receipt_text_matches
    from ..core.identity import EventIdentity
    from ..core.source_ingest import CapturedSource
except ImportError:
    from core.text_cleaning import RECEIPT_PROJECTION_FIELDS, receipt_text_matches
    from core.identity import EventIdentity
    from core.source_ingest import CapturedSource

from .sqlite_executor import SQLiteExecutor


class IngressError(RuntimeError):
    """Safe diagnostic code; never contains captured content or SQL parameters."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class IngressClaim:
    source_id: str
    token: str
    source: CapturedSource
    attempts: int
    max_attempts: int


def _serialize(source: CapturedSource) -> tuple[str, str]:
    try:
        value = asdict(source)
        payload = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        # Re-delivery changes the local observation time, not message identity.
        if value["metadata"].get("delivered_proactive"):
            value["metadata"].pop("replay_only", None)
        else:
            value.pop("observed_at")
        # A resolved group name can change between deliveries of the same message.
        value["identity"].pop("group_name", None)
        digest = hashlib.sha256(
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode()
        ).hexdigest()
        return payload, digest
    except (TypeError, ValueError, OverflowError) as exc:
        raise IngressError("ingress_invalid_payload") from exc


def _deserialize(payload: str) -> CapturedSource:
    value = json.loads(payload)
    value["identity"] = EventIdentity(**value["identity"])
    value["participant_ids"] = tuple(value["participant_ids"])
    return CapturedSource(**value)


def _same_queued_receipt(row, source):
    """Keep the first accepted projection when an identical receipt is delivered again."""
    if "receipt_input_hash" not in source.metadata:
        return False
    queued = _deserialize(row["payload_json"])
    if not receipt_text_matches(queued.content_text, queued.metadata, source.content_text, source.metadata):
        return False
    metadata = {k: v for k, v in source.metadata.items() if k not in RECEIPT_PROJECTION_FIELDS}
    metadata.update({k: v for k, v in queued.metadata.items() if k in RECEIPT_PROJECTION_FIELDS})
    original = replace(source, content_text=queued.content_text, metadata=metadata)
    return _serialize(original)[1] == row["payload_hash"]


def classify_capture_error(exc: Exception) -> tuple[str, bool]:
    """Bound retries to local transient failures; arbitrary exception text is private."""
    if isinstance(exc, sqlite3.IntegrityError):
        return "source_integrity_error", False
    if isinstance(exc, sqlite3.OperationalError):
        return "source_operational_error", True
    if isinstance(exc, sqlite3.DatabaseError):
        return "source_database_error", False
    if isinstance(exc, (TimeoutError, OSError)):
        return "source_io_error", True
    if isinstance(exc, RuntimeError):
        return "source_unavailable", True
    if isinstance(exc, (TypeError, ValueError)):
        return "source_invalid_payload", False
    return "source_unexpected_error", False


class IngressStore:
    """Serializes admissions and claims with short transactions in ingress.db."""

    def __init__(
        self,
        path: str | Path,
        *,
        max_entries: int = 10_000,
        max_bytes: int = 67_108_864,
        clock: Callable[[], float] = time.time,
    ):
        if (
            type(max_entries) is not int
            or max_entries < 1
            or type(max_bytes) is not int
            or max_bytes < 1
        ):
            raise ValueError("ingress capacities must be positive integers")
        self.path = Path(path)
        self.database = SQLiteExecutor(path)
        self.max_entries = max_entries
        self.max_bytes = max_bytes
        self.clock = clock

    async def open(self) -> None:
        await self.database.open()
        try:
            # Acknowledgement means a durable WAL sync, unlike the replaceable cache.
            await self.database.execute("PRAGMA synchronous = FULL")
            async with self.database.transaction() as connection:
                version = await (
                    await connection.execute("PRAGMA user_version")
                ).fetchone()
                if version[0] > 1:
                    raise IngressError("ingress_schema_newer_than_plugin")
                await connection.execute("""
                    CREATE TABLE IF NOT EXISTS ingress_records (
                        arrival_no INTEGER PRIMARY KEY AUTOINCREMENT,
                        source_id TEXT NOT NULL UNIQUE,
                        stream_id TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        payload_hash TEXT NOT NULL,
                        payload_bytes INTEGER NOT NULL,
                        state TEXT NOT NULL CHECK(state IN ('queued','running','retry_wait','failed')),
                        attempts INTEGER NOT NULL DEFAULT 0,
                        total_attempts INTEGER NOT NULL DEFAULT 0,
                        max_attempts INTEGER NOT NULL,
                        available_at REAL NOT NULL,
                        lease_token TEXT,
                        lease_until REAL,
                        error_code TEXT,
                        created_at REAL NOT NULL,
                        updated_at REAL NOT NULL
                    )
                """)
                await connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_ingress_stream ON ingress_records(stream_id,arrival_no)"
                )
                await connection.execute("PRAGMA user_version = 1")
        except BaseException:
            await self.database.close()
            raise

    async def close(self) -> None:
        await self.database.close()

    async def enqueue(
        self, source: CapturedSource, *, max_attempts: int = 3
    ) -> dict[str, Any]:
        if type(max_attempts) is not int or max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        payload, digest = _serialize(source)
        size = len(payload.encode("utf-8"))
        now = self.clock()
        async with self.database.transaction() as connection:
            existing = await (
                await connection.execute(
                    "SELECT * FROM ingress_records WHERE source_id=?",
                    (source.source_id,),
                )
            ).fetchone()
            if existing is not None:
                if existing["payload_hash"] != digest and not _same_queued_receipt(existing, source):
                    raise IngressError("ingress_source_conflict")
                return self._public(existing)
            counts = await (
                await connection.execute(
                    "SELECT COUNT(*) AS count,COALESCE(SUM(payload_bytes),0) AS bytes FROM ingress_records"
                )
            ).fetchone()
            if counts["count"] >= self.max_entries:
                raise IngressError("ingress_entry_limit")
            if size > self.max_bytes or counts["bytes"] + size > self.max_bytes:
                raise IngressError("ingress_byte_limit")
            await connection.execute(
                """
                INSERT INTO ingress_records(source_id,stream_id,payload_json,payload_hash,
                    payload_bytes,state,max_attempts,available_at,created_at,updated_at)
                VALUES(?,?,?,?,?,'queued',?,?,?,?)
            """,
                (
                    source.source_id,
                    source.identity.stream_id,
                    payload,
                    digest,
                    size,
                    max_attempts,
                    now,
                    now,
                    now,
                ),
            )
            row = await (
                await connection.execute(
                    "SELECT * FROM ingress_records WHERE source_id=?",
                    (source.source_id,),
                )
            ).fetchone()
            return self._public(row)

    async def claim(
        self, *, source_id: str | None = None, lease_seconds: float = 30
    ) -> IngressClaim | None:
        now = self.clock()
        async with self.database.transaction() as connection:
            # A crashed attempt is resumed without exhausting the failure budget.
            await connection.execute(
                """
                UPDATE ingress_records SET state='queued',lease_token=NULL,lease_until=NULL,
                    attempts=MAX(0,attempts-1),available_at=?,updated_at=?
                WHERE state='running' AND lease_until<=?
            """,
                (now, now, now),
            )
            sql = """
                SELECT q.* FROM ingress_records q
                WHERE q.state IN ('queued','retry_wait') AND q.available_at<=?
                  AND NOT EXISTS (SELECT 1 FROM ingress_records p
                    WHERE p.stream_id=q.stream_id AND p.arrival_no<q.arrival_no)
            """
            parameters: list[Any] = [now]
            if source_id is not None:
                sql += " AND q.source_id=?"
                parameters.append(source_id)
            sql += " ORDER BY q.arrival_no LIMIT 1"
            row = await (await connection.execute(sql, parameters)).fetchone()
            if row is None:
                return None
            token = uuid.uuid4().hex
            try:
                source = _deserialize(row["payload_json"])
                canonical, digest = _serialize(source)
                if (
                    source.source_id != row["source_id"]
                    or source.identity.stream_id != row["stream_id"]
                    or digest != row["payload_hash"]
                    or len(canonical.encode("utf-8")) != row["payload_bytes"]
                ):
                    raise ValueError("identity mismatch")
            except (KeyError, TypeError, ValueError, IngressError):
                await connection.execute(
                    "UPDATE ingress_records SET state='failed',error_code='ingress_corrupt_payload',updated_at=? WHERE source_id=?",
                    (now, row["source_id"]),
                )
                return None
            await connection.execute(
                """
                UPDATE ingress_records SET state='running',attempts=attempts+1,
                    total_attempts=total_attempts+1,error_code=NULL,lease_token=?,lease_until=?,updated_at=?
                WHERE source_id=?
            """,
                (token, now + lease_seconds, now, row["source_id"]),
            )
            return IngressClaim(
                row["source_id"],
                token,
                source,
                row["attempts"] + 1,
                row["max_attempts"],
            )

    async def acknowledge(self, claim: IngressClaim) -> bool:
        cursor = await self.database.execute(
            "DELETE FROM ingress_records WHERE source_id=? AND state='running' AND lease_token=?",
            (claim.source_id, claim.token),
        )
        return cursor.rowcount == 1

    async def fail(
        self,
        claim: IngressClaim,
        *,
        error_code: str,
        retryable: bool,
        retry_seconds: float,
    ) -> None:
        # Only the internal classifier supplies codes; reject accidental raw exceptions.
        allowed = {
            "source_integrity_error",
            "source_operational_error",
            "source_database_error",
            "source_io_error",
            "source_unavailable",
            "source_invalid_payload",
            "source_unexpected_error",
        }
        if error_code not in allowed:
            error_code = "source_unexpected_error"
        retry = retryable and claim.attempts < claim.max_attempts
        now = self.clock()
        await self.database.execute(
            """
            UPDATE ingress_records SET state=?,available_at=?,lease_token=NULL,
                lease_until=NULL,error_code=?,updated_at=?
            WHERE source_id=? AND state='running' AND lease_token=?
        """,
            (
                "retry_wait" if retry else "failed",
                now + retry_seconds * (2 ** min(claim.attempts - 1, 6))
                if retry
                else now,
                error_code,
                now,
                claim.source_id,
                claim.token,
            ),
        )

    async def wait_for_identity(self, claim: IngressClaim, *, retry_seconds: float) -> None:
        """Keep this stream ordered until the account ownership conflict is resolved."""
        now = self.clock()
        await self.database.execute(
            "UPDATE ingress_records SET state='retry_wait',error_code='identity_binding_conflict',"
            "available_at=?,lease_token=NULL,lease_until=NULL,attempts=MAX(0,attempts-1),updated_at=? "
            "WHERE source_id=? AND state='running' AND lease_token=?",
            (now + retry_seconds, now, claim.source_id, claim.token),
        )

    async def release(self, claim: IngressClaim) -> None:
        now = self.clock()
        await self.database.execute(
            """
            UPDATE ingress_records SET state='queued',available_at=?,lease_token=NULL,
                lease_until=NULL,attempts=MAX(0,attempts-1),updated_at=?
            WHERE source_id=? AND state='running' AND lease_token=?
        """,
            (now, now, claim.source_id, claim.token),
        )

    async def retry(self, source_id: str | None = None) -> int:
        now = self.clock()
        sql = "UPDATE ingress_records SET state='queued',attempts=0,available_at=?,error_code=NULL,updated_at=? WHERE state IN ('failed','retry_wait')"
        parameters: list[Any] = [now, now]
        if source_id is not None:
            sql += " AND source_id=?"
            parameters.append(source_id)
        cursor = await self.database.execute(sql, parameters)
        return cursor.rowcount

    @staticmethod
    def _public(row: Any, *, include_content: bool = False) -> dict[str, Any]:
        result = {
            key: row[key]
            for key in (
                "source_id",
                "stream_id",
                "arrival_no",
                "state",
                "attempts",
                "total_attempts",
                "max_attempts",
                "available_at",
                "error_code",
                "payload_bytes",
                "created_at",
                "updated_at",
            )
        }
        if include_content:
            result["source"] = json.loads(row["payload_json"])
        return result

    async def list(self, *, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        rows = await self.database.fetchall(
            "SELECT * FROM ingress_records ORDER BY arrival_no LIMIT ? OFFSET ?",
            (max(1, min(200, limit)), max(0, offset)),
        )
        return [self._public(row) for row in rows]

    async def get(
        self, source_id: str, *, include_content: bool = False
    ) -> dict[str, Any] | None:
        row = await self.database.fetchone(
            "SELECT * FROM ingress_records WHERE source_id=?", (source_id,)
        )
        return (
            self._public(row, include_content=include_content)
            if row is not None
            else None
        )

    async def health(self) -> dict[str, Any]:
        rows = await self.database.fetchall(
            "SELECT state,COUNT(*) AS count,SUM(payload_bytes) AS bytes FROM ingress_records GROUP BY state"
        )
        return {
            "schema_version": 1,
            "durable": True,
            "pending_count": sum(row["count"] for row in rows),
            "payload_bytes": sum(row["bytes"] for row in rows),
            "counts": {row["state"]: row["count"] for row in rows},
            "max_entries": self.max_entries,
            "max_bytes": self.max_bytes,
        }
