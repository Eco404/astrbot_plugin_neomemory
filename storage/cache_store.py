"""Disposable vectors only; source facts and correctness receipts live elsewhere."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import time
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any

from .sqlite_executor import SQLiteExecutor

CURRENT_SCHEMA_VERSION = 1
CacheIdentity = tuple[str, int]


def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _checked_vector(vector: Any, dimension: int = 0) -> list[float]:
    if not isinstance(vector, (list, tuple)) or not vector:
        raise ValueError("cached vector must be a nonempty sequence")
    if dimension and len(vector) != dimension:
        raise ValueError("cached vector dimension mismatch")
    if any(isinstance(item, bool) or not isinstance(item, (float, int)) for item in vector):
        raise ValueError("cached vector coordinates must be numeric")
    values = [float(item) for item in vector]
    if not all(math.isfinite(item) for item in values) or not any(values):
        raise ValueError("cached vectors must be finite and nonzero")
    return values


def _decode_vectors(raw_vectors: dict[str, str], dimension: int) -> dict[str, list[float]]:
    """CPU-only worker: no connection, event-loop state or mutable cache access."""
    return {
        key: _checked_vector(json.loads(encoded), dimension)
        for key, encoded in raw_vectors.items()
    }


def _encode_vectors(vectors: Sequence[Sequence[float]]) -> tuple[int, list[str]]:
    """Validate and serialize the whole batch before any database mutation."""
    checked = [_checked_vector(vector) for vector in vectors]
    dimension = len(checked[0])
    if any(len(vector) != dimension for vector in checked):
        raise ValueError("cached vector dimension mismatch")
    return dimension, [
        json.dumps(vector, allow_nan=False, separators=(",", ":"))
        for vector in checked
    ]


class CacheStore:
    """A separate SQLite cache with absolute TTL and per-kind LRU capacity.

    Model dimension metadata prevents incompatible vectors from mixing even
    across namespaces or restarts when a provider declares get_dim() == 0.
    No conversation or node body is persisted here.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        message_limit: int = 10_000,
        node_limit: int = 10_000,
        message_ttl_seconds: float = 7200,
        node_ttl_seconds: float = 86_400,
        clock: Callable[[], float] = time.time,
    ) -> None:
        for value in (message_limit, node_limit):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError("cache limits must be nonnegative integers")
        for value in (message_ttl_seconds, node_ttl_seconds):
            if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
                raise ValueError("cache TTL must be finite and positive")
        self.path = Path(path)
        self.database = SQLiteExecutor(path)
        self.limits = {"message": message_limit, "node": node_limit}
        self.ttls = {"message": message_ttl_seconds, "node": node_ttl_seconds}
        self.clock = clock
        self.schema_version = 0
        self.cleanup_batch_size = 500
        self.cleanup_interval_seconds = 30.0
        self.last_cleanup_error: str | None = None
        self._cleanup_task: asyncio.Task | None = None

    async def open(self) -> None:
        await self.database.open()
        try:
            async with self.database.transaction() as connection:
                row = await (await connection.execute("PRAGMA user_version")).fetchone()
                version = int(row[0])
                if version > CURRENT_SCHEMA_VERSION:
                    raise ValueError("cache database schema is newer than this plugin")
                await connection.execute(
                    """CREATE TABLE IF NOT EXISTS embedding_models (
                        fingerprint TEXT PRIMARY KEY,
                        dimension INTEGER NOT NULL CHECK (dimension > 0)
                    )"""
                )
                await connection.execute(
                    """CREATE TABLE IF NOT EXISTS embedding_cache (
                        cache_key TEXT PRIMARY KEY,
                        fingerprint TEXT NOT NULL REFERENCES embedding_models(fingerprint),
                        namespace TEXT NOT NULL,
                        kind TEXT NOT NULL CHECK (kind IN ('message','node')),
                        text_hash TEXT NOT NULL,
                        node_id TEXT,
                        content_version INTEGER,
                        vector_json TEXT NOT NULL,
                        created_at REAL NOT NULL,
                        accessed_at REAL NOT NULL,
                        expires_at REAL NOT NULL
                    )"""
                )
                await connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_embedding_cache_eviction ON embedding_cache(kind, accessed_at, cache_key)"
                )
                await connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_embedding_cache_expiration ON embedding_cache(expires_at)"
                )
                await connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_embedding_cache_node_scope ON embedding_cache(namespace,kind,node_id)"
                )
                await connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_embedding_cache_same_text "
                    "ON embedding_cache(fingerprint,namespace,kind,text_hash,expires_at DESC,cache_key)"
                )
                await connection.execute(f"PRAGMA user_version = {CURRENT_SCHEMA_VERSION}")
            self.schema_version = CURRENT_SCHEMA_VERSION
            await self.cleanup()
            self._cleanup_task = asyncio.create_task(self._cleanup_loop(), name="neomemory-cache-cleanup")
        except BaseException:
            await self.database.close()
            raise

    async def close(self) -> None:
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            await asyncio.gather(self._cleanup_task, return_exceptions=True)
            self._cleanup_task = None
        await self.database.close()

    async def _cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cleanup_interval_seconds)
            try:
                # 每个事务最多删除一批；积压清理主动让出数据库及事件循环。
                while await self.cleanup() == self.cleanup_batch_size:
                    await asyncio.sleep(.05)
                self.last_cleanup_error = None
            except Exception as exc:
                self.last_cleanup_error = type(exc).__name__

    @staticmethod
    def _keys(
        fingerprint: str,
        namespace: str,
        hashes: Sequence[str],
        kind: str,
        identities: Sequence[CacheIdentity] | None,
    ) -> tuple[list[str], list[CacheIdentity | None]]:
        if not fingerprint or not namespace:
            raise ValueError("cache requires a model fingerprint and namespace")
        if kind not in {"message", "node"}:
            raise ValueError("cache kind must be message or node")
        if kind == "node":
            if identities is None or len(identities) != len(hashes):
                raise ValueError("node cache requires matching node identities")
            resolved: list[CacheIdentity | None] = list(identities)
            for identity in resolved:
                if (
                    not isinstance(identity, (tuple, list))
                    or len(identity) != 2
                    or not isinstance(identity[0], str)
                    or not identity[0]
                    or isinstance(identity[1], bool)
                    or not isinstance(identity[1], int)
                    or identity[1] < 1
                ):
                    raise ValueError("node identity must contain node_id and positive version")
        else:
            if identities is not None:
                raise ValueError("message cache must not receive node identities")
            resolved = [None] * len(hashes)
        keys = []
        for digest, identity in zip(hashes, resolved, strict=True):
            if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                raise ValueError("cache text hashes must be SHA-256 hex digests")
            keys.append(hashlib.sha256(json.dumps(
                [fingerprint, namespace, kind, digest, identity], separators=(",", ":")
            ).encode()).hexdigest())
        return keys, resolved

    async def get_dimension(self, fingerprint: str) -> int:
        row = await self.database.fetchone(
            "SELECT dimension FROM embedding_models WHERE fingerprint = ?", (fingerprint,)
        )
        return int(row[0]) if row else 0

    async def get_vectors(
        self,
        fingerprint: str,
        namespace: str,
        hashes: Sequence[str],
        *,
        kind: str = "message",
        identities: Sequence[CacheIdentity] | None = None,
        touch: bool = True,
    ) -> list[list[float] | None]:
        keys, _ = self._keys(fingerprint, namespace, hashes, kind, identities)
        if not keys:
            return []
        now = self.clock()
        raw_vectors: dict[str, str] = {}
        async with self.database.transaction(write=False) as connection:
            dimension_row = await (await connection.execute(
                "SELECT dimension FROM embedding_models WHERE fingerprint = ?", (fingerprint,)
            )).fetchone()
            if dimension_row is None:
                return [None] * len(keys)
            dimension = int(dimension_row[0])
            unique_keys = list(dict.fromkeys(keys))
            for start in range(0, len(unique_keys), 400):
                part = unique_keys[start:start + 400]
                placeholders = ",".join("?" for _ in part)
                rows = await (await connection.execute(
                    f"SELECT cache_key, vector_json FROM embedding_cache WHERE expires_at > ? AND cache_key IN ({placeholders})",
                    (now, *part),
                )).fetchall()
                raw_vectors.update((row[0], row[1]) for row in rows)
        # JSON decoding and validation can touch millions of coordinates. Release
        # the SQLite transaction first; cancellation may abandon this pure worker
        # safely and cannot continue to the access-time update below.
        vectors = await asyncio.to_thread(_decode_vectors, raw_vectors, dimension)
        if touch and vectors:
            async with self.database.transaction() as connection:
                await connection.executemany(
                    "UPDATE embedding_cache SET accessed_at = MAX(accessed_at, ?) WHERE cache_key = ?",
                    [(now, key) for key in vectors],
                )
        return [vectors.get(key) for key in keys]

    async def get_node_vectors_by_text(
        self, fingerprint: str, namespace: str, hashes: Sequence[str], *, touch: bool = True,
    ) -> list[list[float] | None]:
        """Reuse exact node text without claiming any requested version was indexed."""
        self._keys(fingerprint, namespace, hashes, "message", None)
        if not hashes:
            return []
        now = self.clock()
        raw_vectors, keys = {}, {}
        async with self.database.transaction(write=False) as connection:
            row = await (await connection.execute(
                "SELECT dimension FROM embedding_models WHERE fingerprint=?", (fingerprint,),
            )).fetchone()
            if row is None:
                return [None] * len(hashes)
            dimension = int(row[0])
            unique_hashes = list(dict.fromkeys(hashes))
            for start in range(0, len(unique_hashes), 400):
                part = unique_hashes[start:start + 400]
                values = ",".join("(?)" for _ in part)
                rows = await (await connection.execute(
                    f"WITH requested(text_hash) AS (VALUES {values}) "
                    "SELECT requested.text_hash,c.cache_key,c.vector_json FROM requested "
                    "JOIN embedding_cache c ON c.cache_key=("
                    "SELECT existing.cache_key FROM embedding_cache existing "
                    "WHERE existing.fingerprint=? AND existing.namespace=? AND existing.kind='node' "
                    "AND existing.text_hash=requested.text_hash AND existing.expires_at>? "
                    "ORDER BY existing.expires_at DESC,existing.cache_key LIMIT 1)",
                    (*part, fingerprint, namespace, now),
                )).fetchall()
                for digest, key, encoded in rows:
                    raw_vectors[digest], keys[digest] = encoded, key
        vectors = await asyncio.to_thread(_decode_vectors, raw_vectors, dimension)
        if touch and vectors:
            async with self.database.transaction() as connection:
                await connection.executemany(
                    "UPDATE embedding_cache SET accessed_at=MAX(accessed_at,?) WHERE cache_key=?",
                    [(now, keys[digest]) for digest in vectors],
                )
        return [vectors.get(digest) for digest in hashes]

    async def put_vectors(
        self,
        fingerprint: str,
        namespace: str,
        hashes: Sequence[str],
        vectors: Sequence[Sequence[float]],
        *,
        kind: str = "message",
        identities: Sequence[CacheIdentity] | None = None,
    ) -> None:
        keys, resolved = self._keys(fingerprint, namespace, hashes, kind, identities)
        if len(vectors) != len(keys):
            raise ValueError("cache vector count does not match keys")
        if not vectors:
            return
        dimension, encoded = await asyncio.to_thread(_encode_vectors, vectors)
        if self.limits[kind] == 0:
            return
        now = self.clock()
        async with self.database.transaction() as connection:
            previous = await (await connection.execute(
                "SELECT dimension FROM embedding_models WHERE fingerprint = ?", (fingerprint,)
            )).fetchone()
            if previous is not None and int(previous[0]) != dimension:
                raise ValueError("cached model dimension mismatch")
            await connection.execute(
                "INSERT OR IGNORE INTO embedding_models(fingerprint, dimension) VALUES (?, ?)",
                (fingerprint, dimension),
            )
            await connection.executemany(
                """INSERT INTO embedding_cache (
                    cache_key, fingerprint, namespace, kind, text_hash, node_id,
                    content_version, vector_json, created_at, accessed_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    vector_json = excluded.vector_json,
                    created_at = excluded.created_at,
                    accessed_at = excluded.accessed_at,
                    expires_at = excluded.expires_at""",
                [(
                    key, fingerprint, namespace, kind, digest,
                    identity[0] if identity else None,
                    identity[1] if identity else None,
                    vector_json,
                    now, now, now + self.ttls[kind],
                ) for key, digest, identity, vector_json in zip(keys, hashes, resolved, encoded, strict=True)],
            )

    async def _cleanup(self, connection: Any, now: float) -> int:
        deleted = 0
        cursor = await connection.execute(
            "DELETE FROM embedding_cache WHERE cache_key IN ("
            "SELECT cache_key FROM embedding_cache WHERE expires_at <= ? ORDER BY expires_at LIMIT ?)",
            (now, self.cleanup_batch_size))
        deleted += cursor.rowcount
        for kind, limit in self.limits.items():
            remaining = self.cleanup_batch_size - deleted
            if remaining <= 0:
                break
            cursor = await connection.execute(
                """DELETE FROM embedding_cache WHERE cache_key IN (
                    SELECT cache_key FROM embedding_cache WHERE kind = ?
                    ORDER BY accessed_at DESC, cache_key DESC
                    LIMIT ? OFFSET ?
                )""", (kind, remaining, limit),
            )
            deleted += cursor.rowcount
        return deleted

    async def invalidate_nodes(self, namespace: str, node_ids: Iterable[str]) -> int:
        """Drop every model/version cache entry for these nodes in one space.

        Message vectors and model-dimension metadata remain reusable. This is
        cache maintenance, not an authoritative tombstone: scope, generation,
        content version and stale-summary checks still live in memory.db.
        """
        if not isinstance(namespace, str) or not namespace.strip():
            raise ValueError("node invalidation requires a namespace")
        if isinstance(node_ids, (str, bytes)):
            raise ValueError("node_ids must be a collection of node IDs")
        ids = list(node_ids)
        if any(not isinstance(node_id, str) or not node_id.strip() for node_id in ids):
            raise ValueError("node IDs must be nonempty strings")
        ids = list(dict.fromkeys(ids))
        if not ids:
            return 0
        deleted = 0
        async with self.database.transaction() as connection:
            for start in range(0, len(ids), 400):
                batch = ids[start:start + 400]
                placeholders = ",".join("?" for _ in batch)
                cursor = await connection.execute(
                    f"DELETE FROM embedding_cache WHERE namespace=? AND kind='node' AND node_id IN ({placeholders})",
                    (namespace, *batch),
                )
                deleted += cursor.rowcount
        return deleted

    async def cleanup(self) -> int:
        """删除一批过期或超容量项；普通读写不执行全局清理。"""
        async with self.database.transaction() as connection:
            return await self._cleanup(connection, self.clock())

    async def health(self) -> dict[str, Any]:
        rows = await self.database.fetchall(
            "SELECT kind, COUNT(*) AS count FROM embedding_cache GROUP BY kind"
        )
        models = await self.database.fetchone("SELECT COUNT(*) FROM embedding_models")
        counts = {"message": 0, "node": 0}
        counts.update({row["kind"]: int(row["count"]) for row in rows})
        return {
            "path": str(self.path),
            "exists": self.path.exists(),
            "schema_version": self.schema_version,
            "expected_schema_version": CURRENT_SCHEMA_VERSION,
            "entry_count": sum(counts.values()),
            "counts": counts,
            "model_count": int(models[0]),
            "limits": dict(self.limits),
            "ttl_seconds": dict(self.ttls),
            "cleanup_batch_size": self.cleanup_batch_size,
            "cleanup_interval_seconds": self.cleanup_interval_seconds,
            "last_cleanup_error": self.last_cleanup_error,
        }
