"""Bounded embedding batches and disposable, version-aware vector reuse."""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Sequence
from typing import Any

import numpy as np

try:
    from ..adapters.providers import (
        EmbeddingValidationError,
        ProviderAdapter,
        _positive_timeout,
        normalize_vectors,
    )
    from ..storage.cache_store import CacheIdentity, CacheStore, text_hash
except ImportError:  # Tests import from the plugin root.
    from adapters.providers import (
        EmbeddingValidationError,
        ProviderAdapter,
        _positive_timeout,
        normalize_vectors,
    )
    from storage.cache_store import CacheIdentity, CacheStore, text_hash


def _normalize_cached_vectors(
    vectors: list[list[float] | None], dimension: int,
) -> list[list[float] | None]:
    """Normalize CacheStore-validated vectors in a pure worker.

    CacheStore already rejects nonnumeric, boolean, nonfinite, zero or
    dimension-mismatched coordinates before returning any batch. Avoid a
    second Python validation loop over every coordinate. The independent
    provider response path still uses normalize_vectors for untrusted input.
    Scale before norm computation so finite extreme coordinates stay valid.
    """
    present = [vector for vector in vectors if vector is not None]
    if not present:
        return [None] * len(vectors)
    try:
        matrix = np.asarray(present, dtype=np.float64)
    except (TypeError, ValueError) as exc:
        raise EmbeddingValidationError("cached embedding dimension mismatch") from exc
    if matrix.ndim != 2 or matrix.shape[1] == 0 or (dimension and matrix.shape[1] != dimension):
        raise EmbeddingValidationError("cached embedding dimension mismatch")
    if not np.isfinite(matrix).all():
        raise EmbeddingValidationError("cached embedding coordinates must be finite")
    scale = np.max(np.abs(matrix), axis=1, keepdims=True)
    if np.any(scale == 0):
        raise EmbeddingValidationError("cached embedding vector must be nonzero")
    scaled = matrix / scale
    normalized = iter((scaled / np.sqrt(np.sum(scaled * scaled, axis=1, keepdims=True))).tolist())
    return [next(normalized) if vector is not None else None for vector in vectors]


class EmbeddingService:
    """Reuse exact text within a memory space, never across model fingerprints.

    The short-lived lock coalesces overlapping calls through a second cache
    lookup; waiting is covered by the caller's deadline. Individual network
    batches still run concurrently under the adapter's embedding semaphore.
    There is no second retry loop or hidden batch-to-single fallback.
    """

    def __init__(
        self,
        adapter: ProviderAdapter,
        cache: CacheStore,
        *,
        provider_id: str,
        batch_size: int = 16,
        revision: str = "",
    ) -> None:
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
            raise ValueError("embedding batch_size must be a positive integer")
        self.adapter = adapter
        self.cache = cache
        self.provider_id = provider_id
        self.batch_size = batch_size
        self.revision = revision
        self._lock = asyncio.Lock()
        self.last_cache_error: str | None = None

    async def _cache_dimension(self, fingerprint: str) -> int:
        try:
            return await self.cache.get_dimension(fingerprint)
        except (OSError, sqlite3.Error, RuntimeError) as exc:
            self.last_cache_error = type(exc).__name__
            return 0

    @property
    def fingerprint(self) -> str:
        return self.adapter.embedding_fingerprint(self.provider_id, self.revision)

    def _inputs(
        self,
        texts: Sequence[str],
        namespace: str,
        kind: str,
        identities: Sequence[CacheIdentity] | None,
    ) -> tuple[list[str], str]:
        if any(not isinstance(text, str) or not text.strip() for text in texts):
            raise ValueError("embedding inputs must contain nonempty text")
        fingerprint = self.fingerprint
        hashes = [text_hash(text) for text in texts]
        self.cache._keys(fingerprint, namespace, hashes, kind, identities)
        return hashes, fingerprint

    async def _cached(
        self,
        hashes: Sequence[str],
        fingerprint: str,
        namespace: str,
        kind: str,
        identities: Sequence[CacheIdentity] | None,
        *,
        allow_text_reuse: bool = True,
        read_only: bool = False,
    ) -> tuple[list[list[float] | None], bool]:
        try:
            vectors = await self.cache.get_vectors(
                fingerprint, namespace, hashes, kind=kind, identities=identities, touch=not read_only
            )
        except (OSError, sqlite3.Error, RuntimeError, ValueError) as exc:
            # 缓存损坏或不可读等同未命中，调用方仍决定是否允许联网补算。
            self.last_cache_error = type(exc).__name__
            vectors = [None] * len(hashes)
        missing_node_keys = kind == "node" and any(vector is None for vector in vectors)
        if missing_node_keys and allow_text_reuse:
            # Emotion-only versions keep the same embedding input. Reuse that
            # exact text, while embed() still persists the requested version key.
            missing = [index for index, vector in enumerate(vectors) if vector is None]
            try:
                previous = await self.cache.get_node_vectors_by_text(
                    fingerprint, namespace, [hashes[index] for index in missing], touch=not read_only,
                )
            except (OSError, sqlite3.Error, RuntimeError, ValueError) as exc:
                self.last_cache_error = type(exc).__name__
                previous = [None] * len(missing)
            for index, vector in zip(missing, previous, strict=True):
                vectors[index] = vector
            missing = [index for index, vector in enumerate(vectors) if vector is None]
            if missing:
                try:
                    messages = await self.cache.get_vectors(
                        fingerprint, namespace, [hashes[index] for index in missing], touch=not read_only,
                    )
                except (OSError, sqlite3.Error, RuntimeError, ValueError) as exc:
                    self.last_cache_error = type(exc).__name__
                    messages = [None] * len(missing)
                for index, vector in zip(missing, messages, strict=True):
                    vectors[index] = vector
        dimension = await self._cache_dimension(fingerprint)
        declared = self.adapter.embedding_dimension(self.provider_id)
        if self.fingerprint != fingerprint:
            raise EmbeddingValidationError("embedding provider configuration changed during request")
        if dimension and declared and dimension != declared:
            raise EmbeddingValidationError("cached model dimension does not match provider")
        normalized = await asyncio.to_thread(
            _normalize_cached_vectors, vectors, dimension or declared,
        )
        if self.fingerprint != fingerprint:
            raise EmbeddingValidationError("embedding provider configuration changed during request")
        return normalized, missing_node_keys

    async def cached(
        self,
        texts: Sequence[str],
        *,
        namespace: str,
        kind: str = "message",
        identities: Sequence[CacheIdentity] | None = None,
        allow_text_reuse: bool = True,
        read_only: bool = False,
    ) -> list[list[float] | None]:
        """Read vectors only; disable text reuse to require exact node/version keys."""
        hashes, fingerprint = self._inputs(texts, namespace, kind, identities)
        vectors, _ = await self._cached(hashes, fingerprint, namespace, kind, identities,
                                      allow_text_reuse=allow_text_reuse, read_only=read_only)
        return vectors

    async def embed(
        self,
        texts: Sequence[str],
        *,
        namespace: str,
        kind: str = "message",
        timeout_seconds: float = 30,
        identities: Sequence[CacheIdentity] | None = None,
        diagnostics: dict[str, Any] | None = None,
        require_persisted: bool = False,
        read_only: bool = False,
    ) -> list[list[float]]:
        """Use the call deadline for required index writes, short writes otherwise.

        Index callers verify exact node/version keys after this call. Diagnostics
        report this call only; provider timing sums can overlap across batches.
        """
        timeout = _positive_timeout(timeout_seconds)
        # Validate before acquiring the lock; no database or provider operation
        # occurs outside the deadline.
        hashes, _ = self._inputs(texts, namespace, kind, identities)
        if not texts:
            return []
        loop = asyncio.get_running_loop()
        started = loop.time()
        deadline = started + timeout
        acquired = False
        stage = "service_queue"
        batch_metrics: list[dict[str, Any]] = []
        if diagnostics is not None:
            diagnostics.update(service_queue_seconds=0., service_cache_seconds=0., cache_hit_count=0)
        try:
            async with asyncio.timeout(timeout):
                async with self._lock:
                    acquired = True
                    if diagnostics is not None:
                        diagnostics["service_queue_seconds"] = loop.time() - started
                    stage = "service_cache"
                    cache_started = loop.time()
                    fingerprint = self.fingerprint
                    cached, missing_node_keys = await self._cached(
                        hashes, fingerprint, namespace, kind, identities, read_only=read_only)
                    if diagnostics is not None:
                        diagnostics["service_cache_seconds"] = loop.time() - cache_started
                        diagnostics["cache_hit_count"] = sum(vector is not None for vector in cached)
                    by_hash = {
                        digest: vector for digest, vector in zip(hashes, cached, strict=True)
                        if vector is not None
                    }
                    missing = {
                        digest: text for digest, text in zip(hashes, texts, strict=True)
                        if digest not in by_hash
                    }
                    pending = list(missing.items())
                    stage = "provider_batches"
                    tasks = []
                    for start in range(0, len(pending), self.batch_size):
                        kwargs = {}
                        if diagnostics is not None:
                            batch_diagnostics: dict[str, Any] = {}
                            batch_metrics.append(batch_diagnostics)
                            kwargs["diagnostics"] = batch_diagnostics
                        tasks.append(asyncio.create_task(self.adapter.embed_batch(
                            [text for _, text in pending[start:start + self.batch_size]],
                            provider_id=self.provider_id, timeout_seconds=timeout, **kwargs,
                        )))
                    try:
                        results = await asyncio.gather(*tasks)
                    except BaseException:
                        for task in tasks:
                            task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
                        raise
                    stage = "response_validation"
                    dimension = await self._cache_dimension(fingerprint)
                    declared = self.adapter.embedding_dimension(self.provider_id)
                    returned = [vector for batch in results for vector in batch]
                    if returned:
                        returned = await asyncio.to_thread(
                            normalize_vectors, returned, count=len(pending), dimension=dimension or declared
                        )
                    if self.fingerprint != fingerprint:
                        raise EmbeddingValidationError("embedding provider configuration changed during request")
                    by_hash.update({
                        digest: vector for (digest, _), vector in zip(pending, returned, strict=True)
                    })
                    result = [list(by_hash[digest]) for digest in hashes]
                    # 在线缓存写入留出返回余量；后台索引使用已有总期限等待写入，
                    # 由索引调用方读回当前版本，不能把未保存的结果当成完成索引。
                    if not read_only and (pending or missing_node_keys or any(vector is None for vector in cached)):
                        stage = "cache_write"
                        remaining = deadline - asyncio.get_running_loop().time()
                        try:
                            write_timeout = remaining if require_persisted else min(.1, max(.001, remaining / 2))
                            async with asyncio.timeout(write_timeout):
                                await self.cache.put_vectors(
                                    fingerprint, namespace, hashes, result, kind=kind, identities=identities
                                )
                        except (OSError, sqlite3.Error, RuntimeError) as exc:
                            self.last_cache_error = type(exc).__name__
                    return result
        except BaseException as exc:
            if diagnostics is not None:
                diagnostics.update(failure_stage=stage, error_type=type(exc).__name__)
            raise
        finally:
            if diagnostics is not None:
                if not acquired:
                    diagnostics["service_queue_seconds"] = loop.time() - started
                elif stage == "service_cache":
                    diagnostics["service_cache_seconds"] = loop.time() - cache_started
                diagnostics.update(
                    service_total_seconds=loop.time() - started,
                    planned_batch_count=len(batch_metrics),
                    provider_call_count=sum(bool(item.get("call_started")) for item in batch_metrics),
                    provider_completed_batch_count=sum(bool(item.get("completed")) for item in batch_metrics),
                    provider_queue_seconds_sum=sum(item.get("queue_seconds", 0.) for item in batch_metrics),
                    provider_call_seconds_sum=sum(item.get("call_seconds", 0.) for item in batch_metrics),
                    provider_failure_stages=sorted({item["failure_stage"] for item in batch_metrics if "failure_stage" in item}),
                )
