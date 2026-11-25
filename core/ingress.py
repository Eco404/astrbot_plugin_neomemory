"""Durable source capture with bounded retries and per-stream ordering."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

try:
    from ..storage.ingress_store import (
        IngressClaim,
        IngressError,
        IngressStore,
        classify_capture_error,
    )
    from ..storage.social_accounts import AmbiguousAccountOwner
    from ..storage.source_store import CaptureResult, SourceStore
except ImportError:
    from storage.ingress_store import (
        IngressClaim,
        IngressError,
        IngressStore,
        classify_capture_error,
    )
    from storage.social_accounts import AmbiguousAccountOwner
    from storage.source_store import CaptureResult, SourceStore

from .source_ingest import CapturedSource

logger = logging.getLogger(__name__)
__all__ = ["IngressError", "IngressService", "IngressSubmission"]


@dataclass(frozen=True, slots=True)
class IngressSubmission:
    source_id: str
    stream_id: str
    state: str
    durable: bool = True
    captured: CaptureResult | None = None


class IngressService:
    """One local writer over a durable inbox; independent of browser sessions.

    Admission failure is explicit and never reported as durable acceptance. This
    service does not silently move unacknowledged data to a volatile fallback.
    source.db owns deduplication, so a crash after its commit safely retries the
    same source before removing the inbox row. No cross-database transaction is
    used, and an obsolete claimant cannot acknowledge another worker's row.
    """

    def __init__(self, store: IngressStore, sources: SourceStore, config: Any, *,
                 readmit: Callable[[int], Awaitable[int]] | None = None, audit=None):
        self.audit = audit
        self.store = store
        self.sources = sources
        self.readmit = readmit
        self.max_attempts = int(getattr(config, "ingress_max_attempts", 3))
        self.retry_seconds = float(getattr(config, "ingress_retry_seconds", 5))
        self.poll_seconds = float(getattr(config, "ingress_poll_seconds", 0.5))
        if self.max_attempts < 1 or self.retry_seconds <= 0 or self.poll_seconds <= 0:
            raise ValueError("invalid ingress retry configuration")
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._stopping = False
        self._wake = asyncio.Event()
        self._submissions = 0
        self._idle = asyncio.Event()
        self._idle.set()
        self.last_error_code: str | None = None
        self.admission_failures = 0

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self.running:
            return
        self._stopping = False
        self._task = asyncio.create_task(self._run(), name="neomemory-source-ingress")

    async def stop(self) -> None:
        self._stopping = True
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        # Frontend admissions and writes finish before the owner closes either DB.
        await self._idle.wait()
        async with self._lock:
            pass

    async def _diagnostic(self, code: str) -> None:
        if self.audit is not None and code != self.last_error_code:
            await self.audit.record("warning", "system", "原文待写队列异常", error_code=code)
        self.last_error_code = code
        logger.warning("NeoMemory source ingress diagnostic: %s", code)

    async def submit(self, source: CapturedSource) -> IngressSubmission:
        if self._stopping:
            raise IngressError("ingress_stopping")
        self._submissions += 1
        self._idle.clear()
        try:
            return await self._submit(source)
        finally:
            self._submissions -= 1
            if not self._submissions:
                self._idle.set()

    async def enqueue_before_stop(self, source: CapturedSource) -> None:
        """Persist the source for restart, draining eligible queue heads if space is needed."""
        while True:
            try:
                await self.store.enqueue(source, max_attempts=self.max_attempts)
                return
            except IngressError as exc:
                if exc.code not in {"ingress_entry_limit", "ingress_byte_limit"}:
                    raise
                if await self.process_once() is None:
                    raise

    async def _submit(self, source: CapturedSource) -> IngressSubmission:
        try:
            await self.store.enqueue(source, max_attempts=self.max_attempts)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.admission_failures += 1
            code = (
                exc.code
                if isinstance(exc, IngressError)
                else "ingress_persistence_unavailable"
            )
            await self._diagnostic(code)
            raise IngressError(code) from exc
        self._wake.set()
        # Preserve the usual immediate capture path, while still recording it
        # durably before source.capture can run. A blocked head remains queued.
        captured = None
        state = "queued"
        try:
            if not self._stopping:
                captured = await self.process_once(source_id=source.source_id)
            if captured is not None:
                state = "captured"
            else:
                row = await self.store.get(source.source_id)
                state = row["state"] if row is not None else "captured"
        except asyncio.CancelledError:
            raise
        except Exception:
            # Admission already succeeded; a later inbox read/update failure must
            # not misreport it as an unpersisted message or expose exception text.
            await self._diagnostic("ingress_worker_unavailable")
        return IngressSubmission(
            source.source_id, source.identity.stream_id, state, captured=captured
        )

    async def process_once(
        self, *, source_id: str | None = None
    ) -> CaptureResult | None:
        async with self._lock:
            claim = await self.store.claim(source_id=source_id)
            if claim is None:
                return None
            return await self._capture(claim)

    async def _capture(self, claim: IngressClaim) -> CaptureResult | None:
        try:
            result = await self.sources.capture(claim.source)
        except asyncio.CancelledError:
            await asyncio.shield(self.store.release(claim))
            raise
        except AmbiguousAccountOwner:
            await self.store.wait_for_identity(claim, retry_seconds=max(5.0, self.retry_seconds))
            return None
        except Exception as exc:
            code, retryable = classify_capture_error(exc)
            await self._diagnostic(code)
            await self.store.fail(
                claim,
                error_code=code,
                retryable=retryable,
                retry_seconds=self.retry_seconds,
            )
            return None
        # If this acknowledgement fails, retain the row. SourceStore deduplicates
        # a resumed capture, including a commit that completed during cancellation.
        try:
            if await self.store.acknowledge(claim):
                self.last_error_code = None
        except asyncio.CancelledError:
            await asyncio.shield(self.store.release(claim))
            raise
        except Exception:
            await self._diagnostic("ingress_acknowledgement_failed")
            try:
                await self.store.release(claim)
            except Exception:
                pass  # An expired lease is the durable recovery path.
        return result

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        next_readmission = loop.time()
        while not self._stopping:
            try:
                self._wake.clear()
                if self.readmit is not None and loop.time() >= next_readmission:
                    next_readmission = loop.time() + self.retry_seconds
                    await self.readmit(32)
                captured = await self.process_once()
                if captured is not None:
                    continue
                await asyncio.wait_for(self._wake.wait(), timeout=self.poll_seconds)
            except TimeoutError:
                pass
            except asyncio.CancelledError:
                raise
            except Exception:
                await self._diagnostic("ingress_worker_unavailable")
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=self.poll_seconds)
                except TimeoutError:
                    pass

    async def retry(self, source_id: str | None = None) -> int:
        count = await self.store.retry(source_id)
        self._wake.set()
        return count

    async def list(self, *, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        return await self.store.list(limit=limit, offset=offset)

    async def get(
        self, source_id: str, *, include_content: bool = False
    ) -> dict[str, Any] | None:
        return await self.store.get(source_id, include_content=include_content)

    async def health(self) -> dict[str, Any]:
        result = await self.store.health()
        result.update(
            running=self.running,
            last_error_code=self.last_error_code,
            admission_failures=self.admission_failures,
        )
        return result
