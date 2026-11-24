"""Bounded async workers over durable tasks; no browser owns their lifetime."""

from __future__ import annotations

import asyncio
import logging
import math
import re
import uuid
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

try:
    from ..storage.job_store import JobConflict, JobStore, LeaseLost
except ImportError:
    from storage.job_store import JobConflict, JobStore, LeaseLost

from .provider_errors import is_transient_provider_error

__all__ = ["JobConflict", "JobRunner", "LeaseLost", "ModelNetworkError", "RetryableJobError", "ReviewRequired"]

logger = logging.getLogger(__name__)
Handler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


class RetryableJobError(RuntimeError):
    """An adapter classified a transient error after its own retry policy ended."""

    def __init__(self, code: str = "temporary_failure"):
        self.code = code
        super().__init__(code)


class ModelNetworkError(RetryableJobError):
    """A model request failed at the transport or HTTP service boundary."""


class ReviewRequired(RuntimeError):
    """A task needs review; it must not keep spending automatic model attempts."""

    def __init__(self, code: str = "review_required"):
        self.code = code
        super().__init__(code)


def classify_job_error(exc):
    retryable = isinstance(exc, RetryableJobError) or is_transient_provider_error(exc)
    if isinstance(exc, (ValueError, PermissionError, ReviewRequired)):
        retryable = False
    code = "temporary_failure" if retryable else "review_required"
    # Adapter codes are allowed only if they are safe identifiers; never
    # persist arbitrary exception text if an upstream supplied a bad code.
    if isinstance(exc, (RetryableJobError, ReviewRequired)):
        if isinstance(exc.code, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", exc.code):
            code = exc.code
    return retryable, code


class JobRunner:
    """One poller with bounded handlers, lease heartbeats, and graceful resume.

    Handlers commit derived data only while holding a valid lease, and persist a
    checkpoint at safe resumable boundaries. The runner does not wrap Provider
    calls in immediate retries: a transient failure schedules a later job attempt.
    """

    def __init__(
        self, store: JobStore, handlers: Mapping[str, Handler], *, concurrency: int = 2,
        lease_seconds: float = 30, poll_seconds: float = 0.25,
        reprepare: Callable[[dict[str, Any]], Awaitable[dict[str, Any] | None]] | None = None,
    ):
        if type(concurrency) is not int or not 1 <= concurrency <= 32:
            raise ValueError("concurrency must be between 1 and 32")
        for key, value in (("lease_seconds", lease_seconds), ("poll_seconds", poll_seconds)):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{key} must be a finite positive number")
        self.reprepare = reprepare
        self.store = store
        self.handlers = dict(handlers)
        self.concurrency = concurrency
        self.lease_seconds = float(lease_seconds)
        self.poll_seconds = float(poll_seconds)
        self.owner = f"runner_{uuid.uuid4().hex}"
        self._poller: asyncio.Task | None = None
        self._workers: dict[str, asyncio.Task] = {}
        self._stopping = False
        self.paused = False
        self._wake = asyncio.Event()
        self.last_error_type: str | None = None

    @property
    def running(self) -> bool:
        return self._poller is not None and not self._poller.done()

    def health(self) -> dict[str, Any]:
        return {"running": self.running, "running_count": len(self._workers),
                "last_error_type": self.last_error_type}

    async def start(self) -> None:
        if self.running:
            return
        self._stopping = False
        self._poller = asyncio.create_task(self._poll(), name="neomemory-job-poller")

    async def stop(self) -> None:
        self._stopping = True
        poller = self._poller
        if poller is not None:
            poller.cancel()
            await asyncio.gather(poller, return_exceptions=True)
        workers = list(self._workers.values())
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        self._workers.clear()
        self._poller = None

    def _diagnostic(self, exc: BaseException) -> None:
        self.last_error_type = type(exc).__name__
        logger.warning("NeoMemory task runner diagnostic: %s", self.last_error_type)

    async def _poll(self) -> None:
        while not self._stopping:
            try:
                for job_id, task in list(self._workers.items()):
                    if task.done():
                        self._workers.pop(job_id)
                        if not task.cancelled() and task.exception() is not None:
                            self._diagnostic(task.exception())
                while not self._stopping and not self.paused and len(self._workers) < self.concurrency:
                    lease_seconds = self.lease_seconds
                    job = await self.store.claim(self.owner, lease_seconds=lease_seconds)
                    if job is None:
                        break
                    if self.paused:
                        await self._release(job)
                        break
                    self._workers[job["job_id"]] = asyncio.create_task(
                        self._execute(job, lease_seconds=lease_seconds), name=f"neomemory-{job['job_id']}",
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._diagnostic(exc)
            try:
                await asyncio.wait_for(self._wake.wait(), self.poll_seconds)
            except TimeoutError:
                pass
            self._wake.clear()

    async def _heartbeat(self, job: dict[str, Any], lease_seconds: float) -> None:
        while True:
            await asyncio.sleep(lease_seconds / 3)
            await self.store.heartbeat(job["job_id"], job["lease_token"], lease_seconds=lease_seconds)

    async def _release(self, job: dict[str, Any]) -> None:
        try:
            await self.store.release(job["job_id"], job["lease_token"])
        except LeaseLost:
            pass  # Cancelled, completed, or a different worker already owns it.
        except Exception as exc:
            self._diagnostic(exc)  # Lease expiry remains the durable recovery path.

    async def _execute(self, job: dict[str, Any], *, lease_seconds: float | None = None) -> None:
        work: asyncio.Task | None = None
        heartbeat: asyncio.Task | None = None
        try:
            fresh = job["recovery_mode"] == "fresh"
            handler = self.reprepare if fresh else self.handlers.get(job["kind"])
            if handler is None:
                raise ReviewRequired("unsupported_job_kind")
            work = asyncio.create_task(handler(job))
            heartbeat = asyncio.create_task(self._heartbeat(
                job, self.lease_seconds if lease_seconds is None else lease_seconds))
            done, _ = await asyncio.wait((work, heartbeat), return_when=asyncio.FIRST_COMPLETED)
            if heartbeat in done:
                heartbeat.result()  # A lost lease must cancel the old handler.
                raise LeaseLost("heartbeat ended")
            result = work.result()
            if not fresh or result is None:
                await self.store.finish(job["job_id"], job["lease_token"],
                                        result={"skipped": True, "reason": "no_pending_work"} if fresh else result)
        except asyncio.CancelledError:
            # Handler cancellation must finish before releasing its mutation right.
            if work is not None:
                work.cancel()
                await asyncio.gather(work, return_exceptions=True)
            await self._release(job)
            raise
        except LeaseLost:
            pass
        except Exception as exc:
            retryable, code = classify_job_error(exc)
            try:
                await self.store.fail(job["job_id"], job["lease_token"], error_type=type(exc).__name__,
                                      error_code=code, retryable=retryable,
                                      model_network=isinstance(exc, ModelNetworkError))
            except LeaseLost:
                pass
            except Exception as failure:
                self._diagnostic(failure)
        finally:
            for task in (work, heartbeat):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(*(task for task in (work, heartbeat) if task is not None),
                                 return_exceptions=True)
