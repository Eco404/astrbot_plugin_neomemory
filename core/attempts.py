"""Per-execution telemetry shared by the job handler and its model calls."""
import asyncio
from contextvars import ContextVar

from .provider_errors import http_status

current_attempt = ContextVar("neomemory_job_attempt", default=None)


def observe_action(action):
    attempt = current_attempt.get()
    if attempt is not None:
        attempt["action"] = action


def observe_commit():
    attempt = current_attempt.get()
    if attempt is not None:
        attempt["commits"] += 1


async def observed_call(method, *args, **kwargs):
    diagnostics = kwargs.setdefault("diagnostics", {})
    try:
        return await method(*args, **kwargs)
    except BaseException as exc:
        # asyncio.timeout turns a tagged cancellation into TimeoutError at its outer boundary.
        queued = diagnostics.get("call_started") is False and diagnostics.get("failure_stage") == "provider_queue"
        if isinstance(exc, (asyncio.CancelledError, TimeoutError)):
            exc.neomemory_model_stage = "provider_queue" if queued else "provider_call"
        attempt = current_attempt.get()
        if attempt is not None and queued:
            attempt["queued_without_call"] = True
        if attempt is not None and (status := http_status(exc)) is not None:
            attempt["http_status"] = status
        raise
    finally:
        attempt = current_attempt.get()
        if attempt is not None:
            attempt["model_calls"] += diagnostics.get("call_count", int(diagnostics.get("call_started", True)))
