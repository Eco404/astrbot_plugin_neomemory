"""Concurrent topic synthesis and context replay under a single space lease."""

import asyncio
from copy import deepcopy

from .jobs import LeaseLost, ModelNetworkError, RetryableJobError, ReviewRequired, classify_job_error


async def rebuild_sections(engine, job):
    # Both branches have independent, idempotent publication receipts. Only
    # checkpoint writes are serialized; model and embedding work can overlap.
    root = deepcopy(job["checkpoint"])
    previous_failure = root.pop("failed_section", None)
    lanes = root.setdefault("sections", {})
    progress = root.setdefault("section_progress", {})
    for name in ("context", "topics"):
        lanes.setdefault(name, {})
        progress.setdefault(name, {"state": "queued", "step": "queued", "done": 0, "total": None})
    lock = asyncio.Lock()

    async def persist():
        done = sum(entry["done"] for entry in progress.values())
        totals = [entry["total"] for entry in progress.values()]
        await engine._save(job, root, "parallel_rebuild", done, sum(totals) if None not in totals else None)

    async def run(name, method):
        worker = engine.snapshot()

        async def save(_job, checkpoint, step, done=0, total=None):
            async with lock:
                lanes[name] = deepcopy(checkpoint)
                progress[name].update(state="running", step=step, done=done, total=total)
                await persist()

        worker._save = save
        entry = progress[name]
        if entry["state"] == "failed" and entry.get("error_code"):
            error_type = RetryableJobError if entry.get("retry_exhausted") else ReviewRequired
            raise error_type(entry["error_code"])

        # Older checkpoints only recorded the parent's attempt. A waiting lane
        # has already spent that attempt and can resume at its next call.
        if "attempt" not in entry and entry["state"] == "retry_wait":
            entry["attempt"] = job["attempt"]
            if previous_failure and job["attempt"] > 1:
                entry["attempt"] -= 1
        entry.setdefault("attempt", job["attempt"])
        while True:
            if entry["state"] == "retry_wait":
                delay = entry.get("available_at", 0) - engine.jobs.clock()
                if delay > 0:
                    await asyncio.sleep(delay)
                async with lock:
                    entry["attempt"] += 1
                    entry["state"] = "running"
                    entry.pop("available_at", None)
                    entry.pop("error_code", None)
                    await persist()
            try:
                result = await method(worker, {**job, "checkpoint": deepcopy(lanes[name])})
                break
            except LeaseLost:
                raise
            except Exception as exc:
                temporary, code = classify_job_error(exc)
                delay = engine.jobs.next_retry_delay(entry["attempt"], job["max_attempts"],
                    retryable=temporary, model_network=isinstance(exc, ModelNetworkError))
                retry = delay is not None
                async with lock:
                    entry.update(state="retry_wait" if retry else "failed", error_code=code)
                    if retry:
                        entry["available_at"] = engine.jobs.clock() + delay
                    elif temporary:
                        entry["retry_exhausted"] = True
                    await persist()
                if not retry:
                    raise
        async with lock:
            lanes[name] = {}
            done = progress[name]["total"] or progress[name]["done"]
            progress[name].update(state="succeeded", step="complete", done=done, total=done)
            await persist()
        return result

    results = await asyncio.gather(
        run("context", type(engine)._replay_rebuild), run("topics", type(engine)._replace_topics),
        return_exceptions=True)
    failures = [(name, result) for name, result in zip(("context", "topics"), results)
                if isinstance(result, BaseException)]
    if failures:
        # Report manual failures ahead of transient errors so a valid sibling
        # candidate is never selected in place of the actual rejected window.
        name, error = min(failures, key=lambda failure: classify_job_error(failure[1])[0])
        root["failed_section"] = name
        await persist()
        temporary, code = classify_job_error(error)
        if temporary:
            raise ReviewRequired(code) from error
        raise error
    context, topics = results
    return {**context, "topics": topics, "section_progress": progress}
