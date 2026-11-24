"""Concurrent topic synthesis and context replay under a single space lease."""

import asyncio
from copy import deepcopy

from .jobs import JobDeferred, LeaseLost, ModelNetworkError, ReviewRequired, classify_job_error


async def rebuild_sections(engine, job):
    root = deepcopy(job["checkpoint"])
    previous_failure = root.pop("failed_section", None)
    lanes = root.setdefault("sections", {})
    progress = root.setdefault("section_progress", {})
    methods = {"context": type(engine)._replay_rebuild, "topics": type(engine)._replace_topics}
    for name in methods:
        lanes.setdefault(name, {})
        entry = progress.setdefault(name, {"state": "queued", "step": "queued", "done": 0, "total": None})
        entry.setdefault("attempt", max(1, job["attempt"] - int(bool(previous_failure))))
    lock = asyncio.Lock()

    async def persist():
        done = sum(entry["done"] for entry in progress.values())
        totals = [entry["total"] for entry in progress.values()]
        await engine._save(job, root, "parallel_rebuild", done, sum(totals) if None not in totals else None)

    async def run(name):
        entry = progress[name]
        worker = engine.snapshot()

        async def save(_job, checkpoint, step, done=0, total=None):
            async with lock:
                lanes[name] = deepcopy(checkpoint)
                if done > entry["done"]:
                    entry["consecutive_failures"] = 0
                entry.update(state="running", step=step, done=done, total=total)
                await persist()

        worker._save = save
        if entry["state"] == "retry_wait":
            async with lock:
                entry.update(attempt=entry["attempt"] + 1, state="running")
                entry.pop("available_at", None)
                entry.pop("error_code", None)
                await persist()
        try:
            result = await methods[name](worker, {**job, "checkpoint": deepcopy(lanes[name])})
        except LeaseLost:
            raise
        except Exception as exc:
            temporary, code = classify_job_error(exc)
            async with lock:
                if temporary and code == "model_queue_timeout":
                    # A request still waiting for a local slot never reached the provider.
                    entry["attempt"] -= 1
                    delay = engine.jobs.retry_delay(1)
                else:
                    network_failure = isinstance(exc, ModelNetworkError)
                    point = [entry["step"], entry["done"], "network" if network_failure else code]
                    consecutive = entry.get("consecutive_failures", entry["attempt"] - 1) + 1
                    if entry.get("retry_progress", point) != point:
                        consecutive = 1
                    entry.update(retry_progress=point, consecutive_failures=consecutive)
                    delay = engine.jobs.next_retry_delay(consecutive, job["max_attempts"],
                        retryable=temporary, model_network=network_failure)
                if delay is None:
                    entry.update(state="failed", error_code=code, retry_exhausted=temporary)
                else:
                    entry.update(state="retry_wait", error_code=code, available_at=engine.jobs.clock() + delay)
                await persist()
            return None
        async with lock:
            lanes[name] = {}
            done = entry["total"] or entry["done"]
            entry.update(state="succeeded", step="complete", done=done, total=done)
            await persist()
        return result

    active = {}
    results = {}
    try:
        while True:
            now = engine.jobs.clock()
            for name, entry in progress.items():
                if name in active or entry["state"] in {"succeeded", "failed"}:
                    continue
                if entry.get("available_at", 0) <= now:
                    active[name] = asyncio.create_task(run(name))
            waits = [entry["available_at"] for name, entry in progress.items()
                     if name not in active and entry["state"] == "retry_wait"]
            if not active:
                if waits:
                    await engine.jobs.defer_until(job["job_id"], job["lease_token"], min(waits))
                    raise JobDeferred()
                break
            timeout = max(0, min(waits) - now) if waits else None
            finished, _ = await asyncio.wait(active.values(), timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
            for name in list(active):
                if active[name] in finished:
                    results[name] = active.pop(name).result()
    finally:
        for task in active.values():
            task.cancel()
        await asyncio.gather(*active.values(), return_exceptions=True)

    failed = [name for name, entry in progress.items() if entry["state"] == "failed"]
    if failed:
        name = min(failed, key=lambda lane: bool(progress[lane].get("retry_exhausted")))
        root["failed_section"] = name
        await persist()
        raise ReviewRequired(progress[name]["error_code"])

    for name, method in methods.items():
        if results.get(name) is None:
            # A published section from an earlier lease has its own durable receipt.
            results[name] = await method(engine.snapshot(), {**job, "checkpoint": {}})
    return {**results["context"], "topics": results["topics"], "section_progress": progress}
