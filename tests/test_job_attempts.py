"""Recovery decisions use the stage that actually failed."""
import asyncio
import sqlite3

import pytest

from core.attempts import observed_call
from core.jobs import JobRunner, classify_job_error, deadline_failure
from tests.test_jobs import stores, submit


@pytest.mark.asyncio
@pytest.mark.parametrize("started", [True, False])
async def test_shared_deadline_distinguishes_model_wait_from_call(stores, started):
    jobs, reopened, clock = stores
    await submit(jobs, max_attempts=1)

    async def never_returns(*, diagnostics):
        diagnostics["call_started"] = started
        if not started:
            diagnostics["failure_stage"] = "provider_queue"
        await asyncio.Event().wait()

    async def run(job):
        try:
            async with asyncio.timeout(.01):
                await observed_call(never_returns)
        except TimeoutError as exc:
            raise deadline_failure(exc, "extraction_timeout") from None

    runner = JobRunner(jobs, {"extract": run})
    job = await jobs.claim("worker")
    await runner._execute(job)
    failed = await reopened.get(job["job_id"])
    attempt, = failed["attempts"]
    assert attempt["model_calls"] == int(started)
    assert attempt["commits"] == 0
    if started:
        assert failed["state"] == "retry_wait" and failed["available_at"] == clock[0] + 600
    else:
        assert failed["state"] == "retry_wait" and failed["attempt"] == 0
        assert failed["error_code"] == "model_queue_timeout"
        assert attempt["queued_without_call"]


def test_busy_database_classified_using_sqlite_status(tmp_path):
    with sqlite3.connect(tmp_path/"busy.db") as owner, sqlite3.connect(tmp_path/"busy.db", timeout=0) as other:
        owner.execute("CREATE TABLE pending(id)")
        owner.execute("INSERT INTO pending VALUES(1)")
        with pytest.raises(sqlite3.OperationalError) as exc:
            other.execute("INSERT INTO pending VALUES(2)")
        assert classify_job_error(exc.value) == (True, "storage_busy")


@pytest.mark.asyncio
async def test_health_tracks_each_retained_automatic_recovery_chain_once(stores):
    jobs, reopened, clock = stores
    async def fail_once(key, automatic=True):
        await submit(jobs, scope=key, key=key, hide_on_success=automatic)
        job = await jobs.claim("worker")
        return await jobs.fail(job["job_id"], job["lease_token"], error_type="ReviewRequired", error_code="invalid_output")

    async def stats():
        return (await reopened.health())["automatic_recovery"]

    await fail_once("manual-origin", False)
    pending = await fail_once("auto-pending")
    manual = await fail_once("manual-retry")
    await jobs.retry(manual["job_id"])
    waiting = await jobs.claim("worker")
    await jobs.finish(waiting["job_id"], waiting["lease_token"])
    original = await fail_once("auto-fresh")
    assert await stats() == {"recovered":0, "manual":1, "pending":2, "total":3}
    await jobs.cancel(pending["job_id"])
    for cycle in range(2):
        clock[0] += 21600
        resume = await jobs.claim("worker")
        assert resume["job_id"] == original["job_id"]
        # The first failed resume enters immediate fresh recovery.
        await jobs.fail(resume["job_id"], resume["lease_token"], error_type="ReviewRequired", error_code="invalid_output")
        fresh = await jobs.claim("worker")
        replacement = await submit(jobs, scope="auto-fresh", key="replace-"+fresh["job_id"], replaces=fresh)
        await jobs.delete(fresh["job_id"])
        claim = await jobs.claim("worker")
        assert claim["job_id"] == replacement["job_id"]
        if cycle == 0:
            original = await jobs.fail(claim["job_id"], claim["lease_token"], error_type="ReviewRequired", error_code="invalid_output")
        else:
            await jobs.finish(claim["job_id"], claim["lease_token"])
    assert await stats() == {"recovered":1, "manual":2, "pending":0, "total":3}
    await jobs.delete(replacement["job_id"])
    assert await stats() == {"recovered":0, "manual":2, "pending":0, "total":2}


@pytest.mark.asyncio
async def test_successful_window_gets_its_own_network_retry_budget(stores):
    jobs, _, clock = stores
    j = await submit(jobs, max_attempts=2)
    for done, delay in [(0, 5), (0, 600), (1, 5)]:
        claimed = await jobs.claim("worker")
        await jobs.save_checkpoint(j["job_id"], claimed["lease_token"], checkpoint={"done":done},
                                   step="model", progress_done=done, progress_total=2)
        failed = await jobs.fail(j["job_id"], claimed["lease_token"], error_code="provider_temporary_failure",
                                 error_type="ModelNetworkError", retryable=True, model_network=True)
        health = await jobs.health()
        assert health["longest_failure_streak"]["reason"] == "network"
        assert health["longest_failure_streak"]["streak"] == (2 if delay == 600 else 1)
        assert health["automatic_recovery"]["total"] == 0
        assert failed["available_at"] == clock[0] + delay
        clock[0] += delay


@pytest.mark.asyncio
async def test_local_queue_delays_preserve_last_network_streak_after_restart(stores):
    jobs, reopened, clock = stores
    await submit(jobs, max_attempts=1)
    for network in [True, *([False] * 15), True, True, True]:
        worker = await reopened.claim("worker")
        code = "provider_temporary_failure" if network else "model_queue_timeout"
        result = await jobs.fail(worker["job_id"], worker["lease_token"], error_code=code,
                                 error_type="RetryableJobError", retryable=True, model_network=network)
        if result["state"] == "retry_wait":
            assert result["available_at"] == clock[0] + (600 if network else 5)
            clock[0] = result["available_at"]
    assert result["state"] == "failed" and result["attempt"] == 4
    assert result["attempts"][-1]["streak"] == 4
    assert len(result["attempts"]) == 12


@pytest.mark.asyncio
async def test_success_clears_current_error_and_retains_safe_http_diagnosis(stores):
    from core.attempts import observe_commit
    from core.jobs import provider_failure

    jobs, reopened, clock = stores
    job = await submit(jobs)

    async def model(*, diagnostics):
        diagnostics.update(call_started=True)
        if clock[0] == 100:
            error = RuntimeError("synthetic private content")
            error.status_code = 503
            raise error

    async def run(job):
        try:
            await observed_call(model)
        except RuntimeError as exc:
            raise provider_failure(exc) from None
        observe_commit()
        return {"ok": True}

    runner = JobRunner(jobs, {"extract": run})
    await runner._execute(await jobs.claim("first"))
    waiting = await reopened.get(job["job_id"])
    clock[0] = waiting["available_at"]
    await runner._execute(await jobs.claim("second"))
    complete = await reopened.get(job["job_id"])
    assert complete["state"] == "succeeded" and complete["error_code"] is None
    first, second = complete["attempts"]
    assert (first["http_status"], first["model_calls"], first["commits"]) == (503, 1, 0)
    assert (second["model_calls"], second["commits"], second["remaining"]) == (1, 1, 0)
    assert "synthetic private" not in repr(complete)
