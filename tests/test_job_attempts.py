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
async def test_successful_window_gets_its_own_network_retry_budget(stores):
    jobs, _, clock = stores
    j = await submit(jobs, max_attempts=2)
    for done, delay in [(0, 5), (0, 600), (1, 5)]:
        claimed = await jobs.claim("worker")
        await jobs.save_checkpoint(j["job_id"], claimed["lease_token"], checkpoint={"done":done},
                                   step="model", progress_done=done, progress_total=2)
        failed = await jobs.fail(j["job_id"], claimed["lease_token"], error_code="provider_temporary_failure",
                                 error_type="ModelNetworkError", retryable=True, model_network=True)
        assert failed["available_at"] == clock[0] + delay
        clock[0] += delay
