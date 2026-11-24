"""Persistent task admission, fencing, cancellation and restart guarantees."""

import asyncio

import pytest
import pytest_asyncio

from core.jobs import JobRunner, RetryableJobError, ReviewRequired
from storage.memory_schema import initialize_memory_schema
from storage.job_store import JobConflict, JobStore, LeaseLost, assert_job_lease
from storage.sqlite_executor import SQLiteExecutor


@pytest_asyncio.fixture
async def stores(tmp_path):
    first = SQLiteExecutor(tmp_path / "memory.db")
    second = SQLiteExecutor(tmp_path / "memory.db")
    await first.open()
    await initialize_memory_schema(first)
    await second.open()
    clock = [100.0]
    yield JobStore(first, clock=lambda: clock[0]), JobStore(second, clock=lambda: clock[0]), clock
    await second.close()
    await first.close()


async def submit(store, scope="space-a", *, key="key-a", payload=None, kind="extract", **kwargs):
    return await store.submit(kind, scope, payload or {"source_ids": ["a", "b"]}, key, **kwargs)


@pytest.mark.asyncio
async def test_submit_idempotency_includes_finished_tasks(stores):
    first, second, _ = stores
    job = await submit(first)
    same = await submit(second)
    assert same["job_id"] == job["job_id"]
    claimed = await first.claim("one")
    await first.finish(job["job_id"], claimed["lease_token"], result={"nodes": 2})
    final = await submit(second)
    assert final["state"] == "succeeded"
    assert final["result"] == {"nodes": 2}
    assert len(await first.list()) == 1
    for kwargs in ({"payload": {"source_ids": ["different"]}}, {"kind": "rebuild"}, {"scope": "b"}):
        with pytest.raises(JobConflict):
            await submit(second, **kwargs)


@pytest.mark.asyncio
async def test_concurrent_admission_serializes_across_connections(stores):
    first, second, _ = stores
    results = await asyncio.gather(submit(first, key="one"), submit(second, key="two"), return_exceptions=True)
    assert sum(isinstance(result, dict) for result in results) == 1
    assert sum(isinstance(result, JobConflict) for result in results) == 1
    other = await submit(second, scope="space-b", key="three")
    assert other["state"] == "queued"
    availability = await first.availability("*")
    assert availability["available"] is False
    assert len(availability["blocking_jobs"]) == 2
    assert (await first.availability("unused"))["available"] is True


@pytest.mark.asyncio
async def test_concurrent_identical_admission_has_one_identity(stores):
    first, second, _ = stores
    results = await asyncio.gather(submit(first), submit(second))
    assert results[0]["job_id"] == results[1]["job_id"]
    assert len(await first.list()) == 1


@pytest.mark.asyncio
async def test_global_scope_conflicts_in_both_directions(stores):
    first, second, _ = stores
    space = await submit(first)
    with pytest.raises(JobConflict):
        await submit(second, "*", key="global")
    await second.cancel(space["job_id"])
    global_job = await submit(first, "*", key="global")
    with pytest.raises(JobConflict):
        await submit(second, "other", key="other")
    claim = await first.claim("owner")
    async with first.database.transaction() as connection:
        valid = await assert_job_lease(connection, claim["job_id"], claim["lease_token"], scope_key="any", now=100)
    assert valid["job_id"] == global_job["job_id"]
    await first.cancel(global_job["job_id"])
    assert (await first.availability("other"))["available"] is True


@pytest.mark.asyncio
async def test_running_lease_is_exclusive_and_scope_checked(stores):
    first, second, clock = stores
    await submit(first)
    claims = await asyncio.gather(first.claim("first"), second.claim("second"))
    assert sum(claim is not None for claim in claims) == 1
    job = next(claim for claim in claims if claim is not None)
    assert job["attempt"] == 1
    async with first.database.transaction() as connection:
        await assert_job_lease(connection, job["job_id"], job["lease_token"], scope_key="space-a", now=clock[0])
        for token, scope in (("wrong", "space-a"), (job["lease_token"], "space-b"), (job["lease_token"], "*")):
            with pytest.raises(LeaseLost):
                await assert_job_lease(connection, job["job_id"], token, scope_key=scope, now=clock[0])


@pytest.mark.asyncio
async def test_expired_claim_recovers_checkpoint_and_fences_old_worker(stores):
    first, second, clock = stores
    await submit(first)
    old = await first.claim("old", lease_seconds=10)
    await first.save_checkpoint(old["job_id"], old["lease_token"], checkpoint={"cursor": 4},
                                step="extracting", progress_done=4, progress_total=9)
    clock[0] = 110
    with pytest.raises(LeaseLost):
        await first.heartbeat(old["job_id"], old["lease_token"])
    new = await second.claim("new", lease_seconds=20)
    assert new["job_id"] == old["job_id"]
    assert new["lease_token"] != old["lease_token"]
    assert new["checkpoint"] == {"cursor": 4}
    assert new["progress_done"] == 4
    assert new["attempt"] == 1  # A restart does not consume a model retry.
    for action in (
        first.finish(old["job_id"], old["lease_token"]),
        first.release(old["job_id"], old["lease_token"]),
        first.fail(old["job_id"], old["lease_token"], error_type="TimeoutError", error_code="network"),
        first.save_checkpoint(old["job_id"], old["lease_token"], checkpoint={}, step="old", progress_done=0, progress_total=0),
    ):
        with pytest.raises(LeaseLost):
            await action
    assert (await first.get(new["job_id"]))["owner"] == "new"


@pytest.mark.asyncio
async def test_heartbeat_extends_valid_lease_only(stores):
    first, second, clock = stores
    await submit(first)
    job = await first.claim("owner", lease_seconds=10)
    clock[0] = 105
    beat = await first.heartbeat(job["job_id"], job["lease_token"], lease_seconds=10)
    assert beat["lease_until"] == 115
    clock[0] = 110
    assert await second.claim("other") is None
    clock[0] = 115
    with pytest.raises(LeaseLost):
        await first.finish(job["job_id"], job["lease_token"])


@pytest.mark.asyncio
async def test_cancel_immediately_revokes_mutation_right(stores):
    first, second, _ = stores
    job = await submit(first)
    claim = await first.claim("owner")
    cancelled = await second.cancel(job["job_id"])
    assert cancelled["state"] == "cancelled"
    assert cancelled["lease_token"] is None
    with pytest.raises(LeaseLost):
        await first.finish(job["job_id"], claim["lease_token"])
    next_job = await submit(first, key="next")
    with pytest.raises(JobConflict):
        await second.retry(job["job_id"])
    await first.cancel(next_job["job_id"])
    retry = await second.retry(job["job_id"])
    assert retry["state"] == "queued"
    assert retry["attempt"] == 0


@pytest.mark.asyncio
async def test_retry_wait_backoff_cap_and_manual_retry(stores):
    first, second, clock = stores
    job = await submit(first)
    claim = await first.claim("owner")
    await first.save_checkpoint(job["job_id"], claim["lease_token"], checkpoint={"done": ["batch"]},
                                step="extracting", progress_done=1, progress_total=2)
    waiting = await first.fail(job["job_id"], claim["lease_token"], error_type="TimeoutError",
                               error_code="upstream_timeout", retryable=True)
    assert waiting["state"] == "retry_wait"
    assert waiting["available_at"] == 105
    assert waiting["error_type"] == "TimeoutError"
    assert waiting["error_code"] == "upstream_timeout"
    assert await second.claim("next") is None
    with pytest.raises(JobConflict):
        await submit(second, key="blocked")
    clock[0] = 105
    retried = await second.claim("next")
    assert retried["attempt"] == 2
    assert retried["checkpoint"] == {"done": ["batch"]}
    assert retried["error_type"] is None
    assert retried["error_code"] is None
    assert (await first.get(job["job_id"]))["error_code"] is None
    failed = await second.fail(job["job_id"], retried["lease_token"], error_type="TimeoutError",
                               error_code="upstream_timeout", retryable=True)
    assert failed["state"] == "failed"
    assert failed["error_type"] == "TimeoutError"
    assert failed["error_code"] == "upstream_timeout"
    assert (await first.availability("space-a"))["available"] is True
    manual = await first.retry(job["job_id"])
    assert manual["checkpoint"] == {"done": ["batch"]}
    assert manual["progress_done"] == 1
    assert manual["error_type"] is None
    assert manual["error_code"] is None
    assert (await first.retry(job["job_id"]))["job_id"] == job["job_id"]


@pytest.mark.asyncio
async def test_checkpoint_clears_stale_error_from_preexisting_running_task(stores):
    first, second, _ = stores
    await submit(first)
    running = await first.claim("worker")
    # Existing running tasks may have retained the previous attempt's diagnostic.
    await first.database.execute(
        "UPDATE memory_jobs SET error_type='TimeoutError',error_code='extraction_timeout' WHERE job_id=?",
        (running["job_id"],),
    )
    saved = await first.save_checkpoint(
        running["job_id"], running["lease_token"], checkpoint={"cursor": 1},
        step="extracting", progress_done=1, progress_total=2,
    )
    assert saved["state"] == "running"
    assert saved["error_type"] is None
    assert saved["error_code"] is None
    assert (await second.get(running["job_id"]))["error_code"] is None
    completed = await first.finish(running["job_id"], running["lease_token"])
    assert completed["state"] == "succeeded"
    assert completed["error_type"] is None
    assert completed["error_code"] is None


@pytest.mark.asyncio
async def test_retry_rechecks_global_admission(stores):
    first, second, _ = stores
    job = await submit(first)
    await first.cancel(job["job_id"])
    await submit(second, "*", key="global")
    with pytest.raises(JobConflict):
        await first.retry(job["job_id"])


@pytest.mark.asyncio
async def test_explicit_cancel_resolves_failed_job_without_erasing_diagnostics(stores):
    first, _, _ = stores
    await submit(first)
    job = await first.claim("owner")
    await first.save_checkpoint(job["job_id"], job["lease_token"], checkpoint={"window": {"source_ids": ["one"]}},
                                step="validate", progress_done=0, progress_total=1)
    await first.fail(job["job_id"], job["lease_token"], error_type="ReviewRequired", error_code="protected_candidate")
    cancelled = await first.cancel(job["job_id"])
    assert cancelled["state"] == "cancelled"
    assert cancelled["error_code"] == "protected_candidate"
    assert cancelled["checkpoint"] == {"window": {"source_ids": ["one"]}}
    assert await first.list(states=["failed"]) == []


@pytest.mark.asyncio
async def test_release_preserves_progress_but_discards_old_token(stores):
    first, second, _ = stores
    await submit(first)
    job = await first.claim("one")
    await first.save_checkpoint(job["job_id"], job["lease_token"], checkpoint={"at": 7},
                                step="building", progress_done=7, progress_total=None)
    released = await first.release(job["job_id"], job["lease_token"])
    assert released["state"] == "queued"
    assert released["checkpoint"] == {"at": 7}
    resumed = await second.claim("two")
    assert resumed["lease_token"] != job["lease_token"]
    assert resumed["attempt"] == 1
    completed = await second.finish(job["job_id"], resumed["lease_token"])
    assert completed["progress_done"] == 7
    assert completed["checkpoint"] == {}


@pytest.mark.asyncio
async def test_input_validation_and_safe_error_fields(stores):
    first, _, _ = stores
    for kwargs in ({"scope": ""}, {"key": ""}, {"max_attempts": 0}, {"payload": {"score": float("nan")}}):
        with pytest.raises(ValueError):
            await submit(first, **kwargs)
    await submit(first)
    job = await first.claim("owner")
    for kwargs in ({"error_type": "Error: credential", "error_code": "fail"},
                   {"error_type": "Error", "error_code": "https://secret"}):
        with pytest.raises(ValueError):
            await first.fail(job["job_id"], job["lease_token"], **kwargs)
    with pytest.raises(ValueError):
        await first.save_checkpoint(job["job_id"], job["lease_token"], checkpoint={},
                                    step="bad", progress_done=5, progress_total=4)
    with pytest.raises(ValueError):
        await first.claim("owner", lease_seconds=float("inf"))
    with pytest.raises(ValueError):
        await first.list(states="running")
    with pytest.raises(ValueError):
        await first.list(states=["unknown"])


@pytest.mark.asyncio
async def test_list_and_health_filters(stores):
    first, _, _ = stores
    first_job = await submit(first)
    second_job = await submit(first, "space-b", key="second")
    await first.cancel(first_job["job_id"])
    assert [row["job_id"] for row in await first.list(states=["queued"])] == [second_job["job_id"]]
    assert len(await first.list(scope_key="space-a")) == 1
    assert len(await first.list(limit=1)) == 1
    assert (await first.health())["counts"]["cancelled"] == 1
    assert await first.get("missing") is None


async def wait_state(store, job_id, state, timeout=2):
    async with asyncio.timeout(timeout):
        while True:
            job = await store.get(job_id)
            if job["state"] == state:
                return job
            await asyncio.sleep(0.005)


@pytest.mark.asyncio
async def test_runner_failure_isolation_and_bounded_concurrency(stores):
    first, _, clock = stores
    entered = 0
    maximum = 0
    release = asyncio.Event()

    async def handler(job):
        nonlocal entered, maximum
        if job["scope_key"] == "bad":
            raise ValueError("private payload that must not be persisted")
        entered += 1
        maximum = max(maximum, entered)
        try:
            await release.wait()
            return {"ok": True}
        finally:
            entered -= 1

    bad = await submit(first, "bad", key="bad")
    clock[0] += 1
    jobs = [await submit(first, f"good-{i}", key=f"good-{i}") for i in range(4)]
    runner = JobRunner(first, {"extract": handler}, concurrency=2, poll_seconds=0.005)
    try:
        await runner.start()
        await wait_state(first, bad["job_id"], "failed")
        async with asyncio.timeout(2):
            while entered != 2:
                await asyncio.sleep(0.005)
        assert maximum == 2
        release.set()
        for job in jobs:
            await wait_state(first, job["job_id"], "succeeded")
        failed = await first.get(bad["job_id"])
        assert failed["error_type"] == "ValueError"
        assert failed["error_code"] == "review_required"
        assert "private payload" not in str(failed)
        assert failed["attempt"] == 1
    finally:
        await runner.stop()
    assert runner.running is False


@pytest.mark.asyncio
@pytest.mark.parametrize("failure,expected_state", [
    (TimeoutError("secret"), "retry_wait"),
    (ConnectionError("secret"), "retry_wait"),
    (RetryableJobError("provider_busy"), "retry_wait"),
    (PermissionError("secret"), "failed"),
    (ReviewRequired("secret response body"), "failed"),
])
async def test_runner_classifies_errors_without_immediate_retry(stores, failure, expected_state):
    first, _, _ = stores
    calls = 0

    async def handler(job):
        nonlocal calls
        calls += 1
        raise failure

    job = await submit(first)
    runner = JobRunner(first, {"extract": handler}, poll_seconds=0.005)
    try:
        await runner.start()
        result = await wait_state(first, job["job_id"], expected_state)
        assert calls == 1
        assert "secret" not in str(result)
        if isinstance(failure, RetryableJobError):
            assert result["error_code"] == "provider_busy"
    finally:
        await runner.stop()


@pytest.mark.asyncio
async def test_runner_graceful_shutdown_resumes_checkpoint(stores):
    first, second, _ = stores
    saved = asyncio.Event()
    cancelled = asyncio.Event()

    async def interrupted(job):
        await first.save_checkpoint(job["job_id"], job["lease_token"], checkpoint={"next": 2},
                                    step="extracting", progress_done=2, progress_total=5)
        saved.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    job = await submit(first)
    one = JobRunner(first, {"extract": interrupted}, poll_seconds=0.005)
    await one.start()
    await asyncio.wait_for(saved.wait(), 2)
    await one.stop()
    assert cancelled.is_set()
    assert (await first.get(job["job_id"]))["state"] == "queued"
    observed = []

    async def resume(job):
        observed.append(job["checkpoint"])
        return {"resumed": True}

    two = JobRunner(second, {"extract": resume}, poll_seconds=0.005)
    try:
        await two.start()
        result = await wait_state(second, job["job_id"], "succeeded")
        assert observed == [{"next": 2}]
        assert result["progress_done"] == 5
        assert result["result"] == {"resumed": True}
    finally:
        await two.stop()


@pytest.mark.asyncio
async def test_runner_cancellation_stops_handler_and_does_not_requeue(stores):
    first, second, _ = stores
    entered = asyncio.Event()
    exited = asyncio.Event()

    async def handler(job):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            exited.set()

    job = await submit(first)
    runner = JobRunner(first, {"extract": handler}, lease_seconds=0.06, poll_seconds=0.005)
    try:
        await runner.start()
        await asyncio.wait_for(entered.wait(), 2)
        await second.cancel(job["job_id"])
        await asyncio.wait_for(exited.wait(), 2)
        assert (await first.get(job["job_id"]))["state"] == "cancelled"
    finally:
        await runner.stop()


@pytest.mark.asyncio
async def test_unknown_kind_requires_review(stores):
    first, _, _ = stores
    job = await submit(first, kind="unsupported")
    runner = JobRunner(first, {}, poll_seconds=0.005)
    try:
        await runner.start()
        result = await wait_state(first, job["job_id"], "failed")
        assert result["error_type"] == "ReviewRequired"
        assert result["error_code"] == "unsupported_job_kind"
    finally:
        await runner.stop()


@pytest.mark.asyncio
async def test_network_uses_three_spaced_attempts_after_normal_budget(stores):
    from core.jobs import ModelNetworkError

    store, reopened, clock = stores
    await submit(store, max_attempts=2)

    async def offline(job):
        raise ModelNetworkError("provider_temporary_failure")

    runner = JobRunner(store, {"extract": offline})
    for attempt, delay in enumerate([5, 600, 600, 600, None], 1):
        job = await reopened.claim("owner")
        assert job["attempt"] == attempt
        await runner._execute(job)
        failed = await reopened.get(job["job_id"])
        if delay is not None:
            assert failed["state"] == "retry_wait" and failed["available_at"] == clock[0] + delay
            clock[0] += delay - 1
            assert await store.claim("early") is None
            clock[0] += 1
    assert failed["state"] == "failed"
    clock[0] += 21600
    assert await reopened.claim("manual-jobs-stay-failed") is None


@pytest.mark.asyncio
async def test_periodic_resume_obeys_scope_cancel_and_intervened_visibility(stores):
    store, reopened, clock = stores
    job = await submit(store, hide_on_success=True)
    failure = dict(error_type="ReviewRequired", error_code="context_changed")

    async def fail_after_claim():
        running = await store.claim("owner")
        return await store.fail(job["job_id"], running["lease_token"], **failure)

    await fail_after_claim()
    await store.retry(job["job_id"])
    failed = await fail_after_claim()
    assert failed["automatic_origin"] and not failed["hide_on_success"]
    clock[0] += 21599
    assert await reopened.claim("early") is None
    global_job = await submit(store, "*", key="global")
    clock[0] += 1
    assert (await reopened.claim("blocker"))["job_id"] == global_job["job_id"]
    assert await store.claim("busy") is None
    await reopened.cancel(global_job["job_id"])
    resumed = await reopened.claim("resume")
    assert resumed["recovery_mode"] == "resume" and resumed["attempt"] == 1
    await store.save_checkpoint(job["job_id"], resumed["lease_token"], checkpoint={"done": 1},
                                step="replay", progress_done=1, progress_total=2)
    failed = await store.fail(job["job_id"], resumed["lease_token"], **failure)
    assert failed["state"] == "failed" and failed["available_at"] == clock[0] + 21600
    await store.cancel(job["job_id"])
    clock[0] += 21600
    assert await reopened.claim("cancelled") is None


@pytest.mark.asyncio
async def test_recall_cannot_resubmit_failed_automatic_index_during_cooldown(stores):
    store, _, clock = stores
    options = {"kind": "reindex", "hide_on_success": True, "payload": {"expected_generation_id": "g"}}
    job = await submit(store, **options)
    running = await store.claim("indexer")
    await store.fail(job["job_id"], running["lease_token"], error_type="ReviewRequired", error_code="context_changed")
    same = await submit(store, key="another-recall", **options)
    assert same["job_id"] == job["job_id"] and same["available_at"] == clock[0] + 21600
    assert await store.claim("early") is None
