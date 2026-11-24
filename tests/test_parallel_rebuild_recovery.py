"""Manual candidate repair in a combined rebuild targets the failed branch."""

import asyncio
import json

import pytest

from tests.test_maintenance_engine import environment, enqueue_and_claim, execute, restart
from tests.test_topic_maintenance import seed


@pytest.mark.asyncio
@pytest.mark.parametrize("rejected", ["topics", "context"])
async def test_regenerate_only_failed_branch_after_restart(environment, rejected):
    env = environment
    await seed(env)
    calls = {"topics": 0, "context": 0}

    async def generate(messages, **options):
        data = json.loads(messages[1]["content"])
        name = "topics" if "nodes" in data else "context"
        calls[name] += 1
        if rejected == name and calls[name] == 1:
            return "not valid json"
        if name == "topics":
            return json.dumps({"schema_version": data["schema_version"], "changes": []})
        return json.dumps({"profile_changes": []})

    env.adapter.generate = generate
    failed = await execute(env, await enqueue_and_claim(env, "profile_rebuild", include_topics=True))
    assert failed["state"] == "failed", failed
    other = "context" if rejected == "topics" else "topics"
    progress = failed["checkpoint"]["section_progress"]
    assert progress[other]["state"] == "succeeded" and progress[rejected]["state"] == "failed"
    assert failed["checkpoint"]["failed_section"] == rejected
    assert (await env.engine.describe(failed["job_id"]))["regenerate_window_available"]

    restart(env)
    await env.engine.retry(failed["job_id"], regenerate_window=True)
    result = await execute(env, await env.engine.jobs.claim("restart", lease_seconds=60))
    assert result["state"] == "succeeded", result
    assert calls == {other: 1, rejected: 2}
    assert {p["state"] for p in result["result"]["section_progress"].values()} == {"succeeded"}


async def wait_for_lane(env, job_id, lane, state):
    async with asyncio.timeout(6):
        while True:
            job = await env.engine.jobs.get(job_id)
            progress = job["checkpoint"].get("section_progress", {}).get(lane, {})
            if progress.get("state") == state:
                return progress
            await asyncio.sleep(.005)


@pytest.mark.asyncio
@pytest.mark.parametrize("restart_during_wait", [False, True])
async def test_topic_resumes_same_window_while_context_is_still_running(environment, monkeypatch, restart_during_wait):
    env = environment
    await seed(env)
    release_context = asyncio.Event()
    topic_requests = []
    topic_times = []
    monkeypatch.setattr(env.engine.jobs, "retry_delay", lambda attempt: 0.5)

    async def generate(messages, **options):
        data = json.loads(messages[1]["content"])
        if "nodes" in data:
            topic_times.append(env.engine.jobs.clock())
            topic_requests.append(messages)
            if len(topic_requests) == 1:
                raise TimeoutError()
            return json.dumps({"schema_version": data["schema_version"], "changes": []})
        await release_context.wait()
        return json.dumps({"profile_changes": []})

    env.adapter.generate = generate
    job = await enqueue_and_claim(env, "profile_rebuild", include_topics=True)
    job_id = job["job_id"]
    task = asyncio.create_task(execute(env, job))
    try:
        waiting = await wait_for_lane(env, job_id, "topics", "retry_wait")
        assert waiting["attempt"] == 1 and waiting["error_code"] == "provider_temporary_failure"
        if restart_during_wait:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            restart(env)
            task = asyncio.create_task(execute(env, await env.engine.jobs.claim("restart", lease_seconds=60)))
        completed = await wait_for_lane(env, job_id, "topics", "succeeded")
        assert not task.done() and completed["attempt"] == 2
        assert "available_at" not in completed and "error_code" not in completed
        assert len(topic_requests) == 2 and topic_requests[0] == topic_requests[1]
        assert topic_times[1] >= waiting["available_at"]
    finally:
        release_context.set()
        result = await task
    assert result["state"] == "succeeded"


@pytest.mark.asyncio
async def test_exhausted_topic_budget_keeps_context_publication_and_allows_manual_retry(environment, monkeypatch):
    env = environment
    await seed(env)
    topic_attempts = 0
    context_calls = 0
    allow_topics = False
    monkeypatch.setattr("storage.job_store.NETWORK_RETRY_SECONDS", 0)
    monkeypatch.setattr(env.engine.jobs, "retry_delay", lambda attempt: 0)

    async def generate(messages, **options):
        nonlocal topic_attempts, context_calls
        data = json.loads(messages[1]["content"])
        if "nodes" in data:
            topic_attempts += 1
            if not allow_topics:
                raise ConnectionError()
            return json.dumps({"schema_version": data["schema_version"], "changes": []})
        context_calls += 1
        return json.dumps({"profile_changes": []})

    env.adapter.generate = generate
    job = await enqueue_and_claim(env, "profile_rebuild", include_topics=True)
    failed = await execute(env, job)
    assert failed["state"] == "failed" and failed["attempt"] == 1
    assert failed["error_code"] == "provider_temporary_failure"
    progress = failed["checkpoint"]["section_progress"]
    assert progress["context"]["state"] == "succeeded"
    assert progress["topics"]["attempt"] == topic_attempts == job["max_attempts"] + 3
    assert progress["topics"]["state"] == "failed"
    allow_topics = True
    await env.engine.retry(job["job_id"])
    result = await execute(env, await env.engine.jobs.claim("manual", lease_seconds=60))
    assert result["state"] == "succeeded" and context_calls == 1
    assert result["result"]["section_progress"]["topics"]["attempt"] == 1


@pytest.mark.asyncio
async def test_both_sections_can_use_their_full_retry_budget(environment, monkeypatch):
    env = environment
    await seed(env)
    calls = {"topics": 0, "context": 0}
    monkeypatch.setattr(env.engine.jobs, "retry_delay", lambda attempt: 0)

    async def generate(messages, **options):
        data = json.loads(messages[1]["content"])
        name = "topics" if "nodes" in data else "context"
        calls[name] += 1
        if calls[name] == 1:
            raise ConnectionError()
        if name == "topics":
            return json.dumps({"schema_version": data["schema_version"], "changes": []})
        return json.dumps({"profile_changes": []})

    env.adapter.generate = generate
    result = await execute(env, await enqueue_and_claim(env, "profile_rebuild", include_topics=True))
    assert result["state"] == "succeeded" and result["attempt"] == 1
    assert calls == {"topics": 2, "context": 2}
