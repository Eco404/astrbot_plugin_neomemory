"""Provider probes persist no model text and share production concurrency limits."""

import asyncio
import json
from types import SimpleNamespace

import pytest
import pytest_asyncio

from adapters.providers import ProviderAdapter
from core.config import NeoMemoryConfig
from core.jobs import JobRunner, ReviewRequired
from core.model_tests import ModelTestService
from storage.job_store import JobConflict, JobStore
from storage.memory_store import MemoryStore


class Provider:
    def __init__(self, name):
        self.provider_config = {"id": name, "api_key": "never-persist-this-key"}
        self.model = "test-model"
        self.calls = []

    def get_model(self):
        return self.model

    def get_dim(self):
        return 3

    async def get_embeddings(self, texts):
        self.calls.append(texts)
        return [[1, 2, 3] for _ in texts]


@pytest_asyncio.fixture
async def env(tmp_path):
    memory = MemoryStore(tmp_path / "memory.db")
    await memory.open()
    llm, llm2, embedding = Provider("llm"), Provider("llm2"), Provider("emb")
    host = SimpleNamespace(get_provider_by_id=lambda name: {"llm": llm, "llm2": llm2, "emb": embedding}.get(name),
        get_all_providers=lambda: [llm, llm2], get_all_embedding_providers=lambda: [embedding])
    calls = []
    async def generate(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(completion_text="do-not-persist-response")
    host.llm_generate = generate
    adapter = ProviderAdapter(host, llm_concurrency=1, embedding_concurrency=1)
    plugin = SimpleNamespace(ready=True, terminating=False, jobs=JobStore(memory.database),
        config=NeoMemoryConfig(), runtime=SimpleNamespace(adapter=adapter))
    probe = ModelTestService(plugin)
    value = SimpleNamespace(memory=memory, jobs=plugin.jobs, host=host, llm=llm, embedding=embedding,
        calls=calls, service=probe, plugin=plugin, adapter=adapter, runner=JobRunner(plugin.jobs, {"model_test": probe.run}))
    try:
        yield value
    finally:
        await value.runner.stop()
        await memory.close()


async def enqueue(env, **kwargs):
    return await env.service.enqueue(**{"kind": "llm", "provider_id": "llm", "idempotency_key": "one", **kwargs})


@pytest.mark.asyncio
async def test_double_click_is_idempotent_and_provider_scope_blocks_duplicate_tests(env):
    first, second = await asyncio.gather(enqueue(env), enqueue(env))
    assert first["job_id"] == second["job_id"]
    assert first["max_attempts"] == 1 and first["scope_key"].startswith("provider_test:")
    with pytest.raises(JobConflict):
        await enqueue(env, idempotency_key="different-click")
    other = await enqueue(env, kind="embedding", provider_id="emb", idempotency_key="embedding-click")
    assert other["job_id"] != first["job_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,provider_id", [("llm", "llm"), ("embedding", "emb")])
async def test_results_have_metadata_only_and_completed_checkpoint_avoids_repeat_call(env, kind, provider_id):
    queued = await enqueue(env, kind=kind, provider_id=provider_id)
    job = await env.jobs.claim("worker")
    result = await env.service.run(job)
    checkpointed = await env.jobs.get(queued["job_id"])
    assert await env.service.run(checkpointed) == result
    assert result["ok"] is True and result["model"] == "test-model" and result["elapsed_ms"] >= 0
    if kind == "embedding":
        assert result["dimension"] == 3 and len(env.embedding.calls) == 1
    else:
        assert len(env.calls) == 1 and env.calls[0]["contexts"] == [{"role": "user", "content": "Reply OK."}]
    assert "never-persist-this-key" not in json.dumps(checkpointed)
    assert "do-not-persist-response" not in json.dumps(checkpointed)
    await env.jobs.finish(job["job_id"], job["lease_token"], result=result)
    assert (await env.jobs.get(job["job_id"]))["state"] == "succeeded"


@pytest.mark.asyncio
async def test_failure_has_safe_code_and_no_automatic_retry(env):
    async def fail(**kwargs):
        raise OSError("https://secret.example?key=do-not-expose")
    env.host.llm_generate = fail
    queued = await enqueue(env)
    await env.runner._execute(await env.jobs.claim("worker"))
    stored = await env.jobs.get(queued["job_id"])
    assert stored["state"] == "failed" and stored["attempt"] == 1
    assert stored["error_code"] == "model_test_provider_failed"
    assert "do-not-expose" not in json.dumps(stored)
    retried = await env.service.retry(stored["job_id"])
    assert retried["state"] == "queued"


@pytest.mark.asyncio
async def test_provider_change_requires_fresh_test(env):
    queued = await enqueue(env)
    env.llm.model = "changed"
    await env.runner._execute(await env.jobs.claim("worker"))
    stored = await env.jobs.get(queued["job_id"])
    assert stored["error_code"] == "model_test_configuration_changed" and not env.calls
    replacement = await env.service.retry(stored["job_id"], fresh=True)
    assert replacement["job_id"] != stored["job_id"]
    await env.runner._execute(await env.jobs.claim("worker"))
    assert (await env.jobs.get(replacement["job_id"]))["result"]["model"] == "changed"


@pytest.mark.asyncio
async def test_probe_waits_for_shared_adapter_budget_and_keeps_database_unlocked(env):
    entered, release = asyncio.Event(), asyncio.Event()
    async def generate(**kwargs):
        env.calls.append(kwargs)
        entered.set()
        await release.wait()
        return SimpleNamespace(completion_text="OK")
    env.host.llm_generate = generate
    chat = asyncio.create_task(env.adapter.generate([{"role": "user", "content": "existing background task"}],
                                                    provider_id="llm", timeout_seconds=5))
    await entered.wait()
    await enqueue(env)
    probe = asyncio.create_task(env.service.run(await env.jobs.claim("worker")))
    await asyncio.sleep(0.03)
    assert len(env.calls) == 1 and not probe.done()
    # No SQL transaction surrounds the network call or semaphore queue.
    await asyncio.wait_for(env.jobs.submit("other", "unrelated-space", {}, "other-job"), 0.2)
    release.set()
    await chat
    assert (await probe)["ok"] and len(env.calls) == 2


@pytest.mark.asyncio
async def test_timeout_and_cancellation_propagate_without_duplicate_requests(env):
    async def hang(**kwargs):
        await asyncio.sleep(10)
    env.host.llm_generate = hang
    env.service.config = SimpleNamespace(extraction_timeout_seconds=0.02)
    await enqueue(env)
    job = await env.jobs.claim("worker")
    with pytest.raises(ReviewRequired) as error:
        await env.service.run(job)
    assert error.value.code == "model_test_timeout"
    env.service.config = SimpleNamespace(extraction_timeout_seconds=10)
    task = asyncio.create_task(env.service.run(job))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    stored = await env.jobs.get(job["job_id"])
    assert stored["checkpoint"] == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [{"kind": "unknown"}, {"kind": "embedding", "provider_id": "llm"},
    {"provider_id": "missing"}, {"provider_id": " "}, {"idempotency_key": ""}, {"idempotency_key": "a" * 201}])
async def test_invalid_capabilities_do_not_admit_tasks(env, change):
    with pytest.raises(ValueError):
        await enqueue(env, **change)
    assert await env.jobs.list() == []


@pytest.mark.asyncio
@pytest.mark.parametrize("completion", ["", "   ", None])
async def test_empty_llm_response_is_a_failed_probe(env, completion):
    async def empty(**kwargs):
        return SimpleNamespace(completion_text=completion)
    env.host.llm_generate = empty
    queued = await enqueue(env)
    await env.runner._execute(await env.jobs.claim("worker"))
    stored = await env.jobs.get(queued["job_id"])
    assert stored["state"] == "failed" and stored["result"] is None
    assert stored["error_code"] == "model_test_provider_failed"


@pytest.mark.asyncio
async def test_probe_reports_safe_http_status_without_creating_a_task(env):
    async def denied(**kwargs):
        error = RuntimeError("synthetic private token")
        error.status_code = 403
        raise error

    env.host.llm_generate = denied
    with pytest.raises(ReviewRequired) as failure:
        await env.service.probe(kind="llm", provider_id="llm")
    assert failure.value.code == "model_test_provider_failed"
    assert failure.value.http_status == 403 and failure.value.failure_stage == "provider_call"
    assert "private" not in str(failure.value)
    assert await env.jobs.list() == []
