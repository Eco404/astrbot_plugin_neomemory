"""End-to-end durable maintenance using real stores and a controlled Provider."""

import asyncio
import json
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest
import pytest_asyncio

from core.config import NeoMemoryConfig
from core.extraction_apply import ExtractionService
from core.extraction_references import ExtractionReferences
from core.maintenance import MaintenanceEngine
from core.memory_spaces import private_space
from storage.cache_store import CacheStore, text_hash
from storage.job_store import JobConflict
from storage.memory_store import MemoryStore
from storage.source_store import SourceStore
from tests.test_extraction_apply import capture
from tests.test_extraction_apply import node as extraction_node
from tests.test_extraction_apply import output
from tests.test_governance import operation


def node(source_id, *, title="合成事件", **kwargs):
    value = extraction_node(source_id, **kwargs)
    value["title"] = title
    value["emotion"] = None
    value["base_node_ids"] = []
    return value


class ControlledProvider:
    def __init__(self):
        self.calls = []
        self.output = json.dumps(output())
        self.on_generate = None
        self.model_revision = "first"
        self.reference_ids = {}

    def llm_fingerprint(self, provider_id, revision=""):
        return f"{provider_id}:{revision}:{self.model_revision}"

    async def generate(self, messages, *, provider_id, timeout_seconds, diagnostics=None, **kwargs):
        self.calls.append(messages)
        if self.on_generate is not None:
            await self.on_generate(messages)
        return candidate_references(self.output, self.reference_ids)


def candidate_references(raw, reference_ids):
    """Translate known fixture identifiers, leaving invalid candidates invalid."""
    try:
        candidate = json.loads(raw)
    except ValueError:
        return raw

    def translate(value):
        if isinstance(value, list):
            return [translate(item) for item in value]
        if not isinstance(value, dict):
            return value
        result = {}
        for key, item in value.items():
            if key in {"source_ids", "base_node_ids"} and isinstance(item, list):
                result[key] = [reference_ids.get(ref, ref) if isinstance(ref, str) else ref for ref in item]
            elif key in {"node_id", "logical_user_id", "from_ref", "to_ref", "event_ref", "atomic_ref"} and isinstance(item, str):
                result[key] = reference_ids.get(item, item)
            else:
                result[key] = translate(item)
        return result

    return json.dumps(translate(candidate))


def prompt_reference_ids(batch):
    canonical = json.loads(next(message["content"] for message in batch.messages() if message["role"] == "user"))
    wire = json.loads(next(message["content"] for message in ExtractionReferences(batch).messages() if message["role"] == "user"))
    mapping = {}

    def collect(original, encoded):
        if isinstance(original, list):
            for left, right in zip(original, encoded, strict=True):
                collect(left, right)
        elif isinstance(original, dict):
            for key, value in original.items():
                if key in {"source_id", "node_id", "logical_user_id", "actor_logical_user_id", "conversation_logical_user_id"} and isinstance(value, str):
                    mapping[value] = encoded[key]
                else:
                    collect(value, encoded[key])

    collect(canonical, wire)
    return mapping


@pytest_asyncio.fixture
async def environment(tmp_path, monkeypatch):
    sources = SourceStore(tmp_path / "sources.db")
    await sources.open()
    memory = MemoryStore(tmp_path / "memory.db", source_exists=sources.has_source)
    await memory.open()
    records = [await capture(sources, number=index) for index in (1, 2)]
    space = await memory.get_or_create_space(private_space("p", records[0].logical_user_id))
    config = NeoMemoryConfig(llm_provider_id="fake", extraction_auto_regeneration=False, private_min_messages=2,
                             private_topic_messages=3, private_max_messages=4,
                             private_idle_seconds=30, job_poll_seconds=0.005,
                             extraction_scan_seconds=0.01)
    adapter = ControlledProvider()
    prepare = ExtractionService.prepare

    async def prepare_for_provider(service, *args, **kwargs):
        batch = await prepare(service, *args, **kwargs)
        adapter.reference_ids = prompt_reference_ids(batch)
        return batch

    monkeypatch.setattr(ExtractionService, "prepare", prepare_for_provider)
    runtime = SimpleNamespace(adapter=adapter, recall=None, cache=None)
    persona = {"text": "温和且谨慎的陪伴者"}

    async def loader(persona_id):
        assert persona_id == "p"
        return persona["text"]

    engine = MaintenanceEngine(sources, memory, runtime, config, loader)
    env = SimpleNamespace(sources=sources, memory=memory, records=records, space=space,
                          config=config, adapter=adapter, runtime=runtime, persona=persona,
                          loader=loader, engine=engine)
    try:
        yield env
    finally:
        await env.engine.stop()
        await memory.close()
        await sources.close()


def restart(env, *, config=None):
    env.engine = MaintenanceEngine(env.sources, env.memory, env.runtime, config or env.config, env.loader)
    return env.engine


async def enqueue_and_claim(env, kind="extract", key="task", **kwargs):
    queued = await env.engine.enqueue(kind, env.space["space_id"], key, **kwargs)
    claimed = await env.engine.jobs.claim("test-worker", lease_seconds=60)
    assert claimed["job_id"] == queued["job_id"]
    return claimed


async def execute(env, job):
    await env.engine.runner._execute(job)
    return await env.engine.jobs.get(job["job_id"])


async def interrupt_after_raw_saved(env, monkeypatch):
    job = await enqueue_and_claim(env)

    async def interrupted(*args, **kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(env.engine.extraction, "apply", interrupted)
    with pytest.raises(asyncio.CancelledError):
        await env.engine.runner._execute(job)
    saved = await env.engine.jobs.get(job["job_id"])
    assert saved["state"] == "queued"
    assert saved["checkpoint"]["window"]["raw"] == env.adapter.output
    return job


@pytest.mark.asyncio
async def test_saved_provider_candidate_survives_worker_restart_without_second_call(environment, monkeypatch):
    env = environment
    job = await interrupt_after_raw_saved(env, monkeypatch)
    restart(env)
    resumed = await env.engine.jobs.claim("replacement")
    result = await execute(env, resumed)
    assert result["job_id"] == job["job_id"]
    assert result["state"] == "succeeded"
    assert len(env.adapter.calls) == 1
    assert result["checkpoint"] == {}
    generation = await env.memory.get_active_generation(env.space["space_id"])
    assert generation["activity_round"] == 1
    assert await env.engine.pipeline.consumed(generation["generation_id"], [r.source_id for r in env.records]) == {
        r.source_id for r in env.records
    }


@pytest.mark.asyncio
async def test_production_maintenance_rejects_unsupported_schema(environment):
    env = environment
    candidate = {**output(), "schema_version": "extraction.unsupported"}
    env.adapter.output = json.dumps(candidate)
    result = await execute(env, await enqueue_and_claim(env))
    assert result["state"] == "failed"
    assert result["error_code"] == "invalid_output"
    assert result["checkpoint"]["window"]["raw"] == env.adapter.output
    assert len(env.adapter.calls) == 1
    assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 0
    assert await env.memory.database.fetchall("SELECT * FROM extraction_batches") == []


@pytest.mark.asyncio
async def test_default_candidate_requires_explicit_basis_list_without_backfill(environment):
    env = environment
    candidate = output([node(env.records[0].source_id)])
    candidate['node_changes'][0].pop('base_node_ids')
    env.adapter.output = json.dumps(candidate)
    result = await execute(env, await enqueue_and_claim(env))
    assert result['state'] == 'failed' and result['error_code'] == 'invalid_output'
    assert any(error['type'] == 'missing' and error['path'][-1] == 'base_node_ids'
               for error in result['checkpoint']['window']['validation_errors'])
    assert await env.memory.database.fetchall('SELECT * FROM extraction_batches') == []
    assert not await env.memory.list_nodes(space_id=env.space['space_id'])


@pytest.mark.asyncio
async def test_committed_batch_before_checkpoint_recovers_without_extra_round(environment, monkeypatch):
    env = environment
    env.adapter.output = json.dumps(output([node(env.records[0].source_id)]))
    job = await enqueue_and_claim(env)
    original = env.engine.extraction.apply

    async def commit_then_interrupt(*args, **kwargs):
        await original(*args, **kwargs)
        raise asyncio.CancelledError()

    monkeypatch.setattr(env.engine.extraction, "apply", commit_then_interrupt)
    with pytest.raises(asyncio.CancelledError):
        await env.engine.runner._execute(job)
    assert len(await env.memory.list_nodes(space_id=env.space["space_id"])) == 1
    restart(env)
    resumed = await env.engine.jobs.claim("replacement")
    result = await execute(env, resumed)
    assert result["state"] == "succeeded"
    assert len(env.adapter.calls) == 1
    assert len(await env.memory.list_nodes(space_id=env.space["space_id"])) == 1
    assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["persona", "model", "policy"])
async def test_changed_execution_context_refuses_saved_candidate(environment, monkeypatch, changed):
    env = environment
    await interrupt_after_raw_saved(env, monkeypatch)
    config = env.config
    if changed == "persona":
        env.persona["text"] = "人格已发生变化"
    elif changed == "model":
        env.adapter.model_revision = "second"
    elif changed == "policy":
        config = replace(config, private_max_messages=5)
    restart(env, config=config)
    resumed = await env.engine.jobs.claim("replacement")
    failed = await execute(env, resumed)
    assert failed["state"] == "failed"
    assert failed["error_code"] == "configuration_changed"
    assert len(env.adapter.calls) == 1
    assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 0
    assert failed["checkpoint"]["window"]["raw"]


@pytest.mark.asyncio
async def test_persona_change_during_provider_call_prevents_commit(environment):
    env = environment

    async def change_persona(messages):
        env.persona["text"] = "调用期间切换人格"

    env.adapter.on_generate = change_persona
    failed = await execute(env, await enqueue_and_claim(env))
    assert failed["state"] == "failed"
    assert failed["error_code"] == "configuration_changed"
    assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 0


@pytest.mark.asyncio
async def test_protected_candidate_can_be_reviewed_and_explicitly_rejected(environment):
    env = environment
    protected = await env.memory.create_node(space_id=env.space["space_id"], node_type="event",
        content_text="用户手动确认的内容", source_ids=[env.records[0].source_id], manual_protected=True)
    env.adapter.output = json.dumps(output([node(env.records[0].source_id, old=protected, content="不应自动覆盖")]))
    failed = await execute(env, await enqueue_and_claim(env))
    assert failed["state"] == "failed"
    assert failed["error_code"] == "readonly_node"
    review = await env.engine.describe(failed["job_id"])
    assert review["available_at"] == failed["available_at"]
    assert review["error_code"] == "readonly_node"
    assert review["candidate"]["node_changes"][0]["content"] == "不应自动覆盖"
    assert review["source_ids"] == [r.source_id for r in env.records]
    rejection = await enqueue_and_claim(env, "reject", key="reject", failed_job_id=failed["job_id"])
    completed = await execute(env, rejection)
    assert completed["state"] == "succeeded"
    assert completed["result"]["review"] == "rejected"
    assert len(env.adapter.calls) == 1
    assert (await env.memory.get_node(protected["node_id"]))["content_text"] == "用户手动确认的内容"
    assert (await env.engine.jobs.get(failed["job_id"]))["state"] == "cancelled"
    generation = await env.memory.get_active_generation(env.space["space_id"])
    assert await env.engine.pipeline.consumed(generation["generation_id"], [r.source_id for r in env.records]) == {
        r.source_id for r in env.records
    }
    await capture(env.sources, number=3)
    await capture(env.sources, number=4)
    await env.engine.scan_once()
    queued, = await env.engine.jobs.list(states=["queued"])
    assert queued["hide_on_success"]


@pytest.mark.asyncio
async def test_rebuild_uses_frozen_sources_and_new_messages_remain_pending(environment):
    env = environment
    old_generation = await env.memory.get_active_generation(env.space["space_id"])
    await env.memory.create_node(space_id=env.space["space_id"], node_type="event", content_text="应被重建替代",
                                 source_ids=[env.records[0].source_id])
    job = await enqueue_and_claim(env, "rebuild")
    later = []
    assert await env.engine.pending_source_count(env.space) == 2

    async def capture_during_rebuild(messages):
        assert (await env.memory.get_active_generation(env.space["space_id"]))["generation_id"] == old_generation["generation_id"]
        if not later:
            later.extend([await capture(env.sources, number=3), await capture(env.sources, number=4)])
            assert await env.engine.pending_source_count(env.space) == 4

    env.adapter.on_generate = capture_during_rebuild
    completed = await execute(env, job)
    assert completed["state"] == "succeeded"
    assert completed["result"]["source_count"] == 2
    active = await env.memory.get_active_generation(env.space["space_id"])
    assert active["generation_id"] != old_generation["generation_id"]
    assert await env.memory.list_nodes(space_id=env.space["space_id"]) == []
    assert await env.engine.pipeline.consumed(active["generation_id"], [r.source_id for r in later]) == set()
    pending = await env.engine._pending(env.space, active["generation_id"])
    assert [r["source_id"] for r in pending] == [r.source_id for r in later]
    assert await env.engine.pending_source_count(env.space) == len(later)
    await env.engine.scan_once()
    queued = await env.engine.jobs.list(states=["queued"])
    assert len(queued) == 1
    assert queued[0]["payload"]["source_ids"] == [r.source_id for r in later]


@pytest.mark.asyncio
async def test_rebuild_removes_old_vectors_and_preserves_reusable_message_vectors(environment):
    env = environment
    old_generation = await env.memory.get_active_generation(env.space["space_id"])
    old = await env.memory.create_node(space_id=env.space["space_id"], node_type="event",
                                       content_text="旧记忆", source_ids=[env.records[0].source_id])
    cache = CacheStore(env.memory.path.parent / "cache.db")
    await cache.open()
    env.runtime.cache = cache
    try:
        hashes = [text_hash("旧记忆")]
        identities = [(old["node_id"], old["content_version"])]
        await cache.put_vectors("embedding", env.space["space_id"], hashes, [[1, 0]],
                                kind="node", identities=identities)
        await cache.put_vectors("embedding", env.space["space_id"], hashes, [[0, 1]], kind="message")
        env.engine._pending_reads[old_generation["generation_id"]] = "old pending"
        env.engine._read_hints[old_generation["generation_id"]] = 2
        env.adapter.output = json.dumps(output([node(env.records[0].source_id)]))
        completed = await execute(env, await enqueue_and_claim(env, "rebuild"))
        assert completed["state"] == "succeeded", completed
        assert await cache.get_vectors("embedding", env.space["space_id"], hashes,
                                       kind="node", identities=identities) == [None]
        assert await cache.get_vectors("embedding", env.space["space_id"], hashes, kind="message") == [[0, 1]]
        assert await env.memory.get_node(old["node_id"]) is None
        new_nodes = await env.memory.list_nodes(space_id=env.space["space_id"])
        assert len(new_nodes) == 1
        assert new_nodes[0]["content_version"] == 1
        assert old_generation["generation_id"] not in env.engine._pending_reads
        assert old_generation["generation_id"] not in env.engine._read_hints
    finally:
        env.runtime.cache = None
        await cache.close()


@pytest.mark.asyncio
async def test_rebuild_receipt_recovers_after_switch_without_repeating_provider(environment, monkeypatch):
    env = environment
    job = await enqueue_and_claim(env, "rebuild")
    original = env.engine.pipeline.activate_rebuild

    async def switch_then_interrupt(*args, **kwargs):
        await original(*args, **kwargs)
        raise asyncio.CancelledError()

    monkeypatch.setattr(env.engine.pipeline, "activate_rebuild", switch_then_interrupt)
    with pytest.raises(asyncio.CancelledError):
        await env.engine.runner._execute(job)
    active_before = await env.memory.get_active_generation(env.space["space_id"])
    calls = len(env.adapter.calls)
    restart(env)
    result = await execute(env, await env.engine.jobs.claim("replacement"))
    assert result["state"] == "succeeded"
    assert result["result"]["generation_id"] == active_before["generation_id"]
    assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == active_before["activity_round"]
    assert len(env.adapter.calls) == calls


@pytest.mark.asyncio
async def test_rebuild_committed_window_resumes_with_exact_progress(environment, monkeypatch):
    env = environment
    await capture(env.sources, number=3)
    await capture(env.sources, number=4)
    job = await enqueue_and_claim(env, "rebuild")
    original = env.engine.extraction.apply

    async def commit_first_window_then_interrupt(*args, **kwargs):
        await original(*args, **kwargs)
        raise asyncio.CancelledError()

    monkeypatch.setattr(env.engine.extraction, "apply", commit_first_window_then_interrupt)
    with pytest.raises(asyncio.CancelledError):
        await env.engine.runner._execute(job)
    saved = await env.engine.jobs.get(job["job_id"])
    assert saved["checkpoint"]["done"] == 0
    assert len(saved["checkpoint"]["window"]["source_ids"]) == 2
    restart(env)
    result = await execute(env, await env.engine.jobs.claim("replacement"))
    assert result["state"] == "succeeded"
    assert result["result"]["source_count"] == 4
    assert result["progress_done"] == result["progress_total"] == 4
    assert len(env.adapter.calls) == 2
    assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 2


@pytest.mark.asyncio
async def test_failed_old_generation_does_not_block_auto_extraction_after_rebuild(environment):
    env = environment
    env.adapter.output = "invalid model output"
    failed = await execute(env, await enqueue_and_claim(env))
    assert failed["state"] == "failed"
    env.adapter.output = json.dumps(output())
    rebuilt = await execute(env, await enqueue_and_claim(env, "rebuild", key="rebuild"))
    assert rebuilt["state"] == "succeeded"
    await capture(env.sources, number=3)
    await capture(env.sources, number=4)
    await env.engine.scan_once()
    queued = await env.engine.jobs.list(states=["queued"])
    assert len(queued) == 1
    assert queued[0]["payload"]["expected_generation_id"] == rebuilt["result"]["generation_id"]


@pytest.mark.asyncio
async def test_cancellation_during_provider_call_cannot_write_memory(environment):
    env = environment
    env.adapter.output = json.dumps(output([node(env.records[0].source_id)]))
    job = await enqueue_and_claim(env)

    async def cancel(messages):
        await env.engine.jobs.cancel(job["job_id"])

    env.adapter.on_generate = cancel
    result = await execute(env, job)
    assert result["state"] == "cancelled"
    assert await env.memory.list_nodes(space_id=env.space["space_id"]) == []
    assert (await env.memory.get_active_generation(env.space["space_id"]))["activity_round"] == 0


@pytest.mark.asyncio
async def test_oversized_source_has_explicit_rejection_path_without_provider_call(environment):
    env = environment
    env.config = replace(env.config, extraction_max_chars=2)
    restart(env)
    failed = await execute(env, await enqueue_and_claim(env))
    assert failed["state"] == "failed"
    assert failed["error_code"] == "oversized_source"
    assert env.adapter.calls == []
    rejection = await enqueue_and_claim(env, "reject", key="reject-long", failed_job_id=failed["job_id"])
    result = await execute(env, rejection)
    assert result["state"] == "succeeded"
    assert result["result"]["source_count"] == 1
    assert env.adapter.calls == []


@pytest.mark.asyncio
async def test_fresh_retry_rebuilds_candidate_and_resolves_old_failure(environment):
    env = environment
    env.adapter.output = "invalid"
    failed = await execute(env, await enqueue_and_claim(env))
    env.adapter.output = json.dumps(output())
    restarted = await env.engine.retry(failed["job_id"], fresh=True)
    assert restarted["job_id"] != failed["job_id"]
    assert (await env.engine.jobs.get(failed["job_id"]))["state"] == "cancelled"
    completed = await execute(env, await env.engine.jobs.claim("fresh"))
    assert completed["state"] == "succeeded"
    assert len(env.adapter.calls) == 2


@pytest.mark.asyncio
async def test_duplicate_enqueue_is_idempotent_before_and_after_completion(environment):
    env = environment
    job = await env.engine.enqueue("extract", env.space["space_id"], "same")
    assert (await env.engine.enqueue("extract", env.space["space_id"], "same"))["job_id"] == job["job_id"]
    completed = await execute(env, await env.engine.jobs.claim("worker"))
    assert completed["state"] == "succeeded"
    assert (await env.engine.enqueue("extract", env.space["space_id"], "same"))["state"] == "succeeded"
    with pytest.raises(JobConflict, match="different request"):
        await env.engine.enqueue("rebuild", env.space["space_id"], "same")


@pytest.mark.asyncio
async def test_provider_permission_error_requires_review_without_automatic_retry(environment):
    env = environment

    async def deny(messages):
        raise PermissionError("sensitive provider response")

    env.adapter.on_generate = deny
    failed = await execute(env, await enqueue_and_claim(env))
    assert failed["state"] == "failed"
    assert failed["error_code"] == "provider_unavailable"
    assert failed["attempt"] == 1
    assert len(env.adapter.calls) == 1
    assert "sensitive provider" not in str(failed)
    assert await env.engine.jobs.claim("another") is None


class TemporaryProviderError(Exception):
    status_code = 503


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [TimeoutError("private response"), TemporaryProviderError("private response")])
async def test_transient_provider_error_retries_only_as_delayed_job(environment, error):
    env = environment

    async def temporary(messages):
        raise error

    env.adapter.on_generate = temporary
    started = time.time()
    waiting = await execute(env, await enqueue_and_claim(env))
    assert waiting["state"] == "retry_wait"
    assert waiting["error_code"] == "provider_temporary_failure"
    assert waiting["available_at"] >= started + 5
    assert waiting["attempt"] == 1
    assert len(env.adapter.calls) == 1
    assert await env.engine.jobs.claim("too-soon") is None
    assert "private response" not in str(waiting)
    assert "raw" not in waiting["checkpoint"]["window"]
    env.adapter.on_generate = None
    # Advance the durable deadline explicitly; the test never sleeps through a
    # backoff and does not add an immediate retry around the Provider invocation.
    await env.memory.database.execute("UPDATE memory_jobs SET available_at=0 WHERE job_id=?", (waiting["job_id"],))
    result = await execute(env, await env.engine.jobs.claim("later"))
    assert result["state"] == "succeeded"
    assert result["attempt"] == 2
    assert len(env.adapter.calls) == 2


@pytest.mark.asyncio
async def test_rebuild_index_failure_preserves_active_generation_until_retry(environment):
    env = environment

    class Embeddings:
        batch_size = 16
        fingerprint = "test-embedding"

        def __init__(self):
            self.calls = 0
            self.vectors = {}

        async def cached(self, texts, **kwargs):
            return [self.vectors.get(text) for text in texts]

        async def embed(self, texts, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError("embedding network failed")
            self.vectors.update({text: [1.0, 0.0] for text in texts})
            return [self.vectors[text] for text in texts]

    embeddings = Embeddings()
    env.runtime.recall = SimpleNamespace(embeddings=embeddings)
    env.adapter.output = json.dumps(output([node(env.records[0].source_id)]))
    old = await env.memory.get_active_generation(env.space["space_id"])
    waiting = await execute(env, await enqueue_and_claim(env, "rebuild"))
    assert waiting["state"] == "retry_wait"
    assert waiting["step"] == "window_index"
    assert waiting["checkpoint"]["done"] == 0
    assert waiting["checkpoint"]["window_index"]["total"] == 1
    target = waiting["checkpoint"]["generation_id"]
    assert (await env.memory.get_active_generation(env.space["space_id"]))["generation_id"] == old["generation_id"]
    assert len(await env.memory.list_nodes(space_id=env.space["space_id"], generation_id=target)) == 1
    assert await env.engine.pipeline.get_rebuild_receipt(waiting["job_id"]) is None
    await env.memory.database.execute("UPDATE memory_jobs SET available_at=0 WHERE job_id=?", (waiting["job_id"],))
    completed = await execute(env, await env.engine.jobs.claim("retry-index"))
    assert completed["state"] == "succeeded"
    assert (await env.memory.get_active_generation(env.space["space_id"]))["generation_id"] == target
    assert len(env.adapter.calls) == 1
    assert embeddings.calls == 2


@pytest.mark.asyncio
async def test_pending_retains_only_references_across_five_page_scan_budget(environment):
    env = environment
    for index in range(3, 1003):
        await capture(env.sources, number=index)
    generation = await env.memory.get_active_generation(env.space["space_id"])
    generation_id = generation["generation_id"]
    rows = await env.sources.database.fetchall("SELECT source_id,stream_id,sequence_no FROM source_records ORDER BY rowid")
    # Seed committed sources without a scan cursor.
    # The first and final sources remain pending across 1,000 consumed records.
    async with env.memory.database.transaction() as connection:
        await connection.execute(
            "INSERT INTO extraction_batches(batch_id,space_id,generation_id,activity_round,prompt_version,"
            "schema_version,model_id,input_digest,output_digest,result_json,committed_at) "
            "VALUES ('seed',?,?,1,'extraction-prompt.v1','extraction.v1','offline','input','output','{}',?)",
            (env.space["space_id"], generation_id, time.time()),
        )
        await connection.executemany(
            "INSERT INTO extraction_batch_sources(batch_id,generation_id,source_id,ordinal,stream_id,sequence_no) "
            "VALUES ('seed',?,?,?,?,?)",
            [(generation_id, row["source_id"], index, row["stream_id"], row["sequence_no"])
             for index, row in enumerate(rows[1:-1])],
        )
    assert await env.engine._pending(env.space, generation_id) == []
    retained = env.engine._pending_reads[generation_id]
    assert retained["refs"] == [(rows[0]["source_id"], 1)]
    assert retained["after"] == 1000
    assert all(len(ref) == 2 for ref in retained["refs"])
    pending = await env.engine._pending(env.space, generation_id)
    assert [row["source_id"] for row in pending] == [rows[0]["source_id"], rows[-1]["source_id"]]
    assert generation_id not in env.engine._pending_reads
    assert await env.engine.pending_source_count(env.space) == 2
    assert env.adapter.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("cache_fails", [False, True])
async def test_governance_invalidates_changed_node_and_stale_parent_cache(environment, monkeypatch, cache_fails):
    env = environment
    space_id = env.space["space_id"]
    atomic = await env.memory.create_node(space_id=space_id, node_type="atomic", content_text="需要更正的细节",
                                          source_ids=[env.records[0].source_id])
    parent = await env.memory.create_node(space_id=space_id, node_type="event", content_text="引用了旧细节的事件",
                                          source_ids=[env.records[0].source_id])
    unrelated = await env.memory.create_node(space_id=space_id, node_type="atomic", content_text="独立的事实",
                                             source_ids=[env.records[1].source_id])
    await env.memory.add_edge(parent["node_id"], atomic["node_id"], "contains")
    cache = CacheStore(env.memory.path.parent / "cache.db")
    await cache.open()
    env.runtime.cache = cache
    hashes = [text_hash(item["content_text"]) for item in (atomic, parent, unrelated)]
    identities = [(item["node_id"], item["content_version"]) for item in (atomic, parent, unrelated)]
    try:
        await cache.put_vectors("embedding", space_id, hashes, [[1, 0]] * 3, kind="node", identities=identities)
        if cache_fails:
            async def fail_invalidation(*args, **kwargs):
                raise OSError("cache unavailable")
            monkeypatch.setattr(cache, "invalidate_nodes", fail_invalidation)
        op = operation("edit", [atomic], patch={"content_text": "人工确认的新细节"})
        result = await execute(env, await enqueue_and_claim(env, "governance", operation=op))
        assert result["state"] == "succeeded"
        assert (await env.memory.get_node(atomic["node_id"]))["content_text"] == "人工确认的新细节"
        assert (await env.memory.get_node(parent["node_id"]))["metadata"]["summary_stale"] is True
        cached = await cache.get_vectors("embedding", space_id, hashes, kind="node", identities=identities)
        if cache_fails:
            assert env.engine.last_error_type == "OSError"
        else:
            assert cached[:2] == [None, None]
        assert cached[2] == [1, 0]
    finally:
        await cache.close()
        env.runtime.cache = None


@pytest.mark.asyncio
async def test_title_only_governance_keeps_cached_body_vector(environment):
    env = environment
    space_id = env.space["space_id"]
    node = await env.memory.create_node(space_id=space_id, node_type="atomic", content_text="约定周末参观展览。",
                                        source_ids=[env.records[0].source_id])
    cache = CacheStore(env.memory.path.parent / "cache.db")
    await cache.open()
    env.runtime.cache = cache
    hashes = [text_hash(node["content_text"])]
    identities = [(node["node_id"], node["content_version"])]
    try:
        await cache.put_vectors("embedding", space_id, hashes, [[1, 0]], kind="node", identities=identities)
        op = operation("edit", [node], patch={"title": "周末看展"}, expected_titles={node["node_id"]: None})
        result = await execute(env, await enqueue_and_claim(env, "governance", operation=op))
        assert result["state"] == "succeeded"
        assert (await env.memory.get_node(node["node_id"]))["title"] == "周末看展"
        assert await cache.get_vectors("embedding", space_id, hashes, kind="node", identities=identities) == [[1, 0]]
    finally:
        await cache.close()
        env.runtime.cache = None


@pytest.mark.asyncio
async def test_governance_request_keeps_command_body_only_once(environment):
    env = environment
    created = await env.memory.create_node(space_id=env.space["space_id"], node_type="event",
        content_text="原有正文", source_ids=[row.source_id for row in env.records])
    command = operation("edit", [created], patch={"content_text": "人工修订的正文"})
    first = await env.engine.enqueue("governance", env.space["space_id"], "governance-dedupe", operation=command)
    assert first["payload"]["operation"] == command
    assert "operation" not in first["payload"]["request"]
    repeated = await env.engine.enqueue("governance", env.space["space_id"], "governance-dedupe", operation=command)
    assert repeated["job_id"] == first["job_id"]
