"""Recovery regressions found by an independent maintenance interface review."""

import json
from types import SimpleNamespace

import pytest
import pytest_asyncio

from core.config import NeoMemoryConfig
from core.extraction_apply import ExtractionService
from core.identity import EventIdentity
from core.maintenance import MaintenanceEngine
from core.memory_spaces import private_space
from core.source_ingest import CapturedSource
from storage.memory_store import MemoryStore
from storage.source_store import SourceStore


def no_change():
    return json.dumps(
        {
            "schema_version": "extraction.v1",
            "result": "no_change",
            "reason": "no_meaningful_content",
            "node_changes": [],
            "profile_changes": [],
            "edges": [],
            "relationship_change": None,
            "coverage_changes": [],
        }
    )


class Adapter:
    def __init__(self):
        self.calls = 0

    def llm_fingerprint(self, provider_id, revision):
        return f"{provider_id}:{revision}"

    async def generate(self, messages, *, provider_id, timeout_seconds, diagnostics=None, **kwargs):
        self.calls += 1
        return no_change()


@pytest_asyncio.fixture
async def fixture(tmp_path):
    sources = SourceStore(tmp_path / "source.db")
    memory = MemoryStore(tmp_path / "memory.db", source_exists=sources.has_source)
    await sources.open()
    await memory.open()
    adapter = Adapter()

    async def persona_loader(persona_id):
        return "合成测试人格"

    engine = MaintenanceEngine(
        sources,
        memory,
        SimpleNamespace(adapter=adapter, recall=None, cache=None),
        NeoMemoryConfig.from_mapping(
            {"llm_provider_id": "test", "automatic_extraction_enabled": False}
        ),
        persona_loader,
    )
    try:
        yield sources, memory, engine, adapter
    finally:
        await engine.stop()
        await memory.close()
        await sources.close()


async def populate(fixture, count):
    sources, memory, _, _ = fixture
    captured = []
    for index in range(count):
        record = await sources.capture(
            CapturedSource(
                identity=EventIdentity(
                    "test",
                    "test",
                    "FriendMessage",
                    "test:u",
                    "u",
                    "",
                    "u",
                    "User",
                    "bot",
                    f"m{index}",
                ),
                actor_kind="user",
                actor_account_id="u",
                content_text=f"合成消息{index}",
                observed_at=100 + index,
                persona_id="p",
            )
        )
        captured.append(record)
    space = await memory.get_or_create_space(
        private_space("p", captured[0].logical_user_id)
    )
    return space, captured


@pytest.mark.asyncio
async def test_pending_keeps_scanning_a_page_with_sparse_unconsumed_tail(fixture):
    sources, memory, engine, _ = fixture
    space, records = await populate(fixture, 205)
    service = ExtractionService(sources, memory)
    # Seed a consumed batch committed without a scan cursor.
    batch = await service.prepare(
        space_id=space["space_id"],
        source_ids=tuple(r.source_id for r in records[:198]),
        persona="test",
    )
    await service.apply(batch, no_change(), model_id="test")
    generation = await memory.get_active_generation(space["space_id"])
    pending = await engine._pending(space, generation["generation_id"])
    assert [row["source_id"] for row in pending] == [r.source_id for r in records[198:]]
    assert (await engine._decision(space, pending)).status == "ready"


@pytest.mark.asyncio
async def test_reject_refuses_a_failed_window_from_a_retired_generation(fixture):
    _, memory, engine, _ = fixture
    space, _ = await populate(fixture, 5)
    original = await engine.enqueue(
        "extract", space["space_id"], "failed-before-rebuild"
    )
    claimed = await engine.jobs.claim("test")
    await engine.jobs.save_checkpoint(
        claimed["job_id"],
        claimed["lease_token"],
        checkpoint={
            "window": {
                "source_ids": original["payload"]["source_ids"],
                "source_cursor": original["payload"]["source_cursor"],
            }
        },
        step="validate",
        progress_done=0,
        progress_total=5,
    )
    await engine.jobs.fail(
        claimed["job_id"],
        claimed["lease_token"],
        error_type="ValueError",
        error_code="invalid_output",
    )
    new_generation = await memory.create_generation(space["space_id"])
    await memory.activate_generation(space["space_id"], new_generation["generation_id"])
    with pytest.raises(ValueError, match="generation|context|stale"):
        await engine.enqueue(
            "reject",
            space["space_id"],
            "reject-old-generation",
            failed_job_id=original["job_id"],
        )


@pytest.mark.asyncio
async def test_resume_saved_output_survives_recall_strength_updates(
    fixture, monkeypatch
):
    _, memory, engine, adapter = fixture
    space, records = await populate(fixture, 5)
    node = await memory.create_node(
        space_id=space["space_id"],
        node_type="atomic",
        content_text="已知稳定事实",
        source_ids=[records[0].source_id],
    )
    await engine.enqueue("extract", space["space_id"], "strength-during-retry")
    claimed = await engine.jobs.claim("test")
    real_apply = engine.extraction.apply

    async def lost_connection(*args, **kwargs):
        raise ConnectionError("simulated commit connection failure")

    monkeypatch.setattr(engine.extraction, "apply", lost_connection)
    with pytest.raises(ConnectionError):
        await engine._extract(claimed)
    saved = await engine.jobs.get(claimed["job_id"])
    assert saved["checkpoint"]["window"]["raw"] == no_change()
    await engine.jobs.fail(
        claimed["job_id"],
        claimed["lease_token"],
        error_type="ConnectionError",
        error_code="temporary_failure",
    )
    await memory.reinforce_recall(
        space["space_id"],
        node["generation_id"],
        "recall-while-retry",
        [
            {
                "node_id": node["node_id"],
                "content_version": node["content_version"],
                "direct": True,
            }
        ],
    )
    monkeypatch.setattr(engine.extraction, "apply", real_apply)
    await engine.jobs.retry(claimed["job_id"])
    resumed = await engine.jobs.claim("test")
    result = await engine._extract(resumed)
    assert result["source_count"] == 5
    assert adapter.calls == 1


@pytest.mark.asyncio
async def test_committed_candidate_cannot_be_reported_as_rejected(fixture):
    _, memory, engine, _ = fixture
    space, _ = await populate(fixture, 5)
    original = await engine.enqueue(
        "extract", space["space_id"], "committed-before-status-save"
    )
    claimed = await engine.jobs.claim("test")
    result = await engine._extract(claimed)
    assert await memory.get_extraction_batch(result["batch_id"])
    # The memory commit succeeded, then task status persistence failed.
    await engine.jobs.fail(
        claimed["job_id"],
        claimed["lease_token"],
        error_type="ConnectionError",
        error_code="temporary_failure",
    )
    with pytest.raises(ValueError, match="committed"):
        await engine.enqueue(
            "reject",
            space["space_id"],
            "incorrect-rejection",
            failed_job_id=original["job_id"],
        )


@pytest.mark.asyncio
async def test_cancelled_model_response_cannot_commit_or_advance_cursor(
    fixture, monkeypatch
):
    import asyncio

    from storage.job_store import LeaseLost

    _, memory, engine, adapter = fixture
    space, _ = await populate(fixture, 5)
    await engine.enqueue("extract", space["space_id"], "cancel-during-model")
    claimed = await engine.jobs.claim("test")
    entered, release = asyncio.Event(), asyncio.Event()

    async def blocked_generate(*args, **kwargs):
        entered.set()
        await release.wait()
        return no_change()

    monkeypatch.setattr(adapter, "generate", blocked_generate)
    task = asyncio.create_task(engine._extract(claimed))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await engine.jobs.cancel(claimed["job_id"])
        release.set()
        with pytest.raises(LeaseLost):
            await task
        generation = await memory.get_active_generation(space["space_id"])
        assert generation["activity_round"] == 0
        assert await engine.pipeline.get_cursor(generation["generation_id"]) == 0
        assert await memory.database.fetchall("SELECT * FROM extraction_batches") == []
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_old_failed_rebuild_stops_before_model_after_another_rebuild_switches(
    fixture,
):
    from core.jobs import ReviewRequired

    _, memory, engine, adapter = fixture
    space, _ = await populate(fixture, 5)
    first = await engine.enqueue("rebuild", space["space_id"], "original-rebuild")
    claimed = await engine.jobs.claim("test")
    original_target = await engine.pipeline.begin_rebuild(
        claimed, space["space_id"], first["payload"]["expected_generation_id"]
    )
    await engine.jobs.fail(
        claimed["job_id"],
        claimed["lease_token"],
        error_type="ConnectionError",
        error_code="temporary_failure",
    )

    second = await engine.enqueue("rebuild", space["space_id"], "replacement-rebuild")
    replacement = await engine.jobs.claim("test")
    assert replacement["job_id"] == second["job_id"]
    result = await engine._rebuild(replacement)
    await engine.jobs.finish(
        replacement["job_id"], replacement["lease_token"], result=result
    )
    calls_after_replacement = adapter.calls
    assert calls_after_replacement > 0
    assert result["generation_id"] != original_target["generation_id"]

    await engine.jobs.retry(first["job_id"])
    resumed = await engine.jobs.claim("test")
    assert resumed["job_id"] == first["job_id"]
    with pytest.raises(ReviewRequired, match="context_changed"):
        await engine._rebuild(resumed)
    assert adapter.calls == calls_after_replacement
    assert await engine.pipeline.get_cursor(original_target["generation_id"]) == 0
    assert (await memory.get_active_generation(space["space_id"]))[
        "generation_id"
    ] == result["generation_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("replay", [False, True])
async def test_forward_body_counts_towards_extraction_window_budget(fixture, replay):
    from core.extraction_window import WindowPolicy

    _, _, engine, _ = fixture
    engine.policy = lambda space: WindowPolicy(min_messages=1, topic_messages=2, max_messages=2,
                                               max_chars=100, max_tokens=1000)
    rows = [{"source_id": "outer", "observed_at": 1, "content_text": "",
             "content_json": json.dumps([{"type": "Node", "name": "乙", "content": "字" * 101}])},
            {"source_id": "next", "observed_at": 2, "content_text": "收到"}]
    decision = await engine._decision({"space_type": "private"}, rows, replay=replay)
    assert decision.status == "oversized"
    assert decision.blocked_source_id == "outer"
