import asyncio
import json
import sqlite3
from dataclasses import replace
from types import SimpleNamespace

import pytest
import pytest_asyncio

from core.ingress import IngressError, IngressService
from storage.ingress_store import IngressStore
from storage.source_store import SourceStore
from tests.test_source_store import make_source


@pytest_asyncio.fixture
async def ingress(tmp_path):
    now = [100.0]
    pending = IngressStore(tmp_path / "ingress.db", clock=lambda: now[0])
    sources = SourceStore(tmp_path / "source.db")
    await pending.open()
    await sources.open()
    service = IngressService(
        pending,
        sources,
        SimpleNamespace(
            ingress_max_attempts=2, ingress_retry_seconds=5, ingress_poll_seconds=0.01
        ),
    )
    try:
        yield service, now
    finally:
        await service.stop()
        await sources.close()
        await pending.close()


def other_stream(source):
    return replace(
        source,
        identity=replace(
            source.identity,
            unified_msg_origin="adapter-1:FriendMessage:user-2",
            account_id="user-2",
            session_id="user-2",
        ),
        actor_account_id="user-2",
    )


@pytest.mark.asyncio
async def test_admission_precedes_capture_and_cleanup_is_idempotent(
    ingress, monkeypatch
):
    service, _ = ingress
    original = service.sources.capture
    pending_seen = []

    async def checked(source):
        row = await service.store.get(source.source_id, include_content=True)
        pending_seen.append(row["source"]["content_text"])
        assert row["state"] == "running"
        return await original(source)

    monkeypatch.setattr(service.sources, "capture", checked)
    first = await service.submit(make_source())
    duplicate = await service.submit(replace(make_source(), observed_at=999.0))
    assert first.durable is True
    assert first.state == "captured"
    assert first.captured.inserted is True
    assert duplicate.captured.inserted is False
    assert pending_seen == ["hello", "hello"]
    assert await service.list() == []
    assert (await service.sources.health())["source_count"] == 1
    sync = await service.store.database.fetchone("PRAGMA synchronous")
    assert sync[0] == 2  # FULL


@pytest.mark.asyncio
async def test_pending_dedup_conflict_and_capacity_do_not_evict(tmp_path):
    store = IngressStore(tmp_path / "ingress.db", max_entries=1)
    await store.open()
    try:
        source = make_source()
        source = replace(source, identity=replace(source.identity, message_type="GroupMessage",
                         unified_msg_origin="adapter-1:GroupMessage:g", session_id="g", group_id="g"))
        first = await store.enqueue(source)
        redelivered = replace(source, observed_at=999, identity=replace(source.identity, group_name="可用名称"))
        duplicate = await store.enqueue(redelivered)
        assert first["arrival_no"] == duplicate["arrival_no"]
        with pytest.raises(IngressError, match="ingress_source_conflict"):
            await store.enqueue(replace(source, content_text="different actual content"))
        with pytest.raises(IngressError, match="ingress_entry_limit"):
            await store.enqueue(make_source("second"))
        assert len(await store.list()) == 1
        assert (await store.get(source.source_id, include_content=True))[
            "source"
        ]["content_text"] == "hello"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_utf8_payload_byte_cap_applies_atomically(tmp_path):
    store = IngressStore(tmp_path / "ingress.db", max_bytes=100)
    await store.open()
    try:
        with pytest.raises(IngressError, match="ingress_byte_limit"):
            await store.enqueue(make_source(content="我" * 100))
        assert (await store.health())["pending_count"] == 0
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_failure_keeps_stream_order_other_stream_continues_and_manual_retry(
    ingress, monkeypatch
):
    service, now = ingress
    original = service.sources.capture
    blocked = True
    first = make_source("first")
    second = make_source("second")
    other = other_stream(make_source("other"))

    async def sometimes(source):
        running = await service.get(source.source_id)
        assert running["state"] == "running"
        assert running["error_code"] is None
        if blocked and source.source_id == first.source_id:
            raise sqlite3.OperationalError("secret text from SQL parameters")
        return await original(source)

    monkeypatch.setattr(service.sources, "capture", sometimes)
    assert (await service.submit(first)).state == "retry_wait"
    assert (await service.get(first.source_id))["error_code"] == "source_operational_error"
    assert (await service.submit(second)).state == "queued"
    assert (await service.submit(other)).state == "captured"
    assert await service.process_once() is None  # Backoff has not elapsed.
    now[0] += 5
    assert await service.process_once() is None
    row = await service.get(first.source_id)
    assert row["state"] == "retry_wait" and row["attempts"] == 2
    assert row["available_at"] == now[0] + 6 * 3600
    assert row["error_code"] == "source_operational_error"
    assert "secret" not in json.dumps(await service.list())
    assert "hello" not in json.dumps(await service.list())
    now[0] += 100
    assert await service.process_once() is None
    blocked = False
    assert await service.retry(first.source_id) == 1
    captured1 = await service.process_once()
    captured2 = await service.process_once()
    assert captured1.source_id == first.source_id
    assert captured2.source_id == second.source_id
    assert (captured1.sequence_no, captured2.sequence_no) == (1, 2)
    assert (await service.health())["pending_count"] == 0


@pytest.mark.asyncio
async def test_exhausted_transient_head_restarts_and_drains_in_order(ingress, monkeypatch):
    service, now = ingress
    first, second = make_source("busy-head"), make_source("following")
    original = service.sources.capture

    async def busy(source):
        raise OSError("write temporarily unavailable")

    monkeypatch.setattr(service.sources, "capture", busy)
    await service.submit(first)
    await service.submit(second)
    now[0] += 5
    await service.process_once()
    assert (await service.health())["oldest_pending_at"] == 100
    # Also exercise persisted exhausted rows written before scheduled recovery.
    await service.store.database.execute(
        "UPDATE ingress_records SET state='failed',available_at=updated_at WHERE source_id=?", (first.source_id,))
    await service.store.close()
    await service.store.open()
    monkeypatch.setattr(service.sources, "capture", original)
    now[0] += 6 * 3600 - 1
    assert await service.process_once() is None
    now[0] += 1
    assert (await service.process_once()).source_id == first.source_id
    assert (await service.process_once()).source_id == second.source_id
    assert await service.list() == []
    assert (await service.sources.health())["last_source_write_at"]


@pytest.mark.asyncio
async def test_retry_all_does_not_release_running_or_reorder(ingress):
    service, _ = ingress
    one, two = make_source("one"), make_source("two")
    await service.store.enqueue(one)
    await service.store.enqueue(two)
    claim = await service.store.claim()
    assert await service.retry() == 0
    assert await service.store.claim() is None
    await service.store.fail(
        claim, error_code="source_invalid_payload", retryable=False, retry_seconds=1
    )
    assert await service.retry() == 1
    assert (await service.store.claim()).source_id == one.source_id


@pytest.mark.asyncio
async def test_source_commit_before_ack_crash_replays_without_duplicate(tmp_path):
    now = [100.0]
    pending_path, source_path = tmp_path / "ingress.db", tmp_path / "source.db"
    store, sources = (
        IngressStore(pending_path, clock=lambda: now[0]),
        SourceStore(source_path),
    )
    await store.open()
    await sources.open()
    one, two = make_source("one"), make_source("two")
    await store.enqueue(one)
    await store.enqueue(two)
    claim = await store.claim()
    first = await sources.capture(claim.source)
    assert first.sequence_no == 1
    # Process dies here, before acknowledging ingress.db.
    await store.close()
    await sources.close()
    now[0] += 31
    resumed_store, resumed_sources = (
        IngressStore(pending_path, clock=lambda: now[0]),
        SourceStore(source_path),
    )
    await resumed_store.open()
    await resumed_sources.open()
    service = IngressService(resumed_store, resumed_sources, SimpleNamespace())
    try:
        resumed = await service.process_once()
        second = await service.process_once()
        assert resumed.inserted is False
        assert second.sequence_no == 2
        assert (await resumed_sources.health())["source_count"] == 2
        assert await service.list() == []
    finally:
        await service.stop()
        await resumed_store.close()
        await resumed_sources.close()


@pytest.mark.asyncio
async def test_stale_claim_cannot_acknowledge_or_fail_new_claim(ingress):
    service, now = ingress
    await service.store.enqueue(make_source())
    old = await service.store.claim()
    now[0] += 31
    new = await service.store.claim()
    assert old.token != new.token
    assert await service.store.acknowledge(old) is False
    await service.store.fail(
        old, error_code="source_io_error", retryable=False, retry_seconds=1
    )
    assert (await service.get(new.source_id))["state"] == "running"
    assert await service.store.acknowledge(new) is True


@pytest.mark.asyncio
async def test_cancelled_capture_releases_without_consuming_failure_budget(
    ingress, monkeypatch
):
    service, _ = ingress
    entered = asyncio.Event()
    original = service.sources.capture

    async def stalled(source):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(service.sources, "capture", stalled)
    source = make_source()
    task = asyncio.create_task(service.submit(source))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    pending = await service.get(source.source_id)
    assert pending["state"] == "queued" and pending["attempts"] == 0
    monkeypatch.setattr(service.sources, "capture", original)
    result = await service.process_once()
    assert result.sequence_no == 1


@pytest.mark.asyncio
async def test_background_progress_survives_frontend_absence(ingress):
    service, _ = ingress
    for i in range(4):
        await service.store.enqueue(make_source(str(i)))
    await service.start()
    async with asyncio.timeout(2):
        while (await service.health())["pending_count"]:
            await asyncio.sleep(0.01)
    assert (await service.sources.health())["source_count"] == 4
    await service.stop()
    assert service.running is False


@pytest.mark.asyncio
async def test_closed_source_db_is_recoverable_and_queued_payload_is_private(ingress):
    service, now = ingress
    source = make_source(content="private source body")
    await service.sources.close()
    result = await service.submit(source)
    assert result.durable and result.state == "retry_wait"
    assert "private source body" not in json.dumps(await service.get(source.source_id))
    assert (await service.get(source.source_id, include_content=True))["source"][
        "content_text"
    ] == "private source body"
    await service.sources.open()
    now[0] += 5
    assert (await service.process_once()).inserted is True


@pytest.mark.asyncio
async def test_unavailable_queue_never_claims_durable_acceptance(ingress):
    service, _ = ingress
    await service.store.close()
    with pytest.raises(IngressError, match="ingress_persistence_unavailable"):
        await service.submit(make_source())
    assert service.admission_failures == 1
    assert service.last_error_code == "ingress_persistence_unavailable"
    assert (await service.sources.health())["source_count"] == 0


@pytest.mark.asyncio
async def test_real_sqlite_source_constraint_rolls_back_and_keeps_failed_input(ingress):
    service, _ = ingress
    await service.sources.database.execute("""
        CREATE TRIGGER reject_capture BEFORE INSERT ON source_records
        BEGIN SELECT RAISE(ABORT,'PRIVATE PAYLOAD should never be logged'); END
    """)
    source = make_source()
    assert (await service.submit(source)).state == "failed"
    assert (await service.sources.health())["source_count"] == 0
    assert (await service.sources.health())["stream_count"] == 0
    row = await service.get(source.source_id)
    assert row["error_code"] == "source_integrity_error"
    assert "PRIVATE" not in json.dumps(await service.health())
    await service.sources.database.execute("DROP TRIGGER reject_capture")
    assert await service.retry(source.source_id) == 1
    assert (await service.process_once()).sequence_no == 1


@pytest.mark.asyncio
async def test_acknowledgement_failure_remains_recoverable(ingress, monkeypatch):
    service, _ = ingress
    acknowledge = service.store.acknowledge

    async def fail_ack(claim):
        raise sqlite3.OperationalError("private error body")

    monkeypatch.setattr(service.store, "acknowledge", fail_ack)
    source = make_source()
    result = await service.submit(source)
    assert result.captured.inserted is True
    assert (await service.get(source.source_id))["state"] == "queued"
    monkeypatch.setattr(service.store, "acknowledge", acknowledge)
    assert (await service.process_once()).inserted is False
    assert await service.list() == []


@pytest.mark.asyncio
async def test_concurrent_admissions_respect_hard_limit(tmp_path):
    first = IngressStore(tmp_path / "ingress.db", max_entries=3)
    second = IngressStore(tmp_path / "ingress.db", max_entries=3)
    await first.open()
    await second.open()
    try:
        results = await asyncio.gather(
            *(
                store.enqueue(make_source(str(i)))
                for i, store in enumerate([first, second] * 6)
            ),
            return_exceptions=True,
        )
        assert sum(isinstance(result, dict) for result in results) == 3
        assert all(
            isinstance(result, dict) or isinstance(result, IngressError)
            for result in results
        )
        assert (await first.health())["pending_count"] == 3
    finally:
        await first.close()
        await second.close()


@pytest.mark.asyncio
async def test_readonly_inbox_reports_failure_without_calling_source(ingress):
    service, _ = ingress
    await service.store.database.execute("PRAGMA query_only=ON")
    try:
        with pytest.raises(IngressError, match="ingress_persistence_unavailable"):
            await service.submit(make_source())
        assert (await service.sources.health())["source_count"] == 0
        assert (await service.store.health())["pending_count"] == 0
    finally:
        await service.store.database.execute("PRAGMA query_only=OFF")


@pytest.mark.asyncio
async def test_corrupt_payload_requires_review_but_other_stream_is_available(ingress):
    service, _ = ingress
    source, other = make_source("bad"), other_stream(make_source("good"))
    await service.store.enqueue(source)
    await service.store.enqueue(other)
    await service.store.database.execute(
        "UPDATE ingress_records SET payload_json=? WHERE source_id=?",
        ("{bad content", source.source_id),
    )
    assert await service.process_once() is None
    assert (await service.get(source.source_id))[
        "error_code"
    ] == "ingress_corrupt_payload"
    assert (await service.process_once()).source_id == other.source_id


@pytest.mark.asyncio
async def test_shutdown_waits_for_pending_admission_and_preserves_it(
    ingress, monkeypatch
):
    service, _ = ingress
    entered, release = asyncio.Event(), asyncio.Event()
    original = service.store.enqueue

    async def delayed(source, **kwargs):
        entered.set()
        await release.wait()
        return await original(source, **kwargs)

    monkeypatch.setattr(service.store, "enqueue", delayed)
    submission = asyncio.create_task(service.submit(make_source()))
    await entered.wait()
    shutdown = asyncio.create_task(service.stop())
    await asyncio.sleep(0)
    assert not shutdown.done()
    release.set()
    result = await submission
    await shutdown
    assert result.durable and result.state == "queued"
    assert (await service.health())["pending_count"] == 1
    with pytest.raises(IngressError, match="ingress_stopping"):
        await service.submit(make_source("new"))


@pytest.mark.asyncio
async def test_post_admission_read_failure_retains_durable_status(ingress, monkeypatch):
    service, _ = ingress
    original = service.store.claim

    async def unavailable(**kwargs):
        raise sqlite3.OperationalError("private exception body")

    monkeypatch.setattr(service.store, "claim", unavailable)
    result = await service.submit(make_source())
    assert result.durable and result.state == "queued"
    assert service.admission_failures == 0
    assert service.last_error_code == "ingress_worker_unavailable"
    assert (await service.store.health())["pending_count"] == 1
    monkeypatch.setattr(service.store, "claim", original)
    assert (await service.process_once()).inserted is True


@pytest.mark.asyncio
async def test_cancel_after_source_commit_preserves_source_and_resumes_idempotently(
    ingress, monkeypatch
):
    service, _ = ingress
    committed = asyncio.Event()
    original = service.sources.capture

    async def commit_then_pause(source):
        await original(source)
        committed.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(service.sources, "capture", commit_then_pause)
    task = asyncio.create_task(service.submit(make_source()))
    await committed.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert (await service.get(make_source().source_id))["state"] == "queued"
    monkeypatch.setattr(service.sources, "capture", original)
    result = await service.process_once()
    assert result.inserted is False and result.sequence_no == 1
    assert (await service.sources.health())["source_count"] == 1


@pytest.mark.asyncio
async def test_full_queue_rejects_new_submission_without_eviction_or_source_capture(
    tmp_path,
):
    store = IngressStore(tmp_path / "ingress.db", max_entries=1)
    sources = SourceStore(tmp_path / "source.db")
    await store.open()
    await sources.open()
    service = IngressService(store, sources, SimpleNamespace())
    first, rejected = make_source("accepted"), make_source("rejected")
    try:
        await store.enqueue(first)
        with pytest.raises(IngressError, match="ingress_entry_limit"):
            await service.submit(rejected)
        assert service.admission_failures == 1
        assert service.last_error_code == "ingress_entry_limit"
        assert [row["source_id"] for row in await service.list()] == [first.source_id]
        assert await service.get(rejected.source_id) is None
        assert (await sources.health())["source_count"] == 0
        assert (await service.process_once()).source_id == first.source_id
        assert (await service.submit(rejected)).durable is True
    finally:
        await service.stop()
        await store.close()
        await sources.close()


@pytest.mark.asyncio
async def test_shutdown_during_cancelled_submission_recovers_remaining_queue(
    ingress, monkeypatch
):
    service, _ = ingress
    entered = asyncio.Event()
    original = service.sources.capture

    async def paused(source):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(service.sources, "capture", paused)
    task = asyncio.create_task(service.submit(make_source()))
    await entered.wait()
    shutdown = asyncio.create_task(service.stop())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await shutdown
    assert (await service.get(make_source().source_id))["state"] == "queued"
    monkeypatch.setattr(service.sources, "capture", original)
    await service.start()
    async with asyncio.timeout(2):
        while (await service.health())["pending_count"]:
            await asyncio.sleep(0.01)
    assert (await service.sources.health())["source_count"] == 1


@pytest.mark.asyncio
async def test_valid_json_with_changed_content_is_quarantined_before_capture(ingress):
    service, _ = ingress
    source = make_source(content="Original content")
    await service.store.enqueue(source)
    await service.store.database.execute(
        "UPDATE ingress_records SET payload_json=json_set(payload_json,'$.content_text',?) WHERE source_id=?",
        ("Untrusted changed content", source.source_id),
    )
    assert await service.process_once() is None
    assert (await service.get(source.source_id))[
        "error_code"
    ] == "ingress_corrupt_payload"
    assert (await service.sources.health())["source_count"] == 0


@pytest.mark.asyncio
async def test_rejected_newer_schema_closes_its_connection(tmp_path):
    path = tmp_path / "ingress.db"
    original = IngressStore(path)
    await original.open()
    await original.database.execute("PRAGMA user_version=999")
    await original.close()
    newer = IngressStore(path)
    with pytest.raises(IngressError, match="ingress_schema_newer_than_plugin"):
        await newer.open()
    assert newer.database._connection is None


@pytest.mark.asyncio
async def test_ambiguous_new_route_waits_in_order_and_recovers_after_owner_confirmation(ingress):
    service, now = ingress
    sources = service.sources
    for instance, owner in (("qq-a", "old-A"), ("qq-b", "old-B")):
        await sources.bind_account(platform_instance_id=instance, account_id="123", logical_user_id=owner)
    async with sources.database.transaction() as connection:
        for instance in ("qq-a", "qq-b"):
            await connection.execute("INSERT INTO account_platforms VALUES (?,'qq')", (instance,))

    source = make_source("first", "hello")
    identity = replace(source.identity, platform_instance_id="qq-c", platform_name="aiocqhttp", account_id="123")
    first = replace(source, actor_account_id="123", identity=identity)
    second = replace(first, message_id="second")
    assert (await service.submit(first)).state == "retry_wait"
    assert (await service.submit(second)).state == "queued"
    for _ in range(4):
        now[0] += 6
        assert await service.process_once() is None
    waiting = await service.store.get(first.source_id)
    assert (waiting["error_code"], waiting["attempts"]) == ("identity_binding_conflict", 0)
    assert (await service.submit(other_stream(first))).state == "captured"
    await sources.database.execute(
        "UPDATE account_bindings SET logical_user_id='old-A' WHERE platform_instance_id='qq-b'")
    now[0] += 6
    received = [await service.process_once(), await service.process_once()]
    assert [(row.source_id, row.logical_user_id, row.sequence_no) for row in received] == [
        (first.source_id, "old-A", 1), (second.source_id, "old-A", 2)]
    assert await service.list() == []
