import asyncio
import json
from types import SimpleNamespace

import pytest
import pytest_asyncio

pytest.importorskip("astrbot.api.web")

import adapters.maintenance_api as module  # noqa: E402
from adapters.maintenance_api import MaintenanceAPI  # noqa: E402
from core.source_ingest import normalize_event  # noqa: E402
from core.extraction_window import ExtractionBacklog
from core.recall_history import RecallHistory
from core.memory_spaces import group_space, private_space  # noqa: E402
from storage.job_store import JobStore  # noqa: E402
from storage.memory_store import MemoryStore  # noqa: E402
from storage.source_store import SourceStore  # noqa: E402
from tests.test_host_probe import FakeEvent  # noqa: E402


def test_task_summary_exposes_committed_index_progress_without_model_output():
    summary = module._summary({"kind": "extract", "state": "failed",
        "checkpoint": {"window_index": {"batch_id": "synthetic", "indexed": 16, "total": 20},
                       "window": {"source_ids": ["synthetic-source"], "raw": "private model output"}}})
    assert summary["committed_window"] == {"batch_id": "synthetic", "indexed": 16, "total": 20}
    assert "private model output" not in json.dumps(summary)


class FakeEngine:
    def __init__(self, jobs):
        self.jobs = jobs
        self.retries = []

    async def automatic_status(self, space_id):
        return {"state": "enabled", "reason": None, "blocking_jobs": []}

    async def enqueue(self, kind, space_id, key, *, operation=None, reason="", failed_job_id=None, include_topics=False):
        return await self.jobs.submit(kind, space_id,
                                      {"operation": operation, "reason": reason, "failed_job_id": failed_job_id, "include_topics": include_topics}, key)

    async def retry(self, job_id, *, fresh=False):
        self.retries.append((job_id, fresh))
        return await self.jobs.retry(job_id)

    async def delete(self, job_id):
        return await self.jobs.delete(job_id)

    async def clear_completed(self, *, scope_key=None):
        result = await self.jobs.clear_completed(scope_key=scope_key)
        result.pop("removed_generation_ids")
        return result

    async def describe(self, job_id):
        job = await self.jobs.get(job_id)
        if job is None:
            raise KeyError(job_id)
        return {**module._summary(job), "candidate": None, "sources": [], "restart_required": False}


@pytest_asyncio.fixture
async def api(tmp_path, monkeypatch):
    memory = MemoryStore(tmp_path / "memory.db")
    await memory.open()
    jobs = JobStore(memory.database)
    plugin = SimpleNamespace(ready=True, terminating=False, memory_store=memory, jobs=jobs, maintenance=FakeEngine(jobs))
    result = MaintenanceAPI(plugin)
    result.test_space = await memory.get_or_create_space(private_space("p", "u"))
    result.test_body = None
    result.test_query = {}

    async def body(**kwargs):
        return result.test_body

    monkeypatch.setattr(module, "request", SimpleNamespace(json=body, query=result.test_query))
    try:
        yield result
    finally:
        await memory.close()


def payload(response, status=200):
    assert response.status_code == status, response.body
    return json.loads(response.body)


def submit_body(api, **kwargs):
    api.test_body = {"kind": "extract", "space_id": api.test_space["space_id"], "idempotency_key": "same-click", **kwargs}


async def make_node(api, **kwargs):
    return await api.plugin.memory_store.create_node(space_id=api.test_space["space_id"], node_type="event",
                                                      content_text="A remembered event.", source_ids=["source"], **kwargs)


def test_registration_uses_host_api_only(api):
    registrations = []
    api.register(SimpleNamespace(register_web_api=lambda *args: registrations.append(args)))
    assert len(registrations) == 22
    assert not any(path.endswith(("/companion/preview", "/companion/backfill")) for path, *_ in registrations)
    assert {(row[0], tuple(row[2])) for row in registrations} >= {
        ("/astrbot_plugin_neomemory/tasks", ("GET",)),
        ("/astrbot_plugin_neomemory/topics/rebuild-preview", ("GET",)),
        ("/astrbot_plugin_neomemory/extraction/backlog", ("GET",)),
        ("/astrbot_plugin_neomemory/recall/test", ("POST",)),
        ("/astrbot_plugin_neomemory/recall/history", ("GET",)),
        ("/astrbot_plugin_neomemory/recall/recording", ("POST",)),
        ("/astrbot_plugin_neomemory/tasks", ("POST",)),
        ("/astrbot_plugin_neomemory/memory/versions", ("GET",)),
        ("/astrbot_plugin_neomemory/memory/node-draft", ("GET",)),
        ("/astrbot_plugin_neomemory/memory/node-draft/discard", ("POST",)),
        ("/astrbot_plugin_neomemory/memory/relations", ("GET",)),
        ("/astrbot_plugin_neomemory/tasks/clear-completed", ("POST",)),
        ("/astrbot_plugin_neomemory/space/clear", ("POST",)),
        ("/astrbot_plugin_neomemory/space/delete", ("POST",)),
    }
    assert all(callable(row[1]) for row in registrations)
    api.register(SimpleNamespace())


@pytest.mark.asyncio
@pytest.mark.parametrize("attribute,value", [("ready", False), ("terminating", True), ("maintenance", None), ("jobs", None)])
async def test_all_endpoints_require_ready_maintenance(api, attribute, value):
    setattr(api.plugin, attribute, value)
    for name in ("tasks", "task", "test_recall", "recall_history", "recall_recording", "extraction_backlog", "availability", "submit", "retry", "cancel", "delete", "clear_completed", "spaces", "nodes", "versions", "relations"):
        result = payload(await getattr(api, name)(), 503)
        assert result["data"]["error_code"] == "maintenance_unavailable"


@pytest.mark.asyncio
async def test_manual_extract_reports_window_that_became_unavailable_before_submit(api, monkeypatch):
    async def enqueue(*args, **kwargs):
        raise module.ExtractionWindowPending(ExtractionBacklog(1, 1, 3))

    monkeypatch.setattr(api.plugin.maintenance, "enqueue", enqueue)
    submit_body(api)
    response = payload(await api.submit(), 409)
    assert "还差 2 条" in response["message"]
    assert response["data"] == {"error_code": "extraction_window_pending", "pending_count": 1, "min_messages": 3}
    assert await api.plugin.jobs.list() == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["extract", "topic_rebuild", "profile_rebuild", "relationship_rebuild"])
async def test_submit_is_idempotent_and_conflicts_use_409(api, kind):
    submit_body(api, kind=kind)
    first = payload(await api.submit())
    again = payload(await api.submit())
    assert first["job_id"] == again["job_id"]
    assert first["scope_key"] == api.test_space["space_id"]
    assert "payload" not in first and "lease_token" not in first
    submit_body(api, idempotency_key="different-click")
    assert payload(await api.submit(), 409)["data"]["error_type"] == "JobConflict"
    submit_body(api, kind="rebuild")
    payload(await api.submit(), 409)


@pytest.mark.asyncio
@pytest.mark.parametrize("overrides", [
    {"kind": "unknown"}, {"kind": []}, {"idempotency_key": ""}, {"idempotency_key": "x" * 201},
    {"idempotency_key": 123}, {"space_id": False}, {"extra": "ignored?"}, {"operation": {}},
    {"reason": []}, {"failed_job_id": "irrelevant"}, {"kind": "reject", "failed_job_id": "job", "reason": ""},
    {"reason": "x" * 2001},
])
async def test_submit_rejects_invalid_and_unknown_fields_without_creating_job(api, overrides):
    submit_body(api, **overrides)
    payload(await api.submit(), 400)
    assert await api.plugin.jobs.list() == []


@pytest.mark.asyncio
async def test_submit_requires_real_space_id_and_complete_object(api):
    for body in (None, [], {}, {"kind": "extract"}):
        api.test_body = body
        payload(await api.submit(), 400)
    submit_body(api, space_id="missing")
    payload(await api.submit(), 404)
    submit_body(api, space_id=api.test_space["scope_key"])
    payload(await api.submit(), 404)


@pytest.mark.asyncio
async def test_governance_validates_schema_before_submission(api):
    node = await make_node(api)
    operation = {"action": "edit", "node_id": node["node_id"], "expected_generation_id": node["generation_id"],
                 "expected_versions": {node["node_id"]: node["content_version"]},
                 "expected_updated_at": {node["node_id"]: node["updated_at"]},
                 "patch": {"content_text": "Corrected memory.", "importance": 0.8}}
    submit_body(api, kind="governance", operation=operation)
    result = payload(await api.submit())
    job = await api.plugin.jobs.get(result["job_id"])
    assert job["payload"]["operation"] == operation
    assert (await api.plugin.memory_store.get_node(node["node_id"]))["content_text"] == node["content_text"]
    submit_body(api, kind="governance", operation={**operation, "sql": "unsafe"})
    payload(await api.submit(), 400)
    submit_body(api, kind="governance", operation={key: value for key, value in operation.items() if key != "expected_versions"})
    payload(await api.submit(), 400)


@pytest.mark.asyncio
async def test_task_list_has_safe_projection_filter_and_pagination(api):
    jobs = api.plugin.jobs
    secret = "provider-secret-response-body"
    first = await jobs.submit("extract", api.test_space["space_id"], {"secret": secret}, "first")
    running = await jobs.claim("worker")
    await jobs.save_checkpoint(running["job_id"], running["lease_token"], checkpoint={"response": secret},
                               step="extracting", progress_done=1, progress_total=5)
    await jobs.submit("extract", "other-space", {"secret": secret}, "other")
    api.test_query.update(scope_key=api.test_space["space_id"], limit="1")
    result = payload(await api.tasks())
    assert len(result["items"]) == 1 and not result["has_more"]
    assert result["items"][0]["job_id"] == first["job_id"]
    assert result["items"][0]["step"] == "extracting"
    assert result["items"][0]["progress_done"] == 1
    assert secret not in json.dumps(result)
    assert not {"lease_token", "checkpoint", "payload", "result"} & result["items"][0].keys()
    api.test_query.pop("scope_key")
    assert payload(await api.tasks())["has_more"]


@pytest.mark.asyncio
async def test_task_history_shows_latest_50_in_scope(api):
    jobs = api.plugin.jobs
    scope = api.test_space["space_id"]
    ids = []
    for i in range(52):
        job = await jobs.submit("extract", scope if i < 51 else "other-space", {}, f"job-{i}",
                                hide_on_success=i % 2 == 0)
        running = await jobs.claim("worker")
        await jobs.finish(job["job_id"], running["lease_token"])
        ids.append(job["job_id"])
    for query, expected in (({"scope_key": scope}, ids[:-1]), ({}, ids)):
        api.test_query.clear()
        api.test_query.update(query)
        listing = payload(await api.tasks())
        assert [job["job_id"] for job in listing["history"]] == expected[-50:][::-1]
        assert listing["history_has_more"]
        assert listing["completed_counts"]["total"] == len(expected)


@pytest.mark.asyncio
async def test_success_moves_to_history_for_all_task_origins(api):
    jobs = api.plugin.jobs
    scope = api.test_space["space_id"]
    api.test_query.update(scope_key=scope, limit="1")

    async def visible():
        return [item["job_id"] for item in payload(await api.tasks())["items"]]

    first = await jobs.submit("extract", scope, {}, "auto1", hide_on_success=True)
    job = await jobs.claim("worker")
    await jobs.fail(job["job_id"], job["lease_token"], error_type="provider", error_code="timeout")
    assert await visible() == [first["job_id"]]
    await jobs.retry(first["job_id"])
    job = await jobs.claim("worker")
    await jobs.finish(job["job_id"], job["lease_token"])
    assert await visible() == []

    second = await jobs.submit("extract", scope, {}, "auto2", hide_on_success=True)
    job = await jobs.claim("worker")
    await jobs.fail(job["job_id"], job["lease_token"], error_type="provider", error_code="timeout",
                    retryable=True)
    assert await visible() == [second["job_id"]]
    await jobs.database.execute("UPDATE memory_jobs SET available_at=0 WHERE job_id=?", (second["job_id"],))
    job = await jobs.claim("worker")
    await jobs.finish(job["job_id"], job["lease_token"])
    manual = await jobs.submit("extract", scope, {}, "manual")
    assert await visible() == [manual["job_id"]]
    job = await jobs.claim("worker")
    await jobs.finish(job["job_id"], job["lease_token"])
    listing = payload(await api.tasks())
    assert not listing["items"] and not listing["has_more"]
    assert [item["job_id"] for item in listing["history"]] == [manual["job_id"], second["job_id"], first["job_id"]]
    assert listing["completed_counts"] == {"total": 3, "automatic_count": 1, "manual_count": 2}
    api.test_body = {}
    cleared = payload(await api.clear_completed())
    assert (cleared["deleted_count"], cleared["automatic_count"], cleared["manual_count"]) == (3, 1, 2)
    assert not payload(await api.tasks())["history"]
    assert (await jobs.submit("extract", scope, {}, "auto2", hide_on_success=True))["state"] == "queued"


@pytest.mark.asyncio
async def test_detail_retry_cancel_and_availability(api):
    submit_body(api)
    job = payload(await api.submit())
    api.test_query.update(space_id=api.test_space["space_id"])
    assert not payload(await api.availability())["available"]
    api.test_body = {"job_id": job["job_id"]}
    assert payload(await api.cancel())["state"] == "cancelled"
    assert payload(await api.cancel())["state"] == "cancelled"
    assert payload(await api.availability())["available"]
    api.test_body["fresh"] = "false"
    payload(await api.retry(), 400)
    api.test_body["fresh"] = True
    assert payload(await api.retry())["state"] == "queued"
    assert api.plugin.maintenance.retries == [(job["job_id"], True)]
    api.test_query["job_id"] = job["job_id"]
    result = payload(await api.task())
    assert "candidate" in result and result["restart_required"] is False
    api.test_query["job_id"] = "missing"
    payload(await api.task(), 404)


@pytest.mark.asyncio
async def test_unknown_cancel_and_extra_retry_fields_are_rejected(api):
    api.test_body = {"job_id": "missing"}
    payload(await api.cancel(), 404)
    api.test_body = {"job_id": "missing", "space_id": "injected"}
    payload(await api.retry(), 400)
    payload(await api.cancel(), 400)


@pytest.mark.asyncio
async def test_regenerate_window_mode_is_explicit_and_mutually_exclusive(api, monkeypatch):
    submit_body(api)
    job = payload(await api.submit())
    received = []

    async def retry(job_id, *, fresh=False, regenerate_window=False):
        received.append((job_id, fresh, regenerate_window))
        return await api.plugin.jobs.get(job_id)

    monkeypatch.setattr(api.plugin.maintenance, "retry", retry)
    for flags in ({"regenerate_window": "true"}, {"regenerate_window": 1},
                  {"regenerate_window": True, "fresh": True}):
        api.test_body = {"job_id": job["job_id"], **flags}
        payload(await api.retry(), 400)
    assert received == []
    api.test_body = {"job_id": job["job_id"], "regenerate_window": True}
    payload(await api.retry())
    assert received == [(job["job_id"], False, True)]


@pytest.mark.asyncio
@pytest.mark.parametrize(("kind", "code", "checkpoint"), [
    ("extract", "invalid_output", {"window": {"raw": "private rejected candidate"}}),
    ("short_term", "invalid_short_term_output", {"raw": "private rejected candidate"}),
    ("topic_rebuild", "invalid_topic_output", {"topic_window": {"raw": "private rejected candidate", "receipt_key": "k"}}),
])
async def test_task_list_exposes_regeneration_availability_without_raw_candidate(api, kind, code, checkpoint):
    job = await api.plugin.jobs.submit(kind, api.test_space["space_id"], {}, "failed-candidate")
    claimed = await api.plugin.jobs.claim("worker")
    await api.plugin.jobs.save_checkpoint(job["job_id"], claimed["lease_token"],
        checkpoint=checkpoint, step="validate", progress_done=0, progress_total=2)
    await api.plugin.jobs.fail(job["job_id"], claimed["lease_token"], error_type="ReviewRequired", error_code=code)
    listed = payload(await api.tasks())
    assert listed["items"][0]["regenerate_window_available"]
    assert listed["items"][0]["recovery_action"] == "regenerate"
    assert "private rejected candidate" not in json.dumps(listed)


@pytest.mark.asyncio
async def test_memory_listing_includes_all_states_only_in_active_generation(api):
    memory = api.plugin.memory_store
    old = await make_node(api)
    generation = await memory.create_generation(api.test_space["space_id"])
    await memory.activate_generation(api.test_space["space_id"], generation["generation_id"])
    for status in ("active", "historical", "disabled", "superseded", "deleted"):
        await make_node(api, status=status)
    api.test_query.update(space_id=api.test_space["space_id"])
    result = payload(await api.nodes())
    assert {node["status"] for node in result["items"]} == {"active", "historical", "disabled", "superseded", "deleted"}
    assert old["node_id"] not in {node["node_id"] for node in result["items"]}
    assert result["generation_id"] == generation["generation_id"]
    api.test_query.update(limit="2")
    assert len(payload(await api.nodes())["items"]) == 2
    assert payload(await api.nodes())["has_more"]
    api.test_query.update(node_id=old["node_id"])
    payload(await api.versions(), 409)


@pytest.mark.asyncio
async def test_node_sort_and_filters_apply_before_paging(api):
    values = [(100, .4, "trust"), (150, .6, "joy"), (199, .6, "trust"),
              (200, .6, "trust"), (None, .6, "trust"), (155, .9, None)]
    for observed, importance, tag in values:
        emotion = {"description": "test", "tags": [{"tag": tag, "intensity": .1}]} if tag else None
        if tag == "trust":
            emotion["tags"].append({"tag": "joy", "intensity": .8})
        node = await make_node(api, importance=importance, emotion=emotion)
        await api.plugin.memory_store.database.execute(
            "UPDATE memory_nodes SET content_updated_at=? WHERE node_id=?", (observed, node["node_id"]))
    api.test_query.update(space_id=api.test_space["space_id"], sort="importance_desc", updated_from="100",
                          updated_before="200", importance_min=".4", importance_max=".6",
                          emotion="trust", node_type="event", status="active", q="remembered", limit="1")
    first = payload(await api.nodes())
    assert first["total"] == 2 and first["has_more"] and first["items"][0]["content_updated_at"] == 199
    api.test_query["offset"] = "1"
    second = payload(await api.nodes())
    assert second["total"] == 2 and not second["has_more"] and second["items"][0]["content_updated_at"] == 100
    api.test_query.update(limit="50", offset="0", sort="importance_asc")
    assert [row["content_updated_at"] for row in payload(await api.nodes())["items"]] == [100, 199]
    api.test_query.update(sort="updated_asc", importance_min=".6", emotion="joy", updated_from="", updated_before="")
    result = payload(await api.nodes())
    assert [row["content_updated_at"] for row in result["items"]] == [150, 199, 200, None]
    api.test_query.update(emotion="none", importance_max="1")
    assert [row["content_updated_at"] for row in payload(await api.nodes())["items"]] == [155]
    for invalid in ({"sort": "invalid"}, {"emotion": "invalid"}, {"importance_min": "1.1"},
                    {"updated_from": "nan"}, {"updated_from": "200", "updated_before": "100"}):
        api.test_query.clear()
        api.test_query.update(space_id=api.test_space["space_id"], **invalid)
        payload(await api.nodes(), 400)


@pytest.mark.asyncio
async def test_versions_include_provenance_and_current_snapshot(api):
    node = await make_node(api)
    await api.plugin.memory_store.update_node_content(node["node_id"], "Updated event.")
    api.test_query["node_id"] = node["node_id"]
    result = payload(await api.versions())
    assert result["node"]["content_version"] == 2
    assert [version["version_no"] for version in result["items"]] == [2, 1]
    assert [source["source_id"] for source in result["sources"]] == ["source"]
    assert result["participants"] == result["edges"] == []
    assert not result["truncated"]


@pytest.mark.asyncio
async def test_nodes_and_versions_render_user_references_without_changing_governance_fields(api):
    memory = api.plugin.memory_store
    sid = api.test_space["space_id"]
    node = await memory.create_node(space_id=sid, node_type="event", source_ids=["source"],
                                    content_text="我与{{user:u}}一起练习。")
    updated = await memory.update_node_content(node["node_id"], "我记得{{user:u}}让我叫她‘小鹿’。")
    calls = []

    async def names(ids):
        assert not memory.database._lock.locked()
        calls.append(tuple(ids))
        return {"u": "明月"}

    api.plugin.source_store = SimpleNamespace(get_user_display_names=names)
    api.test_query.update(space_id=sid, node_id=node["node_id"])
    listing = payload(await api.nodes())
    assert listing["items"][0]["content_text"] == updated["content_text"]
    assert listing["items"][0]["display_content_text"] == "我记得明月让我叫她‘小鹿’。"
    versions = payload(await api.versions())
    assert calls == [("u",), ("u",)]
    assert versions["node"]["content_text"] == updated["content_text"]
    assert versions["node"]["content_version"] == 2
    assert [item["version_no"] for item in versions["items"]] == [2, 1]
    assert [item["content_text"] for item in versions["items"]] == [updated["content_text"], node["content_text"]]
    assert [item["display_content_text"] for item in versions["items"]] == [
        "我记得明月让我叫她‘小鹿’。", "我与明月一起练习。"]
    assert versions["sources"][0]["source_id"] == "source"


@pytest.mark.asyncio
async def test_spaces_list_is_paginated(api):
    await api.plugin.memory_store.get_or_create_space(private_space("p", "another"))
    api.test_query.update(limit="1")
    first = payload(await api.spaces())
    assert len(first["items"]) == 1 and first["has_more"]
    assert first["items"][0]["display_label"] == "p · 未命名用户"
    api.test_query["offset"] = "1"
    second = payload(await api.spaces())
    assert len(second["items"]) == 1 and not second["has_more"]
    assert second["items"][0]["space_id"] != first["items"][0]["space_id"]


@pytest.mark.asyncio
async def test_space_labels_use_user_or_group_name_across_adapter_instances(api, tmp_path):
    sources = SourceStore(tmp_path / "source.db")
    api.plugin.source_store = sources
    await sources.open()
    try:
        for platform, account in (("adapter-a", "a1"), ("adapter-a", "a2"), ("adapter-b", "b")):
            await sources.bind_account(platform_instance_id=platform, account_id=account,
                                       logical_user_id="u", display_name="小溪")
        memory = api.plugin.memory_store
        first = await memory.get_or_create_space(group_space("p", "test-platform", "g"))
        second = await memory.get_or_create_space(group_space("p", "other-platform", "g"))

        async def labels():
            return {row["space_id"]: row["display_label"] for row in payload(await api.spaces())["items"]}

        initial = await labels()
        assert first["space_id"] != second["space_id"]
        assert initial[first["space_id"]] == initial[second["space_id"]] == "p · 群 g"
        for timestamp, name in ((100, "旧群名"), (110, "读书会"), (105, "迟到的旧名称"), (120, "")):
            event = FakeEvent(message_id=str(timestamp), text="你好", group_id="g")
            event.message_obj.group = SimpleNamespace(group_id="g", group_name=name)
            await sources.capture(normalize_event(event, observed_at=timestamp))
        await sources.close()
        await sources.open()
        refreshed = await labels()
        assert refreshed[first["space_id"]] == "p · 群 读书会"
        assert refreshed[second["space_id"]] == "p · 群 g"
        assert refreshed[api.test_space["space_id"]] == "p · 小溪"
    finally:
        await sources.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("query", [{"limit": "101"}, {"limit": "nan"}, {"limit": True}, {"offset": "-1"}, {"offset": "1.5"}])
async def test_pagination_is_bounded_and_strict(api, query):
    api.test_query.update(space_id=api.test_space["space_id"], **query)
    for endpoint in (api.nodes, api.spaces, api.tasks):
        payload(await endpoint(), 400)


@pytest.mark.asyncio
async def test_internal_errors_are_safe_and_cancellation_propagates(api, monkeypatch):
    async def fail(*args, **kwargs):
        raise RuntimeError("Authorization: Bearer do-not-expose")

    monkeypatch.setattr(api.plugin.maintenance, "enqueue", fail)
    submit_body(api)
    result = payload(await api.submit(), 500)
    assert "do-not-expose" not in json.dumps(result)
    assert result["data"]["error_code"] == "maintenance_request_failed"

    async def cancel(*args, **kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(api.plugin.maintenance, "enqueue", cancel)
    with pytest.raises(asyncio.CancelledError):
        await api.submit()


@pytest.mark.asyncio
async def test_task_deletion_api_requires_stopped_task(api):
    submit_body(api)
    task = payload(await api.submit())
    api.test_body = {"job_id": task["job_id"]}
    running = await api.plugin.jobs.claim("worker")
    payload(await api.delete(), 409)
    assert await api.plugin.jobs.get(running["job_id"])
    await api.plugin.jobs.cancel(running["job_id"])
    assert payload(await api.delete())["deleted"] is True
    payload(await api.delete(), 404)
    api.test_body = {"job_id": "missing", "unexpected": True}
    payload(await api.delete(), 400)


@pytest.mark.asyncio
async def test_relations_endpoint_lists_topic_members_in_requested_space(api):
    event = await make_node(api)
    memory = api.plugin.memory_store
    topic = await memory.create_node(space_id=api.test_space["space_id"], node_type="topic",
                                    content_text="A continuing undertaking.", source_ids=["source"])
    await memory.add_edge(topic["node_id"], event["node_id"], "contains")
    api.test_query.update(space_id=api.test_space["space_id"], node_id=topic["node_id"], limit="1", offset="0")
    result = payload(await api.relations())
    assert result["relation"] == "members"
    assert [item["node_id"] for item in result["items"]] == [event["node_id"]]
    other = await memory.get_or_create_space(private_space("other", "other"))
    api.test_query["space_id"] = other["space_id"]
    payload(await api.relations(), 404)


@pytest.mark.asyncio
async def test_clear_completed_api_scope_and_all_spaces(api):
    jobs = api.plugin.jobs
    main_scope = api.test_space['space_id']
    for scope, key in ((main_scope, 'same-space'), ('another-space', 'elsewhere')):
        job = await jobs.submit('model_test', scope, {}, key, hide_on_success=scope == main_scope)
        claimed = await jobs.claim('worker')
        await jobs.finish(job['job_id'], claimed['lease_token'], result={})
    queued = await jobs.submit('extract', main_scope, {}, 'unfinished')
    api.test_body = {'scope_key': main_scope}
    assert payload(await api.clear_completed()) == {'deleted_count': 1, 'automatic_count': 1, 'manual_count': 0, 'removed_node_count': 0}
    assert payload(await api.clear_completed()) == {'deleted_count': 0, 'automatic_count': 0, 'manual_count': 0, 'removed_node_count': 0}
    api.test_body = {}
    assert payload(await api.clear_completed()) == {'deleted_count': 1, 'automatic_count': 0, 'manual_count': 1, 'removed_node_count': 0}
    assert (await jobs.get(queued['job_id']))['state'] == 'queued'


@pytest.mark.asyncio
@pytest.mark.parametrize('body', [{'scope_key': ''}, {'scope_key': None}, {'scope_key': []}, {'scope_key': 's', 'state': 'failed'}])
async def test_clear_completed_api_uses_existing_scope_contract(api, body):
    api.test_body = body
    payload(await api.clear_completed(), 400)


@pytest.mark.asyncio
async def test_space_cleanup_requires_exact_space_id_confirmation(api):
    api.test_body = {"space_id": api.test_space["space_id"], "projects": ["sources"], "confirmation": "wrong"}
    payload(await api.clear_space(), 400)
    api.test_body = {"space_id": api.test_space["space_id"], "confirmation": "wrong"}
    payload(await api.delete_space(), 400)

@pytest.mark.asyncio
@pytest.mark.parametrize(("sections", "kind", "topics"), [
    (["topics"], "topic_rebuild", False), (["profiles"], "profile_rebuild", False),
    (["relationship"], "relationship_rebuild", False), (["profiles", "relationship"], "context_rebuild", False),
    (["topics", "profiles"], "profile_rebuild", True), (["relationship", "topics"], "relationship_rebuild", True),
    (["topics", "relationship", "profiles"], "context_rebuild", True),
])
async def test_section_rebuild_routes_selected_work(api, sections, kind, topics):
    submit_body(api, kind="section_rebuild", sections=sections)
    result = payload(await api.submit())
    job = await api.plugin.jobs.get(result["job_id"])
    assert job["kind"] == kind and job["payload"]["include_topics"] is topics
    if topics:
        assert set(result["section_progress"]) == {"topics", "context"}

@pytest.mark.asyncio
async def test_recall_console_previews_and_scopes_optional_recording(api):
    history = RecallHistory(api.plugin.memory_store.database)
    space = api.test_space["space_id"]
    async def preview(space_id, query):
        assert space_id == space
        return {"mode": "test", "query": query}

    api.plugin.runtime = SimpleNamespace(recall_history=history, test_recall=preview)
    assert payload(await api.recall_history()) == {"enabled": False, "records": []}
    api.test_body = {"space_id": space, "query": " 观星 "}
    assert payload(await api.test_recall()) == {"mode": "test", "query": "观星"}
    api.test_body["space_id"] = "unknown"
    payload(await api.test_recall(), 404)
    api.test_body = {"space_id": space, "query": " "}
    payload(await api.test_recall(), 400)
    api.test_body = {"enabled": "false"}
    payload(await api.recall_recording(), 400)
    api.test_body = {"enabled": True}
    assert payload(await api.recall_recording()) == {"enabled": True}
    ticket = history.ticket()
    for i in range(51):
        await history.append(ticket, space, {"query": str(i)})
    other = await api.plugin.memory_store.get_or_create_space(private_space("other", "u"))
    await history.append(ticket, other["space_id"], {"query": "other"})
    assert payload(await api.recall_history()) == {"enabled": True, "records": []}
    api.test_query["space_id"] = space
    assert payload(await api.recall_history())["records"] == [{"query": str(i)} for i in range(50, 1, -1)]
    api.test_body = {"enabled": False}
    assert payload(await api.recall_recording()) == {"enabled": False}
    api.test_body = {"enabled": True}
    await api.recall_recording()
    await history.append(ticket, space, {"query": "from old session"})
    assert payload(await api.recall_history()) == {"enabled": True, "records": []}


@pytest.mark.asyncio
async def test_topic_rebuild_preview_counts_current_space(api):
    space = api.test_space['space_id']
    await make_node(api)
    await make_node(api, manual_protected=True)
    await make_node(api, status='historical')
    await api.plugin.memory_store.create_node(space_id=space, node_type='topic',
        content_text='独立合成主题', source_ids=['source'], manual_protected=True)
    api.test_query['space_id'] = space
    assert payload(await api.topic_rebuild_preview()) == {
        'eligible_members': 1, 'protected_members': 1, 'old_topics': 1}
