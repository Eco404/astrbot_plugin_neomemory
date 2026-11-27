import json

import pytest

from core.node_reconstruction import ReconstructionUnavailable
from tests.test_maintenance_engine import environment, enqueue_and_claim, execute, node, output


async def draft(env, target, body, *, key="draft", selection=None):
    env.adapter.output = json.dumps({"content": body})
    task = await enqueue_and_claim(env, "node_draft", key, operation={"node_id": target["node_id"]},
                                   model_selection=selection)
    result = await execute(env, task)
    assert result["state"] == "succeeded", result["error_code"]
    assert result["result"]["protocol"] == "node-reconstruction.v1"
    return result


async def apply_draft(env, preview, content):
    task = await enqueue_and_claim(env, "node_apply", "apply",
                                   operation={"draft_id": preview["job_id"], "content": content})
    result = await execute(env, task)
    assert result["state"] == "succeeded", result["error_code"]
    return result


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["event", "atomic"])
async def test_single_node_reconstruction_preserves_identity_and_extraction_cursor(environment, kind):
    env = environment
    env.adapter.output = json.dumps(output([node("s1", kind=kind)]))
    original_job = await execute(env, await enqueue_and_claim(env))
    assert original_job["state"] == "succeeded"
    sid = env.space["space_id"]
    original, = await env.memory.list_nodes(space_id=sid)
    parent = await env.memory.create_node(space_id=sid, source_ids=[env.records[0].source_id], node_type="topic", content_text="原有概括。")
    await env.memory.add_edge(parent["node_id"], original["node_id"], "contains")
    if kind == "atomic":
        await env.memory.set_manual_protection(original["node_id"], True)
    before = await env.memory.get_active_generation(sid)
    preview = await draft(env, original, "已收到配件。")
    assert preview["result"]["source_count"] == 1
    assert (await env.memory.get_node(original["node_id"]))["content_text"] == original["content_text"]
    result = await apply_draft(env, preview, "确认已收到配件。")
    stored = await env.memory.get_node(original["node_id"])
    assert (stored["content_version"], stored["content_text"], stored["manual_protected"]) == (2, "确认已收到配件。", kind == "atomic")
    assert stored["content_updated_at"] == original["content_updated_at"]
    assert result["result"]["stale_parent_ids"] == [parent["node_id"]]
    assert before["activity_round"] == (await env.memory.get_active_generation(sid))["activity_round"]
    assert len(await env.memory.database.fetchall("select * from extraction_batches")) == 1


@pytest.mark.asyncio
async def test_topic_rebuild_fixes_only_text_and_refuses_changed_members(environment):
    env = environment
    sid = env.space["space_id"]
    topic = await env.memory.create_node(space_id=sid, source_ids=[env.records[0].source_id], node_type="topic", content_text="原概括。")
    a = await env.memory.create_node(space_id=sid, source_ids=[env.records[0].source_id], node_type="event", content_text="书已借出。")
    b = await env.memory.create_node(space_id=sid, source_ids=[env.records[0].source_id], node_type="atomic", content_text="周末归还。")
    for member in (a, b):
        await env.memory.add_edge(topic["node_id"], member["node_id"], "contains")
    preview = await draft(env, topic, "这本书已借出，约好周末归还。", selection={"topic_provider_id": "topic-model"})
    assert preview["result"]["model_route"]["provider_id"] == "topic-model"
    material = json.loads(env.adapter.calls[-1][1]["content"])
    assert {m["text"] for m in material["members"]} == {a["content_text"], b["content_text"]}
    await env.memory.update_node_content(a["node_id"], "书还未借出。")
    with pytest.raises(ReconstructionUnavailable):
        await apply_draft(env, preview, preview["result"]["content"])
    preview = await draft(env, topic, "这本书尚未借出，原先计划周末归还。", key="again")
    await apply_draft(env, preview, preview["result"]["content"])
    deps = await env.memory.database.fetchall("select member_id,member_version from topic_dependencies where topic_id=?", (topic["node_id"],))
    assert {r["member_id"]: r["member_version"] for r in deps} == {a["node_id"]: 2, b["node_id"]: 1}
    assert len(await env.memory.database.fetchall("select * from memory_edges where source_node_id=?", (topic["node_id"],))) == 2


@pytest.mark.asyncio
async def test_retry_after_committed_body_only_indexes_existing_version(environment, monkeypatch):
    from types import SimpleNamespace
    from core.jobs import ReviewRequired
    env = environment
    sid = env.space["space_id"]
    target = await env.memory.create_node(space_id=sid, node_type="event", content_text="维修计划等配件。",
                                          source_ids=[env.records[0].source_id])
    preview = await draft(env, target, "配件已经寄出。")
    env.runtime.recall = SimpleNamespace(embeddings=object())
    indexed = []

    async def index(embeddings, nodes, space, **kwargs):
        indexed.append((nodes[0]["node_id"], nodes[0]["content_version"]))
        if len(indexed) == 1:
            raise ValueError("unavailable")

    monkeypatch.setattr("core.maintenance.node_vectors", index)
    monkeypatch.setattr(env.engine, "_embedding_failure", lambda exc: ReviewRequired("index_provider_required"))
    task = await enqueue_and_claim(env, "node_apply", "apply", operation={
        "draft_id": preview["job_id"], "content": preview["result"]["content"]})
    assert (await execute(env, task))["state"] == "failed"
    assert (await env.memory.get_node(target["node_id"]))["content_version"] == 2
    await env.engine.retry(task["job_id"])
    result = await execute(env, await env.engine.jobs.claim("retry"))
    assert result["state"] == "succeeded" and result["result"]["replayed"]
    assert indexed == [(target["node_id"], 2)] * 2
    assert len(env.adapter.calls) == 1


@pytest.mark.asyncio
async def test_updated_event_uses_old_and_new_sources_without_consuming_window(environment):
    from tests.test_extraction_apply import capture
    env = environment
    sid = env.space["space_id"]
    env.adapter.output = json.dumps(output([node("s1", content="周末打算借书。")]))
    assert (await execute(env, await enqueue_and_claim(env)))["state"] == "succeeded"
    original, = await env.memory.list_nodes(space_id=sid)
    third = await capture(env.sources, number=3)
    await capture(env.sources, number=4)
    env.adapter.output = json.dumps(output([node("s1", old=original, content="约好周末借书，已取到。")]))
    assert (await execute(env, await enqueue_and_claim(env, key="second")))["state"] == "succeeded"
    preview = await draft(env, original, "原来约好周末借书，现在书已取到。")
    snap = preview["result"]["snapshot"]
    assert set(snap["sources"]) == {env.records[0].source_id, third.source_id}
    assert len(snap["context_sources"]) == 2
    data = json.loads(env.adapter.calls[-1][1]["content"])
    assert [r["evidence"] for r in data["sources"]] == ["direct", "context", "direct", "context"]
    assert len(await env.memory.database.fetchall("select * from extraction_batches")) == 2


@pytest.mark.asyncio
async def test_discarded_draft_survives_as_task_but_cannot_be_applied(environment):
    env = environment
    sid = env.space["space_id"]
    target = await env.memory.create_node(space_id=sid, node_type="atomic", content_text="已登记。",
                                          source_ids=[env.records[0].source_id])
    preview = await draft(env, target, "借阅已登记。")
    await env.engine.jobs.discard_node_draft(sid, preview["job_id"])
    saved = await env.engine.jobs.get(preview["job_id"])
    assert saved["result"] == {"discarded": True}
    with pytest.raises(ReconstructionUnavailable, match="放弃"):
        await apply_draft(env, preview, "新正文。")
    assert (await env.memory.get_node(target["node_id"]))["content_version"] == 1


@pytest.mark.asyncio
async def test_topic_members_rechecked_inside_apply_transaction(environment):
    env = environment
    sid = env.space["space_id"]
    topic, a, b = [await env.memory.create_node(space_id=sid, source_ids=[env.records[0].source_id],
                        node_type=kind, content_text=text) for kind, text in (
                            ("topic", "原概括。"), ("atomic", "周末归还。"), ("event", "已经到馆。"))]
    await env.memory.add_edge(topic["node_id"], a["node_id"], "contains")
    preview = await draft(env, topic, "周末要还书。")
    task = await enqueue_and_claim(env, "node_apply", "accepted", operation={
        "draft_id": preview["job_id"], "content": preview["result"]["content"]})
    await env.memory.add_edge(topic["node_id"], b["node_id"], "contains")
    failed = await execute(env, task)
    assert failed["error_code"] == "reconstruction_changed"
    assert (await env.memory.get_node(topic["node_id"]))["content_version"] == 1
