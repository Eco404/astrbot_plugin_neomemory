"""话题切点经自动维护提交后，只消耗旧话题前缀的原始记录。"""

import time
from dataclasses import replace
from types import SimpleNamespace

import pytest

import core.maintenance as maintenance_module
from core.memory_spaces import private_space
from tests.test_extraction_apply import capture
from tests.test_maintenance_engine import environment, execute, restart
from tests.test_topic_windows import CachedOnly, TEXT


@pytest.mark.asyncio
async def test_automatic_topic_window_leaves_new_suffix_unconsumed(environment, monkeypatch):
    env = environment
    records = [await capture(env.sources, TEXT, number=number, user="topic-test") for number in range(10, 17)]
    space = await env.memory.get_or_create_space(private_space("p", records[0].logical_user_id))
    config = replace(env.config, private_min_messages=3, private_topic_messages=5,
                     private_max_messages=12, private_idle_seconds=900)
    embeddings = CachedOnly([[1, 0]] * 4 + [[0, 1]] * 3)
    embeddings.fingerprint = "synthetic-topic-only"
    env.runtime.recall = SimpleNamespace(embeddings=embeddings)
    restart(env, config=config)
    # 当前时间等于合成记录尾部，明确以话题而非空闲触发。
    monkeypatch.setattr(maintenance_module, "time", SimpleNamespace(time=lambda: 1600, monotonic=time.monotonic))
    await env.engine.scan_once()
    job = await env.engine.jobs.claim("topic-worker")
    assert job["scope_key"] == space["space_id"]
    assert job["payload"]["source_ids"] == [record.source_id for record in records[:4]]
    result = await execute(env, job)
    assert result["state"] == "succeeded", result["error_code"]
    generation = await env.memory.get_active_generation(space["space_id"])
    consumed = await env.engine.pipeline.consumed(generation["generation_id"], [record.source_id for record in records])
    assert consumed == {record.source_id for record in records[:4]}
    pending = await env.engine._pending(space, generation["generation_id"])
    assert [record["source_id"] for record in pending] == [record.source_id for record in records[4:]]
    assert generation["activity_round"] == 1
    assert len(env.adapter.calls) == 1
