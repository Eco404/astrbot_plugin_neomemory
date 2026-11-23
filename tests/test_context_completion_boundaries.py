"""Short-term source evidence and passive recall deadlines under contention."""

import asyncio
import json
import sqlite3
import time

import pytest

pytest.importorskip("astrbot.api")

from astrbot.api.provider import ProviderRequest  # noqa: E402

from core.short_term_prompts import SCHEMA_VERSION  # noqa: E402
from tests.test_recall_runtime import RuntimeEvent, seed  # noqa: E402
from tests.test_recall_runtime import plugin_factory as _plugin_factory  # noqa: E402
from tests.test_short_term import claim, current, execute  # noqa: E402
from tests.test_short_term import env as _short_env  # noqa: E402

plugin_factory = _plugin_factory
short_env = _short_env


@pytest.mark.asyncio
async def test_first_nonempty_summary_cannot_claim_no_source_evidence(short_env):
    env = short_env
    env.adapter.output = json.dumps(
        {
            "schema_version": SCHEMA_VERSION,
            "summary_text": "用户已经确定明天完成付款。",
            "evidence_source_ids": [],
        }
    )
    result = await execute(env, await claim(env))
    assert result["state"] == "failed"
    assert result["error_code"] == "invalid_short_term_output"
    assert await current(env) is None


@pytest.mark.asyncio
async def test_existing_space_passive_deadline_does_not_wait_for_unrelated_sqlite_writer(plugin_factory):
    plugin = await plugin_factory(
        {
            "embedding_provider_id": "",
            "automatic_extraction_enabled": False,
            "short_term_enabled": False,
            "recall_deadline_seconds": 0.1,
        }
    )
    event = RuntimeEvent()
    space, _ = await seed(plugin, event)
    await plugin.memory_store.upsert_profile(
        space_id=space["space_id"],
        logical_user_id=space["logical_user_id"],
        tags=[],
        description="用户喜欢绘画。",
    )
    await plugin.maintenance.stop()
    writer = sqlite3.connect(plugin.memory_store.path)
    writer.execute("BEGIN IMMEDIATE")

    async def release_external_writer():
        await asyncio.sleep(0.45)
        writer.rollback()

    release = asyncio.create_task(release_external_writer())
    try:
        request = ProviderRequest()
        started = time.monotonic()
        await plugin.inject_memory(event, request)
        elapsed = time.monotonic() - started
        assert elapsed < 0.25, f"0.1s deadline took {elapsed:.3f}s under an unrelated SQLite write lock"
        assert "用户喜欢绘画" in request.extra_user_content_parts[0].text
    finally:
        await release
        writer.close()
