"""Pause diagnostics and short-summary candidates use real durable job state."""

import json
from types import SimpleNamespace

import pytest

from core.config import NeoMemoryConfig
from core.maintenance import MaintenanceEngine
from core.short_term_prompts import PROMPT_VERSION, SCHEMA_VERSION
from tests.test_maintenance_engine import enqueue_and_claim, execute
from tests.test_maintenance_engine import environment as _environment
from tests.test_short_term import claim
from tests.test_short_term import env as _short_env

environment = _environment
short_env = _short_env


@pytest.mark.asyncio
async def test_failed_window_pauses_automatic_updates_without_occupying_a_job_lock(environment):
    env = environment
    env.adapter.output = "invalid-json"
    job = await enqueue_and_claim(env)
    assert (await execute(env, job))["state"] == "failed"
    state = await env.engine.automatic_status(env.space["space_id"])
    assert state["state"] == "paused" and state["reason"] == "failed_window"
    assert [item["job_id"] for item in state["blocking_jobs"]] == [job["job_id"]]
    assert "payload" not in state["blocking_jobs"][0]
    assert (await env.engine.jobs.availability(env.space["space_id"]))["available"]
    await env.engine.jobs.cancel(job["job_id"])
    assert (await env.engine.automatic_status(env.space["space_id"]))["state"] == "enabled"


@pytest.mark.asyncio
async def test_failed_short_term_does_not_pause_automatic_long_term(environment):
    env = environment
    generation = await env.memory.get_active_generation(env.space["space_id"])
    await env.engine.jobs.submit("short_term", env.space["space_id"],
        {"generation_id": generation["generation_id"]}, "failed-summary")
    job = await env.engine.jobs.claim("worker")
    await env.engine.jobs.fail(job["job_id"], job["lease_token"],
        error_type="ReviewRequired", error_code="invalid_short_term_output")
    assert (await env.engine.automatic_status(env.space["space_id"]))["state"] == "enabled"


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [False, True])
async def test_short_term_detail_exposes_candidate_and_same_window_regeneration(short_env, invalid):
    env = short_env
    job = await claim(env)
    candidate = {"schema_version": SCHEMA_VERSION, "summary_text": "用户正在准备面试。",
                 "evidence_source_ids": job["payload"]["source_ids"][:1]}
    raw = "invalid-json" if invalid else json.dumps(candidate)
    await env.jobs.save_checkpoint(job["job_id"], job["lease_token"],
        checkpoint={"raw": raw, "signature": "private-signature", "prompt_version": PROMPT_VERSION, "user_ids": []}, step="short_term_validate",
        progress_done=0, progress_total=len(job["payload"]["source_ids"]))
    await env.jobs.fail(job["job_id"], job["lease_token"],
        error_type="ReviewRequired", error_code="invalid_short_term_output")
    engine = MaintenanceEngine(env.sources, env.memory,
        SimpleNamespace(adapter=env.adapter), NeoMemoryConfig(), lambda _: "unused")
    detail = await engine.describe(job["job_id"])
    assert not detail["restart_required"]
    assert detail["regenerate_window_available"]
    assert detail["source_ids"] == job["payload"]["source_ids"]
    assert detail["candidate"] == (None if invalid else candidate)
    assert bool(detail.get("candidate_format_invalid")) == invalid
    assert "raw" not in detail and "checkpoint" not in detail and "lease_token" not in detail
