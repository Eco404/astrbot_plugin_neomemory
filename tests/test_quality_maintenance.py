"""Quality candidates use the same durable jobs, review and explicit retry path."""
import json

import pytest

from tests.test_maintenance_engine import environment, enqueue_and_claim, execute


@pytest.mark.asyncio
async def test_manual_reject_uses_prepared_candidate_schema_without_model(environment):
    env = environment
    env.adapter.output = "invalid JSON"
    failed = await execute(env, await enqueue_and_claim(env))
    completed = await execute(env, await enqueue_and_claim(env, "reject", key="manual-skip", failed_job_id=failed["job_id"]))
    assert completed["state"] == "succeeded" and len(env.adapter.calls) == 1
    receipt = (await env.memory.database.fetchall("SELECT * FROM extraction_batches"))[0]
    assert receipt["schema_version"] == "extraction.v1" and receipt["model_id"] == "manual-rejection"
    assert (await env.engine.jobs.get(failed["job_id"]))["state"] == "cancelled"
