"""Restored recovery descriptors keep their saved feedback under a fixed first request."""

import asyncio
from dataclasses import replace
import json

import pytest

from core.candidate_feedback import recovery_messages
from tests.test_maintenance_engine import environment, enqueue_and_claim, execute, restart
from tests.test_pipeline_candidate_recovery import summary
from tests.test_short_term import env, claim, execute as execute_summary, restart as restart_summary
from tests.test_topic_maintenance import seed, candidate




@pytest.mark.asyncio
async def test_restored_topic_descriptor_sends_original_protocol_and_errors(environment):
    e = environment
    e.engine.config = replace(e.config, extraction_auto_regeneration=True)
    valid = candidate(await seed(e))
    bad = json.loads(valid)
    bad['changes'][0]['title'] = '字' * 61
    raw = json.dumps(bad)
    e.adapter.output = raw

    async def interrupt(messages):
        if len(e.adapter.calls) == 2:
            raise asyncio.CancelledError()

    e.adapter.on_generate = interrupt
    job = await enqueue_and_claim(e, 'topic_consolidate')
    with pytest.raises(asyncio.CancelledError):
        await execute(e, job)
    saved = await e.engine.jobs.get(job['job_id'])
    checkpoint = saved['checkpoint']
    window = checkpoint['topic_window']
    errors = window['errors']
    assert window['regeneration']['version'] == 'topic-regeneration.v2'
    expected = recovery_messages(e.adapter.calls[0], raw, errors, 'topic-regeneration.v2')
    e.adapter.on_generate = None
    e.adapter.output = valid
    restart(e)
    result = await execute(e, await e.engine.jobs.claim('restored-topic'))
    assert result['state'] == 'succeeded', result
    assert e.adapter.calls[-1] == expected
    assert len(await e.memory.database.fetchall('SELECT * FROM topic_receipts')) == 1
    assert len(await e.engine.jobs.list_candidates(job['job_id'])) == 1


@pytest.mark.asyncio
async def test_restored_summary_descriptor_sends_original_protocol_and_errors(env):
    e = env
    job = await claim(e)
    valid = summary(job)
    bad = {**valid, 'summary_text': '字' * 401}
    raw = json.dumps(bad)
    e.adapter.output = raw

    async def interrupt():
        if len(e.adapter.calls) == 2:
            raise asyncio.CancelledError()

    e.adapter.hook = interrupt
    with pytest.raises(asyncio.CancelledError):
        await execute_summary(e, job)
    saved = await e.jobs.get(job['job_id'])
    checkpoint = saved['checkpoint']
    errors = checkpoint['errors']
    assert checkpoint['regeneration']['version'] == 'short-term-regeneration.v1'
    expected = recovery_messages(e.adapter.calls[0], raw, errors, 'short-term-regeneration.v1')
    e.adapter.hook = None
    e.adapter.output = json.dumps(valid)
    restart_summary(e)
    result = await execute_summary(e, await e.jobs.claim('restored-summary'))
    assert result['state'] == 'succeeded', result
    assert e.adapter.calls[-1] == expected
    assert len(await e.memory.database.fetchall('SELECT * FROM short_term_receipts')) == 1
    assert len(await e.jobs.list_candidates(job['job_id'])) == 1
