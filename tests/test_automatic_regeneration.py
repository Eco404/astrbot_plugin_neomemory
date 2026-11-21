"""Durable bounded candidate corrections per fixed window, without replaying committed work."""
import asyncio
import json
from dataclasses import replace

import pytest

from core.extraction_errors import DuplicateJSONKey
from core.extraction_regeneration import validation_feedback
from core.extraction_schema import parse_extraction_output
from tests.test_maintenance_engine import environment, enqueue_and_claim, execute, node, output, restart


def enable(env):
    env.config = replace(env.config, extraction_auto_regeneration=True, extraction_max_regenerations=1)
    env.engine.config = env.config


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['syntax', 'pattern', 'enum'])
async def test_auto_correction_commits_once_with_original_input(environment, fault):
    env = environment
    enable(env)
    bad = output([node('s1')])
    if fault == 'pattern':
        bad['node_changes'][0]['local_ref'] = 'local_传感器'
    if fault == 'enum':
        bad['node_changes'][0]['emotion'] = {'tags': ['invalid'], 'intensity': .2, 'description': '担心。'}
    env.adapter.output = '{"node_changes",[]}' if fault == 'syntax' else json.dumps(bad)

    async def respond(messages):
        if len(env.adapter.calls) == 2:
            assert messages[:2] == env.adapter.calls[0]
            assert len(messages) == 4
            assert json.loads(messages[-1]['content'])['errors']
            env.adapter.output = json.dumps(output([node('s1')]))
    env.adapter.on_generate = respond
    completed = await execute(env, await enqueue_and_claim(env))
    assert completed['state'] == 'succeeded'
    assert len(env.adapter.calls) == 2
    assert len(await env.engine.jobs.list_candidates(completed['job_id'])) == 1
    assert (await env.memory.get_active_generation(env.space['space_id']))['activity_round'] == 1


@pytest.mark.asyncio
async def test_failed_correction_and_manual_retry_do_not_reset_budget(environment):
    env = environment
    enable(env)
    env.adapter.output = '{"x",1}'
    failed = await execute(env, await enqueue_and_claim(env))
    assert failed['state'] == 'failed'
    assert failed['checkpoint']['window']['correction_attempt'] == 1
    assert len(env.adapter.calls) == 2
    await env.engine.retry(failed['job_id'], regenerate_window=True)
    failed = await execute(env, await env.engine.jobs.claim('manual'))
    assert failed['state'] == 'failed'
    assert len(env.adapter.calls) == 3
    assert failed['checkpoint']['window']['correction_attempt'] == 1


@pytest.mark.asyncio
async def test_restart_after_correction_reservation_keeps_budget(environment):
    env = environment
    enable(env)
    env.adapter.output = '{"x",1}'

    async def interrupt(messages):
        if len(env.adapter.calls) == 2:
            raise asyncio.CancelledError()
    env.adapter.on_generate = interrupt
    job = await enqueue_and_claim(env)
    with pytest.raises(asyncio.CancelledError):
        await execute(env, job)
    saved = await env.engine.jobs.get(job['job_id'])
    assert saved['checkpoint']['window']['correction_attempt'] == 1
    restart(env)
    env.adapter.on_generate = None
    failed = await execute(env, await env.engine.jobs.claim('restart'))
    assert failed['state'] == 'failed'
    assert len(env.adapter.calls) == 3


@pytest.mark.asyncio
async def test_unknown_reference_stays_rejected_after_bounded_correction(environment):
    env = environment
    enable(env)
    env.adapter.output = json.dumps(output([node('missing-source')]))
    failed = await execute(env, await enqueue_and_claim(env))
    assert failed['state'] == 'failed'
    assert len(env.adapter.calls) == 2
    assert len(await env.engine.jobs.list_candidates(failed['job_id'])) == 1


def test_duplicate_key_feedback_identifies_known_field_without_overwriting():
    raw = '{"node_changes":[],"node_changes":[]}'
    with pytest.raises(DuplicateJSONKey):
        parse_extraction_output(raw)
    archived = {'window': {'raw': raw}, 'error_code': 'invalid_output'}
    assert validation_feedback(archived)['errors'][0]['field'] == 'node_changes'
    assert validation_feedback(archived)['errors'][0]['type'] == 'duplicate_json_key'


@pytest.mark.asyncio
@pytest.mark.parametrize('diagnostic', [{'response_status': 'incomplete', 'incomplete_reason': 'max_output_tokens'}, {'finish_reason': 'length'}])
async def test_incomplete_response_is_retained_without_format_retry(environment, diagnostic):
    env = environment
    enable(env)
    original = env.adapter.generate

    async def generate(messages, **kwargs):
        kwargs['diagnostics'].update(diagnostic)
        return await original(messages, **kwargs)
    env.adapter.generate = generate
    failed = await execute(env, await enqueue_and_claim(env))
    assert failed['state'] == 'failed' and failed['error_code'] == 'output_incomplete'
    assert len(env.adapter.calls) == 1
    assert failed['checkpoint']['window']['response_diagnostics'] == diagnostic
    assert not await env.memory.database.fetchall('SELECT * FROM extraction_batches')
    assert (await env.engine.describe(failed['job_id']))['regenerate_window_available']


@pytest.mark.asyncio
async def test_transport_failure_during_correction_preserves_reserved_budget(environment):
    env = environment
    enable(env)
    env.adapter.output = '{"broken",1}'

    async def fail_second_call(messages):
        if len(env.adapter.calls) == 2:
            raise ConnectionError('synthetic transport outage')
    env.adapter.on_generate = fail_second_call
    waiting = await execute(env, await enqueue_and_claim(env))
    assert waiting['state'] == 'retry_wait'
    assert waiting['checkpoint']['window']['correction_attempt'] == 1
    await env.memory.database.execute('UPDATE memory_jobs SET available_at=0 WHERE job_id=?', (waiting['job_id'],))
    failed = await execute(env, await env.engine.jobs.claim('retry'))
    assert failed['state'] == 'failed' and failed['error_code'] == 'invalid_output'
    assert len(env.adapter.calls) == 3
    assert env.adapter.calls[1] == env.adapter.calls[2]


@pytest.mark.asyncio
async def test_auto_correction_of_later_rebuild_window_does_not_replay_commits(environment):
    from tests.test_extraction_apply import capture
    env = environment
    enable(env)
    for number in range(3, 7):
        await capture(env.sources, number=number)

    async def respond(messages):
        if len(env.adapter.calls) == 2:
            env.adapter.output = 'invalid second window'
        else:
            env.adapter.output = json.dumps(output())
    env.adapter.on_generate = respond
    completed = await execute(env, await enqueue_and_claim(env, 'rebuild'))
    assert completed['state'] == 'succeeded'
    assert completed['result']['source_count'] == 6
    assert len(env.adapter.calls) == 4
    assert env.adapter.calls[1][:2] == env.adapter.calls[2][:2]
    assert len(await env.memory.database.fetchall('SELECT * FROM extraction_batches')) == 3
    assert len(await env.engine.jobs.list_candidates(completed['job_id'])) == 1


@pytest.mark.asyncio
async def test_correction_uses_remaining_window_deadline(environment):
    import time
    env = environment
    enable(env)
    env.engine.config = replace(env.config, extraction_timeout_seconds=1)
    env.adapter.output = '{"broken",1}'
    original = env.adapter.generate
    remaining = []

    async def generate(messages, **kwargs):
        remaining.append(kwargs['timeout_seconds'])
        if len(remaining) == 1:
            await asyncio.sleep(.03)
        else:
            await asyncio.sleep(2)
        return await original(messages, **kwargs)
    env.adapter.generate = generate
    start = time.monotonic()
    waiting = await execute(env, await enqueue_and_claim(env))
    assert waiting['error_code'] == 'extraction_timeout'
    assert len(remaining) == 2 and remaining[1] < remaining[0] - .02
    assert time.monotonic() - start < 1.8
    assert waiting['checkpoint']['window']['correction_attempt'] == 1
