"""Subjective slot validation reaches bounded correction in the real runner."""

import json

import pytest

from tests.test_candidate_recovery import enable
from tests.test_maintenance_engine import environment, enqueue_and_claim, execute, output


@pytest.mark.asyncio
@pytest.mark.parametrize('slot', ['long_term', 'aftereffect'])
@pytest.mark.parametrize('operation,body', [('keep', '保留时不应重新输出正文。'),
                                           ('clear', '清空时不应带正文。'),
                                           ('replace', '   '), ('replace', ' RePLaCe '),
                                           ('replace', 'keep'), ('replace', 'clear')])
async def test_slot_body_failure_corrects_without_exposing_failed_task(environment, slot, operation, body):
    env = environment
    enable(env)
    relationship = {
        'logical_user_id': env.records[0].logical_user_id,
        'expected_version': 0, 'source_ids': ['s1'],
        'long_term': {'op': 'replace', 'body': '我喜欢和对方安静地商量事情。'},
        'aftereffect': {'op': 'replace', 'body': '刚才那份耐心让我心里很舒展。'},
    }
    valid = output(relationship=relationship)
    bad = json.loads(json.dumps(valid))
    bad['relationship_change'][slot] = {'op': operation, 'body': body}
    env.adapter.output = json.dumps(bad)
    job = await enqueue_and_claim(env)

    async def correct(messages):
        if len(env.adapter.calls) != 2:
            return
        assert messages[:2] == env.adapter.calls[0]
        feedback = json.loads(messages[-1]['content'])
        issue = feedback['errors'][0]
        assert issue['type'] == 'relationship_slot_body'
        assert issue['path'] == '$.relationship_change.' + slot
        assert issue['constraints'] == {'op': operation, 'body':
            {'min_non_whitespace_chars': 1, 'not_standalone_casefolded': ['keep', 'replace', 'clear']} if operation == 'replace' else {'const': ''}}
        assert issue['actual_encoded_chars'] == len(body)
        assert issue['actual_non_whitespace_chars'] == len(body.strip())
        running = await env.engine.describe(job['job_id'])
        assert running['state'] == 'running'
        assert running['error_code'] is None and running['error_type'] is None
        assert running['validation_errors'] == []
        assert running['automatic_corrections'] == 1
        assert not running['regenerate_window_available']
        env.adapter.output = json.dumps(valid)

    env.adapter.on_generate = correct
    completed = await execute(env, job)
    assert completed['state'] == 'succeeded'
    assert len(env.adapter.calls) == 2
    assert len(await env.memory.database.fetchall('SELECT * FROM extraction_batches')) == 1
    stored = await env.memory.database.fetchall('SELECT * FROM relationship_states')
    assert len(stored) == 1 and stored[0]['content_version'] == 1
    detail = await env.engine.describe(job['job_id'])
    assert detail['error_code'] is None and detail['validation_errors'] == []
    assert len(detail['candidate_history']) == 1
    assert not detail['regenerate_window_available']


@pytest.mark.asyncio
async def test_unrecognized_schema_is_automatically_corrected(environment):
    env = environment
    enable(env)
    valid = output()
    old = {**valid, 'schema_version': 'unknown.schema'}
    env.adapter.output = json.dumps(old)
    feedbacks = []

    async def correct(messages):
        if len(env.adapter.calls) == 2:
            feedback = json.loads(messages[-1]['content'])
            feedbacks.append(feedback)
            assert messages[:2] == env.adapter.calls[0]
            env.adapter.output = json.dumps(valid)

    env.adapter.on_generate = correct
    completed = await execute(env, await enqueue_and_claim(env))
    assert completed['state'] == 'succeeded'
    assert len(env.adapter.calls) == 2
    assert len(await env.memory.database.fetchall('SELECT * FROM extraction_batches')) == 1
    issue = feedbacks[0]['errors'][0]
    assert issue['type'] == 'literal_error'
    assert issue['path'] == '$.schema_version'
    assert issue['constraints']['const'] == 'extraction.v1'
    history = await env.engine.jobs.list_candidates(completed['job_id'])
    assert history[0]['error_code'] == 'invalid_output'


@pytest.mark.asyncio
async def test_repeated_slot_failure_stops_at_frozen_correction_limit(environment):
    env = environment
    enable(env, limit=2)
    env.adapter.output = json.dumps(output(relationship={
        'logical_user_id': env.records[0].logical_user_id, 'expected_version': 0,
        'source_ids': ['s1'], 'long_term': {'op': 'keep', 'body': '错误的保留正文。'},
        'aftereffect': {'op': 'keep', 'body': ''},
    }))
    failed = await execute(env, await enqueue_and_claim(env))
    assert failed['state'] == 'failed'
    assert len(env.adapter.calls) == 3
    detail = await env.engine.describe(failed['job_id'])
    assert detail['automatic_corrections'] == detail['automatic_correction_limit'] == 2
    assert detail['validation_errors'][0]['type'] == 'relationship_slot_body'
    assert detail['regenerate_window_available']
    assert not await env.memory.database.fetchall('SELECT * FROM extraction_batches')
