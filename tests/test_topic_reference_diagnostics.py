"""Topic identity correction uses exact frozen IDs without accepting near matches."""
from copy import deepcopy
from dataclasses import replace
import asyncio
import json

import pytest

from core.candidate_feedback import CandidateValidationError, validation_errors
from core.topic_regeneration import EXPLANATIONS, refresh_feedback
from core.topic_schema import VERSION, output_schema, parse_output
from tests.test_maintenance_engine import environment, enqueue_and_claim, execute
from tests.test_topic_maintenance import candidate, seed

TOPIC_ID = 'node_726b5c3f2d194b6b988d014fc67a930e'
MEMBER_ID = 'node_413d89b20dc54d45bc933b983e40647a'
OTHER_ID = 'node_583c91e6f8494ce4a971f7d05b71a32d'
DIRECTORY_ID = 'node_d3e3849d657c4bed9cbb3160acfa734e'
CONTEXT = {'nodes': [
    {'node_id': TOPIC_ID, 'node_type': 'topic', 'content_version': 4, 'status': 'active'},
    {'node_id': MEMBER_ID, 'node_type': 'event', 'content_version': 2, 'status': 'active'},
    {'node_id': OTHER_ID, 'node_type': 'atomic', 'content_version': 1, 'status': 'active'},
], 'related_topics': [{'node_id': DIRECTORY_ID, 'node_type': 'topic', 'content_version': 3,
                       'status': 'active', 'read_only': True}], 'edges': []}


def proposal():
    return {'schema_version': VERSION, 'changes': [{
        'operation': 'update', 'topic_id': TOPIC_ID, 'expected_version': 4,
        'title': '社区观测安排', 'content': '两次独立观测延续同一社区科普计划。',
        'importance': .5, 'emotion': None, 'member_ids': [MEMBER_ID, OTHER_ID],
    }]}


@pytest.mark.parametrize('wrong', [TOPIC_ID[:-1], MEMBER_ID, DIRECTORY_ID, None])
def test_invalid_topic_identity_names_exact_field_value_and_editable_domain(wrong):
    value = proposal()
    value['changes'][0]['topic_id'] = wrong
    error, = validation_errors(CandidateValidationError('topic_update_identity', ('changes', 0), ''),
        json.dumps(value), output_schema(), EXPLANATIONS, context=CONTEXT, pipeline='topic')
    assert error['path'] == '$.changes[0]'
    assert error['constraints']['topic_id']['enum'] == [TOPIC_ID]
    violation, = error['violations']
    assert violation['path'] == '$.changes[0].topic_id'
    assert violation['actual_value'] == wrong
    assert violation['constraints'] == {'enum': [TOPIC_ID], 'nullable': False}
    assert violation['allowed_topics'] == [{'topic_id': TOPIC_ID}]


@pytest.mark.parametrize('wrong', [None, 3])
def test_program_version_is_ignored_in_topic_feedback(wrong):
    value = proposal()
    value['changes'][0]['expected_version'] = wrong
    error, = validation_errors(CandidateValidationError('topic_update_identity', ('changes', 0), ''),
        json.dumps(value), output_schema(), EXPLANATIONS, context=CONTEXT, pipeline='topic')
    assert 'violations' not in error
    assert 'expected_version' not in error['constraints']


def test_missing_version_is_filled_by_program_context():
    value = proposal()
    del value['changes'][0]['expected_version']
    errors = refresh_feedback(json.dumps(value), [], schema_version=VERSION, context=CONTEXT)
    assert errors == []


@pytest.mark.parametrize('wrong', [MEMBER_ID[:-1], TOPIC_ID, DIRECTORY_ID])
def test_wrong_member_identity_exposes_actual_and_exact_permitted_member_ids(wrong):
    value = proposal()
    value['changes'][0]['member_ids'][1] = wrong
    error, = validation_errors(CandidateValidationError('topic_member_unavailable', ('changes', 0, 'member_ids', 1), ''),
        json.dumps(value), output_schema(), EXPLANATIONS, context=CONTEXT, pipeline='topic')
    assert error['path'] == '$.changes[0].member_ids[1]'
    assert error['actual_value'] == wrong
    assert error['constraints']['enum'] == [MEMBER_ID, OTHER_ID]


def legacy_diagnostics(*args, **kwargs):
    errors = validation_errors(*args, **kwargs)
    for error in errors:
        error.pop('violations', None)
        error.pop('actual_value', None)
    return errors


async def initial_and_update(env):
    nodes = await seed(env)
    env.adapter.output = candidate(nodes)
    initial = await execute(env, await enqueue_and_claim(env, 'topic_consolidate', key='initial'))
    assert initial['state'] == 'succeeded'
    topic = await env.memory.get_node(initial['result']['topic_ids'][0])
    nodes[0] = await env.memory.update_node_content(nodes[0]['node_id'], content_text='第一场社区观测已完成准备，第二场仍在安排。')
    valid = json.loads(candidate(nodes))
    valid['changes'][0].update(operation='update', topic_id=topic['node_id'], expected_version=1,
                               content='社区科普观测持续推进，第一场准备完成，第二场仍在安排。')
    bad = deepcopy(valid)
    bad['changes'][0]['topic_id'] = topic['node_id'][:-1]
    env.adapter.calls.clear()
    env.adapter.output = json.dumps(bad)
    return topic, valid, bad


@pytest.mark.asyncio
async def test_manual_regeneration_refreshes_exhausted_frozen_errors_and_commits_once(environment, monkeypatch):
    env = environment
    env.engine.config = replace(env.config, extraction_auto_regeneration=True, extraction_max_regenerations=3)
    topic, valid, bad = await initial_and_update(env)
    with monkeypatch.context() as old:
        old.setattr('core.topic_maintenance.validation_errors', legacy_diagnostics)
        failed = await execute(env, await enqueue_and_claim(env, 'topic_consolidate', key='truncated-id'))
    assert failed['state'] == 'failed'
    assert len(env.adapter.calls) == 4
    window = deepcopy(failed['checkpoint']['topic_window'])
    assert window['corrections'] == 3
    assert 'violations' not in window['errors'][0]
    assert (await env.memory.get_node(topic['node_id']))['content_version'] == 1
    receipts_before = len(await env.memory.database.fetchall('SELECT * FROM topic_receipts'))
    await env.engine.retry(failed['job_id'], regenerate_window=True)

    async def corrected(messages):
        assert messages[:2] == env.adapter.calls[0]
        assert json.loads(messages[-2]['content']) == bad
        error, = json.loads(messages[-1]['content'])['errors']
        violation, = error['violations']
        assert violation['path'] == '$.changes[0].topic_id'
        assert violation['actual_value'] == bad['changes'][0]['topic_id']
        assert topic['node_id'] in violation['constraints']['enum']
        saved = await env.engine.jobs.get(failed['job_id'])
        saved_window = saved['checkpoint']['topic_window']
        assert saved_window['corrections'] == 3
        assert saved_window['errors'] == [error]
        assert saved_window['prepared'] == window['prepared']
        assert len(await env.memory.database.fetchall('SELECT * FROM topic_receipts')) == receipts_before
        env.adapter.output = json.dumps(valid)

    env.adapter.on_generate = corrected
    complete = await execute(env, await env.engine.jobs.claim('manual-refresh'))
    assert complete['state'] == 'succeeded', complete
    assert len(env.adapter.calls) == 5
    assert len(await env.memory.database.fetchall('SELECT * FROM topic_receipts')) == receipts_before + 1
    assert (await env.memory.get_node(topic['node_id']))['content_version'] == 2
    archived = await env.engine.jobs.list_candidates(failed['job_id'])
    assert len(archived) == 4
    assert all('violations' not in row['window']['errors'][0] for row in archived)


@pytest.mark.asyncio
async def test_automatic_resume_keeps_its_existing_feedback_until_explicit_regeneration(environment, monkeypatch):
    env = environment
    env.engine.config = replace(env.config, extraction_auto_regeneration=True, extraction_max_regenerations=3)
    _, valid, _ = await initial_and_update(env)
    async def interrupt(messages):
        if len(env.adapter.calls) == 2:
            assert 'violations' not in json.loads(messages[-1]['content'])['errors'][0]
            raise asyncio.CancelledError()
    env.adapter.on_generate = interrupt
    with monkeypatch.context() as old:
        old.setattr('core.topic_maintenance.validation_errors', legacy_diagnostics)
        job = await enqueue_and_claim(env, 'topic_consolidate', key='frozen-feedback')
        with pytest.raises(asyncio.CancelledError):
            await execute(env, job)
    frozen_request = deepcopy(env.adapter.calls[-1])
    env.adapter.on_generate = None
    env.adapter.output = json.dumps(valid)
    complete = await execute(env, await env.engine.jobs.claim('resumed'))
    assert complete['state'] == 'succeeded', complete
    assert env.adapter.calls[-1][:3] == frozen_request[:3]
    assert json.loads(env.adapter.calls[-1][-1]['content']) == json.loads(frozen_request[-1]['content'])
