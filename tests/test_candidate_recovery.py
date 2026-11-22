"""Candidate corrections retain strict application rules and durable finite budgets."""
import asyncio
import json
from dataclasses import replace

import pytest

from core.config import NeoMemoryConfig
from core.relationship_state import LONG_TERM_MAX_CHARS
from core.extraction_regeneration import automatic_correction_allowed, validation_feedback
from tests.test_extraction_apply import capture
from tests.test_maintenance_engine import environment, enqueue_and_claim, execute, node, output, restart


def enable(env, limit=3):
    env.config = replace(env.config, extraction_auto_regeneration=True, extraction_max_regenerations=limit)
    env.engine.config = env.config


@pytest.mark.asyncio
@pytest.mark.parametrize('fault,code', [
    ('empty_time', 'unknown_event_time'),
    ('create_fields', 'create_identity_invalid'),
    ('update_fields', 'update_identity_required'),
    ('node_reference_kind', 'unknown_reference'),
    ('version', 'expected_node_version_mismatch'),
    ('edge', 'unknown_local_reference'),
    ('self_edge', 'edge_self_reference'),
    ('relationship_evidence', 'evidence_user_mismatch'),
])
async def test_fixed_input_candidate_errors_recover_and_commit_once(environment, fault, code):
    env = environment
    enable(env)
    old = await env.memory.create_node(space_id=env.space['space_id'], node_type='event',
        content_text='独立的设备维修计划。', source_ids=[env.records[0].source_id])
    valid = output([node('s1', old=old, content='设备维修计划仍待开始，新增了检测要求。')])
    bad = json.loads(json.dumps(valid))
    change = bad['node_changes'][0]
    if fault == 'empty_time':
        change['event_time'] = {'start': None, 'end': None, 'text': None}
    elif fault == 'create_fields':
        change['operation'] = 'create'
    elif fault == 'update_fields':
        change['node_id'] = None
    elif fault == 'node_reference_kind':
        change['node_id'] = 'u1'
    elif fault == 'version':
        change['expected_version'] += 1
    elif fault in {'edge', 'self_edge'}:
        bad['edges'] = [{'from_ref': change['local_ref'], 'to_ref': change['local_ref'] if fault == 'self_edge' else 'local_undeclared',
                         'relation_type': 'related', 'source_ids': ['s1']}]
    else:
        agent = await capture(env.sources, number=3, actor='agent', text='我感到对方愿意讨论计划。')
        relationship = {'logical_user_id': env.records[0].logical_user_id, 'expected_version': 0,
            'long_term': {'op': 'replace', 'body': '我愿意陪对方讨论这项计划。'},
            'aftereffect': {'op': 'keep', 'body': ''}, 'source_ids': ['s1']}
        valid['relationship_change'] = relationship
        bad['relationship_change'] = {**relationship, 'source_ids': [agent.source_id]}
    env.adapter.output = json.dumps(bad)

    async def correct(messages):
        if len(env.adapter.calls) == 2:
            assert messages[:2] == env.adapter.calls[0]
            feedback = json.loads(messages[-1]['content'])
            assert code in {item['type'] for item in feedback['errors']}
            assert len(messages) == 4
            env.adapter.output = json.dumps(valid)
    env.adapter.on_generate = correct
    completed = await execute(env, await enqueue_and_claim(env))
    assert completed['state'] == 'succeeded'
    assert len(env.adapter.calls) == 2
    assert len(await env.memory.database.fetchall('SELECT * FROM extraction_batches')) == 1
    assert (await env.memory.get_node(old['node_id']))['content_version'] == 2
    assert len(await env.memory.list_nodes(space_id=env.space['space_id'])) == 1
    candidates = await env.engine.jobs.list_candidates(completed['job_id'])
    assert len(candidates) == 1
    assert code in {item['type'] for item in candidates[0]['window']['validation_errors']}


@pytest.mark.asyncio
async def test_default_is_three_corrections_not_three_total_calls(environment):
    env = environment
    assert NeoMemoryConfig().extraction_max_regenerations == 3
    enable(env)
    env.adapter.output = '{"broken",1}'
    failed = await execute(env, await enqueue_and_claim(env))
    assert failed['state'] == 'failed'
    assert len(env.adapter.calls) == 4
    assert failed['checkpoint']['window']['correction_attempt'] == 3
    assert (await env.engine.describe(failed['job_id']))['automatic_correction_limit'] == 3
    assert len(await env.engine.jobs.list_candidates(failed['job_id'])) == 3
    assert not await env.memory.database.fetchall('SELECT * FROM extraction_batches')
    # A known repeated draft still receives explicit field feedback, never an unbounded loop.
    assert '完全相同' in env.adapter.calls[-1][-1]['content']
    await env.engine.retry(failed['job_id'], regenerate_window=True)
    failed = await execute(env, await env.engine.jobs.claim('manual'))
    assert len(env.adapter.calls) == 5
    assert failed['checkpoint']['window']['correction_attempt'] == 3


@pytest.mark.asyncio
async def test_last_allowed_correction_can_commit(environment):
    env = environment
    enable(env)
    env.adapter.output = '{"broken",1}'
    async def correct(messages):
        if len(env.adapter.calls) == 4:
            env.adapter.output = json.dumps(output([node('s1')]))
    env.adapter.on_generate = correct
    completed = await execute(env, await enqueue_and_claim(env))
    assert completed['state'] == 'succeeded'
    assert len(env.adapter.calls) == 4
    assert len(await env.memory.database.fetchall('SELECT * FROM extraction_batches')) == 1


@pytest.mark.asyncio
async def test_restart_and_config_change_do_not_replenish_frozen_corrections(environment):
    env = environment
    enable(env, 2)
    env.adapter.output = '{"broken",1}'
    async def interrupt(messages):
        if len(env.adapter.calls) == 3:
            raise asyncio.CancelledError()
    env.adapter.on_generate = interrupt
    job = await enqueue_and_claim(env)
    with pytest.raises(asyncio.CancelledError):
        await execute(env, job)
    saved = await env.engine.jobs.get(job['job_id'])
    assert saved['checkpoint']['window']['correction_attempt'] == 2
    assert saved['checkpoint']['window']['correction_limit'] == 2
    restart(env, config=replace(env.config, extraction_max_regenerations=10))
    env.adapter.on_generate = None
    failed = await execute(env, await env.engine.jobs.claim('restart'))
    assert failed['state'] == 'failed' and len(env.adapter.calls) == 4
    assert failed['checkpoint']['window']['correction_attempt'] == 2
    assert failed['checkpoint']['window']['correction_limit'] == 2




@pytest.mark.asyncio
@pytest.mark.parametrize('conflict', ['protected', 'changed_input'])
async def test_actual_conflicts_never_become_candidate_corrections(environment, conflict):
    env = environment
    enable(env)
    old = await env.memory.create_node(space_id=env.space['space_id'], node_type='event',
        content_text='人工维护的独立经历。', source_ids=[env.records[0].source_id])
    async def change_during_call(messages):
        if conflict == 'protected':
            await env.memory.set_manual_protection(old['node_id'], True)
        else:
            await env.memory.update_node_content(old['node_id'], '外部更新后的内容。')
    env.adapter.on_generate = change_during_call
    env.adapter.output = json.dumps(output([node('s1', old=old)]))
    failed = await execute(env, await enqueue_and_claim(env))
    assert failed['state'] == 'failed'
    assert failed['error_code'] == ('context_changed' if conflict == 'protected' else 'context_or_evidence_invalid')
    assert len(env.adapter.calls) == 1
    assert not await env.engine.jobs.list_candidates(failed['job_id'])
    assert not await env.memory.database.fetchall('SELECT * FROM extraction_batches')




def length_candidate(env, field):
    value = output([node('s1', content='校准完成后才能开始维修，目前仍在等待。')])
    user = env.records[0].logical_user_id
    target = value['node_changes'][0]
    if field == 'atomic':
        target['node_type'] = 'atomic'
        return value, target, 'content', 800
    if field == 'title':
        return value, target, 'title', 60
    if field == 'emotion':
        target['emotion'] = {'tags': [{'tag': 'concern', 'intensity': .3}], 'description': '对校准还未完成感到担心。'}
        return value, target['emotion'], 'description', 160
    if field == 'time':
        target['event_time'] = {'start': None, 'end': None, 'text': '大约去年夏季'}
        return value, target['event_time'], 'text', 120
    if field == 'profile':
        value['profile_changes'] = [{'logical_user_id': user, 'expected_version': 0,
            'tags': [], 'description': '喜欢维修旧设备。', 'source_ids': ['s1']}]
        return value, value['profile_changes'][0], 'description', 600
    if field == 'relationship':
        value['relationship_change'] = {'logical_user_id': user, 'expected_version': 0,
            'long_term': {'op': 'replace', 'body': '我愿意耐心陪对方等待，不把准备变成催促。'},
            'aftereffect': {'op': 'keep', 'body': ''}, 'source_ids': ['s1']}
        return value, value['relationship_change']['long_term'], 'body', LONG_TERM_MAX_CHARS
    if field == 'encoded':
        return value, target, 'local_ref', 70
    return value, target, 'title', 0


@pytest.mark.asyncio
@pytest.mark.parametrize('field', ['atomic', 'title', 'emotion', 'time', 'profile', 'relationship', 'encoded', 'empty'])
async def test_field_length_correction_uses_exact_limit_and_preserves_valid_candidate(environment, field):
    env = environment
    enable(env)
    bad, target, name, limit = length_candidate(env, field)
    valid = json.loads(json.dumps(bad))
    if field == 'encoded':
        target[name] = 'local_' + 'x' * 65
        code, actual = 'string_too_long', 'actual_encoded_chars'
    elif field == 'empty':
        target[name] = ''
        code, actual = 'too_short', 'actual_encoded_chars'
    else:
        target[name] = '字' * (limit + 1)
        code, actual = 'readable_too_long', 'actual_readable_chars'
    env.adapter.output = json.dumps(bad)

    async def correct(messages):
        if len(env.adapter.calls) == 2:
            diagnostic = json.loads(messages[-1]['content'])['errors'][0]
            assert diagnostic['type'] == code
            assert diagnostic[actual] == (0 if field == 'empty' else limit + 1)
            constraint = 'minLength' if field == 'empty' else 'maxLength' if field == 'encoded' else 'x-readable-maxLength'
            assert diagnostic['constraints'][constraint] == (1 if field == 'empty' else limit)
            env.adapter.output = json.dumps(valid)
    env.adapter.on_generate = correct
    completed = await execute(env, await enqueue_and_claim(env))
    assert completed['state'] == 'succeeded'
    assert len(env.adapter.calls) == 2
    assert len(await env.memory.database.fetchall('SELECT * FROM extraction_batches')) == 1
    stored = await env.memory.list_nodes(space_id=env.space['space_id'])
    assert stored[0]['content_text'] == valid['node_changes'][0]['content']


@pytest.mark.asyncio
async def test_failed_length_checkpoint_resumes_without_explicit_regeneration(environment, monkeypatch):
    env = environment
    enable(env)
    bad, target, name, limit = length_candidate(env, 'relationship')
    valid = json.loads(json.dumps(bad))
    target[name] = '字' * (limit + 1)
    env.adapter.output = json.dumps(bad)
    # Persist the same failure shape produced before length errors were eligible.
    with monkeypatch.context() as old:
        old.setattr('core.maintenance.automatic_correction_allowed', lambda *_: False)
        failed = await execute(env, await enqueue_and_claim(env))
    window = failed['checkpoint']['window']
    assert failed['state'] == 'failed'
    assert window['validation_errors'][0]['type'] == 'readable_too_long'
    assert window.get('correction_attempt', 0) == 0 and window['correction_limit'] == 3
    restart(env)
    env.adapter.output = json.dumps(valid)
    await env.engine.retry(failed['job_id'])
    completed = await execute(env, await env.engine.jobs.claim('resume-length'))
    assert completed['state'] == 'succeeded'
    assert len(env.adapter.calls) == 2  # Stored draft is revalidated, not generated again.
    archived = await env.engine.jobs.list_candidates(failed['job_id'])
    assert len(archived) == 1 and archived[0]['window']['input_digest'] == window['input_digest']
    assert len(await env.memory.database.fetchall('SELECT * FROM extraction_batches')) == 1


@pytest.mark.asyncio
async def test_length_correction_budget_survives_restart(environment):
    env = environment
    enable(env, 2)
    bad, target, name, limit = length_candidate(env, 'relationship')
    target[name] = '字' * (limit + 1)
    env.adapter.output = json.dumps(bad)
    async def interrupt(messages):
        if len(env.adapter.calls) == 2:
            raise asyncio.CancelledError()
    env.adapter.on_generate = interrupt
    job = await enqueue_and_claim(env)
    with pytest.raises(asyncio.CancelledError):
        await execute(env, job)
    saved = await env.engine.jobs.get(job['job_id'])
    assert saved['checkpoint']['window']['correction_attempt'] == 1
    restart(env, config=replace(env.config, extraction_max_regenerations=10))
    env.adapter.on_generate = None
    failed = await execute(env, await env.engine.jobs.claim('resume-length-budget'))
    assert failed['state'] == 'failed'
    assert failed['checkpoint']['window']['correction_attempt'] == 2
    assert failed['checkpoint']['window']['correction_limit'] == 2
    assert not await env.memory.database.fetchall('SELECT * FROM extraction_batches')


@pytest.mark.parametrize('other', ['output_capacity_exceeded', 'value_error', 'prompt_budget_exceeded', 'unknown_failure'])
def test_mixed_unclassified_or_capacity_errors_do_not_enter_length_correction(other):
    window = {'validation_errors': [{'type': 'readable_too_long'}, {'type': other}]}
    assert not automatic_correction_allowed('invalid_output', window)




@pytest.mark.asyncio
@pytest.mark.parametrize('fault,code', [
    ('tags_count', 'too_long'), ('empty_sources', 'too_short'),
    ('importance_low', 'greater_than_equal'), ('importance_high', 'less_than_equal'),
    ('importance_type', 'float_type'), ('content_type', 'string_type'),
    ('sources_type', 'list_type'), ('emotion_type', 'model_type'),
    ('duplicate_sources', 'duplicate_values'), ('duplicate_nodes', 'duplicate_values'),
    ('neutral_conflict', 'neutral_emotion_conflict'), ('result_mismatch', 'result_changes_mismatch'),
])
async def test_explicit_schema_rules_correct_through_durable_job(environment, fault, code):
    env = environment
    enable(env)
    valid = output([node('s1', content='只有仪器到货才开始，尚未开工。')])
    valid['node_changes'][0]['emotion'] = {'tags': [{'tag': 'concern', 'intensity': .3}], 'description': '我担心准备不足。'}
    bad = json.loads(json.dumps(valid))
    item = bad['node_changes'][0]
    if fault == 'tags_count':
        item['emotion']['tags'] = [{'tag': tag, 'intensity': .3} for tag in ['concern', 'fear', 'sadness', 'trust']]
    elif fault == 'empty_sources':
        item['source_ids'] = []
    elif fault == 'importance_low':
        item['importance'] = -.1
    elif fault == 'importance_high':
        item['importance'] = 1.1
    elif fault == 'importance_type':
        item['importance'] = '0.5'
    elif fault == 'content_type':
        item['content'] = []
    elif fault == 'sources_type':
        item['source_ids'] = 's1'
    elif fault == 'emotion_type':
        item['emotion'] = []
    elif fault == 'duplicate_sources':
        item['source_ids'] = ['s1', 's1']
    elif fault == 'duplicate_nodes':
        bad['node_changes'].append(json.loads(json.dumps(item)))
    elif fault == 'neutral_conflict':
        item['emotion']['tags'] = [{'tag': tag, 'intensity': .3} for tag in ['neutral', 'concern']]
    else:
        bad['result'] = 'no_change'
    env.adapter.output = json.dumps(bad)
    async def correct(messages):
        if len(env.adapter.calls) == 2:
            feedback = json.loads(messages[-1]['content'])
            assert feedback['errors'][0]['type'] == code
            env.adapter.output = json.dumps(valid)
    env.adapter.on_generate = correct
    completed = await execute(env, await enqueue_and_claim(env))
    assert completed['state'] == 'succeeded'
    assert len(env.adapter.calls) == 2
    assert len(await env.memory.database.fetchall('SELECT * FROM extraction_batches')) == 1


@pytest.mark.parametrize('case,expected', [
    ('empty_time', 'unknown_event_time'), ('create_fields', 'create_identity_invalid'),
    ('user_as_node', None), ('expected_version', None), ('relationship_evidence', None),
    ('local_edge', 'unknown_local_reference'), ('relationship_length', 'readable_too_long'),
    ('node_length', 'readable_too_long'), ('empty_title', 'too_short'),
    ('emotion_count', 'too_long'), ('importance_bound', 'less_than_equal'),
    ('importance_type', 'float_type'), ('duplicate_sources', 'duplicate_values'),
    ('neutral_conflict', 'neutral_emotion_conflict'), ('result_mismatch', 'result_changes_mismatch'),
])
def test_real_provider_probe_injects_only_its_designated_schema_fault(case, expected):
    from pydantic import ValidationError
    from core.extraction_schema import SCHEMA_VERSION, parse_extraction_output
    from scripts.probe_candidate_recovery import candidate

    value = candidate(case)
    assert value['schema_version'] == SCHEMA_VERSION
    raw = json.dumps(value)
    if expected is None:
        # Reference identity, evidence and versions are rejected after parsing.
        parse_extraction_output(raw, schema_version=SCHEMA_VERSION)
    else:
        with pytest.raises(ValidationError) as failure:
            parse_extraction_output(raw, schema_version=SCHEMA_VERSION)
        assert [item['type'] for item in failure.value.errors()] == [expected]


@pytest.mark.asyncio
async def test_readonly_update_corrects_without_losing_unrelated_event(environment):
    env = environment
    enable(env)
    old = await env.memory.create_node(space_id=env.space['space_id'], node_type='event',
        content_text='已经确认的独立经历。', source_ids=[env.records[0].source_id], manual_protected=True)
    good = node('s1', ref='local_new', content='新发生的独立事件。')
    env.adapter.output = json.dumps(output([good, node('s1', old=old, ref='local_edit')]))

    async def correct(messages):
        if len(env.adapter.calls) == 2:
            issue, = json.loads(messages[-1]['content'])['errors']
            assert issue['type'] == 'readonly_node' and issue['path'] == '$.node_changes[1].node_id'
            assert issue['actual_value'] not in issue['writable_nodes']
            env.adapter.output = json.dumps(output([good]))

    env.adapter.on_generate = correct
    result = await execute(env, await enqueue_and_claim(env))
    assert result['state'] == 'succeeded', result['error_code']
    assert len(env.adapter.calls) == 2
    assert (await env.memory.get_node(old['node_id']))['content_version'] == 1
    assert len(await env.memory.database.fetchall('SELECT * FROM extraction_batches')) == 1
