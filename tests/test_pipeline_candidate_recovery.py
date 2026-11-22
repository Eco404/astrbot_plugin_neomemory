"""Topic and rolling-summary corrections use fixed input and durable budgets."""
import asyncio
from dataclasses import replace
import json

import pytest

from core.jobs import ReviewRequired
from core.maintenance import MaintenanceEngine
from core.topic_maintenance import TopicStore
from core.short_term_prompts import PROMPT_VERSION
from tests.test_maintenance_engine import environment, enqueue_and_claim, execute
from tests.test_topic_maintenance import seed, candidate
from tests.test_short_term import env, claim, execute as execute_summary, restart


@pytest.mark.asyncio
@pytest.mark.parametrize('fault,code', [
    ('title', 'readable_too_long'), ('score', 'less_than_equal'),
    ('members', 'topic_duplicate_members'), ('member_id', 'topic_member_unavailable'),
    ('count', 'too_long'), ('empty', 'too_short'),
])
async def test_topic_rules_get_schema_feedback_and_one_commit(environment, fault, code):
    e = environment
    e.engine.config = replace(e.config, extraction_auto_regeneration=True)
    valid = candidate(await seed(e))
    bad = json.loads(valid)
    change = bad['changes'][0]
    if fault == 'title':
        change['title'] = '字' * 61
    elif fault == 'score':
        change['importance'] = 1.1
    elif fault == 'members':
        change['member_ids'] = change['member_ids'][:1] * 2
    elif fault == 'member_id':
        change['member_ids'][0] = 'unavailable-member'
    elif fault == 'empty':
        change['title'] = ''
    else:
        change['emotion'] = {'tags': ['joy', 'trust', 'concern', 'fear'], 'intensity': .3, 'description': '有依据的合成感受。'}
    e.adapter.output = json.dumps(bad)
    async def correct(messages):
        if len(e.adapter.calls) == 2:
            assert len(messages) == 4
            assert json.loads(messages[-2]['content']) == bad
            feedback = json.loads(messages[-1]['content'])
            assert feedback['protocol'] == 'topic-regeneration.v2-dev.1'
            assert feedback['errors'][0]['type'] == code
            if fault == 'title':
                assert feedback['errors'][0]['constraints']['x-readable-maxLength'] == 60
                assert feedback['errors'][0]['actual_readable_chars'] == 61
            e.adapter.output = valid
    e.adapter.on_generate = correct
    result = await execute(e, await enqueue_and_claim(e, 'topic_consolidate'))
    assert result['state'] == 'succeeded'
    assert len(e.adapter.calls) == 2
    assert len(await e.memory.database.fetchall('SELECT * FROM topic_receipts')) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('fault,code', [('incomplete', 'output_incomplete'),
    ('capacity', 'output_capacity_exceeded'), ('unknown', 'unclassified_candidate_error')])
async def test_topic_non_candidate_failures_do_not_spend_correction_budget(environment, monkeypatch, fault, code):
    e = environment
    e.engine.config = replace(e.config, extraction_auto_regeneration=True)
    valid = candidate(await seed(e))
    async def generate(messages, *, diagnostics=None, **kwargs):
        e.adapter.calls.append(messages)
        if fault == 'incomplete':
            diagnostics['finish_reason'] = 'length'
        return 'x' * 120001 if fault == 'capacity' else valid
    monkeypatch.setattr(e.adapter, 'generate', generate)
    if fault == 'unknown':
        async def reject(*args, **kwargs):
            raise ValueError('unknown internal issue')
        monkeypatch.setattr(TopicStore, 'apply', reject)
    result = await execute(e, await enqueue_and_claim(e, 'topic_consolidate'))
    assert result['state'] == 'failed'
    assert result['checkpoint']['topic_window']['errors'][0]['type'] == code
    assert result['checkpoint']['topic_window']['corrections'] == 0
    assert len(e.adapter.calls) == 1
    assert not await e.memory.database.fetchall('SELECT * FROM topic_receipts')


@pytest.mark.asyncio
async def test_topic_permission_error_is_not_a_temporary_network_failure(environment):
    e = environment
    await seed(e)
    async def denied(messages, **kwargs):
        e.adapter.calls.append(messages)
        raise PermissionError('provider access denied')
    e.adapter.generate = denied
    result = await execute(e, await enqueue_and_claim(e, 'topic_consolidate'))
    assert result['state'] == 'failed'
    assert result['error_code'] == 'provider_unavailable'
    assert len(e.adapter.calls) == 1
    assert result['checkpoint']['topic_window']['corrections'] == 0
    assert not await e.memory.database.fetchall('SELECT * FROM topic_receipts')


def summary(job):
    return {'schema_version': PROMPT_VERSION, 'summary_text': '只有工具到货才开始，尚未开工。',
            'evidence_source_ids': [job['payload']['source_ids'][0]]}


@pytest.mark.asyncio
@pytest.mark.parametrize('fault,code', [('length', 'string_too_long'), ('version', 'literal_error'),
    ('source', 'literal_error'), ('duplicate', 'duplicate_values'),
    ('initial', 'initial_summary_evidence_required'), ('user', 'summary_unknown_user')])
async def test_summary_schema_faults_correct_and_preserve_frozen_range(env, fault, code):
    e = env
    job = await claim(e)
    valid = summary(job)
    bad = json.loads(json.dumps(valid))
    if fault == 'length':
        bad['summary_text'] = '字' * 401
    elif fault == 'version':
        bad['schema_version'] = 'unknown'
    elif fault == 'source':
        bad['evidence_source_ids'] = ['unknown-source']
    elif fault == 'duplicate':
        bad['evidence_source_ids'] *= 2
    elif fault == 'initial':
        bad['evidence_source_ids'] = []
    else:
        bad['summary_text'] = '{{user:unknown}}尚未开工。'
    e.adapter.output = json.dumps(bad)
    async def correct():
        if len(e.adapter.calls) == 2:
            messages = e.adapter.calls[-1]
            assert messages[:2] == e.adapter.calls[0]
            assert json.loads(messages[-2]['content']) == bad
            feedback = json.loads(messages[-1]['content'])
            assert feedback['protocol'] == 'short-term-regeneration.v1'
            assert feedback['errors'][0]['type'] == code
            if fault == 'length':
                assert feedback['errors'][0]['constraints']['maxLength'] == 400
                assert feedback['errors'][0]['actual_encoded_chars'] == 401
            e.adapter.output = json.dumps(valid)
    e.adapter.hook = correct
    result = await execute_summary(e, job)
    assert result['state'] == 'succeeded'
    assert len(e.adapter.calls) == 2
    assert len(await e.memory.database.fetchall('SELECT * FROM short_term_receipts')) == 1
    assert len(await e.memory.database.fetchall('SELECT * FROM short_term_sources')) == len(job['payload']['source_ids'])


@pytest.mark.asyncio
async def test_summary_explicit_regeneration_does_not_replenish_exhausted_budget(env):
    e = env
    e.adapter.output = 'invalid json'
    job = await claim(e)
    failed = await execute_summary(e, job)
    assert failed['state'] == 'failed'
    assert len(e.adapter.calls) == 4
    assert failed['checkpoint']['corrections'] == 3
    assert len(await e.jobs.list_candidates(job['job_id'])) == 3
    await e.jobs.regenerate_window(job['job_id'])
    failed = await execute_summary(e, await e.jobs.claim('manual'))
    assert failed['state'] == 'failed'
    assert len(e.adapter.calls) == 5
    assert failed['checkpoint']['corrections'] == 3
    assert not await e.memory.database.fetchall('SELECT * FROM short_term_receipts')


@pytest.mark.asyncio
async def test_summary_failure_is_reviewable_and_manual_retry_uses_latest_candidate(env):
    e = env
    e.config.extraction_max_regenerations = 0
    e.adapter.output = 'invalid json'
    job = await claim(e)
    await execute_summary(e, job)
    # describe uses these read-only dependencies; no second runner is started.
    engine = object.__new__(MaintenanceEngine)
    engine.jobs, engine.memory, engine.config = e.jobs, e.memory, e.config
    detail = await engine.describe(job['job_id'])
    assert detail['regenerate_window_available'] is True
    assert detail['restart_required'] is False
    assert detail['validation_errors'][0]['type'] == 'json_invalid'
    assert detail['automatic_corrections'] == detail['automatic_correction_limit'] == 0
    await e.jobs.regenerate_window(job['job_id'])
    e.adapter.output = json.dumps(summary(job))
    result = await execute_summary(e, await e.jobs.claim('manual-summary'))
    assert result['state'] == 'succeeded'
    assert e.adapter.calls[-1][-2]['content'] == 'invalid json'
    assert (await engine.describe(job['job_id']))['regenerate_window_available'] is False


@pytest.mark.parametrize('pipeline', ['topic', 'summary'])
def test_nonfinite_json_has_explicit_parse_feedback(pipeline):
    from core.candidate_feedback import correction_allowed, validation_errors
    from core.short_term_prompts import parse_output as parse_summary
    from core.topic_schema import parse_output as parse_topic
    raw = '{"schema_version": NaN}'
    with pytest.raises(ValueError) as failure:
        if pipeline == 'topic':
            parse_topic(raw)
        else:
            parse_summary(raw, source_ids=[], summary_chars=400)
    errors = validation_errors(failure.value, raw, {}, {})
    assert errors[0]['type'] == 'json_invalid'
    assert correction_allowed(errors, {})


def test_oversized_summary_is_not_reparsed_as_correctable_json():
    from core.candidate_feedback import correction_allowed, validation_errors
    from core.short_term_prompts import parse_output
    raw = 'x' * 100001
    with pytest.raises(ValueError) as failure:
        parse_output(raw, source_ids=[], summary_chars=400)
    errors = validation_errors(failure.value, raw, {}, {})
    assert errors == [{'path': '$', 'type': 'output_capacity_exceeded'}]
    assert not correction_allowed(errors, {})




@pytest.mark.asyncio
async def test_summary_cancelled_correction_retains_budget_after_restart(env):
    e = env
    e.config.extraction_max_regenerations = 2
    e.adapter.output = 'invalid json'
    async def interrupt():
        if len(e.adapter.calls) == 2:
            raise asyncio.CancelledError()
    e.adapter.hook = interrupt
    job = await claim(e)
    with pytest.raises(asyncio.CancelledError):
        await execute_summary(e, job)
    saved = await e.jobs.get(job['job_id'])
    assert saved['checkpoint']['corrections'] == 1
    e.config.extraction_max_regenerations = 10
    e.adapter.hook = None
    restart(e)
    result = await execute_summary(e, await e.jobs.claim('restart'))
    assert result['state'] == 'failed'
    assert result['checkpoint']['corrections'] == 2 and result['checkpoint']['correction_limit'] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['incomplete', 'capacity'])
async def test_summary_incomplete_and_capacity_do_not_correct_or_commit(env, fault):
    e = env
    job = await claim(e)
    async def generate(messages, *, diagnostics=None, **kwargs):
        e.adapter.calls.append(messages)
        if fault == 'incomplete':
            diagnostics['finish_reason'] = 'length'
        return 'x' * 100001 if fault == 'capacity' else json.dumps(summary(job))
    e.adapter.generate = generate
    result = await execute_summary(e, job)
    assert result['state'] == 'failed'
    assert len(e.adapter.calls) == 1
    assert result['checkpoint']['corrections'] == 0
    assert not await e.memory.database.fetchall('SELECT * FROM short_term_receipts')


@pytest.mark.asyncio
async def test_summary_corrections_share_one_execution_deadline(env):
    e = env
    e.config.extraction_timeout_seconds = .2
    budgets = []
    async def generate(messages, *, timeout_seconds, **kwargs):
        budgets.append(timeout_seconds)
        await asyncio.sleep(.04 if len(budgets) == 1 else .3)
        return 'invalid json'
    e.adapter.generate = generate
    result = await execute_summary(e, await claim(e))
    assert result['state'] == 'retry_wait' and result['error_code'] == 'short_term_timeout'
    assert len(budgets) == 2 and budgets[1] < budgets[0] - .03
    assert result['checkpoint']['corrections'] == 1
    assert not await e.memory.database.fetchall('SELECT * FROM short_term_receipts')


@pytest.mark.asyncio
async def test_topic_manual_regeneration_uses_last_rejected_draft(environment):
    e = environment
    e.engine.config = replace(e.config, extraction_auto_regeneration=True, extraction_max_regenerations=1)
    valid = candidate(await seed(e))
    e.adapter.output = 'first invalid'
    async def second_bad(messages):
        if len(e.adapter.calls) == 2:
            e.adapter.output = 'second invalid'
    e.adapter.on_generate = second_bad
    failed = await execute(e, await enqueue_and_claim(e, 'topic_consolidate'))
    assert failed['state'] == 'failed'
    await e.engine.retry(failed['job_id'], regenerate_window=True)
    async def verify_last(messages):
        assert messages[-2]['content'] == 'second invalid'
        e.adapter.output = valid
    e.adapter.on_generate = verify_last
    result = await execute(e, await e.engine.jobs.claim('manual-topic'))
    assert result['state'] == 'succeeded'
    assert len(e.adapter.calls) == 3
