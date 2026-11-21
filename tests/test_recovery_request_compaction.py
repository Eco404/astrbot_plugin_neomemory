"""Overflow transport compaction retains complete frozen JSON and raw evidence."""
from copy import deepcopy
from dataclasses import replace
import asyncio
import json

import pytest

from core.candidate_feedback import compact_json_messages, recovery_messages
from core.extraction_apply import digest
from core.topic_budget import message_bytes
from tests.test_maintenance_engine import environment, enqueue_and_claim, execute, restart
from tests.test_topic_maintenance import candidate, seed


def use_verbatim_provider(env):
    async def generate(messages, **kwargs):
        env.adapter.calls.append(messages)
        if env.adapter.on_generate is not None:
            await env.adapter.on_generate(messages)
        return env.adapter.output
    env.adapter.generate = generate


def test_compaction_preserves_string_bytes_number_tokens_duplicates_and_field_order():
    raw = ' { "z" : "含 空格\\t与\\n以及\\\"引号\\\"和\\\\路径", "n" : 1.00, "n" : -0, "e" : 1e+04, "a" : [ true, null ] } \n'
    expected = '{"z":"含 空格\\t与\\n以及\\\"引号\\\"和\\\\路径","n":1.00,"n":-0,"e":1e+04,"a":[true,null]}'
    messages = [{'role': 'system', 'content': 'JSON schema:\n { "type" : "object" }'},
                {'role': 'user', 'content': raw}, {'role': 'assistant', 'content': raw},
                {'role': 'user', 'content': '{ "errors" : [ { "path" : "$.x", "instruction" : "保持 字符串" } ] }'}]
    before = deepcopy(messages)
    compact = compact_json_messages(messages)
    assert messages == before
    assert compact[0] == messages[0]
    assert compact[1]['content'] == compact[2]['content'] == expected
    assert compact[3]['content'] == '{"errors":[{"path":"$.x","instruction":"保持 字符串"}]}'
    assert json.loads(raw, object_pairs_hook=list, parse_int=str, parse_float=str) == json.loads(
        expected, object_pairs_hook=list, parse_int=str, parse_float=str)
    assert message_bytes(compact) < message_bytes(messages)


@pytest.mark.parametrize('raw', [' { "value": 1,\n', ' { "n": NaN } ', ' { "n": Infinity } ',
                                 ' { "text": "invalid\nliteral newline" } '])
def test_invalid_json_stays_verbatim_including_error_positions(raw):
    messages = [{'role': 'assistant', 'content': raw}, {'role': 'user', 'content': '{ "errors" : [] }'}]
    compact = compact_json_messages(messages)
    assert compact[0]['content'] == raw
    assert compact[1]['content'] == '{"errors":[]}'


@pytest.mark.asyncio
@pytest.mark.parametrize('padding,interrupt', [(0, False), (18000, False), (18000, True)],
                         ids=['within_budget_verbatim', 'overflow_compacted', 'compacted_resume'])
async def test_topic_correction_keeps_frozen_window_and_commits_once(environment, padding, interrupt):
    env = environment
    use_verbatim_provider(env)
    env.engine.config = replace(env.config, extraction_auto_regeneration=True, extraction_max_prompt_bytes=20000)
    nodes = await seed(env)
    valid = candidate(nodes)
    bad = json.loads(valid)
    bad['changes'][0]['member_ids'] = bad['changes'][0]['member_ids'][:1]
    raw = json.dumps(bad, ensure_ascii=False) + ' ' * padding
    env.adapter.output = raw
    captured = {}

    async def answer(messages):
        saved = await env.engine.jobs.get(job['job_id'])
        window = saved['checkpoint']['topic_window']
        if len(env.adapter.calls) == 1:
            captured['prepared'] = deepcopy(window['prepared'])
            captured['request_digest'] = window['request_digest']
            return
        assert len(env.adapter.calls) in (2, 3)
        assert window['prepared'] == captured['prepared']
        assert window['request_digest'] == captured['request_digest'] == digest(env.adapter.calls[0])
        assert window['corrections'] == 1
        assert not await env.memory.database.fetchall('SELECT * FROM topic_receipts')
        archived = await env.engine.jobs.get_candidate(job['job_id'], window['regeneration']['candidate_id'])
        assert archived['window']['raw'] == raw
        canonical = recovery_messages(env.adapter.calls[0], raw, window['errors'], window['regeneration']['version'])
        if padding:
            assert message_bytes(canonical) > env.engine.config.extraction_max_prompt_bytes
            assert message_bytes(messages) <= env.engine.config.extraction_max_prompt_bytes
            assert messages[0] == canonical[0]
            for actual, original in zip(messages[1:], canonical[1:], strict=True):
                assert json.loads(actual['content']) == json.loads(original['content'])
            assert messages[1]['content'] != canonical[1]['content']
            transport = window['request_transport']
            assert transport['format'] == 'json-whitespace.v1'
            assert transport['original_bytes'] == message_bytes(canonical)
            assert transport['transport_bytes'] == message_bytes(messages)
            assert transport['canonical_base_digest'] == captured['request_digest']
            assert transport['transport_digest'] == digest(messages)
        else:
            assert messages[:3] == canonical[:3]
            assert json.loads(messages[-1]['content']) == json.loads(canonical[-1]['content'])
            assert messages[-1]['content'] == json.dumps(json.loads(messages[-1]['content']), ensure_ascii=False)
            assert 'request_transport' not in window
        if interrupt and len(env.adapter.calls) == 2:
            captured['wire'] = deepcopy(messages)
            raise asyncio.CancelledError()
        if interrupt:
            assert messages[:3] == captured['wire'][:3]
            assert json.loads(messages[-1]['content']) == json.loads(captured['wire'][-1]['content'])
        env.adapter.output = valid

    env.adapter.on_generate = answer
    job = await enqueue_and_claim(env, 'topic_consolidate')
    if interrupt:
        with pytest.raises(asyncio.CancelledError):
            await execute(env, job)
        saved = await env.engine.jobs.get(job['job_id'])
        assert saved['checkpoint']['topic_window']['request_transport']['canonical_base_digest'] == captured['request_digest']
        restart(env, config=env.engine.config)
        finished = await execute(env, await env.engine.jobs.claim('resumed'))
    else:
        finished = await execute(env, job)
    assert finished['state'] == 'succeeded', finished
    assert len(env.adapter.calls) == (3 if interrupt else 2)
    assert len(await env.memory.database.fetchall('SELECT * FROM topic_receipts')) == 1
    assert len(await env.engine.jobs.list_candidates(job['job_id'])) == 1
    assert len(finished['result']['topic_ids']) == 1
    for original in nodes:
        assert await env.memory.get_node(original['node_id']) == original


@pytest.mark.asyncio
@pytest.mark.parametrize('valid_json', [False, True], ids=['invalid_json_offsets_preserved', 'large_string_cannot_shrink'])
async def test_uncompactable_overflow_stops_without_dropping_content_or_extra_calls(environment, valid_json):
    env = environment
    use_verbatim_provider(env)
    env.engine.config = replace(env.config, extraction_auto_regeneration=True, extraction_max_prompt_bytes=20000)
    nodes = await seed(env)
    value = json.loads(candidate(nodes))
    value['changes'][0]['content'] = '界' * 10000
    raw = json.dumps(value, ensure_ascii=False, separators=(',', ':')) if valid_json else '{ "changes" : ' + ' ' * 25000
    env.adapter.output = raw
    failed = await execute(env, await enqueue_and_claim(env, 'topic_consolidate'))
    assert failed['state'] == 'failed' and failed['error_code'] == 'prompt_budget_exceeded'
    assert len(env.adapter.calls) == 1
    window = failed['checkpoint']['topic_window']
    assert window['corrections'] == 1
    assert window['request_transport']['transport_bytes'] > env.engine.config.extraction_max_prompt_bytes
    assert window['request_digest'] == digest(env.adapter.calls[0])
    archived, = await env.engine.jobs.list_candidates(failed['job_id'])
    assert archived['window']['raw'] == raw
    assert compact_json_messages([{'role': 'assistant', 'content': raw}])[0]['content'] == raw
    assert not await env.memory.database.fetchall('SELECT * FROM topic_receipts')
