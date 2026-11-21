"""Fixed topic standards survive rejection, feedback and atomic recovery."""

from copy import deepcopy
from dataclasses import replace
import json

import pytest

from core.topic_schema import VERSION
from tests.test_maintenance_engine import environment, enqueue_and_claim, execute
from tests.test_topic_maintenance import candidate, seed


@pytest.mark.asyncio
@pytest.mark.parametrize('fault,code,path', [
    ('schema_version', 'literal_error', '$.schema_version'),
    ('single_member', 'topic_create_members', '$.changes[0]'),
    ('unknown_user', 'topic_unknown_user', '$.changes[0]'),
    ('update_version', 'topic_update_identity', '$.changes[0]'),
    ('update_member_identity', 'topic_update_identity', '$.changes[0]'),
])
async def test_topic_declared_rules_recover_against_same_inputs(environment, fault, code, path):
    env = environment
    env.engine.config = replace(env.config, extraction_auto_regeneration=True)
    nodes = await seed(env)
    valid = json.loads(candidate(nodes))
    previous = None
    if fault.startswith('update_'):
        env.adapter.output = json.dumps(valid)
        initial = await execute(env, await enqueue_and_claim(env, 'topic_consolidate', key='initial'))
        assert initial['state'] == 'succeeded'
        previous = await env.memory.get_node(initial['result']['topic_ids'][0])
        nodes[0] = await env.memory.update_node_content(nodes[0]['node_id'],
            content_text='合成社区观星计划的首场活动完成了准备，另一场仍在安排。')
        valid['changes'][0].update(operation='update', topic_id=previous['node_id'],
            expected_version=previous['content_version'], content='社区观星计划持续推进，首场准备完成，另一场仍在安排。')
        env.adapter.calls.clear()
    bad = deepcopy(valid)
    change = bad['changes'][0]
    if fault == 'schema_version':
        bad['schema_version'] = 'topic-consolidation.unknown'
    elif fault == 'single_member':
        change['member_ids'] = change['member_ids'][:1]
    elif fault == 'unknown_user':
        change['content'] = '{{user:not_in_input}}参与了计划。'
    elif fault == 'update_version':
        change['expected_version'] += 1
    else:
        change['topic_id'] = nodes[0]['node_id']
    env.adapter.output = json.dumps(bad)
    receipts_before = len(await env.memory.database.fetchall('SELECT * FROM topic_receipts'))

    async def correct(messages):
        if len(env.adapter.calls) != 2:
            return
        # The rejected candidate cannot change the supplied identity/version
        # snapshot or leave an early write behind before the corrected commit.
        assert messages[:2] == env.adapter.calls[0]
        assert json.loads(messages[-2]['content']) == bad
        assert len(await env.memory.database.fetchall('SELECT * FROM topic_receipts')) == receipts_before
        topics = await env.memory.database.fetchall("SELECT node_id,content_version FROM memory_nodes WHERE node_type='topic'")
        assert len(topics) == int(previous is not None)
        if previous:
            assert topics[0]['node_id'] == previous['node_id']
            assert topics[0]['content_version'] == previous['content_version']
        feedback = json.loads(messages[-1]['content'])
        assert feedback['protocol'] == 'topic-regeneration.v1'
        error = feedback['errors'][0]
        assert error['type'] == code and error['path'] == path
        initial_rules, initial_schema_text = messages[0]['content'].split('\nJSON schema:\n')
        initial_schema = json.loads(initial_schema_text)
        if fault == 'schema_version':
            assert initial_schema['properties']['schema_version']['const'] == VERSION
            assert error['constraints']['const'] == VERSION
        elif fault == 'single_member':
            assert '至少两项独立成员' in initial_rules
            assert '至少' in error['instruction'] and '两个' in error['instruction']
            assert '无关成员' in error['instruction']
        elif fault == 'unknown_user':
            assert '身份引用沿用节点中已有的{{user:ID}}' in initial_rules
            assert '未知逻辑用户' in initial_rules
            assert '不猜测、替换或新增用户ID' in error['instruction']
        else:
            assert 'update必须复制已有topic_id和content_version' in initial_rules
            assert '当前content_version' in error['instruction']
            assert '不能使用成员ID' in error['instruction']
            supplied = json.loads(messages[1]['content'])['nodes']
            original = next(node for node in supplied if node['node_id'] == previous['node_id'])
            assert original['content_version'] == valid['changes'][0]['expected_version']
        env.adapter.output = json.dumps(valid)

    env.adapter.on_generate = correct
    result = await execute(env, await enqueue_and_claim(env, 'topic_consolidate', key='recovery'))
    assert result['state'] == 'succeeded', result
    assert len(env.adapter.calls) == 2
    assert len(await env.memory.database.fetchall('SELECT * FROM topic_receipts')) == receipts_before + 1
    topics = await env.memory.database.fetchall("SELECT node_id FROM memory_nodes WHERE node_type='topic'")
    assert len(topics) == 1
    current = await env.memory.get_node(topics[0]['node_id'])
    assert current['content_text'] == valid['changes'][0]['content']
    assert current['content_version'] == (previous['content_version'] + 1 if previous else 1)
    if previous:
        assert current['node_id'] == previous['node_id']
    for member in nodes:
        assert await env.memory.get_node(member['node_id']) == member
    assert (await env.memory.get_active_generation(env.space['space_id']))['activity_round'] == 0
    assert not await env.memory.database.fetchall('SELECT * FROM extraction_batch_sources')
