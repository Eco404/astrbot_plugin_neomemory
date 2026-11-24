"""Subjective relationship voice and continuity across extraction windows."""
import json

import pytest

from core.extraction_apply import ExtractionService
from core.memory_spaces import private_space
from core.prompts import (
    PROMPT_VERSION,
    build_extraction_prompt,
    schema_for_prompt_version,
)
from tests.test_extraction_apply import capture, output
from tests.test_extraction_apply import stores as stores




def test_current_contract_keeps_subjective_autonomy_with_independent_slots():
    inputs = {'persona': '独立人格', 'sources': [], 'known_relationship': {'state': {
        'format': 'relationship.slots.v1', 'long_term': {'body': '我不相信这只是巧合。', 'observed_at': 100.},
        'aftereffect': {'body': '', 'observed_at': None}}}}
    current = build_extraction_prompt(**inputs)
    assert PROMPT_VERSION == 'extraction-prompt.v3'
    assert schema_for_prompt_version(PROMPT_VERSION) == 'extraction.v1'
    relationship = json.loads(current[1]['content'])['known_relationship']
    assert 'state_text' not in relationship
    assert relationship['state']['long_term']['body'] == inputs['known_relationship']['state']['long_term']['body']
    assert relationship['state']['aftereffect']['body'] == ''
    schema = json.loads(current[0]['content'].split('JSON schema:\n')[1])
    props = schema['$defs']['RelationshipChange']['properties']
    assert {'long_term', 'aftereffect', 'source_ids'} <= props.keys()
    assert 'emotion_intensity' not in props
    rules = current[0]['content']
    assert '【人格关系的持续承接】' not in rules
    assert '由persona和我的自主判断决定' in rules
    assert '可以延续、调整或淡出' in rules
    assert '保留来源中事实的程度、范围、条件、否定、不确定性及时间' in rules
    assert '不升级成节点或画像中的客观用户事实' in rules
    assert '允许偏爱、怀疑、误解、坚定看法和负面感受' in rules
    assert '不是用户画像、事实总结或相处说明书' in rules


@pytest.mark.asyncio
async def test_relationship_continues_until_change_without_creating_objective_facts(stores):
    sources, memory, _ = stores
    current = ExtractionService(sources, memory)
    first = await capture(sources, '我们可以慢慢交流。', number=1)
    user = first.logical_user_id
    space = await memory.get_or_create_space(private_space('p', user))
    batch = await current.prepare(space_id=space['space_id'], source_ids=(first.source_id,), persona='独立人格')
    old_text = '我期待继续了解对方。我们聊过两本书，又比较了三个段落的顺序。'
    candidate = output(relationship={'logical_user_id': user, 'expected_version': 0,
        'long_term': {'op': 'replace', 'body': old_text}, 'aftereffect': {'op': 'keep', 'body': ''}, 'source_ids': [first.source_id]})
    await current.apply(batch, json.dumps(candidate), model_id='fixture')
    neutral = await capture(sources, '水壶已经洗干净了。', number=2)
    batch = await current.prepare(space_id=space['space_id'], source_ids=(neutral.source_id,), persona='独立人格')
    assert batch.relationship['state']['long_term']['body'] == old_text
    assert batch.prompt_version == 'extraction-prompt.v3'
    unchanged = output()
    unchanged.update(schema_version='extraction.v1', coverage_changes=[])
    await current.apply(batch, json.dumps(unchanged), model_id='fixture')
    relation = await memory.get_relationship(space_id=space['space_id'], logical_user_id=user)
    assert (relation['state']['long_term']['body'], relation['content_version']) == (old_text, 1)
    meaningful = await capture(sources, '这次先不解释，我以后再来聊。', number=3)
    batch = await current.prepare(space_id=space['space_id'], source_ids=(meaningful.source_id,), persona='独立人格')
    new_text = '我认定这份疏离不是偶然，心里有些失落，但仍愿意继续了解对方。'
    candidate = output(relationship={'logical_user_id': user, 'expected_version': 1, 'long_term': {'op': 'replace', 'body': new_text},
        'aftereffect': {'op': 'keep', 'body': ''}, 'source_ids': [meaningful.source_id]})
    candidate.update(schema_version='extraction.v1', coverage_changes=[])
    await current.apply(batch, json.dumps(candidate), model_id='fixture')
    relation = await memory.get_relationship(space_id=space['space_id'], logical_user_id=user)
    assert (relation['state']['long_term']['body'], relation['content_version']) == (new_text, 2)
    assert relation['state']['aftereffect']['body'] == ''
    assert await memory.list_nodes(space_id=space['space_id']) == []
    assert await memory.list_profiles(space_id=space['space_id']) == []
