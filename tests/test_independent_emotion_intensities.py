"""Independent label scores survive extraction contracts, content versions."""
import copy
import json

import pytest
from pydantic import ValidationError

from core.extraction_schema import NodeEmotion
from core.memory_emotions import decode_emotion, encode_emotion
from core.topic_prompt import build_messages
from core.topic_schema import VERSION, parse_output
from tests.test_governance import setup_nodes, store  # noqa: F401
from tests.test_extraction_apply import stores  # noqa: F401
from tests.test_topic_schema import proposal


def independent():
    return {"tags": [{"tag": "joy", "intensity": 0.8}, {"tag": "concern", "intensity": 0.2}],
            "description": "我既期待共同安排，也担心时间不够。"}


def test_each_emotion_label_requires_its_own_score():
    value = independent()
    assert NodeEmotion.model_validate(value).model_dump() == value
    assert decode_emotion(encode_emotion(value)) == value
    for bad in (
        {"tags": ["joy"], "intensity": .8, "description": "旧格式"},
        {**value, "intensity": .8},
        {**value, "tags": [{"tag": "joy", "intensity": .8}, {"tag": "joy", "intensity": .2}]},
        {**value, "tags": [{"tag": "neutral", "intensity": 0}, {"tag": "joy", "intensity": .2}]},
        {**value, "tags": [{"tag": "joy", "intensity": True}]},
    ):
        with pytest.raises(ValidationError):
            NodeEmotion.model_validate(bad)


@pytest.mark.asyncio
async def test_changing_one_tag_preserves_the_other_and_versions_content(store):
    space, _ = await setup_nodes(store)
    original = await store.create_node(space_id=space["space_id"], node_type="event",
        content_text="合成共同安排。", source_ids=["source"], emotion=independent())
    changed = independent()
    changed["tags"][1]["intensity"] = .6
    updated = await store.update_node_content(original["node_id"], original["content_text"], emotion=changed)
    assert updated["content_version"] == 2
    assert updated["emotion"]["tags"] == [{"tag": "joy", "intensity": .8}, {"tag": "concern", "intensity": .6}]
    assert [v["emotion"] for v in await store.list_node_versions(original["node_id"])] == [independent(), changed]


def test_topic_schema_and_prompt_require_independent_intensities():
    body = {'schema_version': VERSION, 'changes': [proposal(emotion=independent())]}
    assert parse_output(json.dumps(body), expected_schema_version=VERSION)['changes'][0]['emotion'] == independent()
    legacy_emotion = {'tags': ['joy'], 'intensity': .3, 'description': '旧情绪'}
    body['changes'][0]['emotion'] = legacy_emotion
    with pytest.raises(ValidationError):
        parse_output(json.dumps(body), expected_schema_version=VERSION)
    current = build_messages('合成人格', [], [])
    assert json.loads(current[1]['content'])['schema_version'] == VERSION
    assert '每个intensity独立' in current[0]['content']


@pytest.mark.asyncio
async def test_joint_extraction_preserves_two_scores_and_feedback_targets_one(stores):
    from core.extraction_apply import ExtractionService
    from core.extraction_regeneration import validation_feedback
    from tests.test_extraction_apply import node, output, prepare

    sources, memory, _ = stores
    service = ExtractionService(sources, memory)
    batch, source = await prepare((sources, memory, service))
    change = {**node(source.source_id), 'title': '合成准备', 'base_node_ids': [], 'emotion': independent()}
    value = {**output([change]), 'schema_version': 'extraction.v1', 'coverage_changes': []}
    bad = copy.deepcopy(value)
    bad['node_changes'][0]['emotion']['tags'][1]['intensity'] = 1.5
    feedback = validation_feedback({'window': {'raw': json.dumps(bad)}, 'error_code': 'invalid_output'}, schema_version='extraction.v1')
    error = next(item for item in feedback['errors'] if item['path'].endswith('tags[1].intensity'))
    assert error['constraints']['maximum'] == 1
    bad['node_changes'][0]['emotion']['tags'] = [
        {'tag': 'joy', 'intensity': .3}, {'tag': 'joy', 'intensity': .8}]
    feedback = validation_feedback({'window': {'raw': json.dumps(bad)}, 'error_code': 'invalid_output'}, schema_version='extraction.v1')
    duplicate = next(item for item in feedback['errors'] if item['type'] == 'duplicate_values')
    assert duplicate['violations'] == [{'path': '$.node_changes[0].emotion.tags',
        'actual_items': 2, 'distinct_items': 1, 'constraints': {'unique_by': 'tag'}}]
    receipt = await service.apply(batch, json.dumps(value), model_id='synthetic')
    saved = await memory.get_node(receipt['result']['node_refs']['local_interview'])
    assert saved['emotion'] == independent()
