"""Short new proposals preserve older states and use matching recovery limits."""
import copy
import hashlib
import json

import pytest

from core.extraction_apply import ExtractionService
from core.extraction_regeneration import validation_feedback
from core.extraction_schema import extraction_json_schema, parse_extraction_output
from core.prompts import build_extraction_prompt
from core.text_lengths import readable_text_length
from tests.test_extraction_apply import prepare
from tests.test_relationship_slots_runtime import stores, slot, output
from tests.test_regeneration_contract import complete_candidate


@pytest.mark.parametrize('part,maximum', [('long_term', 500), ('aftereffect', 150)])
def test_new_slot_limits_apply_to_readable_text_and_correction_feedback(part, maximum):
    value = complete_candidate('extraction.v1')
    body = '{{user:long_logical_identity}}' + '字' * (maximum - 1)
    value['relationship_change'][part] = slot('replace', body)
    assert readable_text_length(body) == maximum
    parse_extraction_output(json.dumps(value), schema_version='extraction.v1')
    value['relationship_change'][part]['body'] += '界'
    raw = json.dumps(value)
    with pytest.raises(ValueError, match='readable_too_long'):
        parse_extraction_output(raw, schema_version='extraction.v1')
    feedback = validation_feedback({'error_code': 'invalid_output', 'window': {'raw': raw}},
                                   schema_version='extraction.v1')
    issue, = feedback['errors']
    assert issue['path'] == f'$.relationship_change.{part}.body'
    assert issue['constraints']['x-readable-maxLength'] == maximum
    assert issue['actual_readable_chars'] == maximum + 1


@pytest.mark.asyncio
async def test_independent_replacements_keep_other_slots_and_their_source_clocks(stores):
    sources, memory, current = stores
    batch, source = await prepare(stores)
    old_body = '我愿意听完对方的顾虑，再一起作决定。'
    await current.apply(batch, json.dumps(output(source, 0,
        long_term=slot('replace', old_body), aftereffect=slot('replace', '有一点迟疑。'))), model_id='synthetic')
    original = await memory.get_relationship(space_id=batch.space['space_id'], logical_user_id=source.logical_user_id)
    batch, source = await prepare((sources, memory, current), number=2)
    value = output(source, 1, aftereffect=slot('replace', '我想缓缓再接近。'))
    value['schema_version'] = 'extraction.v1'
    await current.apply(batch, json.dumps(value), model_id='synthetic')
    kept = await memory.get_relationship(space_id=batch.space['space_id'], logical_user_id=source.logical_user_id)
    assert kept['state']['long_term'] == original['state']['long_term']
    assert kept['state']['aftereffect']['observed_at'] == 200
    batch, source = await prepare((sources, memory, current), number=3)
    value = output(source, 2, long_term=slot('replace', '我愿意相信这份体谅，同时保留独处的空间。'))
    value['schema_version'] = 'extraction.v1'
    await current.apply(batch, json.dumps(value), model_id='synthetic')
    compact = await memory.get_relationship(space_id=batch.space['space_id'], logical_user_id=source.logical_user_id)
    assert compact['state']['long_term']['body'] == value['relationship_change']['long_term']['body']
    assert compact['state']['long_term']['observed_at'] == 300
    assert compact['state']['aftereffect'] == kept['state']['aftereffect']
    versions = await memory.list_relationship_versions(compact['relationship_id'])
    assert len(versions) == 3 and versions[0]['state']['long_term']['body'] == old_body


def test_current_rules_reorganize_psychological_meaning_without_forced_edits():
    rules = build_extraction_prompt(persona='', sources=[])[0]['content'].split('\nJSON schema:\n')[0]
    assert '结合旧理解重新组织完整正文' in rules
    assert '失去心理意义的事实细节可以淡出' in rules
    assert '无实质变化仍可keep' in rules
    assert '最多500可读字符，aftereffect.body最多150' in rules
    assert '不设最低字数' in rules
