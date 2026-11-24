"""The current joint protocol keeps independent subjective slots and tag scores."""
import json

import pytest

from core.config import NeoMemoryConfig
from core.extraction_schema import extraction_json_schema, parse_extraction_output, SCHEMA_VERSION
from core.prompts import build_extraction_prompt, PROMPT_VERSION, schema_for_prompt_version
from core.relationship_state import LONG_TERM_MAX_CHARS, AFTEREFFECT_MAX_CHARS
from tests.test_regeneration_contract import complete_candidate


def test_default_prompt_matches_slot_and_emotion_schema():
    messages = build_extraction_prompt(persona='合成人格', sources=[])
    system = messages[0]['content'].split('\nJSON schema:\n')[0]
    assert PROMPT_VERSION == 'extraction-prompt.v3' and SCHEMA_VERSION == 'extraction.v1'
    assert schema_for_prompt_version(PROMPT_VERSION) == SCHEMA_VERSION
    assert 'state_text' not in system and 'emotion_intensity' not in system
    assert '{tags,intensity,description}' not in system
    assert '每个标签的intensity独立' in system
    assert f'long_term.body最多{LONG_TERM_MAX_CHARS}' in system
    assert f'aftereffect.body最多{AFTEREFFECT_MAX_CHARS}' in system
    schema = extraction_json_schema()
    assert schema['$defs']['LongTermRelationshipUpdate']['properties']['body']['x-readable-maxLength'] == LONG_TERM_MAX_CHARS
    assert schema['$defs']['AftereffectRelationshipUpdate']['properties']['body']['x-readable-maxLength'] == AFTEREFFECT_MAX_CHARS
    assert set(schema['$defs']['NodeEmotion']['properties']) == {'tags','description'}


def test_slot_contract_has_no_minimum_but_keep_clear_cannot_replace_text():
    candidate = complete_candidate()
    relation = candidate['relationship_change']
    relation['long_term'] = {'op':'replace','body':'我愿意。'}
    relation['aftereffect'] = {'op':'clear','body':''}
    parse_extraction_output(json.dumps(candidate))
    relation['long_term'] = {'op':'keep','body':'不能偷偷改写。'}
    with pytest.raises(ValueError, match='relationship_slot_body'):
        parse_extraction_output(json.dumps(candidate))


def test_config_exposes_one_day_aftereffect_and_whole_relationship_budget():
    config = NeoMemoryConfig.from_mapping({})
    assert config.relationship_aftereffect_seconds == 86400
    assert config.context_relationship_max_chars >= LONG_TERM_MAX_CHARS + AFTEREFFECT_MAX_CHARS
    assert NeoMemoryConfig.from_mapping({'relationship_aftereffect_seconds':0}).relationship_aftereffect_seconds == 0
