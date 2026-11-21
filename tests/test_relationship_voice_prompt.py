"""The joint request carries subjective relationship prose and source grounding."""
from core.prompts import build_extraction_prompt
from core.relationship_slots_prompt import COMPACT_RELATIONSHIP_RULES, SLOT_BODY_RULE


def test_relationship_voice_and_operation_rules_are_grounded_in_the_same_request():
    rules = build_extraction_prompt(persona="合成人格", sources=[])[0]["content"]
    assert COMPACT_RELATIONSHIP_RULES in rules
    assert SLOT_BODY_RULE in rules
    assert "【来源观察与主题组织】" in rules
