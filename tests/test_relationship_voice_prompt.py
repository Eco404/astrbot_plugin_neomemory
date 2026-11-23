"""The joint request carries subjective relationship prose and source grounding."""
from core.context_rebuild import scoped_messages
from core.prompts import PROMPT_VERSION, build_extraction_prompt
from core.relationship_slots_prompt import RELATIONSHIP_RULES, SLOT_BODY_RULE


def test_relationship_voice_and_operation_rules_are_grounded_in_the_same_request():
    rules = build_extraction_prompt(persona="合成人格", sources=[])[0]["content"]
    assert RELATIONSHIP_RULES in rules
    assert SLOT_BODY_RULE in rules
    assert "【来源观察与主题组织】" in rules


def test_relationship_replay_uses_the_same_versioned_detail_guidance():
    for version in ("extraction-prompt.v2", PROMPT_VERSION):
        messages = build_extraction_prompt(persona="p", sources=[], prompt_version=version)
        scoped = scoped_messages(messages, "context_rebuild", {}, prompt_version=version)
        for request in (messages, scoped):
            assert ("把那个细节留在心里话里" in request[0]["content"]) == (version == PROMPT_VERSION)
