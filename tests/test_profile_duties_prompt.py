"""Profile extraction and selective replay share the same subject and granularity rules."""
import pytest

from core.context_rebuild import MODELS, scoped_messages
from core.profile_memory_prompt import PROFILE_DUTIES_RULES
from core.prompts import PROMPT_VERSION, build_extraction_prompt


@pytest.mark.parametrize("kind", [None, "profile_rebuild", "relationship_rebuild", "context_rebuild"])
def test_profile_duties_are_present_only_when_profiles_are_in_scope(kind):
    messages = build_extraction_prompt(persona="示例", sources=[])
    if kind:
        messages = scoped_messages(messages, kind, MODELS[kind].model_json_schema(), prompt_version=PROMPT_VERSION)
    assert messages[0]["content"].count(PROFILE_DUTIES_RULES) == (0 if kind == "relationship_rebuild" else 1)
