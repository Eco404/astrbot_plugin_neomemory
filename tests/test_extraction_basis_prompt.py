"""The extraction prompt and schema agree on inheritance and standalone prose."""
from core.extraction_schema import SCHEMA_VERSION, extraction_json_schema
from core.prompts import (
    PROMPT_VERSION, build_extraction_prompt, resolve_extraction_prompt_version, schema_for_prompt_version,
)


def test_prompt_declares_the_same_basis_contract_as_the_validator():
    assert resolve_extraction_prompt_version() == PROMPT_VERSION == "extraction-prompt.v2-dev.1"
    assert schema_for_prompt_version(PROMPT_VERSION) == SCHEMA_VERSION == "extraction.v1"
    schema = extraction_json_schema()
    assert "base_node_ids" in schema["$defs"]["NodeChange"]["required"]
    rules = build_extraction_prompt(persona="合成人格", sources=[])[0]["content"]
    assert "schema_version=extraction.v1" in rules
    assert "不会自动把依据节点的正文展开" in rules
