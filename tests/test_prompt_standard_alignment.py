"""Initial standards and authoritative identity scope are shared across the pipeline."""

import json

import pytest

from core import prompts, short_term_prompts, topic_prompt, topic_schema
from core.extraction_apply import digest
from core.extraction_schema import MAX_OUTPUT_CHARS
from core.topic_budget import fit_topic_evidence, message_bytes
from storage.topic_store import TopicStore
from tests.test_maintenance_engine import environment, enqueue_and_claim
from tests.test_topic_maintenance import candidate, seed


def test_initial_capacity_units_and_identity_scope_are_explicit():
    extraction = prompts.build_extraction_prompt(persona="", sources=[])[0]["content"]
    assert "prompt_version=extraction-prompt.v1" in extraction
    assert f"{MAX_OUTPUT_CHARS}个编码字符" in extraction
    assert "不是UTF-8字节数" in extraction and "稳定引用展开" in extraction
    messages = topic_prompt.build_messages("", [], [], allowed_user_ids=["known_member", "space_user"])
    payload = json.loads(messages[1]["content"])
    assert payload["prompt_version"] == topic_prompt.PROMPT_VERSION == "topic-prompt.v1"
    assert payload["allowed_user_ids"] == ["u1", "u2"]
    assert f"{topic_schema.MAX_OUTPUT_CHARS}个编码字符" in messages[0]["content"]
    summary = short_term_prompts.messages(persona="", previous_summary="", records=[], summary_chars=227)
    assert f"{short_term_prompts.MAX_OUTPUT_BYTES}个UTF-8字节" in summary[0]["content"]
    assert "{{user:u1}}占11个字符" in summary[0]["content"]
    assert "stored_reference_chars" not in json.loads(summary[1]["content"])
    assert short_term_prompts.output_schema(source_ids=[], summary_chars=227)["properties"]["summary_text"]["maxLength"] == 227


def test_topic_budget_counts_the_same_fixed_identity_payload():
    identities = ["logical_" + str(index) for index in range(12)]
    messages = topic_prompt.build_messages("", [], [], allowed_user_ids=identities)
    limit = message_bytes(messages)
    fitted, diagnostics = fit_topic_evidence("", [], [], [], limit, allowed_user_ids=identities)
    assert fitted == []
    assert diagnostics["prompt_bytes"] == limit
    fitted, diagnostics = fit_topic_evidence("", [], [], [], limit - 1, allowed_user_ids=identities)
    assert fitted is None and diagnostics["base_prompt_bytes"] == limit


@pytest.mark.asyncio
async def test_topic_freezes_every_authoritative_identity_source(environment):
    env = environment
    nodes = await seed(env)
    nodes[0] = await env.memory.update_node_content(nodes[0]["node_id"],
        content_text="{{user:prose_member}}提出一个合成活动。")
    await env.memory.add_node_participant(nodes[1]["node_id"], participant_kind="logical_user",
                                          participant_id="participant_only")
    store = TopicStore(env.memory)
    generation = await env.memory.get_active_generation(env.space["space_id"])
    prepared = await store.prepare(env.space["space_id"], generation["generation_id"],
                                   [node["node_id"] for node in nodes])
    expected = {env.space["logical_user_id"], "prose_member", "participant_only"}
    assert set(prepared["allowed_user_ids"]) == expected
    assert prepared["input_digest"] == digest({k: v for k, v in prepared.items() if k != "input_digest"})
    messages = topic_prompt.build_messages("", prepared["nodes"], prepared["edges"],
                                           allowed_user_ids=prepared["allowed_user_ids"])
    assert json.loads(messages[1]["content"])["allowed_user_ids"] == ["u1", "u2", "u3"]
    job = await enqueue_and_claim(env, "topic_consolidate")
    output = json.loads(candidate(nodes))
    output["changes"][0]["content"] = "{{user:participant_only}}参与了合成活动的持续安排。"
    result = await store.apply(job, prepared, output)
    assert len(result["topic_ids"]) == 1


@pytest.mark.asyncio
async def test_topic_identity_change_requires_new_preparation(environment):
    env = environment
    nodes = await seed(env)
    store = TopicStore(env.memory)
    generation = await env.memory.get_active_generation(env.space["space_id"])
    prepared = await store.prepare(env.space["space_id"], generation["generation_id"],
                                   [node["node_id"] for node in nodes])
    await env.memory.add_node_participant(nodes[0]["node_id"], participant_kind="logical_user",
                                          participant_id="new_participant")
    job = await enqueue_and_claim(env, "topic_consolidate")
    with pytest.raises(ValueError, match="topic context changed"):
        await store.apply(job, prepared, json.loads(candidate(nodes)))
    assert not await env.memory.database.fetchall("SELECT * FROM topic_receipts")
