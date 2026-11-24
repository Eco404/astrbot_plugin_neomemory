"""Exercise incremental model output through the ordinary and scoped write paths."""
from dataclasses import replace
import json

import pytest

from core.extraction_references import ExtractionReferences
from tests.test_extraction_apply import capture, output
from tests.test_extraction_references import batch
from tests.test_maintenance_engine import environment, enqueue_and_claim, execute


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["extract", "profile_rebuild", "context_rebuild"])
async def test_profile_edits_preserve_frozen_details_and_history(environment, kind):
    env = environment
    space, user = env.space["space_id"], env.space["logical_user_id"]
    old = "在合唱团担任指挥。只在室内练习。"
    if kind == "extract":
        await env.memory.upsert_profile(space_id=space, logical_user_id=user,
                                        description=old, tags=["指挥", "室内练习"])
    else:
        for i in range(3, 5):
            await capture(env.sources, number=i)

    calls = []
    async def model(messages, **opts):
        data = json.loads(messages[1]["content"])
        declared = json.loads(messages[0]["content"].split("\nJSON schema:\n")[1])
        assert opts["output_schema"] == declared
        assert {"remove_tags", "remove_sentences"} <= set(declared["$defs"]["ProfileChange"]["required"])
        first = not data["known_profiles"]
        if first:
            change = {"description": old, "tags": ["指挥", "室内练习"], "remove_tags": [], "remove_sentences": []}
        else:
            previous, = data["known_profiles"]
            assert previous["description_sentences"] == ["在合唱团担任指挥。", "只在室内练习。"]
            assert "description" not in previous
            change = {"description": "雨天在室内练习。", "tags": ["雨天室内练习"],
                      "remove_sentences": [2], "remove_tags": [2]}
        calls.append(first)
        change.update(logical_user_id="u1", source_ids=["s1"])
        answer = {"profile_changes": [change]}
        if kind == "extract":
            answer = output(profiles=[change])
        elif kind == "context_rebuild":
            answer["relationship_change"] = None
        return json.dumps(answer, ensure_ascii=False)

    env.adapter.generate = model
    result = await execute(env, await enqueue_and_claim(env, kind))
    assert result["state"] == "succeeded", (result["error_code"], result["checkpoint"])
    assert calls == ([False] if kind == "extract" else [True, False])
    profile, = await env.memory.list_profiles(space_id=space)
    assert profile["tags"] == ["指挥", "雨天室内练习"]
    assert profile["description"] == "在合唱团担任指挥。雨天在室内练习。"
    assert profile["content_version"] == 2
    versions = await env.memory.list_profile_versions(profile["profile_id"])
    assert sorted(v["version_no"] for v in versions) == [1, 2]


@pytest.mark.parametrize("version,expected", [
    ("extraction-prompt.v2", "参加器乐团。"),
    ("extraction-prompt.v3", "An instrument owner. 参加器乐团。"),
])
def test_text_output_under_frozen_prompt_keeps_protocol_semantics(batch, version, expected):
    refs = ExtractionReferences(replace(batch, prompt_version=version))
    change = {"logical_user_id": "u2", "tags": [], "description": "参加器乐团。", "source_ids": ["s2"]}
    canonical = json.loads(refs.decode(json.dumps({"profile_changes": [change]}), scoped_kind="profile_rebuild"))
    assert canonical["profile_changes"][0]["description"] == expected


@pytest.mark.asyncio
async def test_invalid_index_corrects_against_same_profile_before_writing(environment):
    env = environment
    env.engine.config = replace(env.config, extraction_auto_regeneration=True)
    space = env.space["space_id"]
    await env.memory.upsert_profile(space_id=space, logical_user_id=env.space["logical_user_id"],
                                    description="在社区剧团负责灯光。", tags=["灯光"])
    calls = []
    async def model(messages, **opts):
        data = json.loads(messages[1]["content"])
        old, = data["known_profiles"]
        calls.append(old)
        if len(calls) == 2:
            feedback, = json.loads(messages[-1]["content"])["errors"]
            assert feedback["type"] == "profile_edit_index"
            assert feedback["path"] == "$.profile_changes[0].remove_sentences[0]"
            assert feedback["constraints"]["maximum"] == 1
        return json.dumps(output(profiles=[{"logical_user_id": "u1", "source_ids": ["s1"],
            "tags": [], "description": "周末才排练。", "remove_tags": [],
            "remove_sentences": [2] if len(calls) == 1 else []}]))

    env.adapter.generate = model
    result = await execute(env, await enqueue_and_claim(env))
    assert result["state"] == "succeeded", (result["error_code"], result["checkpoint"])
    assert len(calls) == 2 and calls[0] == calls[1]
    profile, = await env.memory.list_profiles(space_id=space)
    assert profile["content_version"] == 2 and profile["tags"] == ["灯光"]
    assert profile["description"] == "在社区剧团负责灯光。周末才排练。"
