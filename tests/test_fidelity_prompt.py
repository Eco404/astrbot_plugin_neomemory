"""Version identity must follow prepared inputs through rendering and receipt replay."""

import json
from dataclasses import asdict, replace

import pytest

from core.extraction_apply import ExtractionService, PreparedExtraction
from core.identity import EventIdentity
from core.memory_spaces import private_space
from core.prompts import PROMPT_VERSION, build_extraction_prompt
from core.source_ingest import CapturedSource
from storage.memory_store import MemoryStore
from storage.source_store import SourceStore
def test_default_narrative_removes_memory_bookkeeping_from_positive_examples():
    inputs = {
        "persona": "合成陪伴者",
        "sources": [{
            "source_id": "s1", "actor_kind": "user", "actor_logical_user_id": "u1",
            "content_text": "我修好了自行车。", "observed_at": 100.0,
        }],
    }
    candidate = build_extraction_prompt(**inputs)
    rules = candidate[0]["content"]
    assert PROMPT_VERSION == "extraction-prompt.v3-dev.1"
    assert candidate == build_extraction_prompt(**inputs, prompt_version=PROMPT_VERSION)
    assert candidate == build_extraction_prompt(**inputs, schema_version="extraction.v1")
    assert "schema_version=extraction.v1" in rules
    assert "【怎样写自己的记忆】" in rules
    assert "【人格视角与人物指代】" not in rules
    assert "用户自述的“我”仍属于该逻辑用户" in rules
    assert "不用“我记得”“我提过”“我知道”“我会记住”等记忆管理前缀" in rules
    assert "已失效的旧状态不与新状态并列成当前事实" in rules
    assert "记忆管理前缀" in rules


def test_current_default_preserves_persona_voice():
    inputs = {
        "persona": "我是岚笺，负责陪伴和倾听。",
        "sources": [{
            "source_id": "s1", "actor_kind": "agent", "actor_logical_user_id": "u1",
            "content_text": "我答应陪对方看展。", "observed_at": 100.0,
        }],
    }
    baseline = build_extraction_prompt(**inputs)
    assert baseline == build_extraction_prompt(**inputs, prompt_version=PROMPT_VERSION)
    assert "【身份、来源与输入】" in baseline[0]["content"]
    assert "其他空间的往事" in baseline[0]["content"]
    released = build_extraction_prompt(**inputs, prompt_version="extraction-prompt.v1")
    assert "其他空间的往事" not in released[0]["content"]
    rules = baseline[0]["content"]
    for required in (
        "agent 是当前人格", "不用人格名作第三人称主语", "不泛称“用户”",
        "事实与状态以 known_nodes/known_profiles 为基线，关系心理以 known_relationship 为基线",
        "两段都写我脑中自然展开的心里话", "必要引语和专有名词保留原文",
    ):
        assert required in rules


def test_persona_narrative_contract_preserves_data_schema_and_factual_boundaries():
    inputs = {
        "persona": "合成陪伴者",
        "sources": [{
            "source_id": "s1", "actor_kind": "user", "logical_user_id": "u1",
            "content_text": "请继续叫我小舟。", "observed_at": 100.0,
        }],
    }
    messages = build_extraction_prompt(**inputs)
    rules = messages[0]["content"]
    source, = json.loads(messages[1]["content"])["sources"]
    assert all(source[k] == v for k,v in inputs["sources"][0].items())
    for required in (
        "各类只生成一份完整、可独立阅读和直接注入的正文",
        "记忆管理前缀", "{{user:<logical_user_id>}}", "{{user:u1}}",
        "ID和旧版本号逐字取自输入", "明确约定的昵称、专属称呼、必要引语和专有名词保留原文",
        "画像是对方当前仍成立、以后相处有用途的轻量资料，不是我的偏好、评价或经历清单",
        "两段都写我脑中自然展开的心里话", "由persona和我的自主判断决定",
        "可以延续、调整或淡出", "本批变化的来源必须来自当前 sources",
        "按 actor_kind、actor_logical_user_id 和原文确定谁说话",
        "第三方保留原有称谓", "用户自述的“我”仍属于该逻辑用户",
        "我的动作、发言和感受用“我/我的”",
    ):
        assert required in rules


@pytest.mark.parametrize(
    ("schema_version", "prompt_version"),
    [
        ("extraction.v1", "extraction-prompt.v3"),
        ("extraction.v1", "extraction-prompt.v5"),
        ("extraction.v1", "extraction-prompt.v6"),
        ("extraction.v1", "extraction-prompt.v7"),
        ("extraction.v2", "extraction-prompt.v1"),
        ("extraction.v2", "extraction-prompt.v4"),
        ("extraction.v2", "extraction-prompt.v99"),
        ("extraction.v99", "extraction-prompt.v5"),
    ],
)
def test_unsupported_template_or_schema_never_falls_back(schema_version, prompt_version):
    with pytest.raises(ValueError, match="unsupported"):
        build_extraction_prompt(
            persona="synthetic", sources=[], schema_version=schema_version,
            prompt_version=prompt_version,
        )


def test_prepared_version_is_part_of_input_and_survives_snapshot_roundtrip():
    current = PreparedExtraction(
        "batch_test", {"space_id": "private_test"}, "gen_test", 0, (), (), (), None, "synthetic"
    )
    candidate = current
    restored = PreparedExtraction(**asdict(candidate))
    assert restored.prompt_version == "extraction-prompt.v3-dev.1"
    assert restored.input_digest == candidate.input_digest
    assert restored.messages() == candidate.messages()
    with pytest.raises(ValueError, match="unsupported"):
        replace(candidate, prompt_version="extraction-prompt.v4")
    with pytest.raises(ValueError, match="explicit prompt version"):
        replace(candidate, prompt_version=None)


def test_service_rejects_unknown_prompt_before_any_storage_access():
    with pytest.raises(ValueError, match="unsupported"):
        ExtractionService(None, None, prompt_version="extraction-prompt.v4")


@pytest.mark.asyncio
async def test_prepare_commit_and_replay_preserve_version(tmp_path):
    version, schema = "extraction-prompt.v1", "extraction.v1"
    sources = SourceStore(tmp_path / "source.db")
    memory = MemoryStore(tmp_path / "memory.db", source_exists=sources.has_source)
    await sources.open()
    await memory.open()
    try:
        captured = await sources.capture(CapturedSource(
            identity=EventIdentity(
                "synthetic", "test", "FriendMessage", "synthetic:user", "user", "",
                "user", "user", "agent", "message-1",
            ),
            actor_kind="user", content_text="早上好", observed_at=100.0,
            actor_account_id="user", persona_id="synthetic-persona",
        ))
        space = await memory.get_or_create_space(
            private_space("synthetic-persona", captured.logical_user_id)
        )
        inputs = {
            "space_id": space["space_id"], "source_ids": (captured.source_id,),
            "persona": "合成陪伴者",
        }
        candidate_service = ExtractionService(
            sources, memory, prompt_version=version
        )
        candidate = await candidate_service.prepare(**inputs)
        assert candidate.prompt_version == version
        response = json.dumps({
            "schema_version": schema, "result": "no_change",
            "reason": "no_meaningful_content", "node_changes": [], "profile_changes": [],
            "edges": [], "coverage_changes": [], "relationship_change": None,
        })
        await candidate_service.apply(candidate, response, model_id="fake")
        receipt = await memory.get_extraction_batch(candidate.batch_id)
        assert receipt["prompt_version"] == version
        assert receipt["input_digest"] == candidate.input_digest
        assert receipt["activity_round"] == 1

        async def forbidden_model(_messages):
            raise AssertionError("completed batch must not call a model")

        replayed = await candidate_service.run(candidate, forbidden_model, model_id="fake")
        assert replayed["replayed"] is True
        with pytest.raises(ValueError, match="completed batch has different input"):
            await candidate_service.run(replace(candidate, persona="另一人格文本"), forbidden_model, model_id="fake")
    finally:
        await memory.close()
        await sources.close()
