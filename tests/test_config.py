"""Host settings and runtime resource budgets must agree and remain finite."""

import json
import math
from pathlib import Path

import pytest

from core.config import NeoMemoryConfig
from core.extraction_window import WindowPolicy
from core.retrieval import RecallPolicy

SCHEMA = json.loads(
    (Path(__file__).resolve().parents[1] / "_conf_schema.json").read_text()
)
THRESHOLD_NAMES = {
    f"{prefix}_{kind}_messages"
    for prefix in ("private", "group")
    for kind in ("min", "topic", "max")
} | {"short_term_min_messages", "short_term_max_messages"}


def test_schema_defaults_match_runtime_and_preserve_capture_options():
    schema_defaults = {key: metadata["default"] for key, metadata in SCHEMA.items()}
    assert NeoMemoryConfig().as_dict() == schema_defaults
    assert NeoMemoryConfig.from_mapping(schema_defaults).as_dict() == schema_defaults
    assert NeoMemoryConfig.from_mapping(None).as_dict() == schema_defaults
    assert RecallPolicy().deadline_seconds == schema_defaults["recall_deadline_seconds"]
    assert RecallPolicy().budget_tokens == schema_defaults["recall_budget_tokens"]
    assert (
        NeoMemoryConfig.from_mapping({"unknown_future_option": True}).as_dict()
        == schema_defaults
    )
    assert {
        key: schema_defaults[key]
        for key in (
            "capture_private_messages",
            "capture_group_messages",
            "capture_agent_messages",
            "max_content_chars",
            "write_batch_size",
            "write_flush_timeout_ms",
        )
    } == {
        "capture_private_messages": True,
        "capture_group_messages": True,
        "capture_agent_messages": True,
        "max_content_chars": 0,
        "write_batch_size": 1,
        "write_flush_timeout_ms": 5000,
    }


def test_short_term_window_bounds_must_remain_ordered():
    for values in ({"short_term_min_messages": 41}, {"short_term_max_messages": 1}):
        cfg = NeoMemoryConfig.from_mapping(values)
        assert (cfg.short_term_min_messages, cfg.short_term_max_messages) == (6, 40)
    cfg = NeoMemoryConfig.from_mapping({"short_term_min_messages": 1, "short_term_max_messages": 1})
    assert (cfg.short_term_min_messages, cfg.short_term_max_messages) == (1, 1)


def test_context_priority_preserves_old_byte_budget_and_parses_explicit_choice():
    cfg = NeoMemoryConfig.from_mapping({"recall_budget_tokens": 2048})
    assert cfg.recall_budget_tokens == 2048 and cfg.context_budget_priority == "recall_first"
    assert NeoMemoryConfig.from_mapping({"context_budget_priority": "context_first"}).context_budget_priority == "context_first"
    assert NeoMemoryConfig.from_mapping({"context_budget_priority": "invalid"}).context_budget_priority == "recall_first"


@pytest.mark.parametrize(
    "name",
    [
        name
        for name, meta in SCHEMA.items()
        if "slider" in meta and name not in THRESHOLD_NAMES
    ],
)
def test_runtime_bounds_match_host_controls(name):
    metadata = SCHEMA[name]
    lower, upper = metadata["slider"]["min"], metadata["slider"]["max"]
    assert getattr(NeoMemoryConfig.from_mapping({name: -1_000_000_000}), name) == lower
    assert getattr(NeoMemoryConfig.from_mapping({name: upper + 1}), name) == upper
    assert lower <= metadata["default"] <= upper
    parsed = getattr(
        NeoMemoryConfig.from_mapping({name: str(metadata["default"])}), name
    )
    assert parsed == metadata["default"]
    assert type(parsed) is (int if metadata["type"] == "int" else float)


@pytest.mark.parametrize(
    "bad_value",
    [None, "invalid", float("nan"), float("inf"), -float("inf"), True, {}, []],
)
def test_invalid_numeric_settings_fall_back_without_nonfinite_budgets(bad_value):
    names = [name for name, meta in SCHEMA.items() if meta["type"] in {"int", "float"}]
    parsed = NeoMemoryConfig.from_mapping(dict.fromkeys(names, bad_value))
    for name in names:
        assert getattr(parsed, name) == SCHEMA[name]["default"]
        assert math.isfinite(getattr(parsed, name))


def test_explicit_provider_ids_are_trimmed_and_never_invented():
    parsed = NeoMemoryConfig.from_mapping(
        {
            "llm_provider_id": "  configured/chat-model  ",
            "embedding_provider_id": "\tembedding-local\n",
            "embedding_revision": " new-revision ",
            "extraction_model_revision": " extraction-v2 ",
        }
    )
    assert parsed.llm_provider_id == "configured/chat-model"
    assert parsed.embedding_provider_id == "embedding-local"
    assert parsed.embedding_revision == "new-revision"
    assert parsed.extraction_model_revision == "extraction-v2"
    invalid = NeoMemoryConfig.from_mapping(
        {
            "llm_provider_id": None,
            "embedding_provider_id": 123,
            "embedding_revision": {},
            "extraction_model_revision": [],
        }
    )
    assert (
        invalid.llm_provider_id
        == invalid.embedding_provider_id
        == invalid.embedding_revision
        == ""
    )
    assert invalid.extraction_model_revision == ""
    # The v4.29.3 renderer only provides a chat model selector for this special
    # value. It has no supported embedding selector special value.
    assert SCHEMA["llm_provider_id"]["_special"] == "select_provider"
    assert "_special" not in SCHEMA["embedding_provider_id"]


def test_feature_toggles_and_history_limits_can_disable_optional_work():
    parsed = NeoMemoryConfig.from_mapping(
        {
            "enabled": "true",
            "recall_enabled": "false",
            "tools_enabled": False,
            "history_seconds": 0,
            "history_limit": 0,
            "user_weight": 0,
            "agent_weight": 0,
            "recall_max_neighbors": 0,
            "recall_time_neighbor_seconds": 0,
            "cache_message_limit": 0,
            "cache_node_limit": 0,
        }
    )
    assert "enabled" not in parsed.as_dict()
    assert parsed.recall_enabled is parsed.tools_enabled is False
    assert parsed.history_seconds == 1 and parsed.history_limit == 0
    assert parsed.user_weight == parsed.agent_weight == 0
    assert parsed.recall_max_neighbors == 0 and parsed.recall_time_neighbor_seconds == 1
    assert parsed.cache_message_limit == 0 and parsed.cache_node_limit == 1


def test_serialized_config_is_an_independent_roundtrippable_mapping():
    parsed = NeoMemoryConfig.from_mapping(
        {"embedding_batch_size": 1, "llm_concurrency": 3}
    )
    serialized = parsed.as_dict()
    assert NeoMemoryConfig.from_mapping(json.loads(json.dumps(serialized))) == parsed
    serialized["llm_concurrency"] = 99
    assert parsed.llm_concurrency == 3


def window_policy(config, prefix):
    return WindowPolicy(
        min_messages=getattr(config, f"{prefix}_min_messages"),
        topic_messages=getattr(config, f"{prefix}_topic_messages"),
        max_messages=getattr(config, f"{prefix}_max_messages"),
        idle_seconds=getattr(config, f"{prefix}_idle_seconds"),
        max_chars=config.extraction_max_chars,
        max_tokens=config.extraction_max_tokens,
    )


@pytest.mark.parametrize("prefix", ["private", "group"])
@pytest.mark.parametrize("value,expected", [(-1_000_000_000, 1), (1_000_000_000, 200)])
def test_coupled_threshold_bounds_use_a_valid_combination(prefix, value, expected):
    names = [f"{prefix}_{kind}_messages" for kind in ("min", "topic", "max")]
    config = NeoMemoryConfig.from_mapping(dict.fromkeys(names, value))
    assert [getattr(config, name) for name in names] == [expected] * 3
    for name in names:
        assert SCHEMA[name]["slider"]["min"] == 1
        assert SCHEMA[name]["slider"]["max"] == 200
        assert type(getattr(config, name)) is int
    assert window_policy(config, prefix).max_messages == expected


@pytest.mark.parametrize("prefix", ["private", "group"])
@pytest.mark.parametrize("thresholds", [(20, 10, 30), (5, 50, 40), (200, 10, 1)])
def test_unordered_thresholds_reset_entire_group_without_reordering(prefix, thresholds):
    names = [f"{prefix}_{kind}_messages" for kind in ("min", "topic", "max")]
    other = "private" if prefix == "group" else "group"
    custom_other = {
        f"{other}_{kind}_messages": value
        for kind, value in zip(("min", "topic", "max"), (2, 4, 8), strict=True)
    }
    config = NeoMemoryConfig.from_mapping(
        {**custom_other, **dict(zip(names, thresholds, strict=True))}
    )
    assert [getattr(config, name) for name in names] == [
        SCHEMA[name]["default"] for name in names
    ]
    assert window_policy(config, other).min_messages == 2
    assert window_policy(config, other).topic_messages == 4
    assert window_policy(config, other).max_messages == 8
    window_policy(config, prefix)


@pytest.mark.parametrize("prefix", ["private", "group"])
def test_single_changed_threshold_cannot_break_window_construction(prefix):
    config = NeoMemoryConfig.from_mapping({f"{prefix}_min_messages": 200})
    assert window_policy(config, prefix) == window_policy(NeoMemoryConfig(), prefix)
    config = NeoMemoryConfig.from_mapping({f"{prefix}_max_messages": 1})
    assert window_policy(config, prefix) == window_policy(NeoMemoryConfig(), prefix)


@pytest.mark.parametrize("prefix", ["private", "group"])
def test_valid_custom_thresholds_and_zero_idle_remain_configurable(prefix):
    config = NeoMemoryConfig.from_mapping(
        {
            f"{prefix}_min_messages": "3",
            f"{prefix}_topic_messages": "9",
            f"{prefix}_max_messages": "27",
            f"{prefix}_idle_seconds": 0,
            "extraction_max_chars": 5000,
            "extraction_max_tokens": 6000,
        }
    )
    assert window_policy(config, prefix) == WindowPolicy(
        min_messages=3,
        topic_messages=9,
        max_messages=27,
        idle_seconds=0,
        max_chars=5000,
        max_tokens=6000,
    )


def test_default_runtime_windows_match_established_extraction_policy():
    config = NeoMemoryConfig()
    assert window_policy(config, "private") == WindowPolicy()
    assert window_policy(config, "group") == WindowPolicy.for_group()


def test_automatic_extraction_toggle_does_not_disable_manual_task_configuration():
    config = NeoMemoryConfig.from_mapping(
        {
            "automatic_extraction_enabled": "false",
            "llm_provider_id": "configured-model",
            "job_concurrency": 3,
            "job_max_attempts": 1,
        }
    )
    assert config.automatic_extraction_enabled is False
    assert "enabled" not in config.as_dict()
    assert config.llm_provider_id == "configured-model"
    assert config.job_concurrency == 3 and config.job_max_attempts == 1
    assert config.capture_private_messages and config.recall_enabled
    assert NeoMemoryConfig.from_mapping({}).llm_provider_id == ""


def test_context_entry_order_is_explicit_and_preserves_legacy_default():
    assert NeoMemoryConfig().context_entry_order == "short_term_first"
    for order in ("short_term_first", "profile_first", "relationship_first"):
        assert NeoMemoryConfig.from_mapping({"context_entry_order": order}).context_entry_order == order
    assert NeoMemoryConfig.from_mapping({"context_entry_order": "invalid"}).context_entry_order == "short_term_first"
