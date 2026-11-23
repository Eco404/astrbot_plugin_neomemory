"""Validated configuration shared by capture, providers, recall and durable tasks."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "yes", "on", "是"}
    return bool(value)


def _as_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value) if not isinstance(value, bool) else default
    except (TypeError, ValueError, OverflowError):
        parsed = default
    return max(minimum, min(parsed, maximum))


def _as_float(value: Any, default: float, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value) if not isinstance(value, bool) else default
    except (TypeError, ValueError, OverflowError):
        parsed = default
    if not math.isfinite(parsed):
        parsed = default
    return float(max(minimum, min(parsed, maximum)))


def _as_string(value: Any, default: str = "") -> str:
    return value.strip() if isinstance(value, str) else default


def _thresholds(
    data: dict[str, Any], prefix: str, defaults: tuple[int, int, int]
) -> tuple[int, int, int]:
    """Keep the three related thresholds ordered without silently reordering them."""
    values = tuple(
        _as_int(data.get(f"{prefix}_{name}_messages"), default, 1, 200)
        for name, default in zip(("min", "topic", "max"), defaults, strict=True)
    )
    return values if values[0] <= values[1] <= values[2] else defaults


@dataclass(frozen=True, slots=True)
class NeoMemoryConfig:
    """Flat host settings with bounded resource use and explicit model IDs."""

    capture_private_messages: bool = True
    capture_group_messages: bool = True
    capture_agent_messages: bool = True
    max_content_chars: int = 0
    write_batch_size: int = 1
    write_flush_timeout_ms: int = 5000
    recall_enabled: bool = True
    tools_enabled: bool = True
    llm_provider_id: str = ""
    short_term_provider_id: str = ""
    rerank_provider_id: str = ""
    embedding_provider_id: str = ""
    llm_concurrency: int = 2
    embedding_concurrency: int = 2
    embedding_batch_size: int = 16
    embedding_revision: str = ""
    history_seconds: int = 7200
    history_limit: int = 10
    node_version_limit: int = 10
    profile_version_limit: int = 10
    relationship_version_limit: int = 10
    user_weight: float = 0.45
    agent_weight: float = 0.4
    recall_deadline_seconds: float = 15.0
    active_deadline_seconds: float = 15.0
    recall_budget_tokens: int = 15_000
    context_budget_priority: str = "recall_first"
    context_entry_order: str = "short_term_first"
    recall_max_nodes: int = 6
    recall_max_neighbors: int = 2
    recall_candidate_per_query: int = 8
    recall_selection_mode: str = "adaptive"
    recall_similarity_threshold: float = 0.3
    recall_min_strength: float = 0.1
    strength_time_scale_seconds: float = 604800.0
    strength_time_rate: float = 0.03
    strength_max_time_loss: float = 0.15
    strength_activity_loss_per_round: float = 0.001
    strength_max_activity_loss: float = 0.6
    strength_wake_margin: float = 0.01
    strength_importance_decay_weight: float = 1.0
    reinforcement_gain: float = 0.02
    reinforcement_neighbor_multiplier: float = 0.25
    reinforcement_active_multiplier: float = 2.0
    reinforcement_diminishing_power: float = 1.0
    reinforcement_cooldown_seconds: float = 300.0
    reinforcement_max_per_turn: float = 0.12
    reinforcement_max_total: float = 0.2
    recall_time_neighbor_seconds: int = 86_400
    recall_index_limit: int = 2000
    cache_message_limit: int = 10_000
    cache_node_limit: int = 10_000
    cache_message_ttl_seconds: int = 7200
    cache_node_ttl_seconds: int = 86_400

    automatic_extraction_enabled: bool = True
    topic_consolidation_enabled: bool = True
    topic_min_changes: int = 8
    topic_idle_seconds: float = 3600.0
    topic_context_nodes: int = 30
    job_concurrency: int = 2
    job_lease_seconds: float = 30.0
    job_poll_seconds: float = 0.5
    extraction_scan_seconds: float = 10.0
    extraction_timeout_seconds: float = 180.0
    extraction_auto_regeneration: bool = True
    extraction_max_regenerations: int = 3
    extraction_output_format: str = "json_schema"
    index_timeout_seconds: float = 300.0
    job_max_attempts: int = 2
    extraction_model_revision: str = ""
    private_image_limit: int = 10
    private_min_messages: int = 5
    private_topic_messages: int = 20
    private_max_messages: int = 40
    private_idle_seconds: float = 7200.0
    group_min_messages: int = 40
    group_topic_messages: int = 100
    group_max_messages: int = 200
    group_idle_seconds: float = 1800.0
    extraction_max_chars: int = 24_000
    extraction_max_tokens: int = 24_000
    extraction_max_prompt_bytes: int = 256_000
    extraction_context_nodes: int = 30

    ingress_max_entries: int = 10000
    ingress_max_bytes: int = 67108864
    ingress_max_attempts: int = 3
    ingress_retry_seconds: float = 5.0
    ingress_poll_seconds: float = 0.5
    short_term_enabled: bool = True
    short_term_injection_enabled: bool = True
    short_term_keep_messages: int = 10
    short_term_min_messages: int = 6
    short_term_max_messages: int = 40
    short_term_max_chars: int = 16000
    short_term_summary_chars: int = 400
    profile_injection_enabled: bool = True
    relationship_injection_enabled: bool = True
    notes_hint_enabled: bool = True
    context_profile_max_chars: int = 1200
    context_relationship_max_chars: int = 2400
    relationship_aftereffect_seconds: int = 86400
    context_short_term_max_chars: int = 1200

    # Optional interoperability with astrbot_plugin_private_companion.
    # Registration is disabled by default until the host/provider path is
    # explicitly enabled and verified in the deployment.
    external_memory_enabled: bool = False
    external_memory_mode: str = "supplement"
    external_memory_priority: int = 0
    external_memory_timeout_seconds: float = 5.0

    @property
    def version_limits(self):
        return {"node": self.node_version_limit, "profile": self.profile_version_limit,
                "relationship": self.relationship_version_limit}

    @classmethod
    def from_mapping(cls, raw: dict[str, Any] | None) -> NeoMemoryConfig:
        data = raw if isinstance(raw, dict) else {}
        private_thresholds = _thresholds(data, "private", (5, 20, 40))
        group_thresholds = _thresholds(data, "group", (40, 100, 200))
        short_min = _as_int(data.get("short_term_min_messages"), 6, 1, 200)
        short_max = _as_int(data.get("short_term_max_messages"), 40, 1, 200)
        if short_min > short_max:
            short_min, short_max = 6, 40
        return cls(
            ingress_max_entries=_as_int(data.get("ingress_max_entries"), 10000, 1, 1000000),
            ingress_max_bytes=_as_int(data.get("ingress_max_bytes"), 67108864, 1024, 1073741824),
            ingress_max_attempts=_as_int(data.get("ingress_max_attempts"), 3, 1, 10),
            ingress_retry_seconds=_as_float(data.get("ingress_retry_seconds"), 5.0, 0.1, 300.0),
            ingress_poll_seconds=_as_float(data.get("ingress_poll_seconds"), 0.5, 0.05, 5.0),
            short_term_enabled=_as_bool(data.get("short_term_enabled"), True),
            short_term_injection_enabled=_as_bool(data.get("short_term_injection_enabled"), True),
            short_term_keep_messages=_as_int(data.get("short_term_keep_messages"), 10, 1, 200),
            short_term_min_messages=short_min,
            short_term_max_messages=short_max,
            short_term_max_chars=_as_int(data.get("short_term_max_chars"), 16000, 100, 200000),
            short_term_summary_chars=_as_int(data.get("short_term_summary_chars"), 400, 100, 4000),
            profile_injection_enabled=_as_bool(data.get("profile_injection_enabled"), True),
            relationship_injection_enabled=_as_bool(data.get("relationship_injection_enabled"), True),
            notes_hint_enabled=_as_bool(data.get("notes_hint_enabled"), True),
            context_profile_max_chars=_as_int(data.get("context_profile_max_chars"), 1200, 100, 4000),
            context_relationship_max_chars=_as_int(data.get("context_relationship_max_chars"), 2400, 100, 8000),
            relationship_aftereffect_seconds=_as_int(data.get("relationship_aftereffect_seconds"), 86400, 0, 31536000),
            context_short_term_max_chars=_as_int(data.get("context_short_term_max_chars"), 1200, 100, 4000),
            external_memory_enabled=_as_bool(data.get("external_memory_enabled"), False),
            external_memory_mode=(
                _as_string(data.get("external_memory_mode"), "supplement")
                if _as_string(data.get("external_memory_mode"), "supplement") in {"supplement", "owner"}
                else "supplement"
            ),
            external_memory_priority=_as_int(data.get("external_memory_priority"), 0, -100, 100),
            external_memory_timeout_seconds=_as_float(
                data.get("external_memory_timeout_seconds"), 5.0, 0.1, 60.0
            ),
            automatic_extraction_enabled=_as_bool(
                data.get("automatic_extraction_enabled"), True
            ),
            topic_consolidation_enabled=_as_bool(data.get("topic_consolidation_enabled"), True),
            topic_min_changes=_as_int(data.get("topic_min_changes"), 8, 2, 100),
            topic_idle_seconds=_as_float(data.get("topic_idle_seconds"), 3600, 60, 604800),
            topic_context_nodes=_as_int(data.get("topic_context_nodes"), 30, 8, 50),
            job_concurrency=_as_int(data.get("job_concurrency"), 2, 1, 8),
            job_lease_seconds=_as_float(data.get("job_lease_seconds"), 30, 10, 300),
            job_poll_seconds=_as_float(data.get("job_poll_seconds"), 0.5, 0.05, 5),
            extraction_scan_seconds=_as_float(
                data.get("extraction_scan_seconds"), 10, 1, 300
            ),
            extraction_timeout_seconds=_as_float(
                data.get("extraction_timeout_seconds"), 180, 1, 300
            ),
            extraction_auto_regeneration=_as_bool(data.get("extraction_auto_regeneration"), True),
            extraction_max_regenerations=_as_int(data.get("extraction_max_regenerations"), 3, 1, 10),
            extraction_output_format=(data.get("extraction_output_format") if data.get("extraction_output_format") in ("text", "json_object", "json_schema") else "json_schema"),
            index_timeout_seconds=_as_float(
                data.get("index_timeout_seconds"), 300, 1, 3600
            ),
            job_max_attempts=_as_int(data.get("job_max_attempts"), 2, 1, 5),
            extraction_model_revision=_as_string(data.get("extraction_model_revision")),
            private_image_limit=_as_int(data.get("private_image_limit"), 10, 0, 10),
            private_min_messages=private_thresholds[0],
            private_topic_messages=private_thresholds[1],
            private_max_messages=private_thresholds[2],
            private_idle_seconds=_as_float(
                data.get("private_idle_seconds"), 7200, 0, 86400
            ),
            group_min_messages=group_thresholds[0],
            group_topic_messages=group_thresholds[1],
            group_max_messages=group_thresholds[2],
            group_idle_seconds=_as_float(
                data.get("group_idle_seconds"), 1800, 0, 86400
            ),
            extraction_max_chars=_as_int(
                data.get("extraction_max_chars"), 24000, 100, 200000
            ),
            extraction_max_tokens=_as_int(
                data.get("extraction_max_tokens"), 24000, 100, 256000
            ),
            extraction_max_prompt_bytes=_as_int(
                data.get("extraction_max_prompt_bytes"), 256000, 1000, 1000000
            ),
            extraction_context_nodes=_as_int(
                data.get("extraction_context_nodes"), 30, 0, 50
            ),
            capture_private_messages=_as_bool(
                data.get("capture_private_messages"), True
            ),
            capture_group_messages=_as_bool(data.get("capture_group_messages"), True),
            capture_agent_messages=_as_bool(data.get("capture_agent_messages"), True),
            max_content_chars=_as_int(data.get("max_content_chars"), 0, 0, 200_000),
            write_batch_size=_as_int(data.get("write_batch_size"), 1, 1, 100),
            write_flush_timeout_ms=_as_int(
                data.get("write_flush_timeout_ms"), 5000, 100, 30_000
            ),
            recall_enabled=_as_bool(data.get("recall_enabled"), True),
            tools_enabled=_as_bool(data.get("tools_enabled"), True),
            llm_provider_id=_as_string(data.get("llm_provider_id")),
            short_term_provider_id=_as_string(data.get("short_term_provider_id")),
            rerank_provider_id=_as_string(data.get("rerank_provider_id")),
            embedding_provider_id=_as_string(data.get("embedding_provider_id")),
            llm_concurrency=_as_int(data.get("llm_concurrency"), 2, 1, 32),
            embedding_concurrency=_as_int(data.get("embedding_concurrency"), 2, 1, 32),
            embedding_batch_size=_as_int(data.get("embedding_batch_size"), 16, 1, 1024),
            embedding_revision=_as_string(data.get("embedding_revision")),
            history_seconds=_as_int(data.get("history_seconds"), 7200, 1, 604_800),
            history_limit=_as_int(data.get("history_limit"), 10, 0, 50),
            node_version_limit=_as_int(data.get("node_version_limit"), 10, 1, 100),
            profile_version_limit=_as_int(data.get("profile_version_limit"), 10, 1, 100),
            relationship_version_limit=_as_int(data.get("relationship_version_limit"), 10, 1, 100),
            user_weight=_as_float(data.get("user_weight"), 0.45, 0, 1),
            agent_weight=_as_float(data.get("agent_weight"), 0.4, 0, 1),
            recall_deadline_seconds=_as_float(
                data.get("recall_deadline_seconds"), 15, 0.05, 60
            ),
            active_deadline_seconds=_as_float(
                data.get("active_deadline_seconds"), 15, 0.1, 120
            ),
            recall_budget_tokens=_as_int(
                data.get("recall_budget_tokens"), 15_000, 128, 32_000
            ),
            context_budget_priority=("context_first" if data.get("context_budget_priority")
                                     == "context_first" else "recall_first"),
            context_entry_order=(data.get("context_entry_order") if data.get("context_entry_order")
                                 in ("short_term_first", "profile_first", "relationship_first") else "short_term_first"),
            recall_max_nodes=_as_int(data.get("recall_max_nodes"), 6, 1, 30),
            recall_max_neighbors=_as_int(data.get("recall_max_neighbors"), 2, 0, 10),
            recall_candidate_per_query=_as_int(
                data.get("recall_candidate_per_query"), 8, 1, 50
            ),
            recall_selection_mode=(data["recall_selection_mode"] if data.get("recall_selection_mode")
                                   in ("baseline", "gated") else "adaptive"),
            recall_similarity_threshold=_as_float(
                data.get("recall_similarity_threshold"), 0.3, 0, 1
            ),
            recall_min_strength=_as_float(data.get("recall_min_strength"), 0.1, 0, 1),
            strength_time_scale_seconds=_as_float(data.get("strength_time_scale_seconds"), 604800.0, 1.0, 31536000.0),
            strength_time_rate=_as_float(data.get("strength_time_rate"), 0.03, 0.0, 1.0),
            strength_max_time_loss=_as_float(data.get("strength_max_time_loss"), 0.15, 0.0, 0.15),
            strength_activity_loss_per_round=_as_float(data.get("strength_activity_loss_per_round"), 0.001, 0.0, 1.0),
            strength_max_activity_loss=_as_float(data.get("strength_max_activity_loss"), 0.6, 0.0, 0.6),
            strength_wake_margin=_as_float(data.get("strength_wake_margin"), 0.01, 0.0, 0.1),
            strength_importance_decay_weight=_as_float(data.get("strength_importance_decay_weight"), 1.0, 0.0, 10.0),
            reinforcement_gain=_as_float(data.get("reinforcement_gain"), 0.02, 0.0, 0.2),
            reinforcement_neighbor_multiplier=_as_float(data.get("reinforcement_neighbor_multiplier"), 0.25, 0.0, 1.0),
            reinforcement_active_multiplier=_as_float(data.get("reinforcement_active_multiplier"), 2.0, 1.0, 10.0),
            reinforcement_diminishing_power=_as_float(data.get("reinforcement_diminishing_power"), 1.0, 0.1, 4.0),
            reinforcement_cooldown_seconds=_as_float(data.get("reinforcement_cooldown_seconds"), 300.0, 0.0, 86400.0),
            reinforcement_max_per_turn=_as_float(data.get("reinforcement_max_per_turn"), 0.12, 0.0, 0.12),
            reinforcement_max_total=_as_float(data.get("reinforcement_max_total"), 0.2, 0.0, 0.2),
            recall_time_neighbor_seconds=_as_int(
                data.get("recall_time_neighbor_seconds"), 86_400, 1, 31_536_000
            ),
            recall_index_limit=_as_int(data.get("recall_index_limit"), 2000, 1, 2000),
            cache_message_limit=_as_int(
                data.get("cache_message_limit"), 10_000, 0, 1_000_000
            ),
            cache_node_limit=_as_int(
                data.get("cache_node_limit"), 10_000, 1, 1_000_000
            ),
            cache_message_ttl_seconds=_as_int(
                data.get("cache_message_ttl_seconds"), 7200, 1, 31_536_000
            ),
            cache_node_ttl_seconds=_as_int(
                data.get("cache_node_ttl_seconds"), 86_400, 1, 31_536_000
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
