"""Optional extraction inputs grounded in original source observation time."""

from datetime import datetime, timezone


TEMPORAL_SEMANTICS = {
    "display_timezone": "UTC",
    "speaker_local_timezone": "unknown",
    "observed_at": "原始记录的观察时间，不是重构时间，也不等于所述事件发生时间。",
    "relative_time": "相对时间依附于其所在消息；本地时区未知，不凭 UTC 日期断定说话者的本地日期。",
    "version_observations": "每个版本仅列直接来源的观察范围；不是该版本全部事实的发生时间或有效期，继承事实可能更早。",
}


def utc_iso(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()


def source_observation_fields(source):
    """Add readable fields without replacing raw time or identity values."""
    observed = datetime.fromtimestamp(source["observed_at"], timezone.utc)
    result = {
        "observed_at_iso_utc": observed.isoformat(),
        "observed_weekday_utc": ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")[observed.weekday()],
        "speaker_perspective": "persona_self" if source["actor_kind"] == "agent" else "conversation_participant",
    }
    for key in ("event_time_start", "event_time_end"):
        if source.get(key) is not None:
            result[f"{key}_iso_utc"] = utc_iso(source[key])
    return result


async def build_observation_anchors(memory_store, entities):
    """Build bounded version anchors from the direct source times saved at write time."""
    result = {}
    for kind, entity_id, version in dict.fromkeys(entities):
        records = await memory_store.get_observation_anchor_sources(kind, entity_id, version)
        anchors = []
        for record in records:
            times = record["source_observations"].values()
            anchors.append({
                "version_no": record["version_no"],
                "direct_source_count": len(record["source_ids"]),
                "available_source_count": len(times),
                "observed_from_iso_utc": utc_iso(min(times)) if times else None,
                "observed_through_iso_utc": utc_iso(max(times)) if times else None,
            })
        result[kind, entity_id] = anchors
    return result
