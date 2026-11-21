"""Exact assertions for additive observation metadata in current prompt inputs."""

import json

from core.extraction_observations import TEMPORAL_SEMANTICS, source_observation_fields


def assert_observation_only_additions(current_message, legacy_message):
    before = json.loads(legacy_message["content"])
    expected = before | {
        "time_semantics": TEMPORAL_SEMANTICS,
        "sources": [source | source_observation_fields(source) for source in before["sources"]],
    }
    assert json.loads(current_message["content"]) == expected
