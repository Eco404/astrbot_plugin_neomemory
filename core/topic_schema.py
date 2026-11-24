"""Versioned proposals for organizing existing memories into topics."""

from __future__ import annotations

import json
from copy import deepcopy
from typing import Annotated, Literal

from pydantic import Field, StringConstraints, model_validator
from pydantic_core import PydanticCustomError

from .extraction_schema import (
    Identifier, NodeEmotion, Score, StrictModel, _readable_field,
    _unique_object,
)
from .window_users import WindowUsers
from .extraction_errors import ExtractionCapacityError
from .candidate_feedback import CandidateValidationError

VERSION = "topic-consolidation.v1"
TOPIC_CREATE_MIN_MEMBERS = 2
MAX_OUTPUT_CHARS = 120_000


class TopicChange(StrictModel):
    operation: Literal["create", "update"]
    topic_id: Identifier | None
    expected_version: Annotated[int, Field(ge=1)] | None
    title: Annotated[_readable_field(60), StringConstraints(min_length=1, pattern=r"\S")]
    content: Annotated[_readable_field(2000), StringConstraints(min_length=1, pattern=r"\S")]
    importance: Score
    emotion: NodeEmotion | None
    member_ids: Annotated[list[Identifier], Field(min_length=1, max_length=200)]

    @model_validator(mode="after")
    def operation_contract(self):
        if self.operation == "create":
            if self.topic_id is not None or self.expected_version is not None:
                raise PydanticCustomError("topic_create_identity", "create requires null topic_id and expected_version")
            if len(self.member_ids) < TOPIC_CREATE_MIN_MEMBERS:
                raise PydanticCustomError("topic_create_members", "a new topic requires at least two distinct members")
        elif self.topic_id is None or self.expected_version is None:
            raise PydanticCustomError("topic_update_identity", "update requires topic_id and expected_version")
        if len(set(self.member_ids)) != len(self.member_ids):
            raise PydanticCustomError("topic_duplicate_members", "member_ids must be unique")
        return self


class TopicOutput(StrictModel):
    schema_version: Literal["topic-consolidation.v1"]
    changes: Annotated[list[TopicChange], Field(max_length=8)]

    @model_validator(mode="after")
    def unique_updates(self):
        ids = [item.topic_id for item in self.changes if item.operation == "update"]
        if len(ids) != len(set(ids)):
            raise PydanticCustomError("topic_duplicate_update", "each existing topic may be updated at most once")
        return self


def _model(version):
    if version != VERSION:
        raise CandidateValidationError("literal_error", ("schema_version",), "unsupported topic schema")
    return TopicOutput


def parse_output(raw: str, *, expected_schema_version: str | None = None, window_user_ids=None) -> dict:
    if len(raw) > MAX_OUTPUT_CHARS:
        raise ExtractionCapacityError(len(raw), MAX_OUTPUT_CHARS)
    def reject_constant(token):
        raise CandidateValidationError("json_invalid", (), "nonfinite JSON")

    value = json.loads(raw, parse_constant=reject_constant, object_pairs_hook=_unique_object)
    version = expected_schema_version or (value.get("schema_version", VERSION) if isinstance(value, dict) else VERSION)
    model = _model(version)
    output = model.model_validate(value).model_dump(mode="json")
    if window_user_ids is not None:
        users = WindowUsers(window_user_ids)
        for index, change in enumerate(output["changes"]):
            try:
                change["content"] = users.decode_text(change["content"])
                change["title"] = users.decode_text(change["title"])
                if change["emotion"]:
                    change["emotion"]["description"] = users.decode_text(change["emotion"]["description"])
            except KeyError:
                raise CandidateValidationError("topic_unknown_user", ("changes", index),
                                               "unknown topic user reference") from None
        size = len(json.dumps(output, ensure_ascii=False, separators=(",", ":")))
        if size > MAX_OUTPUT_CHARS:
            raise ExtractionCapacityError(size, MAX_OUTPUT_CHARS)
        model.model_validate(output)
    return output


def output_schema(version: str = VERSION) -> dict:
    return _model(version).model_json_schema()


def model_output_schema(version: str = VERSION) -> dict:
    """Return the topic wire schema without storage-managed versions."""
    schema = deepcopy(output_schema(version))
    schema["properties"].pop("schema_version", None)
    if "schema_version" in schema.get("required", []):
        schema["required"].remove("schema_version")
    change = schema["$defs"]["TopicChange"]
    change["properties"].pop("expected_version", None)
    if "expected_version" in change.get("required", []):
        change["required"].remove("expected_version")
    return schema


def _normalise_program_version(decoded: dict, context, *, validate_supplied_versions=True) -> dict:
    from copy import deepcopy
    value = deepcopy(decoded)
    value["schema_version"] = VERSION
    rows = context.get("nodes", ()) if isinstance(context, dict) else getattr(context, "nodes", ())
    nodes = {row["node_id"]: row for row in rows
             if isinstance(row, dict) and row.get("node_id")}
    changes = value.get("changes", []) if isinstance(value.get("changes", []), list) else []
    for index, change in enumerate(changes):
        if not isinstance(change, dict):
            continue
        if change.get("operation") == "create":
            if (validate_supplied_versions and "expected_version" in change and change.get("expected_version") is not None
                    or "topic_id" in change and change.get("topic_id") is not None):
                raise CandidateValidationError("topic_create_identity", ("changes", index),
                                               "create identity must be managed by the program")
            change["expected_version"] = None
        else:
            topic_id = change.get("topic_id")
            old = nodes.get(topic_id) if isinstance(topic_id, str) else None
            if old is None or old.get("node_type") != "topic":
                raise CandidateValidationError("topic_update_identity", ("changes", index),
                                               "update must reference an editable topic from the fixed input")
            expected = old.get("content_version", 1) if old else 1
            if (validate_supplied_versions and "expected_version" in change and change.get("expected_version") is not None
                    and change.get("expected_version") != expected):
                raise CandidateValidationError("topic_update_identity", ("changes", index),
                                               "model supplied a stale program-managed version")
            change["expected_version"] = expected
    return value


def parse_output_for_context(raw: str, context, *, expected_schema_version: str | None = None,
                            window_user_ids=None, validate_supplied_versions: bool = True) -> dict:
    """Parse model output after deriving topic preconditions from input nodes."""
    if not isinstance(raw, str):
        raise ValueError("topic output must be a JSON string")
    if len(raw) > MAX_OUTPUT_CHARS:
        raise ExtractionCapacityError(len(raw), MAX_OUTPUT_CHARS)
    def reject_constant(token):
        raise CandidateValidationError("json_invalid", (), "nonfinite JSON")
    value = json.loads(raw, parse_constant=reject_constant, object_pairs_hook=_unique_object)
    version = expected_schema_version or (value.get("schema_version", VERSION) if isinstance(value, dict) else VERSION)
    value = _normalise_program_version(value, context,
                                       validate_supplied_versions=validate_supplied_versions)
    output = _model(version).model_validate(value).model_dump(mode="json")
    if window_user_ids is not None:
        users = WindowUsers(window_user_ids)
        for index, change in enumerate(output["changes"]):
            try:
                change["content"] = users.decode_text(change["content"])
                change["title"] = users.decode_text(change["title"])
                if change["emotion"]:
                    change["emotion"]["description"] = users.decode_text(change["emotion"]["description"])
            except KeyError:
                raise CandidateValidationError("topic_unknown_user", ("changes", index),
                                               "unknown topic user reference") from None
    return output
