"""Versioned proposals for organizing existing memories into topics."""

from __future__ import annotations

import json
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
