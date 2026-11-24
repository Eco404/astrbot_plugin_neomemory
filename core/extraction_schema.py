"""Strict, versioned candidates emitted by an extraction model.

These objects describe proposed changes only. Source ownership, node versions,
manual protection and generation membership must be checked by the apply layer.
"""

from __future__ import annotations

import json
from copy import deepcopy
from typing import Annotated, Any, Literal, Self

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, StringConstraints, ValidationInfo, field_validator, model_validator
from pydantic_core import PydanticCustomError

from .memory_nodes import validate_title
from .memory_emotions import encode_emotion, validate_emotion
from .text_lengths import encoded_text_limit, readable_text_length
from .relationship_state import LONG_TERM_MAX_CHARS, AFTEREFFECT_MAX_CHARS, is_slot_operation
from .extraction_errors import DuplicateJSONKey, ExtractionCapacityError
from .candidate_feedback import CandidateValidationError
from .memory_text import memory_texts, user_reference_ids
from .profile_edits import model_profile_schema
from .profile_memory_prompt import PROFILE_EDITS_PROMPT_VERSION

SCHEMA_VERSION = "extraction.v1"
MAX_OUTPUT_CHARS = 120_000
NODE_CONTENT_LIMITS = {"atomic": 800, "event": 3000, "topic": 2000}

Identifier = Annotated[
    str, StringConstraints(min_length=1, max_length=200, pattern=r"^\S+$")
]
LocalReference = Annotated[
    str,
    StringConstraints(min_length=7, max_length=70, pattern=r"^local_[A-Za-z0-9_-]+$"),
]
NonEmptyText = Annotated[str, StringConstraints(min_length=1, pattern=r"\S")]
Score = Annotated[float, Field(ge=0.0, le=1.0)]
SourceIds = Annotated[list[Identifier], Field(min_length=1, max_length=200)]
EmotionTag = Literal[
    "joy", "sadness", "anger", "fear", "surprise", "trust", "concern", "affection", "neutral"
]


def _unique(values: list[str], field_name: str) -> None:
    if len(values) != len(set(values)):
        raise PydanticCustomError("duplicate_values", "{field_name} must not contain duplicates", {"field_name": field_name})


class StrictModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", allow_inf_nan=False)


def _check_readable_length(value: str, limit: int) -> str:
    try:
        length = readable_text_length(value)
    except ValueError as exc:
        raise PydanticCustomError("invalid_user_reference", "{reason}", {"reason": str(exc)}) from exc
    if length > limit:
        raise PydanticCustomError("readable_too_long", "Text exceeds {max_length} readable characters",
                                  {"max_length": limit})
    return value


def _readable_field(limit: int) -> Any:
    """Expose encoded capacity and the application-enforced readable limit."""
    return Annotated[str, Field(max_length=encoded_text_limit(limit),
                               json_schema_extra={"x-readable-maxLength": limit}),
                     AfterValidator(lambda value: _check_readable_length(value, limit))]


class EventTime(StrictModel):
    """Occurrence time, never automatically substituted with mention time."""

    start: float | None
    end: float | None
    text: Annotated[_readable_field(120), StringConstraints(min_length=1, pattern=r"\S")] | None

    @model_validator(mode="after")
    def validate_interval(self) -> Self:
        if self.start is not None and self.end is not None and self.end < self.start:
            raise PydanticCustomError("event_time_interval", "event time end cannot precede start")
        if self.start is None and self.end is None and self.text is None:
            raise PydanticCustomError("unknown_event_time", "unknown event time must be null")
        return self


class EmotionTagIntensity(StrictModel):
    tag: EmotionTag
    intensity: Score


class NodeEmotion(StrictModel):
    tags: Annotated[list[EmotionTagIntensity], Field(min_length=1, max_length=3)]
    description: Annotated[_readable_field(160), StringConstraints(min_length=1, pattern=r"\S")]

    @model_validator(mode="after")
    def validate_value(self) -> Self:
        names = [item.tag for item in self.tags]
        _unique(names, "tags")
        if "neutral" in names and len(names) != 1:
            raise PydanticCustomError("neutral_emotion_conflict", "neutral cannot be combined with another emotion")
        validate_emotion(self.model_dump())
        return self


class NodeChange(StrictModel):
    local_ref: LocalReference
    node_type: Literal["atomic", "event", "topic"]
    operation: Literal["create", "update"]
    node_id: Identifier | None
    expected_version: Annotated[int, Field(ge=1)] | None
    content: Annotated[NonEmptyText, Field(max_length=encoded_text_limit(3000),
        json_schema_extra={"x-readable-maxLength": 3000, "x-readable-limitsByNodeType": NODE_CONTENT_LIMITS})]
    importance: Score
    source_ids: SourceIds
    event_time: EventTime | None
    title: Annotated[_readable_field(60), StringConstraints(min_length=1, pattern=r"\S")]
    emotion: NodeEmotion | None
    base_node_ids: Annotated[list[Identifier], Field(max_length=200)]

    @model_validator(mode="after")
    def validate_operation(self) -> Self:
        if self.operation == "create":
            if self.node_id is not None or self.expected_version is not None:
                raise PydanticCustomError("create_identity_invalid", "create requires null node_id and expected_version")
        elif self.node_id is None or self.expected_version is None:
            raise PydanticCustomError("update_identity_required", "update requires node_id and expected_version")
        _unique(self.source_ids, "source_ids")
        return self

    @field_validator("title")
    @classmethod
    def validate_display_title(cls, value: str) -> str:
        return validate_title(value)

    @field_validator("content")
    @classmethod
    def validate_readable_content(cls, value: str, info: ValidationInfo) -> str:
        return _check_readable_length(value, NODE_CONTENT_LIMITS.get(info.data.get("node_type"), 3000))

    @field_validator("base_node_ids")
    @classmethod
    def validate_bases(cls, values: list[str]) -> list[str]:
        _unique(values, "base_node_ids")
        return values


class EdgeChange(StrictModel):
    from_ref: Identifier
    to_ref: Identifier
    relation_type: Literal["contains", "related", "follows", "supersedes"]
    source_ids: SourceIds

    @model_validator(mode="after")
    def validate_endpoints(self) -> Self:
        if self.from_ref == self.to_ref:
            raise PydanticCustomError("edge_self_reference", "edge endpoints must differ")
        _unique(self.source_ids, "source_ids")
        return self


class CoverageChange(StrictModel):
    """Explicit narrative coverage of the final content versions in this batch."""

    event_ref: Identifier
    atomic_ref: Identifier
    expected_event_version: Annotated[int, Field(ge=1)]
    expected_atomic_version: Annotated[int, Field(ge=1)]
    coverage_kind: Literal["full", "partial"]
    source_ids: SourceIds

    @model_validator(mode="after")
    def validate_endpoints(self) -> Self:
        if self.event_ref == self.atomic_ref:
            raise PydanticCustomError("coverage_self_reference", "coverage endpoints must differ")
        _unique(self.source_ids, "source_ids")
        return self


class ProfileChange(StrictModel):
    logical_user_id: Identifier
    expected_version: Annotated[int, Field(ge=0)]
    tags: Annotated[list[Annotated[_readable_field(24), StringConstraints(min_length=1, pattern=r"\S")]],
                    Field(max_length=12)]
    description: _readable_field(600)
    source_ids: SourceIds

    @model_validator(mode="after")
    def validate_unique_values(self) -> Self:
        _unique(self.source_ids, "source_ids")
        _unique(self.tags, "tags")
        return self


class RelationshipSlotUpdate(StrictModel):
    op: Literal["keep", "replace", "clear"]
    body: str

    @model_validator(mode="after")
    def operation_body(self) -> Self:
        if self.op == "replace" and (not self.body.strip() or is_slot_operation(self.body)):
            raise PydanticCustomError("relationship_slot_body", "replace requires complete prose and cannot use an operation as the body")
        if self.op != "replace" and self.body:
            raise PydanticCustomError("relationship_slot_body", "keep and clear require an empty body")
        return self


class LongTermRelationshipUpdate(RelationshipSlotUpdate):
    body: _readable_field(LONG_TERM_MAX_CHARS)


class AftereffectRelationshipUpdate(RelationshipSlotUpdate):
    body: _readable_field(AFTEREFFECT_MAX_CHARS)


class RelationshipChange(StrictModel):
    logical_user_id: Identifier
    expected_version: Annotated[int, Field(ge=0)]
    source_ids: SourceIds
    long_term: LongTermRelationshipUpdate
    aftereffect: AftereffectRelationshipUpdate

    @model_validator(mode="after")
    def unique_sources(self) -> Self:
        _unique(self.source_ids, "source_ids")
        return self


class ExtractionOutput(StrictModel):
    schema_version: Literal["extraction.v1"]
    result: Literal["changes", "no_change"]
    reason: Literal[
        "changes", "no_meaningful_content", "insufficient_evidence", "already_known"
    ]
    node_changes: Annotated[list[NodeChange], Field(max_length=48)]
    edges: Annotated[list[EdgeChange], Field(max_length=96)]
    profile_changes: Annotated[list[ProfileChange], Field(max_length=32)]
    relationship_change: RelationshipChange | None
    coverage_changes: Annotated[list[CoverageChange], Field(max_length=96)]

    @model_validator(mode="after")
    def validate_result(self) -> Self:
        has_changes = bool(
            self.node_changes or self.edges or self.profile_changes or self.relationship_change
            or self.coverage_changes
        )
        if self.result == "no_change":
            if has_changes or self.reason == "changes":
                raise PydanticCustomError("result_changes_mismatch", "no_change requires empty changes and a no-change reason")
        elif not has_changes or self.reason != "changes":
            raise PydanticCustomError("result_changes_mismatch", "changes requires a nonempty change set and reason=changes")
        _unique([change.local_ref for change in self.node_changes], "local_ref")
        _unique(
            [change.node_id for change in self.node_changes if change.node_id is not None],
            "updated node_id",
        )
        _unique([change.logical_user_id for change in self.profile_changes], "profile logical_user_id")
        edge_keys = [(edge.from_ref, edge.to_ref, edge.relation_type) for edge in self.edges]
        if len(edge_keys) != len(set(edge_keys)):
            raise PydanticCustomError("duplicate_edges", "edges must not contain duplicate relationships")
        local_refs = {change.local_ref for change in self.node_changes}
        for edge in self.edges:
            for endpoint in (edge.from_ref, edge.to_ref):
                if endpoint.startswith("local_") and endpoint not in local_refs:
                    raise PydanticCustomError("unknown_local_reference", "edge references an unknown local_ref")
        return self

    @model_validator(mode="after")
    def validate_coverage(self) -> Self:
        keys = [(change.event_ref, change.atomic_ref) for change in self.coverage_changes]
        if len(keys) != len(set(keys)):
            raise PydanticCustomError("duplicate_coverage_pair", "coverage_changes must not contain duplicate endpoints")
        local_refs = {change.local_ref for change in self.node_changes}
        for change in self.coverage_changes:
            for endpoint in (change.event_ref, change.atomic_ref):
                if endpoint.startswith("local_") and endpoint not in local_refs:
                    raise PydanticCustomError("unknown_local_reference", "coverage references an unknown local_ref")
        return self


def _output_model(schema_version: str) -> type[ExtractionOutput]:
    if schema_version != SCHEMA_VERSION:
        raise ValueError("unsupported extraction schema_version")
    return ExtractionOutput


def _reject_constant(value: str) -> None:
    raise ValueError(f"nonfinite JSON number is forbidden: {value}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateJSONKey(key)
        result[key] = value
    return result


def parse_extraction_output(raw: str, *, schema_version: str | None = None) -> ExtractionOutput:
    """Reject invalid output rather than silently repairing model instructions."""
    if not isinstance(raw, str):
        raise ValueError("extraction output must be a JSON string")
    if len(raw) > MAX_OUTPUT_CHARS:
        raise ExtractionCapacityError(len(raw), MAX_OUTPUT_CHARS)
    try:
        decoded = json.loads(raw, parse_constant=_reject_constant, object_pairs_hook=_unique_object)
    except RecursionError as exc:
        raise ValueError("extraction output is nested too deeply") from exc
    if schema_version is not None:
        return _output_model(schema_version).model_validate(decoded)
    if not isinstance(decoded, dict) or not isinstance(decoded.get("schema_version"), str):
        raise ValueError("extraction output requires a schema_version")
    return _output_model(decoded["schema_version"]).model_validate(decoded)


def extraction_json_schema(schema_version: str = SCHEMA_VERSION) -> dict[str, Any]:
    """All properties are required; all object schemas reject extra properties.

    Provider JSON Schema support differs. Cross-field invariants remain enforced
    by parsing and the apply layer even when a provider accepts this schema.
    """
    return _output_model(schema_version).model_json_schema()


def extraction_model_json_schema(schema_version: str = SCHEMA_VERSION, *, prompt_version=None) -> dict[str, Any]:
    """Return the wire schema shown to the model.

    Content versions are storage and concurrency metadata.  They are derived
    from the frozen input snapshot by the application after generation, so the
    model does not spend output budget reproducing internal counters.
    """
    schema = deepcopy(extraction_json_schema(schema_version))
    for field in ("schema_version", "result", "reason"):
        schema["properties"].pop(field, None)
        if field in schema.get("required", []):
            schema["required"].remove(field)
    for definition, fields in {
        "NodeChange": ("expected_version",),
        "ProfileChange": ("expected_version",),
        "RelationshipChange": ("expected_version",),
        "CoverageChange": ("expected_event_version", "expected_atomic_version"),
    }.items():
        value = schema["$defs"][definition]
        for field in fields:
            value["properties"].pop(field, None)
            if field in value.get("required", []):
                value["required"].remove(field)
    if prompt_version is None or prompt_version == PROFILE_EDITS_PROMPT_VERSION:
        return model_profile_schema(schema)
    return schema


def _decode_output(raw: str) -> dict[str, Any]:
    if not isinstance(raw, str):
        raise ValueError("extraction output must be a JSON string")
    if len(raw) > MAX_OUTPUT_CHARS:
        raise ExtractionCapacityError(len(raw), MAX_OUTPUT_CHARS)
    try:
        decoded = json.loads(raw, parse_constant=_reject_constant, object_pairs_hook=_unique_object)
    except RecursionError as exc:
        raise ValueError("extraction output is nested too deeply") from exc
    if not isinstance(decoded, dict):
        raise ValueError("extraction output must be a JSON object")
    return decoded


def _context_rows(context: Any, field: str, default=()):
    if isinstance(context, dict):
        value = context.get(field)
        if value is None and field == "profiles":
            value = context.get("known_profiles")
        if value is None and field == "relationship":
            value = context.get("known_relationship")
        return default if value is None else value
    return getattr(context, field, default)


def _context_aliases(context: Any, field: str, prefix: str) -> dict[str, str]:
    """Recreate the short aliases used in the model-facing request."""
    rows = _context_rows(context, field)
    if field == "profiles" and not rows:
        rows = _context_rows(context, "known_profiles")
    if field == "relationship":
        rows = (rows,) if isinstance(rows, dict) else ()
    if field == "space":
        rows = (rows,) if isinstance(rows, dict) else ()
    values = []
    if field == "space":
        values.extend(row.get("logical_user_id") for row in rows if row.get("logical_user_id"))
    elif field == "known_nodes":
        values.extend(row.get("node_id") for row in rows if row.get("node_id"))
    elif field == "sources":
        for row in rows:
            for key in ("actor_logical_user_id", "conversation_logical_user_id"):
                if row.get(key):
                    values.append(row[key])
    elif field in {"profiles", "relationship"}:
        values.extend(row.get("logical_user_id") for row in rows if row.get("logical_user_id"))
    return {f"{prefix}{index}": value for index, value in enumerate(dict.fromkeys(values), 1)}


def _context_user_aliases(context: Any) -> dict[str, str]:
    rows = []
    space = _context_rows(context, "space", {})
    if isinstance(space, dict):
        rows.append(space)
    rows.extend(_context_rows(context, "sources"))
    rows.extend(_context_rows(context, "profiles"))
    relationship = _context_rows(context, "relationship")
    if isinstance(relationship, dict):
        rows.append(relationship)
    values = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in ("logical_user_id", "actor_logical_user_id", "conversation_logical_user_id"):
            if row.get(key):
                values.append(row[key])
    for row in (*_context_rows(context, "known_nodes"), *_context_rows(context, "profiles")):
        if isinstance(row, dict):
            values.extend(user_reference_ids(memory_texts(row)))
    relationship = _context_rows(context, "relationship")
    if isinstance(relationship, dict):
        values.extend(user_reference_ids(memory_texts(relationship)))
    return {f"u{index}": value for index, value in enumerate(dict.fromkeys(values), 1)}


def _normalise_program_versions(decoded: dict[str, Any], context: Any, *, validate_supplied_versions=True) -> dict[str, Any]:
    """Fill storage preconditions from the prepared snapshot.

    Any model-supplied values are ignored deliberately.  The resulting object
    is the canonical internal candidate consumed by the transactional writer.
    """
    value = deepcopy(decoded)
    value["schema_version"] = SCHEMA_VERSION
    nodes = {row["node_id"]: row for row in _context_rows(context, "known_nodes")
             if isinstance(row, dict) and row.get("node_id")}
    profiles = {row["logical_user_id"]: row for row in _context_rows(context, "profiles")
                if isinstance(row, dict) and row.get("logical_user_id")}
    relationship = _context_rows(context, "relationship")
    node_aliases = _context_aliases(context, "known_nodes", "n")
    user_aliases = _context_user_aliases(context)
    changes = value.get("node_changes", [])
    resolved: dict[str, str] = {}
    versions: dict[str, int] = {}
    for change in changes if isinstance(changes, list) else ():
        if not isinstance(change, dict):
            continue
        operation = change.get("operation")
        node_id = change.get("node_id")
        lookup_node_id = node_aliases.get(node_id, node_id) if isinstance(node_id, str) else None
        if operation == "create":
            supplied_version = change.get("expected_version")
            valid_supplied_version = (type(supplied_version) is int and supplied_version >= 0)
            if ((validate_supplied_versions and valid_supplied_version and supplied_version is not None)
                    or ("node_id" in change and isinstance(node_id, str) and 1 <= len(node_id) <= 200
                        and bool(node_id.strip()) and not any(char.isspace() for char in node_id))):
                raise CandidateValidationError("create_identity_invalid", ("node_changes", changes.index(change)),
                                               "create identity must be managed by the program")
            change["expected_version"] = None
            local_ref = change.get("local_ref")
            if isinstance(local_ref, str):
                versions[local_ref] = 1
        else:
            old = nodes.get(lookup_node_id)
            expected = old.get("content_version", 1) if old else 1
            supplied_version = change.get("expected_version")
            valid_supplied_version = type(supplied_version) is int and supplied_version >= 1
            if (validate_supplied_versions and valid_supplied_version
                    and supplied_version != expected):
                raise CandidateValidationError("expected_node_version_mismatch", ("node_changes", changes.index(change), "expected_version"),
                                               "model supplied a stale program-managed version")
            change["expected_version"] = expected
            local_ref = change.get("local_ref")
            if isinstance(local_ref, str):
                resolved[local_ref] = lookup_node_id
            changed = bool(old and change.get("content") != old.get("content_text"))
            if old and not changed:
                previous_emotion = encode_emotion(old.get("emotion"))
                try:
                    changed = encode_emotion(change.get("emotion")) != previous_emotion
                except ValueError:
                    # NodeChange validates the draft below and reports its exact emotion field.
                    changed = True
            if isinstance(local_ref, str):
                versions[local_ref] = (old.get("content_version", 1) + int(changed)
                                       if old else 1)
            if isinstance(lookup_node_id, str):
                versions[lookup_node_id] = versions.get(local_ref, expected) if isinstance(local_ref, str) else expected
    for change in changes if isinstance(changes, list) else ():
        if not isinstance(change, dict):
            continue
        local_ref = change.get("local_ref")
        if change.get("operation") == "create" and isinstance(local_ref, str):
            resolved[local_ref] = local_ref

    for change in value.get("profile_changes", []) if isinstance(value.get("profile_changes", []), list) else ():
        if isinstance(change, dict):
            logical_user_id = change.get("logical_user_id")
            lookup_user_id = user_aliases.get(logical_user_id, logical_user_id) \
                if isinstance(logical_user_id, str) else None
            expected = profiles.get(lookup_user_id, {}).get("content_version", 0) \
                if isinstance(lookup_user_id, str) else 0
            supplied_version = change.get("expected_version")
            valid_supplied_version = type(supplied_version) is int and supplied_version >= 0
            if (validate_supplied_versions and valid_supplied_version
                    and supplied_version != expected):
                raise CandidateValidationError("expected_profile_version_mismatch", ("profile_changes", value["profile_changes"].index(change), "expected_version"),
                                               "model supplied a stale program-managed version")
            change["expected_version"] = expected
    relation_change = value.get("relationship_change")
    if isinstance(relation_change, dict):
        relation_user_id = relation_change.get("logical_user_id")
        lookup_user_id = user_aliases.get(relation_user_id, relation_user_id) \
            if isinstance(relation_user_id, str) else None
        expected = relationship.get("content_version", 0) if relationship else 0
        supplied_version = relation_change.get("expected_version")
        valid_supplied_version = type(supplied_version) is int and supplied_version >= 0
        if (validate_supplied_versions and valid_supplied_version
                and supplied_version != expected):
            raise CandidateValidationError("expected_relationship_version_mismatch", ("relationship_change", "expected_version"),
                                           "model supplied a stale program-managed version")
        relation_change["expected_version"] = expected

    def final_version(reference):
        if isinstance(reference, str) and reference in versions:
            return versions[reference]
        node_id = resolved.get(reference, node_aliases.get(reference, reference)) if isinstance(reference, str) else None
        if isinstance(node_id, str) and node_id in versions:
            return versions[node_id]
        return nodes.get(node_id, {}).get("content_version", 1) if isinstance(node_id, str) else 1

    for change in value.get("coverage_changes", []) if isinstance(value.get("coverage_changes", []), list) else ():
        if isinstance(change, dict):
            expected_event = final_version(change.get("event_ref"))
            expected_atomic = final_version(change.get("atomic_ref"))
            if (validate_supplied_versions and "expected_event_version" in change and change.get("expected_event_version") is not None
                    and change.get("expected_event_version") != expected_event):
                raise CandidateValidationError("coverage_event_version_mismatch", ("coverage_changes", value["coverage_changes"].index(change), "expected_event_version"),
                                               "model supplied a stale program-managed version")
            if (validate_supplied_versions and "expected_atomic_version" in change and change.get("expected_atomic_version") is not None
                    and change.get("expected_atomic_version") != expected_atomic):
                raise CandidateValidationError("coverage_atomic_version_mismatch", ("coverage_changes", value["coverage_changes"].index(change), "expected_atomic_version"),
                                               "model supplied a stale program-managed version")
            change["expected_event_version"] = expected_event
            change["expected_atomic_version"] = expected_atomic
    has_changes = bool(
        value.get("node_changes") or value.get("edges") or value.get("profile_changes")
        or value.get("relationship_change") or value.get("coverage_changes")
    )
    value["result"] = "changes" if has_changes else "no_change"
    value["reason"] = "changes" if has_changes else "no_meaningful_content"
    return value


def parse_extraction_output_for_context(raw: str, context: Any, *, schema_version: str | None = None,
                                       validate_supplied_versions: bool = True) -> ExtractionOutput:
    """Parse model output after deriving internal versions from trusted input."""
    decoded = _normalise_program_versions(_decode_output(raw), context,
                                          validate_supplied_versions=validate_supplied_versions)
    return _output_model(schema_version or decoded.get("schema_version", SCHEMA_VERSION)).model_validate(decoded)
