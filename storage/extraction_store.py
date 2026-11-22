"""Transactional write boundary for validated extraction candidates."""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

from .job_store import assert_job_lease, assert_scope_available
from .pipeline_store import CONTEXT_REBUILDS, assert_rebuild_target
from .topic_dependencies import write_member_dependencies
from .version_retention import prune_node_history
from .content_times import refresh_content_time
from .provenance_store import (
    record_derivation,
    snapshot_context_version,
    version_reference,
)

try:
    from ..core.extraction_apply import digest
    from ..core.extraction_errors import InvalidExtractionCandidate

    from ..core.memory_emotions import encode_emotion
    from ..core.relationship_state import relationship_slots, relationship_text, transition_relationship
    from ..core.prompts import schema_for_prompt_version
    from ..core.profile_memory_prompt import GROUP_PROFILE_MAX_TAGS, GROUP_PROFILE_MAX_DESCRIPTION_CHARS
    from ..core.group_addressing import address_snapshot_users
    from ..core.memory_text import user_reference_ids

    from ..core.text_lengths import readable_text_length
    from ..core.topic_schema import TOPIC_CREATE_MIN_MEMBERS
except ImportError:
    from core.extraction_apply import digest
    from core.extraction_errors import InvalidExtractionCandidate

    from core.memory_emotions import encode_emotion
    from core.relationship_state import relationship_slots, relationship_text, transition_relationship
    from core.prompts import schema_for_prompt_version
    from core.profile_memory_prompt import GROUP_PROFILE_MAX_TAGS, GROUP_PROFILE_MAX_DESCRIPTION_CHARS
    from core.group_addressing import address_snapshot_users
    from core.memory_text import user_reference_ids

    from core.text_lengths import readable_text_length
    from core.topic_schema import TOPIC_CREATE_MIN_MEMBERS


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)


async def _one(connection: Any, sql: str, values: tuple = ()) -> Any:
    cursor = await connection.execute(sql, values)
    return await cursor.fetchone()


async def assert_extraction_scope(
    connection, *, space_id: str, generation_id: str | None, job_id: str | None = None,
    lease_token: str | None = None, allow_building: bool = False,
):
    """Validate lease, operation kind and generation together inside a transaction."""
    if type(allow_building) is not bool:
        raise ValueError("allow_building must be a boolean")
    if (job_id is None) != (lease_token is None):
        raise ValueError("job_id and lease_token must be supplied together")
    job = None
    if job_id is not None:
        job = await assert_job_lease(connection, job_id, lease_token, scope_key=space_id)
        if job["kind"] not in {"extract", "rebuild", "reject", *CONTEXT_REBUILDS}:
            raise PermissionError("task kind cannot apply extraction")
        if job["kind"] in {"rebuild", *CONTEXT_REBUILDS} and not allow_building:
            raise PermissionError("rebuild extraction must target its building generation")
    else:
        await assert_scope_available(connection, space_id)
    generation = await _one(
        connection,
        "SELECT * FROM memory_generations WHERE space_id=? AND "
        + ("generation_id=?" if generation_id is not None else "status='active'"),
        (space_id, generation_id) if generation_id is not None else (space_id,),
    )
    if generation is None:
        raise ValueError("no matching generation for extraction")
    if allow_building:
        if job is None:
            raise PermissionError("building extraction requires a rebuild task lease")
        await assert_rebuild_target(connection, job, space_id, generation["generation_id"])
        if generation["status"] != "building":
            raise ValueError("rebuild extraction generation is no longer building")
    elif generation["status"] != "active":
        raise ValueError("stale extraction generation is no longer active")
    return generation


def _evidence(
    source_ids: list[str], sources: dict[str, Any], user: str | None = None,
    *, path: str,
) -> None:
    for index, source_id in enumerate(source_ids):
        if source_id not in sources:
            raise InvalidExtractionCandidate("source_outside_window", f"{path}[{index}]",
                "source_id outside the fixed extraction window")
    if user is not None:
        authors = {
            sources[source_id]["actor_logical_user_id"]
            for source_id in source_ids
            if sources[source_id]["actor_kind"] == "user"
        }
        if authors != {user}:
            raise InvalidExtractionCandidate("evidence_user_mismatch", path,
                "profile/relationship evidence must contain this user's own words"
            )


async def _node(
    connection: Any,
    store: Any,
    batch: Any,
    node_id: str,
    known: dict[str, Any],
    *,
    writable: bool = True,
    path: str = "$",
) -> Any:
    snapshot = known.get(node_id)
    if snapshot is None:
        raise InvalidExtractionCandidate("node_outside_context", path,
            "node ID was not provided in the extraction context")
    current = await store._node_row_in_connection(connection, node_id)
    if (
        not current
        or current["space_id"] != batch.space["space_id"]
        or current["generation_id"] != batch.generation_id
    ):
        raise ValueError("node belongs to another space or generation")
    if bool(current["manual_protected"]) != snapshot["manual_protected"]:
        raise ValueError("node protection changed")
    if (
        current["content_version"] != snapshot["content_version"]
        or current["updated_at"] != snapshot["updated_at"]
        or current["status"] != snapshot["status"]
        or current["title"] != snapshot.get("title")
    ):
        raise ValueError("stale node snapshot; prepare extraction again")
    if writable and (current["manual_protected"] or current["status"] != "active"):
        raise InvalidExtractionCandidate("readonly_node", path, "candidate writes a read-only node")
    return current


def _node_precondition(change, old, path):
    if old["node_type"] != change.node_type:
        raise InvalidExtractionCandidate("node_type_mismatch", path + ".node_type",
            "node type or expected_version does not match")
    if old["content_version"] != change.expected_version:
        raise InvalidExtractionCandidate("expected_node_version_mismatch", path + ".expected_version",
            "node type or expected_version does not match")


def _node_content_change(change: Any, old: Any) -> tuple[str | None, bool]:
    """Changing either prose or emotion creates a new content version."""
    emotion_json = encode_emotion(change.emotion.model_dump() if change.emotion else None)
    changed = old is None or old["content_text"] != change.content or old["emotion_json"] != emotion_json
    return emotion_json, changed


async def _write_node(
    connection: Any,
    change: Any,
    old: Any,
    node_id: str,
    batch: Any,
    activity_round: int,
    now: float,
    path: str,
) -> dict:
    title = change.title
    title_changed = old is not None and title != old["title"]
    emotion_json, content_changed = _node_content_change(change, old)
    times = (
        (change.event_time.start, change.event_time.end, change.event_time.text)
        if change.event_time
        else (
            (old["event_time_start"], old["event_time_end"], old["event_time_text"])
            if old
            else (None, None, None)
        )
    )
    if old is None:
        version = 1
        await connection.execute(
            """INSERT INTO memory_nodes(
                node_id,generation_id,space_id,node_type,status,importance,strength,
                event_time_start,event_time_end,event_time_text,created_at,updated_at,
                strength_reset_at,strength_reset_round,title,emotion_json)
                VALUES (?,?,?,?,'active',?,?,?,?,?,?,?,?,?,?,?)""",
            (
                node_id,
                batch.generation_id,
                batch.space["space_id"],
                change.node_type,
                change.importance,
                change.importance,
                *times,
                now,
                now,
                now,
                activity_round,
                title,
                emotion_json,
            ),
        )
    else:
        _node_precondition(change, old, path)
        reset = content_changed or old["importance"] != change.importance
        version = old["content_version"] + int(content_changed)
        title_only = title_changed and not reset and times == (
            old["event_time_start"], old["event_time_end"], old["event_time_text"]
        )
        await connection.execute(
            """UPDATE memory_nodes SET content_version=?, importance=?, strength=?,
                event_time_start=?,event_time_end=?,event_time_text=?,updated_at=?,
                strength_reset_at=?,strength_reset_round=?,reinforcement=?,title=?,emotion_json=? WHERE node_id=?""",
            (
                version,
                change.importance,
                change.importance if reset else old["strength"],
                *times,
                old["updated_at"] if title_only else now,
                now if reset else old["strength_reset_at"],
                activity_round if reset else old["strength_reset_round"],
                0 if reset else old["reinforcement"],
                title,
                emotion_json,
                node_id,
            ),
        )
        if content_changed and old["node_type"] == "event":
            # Old coverage describes the old narrative, not the new event text.
            await connection.execute(
                "DELETE FROM node_coverage WHERE event_node_id=?", (node_id,)
            )
    if content_changed:
        await connection.execute(
            """INSERT INTO memory_node_versions(node_version_id,node_id,version_no,
                content_text,metadata_json,change_kind,editor_kind,created_at,title,emotion_json)
                VALUES (?,?,?,?,?,?,'model',?,?,?)""",
            (
                _id("nodever"),
                node_id,
                version,
                change.content,
                old["content_metadata_json"] if old else "{}",
                "model_update" if old else "created",
                now,
                title,
                emotion_json,
            ),
        )
    await connection.executemany(
        "INSERT OR IGNORE INTO node_sources(node_id,source_id,source_kind,created_at) VALUES (?,?,'source_record',?)",
        [(node_id, source_id, now) for source_id in change.source_ids],
    )
    source_lookup = {source["source_id"]: source for source in batch.sources}
    participants = {
        source_lookup[s]["actor_logical_user_id"]
        for s in change.source_ids
        if source_lookup[s]["actor_kind"] == "user"
    }
    if batch.space["space_type"] == "group":
        targets = address_snapshot_users(source_lookup[s] for s in change.source_ids)
        participants.update(set(targets).intersection(user_reference_ids([change.content])))
    await connection.executemany(
        "INSERT OR IGNORE INTO node_participants(node_id,participant_kind,participant_id) VALUES (?,'logical_user',?)",
        [(node_id, user) for user in participants if user],
    )
    provenance = {}
    if title_changed and not content_changed:
        # 展示标题改变留独立操作；不把它伪装成新正文或旧正文的再确认。
        provenance["semantic"] = {"title_before": old["title"], "title_after": title}
    bases = [version_reference("node", node_id, old["content_version"])] if old and content_changed else []
    if content_changed and change.base_node_ids:
        snapshots = {item["node_id"]: item for item in batch.known_nodes}
        for base_id in change.base_node_ids:
            reference = version_reference("node", base_id, snapshots[base_id]["content_version"])
            if reference not in bases:
                bases.append(reference)
    return {
        "kind": "node", "entity_id": node_id, "version_no": version,
        "operation": ("update" if old else "create") if content_changed else (
            "title_update" if title_changed else "confirm"
        ),
        "base_versions": bases,
        "sources": [(source_id, "source_record") for source_id in change.source_ids],
        **provenance,
    }


async def _write_profile(
    connection: Any, batch: Any, change: Any, sources: dict, now: float, path: str,
) -> dict:
    space = batch.space
    if (
        space["space_type"] == "private"
        and space["logical_user_id"] != change.logical_user_id
    ):
        raise InvalidExtractionCandidate("profile_user_mismatch", path + ".logical_user_id",
            "profile belongs to another private user")
    description_length = readable_text_length(change.description)
    if space["space_type"] == "group" and (
        len(change.tags) > GROUP_PROFILE_MAX_TAGS or description_length > GROUP_PROFILE_MAX_DESCRIPTION_CHARS
    ):
        field = ".tags" if len(change.tags) > GROUP_PROFILE_MAX_TAGS else ".description"
        raise InvalidExtractionCandidate("group_profile_limit", path + field,
            "group profile exceeds the lightweight limit")
    _evidence(change.source_ids, sources, change.logical_user_id, path=path + ".source_ids")
    old = await _one(
        connection,
        "SELECT * FROM user_profiles WHERE generation_id=? AND logical_user_id=?",
        (batch.generation_id, change.logical_user_id),
    )
    snapshot = next(
        (p for p in batch.profiles if p["logical_user_id"] == change.logical_user_id),
        None,
    )
    expected = snapshot["content_version"] if snapshot else 0
    if (
        (old["content_version"] if old else 0) != expected
        or (old and old["status"] != "active")
    ):
        raise ValueError("stale or inactive profile")
    if change.expected_version != expected:
        raise InvalidExtractionCandidate("expected_profile_version_mismatch", path + ".expected_version",
            "stale or inactive profile")
    if old:
        profile_id = old["profile_id"]
        changed = (
            json.loads(old["tags_json"]) != change.tags
            or old["description"] != change.description
        )
        await connection.execute(
            "UPDATE user_profiles SET tags_json=?,description=?,content_version=?,updated_at=? WHERE profile_id=?",
            (
                _json(change.tags),
                change.description,
                expected + int(changed),
                now,
                old["profile_id"],
            ),
        )
    else:
        profile_id = _id("profile")
        changed = True
        await connection.execute(
            """INSERT INTO user_profiles(profile_id,generation_id,space_id,logical_user_id,
                tags_json,description,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)""",
            (
                profile_id,
                batch.generation_id,
                space["space_id"],
                change.logical_user_id,
                _json(change.tags),
                change.description,
                now,
                now,
            ),
        )
    version = expected + int(changed)
    await snapshot_context_version(connection, "profile", profile_id)
    return {
        "kind": "profile", "entity_id": profile_id, "version_no": version,
        "operation": ("update" if old else "create") if changed else "confirm",
        "base_versions": [version_reference("profile", profile_id, expected)] if old and changed else [],
        "sources": [(source_id, "source_record") for source_id in change.source_ids],
    }


async def _write_relationship(
    connection: Any, batch: Any, change: Any, sources: dict, now: float, path: str,
) -> dict | None:
    if (
        batch.space["space_type"] != "private"
        or batch.space["logical_user_id"] != change.logical_user_id
    ):
        raise InvalidExtractionCandidate("relationship_user_mismatch", path + ".logical_user_id",
            "relationship requires the private space's logical user")
    _evidence(change.source_ids, sources, change.logical_user_id, path=path + ".source_ids")
    old = await _one(
        connection,
        "SELECT * FROM relationship_states WHERE generation_id=? AND logical_user_id=?",
        (batch.generation_id, change.logical_user_id),
    )
    expected = batch.relationship["content_version"] if batch.relationship else 0
    if (
        (old["content_version"] if old else 0) != expected
        or (old and old["status"] != "active")
    ):
        raise ValueError("stale or inactive relationship")
    if change.expected_version != expected:
        raise InvalidExtractionCandidate("expected_relationship_version_mismatch", path + ".expected_version",
            "stale or inactive relationship")
    operations = {slot: getattr(change, slot).model_dump() for slot in ("long_term", "aftereffect")}
    previous = relationship_slots(state=json.loads(old["state_json"])) if old else relationship_slots()
    if all(operation["op"] == "keep" for operation in operations.values()):
        return None
    state = transition_relationship(previous, operations,
                                    max(sources[source_id]["observed_at"] for source_id in change.source_ids))
    text = relationship_text(state)
    if state == previous:
        return None
    if old:
        relationship_id = old["relationship_id"]
        changed = (
            old["state_text"] != text
            or json.loads(old["state_json"]) != state
        )
        await connection.execute(
            "UPDATE relationship_states SET state_text=?,state_json=?,content_version=?,updated_at=? WHERE relationship_id=?",
            (
                text,
                _json(state),
                expected + int(changed),
                now,
                old["relationship_id"],
            ),
        )
    else:
        relationship_id = _id("relationship")
        changed = True
        await connection.execute(
            """INSERT INTO relationship_states(relationship_id,generation_id,space_id,logical_user_id,
                state_text,state_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)""",
            (
                relationship_id,
                batch.generation_id,
                batch.space["space_id"],
                change.logical_user_id,
                text,
                _json(state),
                now,
                now,
            ),
        )
    version = expected + int(changed)
    await snapshot_context_version(connection, "relationship", relationship_id)
    return {
        "kind": "relationship", "entity_id": relationship_id, "version_no": version,
        "operation": ("update" if old else "create") if changed else "confirm",
        "base_versions": [version_reference("relationship", relationship_id, expected)] if old and changed else [],
        "sources": [(source_id, "source_record") for source_id in change.source_ids],
    }


async def apply_extraction(
    store: Any, batch: Any, output: Any, *, model_id: str, prompt_version: str, image_evidence=()
) -> dict[str, Any]:
    if prompt_version != batch.prompt_version:
        raise ValueError("commit prompt version differs from the prepared batch")
    if not isinstance(model_id, str) or not model_id.strip() or len(model_id) > 200:
        raise ValueError("model_id is required and must fit 200 characters")
    if type(batch.source_cursor) is not int or batch.source_cursor < 0:
        raise ValueError("source_cursor must be a nonnegative integer")
    if output.schema_version != schema_for_prompt_version(batch.prompt_version):
        raise InvalidExtractionCandidate("schema_mismatch", "$.schema_version", "extraction schema differs from the prepared batch")
    source_lookup = {source["source_id"]: source for source in batch.sources}
    known = {node["node_id"]: node for node in batch.known_nodes}
    output_snapshot = output.model_dump(mode="json")
    if image_evidence:
        output_snapshot["image_evidence"] = image_evidence
    output_digest = digest(output_snapshot)
    input_digest = batch.input_digest
    now = time.time()
    async with store.database.transaction() as connection:
        generation = await assert_extraction_scope(
            connection, space_id=batch.space["space_id"], generation_id=batch.generation_id,
            job_id=batch.job_id, lease_token=batch.lease_token, allow_building=batch.allow_building,
        )
        previous = await _one(
            connection,
            "SELECT * FROM extraction_batches WHERE batch_id=?",
            (batch.batch_id,),
        )
        if previous:
            if (
                previous["output_digest"] != output_digest
                or previous["input_digest"] != input_digest
            ):
                raise ValueError(
                    "batch already committed with different input or output"
                )
            result = dict(previous)
            result["result"] = json.loads(result.pop("result_json"))
            return {**result, "replayed": True}
        if generation["activity_round"] != batch.expected_round:
            raise ValueError("stale extraction generation or activity round")
        # A no-change decision also depends on the supplied context. Validate
        # that context even when the model returns no explicit update targets.
        for node_id in known:
            await _node(connection, store, batch, node_id, known, writable=False)
        for index, change in enumerate(output.node_changes):
            for base_index, base_id in enumerate(change.base_node_ids):
                if base_id not in known:
                    raise InvalidExtractionCandidate("node_outside_context",
                        f"$.node_changes[{index}].base_node_ids[{base_index}]",
                        "basis must name an existing node in the fixed extraction context")
        for topic in (node for node in known.values() if node["node_type"] == "topic"):
            cursor = await connection.execute(
                "SELECT target_node_id FROM memory_edges WHERE source_node_id=? AND relation_type='contains'",
                (topic["node_id"],))
            current_members = {row["target_node_id"] for row in await cursor.fetchall()}
            expected_members = {edge["target_node_id"] for edge in batch.known_edges
                                if edge["source_node_id"] == topic["node_id"]}
            if current_members != expected_members:
                raise ValueError("topic membership changed; prepare extraction again")
        for event in (node for node in known.values() if node["node_type"] == "event"):
            cursor = await connection.execute(
                "SELECT target_node_id FROM memory_edges WHERE source_node_id=? AND relation_type='contains'",
                (event["node_id"],))
            current_members = {row["target_node_id"] for row in await cursor.fetchall()
                               if row["target_node_id"] in known}
            expected_members = {edge["target_node_id"] for edge in batch.known_edges
                                if edge["source_node_id"] == event["node_id"]}
            if current_members != expected_members:
                raise ValueError("event member structure changed; prepare extraction again")
        speakers = {
            source["actor_logical_user_id"]
            for source in batch.sources
            if source["actor_kind"] == "user"
        }
        profile_cursor = await connection.execute(
            "SELECT logical_user_id,content_version,status,updated_at FROM user_profiles WHERE generation_id=?",
            (batch.generation_id,),
        )
        profile_fields = ("logical_user_id", "content_version", "status", "updated_at")
        actual_profiles = sorted(
            tuple(row[field] for field in profile_fields)
            for row in await profile_cursor.fetchall()
            if row["logical_user_id"] in speakers
        )
        expected_profiles = sorted(
            tuple(row[field] for field in profile_fields) for row in batch.profiles
        )
        if actual_profiles != expected_profiles:
            raise ValueError("stale profile context")
        if batch.space["space_type"] == "private":
            relationship = await _one(
                connection,
                "SELECT * FROM relationship_states WHERE generation_id=? AND logical_user_id=?",
                (batch.generation_id, batch.space["logical_user_id"]),
            )
            actual_relationship = (
                tuple(relationship[field] for field in profile_fields)
                if relationship
                else None
            )
            expected_relationship = (
                tuple(batch.relationship[field] for field in profile_fields)
                if batch.relationship
                else None
            )
            if actual_relationship != expected_relationship:
                raise ValueError("stale relationship context")
        for source_id in source_lookup:
            existing = await _one(
                connection,
                "SELECT batch_id FROM extraction_batch_sources WHERE generation_id=? AND source_id=?",
                (batch.generation_id, source_id),
            )
            if existing:
                raise ValueError("source already consumed by another extraction batch")
            source = source_lookup[source_id]
            if source.get("delivered_proactive"):
                continue
            previous_range = await _one(
                connection,
                "SELECT MAX(sequence_no) AS highwater FROM extraction_batch_sources WHERE generation_id=? AND stream_id=? AND delivered_proactive=0",
                (batch.generation_id, source["stream_id"]),
            )
            if (
                previous_range["highwater"] is not None
                and source["sequence_no"] <= previous_range["highwater"]
            ):
                raise ValueError(
                    "out-of-order source range; replay older input in a new generation"
                )
        activity_round = batch.expected_round + 1
        refs: dict[str, str] = {}
        types: dict[str, str] = {}
        # Validate all existing endpoints before any changes; never re-read an
        # updated node against its own pre-update snapshot later in the batch.
        referenced = {change.node_id: f"$.node_changes[{index}].node_id"
            for index, change in enumerate(output.node_changes) if change.operation == "update"}
        local_refs = {c.local_ref for c in output.node_changes}
        for index, edge in enumerate(output.edges):
            for field in ("from_ref", "to_ref"):
                ref = getattr(edge, field)
                if ref not in local_refs:
                    referenced.setdefault(ref, f"$.edges[{index}].{field}")
        coverage_changes = getattr(output, "coverage_changes", ())
        for index, coverage in enumerate(coverage_changes):
            for field in ("event_ref", "atomic_ref"):
                ref = getattr(coverage, field)
                if ref not in local_refs:
                    referenced.setdefault(ref, f"$.coverage_changes[{index}].{field}")
        old_nodes = {
            node_id: await _node(connection, store, batch, node_id, known, path=path)
            for node_id, path in referenced.items()
        }
        versions: dict[str, int] = {}
        for node_id, old in old_nodes.items():
            refs[node_id] = node_id
            types[node_id] = old["node_type"]
            versions[node_id] = old["content_version"]
        for index, change in enumerate(output.node_changes):
            path = f"$.node_changes[{index}]"
            _evidence(change.source_ids, source_lookup, path=path + ".source_ids")
            node_id = change.node_id if change.operation == "update" else _id("node")
            refs[change.local_ref] = node_id
            types[node_id] = change.node_type
            old = old_nodes.get(node_id)
            if old is not None:
                _node_precondition(change, old, path)
            versions[node_id] = (
                old["content_version"] + int(_node_content_change(change, old)[1])
                if old is not None
                else 1
            )
        # Coverage describes final semantic content, including emotion, so validate against the prospective
        # versions before writing any nodes. Existing endpoint checks above
        # enforce the same scope, snapshot and protection rules as node edits.
        coverage_rows = []
        coverage_pairs = set()
        for index, coverage in enumerate(coverage_changes):
            path = f"$.coverage_changes[{index}]"
            _evidence(coverage.source_ids, source_lookup, path=path + ".source_ids")
            event_id, atomic_id = refs[coverage.event_ref], refs[coverage.atomic_ref]
            if (types[event_id], types[atomic_id]) != ("event", "atomic"):
                field = ".event_ref" if types[event_id] != "event" else ".atomic_ref"
                raise InvalidExtractionCandidate("coverage_endpoint_type_mismatch", path + field,
                    "coverage must connect an event to an atomic node")
            pair = (event_id, atomic_id)
            if pair in coverage_pairs:
                raise InvalidExtractionCandidate("duplicate_coverage_pair", path,
                    "coverage endpoints resolve to a duplicate pair")
            coverage_pairs.add(pair)
            if versions[event_id] != coverage.expected_event_version:
                raise InvalidExtractionCandidate("coverage_event_version_mismatch", path + ".expected_event_version",
                    "coverage expected final content versions do not match")
            if versions[atomic_id] != coverage.expected_atomic_version:
                raise InvalidExtractionCandidate("coverage_atomic_version_mismatch", path + ".expected_atomic_version",
                    "coverage expected final content versions do not match")
            coverage_rows.append(
                (
                    event_id,
                    atomic_id,
                    coverage.expected_atomic_version,
                    coverage.expected_event_version,
                    coverage.coverage_kind,
                    now,
                )
            )
        derivations = []
        for index, change in enumerate(output.node_changes):
            node_id = refs[change.local_ref]
            derivations.append(await _write_node(
                connection,
                change,
                old_nodes.get(node_id),
                node_id,
                batch,
                activity_round,
                now,
                f"$.node_changes[{index}]",
            ))
        await connection.executemany(
            """INSERT INTO node_coverage(event_node_id,atomic_node_id,
                atomic_content_version,event_content_version,coverage_kind,created_at)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(event_node_id,atomic_node_id) DO UPDATE SET
                    atomic_content_version=excluded.atomic_content_version,
                    event_content_version=excluded.event_content_version,
                    coverage_kind=excluded.coverage_kind,created_at=excluded.created_at""",
            coverage_rows,
        )
        topic_members = {}
        membership_sources = {}
        for index, edge in enumerate(output.edges):
            path = f"$.edges[{index}]"
            _evidence(edge.source_ids, source_lookup, path=path + ".source_ids")
            left, right = refs[edge.from_ref], refs[edge.to_ref]
            if left == right:
                raise InvalidExtractionCandidate("edge_self_reference", path,
                    "edge endpoints resolve to the same node")
            if edge.relation_type == "contains" and (types[left], types[right]) not in {
                ("event", "atomic"),
                ("topic", "event"),
                ("topic", "atomic"),
            }:
                raise InvalidExtractionCandidate("invalid_contains_hierarchy", path + ".relation_type",
                    "invalid contains hierarchy")
            if edge.relation_type == "supersedes":
                if types[left] != types[right]:
                    raise InvalidExtractionCandidate("supersedes_node_type_mismatch", path + ".relation_type",
                        "supersedes requires matching node types")
                cycle = await _one(
                    connection,
                    """WITH RECURSIVE successors(id) AS (
                        SELECT target_node_id FROM memory_edges
                        WHERE source_node_id=? AND relation_type='supersedes'
                        UNION SELECT e.target_node_id FROM memory_edges e
                        JOIN successors s ON e.source_node_id=s.id
                        WHERE e.relation_type='supersedes'
                    ) SELECT 1 FROM successors WHERE id=? LIMIT 1""",
                    (right, left),
                )
                if cycle:
                    raise InvalidExtractionCandidate("supersedes_cycle", path + ".relation_type",
                        "supersedes must not form a cycle")
                await connection.execute(
                    "UPDATE memory_nodes SET status='historical',updated_at=? WHERE node_id=?",
                    (now, right),
                )
            await connection.execute(
                """INSERT OR IGNORE INTO memory_edges(edge_id,generation_id,space_id,
                    source_node_id,target_node_id,relation_type,created_at) VALUES (?,?,?,?,?,?,?)""",
                (
                    _id("edge"),
                    batch.generation_id,
                    batch.space["space_id"],
                    left,
                    right,
                    edge.relation_type,
                    now,
                ),
            )
            if edge.relation_type == "contains" and types[left] == "topic":
                topic_members.setdefault(left, {})[right] = versions[right]
                membership_sources.setdefault(left, set()).update(edge.source_ids)
        for index, change in enumerate(output.node_changes):
            if change.operation != "create" or change.node_type != "topic":
                continue
            # Member IDs are resolved and deduplicated above; two aliases
            # for one node do not satisfy the structural topic minimum.
            # Semantic independence remains a model judgement.
            members = topic_members.get(refs[change.local_ref], {})
            if len(members) < TOPIC_CREATE_MIN_MEMBERS:
                raise InvalidExtractionCandidate(
                    "ordinary_topic_create_members", f"$.node_changes[{index}]",
                    "a new topic requires at least two distinct event/atomic members",
                )
        # Explicit inherited bases can re-assess existing membership without
        # declaring a new edge. They refer to the prepared version, even when
        # another change in this batch updates that member.
        for change in output.node_changes:
            if change.node_type != "topic":
                continue
            topic_id = refs[change.local_ref]
            for member_id in change.base_node_ids:
                member = await _one(connection,
                    "SELECT 1 FROM memory_edges WHERE source_node_id=? AND target_node_id=? AND relation_type='contains'",
                    (topic_id, member_id))
                if member:
                    topic_members.setdefault(topic_id, {}).setdefault(member_id, known[member_id]["content_version"])
        node_derivations = {item["entity_id"]: item for item in derivations}
        for topic_id, member_versions in topic_members.items():
            await write_member_dependencies(connection, topic_id, member_versions, now=now)
            bases = [version_reference("node", member_id, version) for member_id, version in member_versions.items()]
            derivation = node_derivations.get(topic_id)
            if derivation and derivation["operation"] in {"create", "update"}:
                derivation["base_versions"].extend(base for base in bases if base not in derivation["base_versions"])
            else:
                # Associating an event is not a claim that the unchanged topic
                # text was generated from or fully represents that event.
                derivations.append({
                    "kind": "node", "entity_id": topic_id, "version_no": versions[topic_id],
                    "operation": "membership", "base_versions": [],
                    "sources": [(source_id, "source_record") for source_id in sorted(membership_sources.get(topic_id, ()))],
                    "semantic": {"member_versions": bases},
                })
        for index, change in enumerate(output.profile_changes):
            derivations.append(await _write_profile(connection, batch, change, source_lookup, now, f"$.profile_changes[{index}]"))
        if output.relationship_change:
            derivation = await _write_relationship(
                connection, batch, output.relationship_change, source_lookup, now, "$.relationship_change")
            if derivation is not None:
                derivations.append(derivation)
        result = {
            "kind": output.result,
            "node_refs": {c.local_ref: refs[c.local_ref] for c in output.node_changes},
        }
        await connection.execute(
            "UPDATE memory_generations SET activity_round=?,updated_at=? WHERE generation_id=?",
            (activity_round, now, batch.generation_id),
        )
        await connection.execute(
            """INSERT INTO extraction_batches(batch_id,space_id,generation_id,activity_round,
                prompt_version,schema_version,model_id,input_digest,output_digest,result_json,committed_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                batch.batch_id,
                batch.space["space_id"],
                batch.generation_id,
                activity_round,
                prompt_version,
                output.schema_version,
                model_id,
                input_digest,
                output_digest,
                _json(result),
                now,
            ),
        )
        await connection.executemany(
            "INSERT INTO extraction_batch_sources(batch_id,generation_id,source_id,ordinal,stream_id,sequence_no,delivered_proactive) VALUES (?,?,?,?,?,?,?)",
            [
                (
                    batch.batch_id,
                    batch.generation_id,
                    source_id,
                    index,
                    source_lookup[source_id]["stream_id"],
                    source_lookup[source_id]["sequence_no"],
                    int(bool(source_lookup[source_id].get("delivered_proactive"))),
                )
                for index, source_id in enumerate(source_lookup)
            ],
        )
        if image_evidence:
            # The receipt and its pending transfer survive a restart together.
            await connection.execute(
                "INSERT INTO pending_image_evidence(batch_id, evidence_json) VALUES (?,?)",
                (batch.batch_id, _json(image_evidence)),
            )
        source_observations = {source_id: source["observed_at"] for source_id, source in source_lookup.items()}
        for derivation in derivations:
            await record_derivation(
                connection, **derivation, editor_kind="model", now=now, batch_id=batch.batch_id,
                source_observations=source_observations, refresh_node_time=False,
            )
        # New child nodes may follow their parent in model output order.
        pending_versions = {(d["entity_id"], d["version_no"]) for d in derivations
                            if d["kind"] == "node" and d["operation"] not in {"title_update", "membership"}}
        for node_id, version_no in pending_versions:
            await refresh_content_time(connection, node_id, version_no, pending_versions=pending_versions)
        for node_id, version_no in pending_versions:
            if version_no > 1:
                await prune_node_history(connection, node_id)
        if batch.source_cursor > 0:
            await connection.execute(
                "INSERT INTO derived_cursors(generation_id,scan_cursor) VALUES (?,?) "
                "ON CONFLICT(generation_id) DO UPDATE SET scan_cursor=MAX(scan_cursor,excluded.scan_cursor)",
                (batch.generation_id, batch.source_cursor),
            )
    return await store.get_extraction_batch(batch.batch_id)
