"""Deterministic async CRUD for the derived ``memory.db`` database."""

from __future__ import annotations

import inspect
import json
import math
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable, Mapping
from pathlib import Path
from typing import Any

try:
    from ..core.memory_emotions import EMOTION_UNSET, decode_emotion, encode_emotion
    from ..core.memory_nodes import (
        PARTICIPANT_KINDS,
        validate_content,
        validate_edge_type,
        validate_node_status,
        validate_node_type,
        validate_title,
    )
    from ..core.memory_notes import validate_note_content, validate_note_status
    from ..core.memory_profiles import (
        validate_profile_description,
        validate_profile_tags,
    )
    from ..core.memory_relationships import validate_relationship_text
    from ..core.relationship_state import relationship_slots, relationship_text, validate_relationship_slots
    from ..core.memory_spaces import MemorySpaceSpec, RetiredIdentity
except ImportError:  # Tests import ``storage`` directly from the plugin root.
    from core.memory_emotions import EMOTION_UNSET, decode_emotion, encode_emotion
    from core.memory_nodes import (
        PARTICIPANT_KINDS,
        validate_content,
        validate_edge_type,
        validate_node_status,
        validate_node_type,
        validate_title,
    )
    from core.memory_notes import validate_note_content, validate_note_status
    from core.memory_profiles import validate_profile_description, validate_profile_tags
    from core.memory_relationships import validate_relationship_text
    from core.relationship_state import relationship_slots, relationship_text, validate_relationship_slots
    from core.memory_spaces import MemorySpaceSpec, RetiredIdentity

from .job_store import assert_scope_available
from .extraction_context_store import ExtractionContextStoreMixin
from .memory_schema import MEMORY_SCHEMA
from .schema import CURRENT_SCHEMA_VERSION, initialize_schema
from .provenance_store import (
    ProvenanceStoreMixin,
    record_derivation,
    snapshot_context_version,
    version_reference,
)
from .recall_store import RecallStoreMixin
from .sqlite_executor import SQLiteExecutor
from .version_retention import apply_version_limits

SourceExists = Callable[[str], bool | Awaitable[bool]]
SourceObservations = Callable[[tuple[str, ...]], Awaitable[dict[str, float]]]


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _json(value: Mapping[str, Any] | list[Any] | tuple[Any, ...] | None, default: str) -> str:
    if value is None:
        return default
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _dict_json(value: str | None, default: Any) -> Any:
    try:
        return json.loads(value or "")
    except (TypeError, json.JSONDecodeError):
        return default


def _score(value: float, name: str) -> float:
    result = float(value)
    if not 0 <= result <= 1:
        raise ValueError(f"{name} must be between 0 and 1")
    return result


def _now() -> float:
    return time.time()


class MemoryStore(RecallStoreMixin, ProvenanceStoreMixin, ExtractionContextStoreMixin):
    """Storage for the derived memory database.

    ``source_exists`` is deliberately an injected callback because SQLite does
    not support a foreign key to the separate source database. The callback is
    never called while a memory transaction is held.
    """

    def __init__(self, path: str | Path, *, source_exists: SourceExists | None = None,
                 source_observations: SourceObservations | None = None):
        self.database = SQLiteExecutor(path)
        self.path = Path(path)
        self.schema_version = 0
        self.source_exists = source_exists
        self.source_observations = source_observations

    async def open(self, *, version_limits=None) -> None:
        try:
            await self.database.open()
            self.schema_version = await initialize_schema(self.database, MEMORY_SCHEMA)
            await apply_version_limits(self.database, version_limits)
        except BaseException:
            await self.database.close()
            raise

    async def close(self) -> None:
        await self.database.close()

    async def configure_version_limits(self, limits):
        await apply_version_limits(self.database, limits)

    async def health(self) -> dict[str, Any]:
        nodes = await self.database.fetchone("SELECT COUNT(*) AS count FROM memory_nodes")
        spaces = await self.database.fetchone(
            "SELECT COUNT(*) AS count FROM memory_spaces"
        )
        generations = await self.database.fetchone(
            "SELECT COUNT(*) AS count FROM memory_generations"
        )
        return {
            "path": str(self.path),
            "exists": self.path.exists(),
            "schema_version": self.schema_version,
            "expected_schema_version": CURRENT_SCHEMA_VERSION,
            "node_count": int(nodes["count"] if nodes else 0),
            "space_count": int(spaces["count"] if spaces else 0),
            "generation_count": int(generations["count"] if generations else 0),
        }

    async def _source_is_known(self, source_id: str) -> bool:
        if self.source_exists is None:
            return True
        result = self.source_exists(source_id)
        if inspect.isawaitable(result):
            result = await result
        return bool(result)

    @staticmethod
    def _space_from_row(row: Any) -> dict[str, Any]:
        return dict(row)

    @staticmethod
    def _generation_from_row(row: Any) -> dict[str, Any]:
        return dict(row)

    @staticmethod
    def _node_from_row(row: Any) -> dict[str, Any]:
        item = dict(row)
        item["metadata"] = _dict_json(item.pop("node_metadata_json", None), {})
        item.pop("metadata_json", None)
        item["content_metadata"] = _dict_json(
            item.pop("content_metadata_json", None), {}
        )
        item["manual_protected"] = bool(item["manual_protected"])
        item["emotion"] = decode_emotion(item.pop("emotion_json", None))
        return item

    async def get_or_create_space(self, spec: MemorySpaceSpec) -> dict[str, Any]:
        now = _now()
        async with self.database.transaction() as connection:
            if spec.space_type == "private":
                retired = await connection.execute(
                    "SELECT 1 FROM pipeline_state WHERE state_key=?", ("retired_identity:" + spec.logical_user_id,))
                if await retired.fetchone():
                    raise RetiredIdentity(spec.logical_user_id)
            cursor = await connection.execute(
                "SELECT * FROM memory_spaces WHERE scope_key = ?", (spec.scope_key,)
            )
            existing = await cursor.fetchone()
            if existing:
                return self._space_from_row(existing)
            space_id = _id("space")
            await connection.execute(
                """
                INSERT INTO memory_spaces(
                    space_id, scope_key, space_type, persona_id, logical_user_id,
                    platform_instance_id, group_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    space_id,
                    spec.scope_key,
                    spec.space_type,
                    spec.persona_id,
                    spec.logical_user_id,
                    spec.platform_instance_id,
                    spec.group_id,
                    now,
                    now,
                ),
            )
            generation_id = _id("gen")
            await connection.execute(
                """
                INSERT INTO memory_generations(
                    generation_id, space_id, status, reason, created_at,
                    activated_at, updated_at
                ) VALUES (?, ?, 'active', 'initial', ?, ?, ?)
                """,
                (generation_id, space_id, now, now, now),
            )
            cursor = await connection.execute(
                "SELECT * FROM memory_spaces WHERE space_id = ?", (space_id,)
            )
            return self._space_from_row(await cursor.fetchone())

    async def get_space(self, space_id_or_scope_key: str) -> dict[str, Any] | None:
        row = await self.database.fetchone(
            """
            SELECT * FROM memory_spaces
            WHERE space_id = ? OR scope_key = ?
            """,
            (space_id_or_scope_key, space_id_or_scope_key),
        )
        return self._space_from_row(row) if row else None

    async def _space_in_transaction(self, connection: Any, space_id: str) -> Any:
        cursor = await connection.execute(
            "SELECT * FROM memory_spaces WHERE space_id = ?", (space_id,)
        )
        row = await cursor.fetchone()
        if row is None:
            raise KeyError(f"unknown memory space: {space_id}")
        return row

    async def get_active_generation(self, space_id: str) -> dict[str, Any] | None:
        row = await self.database.fetchone(
            """
            SELECT * FROM memory_generations
            WHERE space_id = ? AND status = 'active'
            """,
            (space_id,),
        )
        return self._generation_from_row(row) if row else None

    async def create_generation(
        self,
        space_id: str,
        *,
        parent_generation_id: str | None = None,
        reason: str | None = None,
        status: str = "building",
        generation_id: str | None = None,
    ) -> dict[str, Any]:
        if status not in {"building", "active", "failed"}:
            raise ValueError(f"unsupported generation status: {status}")
        now = _now()
        generation_id = generation_id or _id("gen")
        async with self.database.transaction() as connection:
            await self._space_in_transaction(connection, space_id)
            if parent_generation_id:
                parent = await connection.execute(
                    """
                    SELECT generation_id FROM memory_generations
                    WHERE generation_id = ? AND space_id = ?
                    """,
                    (parent_generation_id, space_id),
                )
                if await parent.fetchone() is None:
                    raise ValueError("parent generation belongs to another space")
            if status == "active":
                active = await connection.execute(
                    "SELECT generation_id FROM memory_generations WHERE space_id = ? AND status = 'active'",
                    (space_id,),
                )
                if await active.fetchone() is not None:
                    raise ValueError("space already has an active generation")
            await connection.execute(
                """
                INSERT INTO memory_generations(
                    generation_id, space_id, parent_generation_id, status, reason,
                    created_at, activated_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    generation_id,
                    space_id,
                    parent_generation_id,
                    status,
                    reason,
                    now,
                    now if status == "active" else None,
                    now,
                ),
            )
            cursor = await connection.execute(
                "SELECT * FROM memory_generations WHERE generation_id = ?",
                (generation_id,),
            )
            return self._generation_from_row(await cursor.fetchone())

    async def activate_generation(self, space_id: str, generation_id: str) -> dict[str, Any]:
        now = _now()
        async with self.database.transaction() as connection:
            await self._space_in_transaction(connection, space_id)
            target_cursor = await connection.execute(
                """
                SELECT * FROM memory_generations
                WHERE generation_id = ? AND space_id = ?
                """,
                (generation_id, space_id),
            )
            target = await target_cursor.fetchone()
            if target is None:
                raise KeyError(f"unknown generation: {generation_id}")
            if target["status"] == "failed":
                raise ValueError("failed generation cannot be activated")
            await connection.execute(
                """
                UPDATE memory_generations
                SET status = 'retired', retired_at = ?, updated_at = ?
                WHERE space_id = ? AND status = 'active' AND generation_id <> ?
                """,
                (now, now, space_id, generation_id),
            )
            await connection.execute(
                """
                UPDATE memory_generations
                SET status = 'active', activated_at = COALESCE(activated_at, ?),
                    retired_at = NULL, updated_at = ?
                WHERE generation_id = ? AND space_id = ?
                """,
                (now, now, generation_id, space_id),
            )
            cursor = await connection.execute(
                "SELECT * FROM memory_generations WHERE generation_id = ?",
                (generation_id,),
            )
            return self._generation_from_row(await cursor.fetchone())

    async def _generation_in_transaction(
        self, connection: Any, space_id: str, generation_id: str | None
    ) -> Any:
        if generation_id is None:
            cursor = await connection.execute(
                """
                SELECT * FROM memory_generations
                WHERE space_id = ? AND status = 'active'
                """,
                (space_id,),
            )
        else:
            cursor = await connection.execute(
                """
                SELECT * FROM memory_generations
                WHERE space_id = ? AND generation_id = ?
                """,
                (space_id, generation_id),
            )
        generation = await cursor.fetchone()
        if generation is None:
            raise KeyError("no matching generation for memory space")
        return generation

    async def _node_row(self, node_id: str) -> Any:
        return await self.database.fetchone(
            """
            SELECT n.*, n.metadata_json AS node_metadata_json,
                   v.node_version_id, v.version_no AS loaded_version_no,
                   v.content_text, v.metadata_json AS content_metadata_json
            FROM memory_nodes n
            JOIN memory_node_versions v
              ON v.node_id = n.node_id AND v.version_no = n.content_version
            WHERE n.node_id = ?
            """,
            (node_id,),
        )

    async def create_node(
        self,
        *,
        space_id: str,
        node_type: str,
        content_text: str,
        source_ids: Iterable[str],
        title: str | None = None,
        emotion: Mapping[str, Any] | None = None,
        generation_id: str | None = None,
        status: str = "active",
        importance: float = 0.5,
        strength: float | None = None,
        event_time_start: float | None = None,
        event_time_end: float | None = None,
        event_time_text: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        participants: Iterable[Mapping[str, str]] = (),
        source_kind: str = "source_record",
        node_id: str | None = None,
        editor_kind: str = "system",
        manual_protected: bool = False,
        manual_protection_reason: str | None = None,
    ) -> dict[str, Any]:
        node_type = validate_node_type(node_type)
        status = validate_node_status(status)
        content_text = validate_content(content_text)
        title = validate_title(title) if title is not None else None
        emotion_json = encode_emotion(emotion)
        importance = _score(importance, "importance")
        strength = importance if strength is None else _score(strength, "strength")
        source_ids = tuple(dict.fromkeys(str(item).strip() for item in source_ids if str(item).strip()))
        if not source_ids:
            raise ValueError("a memory node requires at least one source_id")
        if (
            event_time_start is not None
            and event_time_end is not None
            and float(event_time_end) < float(event_time_start)
        ):
            raise ValueError("event_time_end cannot precede event_time_start")
        participant_values = self._normalize_participants(participants)
        if source_kind not in {"source_record", "manual", "external"}:
            raise ValueError(f"unsupported source kind: {source_kind}")
        if source_kind == "source_record":
            for source_id in source_ids:
                if not await self._source_is_known(source_id):
                    raise ValueError(f"unknown source_id: {source_id}")
        observations = (await self.source_observations(source_ids)
                        if source_kind == "source_record" and self.source_observations is not None else {})
        if editor_kind not in {"system", "model", "manual", "rebuild"}:
            raise ValueError(f"unsupported editor kind: {editor_kind}")
        now = _now()
        node_id = node_id or _id("node")
        version_id = _id("nodever")
        metadata_json = _json(metadata, "{}")
        if not isinstance(manual_protected, bool):
            raise ValueError("manual_protected must be a boolean")
        if manual_protection_reason is not None:
            manual_protection_reason = str(manual_protection_reason).strip() or None
            if manual_protection_reason and len(manual_protection_reason) > 2000:
                raise ValueError("manual_protection_reason exceeds 2000 characters")
        protected = 1 if manual_protected else 0
        async with self.database.transaction() as connection:
            space = await self._space_in_transaction(connection, space_id)
            generation = await self._generation_in_transaction(
                connection, space_id, generation_id
            )
            await connection.execute(
                """
                INSERT INTO memory_nodes(
                    node_id, generation_id, space_id, node_type, status,
                    content_version, importance, strength, event_time_start,
                    event_time_end, event_time_text, manual_protected,
                    manual_protection_reason,
                    metadata_json, created_at, updated_at,
                    strength_reset_at, strength_reset_round, title, emotion_json
                ) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    node_id,
                    generation["generation_id"],
                    space["space_id"],
                    node_type,
                    status,
                    importance,
                    strength,
                    event_time_start,
                    event_time_end,
                    event_time_text,
                    protected,
                    manual_protection_reason,
                    metadata_json,
                    now,
                    now,
                    now,
                    generation["activity_round"],
                    title,
                    emotion_json,
                ),
            )
            await connection.execute(
                """
                INSERT INTO memory_node_versions(
                    node_version_id, node_id, version_no, content_text,
                    metadata_json, change_kind, editor_kind, created_at, title, emotion_json
                ) VALUES (?, ?, 1, ?, ?, 'created', ?, ?, ?, ?)
                """,
                (version_id, node_id, content_text, metadata_json, editor_kind, now, title, emotion_json),
            )
            await connection.executemany(
                """
                INSERT INTO node_sources(node_id, source_id, source_kind, created_at)
                VALUES (?, ?, ?, ?)
                """,
                [(node_id, source_id, source_kind, now) for source_id in source_ids],
            )
            await record_derivation(
                connection, "node", node_id, 1, operation="create", editor_kind=editor_kind,
                now=now, sources=[(source_id, source_kind) for source_id in source_ids],
                source_observations=observations,
            )
            if participant_values:
                await connection.executemany(
                    """
                    INSERT INTO node_participants(
                        node_id, participant_kind, participant_id, display_name
                    ) VALUES (?, ?, ?, ?)
                    """,
                    [
                        (node_id, kind, participant_id, display_name)
                        for kind, participant_id, display_name in participant_values
                    ],
                )
        row = await self._node_row(node_id)
        return self._node_from_row(row)

    @staticmethod
    def _normalize_participants(
        participants: Iterable[Mapping[str, str]],
    ) -> list[tuple[str, str, str | None]]:
        result = []
        for participant in participants:
            kind = str(participant.get("participant_kind", "")).strip().lower()
            participant_id = str(participant.get("participant_id", "")).strip()
            if kind not in PARTICIPANT_KINDS:
                raise ValueError(f"unsupported participant kind: {kind}")
            if not participant_id:
                raise ValueError("participant_id is required")
            result.append((kind, participant_id, participant.get("display_name")))
        return result

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        row = await self._node_row(node_id)
        return self._node_from_row(row) if row else None

    async def list_nodes(
        self,
        *,
        space_id: str,
        generation_id: str | None = None,
        node_type: str | None = None,
        statuses: Iterable[str] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        if node_type is not None:
            node_type = validate_node_type(node_type)
        if generation_id is None:
            active = await self.get_active_generation(space_id)
            if active is None:
                return []
            generation_id = active["generation_id"]
        if statuses is None:
            statuses = ("active",)
        statuses = tuple(validate_node_status(item) for item in statuses)
        limit = max(1, min(int(limit), 500))
        offset = max(0, int(offset))
        clauses = ["n.space_id = ?"]
        params: list[Any] = [space_id]
        clauses.append("n.generation_id = ?")
        params.append(generation_id)
        if node_type is not None:
            clauses.append("n.node_type = ?")
            params.append(node_type)
        if statuses:
            clauses.append(f"n.status IN ({','.join('?' for _ in statuses)})")
            params.extend(statuses)
        params.extend((limit, offset))
        rows = await self.database.fetchall(
            f"""
            SELECT n.*, n.metadata_json AS node_metadata_json,
                   v.node_version_id, v.version_no AS loaded_version_no,
                   v.content_text, v.metadata_json AS content_metadata_json
            FROM memory_nodes n
            JOIN memory_node_versions v
              ON v.node_id = n.node_id AND v.version_no = n.content_version
            WHERE {' AND '.join(clauses)}
            ORDER BY n.updated_at DESC, n.node_id
            LIMIT ? OFFSET ?
            """,
            params,
        )
        return [self._node_from_row(row) for row in rows]

    async def update_node_content(
        self,
        node_id: str,
        content_text: str,
        *,
        title: str | None = None,
        emotion: Mapping[str, Any] | None | object = EMOTION_UNSET,
        editor_kind: str = "manual",
        change_kind: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        importance: float | None = None,
        event_time_start: float | None = None,
        event_time_end: float | None = None,
        event_time_text: str | None = None,
    ) -> dict[str, Any]:
        content_text = validate_content(content_text)
        title = validate_title(title) if title is not None else None
        if editor_kind not in {"system", "model", "manual", "rebuild"}:
            raise ValueError(f"unsupported editor kind: {editor_kind}")
        if change_kind is None:
            change_kind = "manual_update" if editor_kind == "manual" else "model_update"
        if change_kind not in {"model_update", "manual_update", "rebuild"}:
            raise ValueError(f"unsupported change kind: {change_kind}")
        now = _now()
        async with self.database.transaction() as connection:
            cursor = await connection.execute(
                "SELECT * FROM memory_nodes WHERE node_id = ?", (node_id,)
            )
            node = await cursor.fetchone()
            if node is None:
                raise KeyError(f"unknown memory node: {node_id}")
            if node["manual_protected"] and editor_kind != "manual":
                raise PermissionError("manual-protected node requires manual editor")
            final_title = node["title"] if title is None else title
            title_changed = final_title != node["title"]
            emotion_json = node["emotion_json"] if emotion is EMOTION_UNSET else encode_emotion(emotion)
            old_version = int(node["content_version"])
            old_content_cursor = await connection.execute(
                """
                SELECT content_text, metadata_json FROM memory_node_versions
                WHERE node_id = ? AND version_no = ?
                """,
                (node_id, old_version),
            )
            old_content = await old_content_cursor.fetchone()
            content_changed = (not old_content or old_content["content_text"] != content_text
                               or emotion_json != node["emotion_json"])
            title_only = (
                not content_changed
                and metadata is None
                and importance is None
                and event_time_start is None
                and event_time_end is None
                and event_time_text is None
            )
            if title_only and not title_changed:
                row = await self._node_row_in_connection(connection, node_id)
                return self._node_from_row(row)
            new_version = old_version + 1 if content_changed else old_version
            version_metadata_json = (
                old_content["metadata_json"]
                if metadata is None and old_content
                else _json(metadata, "{}")
            )
            node_metadata_json = (
                version_metadata_json if metadata is not None else node["metadata_json"]
            )
            new_importance = (
                float(node["importance"])
                if importance is None
                else _score(importance, "importance")
            )
            reset_strength = content_changed or new_importance != node["importance"]
            round_cursor = await connection.execute(
                "SELECT activity_round FROM memory_generations WHERE generation_id = ?",
                (node["generation_id"],),
            )
            activity_round = (await round_cursor.fetchone())["activity_round"]
            final_event_time_start = (
                event_time_start
                if event_time_start is not None
                else node["event_time_start"]
            )
            final_event_time_end = (
                event_time_end if event_time_end is not None else node["event_time_end"]
            )
            if (
                final_event_time_start is not None
                and final_event_time_end is not None
                and float(final_event_time_end) < float(final_event_time_start)
            ):
                raise ValueError("event_time_end cannot precede event_time_start")
            await connection.execute(
                """
                UPDATE memory_nodes
                SET content_version = ?, importance = ?, strength = ?,
                    event_time_start = COALESCE(?, event_time_start),
                    event_time_end = COALESCE(?, event_time_end),
                    event_time_text = COALESCE(?, event_time_text),
                    metadata_json = ?, updated_at = ?, title = ?,
                    strength_reset_at = ?, strength_reset_round = ?, reinforcement = ?, emotion_json = ?
                WHERE node_id = ?
                """,
                (
                    new_version,
                    new_importance,
                    new_importance if reset_strength else node["strength"],
                    event_time_start,
                    event_time_end,
                    event_time_text,
                    node_metadata_json,
                    node["updated_at"] if title_only else now,
                    final_title,
                    now if reset_strength else node["strength_reset_at"],
                    activity_round if reset_strength else node["strength_reset_round"],
                    0 if reset_strength else node["reinforcement"],
                    emotion_json,
                    node_id,
                ),
            )
            if content_changed:
                await connection.execute(
                    """
                    INSERT INTO memory_node_versions(
                        node_version_id, node_id, version_no, content_text,
                        metadata_json, change_kind, editor_kind, created_at, title, emotion_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _id("nodever"),
                        node_id,
                        new_version,
                        content_text,
                        version_metadata_json,
                        change_kind,
                        editor_kind,
                        now,
                        final_title,
                        emotion_json,
                    ),
                )
                await record_derivation(
                    connection, "node", node_id, new_version, operation="update",
                    editor_kind=editor_kind, now=now,
                    base_versions=[version_reference("node", node_id, old_version)],
                )
            elif title_changed:
                # 操作留痕与正文快照分开，禁止把新标题倒填到历史版本。
                await record_derivation(
                    connection, "node", node_id, old_version, operation="title_update",
                    editor_kind=editor_kind, now=now,
                    semantic={"title_before": node["title"], "title_after": final_title},
                )
            row = await self._node_row_in_connection(connection, node_id)
        return self._node_from_row(row)

    async def _node_row_in_connection(self, connection: Any, node_id: str) -> Any:
        cursor = await connection.execute(
            """
            SELECT n.*, n.metadata_json AS node_metadata_json,
                   v.node_version_id, v.version_no AS loaded_version_no,
                   v.content_text, v.metadata_json AS content_metadata_json
            FROM memory_nodes n
            JOIN memory_node_versions v
              ON v.node_id = n.node_id AND v.version_no = n.content_version
            WHERE n.node_id = ?
            """,
            (node_id,),
        )
        return await cursor.fetchone()

    async def update_node_status(self, node_id: str, status: str) -> dict[str, Any]:
        status = validate_node_status(status)
        now = _now()
        async with self.database.transaction() as connection:
            cursor = await connection.execute(
                "SELECT node_id FROM memory_nodes WHERE node_id = ?", (node_id,)
            )
            if await cursor.fetchone() is None:
                raise KeyError(f"unknown memory node: {node_id}")
            await connection.execute(
                """
                UPDATE memory_nodes
                SET status = ?,
                    deleted_at = CASE WHEN ? = 'deleted' THEN ? ELSE NULL END,
                    updated_at = ?
                WHERE node_id = ?
                """,
                (status, status, now, now, node_id),
            )
            row = await self._node_row_in_connection(connection, node_id)
        return self._node_from_row(row)

    async def set_manual_protection(
        self,
        node_id: str,
        protected: bool,
        *,
        reason: str | None = None,
    ) -> dict[str, Any]:
        """Set the independent human-protection flag for a memory node.

        Protection prevents automatic content replacement while leaving the lifecycle
        status (active, historical, disabled, or deleted) independent.
        """

        if not isinstance(protected, bool):
            raise ValueError("protected must be a boolean")
        if reason is not None:
            reason = str(reason).strip() or None
            if reason and len(reason) > 2000:
                raise ValueError("reason exceeds 2000 characters")
        async with self.database.transaction() as connection:
            cursor = await connection.execute(
                "SELECT node_id FROM memory_nodes WHERE node_id = ?", (node_id,)
            )
            if await cursor.fetchone() is None:
                raise KeyError(f"unknown memory node: {node_id}")
            await connection.execute(
                """
                UPDATE memory_nodes
                SET manual_protected = ?, manual_protection_reason = ?, updated_at = ?
                WHERE node_id = ?
                """,
                (1 if protected else 0, reason if protected else None, _now(), node_id),
            )
            row = await self._node_row_in_connection(connection, node_id)
        return self._node_from_row(row)

    async def list_node_versions(self, node_id: str) -> list[dict[str, Any]]:
        """Return all persisted content versions in ascending version order."""

        rows = await self.database.fetchall(
            """
            SELECT node_version_id, node_id, version_no, content_text,
                   metadata_json, change_kind, editor_kind, created_at, title, emotion_json,
                   content_updated_at
            FROM memory_node_versions
            WHERE node_id = ?
            ORDER BY version_no ASC
            """,
            (node_id,),
        )
        if not rows:
            node = await self.get_node(node_id)
            if node is None:
                raise KeyError(f"unknown memory node: {node_id}")
        result = []
        for row in rows:
            item = dict(row)
            item["metadata"] = _dict_json(item.pop("metadata_json", None), {})
            item["emotion"] = decode_emotion(item.pop("emotion_json", None))
            result.append(item)
        return await self._attach_derivations("node", node_id, result)

    async def add_node_source(
        self, node_id: str, source_id: str, *, source_kind: str = "source_record"
    ) -> bool:
        source_id = str(source_id).strip()
        if not source_id:
            raise ValueError("source_id is required")
        if source_kind not in {"source_record", "manual", "external"}:
            raise ValueError(f"unsupported source kind: {source_kind}")
        if not await self._source_is_known(source_id):
            raise ValueError(f"unknown source_id: {source_id}")
        async with self.database.transaction() as connection:
            node = await connection.execute(
                "SELECT node_id FROM memory_nodes WHERE node_id = ?", (node_id,)
            )
            if await node.fetchone() is None:
                raise KeyError(f"unknown memory node: {node_id}")
            cursor = await connection.execute(
                """
                INSERT OR IGNORE INTO node_sources(node_id, source_id, source_kind, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (node_id, source_id, source_kind, _now()),
            )
            return cursor.rowcount > 0

    async def remove_node_source(self, node_id: str, source_id: str) -> bool:
        cursor = await self.database.execute(
            "DELETE FROM node_sources WHERE node_id = ? AND source_id = ?",
            (node_id, source_id),
        )
        return cursor.rowcount > 0

    async def list_node_sources(self, node_id: str) -> list[dict[str, Any]]:
        rows = await self.database.fetchall(
            """
            SELECT node_id, source_id, source_kind, created_at
            FROM node_sources WHERE node_id = ? ORDER BY created_at, source_id
            """,
            (node_id,),
        )
        return [dict(row) for row in rows]

    async def add_node_participant(
        self,
        node_id: str,
        participant_kind: str,
        participant_id: str,
        *,
        display_name: str | None = None,
    ) -> bool:
        values = self._normalize_participants(
            ({"participant_kind": participant_kind, "participant_id": participant_id, "display_name": display_name},)
        )
        cursor = await self.database.execute(
            """
            INSERT OR IGNORE INTO node_participants(
                node_id, participant_kind, participant_id, display_name
            ) VALUES (?, ?, ?, ?)
            """,
            (node_id, *values[0]),
        )
        return cursor.rowcount > 0

    async def list_node_participants(self, node_id: str) -> list[dict[str, Any]]:
        rows = await self.database.fetchall(
            """
            SELECT node_id, participant_kind, participant_id, display_name
            FROM node_participants WHERE node_id = ?
            ORDER BY participant_kind, participant_id
            """,
            (node_id,),
        )
        return [dict(row) for row in rows]

    async def _nodes_same_scope(self, connection: Any, first: str, second: str) -> tuple[Any, Any]:
        cursor = await connection.execute(
            """
            SELECT node_id, space_id, generation_id, node_type, content_version
            FROM memory_nodes WHERE node_id IN (?, ?)
            """,
            (first, second),
        )
        rows = {row["node_id"]: row for row in await cursor.fetchall()}
        if first not in rows or second not in rows:
            raise KeyError("unknown node in relationship")
        left, right = rows[first], rows[second]
        if left["space_id"] != right["space_id"] or left["generation_id"] != right["generation_id"]:
            raise ValueError("relationship endpoints must share space and generation")
        return left, right

    async def add_edge(
        self,
        source_node_id: str,
        target_node_id: str,
        relation_type: str,
        *,
        metadata: Mapping[str, Any] | None = None,
        edge_id: str | None = None,
    ) -> dict[str, Any]:
        relation_type = validate_edge_type(relation_type)
        if source_node_id == target_node_id:
            raise ValueError("a node cannot relate to itself")
        now = _now()
        async with self.database.transaction() as connection:
            left, _ = await self._nodes_same_scope(connection, source_node_id, target_node_id)
            await connection.execute(
                """
                INSERT OR IGNORE INTO memory_edges(
                    edge_id, generation_id, space_id, source_node_id, target_node_id,
                    relation_type, metadata_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    edge_id or _id("edge"),
                    left["generation_id"],
                    left["space_id"],
                    source_node_id,
                    target_node_id,
                    relation_type,
                    _json(metadata, "{}"),
                    now,
                ),
            )
            cursor = await connection.execute(
                """
                SELECT * FROM memory_edges
                WHERE source_node_id = ? AND target_node_id = ? AND relation_type = ?
                """,
                (source_node_id, target_node_id, relation_type),
            )
            item = dict(await cursor.fetchone())
            item["metadata"] = _dict_json(item.pop("metadata_json"), {})
            return item

    async def remove_edge(self, source_node_id: str, target_node_id: str, relation_type: str) -> bool:
        relation_type = validate_edge_type(relation_type)
        cursor = await self.database.execute(
            """
            DELETE FROM memory_edges
            WHERE source_node_id = ? AND target_node_id = ? AND relation_type = ?
            """,
            (source_node_id, target_node_id, relation_type),
        )
        return cursor.rowcount > 0

    async def list_edges(self, node_id: str | None = None) -> list[dict[str, Any]]:
        if node_id:
            rows = await self.database.fetchall(
                """
                SELECT * FROM memory_edges
                WHERE source_node_id = ? OR target_node_id = ?
                ORDER BY created_at, edge_id
                """,
                (node_id, node_id),
            )
        else:
            rows = await self.database.fetchall(
                "SELECT * FROM memory_edges ORDER BY created_at, edge_id"
            )
        result = []
        for row in rows:
            item = dict(row)
            item["metadata"] = _dict_json(item.pop("metadata_json", None), {})
            result.append(item)
        return result

    async def add_coverage(
        self,
        event_node_id: str,
        atomic_node_id: str,
        *,
        atomic_content_version: int | None = None,
        event_content_version: int | None = None,
        coverage_kind: str = "full",
    ) -> bool:
        if coverage_kind not in {"full", "partial"}:
            raise ValueError(f"unsupported coverage kind: {coverage_kind}")
        async with self.database.transaction() as connection:
            event, atomic = await self._nodes_same_scope(
                connection, event_node_id, atomic_node_id
            )
            if event["node_type"] != "event" or atomic["node_type"] != "atomic":
                raise ValueError("coverage must connect an event to an atomic node")
            atomic_content_version = (
                atomic["content_version"]
                if atomic_content_version is None else atomic_content_version
            )
            event_content_version = (
                event["content_version"]
                if event_content_version is None else event_content_version
            )
            for node, version, kind in (
                (event, event_content_version, "event"),
                (atomic, atomic_content_version, "atomic"),
            ):
                if type(version) is not int or version < 1:
                    raise ValueError(f"{kind} content version is not available")
                cursor = await connection.execute(
                    "SELECT 1 FROM memory_node_versions WHERE node_id=? AND version_no=?",
                    (node["node_id"], version),
                )
                if await cursor.fetchone() is None:
                    raise ValueError(f"{kind} content version is not available")
            cursor = await connection.execute(
                """
                INSERT INTO node_coverage(
                    event_node_id, atomic_node_id, atomic_content_version,
                    coverage_kind, created_at, event_content_version
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_node_id, atomic_node_id) DO UPDATE SET
                    atomic_content_version=excluded.atomic_content_version,
                    event_content_version=excluded.event_content_version,
                    coverage_kind=excluded.coverage_kind,
                    created_at=excluded.created_at
                WHERE node_coverage.atomic_content_version != excluded.atomic_content_version
                   OR node_coverage.event_content_version != excluded.event_content_version
                   OR node_coverage.coverage_kind != excluded.coverage_kind
                """,
                (
                    event_node_id,
                    atomic_node_id,
                    int(atomic_content_version),
                    coverage_kind,
                    _now(),
                    event_content_version,
                ),
            )
            return cursor.rowcount > 0

    async def upsert_profile(
        self,
        *,
        space_id: str,
        logical_user_id: str,
        tags: Iterable[str],
        description: str = "",
        status: str = "active",
        generation_id: str | None = None,
    ) -> dict[str, Any]:
        tags_list = validate_profile_tags(list(tags))
        description = validate_profile_description(description)
        if status not in {"active", "disabled", "historical"}:
            raise ValueError(f"unsupported profile status: {status}")
        now = _now()
        async with self.database.transaction() as connection:
            space = await self._space_in_transaction(connection, space_id)
            logical_user_id = str(logical_user_id).strip()
            if not logical_user_id:
                raise ValueError("logical_user_id is required")
            if (
                space["space_type"] == "private"
                and space["logical_user_id"] != logical_user_id
            ):
                raise ValueError("logical_user_id does not belong to this private space")
            generation = await self._generation_in_transaction(connection, space_id, generation_id)
            cursor = await connection.execute(
                """
                SELECT * FROM user_profiles
                WHERE generation_id = ? AND space_id = ? AND logical_user_id = ?
                """,
                (generation["generation_id"], space_id, logical_user_id),
            )
            existing = await cursor.fetchone()
            if existing:
                changed = (
                    _dict_json(existing["tags_json"], []) != tags_list
                    or existing["description"] != description
                    or existing["status"] != status
                )
                version = int(existing["content_version"]) + (1 if changed else 0)
                await connection.execute(
                    """
                    UPDATE user_profiles
                    SET tags_json = ?, description = ?, status = ?,
                        content_version = ?, updated_at = ?
                    WHERE profile_id = ?
                    """,
                    (
                        _json(tags_list, "[]"),
                        description,
                        status,
                        version,
                        now,
                        existing["profile_id"],
                    ),
                )
                profile_id = existing["profile_id"]
            else:
                profile_id = _id("profile")
                await connection.execute(
                    """
                    INSERT INTO user_profiles(
                        profile_id, generation_id, space_id, logical_user_id,
                        tags_json, description, status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        profile_id,
                        generation["generation_id"],
                        space_id,
                        logical_user_id,
                        _json(tags_list, "[]"),
                        description,
                        status,
                        now,
                        now,
                    ),
                )
            cursor = await connection.execute(
                "SELECT * FROM user_profiles WHERE profile_id = ?", (profile_id,)
            )
            item = dict(await cursor.fetchone())
            await snapshot_context_version(connection, "profile", profile_id)
            if existing is None or changed:
                await record_derivation(
                    connection, "profile", profile_id, item["content_version"],
                    operation="update" if existing else "create", editor_kind="system", now=now,
                    base_versions=[version_reference("profile", profile_id, existing["content_version"])] if existing else [],
                )
        item["tags"] = _dict_json(item.pop("tags_json"), [])
        return item

    async def list_profiles(
        self, *, space_id: str, generation_id: str | None = None, logical_user_id: str | None = None
    ) -> list[dict[str, Any]]:
        if generation_id is None:
            active = await self.get_active_generation(space_id)
            if active is None:
                return []
            generation_id = active["generation_id"]
        clauses = ["space_id = ?", "generation_id = ?"]
        params: list[Any] = [space_id]
        params.append(generation_id)
        if logical_user_id:
            clauses.append("logical_user_id = ?")
            params.append(logical_user_id)
        rows = await self.database.fetchall(
            f"SELECT * FROM user_profiles WHERE {' AND '.join(clauses)} ORDER BY updated_at DESC",
            params,
        )
        result = []
        for row in rows:
            item = dict(row)
            item["tags"] = _dict_json(item.pop("tags_json"), [])
            result.append(item)
        return result

    async def upsert_relationship(
        self,
        *,
        space_id: str,
        logical_user_id: str,
        state_text: str | None = None,
        state: Mapping[str, Any] | None = None,
        status: str = "active",
        generation_id: str | None = None,
    ) -> dict[str, Any]:
        state_text = validate_relationship_text(state_text) if state_text is not None else None
        if status not in {"active", "historical", "disabled"}:
            raise ValueError(f"unsupported relationship status: {status}")
        now = _now()
        async with self.database.transaction() as connection:
            space = await self._space_in_transaction(connection, space_id)
            if space["space_type"] != "private":
                raise ValueError("persona relationship is only valid for private spaces")
            logical_user_id = str(logical_user_id).strip()
            if space["logical_user_id"] != logical_user_id:
                raise ValueError("logical_user_id does not belong to this private space")
            generation = await self._generation_in_transaction(connection, space_id, generation_id)
            cursor = await connection.execute(
                """
                SELECT * FROM relationship_states
                WHERE generation_id = ? AND space_id = ? AND logical_user_id = ?
                """,
                (generation["generation_id"], space_id, logical_user_id),
            )
            existing = await cursor.fetchone()
            if state is None:
                state = relationship_slots(state=json.loads(existing["state_json"])) if existing else relationship_slots()
                if state_text is not None and (not existing or state_text != existing["state_text"]):
                    state["long_term"] = {"body": state_text, "observed_at": None}
            state = validate_relationship_slots(state)
            state_text = relationship_text(state)
            state_json = _json(state, "{}")
            if existing:
                changed = (
                    existing["state_text"] != state_text
                    or existing["state_json"] != state_json
                    or existing["status"] != status
                )
                version = int(existing["content_version"]) + (1 if changed else 0)
                await connection.execute(
                    """
                    UPDATE relationship_states
                    SET state_text = ?, state_json = ?, status = ?,
                        content_version = ?, updated_at = ?
                    WHERE relationship_id = ?
                    """,
                    (state_text, state_json, status, version, now, existing["relationship_id"]),
                )
                relationship_id = existing["relationship_id"]
            else:
                relationship_id = _id("relationship")
                await connection.execute(
                    """
                    INSERT INTO relationship_states(
                        relationship_id, generation_id, space_id, logical_user_id,
                        state_text, state_json, status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        relationship_id,
                        generation["generation_id"],
                        space_id,
                        logical_user_id,
                        state_text,
                        state_json,
                        status,
                        now,
                        now,
                    ),
                )
            cursor = await connection.execute(
                "SELECT * FROM relationship_states WHERE relationship_id = ?",
                (relationship_id,),
            )
            item = dict(await cursor.fetchone())
            await snapshot_context_version(connection, "relationship", relationship_id)
            if existing is None or changed:
                await record_derivation(
                    connection, "relationship", relationship_id, item["content_version"],
                    operation="update" if existing else "create", editor_kind="system", now=now,
                    base_versions=[version_reference("relationship", relationship_id, existing["content_version"])] if existing else [],
                )
        item["state"] = _dict_json(item.pop("state_json"), {})
        return item

    async def get_relationship(
        self, *, space_id: str, logical_user_id: str, generation_id: str | None = None
    ) -> dict[str, Any] | None:
        generation = await self.get_active_generation(space_id) if generation_id is None else {"generation_id": generation_id}
        if generation is None:
            return None
        row = await self.database.fetchone(
            """
            SELECT * FROM relationship_states
            WHERE space_id = ? AND generation_id = ? AND logical_user_id = ?
            """,
            (space_id, generation["generation_id"], logical_user_id),
        )
        if row is None:
            return None
        item = dict(row)
        item["state"] = _dict_json(item.pop("state_json"), {})
        return item

    async def upsert_short_term(
        self,
        *,
        space_id: str,
        conversation_id: str,
        summary_text: str,
        covered_from_source_id: str | None = None,
        covered_to_source_id: str | None = None,
        coverage: Mapping[str, Any] | None = None,
        status: str = "active",
        generation_id: str | None = None,
    ) -> dict[str, Any]:
        if status not in {"active", "archived", "disabled"}:
            raise ValueError(f"unsupported short-term status: {status}")
        now = _now()
        async with self.database.transaction() as connection:
            await self._space_in_transaction(connection, space_id)
            generation = await self._generation_in_transaction(connection, space_id, generation_id)
            cursor = await connection.execute(
                """
                SELECT * FROM short_term_states
                WHERE generation_id = ? AND space_id = ? AND conversation_id = ?
                """,
                (generation["generation_id"], space_id, conversation_id),
            )
            existing = await cursor.fetchone()
            coverage_json = _json(coverage, "{}")
            if existing:
                changed = (
                    existing["summary_text"] != summary_text
                    or existing["coverage_json"] != coverage_json
                    or existing["status"] != status
                )
                version = int(existing["content_version"]) + (1 if changed else 0)
                await connection.execute(
                    """
                    UPDATE short_term_states
                    SET summary_text = ?, covered_from_source_id = ?,
                        covered_to_source_id = ?, coverage_json = ?, status = ?,
                        content_version = ?, updated_at = ?
                    WHERE state_id = ?
                    """,
                    (
                        summary_text,
                        covered_from_source_id,
                        covered_to_source_id,
                        coverage_json,
                        status,
                        version,
                        now,
                        existing["state_id"],
                    ),
                )
                state_id = existing["state_id"]
            else:
                state_id = _id("short")
                await connection.execute(
                    """
                    INSERT INTO short_term_states(
                        state_id, generation_id, space_id, conversation_id,
                        summary_text, covered_from_source_id, covered_to_source_id,
                        coverage_json, status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        state_id,
                        generation["generation_id"],
                        space_id,
                        conversation_id,
                        summary_text,
                        covered_from_source_id,
                        covered_to_source_id,
                        coverage_json,
                        status,
                        now,
                        now,
                    ),
                )
            cursor = await connection.execute(
                "SELECT * FROM short_term_states WHERE state_id = ?", (state_id,)
            )
            item = dict(await cursor.fetchone())
        item["coverage"] = _dict_json(item.pop("coverage_json"), {})
        return item

    async def get_short_term(
        self, *, space_id: str, conversation_id: str, generation_id: str | None = None
    ) -> dict[str, Any] | None:
        generation = await self.get_active_generation(space_id) if generation_id is None else {"generation_id": generation_id}
        if generation is None:
            return None
        row = await self.database.fetchone(
            """
            SELECT * FROM short_term_states
            WHERE space_id = ? AND generation_id = ? AND conversation_id = ?
            """,
            (space_id, generation["generation_id"], conversation_id),
        )
        if row is None:
            return None
        item = dict(row)
        item["coverage"] = _dict_json(item.pop("coverage_json"), {})
        return item

    async def create_note(
        self,
        *,
        space_id: str,
        content_text: str,
        logical_user_id: str | None = None,
        source_kind: str = "agent",
        source_ref: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        note_id: str | None = None,
    ) -> dict[str, Any]:
        content_text = validate_note_content(content_text)
        if source_kind not in {"agent", "manual", "external"}:
            raise ValueError(f"unsupported note source kind: {source_kind}")
        now = _now()
        async with self.database.transaction() as connection:
            space = await self._space_in_transaction(connection, space_id)
            note_id = note_id or _id("note")
            await assert_scope_available(connection, space_id)
            await connection.execute(
                """
                INSERT INTO notes(
                    note_id, space_id, persona_id, logical_user_id, content_text,
                    source_kind, source_ref, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    note_id,
                    space_id,
                    space["persona_id"],
                    logical_user_id,
                    content_text,
                    source_kind,
                    source_ref,
                    _json(metadata, "{}"),
                    now,
                    now,
                ),
            )
            cursor = await connection.execute("SELECT * FROM notes WHERE note_id = ?", (note_id,))
            item = dict(await cursor.fetchone())
        item["metadata"] = _dict_json(item.pop("metadata_json"), {})
        return item

    async def update_note(
        self,
        note_id: str,
        *,
        content_text: str | None = None,
        space_id: str | None = None,
        status: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if content_text is not None:
            content_text = validate_note_content(content_text)
        if status is not None:
            status = validate_note_status(status)
        now = _now()
        async with self.database.transaction() as connection:
            cursor = await connection.execute("SELECT * FROM notes WHERE note_id = ?", (note_id,))
            existing = await cursor.fetchone()
            if existing is None or existing["status"] == "deleted" or (space_id is not None and existing["space_id"] != space_id):
                raise KeyError("note unavailable in the requested space")
            await assert_scope_available(connection, existing["space_id"])
            now = max(now, math.nextafter(existing["updated_at"], math.inf))
            await connection.execute(
                """
                UPDATE notes
                SET content_text = COALESCE(?, content_text),
                    status = COALESCE(?, status),
                    metadata_json = COALESCE(?, metadata_json),
                    deleted_at = CASE WHEN COALESCE(?, status) = 'deleted' THEN ? ELSE NULL END,
                    updated_at = ?
                WHERE note_id = ?
                """,
                (
                    content_text,
                    status,
                    _json(metadata, "{}") if metadata is not None else None,
                    status,
                    now,
                    now,
                    note_id,
                ),
            )
            cursor = await connection.execute("SELECT * FROM notes WHERE note_id = ?", (note_id,))
            item = dict(await cursor.fetchone())
        item["metadata"] = _dict_json(item.pop("metadata_json"), {})
        return item

    async def list_notes(
        self,
        *,
        space_id: str,
        statuses: Iterable[str] | None = None,
        limit: int = 100,
        query: str = "",
    ) -> list[dict[str, Any]]:
        statuses = tuple(validate_note_status(item) for item in (statuses or ("active",)))
        if not statuses:
            return []
        limit = max(1, min(int(limit), 500))
        placeholders = ",".join("?" for _ in statuses)
        rows = await self.database.fetchall(
            f"""
            SELECT * FROM notes WHERE space_id = ? AND status IN ({placeholders})
                AND (? = '' OR instr(lower(content_text), lower(?)) > 0)
            ORDER BY updated_at DESC LIMIT ?
            """,
            (space_id, *statuses, query, query, limit),
        )
        result = []
        for row in rows:
            item = dict(row)
            item["metadata"] = _dict_json(item.pop("metadata_json"), {})
            result.append(item)
        return result

    async def pending_image_evidence(self, batch_id: str | None) -> list[dict]:
        sql = "SELECT batch_id, evidence_json FROM pending_image_evidence"
        if batch_id is None:
            rows = await self.database.fetchall(sql + " ORDER BY rowid")
        else:
            rows = await self.database.fetchall(sql + " WHERE batch_id=?", (batch_id,))
        return [{"batch_id": row["batch_id"], "evidence": json.loads(row["evidence_json"])} for row in rows]

    async def acknowledge_image_evidence(self, batch_id: str) -> None:
        await self.database.execute("DELETE FROM pending_image_evidence WHERE batch_id=?", (batch_id,))

    async def get_extraction_batch(self, batch_id: str) -> dict[str, Any] | None:
        row = await self.database.fetchone(
            "SELECT * FROM extraction_batches WHERE batch_id = ?", (batch_id,)
        )
        if row is None:
            return None
        result = dict(row)
        result["result"] = json.loads(result.pop("result_json"))
        result["replayed"] = False
        return result

    async def apply_extraction(self, batch: Any, output: Any, *, model_id: str,
                               prompt_version: str, image_evidence=()) -> dict[str, Any]:
        # Kept separate from general CRUD to make the one-batch transaction explicit.
        from .extraction_store import apply_extraction

        if self.source_exists is None:
            raise RuntimeError("extraction requires an authoritative source checker")
        for source in batch.sources:
            if not await self._source_is_known(source["source_id"]):
                raise ValueError("unknown source_id in extraction range")
        return await apply_extraction(self, batch, output, model_id=model_id,
                                      prompt_version=prompt_version, image_evidence=image_evidence)
