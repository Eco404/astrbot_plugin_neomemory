"""Authoritative source records and logical-user identities."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from ..core.extraction_errors import ExtractionWindowChanged
    from ..core.proactive_delivery import merge_deliveries, part_texts
    from ..core.group_addressing import address_entries
    from ..core.text_cleaning import DEFAULT_CLEANER, RECEIPT_PROJECTION_FIELDS, receipt_text_matches
    from ..core.image_capture import project_images, without_image_data
    from ..core.source_ingest import CapturedSource
except ImportError:  # Tests import ``storage`` directly from the plugin root.
    from core.extraction_errors import ExtractionWindowChanged
    from core.proactive_delivery import merge_deliveries, part_texts
    from core.group_addressing import address_entries
    from core.text_cleaning import DEFAULT_CLEANER, RECEIPT_PROJECTION_FIELDS, receipt_text_matches
    from core.image_capture import project_images, without_image_data
    from core.source_ingest import CapturedSource

from .source_schema import SOURCE_FORMAT_REVISION, initialize_source_schema
from .schema import CURRENT_SCHEMA_VERSION
from .social_accounts import AmbiguousAccountOwner, account_routes, delivery_owner, group_accounts, register_platform, resolve_owner
from .sqlite_executor import SQLiteExecutor


@dataclass(frozen=True, slots=True)
class CaptureResult:
    source_id: str
    stream_id: str
    sequence_no: int
    inserted: bool
    logical_user_id: str | None


class SourceStore:
    """Source database facade with idempotent capture semantics."""

    def __init__(self, path: str | Path) -> None:
        self.database = SQLiteExecutor(path)
        self.path = Path(path)
        self.cleaner = DEFAULT_CLEANER
        self.schema_version = 0

    async def open(self) -> None:
        await self.database.open()
        try:
            self.schema_version = await initialize_source_schema(self.database)
        except BaseException:
            await self.database.close()
            raise

    async def close(self) -> None:
        await self.database.close()

    async def health(self) -> dict[str, Any]:
        row = await self.database.fetchone(
            "SELECT COUNT(*) AS count FROM source_records"
        )
        stream_row = await self.database.fetchone(
            "SELECT COUNT(*) AS count,(SELECT MAX(updated_at) FROM source_cursors) AS last_write "
            "FROM conversation_streams"
        )
        return {
            "path": str(self.path),
            "exists": self.path.exists(),
            "schema_version": self.schema_version,
            "expected_schema_version": CURRENT_SCHEMA_VERSION,
            "schema_revision": SOURCE_FORMAT_REVISION,
            "source_count": int(row["count"] if row else 0),
            "stream_count": int(stream_row["count"] if stream_row else 0),
            "last_source_write_at": stream_row["last_write"],
        }

    @staticmethod
    async def _delivery_row(connection, source):
        row = await (await connection.execute(
            "SELECT * FROM source_records WHERE source_id=?", (source.source_id,))).fetchone()
        if row:
            meta = json.loads(row["metadata_json"])
            # Replayed deliveries can predate markup or embedded-image normalization.
            same_text = receipt_text_matches(row["content_text"], meta, source.content_text, source.metadata)
            if (row["stream_id"] != source.identity.stream_id or row["persona_id"] != source.persona_id
                    or row["observed_at"] != source.observed_at or not same_text
                    or row["actor_account_id"] != (source.actor_account_id or None)
                    or any(meta.get(key) != value for key, value in source.metadata.items() if key != "replay_only" and key not in RECEIPT_PROJECTION_FIELDS)):
                raise ValueError("delivery id conflicts with existing source")
        return row

    async def confirmed_delivery(self, segment, contact):
        async with self.database.transaction(write=False) as connection:
            # Source links created by the per-segment writer remain valid on replay.
            legacy = await self._delivery_row(connection, segment)
            if legacy:
                return legacy["source_id"]
            row = await (await connection.execute(
                "SELECT r.source_id,r.content_text,r.metadata_json FROM proactive_receipt_sources p "
                "JOIN source_records r ON r.source_id=p.source_id WHERE p.receipt_source_id=?",
                (segment.source_id,))).fetchone()
            if row is None:
                return None
            if row["source_id"] != contact.source_id:
                raise ValueError("delivery id conflicts with existing source")
            if merge_deliveries(row["content_text"], json.loads(row["metadata_json"]), contact) is not None:
                raise ValueError("delivery sequence conflicts with existing source")
            return row["source_id"]

    @staticmethod
    async def _index_delivery_parts(connection, source):
        for part in source.metadata["delivery_parts"]:
            result = await connection.execute(
                "INSERT INTO proactive_receipt_sources(receipt_source_id,source_id) VALUES (?,?) "
                "ON CONFLICT(receipt_source_id) DO UPDATE SET source_id=excluded.source_id "
                "WHERE source_id=excluded.source_id", (part["receipt_source_id"], source.source_id))
            if not result.rowcount:
                raise ValueError("delivery id conflicts with existing source")

    @staticmethod
    async def _group_addresses(connection, source):
        identity = source.identity
        entries = address_entries(source.content_text, source.metadata.get("group_addressing", ()))
        mentions = {}
        for entry in entries:
            account = entry.get("account_id")
            if entry.get("is_self") or entry["kind"] == "mention_all":
                continue
            if account and account == identity.self_id:
                entry["is_self"] = True
                continue
            if entry["kind"] == "reply":
                entry["logical_user_id"] = None
                if message_id := entry.get("message_id"):
                    # An agent response can carry its triggering inbound ID. Only the
                    # inbound user's receipt identifies the author of this quotation.
                    row = await (await connection.execute(
                        "SELECT actor_account_id,actor_logical_user_id FROM source_records "
                        "WHERE stream_id=? AND dedup_key=?",
                        (identity.stream_id, f"external:{identity.stream_id}:user:{message_id}"))).fetchone()
                    if row and (not account or account == row["actor_account_id"]):
                        entry["logical_user_id"] = row["actor_logical_user_id"]
                continue
            if not account:
                continue
            if account not in mentions:
                routes = await account_routes(connection, identity.platform_instance_id, account, identity.platform_name)
                try:
                    owner = await delivery_owner(connection, identity.platform_instance_id, account,
                                                 identity.platform_name, source.observed_at, routes=routes)
                except AmbiguousAccountOwner:
                    owner = None
                # A previously unseen account is only identifiable if a sender in
                # this window subsequently corroborates its default ownership.
                mentions[account] = {"logical_user_id" if routes else "window_owner": owner}
            entry.update(mentions[account])
        return entries

    async def capture(self, source: CapturedSource) -> CaptureResult:
        now = time.time()
        identity = source.identity
        authored = source.actor_kind == "agent" and source.source_type == "conversation"
        text = without_image_data(source.content_text)
        content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        components = source.content_json
        metadata = dict(source.metadata)
        if authored and text != source.content_text and not metadata.get("content_truncated") and "original_content_length" in metadata:
            metadata["original_content_length"] = len(text)
        if components and "base64" in components.casefold():
            parts = json.loads(components)
            if isinstance(parts, list):
                parts, slots = project_images(parts, private=identity.is_private)
                if slots:
                    metadata.setdefault("image_attachments", slots)
                components = json.dumps(parts, ensure_ascii=False, separators=(",", ":"))
            else:
                components = without_image_data(components)
        identity_pending = (
            source.identity_pending
            or source.persona_id is None
            or identity.platform_instance_id in {"", "unknown"}
            or identity.unified_msg_origin
            in {"", "unknown", "unknown:OtherMessage:unknown"}
        )
        async with self.database.transaction() as connection:
            if identity.is_group and identity.group_id and identity.group_name:
                await connection.execute(
                    "INSERT INTO group_display_names(platform_instance_id,group_id,display_name,observed_at) "
                    "VALUES (?,?,?,?) ON CONFLICT(platform_instance_id,group_id) DO UPDATE SET "
                    "display_name=excluded.display_name,observed_at=excluded.observed_at "
                    "WHERE excluded.observed_at >= group_display_names.observed_at",
                    (identity.platform_instance_id, identity.group_id, identity.group_name, source.observed_at),
                )
            delivered = metadata.get("delivered_proactive") is True
            if metadata.get("delivery_parts"):
                existing = await (await connection.execute(
                    "SELECT * FROM source_records WHERE source_id=?", (source.source_id,))).fetchone()
                if existing:
                    merged = merge_deliveries(existing["content_text"], json.loads(existing["metadata_json"]), source)
                    if merged:
                        await self._index_delivery_parts(connection, source)
                        await connection.execute(
                            "UPDATE source_records SET content_text=?,observed_at=?,source_hash=?,metadata_json=? "
                            "WHERE source_id=?", (merged.content_text, merged.observed_at,
                                hashlib.sha256(merged.content_text.encode()).hexdigest(),
                                json.dumps(merged.metadata, ensure_ascii=False), source.source_id))
                        await connection.execute(
                            "UPDATE conversation_streams SET last_observed_at=MAX(last_observed_at,?),updated_at=? "
                            "WHERE stream_id=?", (merged.observed_at, now, identity.stream_id))
            elif delivered:
                existing = await self._delivery_row(connection, source)
            else:
                existing = await (await connection.execute(
                    "SELECT source_id,stream_id,sequence_no,actor_logical_user_id FROM source_records "
                    "WHERE stream_id=? AND dedup_key=?", (identity.stream_id, source.dedup_key))).fetchone()
            if existing:
                return CaptureResult(
                    source_id=existing["source_id"],
                    stream_id=existing["stream_id"],
                    sequence_no=int(existing["sequence_no"]),
                    inserted=False,
                    logical_user_id=existing["actor_logical_user_id"],
                )

            logical_user_id = None
            recipient = identity.account_id if delivered and identity.is_private else ""
            account = source.actor_account_id if source.actor_kind == "user" else recipient
            binding_owner = None
            if account:
                await register_platform(connection, identity.platform_instance_id, identity.platform_name)
                if recipient:
                    owner = await delivery_owner(connection, identity.platform_instance_id, account,
                                                 identity.platform_name, source.observed_at)
                    metadata["recipient_logical_user_id"] = owner
                    if owner is None:
                        identity_pending = True
                    elif owner == await resolve_owner(connection, identity.platform_instance_id, account):
                        binding_owner = owner
                else:
                    logical_user_id = await resolve_owner(connection, identity.platform_instance_id, account)
                    binding_owner = logical_user_id
                    if logical_user_id is None:
                        identity_pending = True
            if binding_owner is not None:
                await connection.execute(
                    """
                    INSERT INTO logical_users(
                        logical_user_id, display_name, status, created_at, updated_at
                    ) VALUES (?, ?, 'active', ?, ?)
                    ON CONFLICT(logical_user_id) DO UPDATE SET
                        display_name = CASE WHEN logical_users.display_name_source='manual'
                            THEN logical_users.display_name ELSE COALESCE(
                                excluded.display_name, logical_users.display_name
                            ) END,
                        updated_at = excluded.updated_at
                    """,
                    (binding_owner, source.actor_account_name or None, now, now),
                )
                await connection.execute(
                    """
                    INSERT INTO account_bindings(
                        platform_instance_id, account_id, logical_user_id,
                        binding_status, source, created_at, updated_at
                    ) VALUES (?, ?, ?, 'active', 'automatic-single-account', ?, ?)
                    ON CONFLICT(platform_instance_id, account_id) DO UPDATE SET
                        updated_at = excluded.updated_at
                    """,
                    (
                        identity.platform_instance_id,
                        account,
                        binding_owner,
                        now,
                        now,
                    ),
                )

            if identity.is_group and source.actor_kind == "user":
                addressing = await self._group_addresses(connection, source)
                if addressing:
                    metadata["group_addressing"] = addressing

            if delivered and not metadata.get("replay_only"):
                later = await (await connection.execute(
                    "SELECT 1 FROM source_records WHERE stream_id=? AND persona_id=? AND observed_at>? LIMIT 1",
                    (identity.stream_id, source.persona_id, source.observed_at))).fetchone()
                metadata["replay_only"] = bool(later)

            stream_cursor = await connection.execute(
                "SELECT next_sequence,last_observed_at,persona_id FROM conversation_streams WHERE stream_id = ?",
                (identity.stream_id,),
            )
            stream = await stream_cursor.fetchone()
            if stream is None:
                sequence_no = 1
                await connection.execute(
                    """
                    INSERT INTO conversation_streams(
                        stream_id, platform_instance_id, platform_name, message_type,
                        conversation_id, group_id, persona_id, next_sequence,
                        last_observed_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        identity.stream_id,
                        identity.platform_instance_id,
                        identity.platform_name,
                        identity.message_type,
                        identity.unified_msg_origin,
                        identity.group_id or None,
                        source.persona_id,
                        2,
                        source.observed_at,
                        now,
                        now,
                    ),
                )
            else:
                sequence_no = int(stream["next_sequence"])
                older_delivery = delivered and stream["last_observed_at"] is not None and source.observed_at < stream["last_observed_at"]
                await connection.execute(
                    """
                    UPDATE conversation_streams
                    SET next_sequence = ?, last_observed_at = ?,
                        persona_id = COALESCE(?, persona_id), updated_at = ?
                    WHERE stream_id = ?
                    """,
                    (
                        sequence_no + 1,
                        stream["last_observed_at"] if older_delivery else source.observed_at,
                        stream["persona_id"] if older_delivery else source.persona_id,
                        now,
                        identity.stream_id,
                    ),
                )

            await connection.execute(
                """
                INSERT INTO source_records(
                    source_id, stream_id, dedup_key, source_type, actor_kind,
                    actor_account_id, actor_logical_user_id, persona_id, content_text,
                    content_json, observed_at, sequence_no, message_id,
                    source_hash, metadata_json, identity_pending, created_at,
                    schema_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source.source_id,
                    identity.stream_id,
                    source.dedup_key,
                    source.source_type,
                    source.actor_kind,
                    source.actor_account_id or None,
                    logical_user_id,
                    source.persona_id,
                    text,
                    components,
                    source.observed_at,
                    sequence_no,
                    source.message_id or source.response_id or identity.message_id or None,
                    content_hash,
                    json.dumps(metadata, ensure_ascii=False, separators=(",", ":")),
                    1 if identity_pending else 0,
                    now,
                    CURRENT_SCHEMA_VERSION,
                ),
            )
            if metadata.get("delivery_parts"):
                await self._index_delivery_parts(connection, source)
            participant_values = []
            for participant_id in source.participant_ids:
                participant_key = f"account:{participant_id}"
                participant_values.append(
                    (
                        source.source_id,
                        participant_key,
                        "account",
                        participant_id,
                        (
                            logical_user_id
                            if participant_id == source.actor_account_id
                            else None
                        ),
                        source.actor_account_name or None,
                    )
                )
            if participant_values:
                await connection.executemany(
                    """
                    INSERT OR IGNORE INTO source_participants(
                        source_id, participant_key, participant_kind, account_id,
                        logical_user_id, display_name
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    participant_values,
                )
            await connection.execute(
                """
                INSERT INTO source_cursors(
                    stream_id, last_captured_sequence, updated_at, cursor_version
                ) VALUES (?, ?, ?, 1)
                ON CONFLICT(stream_id) DO UPDATE SET
                    last_captured_sequence = excluded.last_captured_sequence,
                    updated_at = excluded.updated_at
                """,
                (identity.stream_id, sequence_no, now),
            )
        return CaptureResult(
            source_id=source.source_id,
            stream_id=identity.stream_id,
            sequence_no=sequence_no,
            inserted=True,
            logical_user_id=logical_user_id,
        )

    async def register_platform(self, platform_instance_id: str, platform_name: str) -> None:
        """Register an installed adapter before managing its first account."""
        async with self.database.transaction() as connection:
            await register_platform(connection, platform_instance_id, platform_name)

    async def bind_account(
        self,
        *,
        platform_instance_id: str,
        account_id: str,
        logical_user_id: str,
        display_name: str | None = None,
        source: str = "manual",
        platform_name: str | None = None,
    ) -> dict[str, Any]:
        """Create or update an explicit account binding."""

        now = time.time()
        async with self.database.transaction() as connection:
            if platform_name:
                await register_platform(connection, platform_instance_id, platform_name)
            routes = await account_routes(connection, platform_instance_id, account_id)
            if len({row["logical_user_id"] for row in routes}) > 1:
                raise AmbiguousAccountOwner()
            if any(row["logical_user_id"] != logical_user_id and row["source"] != "automatic-single-account"
                   for row in routes):
                raise ValueError(
                    "account is already bound to another logical user; "
                    "unbind explicitly first"
                )
            await connection.execute(
                """
                INSERT INTO logical_users(
                    logical_user_id, display_name, status, created_at, updated_at
                ) VALUES (?, ?, 'active', ?, ?)
                ON CONFLICT(logical_user_id) DO UPDATE SET
                    display_name = CASE WHEN logical_users.display_name_source='manual'
                        THEN logical_users.display_name ELSE COALESCE(
                            excluded.display_name, logical_users.display_name
                        ) END,
                    updated_at = excluded.updated_at
                """,
                (logical_user_id, display_name, now, now),
            )
            instances = {route["platform_instance_id"] for route in routes} | {platform_instance_id}
            await connection.executemany(
                """
                INSERT INTO account_bindings(
                    platform_instance_id, account_id, logical_user_id,
                    binding_status, source, created_at, updated_at
                ) VALUES (?, ?, ?, 'active', ?, ?, ?)
                ON CONFLICT(platform_instance_id, account_id) DO UPDATE SET
                    logical_user_id = excluded.logical_user_id,
                    binding_status = 'active', source = excluded.source,
                    updated_at = excluded.updated_at
                """,
                [(instance, account_id, logical_user_id, source, now, now) for instance in sorted(instances)],
            )
        return {
            "platform_instance_id": platform_instance_id,
            "account_id": account_id,
            "logical_user_id": logical_user_id,
            "binding_status": "active",
        }

    async def get_logical_user(self, logical_user_id: str) -> dict[str, Any]:
        """Read a managed identity and all its current account associations."""
        async with self.database.transaction(write=False) as connection:
            cursor = await connection.execute(
                "SELECT * FROM logical_users WHERE logical_user_id=? AND status<>'deleted'",
                (logical_user_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise KeyError(logical_user_id)
            cursor = await connection.execute(
                "SELECT * FROM account_routes WHERE logical_user_id=? "
                "ORDER BY platform_instance_id,account_id", (logical_user_id,),
            )
            return {**dict(row), "accounts": group_accounts(await cursor.fetchall())}

    @staticmethod
    def space_user_query(space):
        """Return the private owner or speakers recorded in this persona's group."""
        if space["space_type"] == "private":
            return "SELECT ?", (space["logical_user_id"],)
        return (
            "SELECT DISTINCT r.actor_logical_user_id FROM source_records r "
            "JOIN conversation_streams s ON s.stream_id=r.stream_id "
            "WHERE s.platform_instance_id=? AND s.group_id=? AND lower(s.message_type)='groupmessage' "
            "AND r.persona_id=? AND r.actor_kind='user' AND r.source_type='conversation' "
            "AND r.identity_pending=0",
            (space["platform_instance_id"], space["group_id"], space["persona_id"]),
        )

    async def list_logical_users(self, q: str = "", limit: int = 50, offset: int = 0, *, space=None) -> dict[str, Any]:
        """List names and accounts without loading source conversations."""
        scope_sql, scope_params = "", ()
        if space is not None:
            members, scope_params = self.space_user_query(space)
            scope_sql = f"u.logical_user_id IN ({members}) AND "
        async with self.database.transaction(write=False) as connection:
            cursor = await connection.execute(
                "SELECT u.* FROM logical_users u WHERE u.status<>'deleted' AND "
                f"{scope_sql}(?='' OR instr(lower(COALESCE(u.display_name,'')),lower(?))>0 "
                "OR instr(lower(u.logical_user_id),lower(?))>0 "
                "OR lower(?)=lower('lu_' || substr(CASE WHEN substr(u.logical_user_id,1,5)='user_' "
                "THEN substr(u.logical_user_id,6) WHEN substr(u.logical_user_id,1,3)='lu_' "
                "THEN substr(u.logical_user_id,4) ELSE u.logical_user_id END,1,7)) OR EXISTS "
                "(SELECT 1 FROM account_bindings a WHERE a.logical_user_id=u.logical_user_id "
                "AND (instr(lower(a.account_id),lower(?))>0 "
                "OR instr(lower(a.platform_instance_id),lower(?))>0))) "
                "ORDER BY u.created_at,u.logical_user_id LIMIT ? OFFSET ?",
                (*scope_params, q, q, q, q, q, q, limit + 1, offset),
            )
            rows = await cursor.fetchall()
            items = [{**dict(row), "accounts": []} for row in rows[:limit]]
            by_id = {item["logical_user_id"]: item for item in items}
            if by_id:
                placeholders = ",".join("?" for _ in by_id)
                cursor = await connection.execute(
                    f"SELECT * FROM account_routes WHERE logical_user_id IN ({placeholders}) "
                    "ORDER BY platform_instance_id,account_id", tuple(by_id),
                )
                for account in group_accounts(await cursor.fetchall()):
                    by_id[account["logical_user_id"]]["accounts"].append(account)
        return {"items": items, "has_more": len(rows) > limit, "limit": limit, "offset": offset}

    async def rename_logical_user(self, logical_user_id: str, display_name: str, *, space=None) -> dict[str, Any]:
        """Give an identity a name that future account nicknames cannot overwrite."""
        name = display_name.strip()
        if not name or len(name) > 200:
            raise ValueError("display name must contain 1 to 200 characters")
        scope_sql, scope_params = "", ()
        if space is not None:
            members, scope_params = self.space_user_query(space)
            scope_sql = f" AND logical_user_id IN ({members})"
        async with self.database.transaction() as connection:
            cursor = await connection.execute(
                "UPDATE logical_users SET display_name=?,display_name_source='manual',updated_at=? "
                f"WHERE logical_user_id=? AND status<>'deleted'{scope_sql}", (name, time.time(), logical_user_id, *scope_params),
            )
            if not cursor.rowcount:
                raise KeyError(logical_user_id)
        return await self.get_logical_user(logical_user_id)

    async def get_user_display_names(self, logical_user_ids, *, manual_only: bool = False) -> dict[str, str]:
        """Read current names by authoritative identity; no nickname matching."""
        ids = tuple(dict.fromkeys(logical_user_ids))
        names = {}
        for offset in range(0, len(ids), 400):
            batch = ids[offset:offset + 400]
            placeholders = ",".join("?" for _ in batch)
            rows = await self.database.fetchall(
                f"SELECT logical_user_id,display_name FROM logical_users "
                f"WHERE logical_user_id IN ({placeholders})"
                + (" AND display_name_source='manual'" if manual_only else ""), batch)
            names.update({row["logical_user_id"]: row["display_name"] for row in rows
                          if row["display_name"]})
        return names

    async def find_user_ids_by_display_name(self, query: str) -> list[str]:
        """Find existing identities for a literal display-name search."""
        if not query:
            return []
        rows = await self.database.fetchall(
            "SELECT logical_user_id FROM logical_users "
            "WHERE instr(lower(display_name),lower(?))>0 ORDER BY logical_user_id",
            (query,))
        return [row["logical_user_id"] for row in rows]

    async def list_streams(self, limit: int = 100) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        rows = await self.database.fetchall(
            """
            SELECT stream_id, platform_instance_id, platform_name, message_type,
                   conversation_id, group_id, persona_id, next_sequence,
                   last_observed_at, created_at, updated_at
            FROM conversation_streams
            ORDER BY updated_at DESC LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in rows]

    async def list_records(
        self, *, stream_id: str | None = None, limit: int = 50, offset: int = 0
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        offset = max(0, int(offset))
        if stream_id:
            rows = await self.database.fetchall(
                """
                SELECT * FROM source_records
                WHERE stream_id = ?
                ORDER BY sequence_no DESC LIMIT ? OFFSET ?
                """,
                (stream_id, limit, offset),
            )
        else:
            rows = await self.database.fetchall(
                """
                SELECT * FROM source_records
                ORDER BY observed_at DESC, sequence_no DESC LIMIT ? OFFSET ?
                """,
                (limit, offset),
            )
        result = []
        for row in rows:
            item = dict(row)
            try:
                item["metadata"] = json.loads(item.pop("metadata_json") or "{}")
            except json.JSONDecodeError:
                item["metadata"] = {}
            result.append(item)
        return result

    async def has_source(self, source_id: str) -> bool:
        """Return whether a stable source record exists for derived references."""

        row = await self.database.fetchone(
            "SELECT 1 FROM source_records WHERE source_id = ? LIMIT 1", (source_id,)
        )
        return row is not None

    async def get_observation_times(self, source_ids: tuple[str, ...]) -> dict[str, float]:
        """Batch-read original timestamps without loading conversation bodies.

        Missing records remain absent: cleaned sources cannot acquire a date
        from a derived database snapshot.
        """
        unique_ids = tuple(dict.fromkeys(source_ids))
        result = {}
        for offset in range(0, len(unique_ids), 200):
            batch = unique_ids[offset:offset + 200]
            placeholders = ",".join("?" for _ in batch)
            rows = await self.database.fetchall(
                f"SELECT source_id,observed_at FROM source_records WHERE source_id IN ({placeholders})",
                batch,
            )
            result.update((row["source_id"], row["observed_at"]) for row in rows)
        return result

    async def list_image_evidence(self, source_ids: tuple[str, ...]) -> list[dict]:
        """Read durable observations in message and original attachment order."""
        rows = await self.database.fetchall(
            "SELECT e.* FROM json_each(?) ids JOIN source_image_evidence e ON e.source_id=ids.value "
            "ORDER BY ids.key, e.slot",
            (json.dumps(source_ids),),
        )
        return [dict(row) for row in rows]

    async def save_image_evidence(self, evidence: list[dict]) -> None:
        """Publish committed observations while their original sources still exist."""
        async with self.database.transaction() as connection:
            await connection.executemany(
                """INSERT INTO source_image_evidence(source_id, slot, description, origin)
                   SELECT source_id, :slot, :description, :origin FROM source_records WHERE source_id=:source_id
                   ON CONFLICT(source_id, slot) DO NOTHING""",
                evidence,
            )

    async def get_records(self, source_ids: tuple[str, ...]) -> list[dict[str, Any]]:
        """Read an exact ordered input range with immutable scope snapshots.

        The stream contributes only platform/group fields; the persona comes
        from the record, since a stream can later switch persona.
        """
        if not source_ids or len(source_ids) > 200 or len(set(source_ids)) != len(source_ids):
            raise ValueError("source range must contain 1..200 unique IDs")
        placeholders = ",".join("?" for _ in source_ids)
        rows = await self.database.fetchall(
            f"""
            SELECT r.*, s.platform_instance_id, s.message_type, s.group_id,
                   COALESCE(json_extract(r.metadata_json,'$.delivered_proactive'),0) AS delivered_proactive,
                   s.conversation_id,
                   CASE WHEN json_extract(r.metadata_json,'$.delivered_proactive') THEN json_extract(r.metadata_json,'$.recipient_logical_user_id')
                   WHEN json_extract(r.metadata_json, '$.trigger_message_id') IS NOT NULL
                   THEN (SELECT u.actor_logical_user_id FROM source_records u
                         WHERE u.stream_id = r.stream_id AND u.persona_id = r.persona_id
                           AND u.actor_kind = 'user'
                           AND u.message_id = json_extract(r.metadata_json, '$.trigger_message_id')
                         LIMIT 1)
                   ELSE (SELECT u.actor_logical_user_id FROM source_records u
                    WHERE u.stream_id = r.stream_id AND u.persona_id = r.persona_id
                      AND u.actor_kind = 'user' AND u.sequence_no <= r.sequence_no
                    ORDER BY u.sequence_no DESC LIMIT 1) END AS conversation_logical_user_id
            FROM source_records r JOIN conversation_streams s USING (stream_id)
            WHERE r.source_id IN ({placeholders})
            """, source_ids,
        )
        by_id = {row["source_id"]: dict(row) for row in rows}
        for record in by_id.values():
            if not record["delivered_proactive"]:
                del record["delivered_proactive"]
        if len(by_id) != len(source_ids):
            raise ExtractionWindowChanged("unknown source_id in extraction range")
        return [by_id[source_id] for source_id in source_ids]

    async def resolve_account(self, platform_instance_id: str, account_id: str, platform_name: str | None = None) -> str | None:
        """Resolve an entry point without creating users during read-only recall."""
        async with self.database.transaction(write=False) as connection:
            try:
                return await resolve_owner(connection, platform_instance_id, account_id, platform_name)
            except AmbiguousAccountOwner:
                return None

    async def delivery_recipient(self, instance, account, platform, timestamp):
        async with self.database.transaction(write=False) as connection:
            try:
                return await delivery_owner(connection, instance, account, platform, timestamp)
            except AmbiguousAccountOwner:
                return None

    async def recent_conversation(self, stream_id: str, persona_id: str, *, since: float,
                                  until: float, limit: int = 10, exclude_message_id: str = "",
                                  host_conversation_id: str | None = None,
                                  logical_user_id: str | None = None, recall_context: bool = False) -> list[dict[str, Any]]:
        """Read bounded history, respecting a known host conversation boundary.

        Older sources without the host conversation ID remain available when
        no ID is supplied, but are never guessed into a known conversation.
        """
        if limit <= 0:
            return []
        conversation_id = host_conversation_id or None
        recall_fields = """actor_account_id,message_id,
            json_extract(metadata_json,'$.trigger_message_id') AS trigger_message_id,
            json_extract(metadata_json,'$.chat_model') AS chat_model_json,""" if recall_context else ""
        rows = await self.database.fetchall(
            f"""SELECT source_id,actor_kind,content_text,observed_at,{recall_fields}
                json_extract(metadata_json,'$.delivered_proactive') AS delivered_proactive,
                json_extract(metadata_json,'$.proactive_contact_id') AS proactive_contact_id,
                json_extract(metadata_json,'$.delivery_parts') AS delivery_parts FROM source_records r
            WHERE stream_id=? AND persona_id=? AND identity_pending=0 AND source_type='conversation'
              AND actor_kind IN ('user','agent') AND observed_at>=? AND observed_at<=?
              AND NOT (actor_kind='user' AND COALESCE(message_id,'')=? AND ?<>'')
              AND (? IS NULL OR json_extract(metadata_json,'$.host_conversation_id')=?)
              AND (? IS NULL OR (CASE WHEN r.actor_kind='user' THEN r.actor_logical_user_id
                WHEN json_extract(r.metadata_json,'$.delivered_proactive') THEN json_extract(r.metadata_json,'$.recipient_logical_user_id')
                WHEN json_extract(r.metadata_json,'$.trigger_message_id') IS NOT NULL THEN
                  (SELECT u.actor_logical_user_id FROM source_records u
                   WHERE u.stream_id=r.stream_id AND u.persona_id=r.persona_id AND u.actor_kind='user'
                   AND u.message_id=json_extract(r.metadata_json,'$.trigger_message_id') LIMIT 1)
                ELSE (SELECT u.actor_logical_user_id FROM source_records u
                   WHERE u.stream_id=r.stream_id AND u.persona_id=r.persona_id AND u.actor_kind='user'
                   AND u.sequence_no<=r.sequence_no ORDER BY u.sequence_no DESC LIMIT 1) END)=?)
            ORDER BY observed_at DESC,sequence_no DESC LIMIT ?""",
            (stream_id, persona_id, since, until, exclude_message_id, exclude_message_id,
             conversation_id, conversation_id, logical_user_id, logical_user_id, min(limit, 50)),
        )
        result = []
        for row in rows:
            item = dict(row)
            parts = item.pop("delivery_parts")
            if parts:
                item["delivered_texts"] = part_texts(item["content_text"], json.loads(parts))
            if not item["delivered_proactive"]:
                del item["delivered_proactive"], item["proactive_contact_id"]
            result.append(item)
        return result
