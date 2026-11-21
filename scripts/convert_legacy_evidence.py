"""Read-only, offline conversion of one legacy private conversation.

This deliberately narrow converter is not the plugin's general import API. It
never contacts models, edits input records, or merges into an existing database.
Only aggregate diagnostics are returned; the private output retains provenance.
"""

from __future__ import annotations

import argparse
import asyncio
import ctypes
import errno
import hashlib
import json
import math
import os
import shutil
import sqlite3
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

from core.text_cleaning import DEFAULT_CLEANER
from core.identity import EventIdentity
from core.source_ingest import CapturedSource
from storage.source_scan import SourceScanner
from storage.source_store import SourceStore

FORMAT_VERSION = "legacy-evidence-conversion.v1"
ORDERING = ["timestamp", "source_message_id", "created_at", "message_uid"]
MESSAGE_FIELDS = {
    "message_uid", "session_id", "source_message_id", "role", "content",
    "content_hash", "sender_id", "sender_name", "actor_id", "platform",
    "timestamp", "sequence", "persona_id", "redacted", "metadata", "created_at",
}
TABLE_FIELDS = {
    "evidence_metadata": {"key", "value"},
    "evidence_messages": MESSAGE_FIELDS,
    "evidence_tombstone_events": {
        "event_sequence", "event_uid", "message_uid", "action", "reason",
        "created_by", "created_at", "restores_event_uid",
    },
    "evidence_content_states": {"message_uid", "scope_uid", "operation_uid", "payload"},
}
VISIBILITY_FLAGS = {
    "redacted", "recalled", "withdrawn", "deleted", "tombstoned", "system",
    "is_redacted", "is_recalled", "is_withdrawn", "is_deleted", "is_system",
    "napcat_recalled", "napcat_system",
}


class ConversionError(ValueError):
    """A stable, content-free error safe to print in CLI output."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _digest_rows(rows: list[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(_json(row).encode("utf-8") + b"\n")
    return digest.hexdigest()


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ConversionError("invalid_metadata")
        result[key] = value
    return result


def _reject_constant(_: str) -> None:
    raise ConversionError("invalid_metadata")


def _visibility_guard(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if key.casefold() in VISIBILITY_FLAGS and item not in (None, False, 0, "", "0", "false"):
                raise ConversionError("visibility_state_not_supported")
            _visibility_guard(item)
    elif isinstance(value, list):
        for item in value:
            _visibility_guard(item)


def _required_text(value: Any, code: str = "invalid_identity") -> str:
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise ConversionError(code)
    return value


def _read_snapshot(path: Path) -> tuple[list[dict[str, Any]], dict[str, str]]:
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA trusted_schema=OFF")
        connection.execute("BEGIN")
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        required = {"evidence_metadata", "evidence_messages", "evidence_tombstone_events"}
        if not required <= tables:
            raise ConversionError("unsupported_schema")
        for table, fields in TABLE_FIELDS.items():
            if table in tables:
                columns = {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}
                if columns != fields:
                    raise ConversionError("unsupported_schema")
        versions = dict(connection.execute("SELECT key,value FROM evidence_metadata"))
        if versions.get("schema_version") != "evidence-v2":
            raise ConversionError("unsupported_schema")
        if versions.get("content_state_schema_version") not in (None, "evidence-content-state-v1"):
            raise ConversionError("unsupported_schema")
        if "content_state_schema_version" in versions and "evidence_content_states" not in tables:
            raise ConversionError("unsupported_schema")
        for table in ("evidence_tombstone_events", "evidence_content_states"):
            if table in tables and connection.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone():
                raise ConversionError("visibility_state_not_supported")
        if connection.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise ConversionError("input_integrity_failed")
        rows = [dict(row) for row in connection.execute(
            "SELECT * FROM evidence_messages ORDER BY timestamp,source_message_id,created_at,message_uid"
        )]
        if not rows:
            raise ConversionError("empty_input")
        order_keys = [(row["timestamp"], row["source_message_id"], row["created_at"]) for row in rows]
        if len(set(order_keys)) != len(order_keys):
            # UID gives deterministic output, but cannot prove the original
            # order of otherwise indistinguishable records (including roles).
            raise ConversionError("ambiguous_dialogue_order")
        for row in rows:
            if row["role"] not in {"user", "assistant"}:
                raise ConversionError("unsupported_role")
            if row["redacted"] != 0:
                raise ConversionError("visibility_state_not_supported")
            for key in ("message_uid", "session_id", "sender_id", "actor_id", "persona_id"):
                _required_text(row[key])
            if not isinstance(row["sender_name"], str) or not isinstance(row["content"], str):
                raise ConversionError("invalid_payload")
            content_hash = hashlib.sha256(row["content"].encode("utf-8")).hexdigest()
            if row["content_hash"] != content_hash:
                raise ConversionError("content_hash_mismatch")
            for key in ("timestamp", "created_at"):
                if not isinstance(row[key], (int, float)) or not math.isfinite(row[key]) or row[key] < 0:
                    raise ConversionError("invalid_timestamp")
            for key in ("sequence", "source_message_id"):
                if type(row[key]) is not int or row[key] < 0:
                    raise ConversionError("invalid_sequence")
            try:
                metadata = json.loads(row["metadata"], object_pairs_hook=_strict_object,
                                      parse_constant=_reject_constant)
                if not isinstance(metadata, dict):
                    raise ConversionError("invalid_metadata")
                _json(metadata)
            except (TypeError, ValueError, RecursionError) as exc:
                raise ConversionError("invalid_metadata") from exc
            _visibility_guard(metadata)
            row["metadata"] = metadata
            _required_text(metadata.get("platform_instance_id"))
            if metadata.get("canonical_platform") != "qq" or row["platform"] not in {"qq", "aiocqhttp"}:
                raise ConversionError("unsupported_platform")
            actor_type = "human" if row["role"] == "user" else "assistant"
            if row["actor_id"] != f"qq:{actor_type}:{row['sender_id']}":
                raise ConversionError("conflicting_identity")
            if metadata.get("actor_id", row["actor_id"]) != row["actor_id"]:
                raise ConversionError("conflicting_identity")
            if metadata.get("persona_id", row["persona_id"]) != row["persona_id"]:
                raise ConversionError("conflicting_identity")
            if metadata.get("actor_type", "human" if row["role"] == "user" else "assistant") != (
                "human" if row["role"] == "user" else "assistant"
            ):
                raise ConversionError("conflicting_identity")
            if "is_bot_message" in metadata and metadata["is_bot_message"] is not (row["role"] == "assistant"):
                raise ConversionError("conflicting_identity")
            if metadata.get("group_id") not in (None, "") or metadata.get("message_type") not in (None, "", "FriendMessage"):
                raise ConversionError("unsupported_conversation")
        for key in ("session_id", "persona_id"):
            if len({row[key] for row in rows}) != 1:
                raise ConversionError("multiple_conversations_not_supported")
        if len({row["metadata"]["platform_instance_id"] for row in rows}) != 1:
            raise ConversionError("conflicting_identity")
        if len({row["message_uid"] for row in rows}) != len(rows):
            raise ConversionError("duplicate_message_uid")
        by_role = {role: [row for row in rows if row["role"] == role] for role in ("user", "assistant")}
        for role_rows in by_role.values():
            if not role_rows or any(len({row[key] for row in role_rows}) != 1 for key in ("sender_id", "actor_id")):
                raise ConversionError("conflicting_identity")
        if by_role["user"][0]["sender_id"] == by_role["assistant"][0]["sender_id"]:
            raise ConversionError("conflicting_identity")
        instance = rows[0]["metadata"]["platform_instance_id"]
        if rows[0]["session_id"] != f"{instance}:FriendMessage:{by_role['user'][0]['sender_id']}":
            raise ConversionError("unsupported_conversation")
        if rows[0]["role"] != "user":
            raise ConversionError("unowned_leading_agent")
        return rows, versions
    finally:
        connection.rollback()
        connection.close()


def _capture(row: dict[str, Any], *, first_user: dict[str, Any], first_agent: dict[str, Any],
             persona_id: str) -> CapturedSource:
    instance = row["metadata"]["platform_instance_id"]
    identity = EventIdentity(
        platform_instance_id=instance, platform_name="qq", message_type="FriendMessage",
        unified_msg_origin=f"{instance}:FriendMessage:{first_user['sender_id']}",
        session_id=first_user["sender_id"], group_id="", account_id=first_user["sender_id"],
        account_name=first_user["sender_name"], self_id=first_agent["sender_id"],
        message_id=row["message_uid"],
    )
    legacy = {key: value for key, value in row.items() if key != "content"}
    metadata = {"host_conversation_id": row["session_id"], "legacy": legacy,
                "legacy_conversion_version": FORMAT_VERSION}
    if row["role"] == "assistant":
        metadata["text_cleaning_revision"] = DEFAULT_CLEANER.revision
    return CapturedSource(
        identity=identity, actor_kind="user" if row["role"] == "user" else "agent",
        content_text=DEFAULT_CLEANER.clean(row["content"]) if row["role"] == "assistant" else row["content"],
        observed_at=float(row["timestamp"]),
        message_id=row["message_uid"], actor_account_id=row["sender_id"],
        actor_account_name=row["sender_name"], participant_ids=(row["sender_id"],),
        persona_id=persona_id,
        metadata=metadata,
    )


async def _verify(store: SourceStore, expected: list[CapturedSource]) -> dict[str, Any]:
    integrity = await store.database.fetchall("PRAGMA integrity_check")
    if len(integrity) != 1 or integrity[0][0] != "ok":
        raise ConversionError("output_integrity_failed")
    if await store.database.fetchall("PRAGMA foreign_key_check"):
        raise ConversionError("output_integrity_failed")
    records = await store.database.fetchall("SELECT * FROM source_records ORDER BY rowid")
    if len(records) != len(expected):
        raise ConversionError("output_verification_failed")
    canonical = []
    for sequence, (row, candidate) in enumerate(zip(records, expected, strict=True), 1):
        comparison = {
            "source_id": candidate.source_id, "stream_id": candidate.identity.stream_id,
            "dedup_key": candidate.dedup_key, "source_type": "conversation",
            "actor_kind": candidate.actor_kind, "actor_account_id": candidate.actor_account_id,
            "persona_id": candidate.persona_id, "content_text": candidate.content_text,
            "observed_at": candidate.observed_at, "sequence_no": sequence,
            "message_id": candidate.message_id, "identity_pending": 0, "content_json": None,
            "source_hash": hashlib.sha256(candidate.content_text.encode("utf-8")).hexdigest(),
        }
        if any(row[key] != value for key, value in comparison.items()):
            raise ConversionError("output_verification_failed")
        if json.loads(row["metadata_json"]) != candidate.metadata:
            raise ConversionError("output_verification_failed")
        if (row["actor_logical_user_id"] is not None) != (candidate.actor_kind == "user"):
            raise ConversionError("output_verification_failed")
        canonical.append({**comparison, "metadata": candidate.metadata,
                          "actor_logical_user_id": row["actor_logical_user_id"]})
    scanner = SourceScanner(store)
    discovery = await scanner.discover_spaces(limit=200)
    if len(discovery.spaces) != 1:
        raise ConversionError("output_replay_failed")
    space = discovery.spaces[0]
    if await scanner.count(space) != len(expected):
        raise ConversionError("output_replay_failed")
    replay_ids: list[str] = []
    cursor = 0
    while True:
        page = await scanner.page(space, after=cursor, limit=200)
        replay_ids.extend(row["source_id"] for row in page.records)
        if not page.has_more:
            break
        cursor = page.next_cursor
    if replay_ids != [candidate.source_id for candidate in expected]:
        raise ConversionError("output_replay_failed")
    counts = {}
    for name in ("conversation_streams", "logical_users", "account_bindings"):
        row = await store.database.fetchone(f"SELECT COUNT(*) AS count FROM {name}")
        counts[name] = row["count"]
    if set(counts.values()) != {1}:
        raise ConversionError("output_verification_failed")
    return {
        "database": "source.db", "rows": len(records), "schema_version": store.schema_version,
        "stream_count": counts["conversation_streams"], "logical_user_count": counts["logical_users"],
        "account_binding_count": counts["account_bindings"], "replay_eligible_count": len(replay_ids),
        "records_sha256": _digest_rows(canonical),
    }


def _publish(staging: Path, output: Path) -> None:
    """Linux no-replace atomic rename: a concurrent empty directory is not replaced."""
    libc = ctypes.CDLL(None, use_errno=True)
    rename = getattr(libc, "renameat2", None)
    if rename is None:
        raise ConversionError("atomic_publish_unavailable")
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    if rename(-100, os.fsencode(staging), -100, os.fsencode(output), 1) != 0:
        code = ctypes.get_errno()
        raise ConversionError("output_exists" if code in {errno.EEXIST, errno.ENOTEMPTY} else "publish_failed")


async def convert_evidence(input_path: str | Path, output_dir: str | Path,
                           persona_file: str | Path, persona_id: str) -> dict[str, Any]:
    """Export a validated snapshot to a new private directory or publish nothing."""
    _required_text(persona_id, "invalid_persona_id")
    original_output = Path(output_dir).absolute()
    if os.path.lexists(original_output):
        raise ConversionError("output_exists")
    staging: Path | None = None
    try:
        input_path = Path(input_path).resolve(strict=True)
        persona_file = Path(persona_file).resolve(strict=True)
        parent = original_output.parent.resolve(strict=True)
        if not input_path.is_file() or not persona_file.is_file() or not parent.is_dir():
            raise ConversionError("invalid_path")
        if not persona_file.stat().st_size:
            raise ConversionError("empty_persona")
        output = parent / original_output.name
        if os.path.lexists(output):
            raise ConversionError("output_exists")
        input_hash = await asyncio.to_thread(_file_sha256, input_path)
        persona_hash = await asyncio.to_thread(_file_sha256, persona_file)
        rows, versions = await asyncio.to_thread(_read_snapshot, input_path)
        first_user = next(row for row in rows if row["role"] == "user")
        first_agent = next(row for row in rows if row["role"] == "assistant")
        candidates = [_capture(row, first_user=first_user, first_agent=first_agent, persona_id=persona_id) for row in rows]
        staging = Path(tempfile.mkdtemp(prefix=".legacy-conversion-", dir=parent))
        staging.chmod(0o700)
        database_path = staging / "source.db"
        database_path.touch(mode=0o600, exist_ok=False)
        store = SourceStore(database_path)
        try:
            await store.open()
            for candidate in candidates:
                result = await store.capture(candidate)
                if not result.inserted:
                    raise ConversionError("unexpected_duplicate")
            output_summary = await _verify(store, candidates)
        finally:
            await store.close()
        if await asyncio.to_thread(_file_sha256, input_path) != input_hash:
            raise ConversionError("input_changed")
        if await asyncio.to_thread(_file_sha256, persona_file) != persona_hash:
            raise ConversionError("persona_changed")
        output_summary["sha256"] = await asyncio.to_thread(_file_sha256, database_path)
        report = {
            "format_version": FORMAT_VERSION,
            "input": {"sha256": input_hash, "snapshot_sha256": _digest_rows(rows), "rows": len(rows),
                      "schema_version": versions["schema_version"],
                      "snapshot_note": "Read-only SQLite transaction; row digest includes committed WAL data."},
            "persona": {"sha256": persona_hash}, "output": output_summary,
            "ordering": ORDERING, "role_counts": dict(Counter(row["role"] for row in rows)),
            "mapping": {
                "content": "Readable assistant text, user text verbatim; original legacy content hash retained",
                "role": {"user": "user", "assistant": "agent"},
                "platform": "metadata.platform_instance_id + canonical_platform=qq",
                "persona": "explicit persona_id; legacy persona retained in per-source metadata",
                "message_id": "message_uid; no text deduplication",
                "observed_at": "timestamp unchanged",
                "conversation": "single private stream; legacy session_id as host_conversation_id",
                "agent_owner": "preceding user; no invented trigger_message_id",
                "metadata": "complete parsed legacy metadata nested under legacy.metadata",
            },
            "preserved_fields": sorted(MESSAGE_FIELDS), "duplicates": 0, "excluded_count": 0,
            "model_calls": 0,
        }
        manifest = staging / "manifest.json"
        with manifest.open("x", encoding="utf-8") as handle:
            handle.write(_json(report) + "\n")
        for artifact in staging.iterdir():
            artifact.chmod(0o600)
        _publish(staging, output)
        staging = None
        return report
    except ConversionError:
        raise
    except sqlite3.Error as exc:
        raise ConversionError("invalid_database") from exc
    except OSError as exc:
        raise ConversionError("filesystem_error") from exc
    finally:
        if staging is not None:
            shutil.rmtree(staging)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--persona-file", required=True, type=Path)
    parser.add_argument("--persona-id", required=True)
    args = parser.parse_args()
    try:
        result = asyncio.run(convert_evidence(args.input, args.output_dir, args.persona_file, args.persona_id))
    except ConversionError as exc:
        print(_json({"status": "failed", "code": exc.code}))
        return 1
    except Exception:
        # Do not print SQLite errors, paths or arbitrary provider/payload text.
        print(_json({"status": "failed", "code": "unexpected_error"}))
        return 1
    print(_json({"status": "completed", **result}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
