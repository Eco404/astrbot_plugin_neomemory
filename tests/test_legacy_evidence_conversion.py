import hashlib
import json
import os
import sqlite3

import pytest

from scripts import convert_legacy_evidence as converter
from storage.source_scan import SourceScanner
from storage.source_store import SourceStore


def legacy_row(uid, role="user", *, timestamp=100.0, sequence=1, content="synthetic source"):
    sender = "human-1" if role == "user" else "bot-1"
    actor_type = "human" if role == "user" else "assistant"
    return {
        "message_uid": uid, "session_id": "adapter-1:FriendMessage:human-1", "source_message_id": int(uid.rsplit("-", 1)[1]),
        "role": role, "content": content, "content_hash": hashlib.sha256(content.encode()).hexdigest(),
        "sender_id": sender, "sender_name": "synthetic name", "actor_id": f"qq:{actor_type}:{sender}",
        "platform": "aiocqhttp", "timestamp": timestamp, "sequence": sequence,
        "persona_id": "old-persona", "redacted": 0,
        "metadata": {
            "platform_instance_id": "adapter-1", "canonical_platform": "qq",
            "actor_id": f"qq:{actor_type}:{sender}", "actor_type": actor_type,
            "is_bot_message": role == "assistant", "trigger_message_id": "unreliable-legacy-trigger",
            "nested": {"attachment": ["synthetic", 3]},
        },
        "created_at": 300.0,
    }


def make_input(tmp_path, rows=None):
    path = tmp_path / "evidence.db"
    rows = rows if rows is not None else [legacy_row("uid-1"), legacy_row("uid-2", "assistant", timestamp=101)]
    with sqlite3.connect(path) as connection:
        connection.executescript("""
            CREATE TABLE evidence_metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            INSERT INTO evidence_metadata VALUES('schema_version','evidence-v2');
            INSERT INTO evidence_metadata VALUES('content_state_schema_version','evidence-content-state-v1');
            CREATE TABLE evidence_messages(
                message_uid TEXT PRIMARY KEY, session_id TEXT NOT NULL, source_message_id INTEGER NOT NULL,
                role TEXT NOT NULL,content TEXT NOT NULL,content_hash TEXT NOT NULL,sender_id TEXT NOT NULL,
                sender_name TEXT NOT NULL,actor_id TEXT NOT NULL,platform TEXT NOT NULL,timestamp REAL NOT NULL,
                sequence INTEGER NOT NULL,persona_id TEXT NOT NULL,redacted INTEGER NOT NULL,
                metadata TEXT NOT NULL,created_at REAL NOT NULL);
            CREATE TABLE evidence_tombstone_events(event_sequence INTEGER PRIMARY KEY, event_uid TEXT,
                message_uid TEXT,action TEXT,reason TEXT,created_by TEXT,created_at REAL,restores_event_uid TEXT);
            CREATE TABLE evidence_content_states(message_uid TEXT PRIMARY KEY,scope_uid TEXT,
                operation_uid TEXT,payload TEXT);
        """)
        for row in rows:
            values = dict(row)
            values["metadata"] = json.dumps(values["metadata"], ensure_ascii=False)
            connection.execute(
                f"INSERT INTO evidence_messages({','.join(values)}) VALUES({','.join('?' for _ in values)})",
                tuple(values.values()),
            )
    persona = tmp_path / "persona.md"
    persona.write_text("A synthetic test persona.", encoding="utf-8")
    return path, persona


@pytest.mark.asyncio
async def test_conversion_preserves_payload_order_provenance_and_real_replay(tmp_path):
    rows = [
        legacy_row("uid-4", "assistant", timestamp=103, sequence=1, content="same words"),
        legacy_row("uid-2", timestamp=101, sequence=7, content="same words"),
        legacy_row("uid-1", timestamp=100, sequence=9, content="中文\nfull text\n" + "x" * 9000),
        legacy_row("uid-3", "assistant", timestamp=102, sequence=7, content="same words"),
    ]
    path, persona = make_input(tmp_path, rows)
    before = path.read_bytes()
    output = tmp_path / "output"
    report = await converter.convert_evidence(path, output, persona, "explicit-persona")
    assert path.read_bytes() == before
    assert report["input"]["rows"] == report["output"]["rows"] == 4
    assert report["duplicates"] == report["excluded_count"] == report["model_calls"] == 0
    assert report["output"]["replay_eligible_count"] == 4
    assert report["role_counts"] == {"user": 2, "assistant": 2}
    assert json.loads((output / "manifest.json").read_text()) == report
    assert report["persona"]["sha256"] == hashlib.sha256(persona.read_bytes()).hexdigest()
    assert report["output"]["sha256"] == hashlib.sha256((output / "source.db").read_bytes()).hexdigest()
    assert output.stat().st_mode & 0o777 == 0o700
    assert all(p.stat().st_mode & 0o777 == 0o600 for p in output.iterdir())
    assert set(p.name for p in output.iterdir()) == {"source.db", "manifest.json"}
    serialized = json.dumps(report)
    for private in ("adapter-1:FriendMessage:human-1", "human-1", "bot-1", "explicit-persona", "synthetic source", "same words"):
        assert private not in serialized
    store = SourceStore(output / "source.db")
    await store.open()
    try:
        records = await store.database.fetchall("SELECT * FROM source_records ORDER BY sequence_no")
        ordered = sorted(rows, key=lambda row: tuple(row[k] for k in converter.ORDERING))
        assert [r["sequence_no"] for r in records] == [1, 2, 3, 4]
        for record, original in zip(records, ordered, strict=True):
            assert record["content_text"] == original["content"]
            assert record["source_hash"] == original["content_hash"]
            assert record["observed_at"] == original["timestamp"]
            assert record["message_id"] == original["message_uid"]
            assert record["persona_id"] == "explicit-persona"
            metadata = json.loads(record["metadata_json"])
            assert metadata["legacy"] == {k: v for k, v in original.items() if k != "content"}
            assert metadata["host_conversation_id"] == "adapter-1:FriendMessage:human-1"
            assert "trigger_message_id" not in metadata
            assert record["content_json"] is None
            if original["role"] == "assistant":
                assert record["actor_kind"] == "agent"
                assert record["actor_account_id"] == "bot-1"
                assert record["actor_logical_user_id"] is None
        bindings = await store.database.fetchall("SELECT account_id FROM account_bindings")
        assert [r["account_id"] for r in bindings] == ["human-1"]
        streams = await store.list_streams()
        assert len(streams) == 1 and streams[0]["platform_name"] == "qq"
        scanner = SourceScanner(store)
        spaces = await scanner.discover_spaces()
        assert len(spaces.spaces) == 1
        page = await scanner.page(spaces.spaces[0])
        assert [r["message_id"] for r in page.records] == [r["message_uid"] for r in ordered]
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_conversion_ids_and_canonical_digests_are_stable(tmp_path):
    path, persona = make_input(tmp_path)
    reports = [await converter.convert_evidence(path, tmp_path / name, persona, "explicit-persona") for name in ("one", "two")]
    assert reports[0]["input"]["snapshot_sha256"] == reports[1]["input"]["snapshot_sha256"]
    assert reports[0]["output"]["records_sha256"] == reports[1]["output"]["records_sha256"]
    ids = []
    for name in ("one", "two"):
        with sqlite3.connect(tmp_path / name / "source.db") as connection:
            ids.append(connection.execute("SELECT source_id FROM source_records ORDER BY sequence_no").fetchall())
    assert ids[0] == ids[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["directory", "file", "symlink", "broken_symlink"])
async def test_existing_destination_is_never_replaced(tmp_path, kind):
    path, persona = make_input(tmp_path)
    output = tmp_path / "output"
    if kind == "directory":
        output.mkdir()
    elif kind == "file":
        output.write_text("keep")
    else:
        output.symlink_to(path if kind == "symlink" else tmp_path / "missing")
    with pytest.raises(converter.ConversionError, match="^output_exists$"):
        await converter.convert_evidence(path, output, persona, "p")
    assert os.path.lexists(output)
    assert not list(tmp_path.glob(".legacy-conversion-*"))


@pytest.mark.asyncio
@pytest.mark.parametrize(("mutation", "code"), [
    ("UPDATE evidence_messages SET role='system' WHERE message_uid='uid-2'", "unsupported_role"),
    ("UPDATE evidence_messages SET content='changed' WHERE message_uid='uid-1'", "content_hash_mismatch"),
    ("UPDATE evidence_messages SET redacted=1 WHERE message_uid='uid-1'", "visibility_state_not_supported"),
    ("UPDATE evidence_messages SET timestamp=-1 WHERE message_uid='uid-1'", "invalid_timestamp"),
    ("UPDATE evidence_messages SET timestamp=1e999 WHERE message_uid='uid-1'", "invalid_timestamp"),
    ("UPDATE evidence_messages SET session_id='another' WHERE message_uid='uid-2'", "multiple_conversations_not_supported"),
    ("UPDATE evidence_messages SET persona_id='another' WHERE message_uid='uid-2'", "multiple_conversations_not_supported"),
    ("UPDATE evidence_messages SET metadata='[]' WHERE message_uid='uid-1'", "invalid_metadata"),
    ("UPDATE evidence_messages SET metadata='{\"x\":1,\"x\":2}' WHERE message_uid='uid-1'", "invalid_metadata"),
    ("UPDATE evidence_messages SET metadata='{\"x\":NaN}' WHERE message_uid='uid-1'", "invalid_metadata"),
    ("UPDATE evidence_messages SET metadata=json_set(metadata,'$.napcat_recalled',json('true')) WHERE message_uid='uid-1'", "visibility_state_not_supported"),
    ("UPDATE evidence_messages SET metadata=json_set(metadata,'$.nested.napcat_system',json('true')) WHERE message_uid='uid-1'", "visibility_state_not_supported"),
    ("UPDATE evidence_messages SET metadata=json_set(metadata,'$.actor_id','conflict') WHERE message_uid='uid-1'", "conflicting_identity"),
    ("UPDATE evidence_messages SET metadata=json_set(metadata,'$.platform_instance_id','other') WHERE message_uid='uid-1'", "conflicting_identity"),
    ("UPDATE evidence_messages SET metadata=json_set(metadata,'$.is_bot_message',json('true')) WHERE message_uid='uid-1'", "conflicting_identity"),
    ("UPDATE evidence_messages SET timestamp=99 WHERE message_uid='uid-2'", "unowned_leading_agent"),
    ("INSERT INTO evidence_tombstone_events(event_uid,action) VALUES('synthetic','restore')", "visibility_state_not_supported"),
    ("INSERT INTO evidence_content_states(message_uid) VALUES('uid-1')", "visibility_state_not_supported"),
    ("DROP TABLE evidence_tombstone_events", "unsupported_schema"),
    ("DROP TABLE evidence_content_states", "unsupported_schema"),
    ("ALTER TABLE evidence_tombstone_events DROP COLUMN action", "unsupported_schema"),
    ("UPDATE evidence_messages SET session_id='adapter-1:GroupMessage:synthetic-group'", "unsupported_conversation"),
    ("UPDATE evidence_messages SET session_id='adapter-1:FriendMessage:other-human'", "unsupported_conversation"),
    ("UPDATE evidence_messages SET metadata=json_set(metadata,'$.group_id','synthetic-group')", "unsupported_conversation"),
    ("UPDATE evidence_messages SET metadata=json_set(metadata,'$.message_type','GroupMessage')", "unsupported_conversation"),
    ("UPDATE evidence_messages SET actor_id='qq:human:other',metadata=json_set(metadata,'$.actor_id','qq:human:other') WHERE role='user'", "conflicting_identity"),
])
async def test_invalid_input_rejected_without_partial_output(tmp_path, mutation, code):
    path, persona = make_input(tmp_path)
    with sqlite3.connect(path) as connection:
        connection.execute(mutation)
    before = path.read_bytes()
    with pytest.raises(converter.ConversionError, match=f"^{code}$"):
        await converter.convert_evidence(path, tmp_path / "output", persona, "p")
    assert path.read_bytes() == before
    assert not (tmp_path / "output").exists()
    assert not list(tmp_path.glob(".legacy-conversion-*"))


@pytest.mark.asyncio
async def test_late_capture_failure_removes_staging(tmp_path, monkeypatch):
    path, persona = make_input(tmp_path)
    capture = SourceStore.capture
    count = 0

    async def fail_second(self, source):
        nonlocal count
        count += 1
        if count == 2:
            raise sqlite3.OperationalError("sensitive failure must not escape")
        return await capture(self, source)

    monkeypatch.setattr(SourceStore, "capture", fail_second)
    with pytest.raises(converter.ConversionError, match="^invalid_database$"):
        await converter.convert_evidence(path, tmp_path / "output", persona, "p")
    assert not (tmp_path / "output").exists()
    assert not list(tmp_path.glob(".legacy-conversion-*"))


@pytest.mark.asyncio
async def test_atomic_publish_detects_late_empty_directory_collision(tmp_path, monkeypatch):
    path, persona = make_input(tmp_path)
    publish = converter._publish

    def race(staging, output):
        output.mkdir()
        publish(staging, output)

    monkeypatch.setattr(converter, "_publish", race)
    with pytest.raises(converter.ConversionError, match="^output_exists$"):
        await converter.convert_evidence(path, tmp_path / "output", persona, "p")
    assert list((tmp_path / "output").iterdir()) == []
    assert not list(tmp_path.glob(".legacy-conversion-*"))


@pytest.mark.asyncio
async def test_mutating_input_during_conversion_is_not_published(tmp_path, monkeypatch):
    path, persona = make_input(tmp_path)
    verify = converter._verify

    async def mutate_after_verify(*args):
        result = await verify(*args)
        with sqlite3.connect(path) as connection:
            connection.execute("UPDATE evidence_messages SET sender_name='late change'")
        return result

    monkeypatch.setattr(converter, "_verify", mutate_after_verify)
    with pytest.raises(converter.ConversionError, match="^input_changed$"):
        await converter.convert_evidence(path, tmp_path / "output", persona, "p")
    assert not (tmp_path / "output").exists()
    assert not list(tmp_path.glob(".legacy-conversion-*"))


@pytest.mark.asyncio
async def test_same_original_order_key_is_rejected_even_with_distinct_uids(tmp_path):
    first, second = legacy_row("uid-1"), legacy_row("uid-2", "assistant")
    second["source_message_id"] = first["source_message_id"]
    path, persona = make_input(tmp_path, [first, second])
    with pytest.raises(converter.ConversionError, match="^ambiguous_dialogue_order$"):
        await converter.convert_evidence(path, tmp_path / "output", persona, "p")
    assert not (tmp_path / "output").exists()


@pytest.mark.asyncio
async def test_committed_wal_records_are_in_readonly_snapshot(tmp_path):
    path, persona = make_input(tmp_path)
    writer = sqlite3.connect(path)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        main_before = path.read_bytes()
        baseline, _ = converter._read_snapshot(path.resolve())
        third = legacy_row("uid-3", timestamp=102, content="committed only in WAL")
        third["metadata"] = json.dumps(third["metadata"])
        writer.execute(
            f"INSERT INTO evidence_messages({','.join(third)}) VALUES({','.join('?' for _ in third)})",
            tuple(third.values()),
        )
        writer.commit()
        wal = path.with_name(path.name + "-wal")
        wal_before = wal.read_bytes()
        assert wal_before and path.read_bytes() == main_before
        report = await converter.convert_evidence(path, tmp_path / "output", persona, "p")
        assert report["input"]["rows"] == report["output"]["replay_eligible_count"] == 3
        assert report["input"]["sha256"] == hashlib.sha256(main_before).hexdigest()
        assert report["input"]["snapshot_sha256"] != converter._digest_rows(baseline)
        assert path.read_bytes() == main_before and wal.read_bytes() == wal_before
        with sqlite3.connect(tmp_path / "output" / "source.db") as output:
            assert output.execute("SELECT content_text FROM source_records ORDER BY sequence_no DESC LIMIT 1").fetchone()[0] == "committed only in WAL"
    finally:
        writer.close()


@pytest.mark.asyncio
async def test_output_foreign_key_violation_is_not_published(tmp_path, monkeypatch):
    path, persona = make_input(tmp_path)
    capture = SourceStore.capture

    async def insert_orphan(self, source):
        result = await capture(self, source)
        if source.actor_kind == "agent":
            await self.database.execute("PRAGMA foreign_keys=OFF")
            await self.database.execute(
                "INSERT INTO source_participants(source_id,participant_key,participant_kind,account_id) "
                "VALUES('missing-source','account:orphan','account','orphan')"
            )
            await self.database.execute("PRAGMA foreign_keys=ON")
        return result

    monkeypatch.setattr(SourceStore, "capture", insert_orphan)
    with pytest.raises(converter.ConversionError, match="^output_integrity_failed$"):
        await converter.convert_evidence(path, tmp_path / "output", persona, "p")
    assert not (tmp_path / "output").exists()
    assert not list(tmp_path.glob(".legacy-conversion-*"))


def test_cli_error_never_prints_payload_or_input_path(tmp_path, monkeypatch, capsys):
    path, persona = make_input(tmp_path)
    monkeypatch.setattr("sys.argv", ["convert", "--input", str(path), "--output-dir", str(tmp_path),
                                     "--persona-file", str(persona), "--persona-id", "private-persona"])
    assert converter.main() == 1
    assert json.loads(capsys.readouterr().out) == {"status": "failed", "code": "output_exists"}


def test_read_snapshot_uses_read_only_transaction(tmp_path, monkeypatch):
    path, _ = make_input(tmp_path)
    connect = sqlite3.connect
    calls = []
    statements = []

    def traced_connect(database, **kwargs):
        calls.append((database, kwargs))
        connection = connect(database, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(converter.sqlite3, "connect", traced_connect)
    converter._read_snapshot(path.resolve())
    assert calls == [(path.resolve().as_uri() + "?mode=ro", {"uri": True})]
    assert "BEGIN" in statements and "ROLLBACK" in statements
    assert "PRAGMA query_only=ON" in statements
