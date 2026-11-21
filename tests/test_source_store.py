import asyncio
from dataclasses import replace

import pytest

from core.identity import EventIdentity, default_logical_user_id
from core.source_ingest import CapturedSource
from storage.schema import CURRENT_SCHEMA_VERSION
from storage.source_store import SourceStore


def make_source(message_id: str = "m-1", content: str = "hello"):
    identity = EventIdentity(
        platform_instance_id="adapter-1",
        platform_name="test",
        message_type="FriendMessage",
        unified_msg_origin="adapter-1:FriendMessage:user-1",
        session_id="user-1",
        group_id="",
        account_id="user-1",
        account_name="Alice",
        self_id="bot-1",
        message_id=message_id,
    )
    return CapturedSource(
        identity=identity,
        actor_kind="user",
        content_text=content,
        observed_at=100.0,
        message_id=message_id,
        actor_account_id="user-1",
        actor_account_name="Alice",
        participant_ids=("user-1",),
        persona_id="persona-default",
    )


@pytest.mark.asyncio
async def test_source_store_initializes_and_deduplicates(tmp_path):
    store = SourceStore(tmp_path / "source.db")
    await store.open()

    first = await store.capture(make_source())
    duplicate = await store.capture(make_source())
    second = await store.capture(make_source("m-2", "second"))

    assert store.schema_version == CURRENT_SCHEMA_VERSION == 1
    assert first.inserted is True
    assert duplicate.inserted is False
    assert duplicate.sequence_no == first.sequence_no == 1
    assert second.inserted is True
    assert second.sequence_no == 2
    health = await store.health()
    assert health["source_count"] == 2
    assert health["stream_count"] == 1
    records = await store.list_records()
    assert records[0]["persona_id"] == "persona-default"
    assert records[0]["identity_pending"] == 0
    await store.close()


@pytest.mark.asyncio
async def test_source_store_explicit_binding_is_idempotent_and_conflicts(tmp_path):
    store = SourceStore(tmp_path / "source.db")
    await store.open()
    await store.capture(make_source())

    result = await store.bind_account(
        platform_instance_id="adapter-1",
        account_id="user-1",
        logical_user_id="logical-alice",
        display_name="Alice",
    )
    assert result["binding_status"] == "active"
    same = await store.bind_account(
        platform_instance_id="adapter-1",
        account_id="user-1",
        logical_user_id="logical-alice",
    )
    assert same["logical_user_id"] == "logical-alice"

    with pytest.raises(ValueError):
        await store.bind_account(
            platform_instance_id="adapter-1",
            account_id="user-1",
            logical_user_id="another-user",
        )
    await store.close()


@pytest.mark.asyncio
async def test_manual_logical_name_survives_capture_binding_and_reopen(tmp_path):
    path = tmp_path / "source.db"
    store = SourceStore(path)
    await store.open()
    try:
        captured = await store.capture(make_source())
        logical = captured.logical_user_id
        renamed = await store.rename_logical_user(logical, "  Common name  ")
        assert renamed["display_name"] == "Common name"
        assert renamed["display_name_source"] == "manual"
        assert len(renamed["accounts"]) == 1
        await store.capture(replace(make_source("later"), actor_account_name="New platform name"))
        await store.bind_account(platform_instance_id="another-platform", account_id="second-account",
                                 logical_user_id=logical, display_name="Another nickname")
        assert await store.get_user_display_names([logical]) == {logical: "Common name"}
        assert await store.get_user_display_names([logical], manual_only=True) == {logical: "Common name"}
        with pytest.raises(ValueError):
            await store.rename_logical_user(logical, " ")
        with pytest.raises(KeyError):
            await store.rename_logical_user("absent", "Name")
    finally:
        await store.close()
    await store.open()
    try:
        read = await store.get_logical_user(logical)
        assert read["display_name"] == "Common name"
        assert len(read["accounts"]) == 2
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_logical_user_listing_search_pagination_and_deleted_filter(tmp_path):
    store = SourceStore(tmp_path / "source.db")
    await store.open()
    try:
        for user in ("one", "two", "gone"):
            await store.bind_account(platform_instance_id="test", account_id=user,
                                     logical_user_id=user, display_name="Same name")
        await store.bind_account(platform_instance_id="second-platform", account_id="external-42",
                                 logical_user_id="two")
        await store.database.execute("UPDATE logical_users SET status='deleted' WHERE logical_user_id='gone'")
        first = await store.list_logical_users(limit=1)
        second = await store.list_logical_users(limit=1, offset=1)
        assert first["has_more"] and not second["has_more"]
        assert first["items"][0]["logical_user_id"] == "one"
        assert second["items"][0]["logical_user_id"] == "two"
        assert len(second["items"][0]["accounts"]) == 2
        assert len((await store.list_logical_users(q="same"))["items"]) == 2
        assert (await store.list_logical_users(q="external-42"))["items"][0]["logical_user_id"] == "two"
        assert (await store.list_logical_users(q="second-platform"))["items"][0]["logical_user_id"] == "two"
        assert not (await store.list_logical_users(q="gone"))["items"]
        for user in ("lu_1234567aaa", "user_1234567bbb"):
            await store.bind_account(platform_instance_id="test", account_id=user, logical_user_id=user)
        matches = await store.list_logical_users(q="lu_1234567")
        assert {item["logical_user_id"] for item in matches["items"]} == {"lu_1234567aaa", "user_1234567bbb"}
        with pytest.raises(KeyError):
            await store.get_logical_user("gone")
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_display_name_search_is_literal_and_keeps_duplicate_names_distinct(tmp_path, monkeypatch):
    store = SourceStore(tmp_path / "source.db")
    await store.open()
    try:
        for uid, name in (("first", "Alice 50%_"), ("second", "Alice 50%_"), ("third", "Alicia")):
            await store.bind_account(platform_instance_id="synthetic", account_id=uid,
                logical_user_id=uid, display_name=name)
        assert await store.find_user_ids_by_display_name("aLiCe") == ["first", "second"]
        assert await store.find_user_ids_by_display_name("50%_") == ["first", "second"]
        assert await store.find_user_ids_by_display_name("50__") == []
        assert await store.find_user_ids_by_display_name("' OR 1=1 --") == []

        async def unexpected_read(*_args, **_kwargs):
            raise AssertionError("empty name search should not query the database")

        monkeypatch.setattr(store.database, "fetchall", unexpected_read)
        assert await store.find_user_ids_by_display_name("") == []
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_source_store_keeps_streams_separate(tmp_path):
    store = SourceStore(tmp_path / "source.db")
    await store.open()
    first = make_source("m-1", "one")
    second = replace(
        first,
        identity=replace(
            first.identity,
            unified_msg_origin="adapter-1:FriendMessage:user-2",
            session_id="user-2",
            account_id="user-2",
            message_id="m-2",
        ),
        message_id="m-2",
        actor_account_id="user-2",
    )
    first_result = await store.capture(first)
    second_result = await store.capture(second)

    assert first_result.stream_id != second_result.stream_id
    assert first_result.sequence_no == second_result.sequence_no == 1
    assert len(await store.list_streams()) == 2
    await store.close()


@pytest.mark.asyncio
async def test_source_store_serializes_same_stream_sequences(tmp_path):
    store = SourceStore(tmp_path / "source.db")
    await store.open()

    results = await asyncio.gather(
        *(
            store.capture(make_source(f"m-{index}", f"message-{index}"))
            for index in range(20)
        )
    )
    assert sorted(result.sequence_no for result in results) == list(range(1, 21))
    assert all(result.inserted for result in results)
    await store.close()


@pytest.mark.asyncio
async def test_agent_source_does_not_create_logical_user_binding(tmp_path):
    store = SourceStore(tmp_path / "source.db")
    await store.open()
    source = make_source("agent-1", "agent reply")
    source = replace(
        source,
        actor_kind="agent",
        actor_account_id="bot-1",
        actor_account_name="Agent",
    )
    result = await store.capture(source)
    assert result.logical_user_id is None
    records = await store.list_records()
    assert records[0]["actor_kind"] == "agent"
    assert records[0]["actor_logical_user_id"] is None
    logical_users = await store.database.fetchall("SELECT * FROM logical_users")
    assert logical_users == []
    await store.close()


@pytest.mark.asyncio
async def test_recent_conversation_time_order_actor_scope_and_current_message(tmp_path):
    store = SourceStore(tmp_path / "source.db")
    await store.open()
    base = make_source()
    records = [
        replace(make_source("at-start", "start"), observed_at=100.0),
        replace(make_source("agent", "agent"), actor_kind="agent", observed_at=150.0),
        replace(make_source("tie-user", "tie-user"), observed_at=150.0),
        replace(make_source("current", "exclude-current"), observed_at=200.0),
        replace(make_source("current", "agent-current"), actor_kind="agent", observed_at=200.0),
        replace(make_source("too-old", "too-old"), observed_at=99.0),
        replace(make_source("future", "future"), observed_at=201.0),
        replace(make_source("system", "system"), actor_kind="system", observed_at=170.0),
        replace(make_source("external", "external"), actor_kind="external", observed_at=170.0),
        replace(make_source("event", "source-event"), source_type="source_event", observed_at=170.0),
        replace(make_source("persona", "other-persona"), persona_id="another-persona", observed_at=170.0),
        replace(make_source("pending", "unresolved"), identity_pending=True, observed_at=170.0),
        replace(make_source("stream", "another-stream"), observed_at=170.0,
            identity=replace(base.identity, unified_msg_origin="adapter-1:FriendMessage:another-user")),
    ]
    try:
        for record in records:
            await store.capture(record)
        history = await store.recent_conversation(
            base.identity.stream_id, "persona-default", since=100.0, until=200.0,
            exclude_message_id="current",
        )
        assert [record["content_text"] for record in history] == ["agent-current", "tie-user", "agent", "start"]
        assert [record["actor_kind"] for record in history] == ["agent", "user", "agent", "user"]
        assert set(history[0]) == {"source_id", "actor_kind", "content_text", "observed_at"}
        shorter = await store.recent_conversation(
            base.identity.stream_id, "persona-default", since=100, until=200, limit=2,
            exclude_message_id="current",
        )
        assert shorter == history[:2]
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_recent_conversation_does_not_guess_old_records_into_new_host_conversation(tmp_path):
    store = SourceStore(tmp_path / "source.db")
    await store.open()
    base = make_source()
    try:
        for index, conversation in enumerate((None, "old-conversation", "new-conversation")):
            for actor in ("user", "agent"):
                record = replace(
                    make_source(f"message-{index}", f"{conversation}:{actor}"),
                    actor_kind=actor, observed_at=100.0 + index,
                    metadata={"host_conversation_id": conversation} if conversation else {},
                )
                await store.capture(record)
        history = await store.recent_conversation(
            base.identity.stream_id, "persona-default", since=0, until=1000,
            host_conversation_id="new-conversation",
        )
        assert [record["content_text"] for record in history] == ["new-conversation:agent", "new-conversation:user"]
        for missing_id in (None, ""):
            unscoped = await store.recent_conversation(
                base.identity.stream_id, "persona-default", since=0, until=1000,
                host_conversation_id=missing_id,
            )
            assert len(unscoped) == 6
        assert await store.recent_conversation(
            base.identity.stream_id, "persona-default", since=0, until=1000,
            host_conversation_id="unknown-conversation",
        ) == []
        assert store.schema_version == CURRENT_SCHEMA_VERSION
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_recent_conversation_enforces_default_and_hard_limits(tmp_path):
    store = SourceStore(tmp_path / "source.db")
    await store.open()
    base = make_source()
    try:
        for index in range(55):
            await store.capture(replace(make_source(str(index), str(index)), observed_at=float(index)))
        args = (base.identity.stream_id, "persona-default")
        recent = await store.recent_conversation(*args, since=0, until=100)
        assert [item["content_text"] for item in recent] == [str(index) for index in range(54, 44, -1)]
        limited = await store.recent_conversation(*args, since=0, until=100, limit=1000)
        assert len(limited) == 50
        assert limited[-1]["content_text"] == "5"
        for limit in (0, -1):
            assert await store.recent_conversation(*args, since=0, until=100, limit=limit) == []
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_resolve_account_derives_identity_without_creating_records(tmp_path):
    store = SourceStore(tmp_path / "source.db")
    await store.open()
    try:
        assert await store.resolve_account("adapter", "account") == default_logical_user_id("adapter", "account")
        assert await store.resolve_account("adapter", "account") != await store.resolve_account("other-adapter", "account")
        assert await store.database.fetchall("SELECT * FROM logical_users") == []
        assert await store.database.fetchall("SELECT * FROM account_bindings") == []
        assert (await store.health())["source_count"] == 0
    finally:
        await store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["active", "disabled", "conflict"])
async def test_resolve_account_honors_binding_status_without_reactivating_it(tmp_path, status):
    store = SourceStore(tmp_path / "source.db")
    await store.open()
    try:
        await store.bind_account(platform_instance_id="adapter", account_id="account", logical_user_id="logical-user")
        await store.database.execute(
            "UPDATE account_bindings SET binding_status=? WHERE platform_instance_id=? AND account_id=?",
            (status, "adapter", "account"),
        )
        before = [dict(row) for row in await store.database.fetchall("SELECT * FROM account_bindings")]
        assert await store.resolve_account("adapter", "account") == ("logical-user" if status == "active" else None)
        after = [dict(row) for row in await store.database.fetchall("SELECT * FROM account_bindings")]
        assert before == after
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_group_mention_unknown_until_corroborated_and_never_guesses_later_transfer(tmp_path):
    from core.group_addressing import group_source_addresses

    store = SourceStore(tmp_path/"source.db")
    await store.open()
    try:
        base = make_source("first", "<@unknown> <@later> <@manual>")
        identity = replace(base.identity, group_id="group", message_type="GroupMessage",
                           unified_msg_origin="adapter-1:GroupMessage:group")
        first = await store.capture(replace(base, identity=identity))
        # A new manual owner may appear after the original mention.
        await store.bind_account(platform_instance_id="adapter-1", account_id="manual", logical_user_id="manual-new")
        captured = [first]
        for account in ("later", "manual"):
            user = replace(identity, message_id=account, account_id=account)
            captured.append(await store.capture(replace(base, message_id=account, identity=user,
                                actor_account_id=account, content_text="到场", participant_ids=(account,))))
        rows = await store.get_records(tuple(result.source_id for result in captured))
        targets = group_source_addresses(rows)[first.source_id]["group_addressing"]
        assert [target["actor_kind"] for target in targets] == ["unknown", "user", "unknown"]
        assert targets[1]["logical_user_id"] == captured[1].logical_user_id
        assert group_source_addresses(rows[:1])[first.source_id]["group_addressing"][1]["actor_kind"] == "unknown"
        assert not await store.database.fetchone("SELECT 1 FROM account_bindings WHERE account_id='unknown'")
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_ingress_drains_without_storing_image_bytes(tmp_path):
    import hashlib
    import json

    from storage.ingress_store import IngressStore

    source = SourceStore(tmp_path / "source.db")
    queue = IngressStore(tmp_path / "ingress.db")
    await source.open()
    await queue.open()
    try:
        uri = "base64://QVVUSE9SRUQ="
        record = replace(
            make_source(message_id="", content="附图 " + uri),
            content_json=json.dumps([{"type": "Node", "name": "甲", "content": [{"type": "Image", "file": uri}]}]),
        )
        await queue.enqueue(record)
        claim = await queue.claim(source_id=record.source_id)
        captured = await source.capture(claim.source)
        assert captured.source_id == record.source_id
        again = await source.capture(claim.source)
        assert not again.inserted
        row, = await source.get_records((captured.source_id,))
        assert row["content_text"] == "附图 [图片]"
        assert row["source_hash"] == hashlib.sha256(row["content_text"].encode()).hexdigest()
        assert uri not in row["content_json"]
        assert json.loads(row["metadata_json"])["image_attachments"] == [{"slot": 1, "author_path": ["甲"]}]
    finally:
        await queue.close()
        await source.close()
