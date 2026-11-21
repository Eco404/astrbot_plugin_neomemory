from dataclasses import replace

import pytest
import pytest_asyncio

from core.memory_spaces import group_space, private_space
from storage.source_scan import SourceScanner
from storage.source_store import SourceStore
from tests.test_source_store import make_source


@pytest_asyncio.fixture
async def source(tmp_path):
    store = SourceStore(tmp_path / "sources.db")
    await store.open()
    try:
        yield store
    finally:
        await store.close()


def different_stream(message_id, *, platform="adapter-2", account="user-2", persona="persona-default", group=""):
    item = make_source(message_id)
    message_type = "GroupMessage" if group else "FriendMessage"
    return replace(item, persona_id=persona, actor_account_id=account,
                   identity=replace(item.identity, platform_instance_id=platform, account_id=account,
                                    message_type=message_type, group_id=group,
                                    unified_msg_origin=f"{platform}:{message_type}:{group or account}"))


@pytest.mark.asyncio
async def test_discovery_is_bounded_and_advances_over_unsupported_sources(source):
    records = [
        replace(make_source("pending"), identity_pending=True),
        replace(make_source("external"), actor_kind="external"),
        replace(make_source("event"), source_type="source_event"),
        replace(make_source("unresolved"), persona_id=None),
        make_source("usable"),
        make_source("duplicate-space"),
        different_stream("group", group="group-1"),
    ]
    for record in records:
        await source.capture(record)
    scanner = SourceScanner(source)
    page1 = await scanner.discover_spaces(limit=2)
    assert page1.spaces == ()
    assert page1.next_cursor == 2 and page1.has_more
    page2 = await scanner.discover_spaces(after=page1.next_cursor, limit=2)
    assert page2.spaces == ()
    assert page2.next_cursor == 4 and page2.has_more
    page3 = await scanner.discover_spaces(after=page2.next_cursor, limit=2)
    assert len(page3.spaces) == 1 and page3.spaces[0].space_type == "private"
    page4 = await scanner.discover_spaces(after=page3.next_cursor, limit=2)
    assert page4.spaces == (group_space("persona-default", "adapter-2", "group-1"),)
    assert not page4.has_more
    empty = await scanner.discover_spaces(after=page4.next_cursor, limit=2)
    assert empty.spaces == () and empty.next_cursor == page4.next_cursor


@pytest.mark.asyncio
async def test_page_filters_persona_user_pending_and_source_types(source):
    first = await source.capture(make_source("first"))
    for record in (
        replace(make_source("other-persona"), persona_id="another"),
        different_stream("other-user"),
        replace(make_source("pending"), identity_pending=True),
        replace(make_source("external"), actor_kind="external"),
        replace(make_source("event"), source_type="source_event"),
        make_source("last"),
    ):
        await source.capture(record)
    scanner = SourceScanner(source)
    spec = private_space("persona-default", first.logical_user_id)
    page = await scanner.page(spec)
    assert [row["message_id"] for row in page.records] == ["first", "last"]
    assert [row["scan_cursor"] for row in page.records] == [1, 7]
    assert not page.has_more
    other = await scanner.page(private_space("another", first.logical_user_id))
    assert [row["message_id"] for row in other.records] == ["other-persona"]


@pytest.mark.asyncio
async def test_group_scope_is_persona_platform_group_and_shared_members(source):
    first = different_stream("first", platform="a", group="g", account="member-1", persona="p")
    second = different_stream("second", platform="a", group="g", account="member-2", persona="p")
    for item in (
        first, second,
        different_stream("other-persona", platform="a", group="g", persona="q"),
        different_stream("other-group", platform="a", group="h", persona="p"),
        different_stream("other-platform", platform="b", group="g", persona="p"),
        replace(second, message_id="agent", actor_kind="agent", actor_account_id="bot"),
    ):
        await source.capture(item)
    scanner = SourceScanner(source)
    page = await scanner.page(group_space("p", "a", "g"))
    assert [row["message_id"] for row in page.records] == ["first", "second", "agent"]
    assert await scanner.highwater(group_space("p", "a", "g")) == {first.identity.stream_id: 4}


@pytest.mark.asyncio
async def test_binding_snapshot_and_cross_stream_insertion_order(source):
    before = await source.capture(make_source("before-binding"))
    await source.bind_account(platform_instance_id="adapter-1", account_id="user-1", logical_user_id="shared")
    await source.bind_account(platform_instance_id="adapter-2", account_id="user-2", logical_user_id="shared")
    await source.capture(make_source("same-first"))
    await source.capture(different_stream("other-stream"))
    await source.capture(make_source("same-last"))
    scanner = SourceScanner(source)
    shared = await scanner.page(private_space("persona-default", "shared"))
    assert [row["message_id"] for row in shared.records] == ["same-first", "other-stream", "same-last"]
    old = await scanner.page(private_space("persona-default", before.logical_user_id))
    assert [row["message_id"] for row in old.records] == ["before-binding"]


@pytest.mark.asyncio
async def test_agent_uses_trigger_or_preceding_user_but_never_next_user(source):
    # Unowned agent before the first user cannot borrow ownership from a later user.
    await source.capture(replace(make_source("orphan"), actor_kind="agent"))
    user = await source.capture(make_source("trigger"))
    await source.bind_account(platform_instance_id="adapter-1", account_id="user-1", logical_user_id="other")
    await source.capture(make_source("later-user"))
    await source.capture(replace(make_source("trigger-reply"), actor_kind="agent",
                                 metadata={"trigger_message_id": "trigger"}))
    await source.capture(replace(make_source("preceding-reply"), actor_kind="agent"))
    await source.capture(replace(make_source("missing-trigger"), actor_kind="agent",
                                 metadata={"trigger_message_id": "unknown"}))
    await source.capture(replace(make_source("wrong-persona"), actor_kind="agent", persona_id="q",
                                 metadata={"trigger_message_id": "trigger"}))
    scanner = SourceScanner(source)
    old = await scanner.page(private_space("persona-default", user.logical_user_id))
    assert [row["message_id"] for row in old.records] == ["trigger", "trigger-reply"]
    current = await scanner.page(private_space("persona-default", "other"))
    assert [row["message_id"] for row in current.records] == ["later-user", "preceding-reply"]
    discovered = await scanner.discover_spaces()
    assert {space.scope_key for space in discovered.spaces} == {
        private_space("persona-default", user.logical_user_id).scope_key,
        private_space("persona-default", "other").scope_key,
    }


@pytest.mark.asyncio
async def test_fixed_highwater_excludes_late_arrivals_and_new_streams(source, monkeypatch):
    # Wall-clock rollback and backdated platform timestamps do not reorder input.
    monkeypatch.setattr("storage.source_store.time.time", lambda: 1000)
    first = await source.capture(replace(make_source("first"), observed_at=200))
    monkeypatch.setattr("storage.source_store.time.time", lambda: 500)
    await source.capture(replace(make_source("second"), observed_at=100))
    scanner = SourceScanner(source)
    spec = private_space("persona-default", first.logical_user_id)
    watermark = await scanner.highwater(spec)
    await source.capture(replace(make_source("late"), observed_at=50))
    await source.bind_account(platform_instance_id="adapter-2", account_id="user-2", logical_user_id=first.logical_user_id)
    await source.capture(different_stream("new-stream"))
    frozen = await scanner.page(spec, highwater=watermark)
    assert [row["message_id"] for row in frozen.records] == ["first", "second"]
    assert await scanner.count(spec, highwater=watermark) == len(frozen.records) == 2
    live = await scanner.page(spec)
    assert [row["message_id"] for row in live.records] == ["first", "second", "late", "new-stream"]
    assert await scanner.count(spec) == len(live.records) == 4
    assert (await scanner.page(spec, highwater={})).records == []
    assert await scanner.count(spec, highwater={}) == 0


@pytest.mark.asyncio
async def test_highwater_blocks_ownership_from_late_trigger(source):
    first = await source.capture(make_source("first"))
    await source.capture(replace(make_source("waiting-agent"), actor_kind="agent",
                                 metadata={"trigger_message_id": "late-trigger"}))
    await source.capture(make_source("last-existing"))
    scanner = SourceScanner(source)
    spec = private_space("persona-default", first.logical_user_id)
    watermark = await scanner.highwater(spec)
    await source.capture(make_source("late-trigger"))
    assert await scanner.count(spec, highwater=watermark) == 2
    assert await scanner.count(spec) == 4
    assert [row["message_id"] for row in (await scanner.page(spec, highwater=watermark)).records] == [
        "first", "last-existing"]
    assert "waiting-agent" in [row["message_id"] for row in (await scanner.page(spec)).records]


@pytest.mark.asyncio
async def test_page_cursor_can_commit_only_a_consumed_prefix(source):
    first = await source.capture(make_source("m-1"))
    for index in range(2, 6):
        await source.capture(make_source(f"m-{index}"))
    scanner = SourceScanner(source)
    # A real memory-space row (with unrelated DB fields) is accepted too.
    space = {"space_type": "private", "persona_id": "persona-default", "logical_user_id": first.logical_user_id,
             "space_id": "ignored-in-source-database"}
    page1 = await scanner.page(space, limit=3)
    assert page1.has_more and len(page1.records) == 3
    page2 = await scanner.page(space, after=page1.records[0]["scan_cursor"], limit=3)
    assert [row["message_id"] for row in page2.records] == ["m-2", "m-3", "m-4"]
    page3 = await scanner.page(space, after=page2.next_cursor, limit=3)
    assert [row["message_id"] for row in page3.records] == ["m-5"]
    assert not page3.has_more


@pytest.mark.asyncio
@pytest.mark.parametrize("arguments", [{"after": -1}, {"after": True}, {"limit": 0}, {"limit": 201}, {"limit": 1.5}])
async def test_scanner_rejects_invalid_pagination(source, arguments):
    scanner = SourceScanner(source)
    with pytest.raises(ValueError):
        await scanner.discover_spaces(**arguments)
    with pytest.raises(ValueError):
        await scanner.page(private_space("p", "u"), **arguments)


@pytest.mark.asyncio
@pytest.mark.parametrize("highwater", [{"": 1}, {"stream": True}, {"stream": -1}, {"stream": 1.1}])
async def test_scanner_rejects_invalid_watermark(source, highwater):
    with pytest.raises(ValueError):
        await SourceScanner(source).page(private_space("p", "u"), highwater=highwater)
