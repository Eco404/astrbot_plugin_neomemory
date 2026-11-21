"""Identity changes preserve old attribution and separate subsequent recall."""
from dataclasses import replace

import pytest

from core.memory_spaces import RetiredIdentity, private_space
from storage.memory_store import MemoryStore
from storage.source_scan import SourceScanner
from storage.source_store import SourceStore
from tests.test_source_store import make_source


@pytest.mark.asyncio
async def test_history_filters_captured_owner_for_users_and_agent_replies(tmp_path):
    store = SourceStore(tmp_path / 'source.db')
    await store.open()
    try:
        before = await store.capture(make_source('before'))
        old_answer = await store.capture(replace(make_source('old-answer'), actor_kind='agent', actor_account_id='bot'))
        await store.bind_account(platform_instance_id='adapter-1', account_id='user-1', logical_user_id='new-owner')
        await store.capture(make_source('after'))
        new_answer = await store.capture(replace(make_source('new-answer'), actor_kind='agent', actor_account_id='bot'))
        old = await store.recent_conversation(before.stream_id, 'persona-default', since=0, until=200,
                                              logical_user_id=before.logical_user_id)
        new = await store.recent_conversation(before.stream_id, 'persona-default', since=0, until=200,
                                              logical_user_id='new-owner')
        assert {row['source_id'] for row in old} == {make_source('before').source_id, old_answer.source_id}
        assert {row['source_id'] for row in new} == {make_source('after').source_id, new_answer.source_id}
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_retired_identity_sources_remain_readable_but_do_not_rebuild(tmp_path):
    store = SourceStore(tmp_path / 'source.db')
    memory = MemoryStore(tmp_path / 'memory.db')
    await store.open()
    await memory.open()
    try:
        source = await store.capture(make_source('retired'))
        spec = private_space('persona-default', source.logical_user_id)
        scanner = SourceScanner(store)
        stale_spec = (await scanner.discover_spaces()).spaces[0]
        await store.database.execute("UPDATE logical_users SET status='deleted' WHERE logical_user_id=?", (source.logical_user_id,))
        await memory.database.execute("INSERT INTO pipeline_state VALUES (?,'true',0)", ('retired_identity:' + source.logical_user_id,))
        assert (await scanner.discover_spaces()).spaces == ()
        assert await scanner.count(spec) == 0
        assert (await scanner.page(spec)).records == []
        assert len(await store.get_records((source.source_id,))) == 1
        with pytest.raises(RetiredIdentity):
            await memory.get_or_create_space(stale_spec)
    finally:
        await memory.close()
        await store.close()
