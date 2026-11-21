import pytest
import pytest_asyncio

from core.memory_spaces import group_space, private_space
from storage.context_store import ContextStore
from storage.memory_store import MemoryStore


@pytest_asyncio.fixture
async def memory(tmp_path):
    store = MemoryStore(tmp_path / "memory.db")
    await store.open()
    yield store
    await store.close()


async def snapshot(context, space, user="u", stream="s", conversation="c", key="key"):
    return await context.snapshot(space["space_id"], logical_user_id=user, stream_id=stream,
        host_conversation_id=conversation, conversation_key=key)


@pytest.mark.asyncio
async def test_snapshot_keeps_private_user_and_current_group_speaker_isolated(memory):
    context = ContextStore(memory)
    private = await memory.get_or_create_space(private_space("persona", "u"))
    group = await memory.get_or_create_space(group_space("persona", "platform", "g"))
    for space in (private, group):
        await memory.upsert_profile(space_id=space["space_id"], logical_user_id="u",
            tags=["运动"], description="私聊资料" if space == private else "群内资料")
    await memory.upsert_profile(space_id=group["space_id"], logical_user_id="other",
        tags=["秘密"], description="其他成员资料")
    await memory.upsert_relationship(space_id=private["space_id"], logical_user_id="u",
        state_text="我觉得与用户熟悉。")
    own = await snapshot(context, private)
    assert own["profile"]["description"] == "私聊资料"
    assert own["relationship"]["state"]["long_term"]["body"] == "我觉得与用户熟悉。"
    grouped = await snapshot(context, group)
    assert grouped["profile"]["description"] == "群内资料"
    assert grouped["relationship"] is None
    wrong = await snapshot(context, private, user="other")
    assert wrong["profile"] is None and wrong["relationship"] is None
    unknown = await snapshot(context, group, user=None)
    assert unknown["profile"] is None


@pytest.mark.asyncio
async def test_short_term_requires_both_stream_and_host_conversation_and_active_state(memory):
    context = ContextStore(memory)
    space = await memory.get_or_create_space(private_space("p", "u"))
    await memory.upsert_short_term(space_id=space["space_id"], conversation_id="key",
        summary_text="当前讨论", coverage={"stream_id": "s", "host_conversation_id": "c"})
    assert (await snapshot(context, space))["short_term"]["summary_text"] == "当前讨论"
    assert (await snapshot(context, space, stream="other"))["short_term"] is None
    assert (await snapshot(context, space, conversation="other"))["short_term"] is None
    assert (await snapshot(context, space, conversation=None))["short_term"] is None
    await memory.upsert_short_term(space_id=space["space_id"], conversation_id="key",
        summary_text="已停用", coverage={"stream_id": "s", "host_conversation_id": "c"}, status="disabled")
    assert (await snapshot(context, space))["short_term"] is None


@pytest.mark.asyncio
async def test_revalidation_drops_edited_and_disabled_context_but_refreshes_active_note_count(memory):
    context = ContextStore(memory)
    space = await memory.get_or_create_space(private_space("p", "u"))
    await memory.upsert_profile(space_id=space["space_id"], logical_user_id="u",
        tags=[], description="旧画像")
    await memory.upsert_relationship(space_id=space["space_id"], logical_user_id="u", state_text="旧关系")
    note = await memory.create_note(space_id=space["space_id"], content_text="笔记")
    before = await snapshot(context, space)
    assert before["active_notes"] == 1
    await memory.upsert_profile(space_id=space["space_id"], logical_user_id="u",
        tags=[], description="新画像")
    await memory.upsert_relationship(space_id=space["space_id"], logical_user_id="u",
        state_text="旧关系", status="disabled")
    await memory.update_note(note["note_id"], space_id=space["space_id"], status="archived")
    after, hits = await context.revalidate(before)
    assert after["profile"] is None and after["relationship"] is None
    assert after["active_notes"] == 0 and hits == []


@pytest.mark.asyncio
async def test_generation_switch_rejects_entire_old_snapshot_and_old_recall(memory):
    context = ContextStore(memory)
    space = await memory.get_or_create_space(private_space("p", "u"))
    await memory.upsert_profile(space_id=space["space_id"], logical_user_id="u",
        tags=[], description="旧代画像")
    node = await memory.create_node(space_id=space["space_id"], node_type="event",
        content_text="旧代事件", source_ids=["source"])
    before = await snapshot(context, space)
    target = await memory.create_generation(space["space_id"])
    await memory.activate_generation(space["space_id"], target["generation_id"])
    after, hits = await context.revalidate(before, recalled_generation_id=node["generation_id"], recalled_hits=[node])
    assert after["profile"] is None and hits == []


@pytest.mark.asyncio
async def test_final_validation_rejects_stale_parent_and_wrong_recall_generation(memory):
    context = ContextStore(memory)
    space = await memory.get_or_create_space(private_space("p", "u"))
    node = await memory.create_node(space_id=space["space_id"], node_type="event",
        content_text="事件", source_ids=["source"])
    before = await snapshot(context, space)
    _, hits = await context.revalidate(before, recalled_generation_id="different", recalled_hits=[node])
    assert hits == []
    async with memory.database.transaction() as connection:
        await connection.execute("UPDATE memory_nodes SET metadata_json=? WHERE node_id=?",
            ('{"summary_stale":true}', node["node_id"]))
    _, hits = await context.revalidate(before, recalled_generation_id=node["generation_id"], recalled_hits=[node])
    assert hits == []
