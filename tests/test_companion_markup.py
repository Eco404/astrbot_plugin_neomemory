import hashlib
from types import SimpleNamespace

import pytest

from core.text_cleaning import DEFAULT_CLEANER
from core.source_ingest import normalize_agent_response, normalize_event, normalize_sent_images
from tests.test_source_ingest import FakeEvent
from tests.test_delivered_extraction import receipt
from tests.test_recall_runtime import plugin_factory

SPLIT = "<<PRIVATE_COMPANION_SPLIT>>"
REACTION = '<pc_reaction_expression>{"intent":"合成测试"}</pc_reaction_expression>'


@pytest.mark.parametrize("original,expected", [
    ("甲\n" + SPLIT + "\n乙", "甲\n乙"),
    ("甲&lt;&lt;PRIVATE_COMPANION_SPLIT&gt;&gt;乙", "甲\n乙"),
    ("甲{{split_marker}}乙", "甲\n乙"),
    ("甲" + REACTION + "乙", "甲乙"),
    ("甲<pc_member_safety><think>hidden</think></pc_member_safety>乙", "甲乙"),
    ("甲＜PC-Reaction_Expression＞隐藏" + SPLIT + "乙", "甲\n乙"),
    ("甲" + REACTION.replace("合成测试", SPLIT) + "乙", "甲乙"),
    ("甲&lt;personality_sync&gt;private&lt;/personality_sync&gt;乙", "甲乙"),
    ("甲<｜｜DSML｜｜ calls><｜｜DSML｜｜ invoke name='x'>hi</｜｜DSML｜｜ invoke></｜｜DSML｜｜ calls>乙", "甲乙"),
    ("甲<call:send>{content:乙}</call>丙", "甲乙丙"),
    ("甲<pc_tts>[情绪：轻快]记得拿<soft>伞</soft>。<pause/></pc_tts>乙", "甲记得拿伞。乙"),
    ("甲&&angry&&乙", "甲乙"),
    ("C 代码 x&&y&&z | [轻笑] [1](link) <soft>引用</soft>", "C 代码 x&&y&&z | [轻笑] [1](link) <soft>引用</soft>"),
    (f"示例 `{SPLIT}`\n~~~xml\n{REACTION}\n~~~\n> {SPLIT}\n好", f"示例 `{SPLIT}`\n~~~xml\n{REACTION}\n~~~\n> {SPLIT}\n好"),
])
def test_text_projection(original, expected):
    assert DEFAULT_CLEANER.clean(original) == expected
    assert DEFAULT_CLEANER.clean(expected) == expected


def test_image_caption_component_boundaries_and_user_original():
    event = FakeEvent()
    event.message_str = SPLIT
    assert normalize_event(event).content_text == SPLIT
    resp = SimpleNamespace(completion_text=REACTION)
    assert normalize_agent_response(event, resp) is None
    resp.completion_text = REACTION + "visible"
    assert normalize_agent_response(event, resp, max_content_chars=3).content_text == "vis"
    parts = ["前<pc_reac", 'tion_expression>{"test":1}', "</pc_reaction_expression>尾"]
    assert DEFAULT_CLEANER.clean_parts(parts) == ["前", "", "尾"]
    chain = [{"type": "Plain", "text": s} for s in parts]
    chain.insert(1, {"type": "Image", "file": "x"})
    chain.append({"type": "Node", "name": "另一人", "content": [{"type": "Plain", "text": SPLIT}]})
    source = normalize_sent_images(event, chain, persona_id="p", send_number=1, image_references={})
    assert source.content_text == "前尾"
    assert "pc_reac" not in source.content_json and SPLIT in source.content_json


@pytest.mark.asyncio
async def test_proactive_receipt_replay_and_marker_only_item(plugin_factory):
    plugin = await plugin_factory()
    adapter = plugin.external_memory
    raw = "甲" + SPLIT + "乙"
    record = receipt(1, 50, text=raw)
    source_id = await adapter.write(record)
    (row,) = await plugin.source_store.get_records((source_id,))
    assert row["content_text"] == "甲\n乙"
    assert row["source_hash"] == hashlib.sha256("甲\n乙".encode()).hexdigest()
    await plugin.source_store.database.execute("UPDATE source_records SET content_text=?,source_hash=? WHERE source_id=?",
                                               (raw, hashlib.sha256(raw.encode()).hexdigest(), source_id))
    assert await adapter.accept_delivery(record, historical=True) == source_id
    assert await adapter.write(receipt(2, 70, text=REACTION)) is None


@pytest.mark.asyncio
async def test_pending_agent_source_storage_preserves_identity_and_sanitizes_components(tmp_path):
    import json
    from storage.source_store import SourceStore

    event = FakeEvent()
    raw = "甲" + SPLIT + "乙"
    src = normalize_sent_images(event, [{"type":"Plain", "text":raw}, {"type":"Image", "file":"x"}],
                                persona_id="p", send_number=1, image_references={})
    store = SourceStore(tmp_path / "source.db")
    await store.open()
    try:
        result = await store.capture(src)
        row, = await store.get_records((result.source_id,))
        assert result.source_id == src.source_id
        assert row["content_text"] == "甲\n乙"
        assert json.loads(row["metadata_json"])["original_content_length"] == 3
        assert json.loads(row["content_json"])[0]["text"] == "甲\n乙"
    finally:
        await store.close()
