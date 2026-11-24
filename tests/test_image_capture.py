import asyncio
import io
import json
import os
import time
from types import SimpleNamespace

import pytest
from PIL import Image

from core.image_cache import PrivateImageCache
from core.image_capture import project_images, unambiguous_caption
from core.source_ingest import normalize_event, normalize_sent_images
from tests.test_source_ingest import FakeEvent
from tests.test_recall_runtime import plugin_factory


def image_component(ref):
    return {"type": "Image", "file": ref}


def test_cacheable_slots_preserve_failed_positions_forward_authors_and_outer_sticker_hint():
    raw = {"message": [{"type": "image", "data": {"sub_type": 1}}, {"type": "image", "data": {}},
                       {"type": "image", "data": {}}]}
    parts = [image_component("sticker"), image_component(""), {"type": "Plain", "text": "说明"},
             {"type": "Node", "name": "小乔", "content": [
                 {"type": "Node", "name": None, "content": [image_component("nested")]}]},
             {"type": "Reply", "chain": [image_component("quoted")]}, image_component("last")]
    refs = {}
    safe, slots = project_images(parts, private=True, raw=raw, references=refs)
    assert slots == [{"slot": 1}, {"slot": 2, "author_path": ["小乔", None]}, {"slot": 3}]
    assert refs == {2: "nested", 3: "last"}
    assert safe[0] == {"type": "Face"}
    assert "quoted" not in json.dumps(safe)
    group_refs = {}
    _, slots = project_images(parts, private=False, raw=raw, references=group_refs)
    assert slots == [] and group_refs == {}


def test_both_source_directions_contain_only_image_descriptors():
    ref = "data:image/png;base64,cHJpdmF0ZQ=="
    event = FakeEvent()
    event.message_obj = SimpleNamespace(message_id="test", message=[image_component(ref)])
    event.message_str = "看图 " + ref
    refs = {}
    source = normalize_event(event, image_references=refs)
    assert refs == {1: ref}
    assert source.content_text == "看图 [图片]"
    assert json.loads(source.content_json) == [{"type": "Image", "slot": 1}]
    sent = normalize_sent_images(event, [image_component(ref), {"type": "Plain", "text": "我拍的"}],
                                 persona_id="P", send_number=2, image_references=refs)
    assert sent.content_text == "我拍的" and sent.actor_kind == "agent"
    assert sent.source_id != source.source_id
    assert "base64" not in sent.content_json
    assert sent.metadata["trigger_message_id"] == source.message_id


def test_host_caption_requires_one_direct_image():
    parts = [SimpleNamespace(text='<image_caption>已有描述</image_caption>')]
    req = SimpleNamespace(extra_user_content_parts=parts)
    assert unambiguous_caption(req, [image_component("a")]) == "已有描述"
    assert unambiguous_caption(req, [image_component("a"), image_component("b")]) is None
    assert unambiguous_caption(req, [{"type": "Reply"}, image_component("a")]) is None


@pytest.mark.asyncio
async def test_image_sweeper_recovers_after_temporary_filesystem_error(tmp_path, monkeypatch):
    cache = PrivateImageCache(tmp_path / "images", ttl_seconds=.01)
    original = cache.prune
    recovered = asyncio.Event()
    calls = 0

    def once_unavailable():
        nonlocal calls
        calls += 1
        if calls <= 2:
            raise PermissionError("temporary image directory failure")
        original()
        recovered.set()

    monkeypatch.setattr(cache, "prune", once_unavailable)
    await cache.open()
    try:
        assert cache.last_cleanup_error == "PermissionError"
        await asyncio.wait_for(recovered.wait(), 2)
        assert cache.last_cleanup_error is None
        assert cache.last_cleanup_at is not None
    finally:
        await cache.close()


@pytest.mark.asyncio
async def test_image_cache_pending_order_reopen_expiry_and_discard(tmp_path):
    gate = asyncio.Event()
    async def resolve(ref):
        if ref == "slow":
            await gate.wait()
        data = io.BytesIO()
        Image.new("RGBA", (1200, 300), "blue").save(data, format="PNG")
        return data.getvalue()

    directory = tmp_path / "images"
    cache = PrivateImageCache(directory, resolver=resolve)
    await cache.open()
    cache.stage("source", {1: "slow", 3: "fast"})
    assert await cache.read("source", 3)
    gate.set()
    jpeg = await cache.read("source", 1)
    with Image.open(io.BytesIO(jpeg)) as image:
        assert image.format == "JPEG" and image.size == (768, 192)
    event = FakeEvent()
    cache.expect_caption("source", event)
    cache.set_caption("source", event, "说明")
    await cache.close()
    cache = PrivateImageCache(directory, ttl_seconds=30)
    await cache.open()
    try:
        assert await cache.read("source", 2) is None
        assert await cache.read("source", 1) == jpeg
        assert await cache.caption("source", 1) == "说明"
        for p in directory.glob("*.3.jpg"):
            os.utime(p, (time.time()-31, time.time()-31))
        assert await cache.read("source", 3) is None
        cache.expect_caption("source", event)
        await cache.discard("source")
        cache.set_caption("source", event, "迟到的描述")
        assert list(directory.iterdir()) == []
    finally:
        await cache.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("platform", ["aiocqhttp", "qq_official"])
async def test_delivered_private_images_keep_sender_and_accompanying_text(plugin_factory, platform):
    from astrbot.api.message_components import Image as HostImage, Plain
    from astrbot.api.provider import LLMResponse
    from astrbot.core.message.message_event_result import MessageChain
    from core.window_images import prepare_slots
    from main import SourceCaptureFilter
    from tests.test_recall_runtime import RuntimeEvent

    plugin = await plugin_factory()
    event = RuntimeEvent()
    event.get_platform_name = lambda: platform
    refuse = False

    async def deliver(chain, stream=None):
        if refuse:
            if platform == "qq_official":
                return None
            raise RuntimeError("delivery failed")
        return "receipt"

    event.send = deliver
    if platform == "qq_official":
        event._post_send_one = deliver
    SourceCaptureFilter().filter(event, None)
    send = event._post_send_one if platform == "qq_official" else event.send
    await plugin.capture_agent_response(event, LLMResponse("assistant", completion_text="随图发的文字", id="r1"))
    def component(color):
        buf = io.BytesIO()
        Image.new("RGB", (8, 8), color).save(buf, format="PNG")
        return HostImage.fromBytes(buf.getvalue())

    chain = MessageChain([Plain("随图发的文字"), component("red"), component("blue")])
    assert await send(chain) == "receipt"
    assert await send(MessageChain([component("green")])) == "receipt"
    refuse = True
    if platform == "qq_official":
        assert await send(chain) is None
    else:
        with pytest.raises(RuntimeError, match="delivery failed"):
            await send(chain)
    await event._neomemory_source_capture
    rows = [dict(r) for r in await plugin.source_store.database.fetchall(
        "SELECT * FROM source_records ORDER BY sequence_no")]
    assert [row["actor_kind"] for row in rows] == ["user", "agent", "agent", "agent"]
    assert [row["content_text"] for row in rows[1:]] == ["随图发的文字", "", ""]
    assert all("base64" not in str(row) for row in rows)
    assert all(row["actor_account_id"] == event.get_self_id() for row in rows[1:])
    slots = await prepare_slots(plugin.source_store, plugin.image_cache, rows, limit=10)
    assert [(slot["source_alias"], slot["slot"]) for slot in slots] == [("s3", 1), ("s3", 2), ("s4", 1)]
    assert [slot.get("sent_with_source") for slot in slots] == ["s2", "s2", None]
    for slot, dominant in zip(slots, (0, 2, 1)):
        data = await plugin.image_cache.read(slot["source_id"], slot["slot"])
        with Image.open(io.BytesIO(data)) as jpeg:
            pixel = jpeg.getpixel((0, 0))
            assert pixel[dominant] == max(pixel)
    isolated = await prepare_slots(plugin.source_store, plugin.image_cache, rows[2:3], limit=10)
    assert [slot["sent_with_text"] for slot in isolated] == ["随图发的文字"] * 2
