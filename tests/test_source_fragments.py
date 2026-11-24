import json

from core.identity import EventIdentity
from core.source_fragments import fragment_source, fragment_info
from core.source_ingest import CapturedSource


def source(text, *, content_json=None, metadata=None):
    return CapturedSource(
        identity=EventIdentity(
            "adapter", "test", "GroupMessage", "adapter:group:g", "g", "g",
            "u", "User", "bot", "message-1",
        ),
        actor_kind="user",
        actor_account_id="u",
        content_text=text,
        content_json=content_json,
        metadata=metadata or {},
        observed_at=100,
        message_id="message-1",
        persona_id="p",
    )


def test_oversized_plain_source_is_split_without_loss_and_with_stable_ids():
    text = "=== 消息 1 ===\n" + ("第一条内容。\n" * 20) + "=== 消息 2 ===\n" + ("第二条内容。\n" * 20)
    original = source(text, content_json=json.dumps([{"type": "Plain", "text": text}]))

    fragments = fragment_source(original, max_chars=80, max_tokens=160)

    assert len(fragments) > 1
    assert "".join(item.content_text for item in fragments) == text
    assert fragments[0].source_id == original.source_id
    assert len({item.source_id for item in fragments}) == len(fragments)
    assert [fragment_info({"metadata": item.metadata})["index"] for item in fragments] == list(range(1, len(fragments) + 1))
    assert all(len(item.content_text) <= 80 for item in fragments)
    assert all(len(item.content_text.encode("utf-8")) + 8 <= 160 for item in fragments)


def test_fragmenting_is_stable_for_repeated_delivery():
    original = source("一\n" * 100)
    first = fragment_source(original, max_chars=20, max_tokens=80)
    second = fragment_source(original, max_chars=20, max_tokens=80)

    assert [item.source_id for item in first] == [item.source_id for item in second]
    assert [item.content_text for item in first] == [item.content_text for item in second]


def test_structured_and_image_sources_remain_atomic():
    forwarded = source("x" * 100, content_json=json.dumps([{"type": "Node", "content": "x"}]))
    image = source("x" * 100, metadata={"image_attachments": [{"slot": 1}]})

    assert fragment_source(forwarded, max_chars=20, max_tokens=80) == (forwarded,)
    assert fragment_source(image, max_chars=20, max_tokens=80) == (image,)
