"""Rule order, author boundaries and durable receipt identity under live edits."""

import json
from types import SimpleNamespace

import pytest

from core.source_ingest import normalize_agent_response, normalize_event, normalize_sent_images
from core.text_cleaning import TextCleaner, default_cleaning_config
from storage.ingress_store import IngressError, IngressStore
from tests.test_delivered_extraction import receipt
from tests.test_recall_runtime import plugin_factory
from tests.test_source_ingest import FakeEvent


def rule(kind, pattern, replacement, *, group=None, enabled=True):
    return {"id": pattern, "group_id": group, "enabled": enabled, "name": pattern,
            "kind": kind, "pattern": pattern, "replacement": replacement}


def cleaner(*rules, revision=2, companion=False):
    config = default_cleaning_config()
    config["revision"] = revision
    config["groups"][0]["enabled"] = companion
    config["rules"].extend(rules)
    return TextCleaner(config)


def test_group_and_rule_switches_and_order_across_plain_parts():
    projector = cleaner(rule("literal", ".", r"\1"), rule("regex", r"(Paris)-(\d+)", r"\2/\1"),
                        rule("regex", "$", "!"), rule("literal", "Paris", "wrong", enabled=False),
                        rule("literal", "Paris", "wrong", group="companion"))
    assert projector.clean_parts(["Pa", "ris-", "17."]) == [r"17/Paris", "", r"\1!"]
    assert projector.clean("<think>x</think>") == "<think>x</think>!"
    with pytest.raises(ValueError):
        cleaner(rule("regex", r"(a)", r"\2", enabled=False))


@pytest.mark.asyncio
async def test_custom_replacement_is_applied_once_and_preserves_other_authors(plugin_factory):
    plugin = await plugin_factory()
    event = FakeEvent()
    event.message_str = "x"
    store = plugin.source_store
    store.cleaner = cleaner(rule("literal", "x", "xx"))
    # Public event handlers choose the store's current rules.
    await plugin.capture_agent_response(event, SimpleNamespace(role="assistant", completion_text="x"))
    await event._neomemory_source_capture
    user = normalize_event(event)
    await store.capture(user)
    chain = [{"type": "Plain", "text": "x"}, {"type": "Image", "file": "synthetic.png"},
             {"type": "Node", "content": [{"type": "Plain", "text": "x"}]}]
    media = normalize_sent_images(event, chain, persona_id="p", send_number=2,
                                  image_references={}, cleaner=store.cleaner)
    await store.capture(media)
    rows = await store.list_records()
    assert sorted(row["content_text"] for row in rows) == ["x", "xx", "xx"]
    parts = json.loads((await store.get_records((media.source_id,)))[0]["content_json"])
    assert parts[0]["text"] == "xx" and parts[2]["content"][0]["text"] == "x"
    assert normalize_agent_response(event, SimpleNamespace(completion_text="x"),
                                    cleaner=cleaner(rule("literal", "x", ""))) is None


@pytest.mark.asyncio
async def test_live_edits_preserve_accepted_and_queued_receipt_projections(plugin_factory, tmp_path):
    plugin = await plugin_factory()
    store = plugin.source_store
    adapter = plugin.external_memory
    store.cleaner = cleaner(rule("literal", "x", "xx"))
    record = receipt(70, 300, text="x")
    external = receipt(71, 310, text="x")
    external["metadata"].pop("delivery_status")
    external_id = await adapter.write(external)
    saved_id = await adapter.write(record)
    queued = IngressStore(tmp_path / "queue.db")
    await queued.open()
    try:
        first = adapter._delivery(record, cleaner=store.cleaner)
        await queued.enqueue(first)
        store.cleaner = cleaner(rule("literal", "x", "y"), revision=3)
        assert await adapter.write(record) == saved_id
        assert await adapter.write(external) == external_id
        duplicate = adapter._delivery(record, historical=True, cleaner=store.cleaner)
        await queued.enqueue(duplicate)
        different = adapter._delivery({**record, "content": "y"}, cleaner=store.cleaner)
        with pytest.raises(IngressError, match="conflict"):
            await queued.enqueue(different)
        assert (await queued.claim()).source.content_text == "xx"
        assert all(row["content_text"] == "xx" for row in await store.get_records((saved_id, external_id)))
        store.cleaner = cleaner(rule("literal", "x", ""), revision=4)
        assert await adapter.write(record) == saved_id
        assert await adapter.write(receipt(72, 320, text="x")) is None
    finally:
        await queued.close()
