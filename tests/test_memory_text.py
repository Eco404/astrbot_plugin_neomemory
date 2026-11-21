"""Derived prose keeps stable identities until one-pass presentation rendering."""

import copy

from core.memory_text import (
    map_user_references,
    memory_texts,
    render_memory_item,
    render_memory_text,
    user_reference_ids,
)


def test_user_reference_ids_preserve_first_occurrence_and_ignore_ordinary_text():
    texts = [
        "我与{{user:member_b}}和{{user:member_a}}见面，用户和小舟是普通文本。",
        "{{user:member_b}}提到了{{user:member_c}}。",
    ]
    assert user_reference_ids(texts) == ("member_b", "member_a", "member_c")
    assert texts[1] == "{{user:member_b}}提到了{{user:member_c}}。"


def test_mapping_occurs_once_and_does_not_rewrite_aliases_or_nicknames():
    replacements = {"member_a": "member_b", "member_b": "member_c"}
    text = "{{user:member_a}}与{{user:member_b}}，小舟、用户和member_a是原话。"
    assert map_user_references(text, replacements.__getitem__) == (
        "{{user:member_b}}与{{user:member_c}}，小舟、用户和member_a是原话。"
    )


def test_rendering_is_one_pass_and_keeps_plain_names_and_user_word():
    text = "我和{{user:a}}、{{user:b}}见面；用户说仍叫小舟。"
    names = {"a": "{{user:b}}", "b": "阿岚"}
    original_names = dict(names)
    assert render_memory_text(text, names) == "我和{{user:b}}、阿岚见面；用户说仍叫小舟。"
    assert names == original_names
    assert text == "我和{{user:a}}、{{user:b}}见面；用户说仍叫小舟。"


def test_missing_or_empty_nickname_has_explicit_identity_fallback():
    assert render_memory_text("{{user:a}}", {}) == "未命名成员（a）"
    assert render_memory_text("{{user:a}}") == "未命名成员（a）"
    assert render_memory_text("{{user:a}}", {"a": ""}) == "未命名成员（a）"
    assert render_memory_text("没有占位符的用户原话。") == "没有占位符的用户原话。"


def test_item_rendering_adds_display_fields_without_mutating_stored_content():
    item = {
        "node_id": "node_1", "content_text": "我记得{{user:a}}的演奏。",
        "description": "{{user:a}}喜欢乐器。", "state_text": "我期待与{{user:a}}再见。",
        "event_time_text": "{{user:a}}所说的去年", "tags": ["演奏者", "{{user:a}}"],
        "source_ids": ["src_1"], "extra": {"text": "{{user:a}}"},
    }
    original = copy.deepcopy(item)
    names = {"a": "小舟"}
    rendered = render_memory_item(item, names)
    assert item == original
    for key, value in original.items():
        assert rendered[key] == value
    assert rendered["display_content_text"] == "我记得小舟的演奏。"
    assert rendered["display_description"] == "小舟喜欢乐器。"
    assert rendered["display_state_text"] == "我期待与小舟再见。"
    assert rendered["display_event_time_text"] == "小舟所说的去年"
    assert rendered["display_tags"] == ["演奏者", "小舟"]
    assert rendered["extra"] == {"text": "{{user:a}}"}
    assert memory_texts(item) == [
        original["content_text"], original["description"], original["state_text"],
        original["event_time_text"], *original["tags"],
    ]


def test_optional_text_fields_are_not_invented():
    item = {"content_text": None, "source_ids": ["src_1"]}
    assert memory_texts(item) == []
    assert render_memory_item(item, {}) == item
