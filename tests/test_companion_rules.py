"""Editable presets keep protected text, message boundaries and independent controls."""

import pytest

from core.text_cleaning import TextCleaner, default_cleaning_config


@pytest.mark.parametrize(("text", "expected"), [
    ("甲<think>一<think>二</think>三</think>乙", "甲乙"),
    ("甲<think/>，<timer>尚未设置，<ttl>原样</ttl>", "甲，尚未设置，<ttl>原样</ttl>"),
    ("甲<TTS>[一]<tts>[二]</tts>[三]</TTS>[四]", "甲[四]"),
    ("甲<tts/>[原话]；<tts>［原样］[指令]发声</tts>[原话]", "甲[原话]；［原样］发声[原话]"),
    ("甲&#60;pc_tts&#62;[轻声]乙&#60;/pc_tts&#62;丙", "甲乙丙"),
    ("甲<pc_reaction_expression>内部内容{{split_marker}}乙", "甲\n乙"),
    ("甲<call:send>{content:乙}</call>丙}</call>丁", "甲乙丙}</call>丁"),
    ("<think>内部示例 `</think>` 后仍隐藏</think>正文", "正文"),
    ("`<tts>`[正常语句]", "`<tts>`[正常语句]"),
])
def test_nested_and_malformed_controls(text, expected):
    assert TextCleaner(default_cleaning_config()).clean(text) == expected


def test_edit_a_preset_across_message_components_and_disable_another():
    config = default_cleaning_config()
    assert all(item["kind"] == "regex" and item["pattern"] for item in config["rules"])
    by_id = {item["id"]: item for item in config["rules"]}
    by_id["companion-reaction"]["replacement"] = "（表情）"
    by_id["companion-emotion"]["enabled"] = False
    by_id["companion-inline-split"]["replacement"] = " / "
    by_id["companion-voice-cue"]["enabled"] = False
    text = ["甲<pc_reac", "tion_expression>隐藏", "</pc_reaction_expression>乙{{split_marker}}丙",
            "<tts>[轻声]<soft>晚</soft></tts>&&joy&&"]
    assert TextCleaner(config).clean_parts(text) == ["甲（表情）", "", "乙 / 丙", "[轻声]晚&&joy&&"]


def test_editable_replacement_keeps_original_markdown_in_named_and_numbered_groups():
    config = default_cleaning_config()
    config["rules"] = [dict(config["rules"][0], pattern=r"(?s)<sample>(?P<body>.*?)</sample>(.*)",
                            replacement=r"\2 | \g<body> | \1")]
    sample = "代码 `</sample>`\n> 引文也用 </sample> 举例\n原文"
    assert TextCleaner(config).clean(f"<sample>{sample}</sample>乙") == f"乙 | {sample} | {sample}"
