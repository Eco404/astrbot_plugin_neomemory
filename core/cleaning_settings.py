"""Persistent, hot-applied text cleaning settings for all memory spaces."""

import asyncio

from .settings import SettingsConflict
from .text_cleaning import TextCleaner, default_cleaning_config


def _validate_entries(entries, fields, label, *, optional=()):
    if not isinstance(entries, list):
        raise ValueError(f"{label}列表格式有误")
    ids = set()
    for entry in entries:
        if not isinstance(entry, dict) or not fields <= set(entry) <= fields.union(optional):
            raise ValueError(f"{label}字段不完整")
        identifier = entry["id"]
        if not isinstance(identifier, str) or not identifier or identifier in ids:
            raise ValueError(f"{label}标识重复或为空")
        if not isinstance(entry["name"], str) or not entry["name"].strip():
            raise ValueError(f"请填写{label}名称")
        if type(entry["enabled"]) is not bool:
            raise ValueError(f"{label}开关值有误")
        ids.add(identifier)
    return ids


def validate_rules(config):
    group_fields = {"id", "name", "enabled"}
    group_ids = _validate_entries(config["groups"], group_fields, "分组")
    _validate_entries(config["rules"], group_fields | {"group_id", "kind", "pattern", "replacement"}, "条目",
                      optional=("skip_markdown",))
    for rule in config["rules"]:
        group = rule["group_id"]
        if group is not None and (not isinstance(group, str) or group not in group_ids):
            raise ValueError("所选分组不存在")
        if rule["kind"] not in ("literal", "regex"):
            raise ValueError("未知的清理方式")
        if type(rule.get("skip_markdown", False)) is not bool:
            raise ValueError("跳过 Markdown 的开关值有误")
        if not isinstance(rule["pattern"], str) or not isinstance(rule["replacement"], str):
            raise ValueError("匹配内容和替换内容必须是文本")
        if not rule["pattern"]:
            raise ValueError("请填写匹配内容")
    if "root_order" in config:
        order = config["root_order"]
        expected = {("group", group_id) for group_id in group_ids}
        expected.update(("rule", rule["id"]) for rule in config["rules"] if rule["group_id"] is None)
        if not isinstance(order, list) or len(order) != len(expected):
            raise ValueError("一级排序必须包含全部分组和未分组条目")
        for item in order:
            if not isinstance(item, dict) or set(item) != {"kind", "id"} or (item["kind"], item["id"]) not in expected:
                raise ValueError("一级排序中存在重复或未知条目")
            expected.remove((item["kind"], item["id"]))


class CleaningSettings:
    KEY = "text_cleaning"

    def __init__(self, pipeline, sources):
        self.pipeline = pipeline
        self.sources = sources
        self.lock = asyncio.Lock()

    async def _persist(self, config):
        await self.pipeline.set_state(self.KEY, {**config, "format_version": 1}, max_bytes=None)

    async def load(self):
        stored = await self.pipeline.get_state(self.KEY)
        if stored is None:
            config = default_cleaning_config()
        else:
            if stored.pop("format_version") != 1:
                raise ValueError("unsupported text cleaning format")
            config = stored
        cleaner = TextCleaner(config)
        self.config, self.sources.cleaner = config, cleaner

    async def save(self, config):
        async with self.lock:
            if type(config["revision"]) is not int or config["revision"] != self.config["revision"]:
                raise SettingsConflict()
            validate_rules(config)
            updated = {**config, "revision": config["revision"] + 1}
            cleaner = TextCleaner(updated)
            await self._persist(updated)
            self.sources.cleaner = cleaner
            self.config = updated
            return updated
