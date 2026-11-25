import asyncio
from datetime import datetime, timedelta

import pytest

from storage.audit_log import AuditLog


@pytest.mark.asyncio
async def test_rollover_retention_pause_restart_and_clear(tmp_path):
    now = datetime(2026, 3, 12, 12)
    audit = AuditLog(tmp_path, clock=lambda: now.timestamp())
    await audit.open()
    try:
        for _ in range(8):
            await audit.record("info", "system", "启动")
            now += timedelta(days=1)
        assert len(list(audit.directory.glob("*.jsonl"))) == 7
        now -= timedelta(days=1)
        await audit.configure(False, 2)
        assert len(list(audit.directory.glob("*.jsonl"))) == 2
        await audit.record("error", "task", "暂停期间")
        assert not (await audit.query(keyword="暂停期间"))["items"]
        with pytest.raises(ValueError):
            await audit.configure(True, 31)
    finally:
        await audit.close()

    reopened = AuditLog(tmp_path, clock=lambda: now.timestamp())
    await reopened.open()
    try:
        result = await reopened.query()
        assert result["settings"] == {"enabled": False, "days": 2}
        assert result["items"][0]["event"] == "更新日志设置"
        assert result["items"][0]["format"] == "plugin-log.v1"
        await reopened.clear()
        assert not list(reopened.directory.iterdir())
        assert (await reopened.query())["settings"] == result["settings"]
        await reopened.configure(True, 1)
        await reopened.record("debug", "task", "开始执行")
        assert (await reopened.query(level="debug"))["items"][0]["event"] == "开始执行"
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_combined_filters_and_stable_paging_across_files_and_concurrent_appends(tmp_path):
    now = datetime(2026, 3, 10, 12)
    audit = AuditLog(tmp_path, clock=lambda: now.timestamp())
    await audit.open()
    try:
        await audit.record("warning", "task", "等待", job_id="test-42")
        now += timedelta(days=1)
        await audit.record("info", "system", "启动")
        await asyncio.gather(*(audit.record("warning", "task", "重试", job_id="test-42", attempt=i) for i in range(650)))
        filters = dict(level="warning", category="task", keyword="TEST-42", limit=47)
        page = await audit.query(**filters)
        items = page["items"].copy()
        await audit.record("warning", "task", "稍后追加", job_id="test-42")
        while page["next_cursor"]:
            page = await audit.query(**filters, cursor=page["next_cursor"])
            items.extend(page["items"])
        assert len(items) == 651 and items[-1]["event"] == "等待"
        assert {r["details"]["attempt"] for r in items[:-1]} == set(range(650))
        assert (await audit.query(day="2026-03-10"))["items"] == items[-1:]
    finally:
        await audit.close()
