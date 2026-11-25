"""Daily, append-only plugin audit files. Cursor offsets refer to complete JSON lines."""

import asyncio
import json
import logging
import threading
import time
from contextlib import ExitStack
from datetime import date, datetime, timedelta

LOG_FORMAT = "plugin-log.v1-dev.1"
LEVELS = ("debug", "info", "warning", "error")
CATEGORIES = ("system", "operation", "task")
logger = logging.getLogger(__name__)


def _reverse_lines(stream, end):
    tail = b""
    while end:
        start = max(0, end - 65536)
        stream.seek(start)
        data = stream.read(end - start) + tail
        right = len(data)
        while (index := data.rfind(b"\n", 0, right)) >= 0:
            if right > index + 1:
                yield start + index + 1, data[index + 1:right]
            right = index
        tail = data[:right]
        end = start
    if tail:
        yield 0, tail


class AuditLog:
    def __init__(self, data_dir, *, clock=time.time):
        self.directory = data_dir / "logs"
        self.settings_file = data_dir / "plugin_log_settings.json"
        self.clock = clock
        self.enabled = True
        self.days = 7
        self._last_cleanup = None
        self._lock = threading.Lock()
        self._timer = None

    def _locked(self, action, *args):
        with self._lock:
            return action(*args)

    async def _run(self, action, *args):
        return await asyncio.to_thread(self._locked, action, *args)

    def _today(self):
        return datetime.fromtimestamp(self.clock()).date()

    def _files(self):
        return sorted(self.directory.glob("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9].jsonl"), reverse=True)

    def _prune(self):
        today = self._today()
        if today == self._last_cleanup:
            return
        first = (today - timedelta(days=self.days - 1)).isoformat()
        for path in self._files():
            if path.stem < first:
                path.unlink(missing_ok=True)
        self._last_cleanup = today

    def _open(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        if self.settings_file.exists():
            config = json.loads(self.settings_file.read_text(encoding="utf-8"))
            self.enabled, self.days = config["enabled"], config["days"]
        self._prune()

    async def open(self):
        await self._run(self._open)
        self._timer = asyncio.create_task(self._rollover(), name="neomemory-log-retention")

    async def _rollover(self):
        while True:
            midnight = datetime.combine(self._today() + timedelta(days=1), datetime.min.time())
            await asyncio.sleep(max(1, midnight.timestamp() - self.clock()))
            try:
                await self._run(self._prune)
            except OSError as exc:
                logger.warning("NeoMemory log retention: %s", type(exc).__name__)

    async def close(self):
        if self._timer is not None:
            self._timer.cancel()
            await asyncio.gather(self._timer, return_exceptions=True)
            self._timer = None

    def _append(self, level, category, event, details, *, setting_change=False):
        if not self.enabled and not setting_change:
            return
        self._prune()
        now = self.clock()
        path = self.directory / (datetime.fromtimestamp(now).date().isoformat() + ".jsonl")
        row = dict(format=LOG_FORMAT, time=now, level=level, category=category, event=event, details=details)
        with path.open("ab+") as stream:
            if stream.tell():
                stream.seek(-1, 2)
                if stream.read(1) != b"\n":
                    stream.write(b"\n")
            stream.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode() + b"\n")

    async def record(self, level, category, event, **details):
        try:
            await self._run(self._append, level, category, event, details)
        except OSError as exc:
            logger.warning("NeoMemory audit write: %s", type(exc).__name__)

    def _configure(self, enabled, days):
        if type(enabled) is not bool or type(days) is not int or not 1 <= days <= 30:
            raise ValueError("invalid log settings")
        config = {"enabled": enabled, "days": days}
        tmp = self.settings_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(config), encoding="utf-8")
        tmp.replace(self.settings_file)
        was_enabled = self.enabled
        self.days = days
        self._last_cleanup = None
        self.enabled = enabled
        self._prune()
        if enabled or was_enabled:
            # The switch itself is the final event when recording is disabled.
            self._append("info", "operation", "更新日志设置", config, setting_change=True)
        return config

    async def configure(self, enabled, days):
        return await self._run(self._configure, enabled, days)

    def _clear(self):
        for path in self._files():
            path.unlink(missing_ok=True)

    async def clear(self):
        await self._run(self._clear)

    async def query(self, level="", category="", day="", keyword="", cursor="", limit=50):
        if level and level not in LEVELS or category and category not in CATEGORIES:
            raise ValueError("invalid log filter")
        if day:
            date.fromisoformat(day)
        before_day, offset = "", 0
        if cursor:
            before_day, raw = cursor.split("/", 1)
            date.fromisoformat(before_day)
            offset = int(raw)
        if offset < 0 or not 1 <= limit <= 100:
            raise ValueError("invalid page")
        return await asyncio.to_thread(self._query, level, category, day, keyword.casefold(), before_day, offset, limit)

    def _query(self, level, category, day, keyword, before_day, offset, limit):
        items = []
        last = None
        with ExitStack() as opened:
            # Snapshot file lengths under the writer lock. Keyword scans can then
            # run while new tasks append without changing this page's boundaries.
            with self._lock:
                self._prune()
                result = {"items": items, "next_cursor": None, "settings": {"enabled": self.enabled, "days": self.days}}
                files = []
                for path in self._files():
                    if (day and path.stem != day) or (before_day and path.stem > before_day):
                        continue
                    stream = opened.enter_context(path.open("rb"))
                    end = stream.seek(0, 2)
                    files.append((path.stem, stream, min(offset, end) if path.stem == before_day else end))
            for file_day, stream, end in files:
                for pos, line in _reverse_lines(stream, end):
                    try:
                        row = json.loads(line)
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        continue  # A process interruption can leave the last append incomplete.
                    if level and row["level"] != level or category and row["category"] != category:
                        continue
                    if keyword and keyword not in (row["event"] + " " + json.dumps(row["details"], ensure_ascii=False)).casefold():
                        continue
                    if len(items) == limit:
                        result["next_cursor"] = last
                        return result
                    items.append(row)
                    last = f"{file_day}/{pos}"
        return result
