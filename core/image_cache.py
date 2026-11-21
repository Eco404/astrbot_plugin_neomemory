"""Bounded, expiring private JPEGs for source attachments awaiting extraction."""

import asyncio
import hashlib
import io
import logging
import time
import weakref
from pathlib import Path

from PIL import Image, ImageOps

logger = logging.getLogger(__name__)


def compact_jpeg(data):
    with Image.open(io.BytesIO(data)) as original:
        image = ImageOps.exif_transpose(original)
        image.thumbnail((768, 768), Image.Resampling.LANCZOS)
        if image.mode in {"RGBA", "LA"} or "transparency" in image.info:
            rgba = image.convert("RGBA")
            rgb = Image.new("RGB", rgba.size, "white")
            rgb.paste(rgba, mask=rgba.getchannel("A"))
        else:
            rgb = image.convert("RGB")
        buffer = io.BytesIO()
        rgb.save(buffer, format="JPEG", quality=80)
        return buffer.getvalue()


async def resolve_image(reference):
    from astrbot.core.utils.media_utils import MediaResolver
    return await MediaResolver(reference, media_type="image").to_bytes()


class PrivateImageCache:
    def __init__(self, directory, *, max_bytes=128 << 20, ttl_seconds=72 * 3600, resolver=resolve_image):
        self.directory = Path(directory)
        self.max_bytes = max_bytes
        self.ttl_seconds = ttl_seconds
        self.resolver = resolver
        self._pending = {}
        self._caption_events = weakref.WeakValueDictionary()
        self._limiter = asyncio.Semaphore(4)
        self._sweeper = None

    async def open(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        self.prune()
        self._sweeper = asyncio.create_task(self._sweep())

    async def _sweep(self):
        while True:
            await asyncio.sleep(min(300, self.ttl_seconds))
            self.prune()

    def prune(self):
        files = []
        now = time.time()
        for path in self.directory.iterdir():
            if path.suffix not in {".jpg", ".txt", ".tmp"}:
                continue
            info = path.stat()
            if path.suffix == ".tmp" or info.st_mtime + self.ttl_seconds <= now:
                path.unlink(missing_ok=True)
            else:
                files.append((info.st_mtime, path, info.st_size))
        total = sum(size for _, _, size in files)
        if total > self.max_bytes:
            for _, path, size in sorted(files):
                path.unlink(missing_ok=True)
                total -= size
                if total <= self.max_bytes:
                    break

    def _path(self, source_id, slot, suffix):
        digest = hashlib.sha256(source_id.encode()).hexdigest()
        return self.directory / f"{digest}.{slot}.{suffix}"

    def stage(self, source_id, references):
        for slot, reference in references.items():
            key = source_id, slot
            if key in self._pending or self._path(*key, "jpg").exists():
                continue
            if len(self._pending) >= 128:
                break
            task = asyncio.create_task(self._load(key, reference))
            self._pending[key] = task

    async def _load(self, key, reference):
        try:
            async with self._limiter, asyncio.timeout(15):
                image = await self.resolver(reference)
                data = await asyncio.to_thread(compact_jpeg, image)
                self._write(self._path(*key, "jpg"), data)
        except Exception as exc:
            logger.debug("Private image unavailable (%s)", type(exc).__name__)
        finally:
            self._pending.pop(key, None)

    def _write(self, path, data):
        if len(data) <= self.max_bytes:
            part = path.with_suffix(".tmp")
            part.write_bytes(data)
            part.replace(path)
            self.prune()

    def expect_caption(self, source_id, event):
        self._caption_events[source_id] = event

    def set_caption(self, source_id, event, caption):
        # A slow host caption can arrive after this window has already committed.
        if self._caption_events.get(source_id) is event:
            self._caption_events.pop(source_id)
            self._write(self._path(source_id, 1, "txt"), caption.encode())

    def _read(self, path):
        if path.is_file():
            if path.stat().st_mtime + self.ttl_seconds > time.time():
                return path.read_bytes()
            path.unlink(missing_ok=True)
        return None

    async def caption(self, source_id, slot):
        data = self._read(self._path(source_id, slot, "txt"))
        return data.decode() if data else None

    async def read(self, source_id, slot):
        pending = self._pending.get((source_id, slot))
        if pending:
            # Another consumer can release an attachment after committing a window.
            await asyncio.shield(asyncio.gather(pending, return_exceptions=True))
        return self._read(self._path(source_id, slot, "jpg"))

    async def discard(self, source_id):
        await self.discard_many([source_id])

    async def discard_many(self, source_ids):
        owners = set(source_ids)
        if not owners:
            return
        for source_id in owners:
            self._caption_events.pop(source_id, None)
        tasks = [self._pending.pop(key) for key in list(self._pending) if key[0] in owners]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        prefixes = {hashlib.sha256(owner.encode()).hexdigest() for owner in owners}
        for file in self.directory.iterdir():
            if file.name.partition(".")[0] in prefixes:
                file.unlink(missing_ok=True)

    async def close(self):
        tasks = list(self._pending.values())
        if self._sweeper:
            tasks.append(self._sweeper)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._pending.clear()
        self._caption_events.clear()
