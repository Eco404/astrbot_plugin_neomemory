"""Strict Web settings edits persisted by the host and applied without reload."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import math
from collections.abc import Mapping, MutableMapping
from pathlib import Path
from typing import Any

from .config import NeoMemoryConfig

_SCHEMA = json.loads((Path(__file__).resolve().parents[1] / "_conf_schema.json").read_text())
_MODEL_FIELDS = {"llm_provider_id", "short_term_provider_id", "embedding_provider_id", "rerank_provider_id"}


class SettingsConflict(RuntimeError):
    """The caller must reload the current host configuration before editing."""


class SettingsUnavailable(RuntimeError):
    """The host configuration is absent, read-only, or closing."""


class SettingsSaveError(RuntimeError):
    """Persistence failed; exception text is deliberately not exposed."""


class SettingsApplyError(RuntimeError):
    """Saved settings could not be applied and remain available for retry."""


def _revision(values: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(values, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _validate(values: dict[str, Any]) -> None:
    for name, value in values.items():
        meta = _SCHEMA[name]
        kind = meta["type"]
        if kind == "bool":
            valid = type(value) is bool
        elif kind == "int":
            valid = type(value) is int
        elif kind == "float":
            valid = type(value) in (int, float) and math.isfinite(value)
        else:
            valid = isinstance(value, str) and len(value) <= 256 and value == value.strip()
        if not valid:
            raise ValueError("invalid setting type or value")
        if "options" in meta and value not in meta["options"]:
            raise ValueError("setting outside allowed options")
        bounds = meta.get("slider")
        if bounds and not bounds["min"] <= value <= bounds["max"]:
            raise ValueError("setting outside allowed range")
    for prefix in ("private", "group"):
        if not values[f"{prefix}_min_messages"] <= values[f"{prefix}_topic_messages"] <= values[f"{prefix}_max_messages"]:
            raise ValueError("extraction thresholds must remain ordered")
    if values["short_term_min_messages"] > values["short_term_max_messages"]:
        raise ValueError("short-term thresholds must remain ordered")


class SettingsService:
    """Serialize our own saves and detect host edits since the last GET.

    Each runtime configuration is immutable and replaced after a successful
    apply. The host owns disk persistence and atomic replacement. This service
    cannot lock edits in a different AstrBot page or a different process.
    """

    def __init__(self, plugin: Any):
        self.plugin = plugin
        self._lock = asyncio.Lock()
        self._apply_error = None
        configured_path = getattr(self._host(), "config_path", None)
        self._config_path = Path(configured_path) if isinstance(configured_path, (str, Path)) else None
        # The object has just been loaded by the host. Capture its disk identity
        # once before any Web edits; request-time reads run off the event loop.
        self._disk_baseline = self._disk_digest()

    def _disk_digest(self):
        if self._config_path is None:
            return None
        try:
            digest = hashlib.sha256()
            with self._config_path.open("rb") as stream:
                while chunk := stream.read(65536):
                    digest.update(chunk)
            return digest.hexdigest()
        except OSError:
            return None

    async def _disk_changed(self):
        if self._config_path is None:
            return False
        current = await asyncio.to_thread(self._disk_digest)
        return current is None or current != self._disk_baseline

    def _host(self):
        return getattr(self.plugin, "_host_config", None)

    def _can_save(self):
        host = self._host()
        return (not getattr(self.plugin, "terminating", False)
                and isinstance(host, MutableMapping)
                and any(callable(getattr(host, name, None)) for name in ("save_config_async", "save_config")))

    def _snapshot(self):
        host = self._host()
        # Unknown fields participate in revision checks but never appear in the
        # Web payload. They may belong to a future host/plugin schema.
        return copy.deepcopy(dict(host)) if isinstance(host, Mapping) else {}

    def _result(self, *, external_change=False):
        snapshot = self._snapshot()
        values = {key: copy.deepcopy(snapshot.get(key, meta["default"])) for key, meta in _SCHEMA.items()}
        runtime = getattr(self.plugin, "config", None)
        runtime_values = runtime.as_dict() if isinstance(runtime, NeoMemoryConfig) else NeoMemoryConfig().as_dict()
        schema = copy.deepcopy(_SCHEMA)
        for key, meta in schema.items():
            # Visibility belongs to AstrBot's basic configuration form only.
            meta.pop("invisible", None)
            meta["group"] = "model" if key in _MODEL_FIELDS else "功能" if meta["type"] == "bool" else "高级"
        return {"schema": schema, "values": values, "runtime_values": runtime_values,
                "revision": _revision(snapshot), "can_save": self._can_save() and not external_change,
                "reload_required": external_change,
                "applied": not external_change and not self._apply_error and values == runtime_values,
                "apply_error": self._apply_error,
                "external_change": external_change}

    async def get(self):
        async with self._lock:
            return self._result(external_change=await self._disk_changed())

    async def drain(self):
        """Finish admitted saves and applications before plugin shutdown."""
        async with self._lock:
            pass

    async def _persist(self, changes):
        host = self._host()
        save_async = getattr(host, "save_config_async", None)
        if callable(save_async):
            return await save_async(changes)
        return await asyncio.to_thread(host.save_config, changes)

    def _restore(self, previous, changes):
        host = self._host()
        # Do not roll back a later host edit to the same field, or unrelated
        # unknown fields, if an external caller modified them while saving.
        for key, value in changes.items():
            if host.get(key) == value:
                if key in previous:
                    host[key] = previous[key]
                else:
                    host.pop(key, None)

    async def _persist_and_apply(self, previous, changes):
        if changes:
            try:
                committed = await self._persist(changes)
            except Exception:
                self._restore(previous, changes)
                raise SettingsSaveError() from None
            if committed is False:
                # A newer host snapshot won; do not apply or restore stale data.
                raise SettingsConflict()
            self._disk_baseline = await asyncio.to_thread(self._disk_digest)
        try:
            await self.plugin.apply_config(NeoMemoryConfig.from_mapping(self._snapshot()))
        except Exception:  # noqa: BLE001 - Report application failures through the retryable settings API.
            self._apply_error = "settings_apply_failed"
            raise SettingsApplyError() from None
        self._apply_error = None
        return self._result(external_change=await self._disk_changed())

    async def save(self, revision: str, changes: dict[str, Any]):
        if not isinstance(revision, str) or len(revision) != 64:
            raise ValueError("invalid settings revision")
        if not isinstance(changes, dict) or set(changes) - set(_SCHEMA):
            raise ValueError("unknown settings changes")
        async with self._lock:
            if not self._can_save():
                raise SettingsUnavailable()
            if await self._disk_changed():
                raise SettingsConflict()
            previous = self._snapshot()
            if revision != _revision(previous):
                raise SettingsConflict()
            if not changes and self._result()["applied"]:
                raise ValueError("no pending settings to apply")
            merged = {key: previous.get(key, meta["default"]) for key, meta in _SCHEMA.items()}
            merged.update(changes)
            _validate(merged)
            # Finish persistence and runtime application before releasing the
            # lock, including when the initiating browser disconnects.
            task = asyncio.create_task(self._persist_and_apply(previous, copy.deepcopy(changes)))
            cancelled = False
            try:
                while not task.done():
                    try:
                        await asyncio.shield(task)
                    except asyncio.CancelledError:
                        cancelled = True
                result = task.result()
            except Exception:
                if cancelled:
                    raise asyncio.CancelledError() from None
                raise
            if cancelled:
                raise asyncio.CancelledError()
            return result
