"""AstrBot v4.29.3 integration probe for the stage-one capture boundary.

Run explicitly with ``PYTHONPATH=/workspace/astrbot/AstrBot:.`` so the probe
uses the checked-out host API rather than a possibly installed package.
"""

from __future__ import annotations

import asyncio
import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("astrbot.api")

from astrbot.api.platform import MessageType  # noqa: E402
from astrbot.api.provider import LLMResponse  # noqa: E402
from astrbot.core.star import star_manager, star_map  # noqa: E402
from astrbot.core.star.star_manager import PluginManager  # noqa: E402

import adapters.maintenance_api as maintenance_api  # noqa: E402
import main as plugin_module  # noqa: E402
from core.memory_spaces import private_space  # noqa: E402
from core.source_ingest import normalize_event  # noqa: E402
from main import NeoMemoryPlugin, SourceCaptureFilter  # noqa: E402
from storage.memory_schema import MEMORY_FORMAT  # noqa: E402
from storage.memory_store import MemoryStore  # noqa: E402
from storage.source_store import SourceStore  # noqa: E402


class FakeContext:
    def __init__(self) -> None:
        self.registered_web_apis: list[tuple] = []
        self.conversation_manager = SimpleNamespace(
            get_curr_conversation_id=self._no_conversation,
            get_conversation=self._no_conversation,
        )

    async def _no_conversation(self, *args):
        del args
        return None

    def register_web_api(self, route, handler, methods, desc):
        self.registered_web_apis.append((route, handler, methods, desc))


def assert_registered_web_apis(context):
    prefix = "/astrbot_plugin_neomemory/"
    routes = {(path, tuple(methods)) for path, _, methods, _ in context.registered_web_apis}
    assert len(context.registered_web_apis) == len(routes) == 53
    assert all(path.startswith(prefix) for path, _ in routes)
    assert (prefix + "tasks/clear-completed", ("POST",)) in routes
    assert (prefix + "memory/node-draft", ("GET",)) in routes
    assert (prefix + "memory/node-draft/discard", ("POST",)) in routes


class FakeEvent:
    def __init__(self, *, message_id: str, text: str, group_id: str = "") -> None:
        self.message_str = text
        self._group_id = group_id
        self._message_type = (
            MessageType.GROUP_MESSAGE if group_id else MessageType.FRIEND_MESSAGE
        )
        self.message_obj = SimpleNamespace(
            type=self._message_type,
            message_id=message_id,
            message=[],
            sender=SimpleNamespace(user_id="u-1", nickname="Alice"),
            self_id="bot-1",
            group_id=group_id,
        )

    @property
    def unified_msg_origin(self) -> str:
        message_type = self._message_type.value
        session = self._group_id or "u-1"
        return f"test:{message_type}:{session}"

    def get_platform_id(self) -> str:
        return "test-platform"

    def get_platform_name(self) -> str:
        return "test"

    def get_message_type(self) -> MessageType:
        return self._message_type

    def get_session_id(self) -> str:
        return "u-1"

    def get_group_id(self) -> str:
        return self._group_id

    def get_sender_id(self) -> str:
        return "u-1"

    def get_sender_name(self) -> str:
        return "Alice"

    def get_self_id(self) -> str:
        return "bot-1"

    def get_message_str(self) -> str:
        return self.message_str

    def get_extra(self, key, default=None):
        return {"persona_id": "persona-test"}.get(key, default)


@pytest.mark.asyncio
async def test_memory_store_lifecycle_and_source_validation(tmp_path, monkeypatch):
    monkeypatch.setattr(
        plugin_module.StarTools, "get_data_dir", staticmethod(lambda _name: tmp_path)
    )
    monkeypatch.setattr(plugin_module, "json_response", lambda payload: payload)
    close_order = []
    source_close, memory_close = SourceStore.close, MemoryStore.close

    async def close_source(store):
        close_order.append("source")
        await source_close(store)

    async def close_memory(store):
        close_order.append("memory")
        await memory_close(store)

    monkeypatch.setattr(SourceStore, "close", close_source)
    monkeypatch.setattr(MemoryStore, "close", close_memory)
    plugin = NeoMemoryPlugin(FakeContext(), {})
    await plugin.initialize()
    source_store, memory_store = plugin.source_store, plugin.memory_store
    assert source_store is not None and memory_store is not None
    try:
        assert plugin.ready is True
        assert memory_store.source_exists == source_store.has_source
        source = normalize_event(
            FakeEvent(message_id="memory-source", text="周末和朋友去徒步"),
            persona_id="persona-test",
        )
        assert source is not None
        captured = await source_store.capture(source)
        space = await memory_store.get_or_create_space(
            private_space("persona-test", captured.logical_user_id)
        )
        node = await memory_store.create_node(
            space_id=space["space_id"],
            node_type="event",
            content_text="用户和朋友周末徒步。",
            source_ids=[source.source_id],
        )
        with pytest.raises(ValueError, match="unknown source_id"):
            await memory_store.add_node_source(node["node_id"], "missing-source")
        health = await plugin.api_health()
        assert health["ready"] is True
        assert health["maintenance"]["extraction_prompt_version"] == "extraction-prompt.v3"
        assert health["source_count"] == 1
        assert health["memory"]["node_count"] == 1
        assert health["memory"]["schema_version"] == MEMORY_FORMAT
        assert health["memory"]["exists"] is True
    finally:
        await plugin.terminate()
    assert close_order == ["memory", "source"]
    assert plugin.ready is False
    assert plugin.source_store is None and plugin.memory_store is None
    for store in (memory_store, source_store):
        with pytest.raises(RuntimeError, match="not open"):
            _ = store.database.connection
    assert (await plugin.api_health())["ready"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("failing_store", ["source", "memory"])
@pytest.mark.parametrize("failure_kind", ["error", "cancel"])
async def test_initialization_failure_releases_open_stores(
    tmp_path, monkeypatch, failing_store, failure_kind
):
    monkeypatch.setattr(
        plugin_module.StarTools, "get_data_dir", staticmethod(lambda _name: tmp_path)
    )
    target = SourceStore if failing_store == "source" else MemoryStore
    original_open = target.open
    reached_failure = asyncio.Event()
    opened_stores = []
    close_order = []
    source_close, memory_close = SourceStore.close, MemoryStore.close

    async def close_source(store):
        close_order.append("source")
        opened_stores.append(store)
        await source_close(store)

    async def close_memory(store):
        close_order.append("memory")
        opened_stores.append(store)
        await memory_close(store)

    async def fail_open(store, **options):
        await original_open(store, **options)
        reached_failure.set()
        if failure_kind == "cancel":
            await asyncio.Future()
        raise RuntimeError("synthetic open failure")

    monkeypatch.setattr(SourceStore, "close", close_source)
    monkeypatch.setattr(MemoryStore, "close", close_memory)
    monkeypatch.setattr(target, "open", fail_open)
    plugin = NeoMemoryPlugin(FakeContext(), {})
    task = asyncio.create_task(plugin.initialize())
    await asyncio.wait_for(reached_failure.wait(), timeout=5)
    if failure_kind == "cancel":
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        with pytest.raises(RuntimeError, match="synthetic open failure"):
            await task
    assert close_order == (
        ["memory", "source"] if failing_store == "memory" else ["source"]
    )
    assert plugin.ready is False
    assert plugin.source_store is None and plugin.memory_store is None
    for store in opened_stores:
        with pytest.raises(RuntimeError, match="not open"):
            _ = store.database.connection
    await plugin.terminate()


@pytest.mark.asyncio
async def test_stage_one_host_capture_and_restart(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("ASTRBOT_ROOT", str(tmp_path / "astrbot"))
    context = FakeContext()
    plugin = NeoMemoryPlugin(context, {})
    assert_registered_web_apis(context)

    await plugin.initialize()
    event = FakeEvent(message_id="m-1", text="hello from host")
    failed_source = normalize_event(event, persona_id="persona-test")
    assert failed_source is not None

    original_capture = plugin.source_store.capture  # type: ignore[union-attr]

    async def fail_capture(source):
        del source
        raise RuntimeError("synthetic write failure")

    monkeypatch.setattr(plugin.source_store, "capture", fail_capture)
    await plugin._persist_source(failed_source)
    assert len(plugin._failed_sources) == 0
    assert (await plugin.ingress.health())["pending_count"] == 1
    monkeypatch.setattr(plugin.source_store, "capture", original_capture)
    assert await plugin.ingress.retry(failed_source.source_id) == 1
    await plugin.ingress.process_once(source_id=failed_source.source_id)
    assert (await plugin.ingress.health())["pending_count"] == 0

    assert SourceCaptureFilter().filter(event, {}) is False
    await asyncio.sleep(0.05)

    await plugin.capture_agent_response(
        event, LLMResponse(role="assistant", completion_text="agent reply", id="r-1")
    )
    group_event = FakeEvent(
        message_id="m-group", text="group observation", group_id="g-1"
    )
    assert SourceCaptureFilter().filter(group_event, {}) is False
    await asyncio.sleep(0.05)
    records = await plugin.source_store.list_records()  # type: ignore[union-attr]
    assert {item["actor_kind"] for item in records} == {"user", "agent"}
    streams = await plugin.source_store.list_streams()  # type: ignore[union-attr]
    assert any(item["group_id"] == "g-1" for item in streams)
    assert all(item["persona_id"] == "persona-test" for item in records)
    assert all(item["identity_pending"] == 0 for item in records)
    backlog = next(handler for path, handler, methods, _ in context.registered_web_apis
                   if path.endswith("/extraction/backlog") and methods == ["GET"])
    await plugin.maintenance.scan_once()
    spaces = await plugin.memory_store.database.fetchall("SELECT space_id,space_type FROM memory_spaces")
    assert {space["space_type"] for space in spaces} == {"private", "group"}
    for space in spaces:
        monkeypatch.setattr(maintenance_api, "request", SimpleNamespace(query={"space_id": space["space_id"]}))
        response = await backlog()
        expected = 2 if space["space_type"] == "private" else 1
        assert response.status_code == 200
        result = json.loads(response.body)
        assert result["pending_count"] == expected
        minimum = getattr(plugin.config, f"{space['space_type']}_min_messages")
        assert f"还差 {minimum - expected} 条" in result["hint"]
    await plugin.terminate()

    restarted = NeoMemoryPlugin(FakeContext(), {})
    await restarted.initialize()
    health = await restarted.source_store.health()  # type: ignore[union-attr]
    assert health["source_count"] == 3
    from storage.source_schema import SOURCE_FORMAT as SOURCE_SCHEMA_VERSION
    assert health["schema_version"] == SOURCE_SCHEMA_VERSION
    await restarted.terminate()


@pytest.mark.asyncio
async def test_stage_one_plugin_manager_loads_real_plugin_layout(
    tmp_path: Path, monkeypatch
):
    """Exercise the v4.29.3 loader using the same data/plugins layout as AstrBot."""

    monkeypatch.setenv("ASTRBOT_ROOT", str(tmp_path / "astrbot"))
    source_root = Path(__file__).resolve().parents[1]
    plugin_root = (
        tmp_path / "astrbot" / "data" / "plugins" / "astrbot_plugin_neomemory"
    )
    plugin_root.mkdir(parents=True)
    for filename in (
        "main.py",
        "metadata.yaml",
        "_conf_schema.json",
        "requirements.txt",
    ):
        shutil.copy2(source_root / filename, plugin_root / filename)
    for directory in ("core", "storage", "views", "adapters"):
        shutil.copytree(source_root / directory, plugin_root / directory)
    monkeypatch.syspath_prepend(str(tmp_path / "astrbot"))

    context = FakeContext()
    manager = object.__new__(PluginManager)
    manager.context = context
    manager.config = {}
    manager.plugin_store_path = str(tmp_path / "astrbot" / "data" / "plugins")
    manager.reserved_plugin_path = str(tmp_path / "astrbot" / "no-reserved")
    manager.plugin_config_path = str(tmp_path / "astrbot" / "data" / "config")
    manager._pm_lock = asyncio.Lock()
    manager.failed_plugin_dict = {}
    manager.failed_plugin_info = ""
    manager.conf_schema_fname = "_conf_schema.json"
    manager.logo_fname = "logo.png"

    async def global_get(_key, default=None):
        return default

    monkeypatch.setattr(star_manager.sp, "global_get", global_get)
    result = await manager.load(specified_dir_name="astrbot_plugin_neomemory")
    assert result == (True, None)
    metadata = next(
        value
        for key, value in star_map.items()
        if key.endswith("astrbot_plugin_neomemory.main")
    )
    assert metadata.star_cls is not None
    assert metadata.star_cls.ready is True
    assert_registered_web_apis(context)
    await manager._terminate_plugin(metadata)
    manager._cleanup_plugin_state("astrbot_plugin_neomemory", False)
