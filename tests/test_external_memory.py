from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace

import pytest

from core.text_cleaning import DEFAULT_CLEANER

from tests.test_recall_runtime import plugin_factory
from core.memory_spaces import private_space
from core.retrieval import RecallResult

from adapters.external_memory import (
    PROVIDER_API_VERSION,
    PROVIDER_ID,
    ExternalMemoryAdapter,
    ExternalMemoryContractError,
    discover_companion_api,
    parse_external_scope,
)


def companion_context(*, kind="private", account="user-1", role="persona-a"):
    return {
        "scope": {
            "installation_id": "install-a",
            "platform": "qq",
            "kind": kind,
            "native_scope_id": "user-1" if kind == "private" else "group-1",
            "bot_id": "bot-a",
            "account_id": "bot-account",
            "conversation_id": "conversation-a",
            "session_id": "session-a",
        },
        "role": {"installation_id": "install-a", "subject_id": role},
        "principal": {"installation_id": "install-a", "native_account_id": account},
        "user": {"installation_id": "install-a", "subject_id": account},
    }


def request(*, query="咖啡", kind="private"):
    return {"schema": PROVIDER_API_VERSION, "purpose": "recall", "query": query,
            "limit": 4, "max_chars": 2000, "context": companion_context(kind=kind)}


class FakeAPI:
    memory_provider_api_version = PROVIDER_API_VERSION

    def __init__(self):
        self.calls = []
        self.unregistered = []

    def register_memory_provider(self, provider_id, **kwargs):
        self.calls.append((provider_id, kwargs))
        return "registration-1"

    def unregister_memory_provider(self, registration):
        self.unregistered.append(registration)
        return True


class FakeCompanionStar:
    activated = True
    root_dir_name = "astrbot_plugin_private_companion"
    name = "陪伴插件"

    def __init__(self, api):
        self.star_cls = SimpleNamespace(extension_api=api)


class FakeContext:
    def __init__(self, api):
        self.api = api

    def get_all_stars(self):
        return [FakeCompanionStar(self.api)]


class FakeDatabase:
    def __init__(self, rows):
        self.rows = iter(rows)

    async def fetchone(self, query, params):
        del query, params
        return next(self.rows)


class FakePlugin:
    def __init__(self, *, mode="supplement", enabled=True):
        api = FakeAPI()
        self.context = FakeContext(api)
        self.config = SimpleNamespace(
            external_memory_enabled=enabled,
            external_memory_mode=mode,
            external_memory_priority=3,
            external_memory_timeout_seconds=4.0,
        )
        self.ready = True
        self.terminating = False
        self.source_store = SimpleNamespace(cleaner=DEFAULT_CLEANER)
        self.memory_store = SimpleNamespace()
        self.runtime = SimpleNamespace()
        self.ingress = SimpleNamespace()
        self.api = api

    async def submit_sources(self, sources):
        return await self.ingress.submit(sources[0])


def test_parse_scope_rejects_untrusted_or_non_private_context():
    scope = parse_external_scope(companion_context())
    assert scope.persona_id == "persona-a"
    assert scope.platform_instance_id == "qq"
    assert scope.account_id == "user-1"
    with pytest.raises(ExternalMemoryContractError):
        parse_external_scope(companion_context(kind="group"), require_private=True)
    invalid = companion_context()
    invalid["role"] = {"subject_id": ""}
    with pytest.raises(ExternalMemoryContractError):
        parse_external_scope(invalid)
    invalid = companion_context()
    invalid["role"] = {"subject_id": "persona-a", "installation_id": "other-install"}
    with pytest.raises(ExternalMemoryContractError):
        parse_external_scope(invalid)
    invalid = companion_context(account="user-2")
    with pytest.raises(ExternalMemoryContractError):
        parse_external_scope(invalid)


def test_discover_companion_api_requires_active_public_version():
    api = FakeAPI()
    assert discover_companion_api(FakeContext(api)) is api
    api.memory_provider_api_version = "other"
    assert discover_companion_api(FakeContext(api)) is None


def test_connect_registers_contract_and_disconnects():
    plugin = FakePlugin(mode="owner")
    adapter = ExternalMemoryAdapter(plugin)
    assert adapter.connect() is True
    assert plugin.api.calls[0][0] == PROVIDER_ID
    values = plugin.api.calls[0][1]
    assert values["handles_model_memory"] is True
    assert values["priority"] == 3
    assert adapter.connect() is True
    adapter.disconnect()
    assert plugin.api.unregistered == ["registration-1"]


def test_connect_refreshes_a_reloaded_companion_api():
    plugin = FakePlugin()
    adapter = ExternalMemoryAdapter(plugin)
    assert adapter.connect() is True
    previous = plugin.api
    plugin.api = FakeAPI()
    plugin.context.api = plugin.api
    assert adapter.connect() is True
    assert previous.unregistered == ["registration-1"]
    assert plugin.api.calls[0][0] == PROVIDER_ID


def test_only_standard_registration_arguments_are_used_even_with_other_host_capabilities():
    class CompanionAPI(FakeAPI):
        def register_memory_provider(self, provider_id, *, plugin, read, write, summary, display_name,
                                     priority, handles_model_memory, is_active, timeout_seconds):
            assert callable(read) and callable(write) and callable(summary)
            return "standard-registration"

        async def list_delivered_proactive_messages(self):
            pytest.fail("the writer does not poll for platform receipts")

    plugin = FakePlugin()
    plugin.context.api = CompanionAPI()
    adapter = ExternalMemoryAdapter(plugin)
    assert adapter.connect() and adapter.registration_id == "standard-registration"


def test_unrelated_registration_type_error_is_not_retried_as_legacy():
    class BrokenAPI(FakeAPI):
        async def list_delivered_proactive_messages(self):
            pytest.fail("registration failed")

        def register_memory_provider(self, provider_id, **options):
            self.calls.append((provider_id, options))
            raise TypeError("unexpected callback type")

    plugin = FakePlugin()
    plugin.context.api = api = BrokenAPI()
    adapter = ExternalMemoryAdapter(plugin)
    assert not adapter.connect()
    assert len(api.calls) == 1
    assert "TypeError" in adapter.registration_issue


@pytest.mark.asyncio
async def test_read_uses_resolved_private_space_and_whole_nodes():
    plugin = FakePlugin()
    plugin.source_store.resolve_account = _async_resolved
    plugin.memory_store.get_space = _async_space
    plugin.runtime.recall = SimpleNamespace(recall=lambda space_id, query: _recall(space_id, query))
    adapter = ExternalMemoryAdapter(plugin)
    result = await adapter.read(request())
    assert result["items"] == [{"id": "node-1", "content": "每天只喝咖啡", "source": "neomemory"}]


@pytest.mark.asyncio
async def test_external_record_contains_only_nodes_returned_under_companion_budget(plugin_factory):
    plugin = await plugin_factory()
    plugin.source_store.resolve_account = _async_resolved
    space = await plugin.memory_store.get_or_create_space(private_space("persona-a", _resolved("qq", "user-1")))
    history = plugin.runtime.recall_history
    await history.set_enabled(True)

    async def recall(space_id, query):
        result = RecallResult(space_id)
        result.hits = [{"node_id": key, "node_type": "atomic", "content_text": text}
                       for key, text in (("large", "A" * 2500), ("small", "简短的内容"))]
        return result

    plugin.runtime.recall = SimpleNamespace(recall=recall)
    items = (await ExternalMemoryAdapter(plugin).read(request()))["items"]
    assert [row["id"] for row in items] == ["small"]
    record, = await history.list(space["space_id"])
    assert record["mode"] == "external" and record["text"] == "简短的内容"
    assert [row["node_id"] for row in record["items"]] == ["small"]


@pytest.mark.asyncio
async def test_read_rejects_group_and_does_not_guess_scope():
    plugin = FakePlugin()
    adapter = ExternalMemoryAdapter(plugin)
    with pytest.raises(ExternalMemoryContractError):
        await adapter.read(request(kind="group"))


@pytest.mark.asyncio
async def test_write_is_idempotent_and_preserves_source_event_boundary():
    plugin = FakePlugin()
    plugin.source_store.get_records = _missing_records
    captured = []

    async def submit(source):
        captured.append(source)
        return SimpleNamespace(state="captured")

    plugin.ingress.submit = submit
    adapter = ExternalMemoryAdapter(plugin)
    record = {"schema": PROVIDER_API_VERSION, "source_plugin": "companion",
              "memory_id": "life-1", "kind": "persona_life", "content": "今天整理了房间",
              "context": companion_context(), "tags": ["整理"]}
    result = await adapter.write(record)
    assert result == captured[0].source_id
    assert len(captured) == 1
    assert captured[0].source_type == "source_event"
    assert captured[0].actor_kind == "external"
    assert captured[0].identity_pending is True

    source = captured[0]
    plugin.source_store.get_records = lambda ids: _existing_records(ids, source)
    assert await adapter.write(record) == source.source_id
    assert len(captured) == 1


@pytest.mark.asyncio
async def test_write_rejects_conflicting_content_for_same_source_id():
    plugin = FakePlugin()
    source = ExternalMemoryAdapter._external_source(
        {"source_plugin": "companion", "memory_id": "life-1", "kind": "persona_life",
         "content": "旧内容"}, parse_external_scope(companion_context()))
    plugin.source_store.get_records = lambda ids: _existing_records(ids, source)
    plugin.ingress.submit = lambda value: pytest.fail("conflict must not be submitted")
    adapter = ExternalMemoryAdapter(plugin)
    with pytest.raises(ExternalMemoryContractError, match="conflicts"):
        await adapter.write({"schema": PROVIDER_API_VERSION, "source_plugin": "companion",
                             "memory_id": "life-1", "kind": "persona_life", "content": "新内容",
                             "context": companion_context()})


@pytest.mark.asyncio
async def test_write_rejects_same_id_for_another_private_account():
    plugin = FakePlugin()
    source = ExternalMemoryAdapter._external_source(
        {"source_plugin": "companion", "memory_id": "life-1", "kind": "persona_life",
         "content": "同一内容"}, parse_external_scope(companion_context(account="user-1")))
    plugin.source_store.get_records = lambda ids: _existing_records(ids, source)
    adapter = ExternalMemoryAdapter(plugin)
    other_context = companion_context(account="user-2")
    other_context["scope"]["native_scope_id"] = "user-2"
    with pytest.raises(ExternalMemoryContractError, match="conflicts"):
        await adapter.write({"schema": PROVIDER_API_VERSION, "source_plugin": "companion",
                             "memory_id": "life-1", "kind": "persona_life", "content": "同一内容",
                             "context": other_context})


@pytest.mark.asyncio
async def test_write_rejects_non_json_metadata_before_ingress():
    plugin = FakePlugin()
    plugin.ingress.submit = lambda value: pytest.fail("invalid metadata must not be submitted")
    adapter = ExternalMemoryAdapter(plugin)
    with pytest.raises(ExternalMemoryContractError, match="JSON"):
        await adapter.write({"schema": PROVIDER_API_VERSION, "source_plugin": "companion",
                             "memory_id": "life-1", "kind": "persona_life", "content": "内容",
                             "metadata": {"bad": object()}, "context": companion_context()})


@pytest.mark.asyncio
async def test_summary_returns_counts_without_exposing_content():
    plugin = FakePlugin()
    plugin.source_store.resolve_account = _async_resolved
    plugin.memory_store.get_space = _async_space
    plugin.memory_store.get_active_generation = _async_generation
    plugin.memory_store.database = FakeDatabase([{"count": 2}, {"count": 1}, {"count": 5}])
    result = await ExternalMemoryAdapter(plugin).summary({
        "schema": PROVIDER_API_VERSION, "purpose": "summary", "context": companion_context()
    })
    assert result == {"counts": {"profile": 2, "preference": 0, "relationship": 1,
                                  "private_conversation": 5, "other": 0, "total": 8}}


def _resolved(platform, account):
    return f"logical:{platform}:{account}"


def _space(scope_key):
    return {"space_id": "space-1", "scope_key": scope_key, "logical_user_id": scope_key.split(':user:', 1)[1]}


async def _async_resolved(platform, account, platform_name=None):
    return _resolved(platform, account)


async def _async_space(scope_key):
    return _space(scope_key)


async def _async_generation(space_id):
    del space_id
    return {"generation_id": "gen-1"}


async def _recall(space_id, query):
    return SimpleNamespace(hits=[{"node_id": "node-1", "content_text": "每天只喝咖啡"}],
                           space_id=space_id, query=query, diagnostics={})


async def _missing_records(ids):
    del ids
    raise ValueError("unknown source")


async def _existing_records(ids, source):
    del ids
    return [{"source_hash": hashlib.sha256(source.content_text.encode()).hexdigest(),
             "persona_id": source.persona_id,
             "platform_instance_id": source.identity.platform_instance_id,
             "group_id": source.identity.group_id,
             "actor_account_id": source.actor_account_id,
             "source_type": source.source_type,
             "actor_kind": source.actor_kind,
             "metadata_json": json.dumps(source.metadata, ensure_ascii=False)}]


@pytest.mark.asyncio
async def test_companion_first_read_on_another_qq_instance_uses_current_account_owner(tmp_path):
    from storage.source_store import SourceStore

    plugin = FakePlugin()
    plugin.source_store = SourceStore(tmp_path / "source.db")
    await plugin.source_store.open()
    try:
        await plugin.source_store.bind_account(platform_instance_id="first", platform_name="aiocqhttp",
                                              account_id="123", logical_user_id="confirmed")
        plugin.context.get_platform_inst = lambda instance: SimpleNamespace(
            meta=lambda: SimpleNamespace(name="aiocqhttp"))
        plugin.memory_store.get_space = _async_space
        context = companion_context(account="123")
        context["scope"].update(platform="second", native_scope_id="123")
        space, _ = await ExternalMemoryAdapter(plugin)._private_space(context)
        assert space["logical_user_id"] == "confirmed"
        assert not await plugin.source_store.database.fetchone(
            "SELECT 1 FROM account_bindings WHERE platform_instance_id='second'")
    finally:
        await plugin.source_store.close()
