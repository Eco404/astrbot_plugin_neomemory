"""Live configuration preserves running work and persistent store ownership."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

pytest.importorskip("astrbot.api")

from astrbot.api.provider import ProviderRequest

from core.memory_spaces import private_space
from tests.test_external_memory import (
    FakeAPI,
    FakeCompanionStar,
    companion_context,
)
from tests.test_external_memory import request as external_request
from tests.test_maintenance_engine import output
from tests.test_providers import FakeEmbedding
from tests.test_recall_runtime import (
    RuntimeContext,
    RuntimeEvent,
    record,
    seed,
    warm,
)
from tests.test_recall_runtime import plugin_factory as _plugin_factory
from tests.test_source_store import make_source

plugin_factory = _plugin_factory


class LiveContext(RuntimeContext):
    def __init__(self):
        super().__init__()
        self.next_embedding = FakeEmbedding()
        self.next_embedding.provider_config = {**self.next_embedding.provider_config,
                                              "id": "embedding-next", "embedding_model": "embed-v2"}
        self.next_embedding.model = "embed-v2"
        self.next_chat = SimpleNamespace(provider_config={"id": "chat-next"}, get_model=lambda: "chat-v2")
        self.persona_manager = SimpleNamespace(get_persona_v3_by_id=lambda _: {"prompt": "记录有依据的经历。"})

    def get_provider_by_id(self, provider_id):
        return {"embedding-next": self.next_embedding, "chat-next": self.next_chat}.get(
            provider_id, super().get_provider_by_id(provider_id))

    def get_all_providers(self):
        return [self.chat, self.next_chat]

    def get_all_embedding_providers(self):
        return [self.embedding, self.next_embedding]


async def finished(plugin, job_id):
    async with asyncio.timeout(3):
        while True:
            job = await plugin.jobs.get(job_id)
            if job["state"] in {"succeeded", "failed", "cancelled"}:
                return job
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_runtime_apply_keeps_stores_and_updates_models_and_limits(plugin_factory):
    context = LiveContext()
    plugin = await plugin_factory({"automatic_extraction_enabled": False}, context=context)
    source, memory, runtime = plugin.source_store, plugin.memory_store, plugin.runtime
    cache, adapter, recall = runtime.cache, runtime.adapter, runtime.recall
    runner, poller, ingress_task = plugin.maintenance.runner, plugin.maintenance.runner._poller, plugin.ingress._task
    config = replace(plugin.config, llm_provider_id="chat-next", short_term_provider_id="chat-test",
        embedding_provider_id="embedding-next", embedding_batch_size=2, embedding_revision="second",
        history_limit=3, recall_deadline_seconds=8.0, recall_max_nodes=4,
        llm_concurrency=3, embedding_concurrency=1, job_concurrency=3,
        job_lease_seconds=45.0, job_poll_seconds=0.1, cache_message_limit=17,
        cache_node_limit=19, cache_message_ttl_seconds=40, cache_node_ttl_seconds=50,
        ingress_max_entries=23, ingress_max_bytes=4096, ingress_max_attempts=4,
        ingress_retry_seconds=2.0, ingress_poll_seconds=0.1)
    await plugin.apply_config(config)
    assert plugin.ready and plugin.config is config
    assert plugin.source_store is source and plugin.memory_store is memory and plugin.runtime is runtime
    assert runtime.cache is cache and runtime.adapter is adapter and runtime.recall is not recall
    assert plugin.maintenance.runner is runner and runner._poller is poller and plugin.ingress._task is ingress_task
    assert runtime.recall.policy.history_limit == 3 and runtime.recall.policy.deadline_seconds == 8.0
    assert runtime.recall.policy.max_nodes == 4 and runtime.recall.embeddings.batch_size == 2
    assert runtime.recall.embeddings.provider_id == "embedding-next" and runtime.recall.embeddings.revision == "second"
    assert plugin.maintenance.config is config and plugin.short_term.config is config
    assert plugin.short_term._provider_id() == "chat-test"
    assert cache.limits == {"message": 17, "node": 19} and cache.ttls == {"message": 40, "node": 50}
    assert (runner.concurrency, runner.lease_seconds, runner.poll_seconds) == (3, 45.0, 0.1)
    assert (plugin.ingress_store.max_entries, plugin.ingress_store.max_bytes) == (23, 4096)
    assert (plugin.ingress.max_attempts, plugin.ingress.retry_seconds, plugin.ingress.poll_seconds) == (4, 2.0, 0.1)
    await runtime.recall.embeddings.embed(["first", "second", "third"], namespace="live-model-selection")
    assert sorted(map(len, context.next_embedding.calls)) == [1, 2] and not context.embedding.calls
    assert await adapter.generate([{"role": "user", "content": "probe"}], provider_id=plugin.config.llm_provider_id)
    assert context.calls[-1]["chat_provider_id"] == "chat-next"


@pytest.mark.asyncio
async def test_legacy_enabled_setting_is_ignored(plugin_factory):
    plugin = await plugin_factory({"enabled": False, "automatic_extraction_enabled": False})
    assert plugin.ready
    assert "enabled" not in plugin.config.as_dict()
    await record(plugin, RuntimeEvent(message_id="legacy-flag", text="stored record"))
    assert (await plugin.source_store.health())["source_count"] == 1


@pytest.mark.asyncio
async def test_extraction_inflight_keeps_original_model_until_commit(plugin_factory):
    context = LiveContext()
    context.chat_response = json.dumps(output())
    context.chat_gate = asyncio.Event()
    plugin = await plugin_factory({"automatic_extraction_enabled": False, "embedding_provider_id": "",
        "llm_provider_id": "chat-test", "private_min_messages": 1, "private_topic_messages": 2,
        "private_max_messages": 3, "job_poll_seconds": 0.05}, context=context)
    event = RuntimeEvent(message_id="first-extract", text="an independent synthetic observation")
    await record(plugin, event)
    space, _ = await plugin.runtime.scope(event, create=True)
    first = await plugin.maintenance.enqueue("extract", space["space_id"], "live-extract-first")
    await asyncio.wait_for(context.chat_started.wait(), 2)
    await plugin.apply_config(replace(plugin.config, llm_provider_id="chat-next", extraction_model_revision="next"))
    context.chat_gate.set()
    result = await finished(plugin, first["job_id"])
    assert result["state"] == "succeeded", result
    rows = await plugin.memory_store.database.fetchall("SELECT model_id FROM extraction_batches ORDER BY activity_round")
    assert [row["model_id"] for row in rows] == ["chat-test"]
    await record(plugin, RuntimeEvent(message_id="second-extract", text="a later independent observation"))
    second = await plugin.maintenance.enqueue("extract", space["space_id"], "live-extract-second")
    assert (await finished(plugin, second["job_id"]))["state"] == "succeeded"
    assert [call["chat_provider_id"] for call in context.calls] == ["chat-test", "chat-next"]
    rows = await plugin.memory_store.database.fetchall("SELECT model_id FROM extraction_batches ORDER BY activity_round")
    assert [row["model_id"] for row in rows] == ["chat-test", "chat-next"]


@pytest.mark.asyncio
async def test_reindex_inflight_keeps_original_embedding_for_all_batches(plugin_factory):
    context = LiveContext()
    plugin = await plugin_factory({"automatic_extraction_enabled": False, "embedding_batch_size": 1,
                                   "job_poll_seconds": 0.05}, context=context)
    for number in range(3):
        space, _ = await seed(plugin, RuntimeEvent(message_id=f"index-{number}"), content=f"independent event {number}")
    context.embedding.gate = asyncio.Event()
    first = await plugin.maintenance.enqueue("reindex", space["space_id"], "live-index-first")
    await asyncio.wait_for(context.embedding.started.wait(), 2)
    await plugin.apply_config(replace(plugin.config, embedding_provider_id="embedding-next", embedding_batch_size=2))
    context.embedding.gate.set()
    result = await finished(plugin, first["job_id"])
    assert result["state"] == "succeeded" and result["result"]["indexed"] == 3
    assert len(context.embedding.calls) == 3 and not context.next_embedding.calls
    assert context.embedding.cancelled == 0
    second = await plugin.maintenance.enqueue("reindex", space["space_id"], "live-index-second")
    assert (await finished(plugin, second["job_id"]))["state"] == "succeeded"
    assert sorted(map(len, context.next_embedding.calls)) == [1, 2]


@pytest.mark.asyncio
async def test_foreground_recall_finishes_with_admitted_policy(plugin_factory):
    context = LiveContext()
    plugin = await plugin_factory({"automatic_extraction_enabled": False}, context=context)
    space, node = await seed(plugin, RuntimeEvent(message_id="stored-experience"), content="a synthetic shared experience")
    await warm(plugin, space)
    context.embedding.started.clear()
    context.embedding.gate = asyncio.Event()
    request = ProviderRequest(prompt="recall the shared experience")
    pending = asyncio.create_task(plugin.inject_memory(RuntimeEvent(text="recall the shared experience"), request))
    await asyncio.wait_for(context.embedding.started.wait(), 2)
    await plugin.apply_config(replace(plugin.config, recall_enabled=False, embedding_provider_id="embedding-next"))
    assert not pending.done() and context.embedding.cancelled == 0
    context.embedding.gate.set()
    await asyncio.wait_for(pending, 2)
    assert any(node["content_text"] in part.text for part in request.extra_user_content_parts)
    next_request = ProviderRequest(prompt="next query")
    await plugin.inject_memory(RuntimeEvent(message_id="next-query", text="next query"), next_request)
    assert not next_request.extra_user_content_parts and not context.next_embedding.calls


@pytest.mark.asyncio
async def test_external_memory_registration_refreshes_live_options(plugin_factory):
    context, api = LiveContext(), FakeAPI()
    context.get_all_stars = lambda: [FakeCompanionStar(api)]
    plugin = await plugin_factory({"automatic_extraction_enabled": False}, context=context)
    assert not api.calls
    await plugin.apply_config(replace(plugin.config, external_memory_enabled=True))
    assert len(api.calls) == 1 and api.calls[-1][1]["handles_model_memory"] is False
    await plugin.apply_config(replace(plugin.config, external_memory_mode="owner",
                                      external_memory_priority=7, external_memory_timeout_seconds=9.0))
    assert len(api.calls) == 2 and api.unregistered == ["registration-1"]
    options = api.calls[-1][1]
    assert options["handles_model_memory"] is True and options["priority"] == 7 and options["timeout_seconds"] == 9.0
    await plugin.apply_config(replace(plugin.config, external_memory_enabled=False))
    assert len(api.unregistered) == 2 and not plugin.external_memory.is_active()


@pytest.mark.asyncio
@pytest.mark.parametrize("next_provider", ["embedding-next", ""])
async def test_external_read_keeps_admitted_embedding_during_scope_resolution(plugin_factory, monkeypatch, next_provider):
    context = LiveContext()
    plugin = await plugin_factory({"automatic_extraction_enabled": False}, context=context)
    space, node = await seed(plugin, RuntimeEvent(message_id="external-recall-source"), content="a synthetic shared memory")
    await warm(plugin, space)
    entered, proceed = asyncio.Event(), asyncio.Event()
    resolve = plugin.source_store.resolve_account

    async def delayed_resolve(*args, **kwargs):
        entered.set()
        await proceed.wait()
        return await resolve(*args, **kwargs)

    monkeypatch.setattr(plugin.source_store, "resolve_account", delayed_resolve)
    request = external_request(query="recall a shared memory")
    request["context"] = companion_context(account="u-1", role="persona-test")
    request["context"]["scope"].update(platform="test-platform", native_scope_id="u-1")
    pending = asyncio.create_task(plugin.external_memory.read(request))
    await asyncio.wait_for(entered.wait(), 2)
    await plugin.apply_config(replace(plugin.config, embedding_provider_id=next_provider))
    if next_provider:
        await warm(plugin, space)
    proceed.set()
    result = await asyncio.wait_for(pending, 2)
    assert [item["content"] for item in result["items"]] == [node["content_text"]]
    assert len(context.embedding.calls) == 2
    assert len(context.next_embedding.calls) == (1 if next_provider else 0)
    following = await plugin.external_memory.read(request)
    if next_provider:
        assert [item["content"] for item in following["items"]] == [node["content_text"]]
        assert len(context.next_embedding.calls) == 2
    else:
        assert following["items"] == [] and not context.next_embedding.calls
    assert len(context.embedding.calls) == 2
