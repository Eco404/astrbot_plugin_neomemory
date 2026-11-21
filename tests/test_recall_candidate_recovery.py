"""Whole-node recovery after final shared-budget and freshness validation."""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("astrbot.api")

from astrbot.api.provider import ProviderRequest  # noqa: E402

from core.retrieval import render_nodes  # noqa: E402
from tests.test_recall_runtime import RuntimeEvent, record, warm  # noqa: E402
from tests.test_recall_runtime import plugin_factory as _plugin_factory  # noqa: E402

plugin_factory = _plugin_factory


async def covered_pair(plugin, *, with_profile=True):
    """Synthetic facts with an event that fits alone, but not beside a profile."""
    event = RuntimeEvent(text="回忆一下观测活动")
    source = await record(plugin, event)
    space, _ = await plugin.runtime.scope(event, create=True)
    plugin.context.embedding.response = lambda texts: [[1.0, 0.0] for _ in texts]
    atomic = await plugin.memory_store.create_node(
        space_id=space["space_id"], node_type="atomic",
        content_text="观测活动只在晴天进行。", source_ids=[source.source_id], importance=.8,
    )
    parent = await plugin.memory_store.create_node(
        space_id=space["space_id"], node_type="event",
        content_text="我记得那次观测只在晴天进行。" + "我们讨论了望远镜的准备和交通安排。" * 10,
        source_ids=[source.source_id], importance=.8,
    )
    await plugin.memory_store.add_coverage(parent["node_id"], atomic["node_id"])
    description = "喜欢观察天象，出门前会确认天气和交通情况。"
    if with_profile:
        await plugin.memory_store.upsert_profile(
            space_id=space["space_id"], logical_user_id=space["logical_user_id"],
            tags=["观测"], description=description,
        )
    await warm(plugin, space)
    return event, space, parent, atomic, description


async def injected_text(plugin, event):
    request = ProviderRequest(prompt=event.message_str)
    await plugin.inject_memory(event, request)
    if plugin.runtime.tasks:
        await asyncio.gather(*list(plugin.runtime.tasks))
    text = "\n".join(part.text for part in request.extra_user_content_parts)
    assert len(text.encode("utf-8")) <= plugin.runtime.config.recall_budget_tokens
    return text


async def credited_ids(plugin):
    rows = await plugin.memory_store.database.fetchall("SELECT node_id FROM recall_reinforcements")
    return {row["node_id"] for row in rows}


@pytest.mark.asyncio
@pytest.mark.parametrize("node_limit", [1, 6])
async def test_profile_excludes_covering_event_and_recovers_complete_atomic(plugin_factory, node_limit):
    plugin = await plugin_factory({"context_budget_priority": "context_first", "recall_budget_tokens": 1000,
                                   "recall_max_nodes": node_limit})
    event, space, parent, atomic, description = await covered_pair(plugin)
    preliminary = await plugin.runtime.recall.recall(space["space_id"], event.message_str)
    assert [item["node_id"] for item in preliminary.hits] == [parent["node_id"]]
    assert len(render_nodes([parent]).encode("utf-8")) <= 1000
    embedding_calls = len(plugin.context.embedding.calls)

    text = await injected_text(plugin, event)

    assert description in text
    assert atomic["node_id"] in text and atomic["content_text"] in text
    assert parent["node_id"] not in text
    assert await credited_ids(plugin) == {atomic["node_id"]}
    assert (await plugin.memory_store.get_node(parent["node_id"]))["reinforcement"] == 0
    assert plugin.context.calls == []
    assert len(plugin.context.embedding.calls) == embedding_calls


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["disabled", "content", "generation"])
async def test_stale_atomic_candidate_is_never_recovered(plugin_factory, monkeypatch, mutation):
    plugin = await plugin_factory({"context_budget_priority": "context_first", "recall_budget_tokens": 1000})
    event, space, parent, atomic, description = await covered_pair(plugin)
    original = plugin.runtime.context_store.revalidate

    async def mutate(snapshot, **kwargs):
        if mutation == "disabled":
            await plugin.memory_store.update_node_status(atomic["node_id"], "disabled")
        elif mutation == "content":
            await plugin.memory_store.update_node_content(atomic["node_id"], "活动已改为室内讲座。")
        else:
            target = await plugin.memory_store.create_generation(space["space_id"])
            await plugin.memory_store.activate_generation(space["space_id"], target["generation_id"])
        return await original(snapshot, **kwargs)

    monkeypatch.setattr(plugin.runtime.context_store, "revalidate", mutate)
    text = await injected_text(plugin, event)

    assert atomic["node_id"] not in text and parent["node_id"] not in text
    assert atomic["content_text"] not in text and "活动已改为室内讲座。" not in text
    assert (description in text) == (mutation != "generation")
    assert await credited_ids(plugin) == set()


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["disabled", "content", "stale"])
async def test_invalidated_covering_event_does_not_hide_valid_atomic(plugin_factory, monkeypatch, mutation):
    plugin = await plugin_factory({"recall_budget_tokens": 2400})
    event, _, parent, atomic, _ = await covered_pair(plugin, with_profile=False)
    original = plugin.runtime.context_store.revalidate

    async def mutate(snapshot, **kwargs):
        if mutation == "disabled":
            await plugin.memory_store.update_node_status(parent["node_id"], "disabled")
        elif mutation == "content":
            await plugin.memory_store.update_node_content(parent["node_id"], "我重新确认过活动安排。")
        else:
            async with plugin.memory_store.database.transaction() as connection:
                await connection.execute("UPDATE memory_nodes SET metadata_json=? WHERE node_id=?",
                                         ('{"summary_stale":true}', parent["node_id"]))
        return await original(snapshot, **kwargs)

    monkeypatch.setattr(plugin.runtime.context_store, "revalidate", mutate)
    text = await injected_text(plugin, event)

    assert atomic["node_id"] in text and atomic["content_text"] in text
    assert parent["node_id"] not in text and "我重新确认过活动安排。" not in text
    assert await credited_ids(plugin) == {atomic["node_id"]}


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["partial", "delete"])
async def test_final_validation_reloads_coverage_even_without_content_version_change(plugin_factory, monkeypatch, mutation):
    plugin = await plugin_factory({"recall_budget_tokens": 2400})
    event, space, parent, atomic, _ = await covered_pair(plugin, with_profile=False)
    preliminary = await plugin.runtime.recall.recall(space["space_id"], event.message_str)
    assert [item["node_id"] for item in preliminary.hits] == [parent["node_id"]]
    original = plugin.runtime.context_store.revalidate

    async def mutate(snapshot, **kwargs):
        if mutation == "partial":
            await plugin.memory_store.add_coverage(parent["node_id"], atomic["node_id"], coverage_kind="partial")
        else:
            async with plugin.memory_store.database.transaction() as connection:
                await connection.execute("DELETE FROM node_coverage WHERE event_node_id=? AND atomic_node_id=?",
                                         (parent["node_id"], atomic["node_id"]))
        return await original(snapshot, **kwargs)

    monkeypatch.setattr(plugin.runtime.context_store, "revalidate", mutate)
    text = await injected_text(plugin, event)

    assert parent["node_id"] in text and atomic["node_id"] in text
    assert parent["content_text"] in text and atomic["content_text"] in text
    assert await credited_ids(plugin) == {parent["node_id"], atomic["node_id"]}


@pytest.mark.asyncio
async def test_fitting_event_keeps_coverage_deduplication_and_is_the_only_reinforced_node(plugin_factory):
    plugin = await plugin_factory({"recall_budget_tokens": 2400})
    event, _, parent, atomic, description = await covered_pair(plugin)

    text = await injected_text(plugin, event)

    assert description in text and parent["node_id"] in text
    assert atomic["node_id"] not in text
    assert await credited_ids(plugin) == {parent["node_id"]}
    assert (await plugin.memory_store.get_node(atomic["node_id"]))["reinforcement"] == 0
