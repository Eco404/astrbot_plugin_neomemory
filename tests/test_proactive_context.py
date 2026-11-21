from types import SimpleNamespace

import pytest
from astrbot.api.provider import ProviderRequest

from adapters.proactive_context import missing_deliveries
from tests.test_recall_runtime import RuntimeEvent, plugin_factory, seed


def history(*texts, contact="visit"):
    return [
        {"actor_kind": "agent", "content_text": text, "delivered_proactive": True, "proactive_contact_id": contact}
        for text in reversed(texts)
    ]


@pytest.mark.parametrize("contexts,expected", [
    ([{"role": "assistant", "content": "明天见。"}], ["早上十点。"]),
    ([{"role": "user", "content": "明天见。"}], ["明天见。", "早上十点。"]),
    ([{"role": "assistant", "content": "明天见。\n早上十点。"}], []),
    ([SimpleNamespace(role="assistant", content=[SimpleNamespace(text="明天见。早上十点。")])], []),
])
def test_only_missing_delivered_parts_are_added(contexts, expected):
    assert missing_deliveries(history("明天见。", "早上十点。"), contexts) == expected


@pytest.mark.asyncio
async def test_delivered_parts_complete_next_turn_without_embedding_or_saved_host_history(plugin_factory, monkeypatch):
    plugin = await plugin_factory({"external_memory_enabled": True, "embedding_provider_id": ""})
    event = RuntimeEvent(text="好呀")
    await seed(plugin, event)
    calls = []

    async def recent(*args, **kwargs):
        calls.append(kwargs)
        return history("明天见。", "早上十点。")

    monkeypatch.setattr(plugin.source_store, "recent_conversation", recent)
    contexts = [{"role": "assistant", "content": "明天见。"}]
    request = ProviderRequest(prompt="好呀", contexts=contexts)
    await plugin.inject_memory(event, request)
    assert request.contexts == contexts
    part, = request.extra_user_content_parts
    assert part._no_save and "早上十点。" in part.text and "明天见。" not in part.text
    assert len(calls) == 1
    assert plugin.context.embedding.calls == []
