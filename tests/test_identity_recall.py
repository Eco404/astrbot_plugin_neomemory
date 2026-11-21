"""A slow recall cannot return the previous account owner's memories."""
import pytest

from astrbot.api.provider import ProviderRequest
from core.identity import extract_event_identity
from storage.source_scan import SourceScanner
from tests.test_recall_runtime import plugin_factory, RuntimeEvent, record, seed, warm


@pytest.mark.asyncio
@pytest.mark.parametrize('active', [False, True])
async def test_binding_changed_during_recall_drops_old_owner_result(plugin_factory, monkeypatch, active):
    plugin = await plugin_factory()
    event = RuntimeEvent()
    space, _ = await seed(plugin, event, content='独立合成的旧归属旅行记忆。')
    await warm(plugin, space)
    baseline = ProviderRequest(prompt='旅行', extra_user_content_parts=[])
    await plugin.runtime.passive(event, baseline)
    assert baseline.extra_user_content_parts
    original = plugin.runtime.recall.recall
    identity = extract_event_identity(event)

    async def rebound(*args, **kwargs):
        result = await original(*args, **kwargs)
        await plugin.source_store.bind_account(platform_instance_id=identity.platform_instance_id,
            account_id=identity.account_id, logical_user_id='new-owner')
        return result

    monkeypatch.setattr(plugin.runtime.recall, 'recall', rebound)
    if active:
        result = await plugin.runtime.active_recall(event, '旅行')
        assert '绑定已变更' in result
        assert '旧归属旅行记忆' not in result
    else:
        request = ProviderRequest(prompt='旅行', extra_user_content_parts=[])
        await plugin.runtime.passive(event, request)
        assert not request.extra_user_content_parts


@pytest.mark.asyncio
async def test_onebot_instances_share_private_recall_only_within_persona(plugin_factory):
    def message(instance, platform="aiocqhttp", persona="p1", group_id=""):
        event = RuntimeEvent(message_id=instance + persona, persona=persona, group_id=group_id)
        event.get_platform_name = lambda: platform
        event.get_platform_id = lambda: instance
        event.get_sender_id = lambda: "123456"
        return event

    plugin = await plugin_factory()
    first = message("bot-a")
    space, node = await seed(plugin, first, content="我们约好周末一起登山。")
    await warm(plugin, space)

    # Recall may run before source capture on a new adapter instance.
    second = message("bot-b")
    request = ProviderRequest(prompt="周末要一起做什么？")
    await plugin.inject_memory(second, request)
    assert node["content_text"] in request.extra_user_content_parts[0].text
    await record(plugin, second)
    assert (await plugin.runtime.scope(second))[0]["space_id"] == space["space_id"]
    assert await SourceScanner(plugin.source_store).count(space) == 2

    comparisons = [(message("bot-a", persona="p2"), True), (message("tg-a", platform="telegram"), False)]
    for separate, same_user in comparisons:
        await record(plugin, separate)
        other, _ = await plugin.runtime.scope(separate, create=True)
        assert other["space_id"] != space["space_id"]
        assert (other["logical_user_id"] == space["logical_user_id"]) == same_user
        request = ProviderRequest(prompt="周末要一起做什么？")
        await plugin.inject_memory(separate, request)
        assert all(node["content_text"] not in part.text for part in request.extra_user_content_parts)

    group_a, _ = await plugin.runtime.scope(message("bot-a", group_id="g"), create=True)
    group_b, _ = await plugin.runtime.scope(message("bot-b", group_id="g"), create=True)
    assert group_a["space_id"] != group_b["space_id"]
