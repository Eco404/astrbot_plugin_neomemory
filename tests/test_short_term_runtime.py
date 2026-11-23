"""Short summaries enter through a request hook and run outside its latency path."""

import asyncio
import json
from types import SimpleNamespace

import pytest

pytest.importorskip('astrbot.api')

from astrbot.api.provider import ProviderRequest
from core.short_term_prompts import SCHEMA_VERSION
from core.source_ingest import normalize_event
from tests.test_recall_runtime import RuntimeEvent, plugin_factory as _plugin_factory

plugin_factory = _plugin_factory


@pytest.mark.asyncio
async def test_request_hook_enqueues_default_threshold_then_exposes_summary(plugin_factory):
    plugin = await plugin_factory({'llm_provider_id': 'chat-test', 'embedding_provider_id': '',
        'automatic_extraction_enabled': False, 'topic_consolidation_enabled': False})
    plugin.context.persona_manager = SimpleNamespace(get_persona_v3_by_id=lambda _: {'prompt': '我是温和的朋友。'})
    sources = []
    for index in range(16):
        source = normalize_event(RuntimeEvent(message_id=f'short-hook-{index}', text='周六出游尚未订票，先查开放时间。'),
            persona_id='persona-test', resolve_event_persona=False, host_conversation_id='short-hook-chat')
        sources.append(await plugin.source_store.capture(source))
    await plugin.maintenance.scan_once()
    plugin.context.chat_gate = asyncio.Event()
    user_id = sources[0].logical_user_id
    summary = '{{user:' + user_id + '}}周六出游尚未订票，开放时间待确认。'
    plugin.context.chat_response = json.dumps({'schema_version': SCHEMA_VERSION,
        'summary_text': summary.replace(user_id, 'u1'), 'evidence_source_ids': [sources[0].source_id]}, ensure_ascii=False)
    event = RuntimeEvent()
    request = ProviderRequest(prompt='继续说刚才的安排')
    request.conversation = SimpleNamespace(cid='short-hook-chat', persona_id='persona-test')
    await asyncio.wait_for(plugin.inject_memory(event, request), timeout=1)
    await asyncio.wait_for(plugin.context.chat_started.wait(), timeout=2)
    assert not request.extra_user_content_parts
    assert not plugin.context.chat_gate.is_set()  # Hook returned before the model.
    jobs = await plugin.jobs.list()
    assert len(jobs) == 1 and jobs[0]['kind'] == 'short_term'
    assert jobs[0]['payload']['source_ids'] == [row.source_id for row in sources[:6]]
    plugin.context.chat_gate.set()
    async with asyncio.timeout(3):
        while (await plugin.jobs.get(jobs[0]['job_id']))['state'] != 'succeeded':
            await asyncio.sleep(.01)
    space, _ = await plugin.runtime.scope(event)
    listing = await plugin.console_store.context(space['space_id'], 'short_term')
    assert len(listing['items']) == 1
    item = listing['items'][0]
    assert item['coverage']['source_count'] == 6
    assert item['summary_text'] == summary
    following = ProviderRequest(prompt='那接下来呢')
    following.conversation = request.conversation
    await plugin.inject_memory(event, following)
    assert item['display_summary_text'] in following.extra_user_content_parts[0].text
    assert following.extra_user_content_parts[0]._no_save
    await asyncio.gather(*list(plugin.runtime.tasks))
    assert len(await plugin.jobs.list()) == 1  # No duplicate work for unchanged input.
