"""Delivered-receipt reconciliation across interruptions, pages and spaces."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.text_cleaning import DEFAULT_CLEANER

from adapters.proactive_sync import ProactiveSync
from core.memory_spaces import private_space
from core.jobs import RetryableJobError


class Companion:
    def __init__(self):
        self.entries = []
        self.installation = 'one'
        self.writes = []
        self.accept = True
        self.enabled = True
        self.api = self
        self.registration_id = 'writer'
        self.delivery_issue = None

    def connect(self):
        return True

    def add(self, stamp, owner='u', persona='p'):
        self.entries.append(dict(id=str(len(self.entries)+1), time=stamp, owner=owner, content='hello',
                                 metadata={'host_persona_id': persona}, context={'role': {'subject_id': 'companion-role'}}))

    async def list_delivered_proactive_messages(self, registration_id, *, after, since, until, highwater, limit):
        assert registration_id == 'writer'
        bound = len(self.entries) if highwater is None else highwater
        selected = [r for r in self.entries[after:bound] if r['time'] >= since and (until is None or r['time'] < until)]
        more = len(selected) > limit
        items = selected[:limit]
        return {'installation_id': self.installation, 'items': items, 'highwater': bound, 'has_more': more,
                'next_cursor': int(items[-1]['id']) if more else bound}

    async def delivery_space(self, item):
        return private_space(item['metadata']['host_persona_id'], item['owner'])

    def delivered_source_id(self, item):
        return item['id']

    async def accept_delivery(self, item, historical=False):
        if self.accept:
            self.writes.append((item['id'], historical))
            return item['id']


def setup():
    state = {}
    async def set_state(key, value): state[key] = value
    pipeline = SimpleNamespace(get_state=AsyncMock(side_effect=state.get), set_state=set_state)
    adapter = Companion()
    space = dict(scope_key=private_space('p','u').scope_key)
    plugin = SimpleNamespace(maintenance=SimpleNamespace(pipeline=pipeline), external_memory=adapter,
        source_store=SimpleNamespace(cleaner=DEFAULT_CLEANER, has_source=AsyncMock(side_effect=lambda i: i in ('2',))),
        ingress_store=SimpleNamespace(get=AsyncMock(side_effect=lambda i: {'state':'queued'} if i=='4' else None)),
        jobs=SimpleNamespace(save_checkpoint=AsyncMock()), memory_store=SimpleNamespace(get_space=AsyncMock(return_value=space)))
    sync = ProactiveSync(plugin,clock=lambda: 10.)
    sync.PAGE_SIZE = 1
    return sync, adapter, space, state


@pytest.mark.asyncio
async def test_auto_receipts_from_enable_only_resume_on_restart_and_after_transient_failure():
    sync, adapter, _, state = setup()
    adapter.add(5); adapter.add(12); adapter.add(15)
    await sync.configure(True)
    await sync.stop()
    assert await sync.sync_page()
    assert adapter.writes == [('2',False)]
    resume = ProactiveSync(sync.plugin, clock=lambda: 20.)
    resume.PAGE_SIZE = 1
    adapter.accept = False
    with pytest.raises(RetryableJobError):
        await resume.sync_page()
    assert state[sync.STATE_KEY]['after'] == 2
    adapter.accept = True
    assert not await resume.sync_page()
    assert adapter.writes[-1] == ('3',False)
    await resume.configure(False)
    adapter.add(18)
    await resume.configure(True)
    await resume.stop()
    assert not await resume.sync_page()
    assert len(adapter.writes) == 2


@pytest.mark.asyncio
async def test_cursor_skips_unresolved_and_default_host_personas_then_reads_supported_persona():
    sync, adapter, _, state = setup()
    for persona in (None, 'default', 'p'):
        adapter.add(11, persona=persona)
    await sync.configure(True)
    await sync.stop()
    while await sync.sync_page():
        pass
    assert state[sync.STATE_KEY]['after'] == 3
    assert adapter.writes == [('3', False)]
