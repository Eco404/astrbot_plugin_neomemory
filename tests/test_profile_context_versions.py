"""Frozen extraction input is bound to its context policy."""
import pytest

from tests.test_maintenance_engine import environment, execute, interrupt_after_raw_saved, restart


@pytest.mark.asyncio
async def test_saved_input_is_not_reused_after_context_policy_change(environment, monkeypatch):
    from core import maintenance
    env = environment
    original = await interrupt_after_raw_saved(env, monkeypatch)
    restart(env)
    monkeypatch.setattr(maintenance, 'CONTEXT_VERSION', 'independent-test-context-policy')
    failed = await execute(env, await env.engine.jobs.claim('policy-upgrade'))
    assert failed['job_id'] == original['job_id']
    assert failed['error_code'] == 'configuration_changed'
    assert len(env.adapter.calls) == 1
    assert not await env.memory.database.fetchall('SELECT * FROM extraction_batches')
