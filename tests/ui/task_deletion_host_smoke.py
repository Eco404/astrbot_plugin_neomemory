"""Real host routes and Chromium delete a failed, persisted synthetic rebuild.

Uses a controlled in-process provider and an injected index-stage failure to
create actual draft commits, a rejected candidate and a durable checkpoint.
No external providers, live accounts or production databases are accessed.
"""
import asyncio
from dataclasses import replace
import json
from pathlib import Path
import socket
import sys
import tempfile
from unittest.mock import patch

import uvicorn
from playwright.async_api import async_playwright, expect

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core.jobs import ReviewRequired
from core.maintenance import MaintenanceEngine
from storage.cache_store import text_hash
from tests.host_console_harness import HostConsoleHarness
from tests.test_maintenance_engine import ControlledProvider, node, output

TABLES = ('memory_nodes', 'memory_node_versions', 'memory_derivations',
          'memory_derivation_sources', 'extraction_batches', 'extraction_batch_sources',
          'derived_cursors', 'rebuild_runs', 'rebuild_receipts', 'memory_job_candidates', 'memory_jobs')


async def table_snapshot(database):
    return {table: await database.fetchall(f'SELECT * FROM {table} ORDER BY rowid') for table in TABLES}


async def failed_draft(harness):
    plugin = harness.plugin
    engine = plugin.maintenance
    await engine.runner.stop()
    previous = engine.config, engine.runtime.adapter, engine.runtime.recall, engine.persona_loader
    provider = ControlledProvider()

    async def answer(_messages):
        provider.output = '{"schema_version":' if len(provider.calls) == 1 else json.dumps(
            output([node('s1', title='Synthetic unfinished rebuild', content='I inspect the telescope each Monday.')]))

    async def persona(_persona_id):
        return 'A patient synthetic observatory assistant.'

    async def fail_index(*_args):
        raise ReviewRequired('index_unavailable')

    provider.on_generate = answer
    engine.config = replace(engine.config, llm_provider_id='synthetic-controlled', private_min_messages=1,
                            extraction_auto_regeneration=True, topic_consolidation_enabled=False)
    engine.runtime.adapter = provider
    engine.runtime.recall = None
    engine.persona_loader = persona
    try:
        queued = await engine.enqueue('rebuild', harness.space['space_id'], 'synthetic-failed-rebuild-delete')
        claimed = await engine.jobs.claim('isolated-delete-probe', lease_seconds=60)
        assert claimed['job_id'] == queued['job_id']
        with patch.object(MaintenanceEngine, '_index_generation', fail_index):
            await engine.runner._execute(claimed)
        failed = await engine.jobs.get(queued['job_id'])
        assert failed['state'] == 'failed' and failed['error_code'] == 'index_unavailable', failed
        assert failed['checkpoint']['phase'] == 'index'
        assert len(provider.calls) == 2
        candidates = await engine.jobs.list_candidates(failed['job_id'])
        assert len(candidates) == 1
        generation = failed['checkpoint']['generation_id']
        rows = await plugin.memory_store.database.fetchall('SELECT * FROM memory_nodes WHERE generation_id=?', (generation,))
        assert len(rows) == 1
        assert await plugin.memory_store.database.fetchall('SELECT * FROM rebuild_runs WHERE job_id=?', (failed['job_id'],))
        return failed, rows[0], len(provider.calls)
    finally:
        engine.config, engine.runtime.adapter, engine.runtime.recall, engine.persona_loader = previous


async def main():
    with tempfile.TemporaryDirectory(prefix='neomemory-task-delete-host-') as directory:
        harness = HostConsoleHarness(Path(directory))
        await harness.start()
        plugin = harness.plugin
        memory, sources, cache = plugin.memory_store, plugin.source_store, plugin.runtime.cache
        await plugin.maintenance.runner.stop()
        baseline = await table_snapshot(memory.database)
        source_rows = await sources.database.fetchall('SELECT * FROM source_records ORDER BY rowid')
        active = await memory.get_active_generation(harness.space['space_id'])
        failed, draft, controlled_calls = await failed_draft(harness)
        assert await table_snapshot(memory.database) != baseline
        job_id, generation_id = failed['job_id'], failed['checkpoint']['generation_id']
        identities = [(draft['node_id'], draft['content_version']),
                      (harness.node['node_id'], harness.node['content_version'])]
        hashes = [text_hash('Synthetic unpublished vector'), text_hash('Synthetic active vector')]
        await cache.put_vectors('synthetic-delete-probe', harness.space['space_id'], hashes,
                                [[1., 0.], [0., 1.]], kind='node', identities=identities)
        assert all(v is not None for v in await cache.get_vectors('synthetic-delete-probe', harness.space['space_id'], hashes, kind='node', identities=identities))
        plugin.maintenance._pending_reads[generation_id] = {'synthetic': True}
        plugin.maintenance._read_hints[generation_id] = 1
        sock = socket.socket()
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(harness.app, host='127.0.0.1', port=port, log_level='error', access_log=False))
        server_task = asyncio.create_task(server.serve(sockets=[sock]))
        try:
            for _ in range(100):
                if server.started:
                    break
                await asyncio.sleep(.02)
            assert server.started
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=True)
                page = await browser.new_page(viewport={'width': 1280, 'height': 1000})
                page.set_default_timeout(10000)
                errors = []
                page.on('pageerror', lambda error: errors.append(str(error)))
                await page.goto(f'http://127.0.0.1:{port}/host-console')
                frame = page.frame_locator('#plugin')
                await expect(frame.locator('#status')).to_contain_text('已连接')
                await frame.locator('#space').select_option(harness.space['space_id'])
                await frame.locator('[data-page="maintenance"]').click()
                card = frame.locator('#tasks > article').filter(has_text=job_id)
                await expect(card).to_contain_text('失败')
                await card.get_by_role('radio').check()
                await frame.locator('#task-detail-panel > summary').click()
                await expect(frame.locator('#task-detail')).to_contain_text(job_id)
                await card.get_by_role('button', name='删除任务', exact=True).click()
                assert await plugin.jobs.get(job_id) is not None
                async with page.expect_response(lambda response: response.request.method == 'POST'
                                                and response.url.endswith('/tasks/delete')) as pending:
                    await card.get_by_role('button', name='确认删除任务', exact=True).click()
                response = await pending.value
                assert response.status == 200
                raw = await response.json()
                deleted = raw.get('data', raw)
                assert deleted['deleted'] and deleted['removed_generation_id'] == generation_id
                assert deleted['removed_node_count'] == 1
                await expect(card).to_have_count(0)
                await expect(frame.locator('#task-detail')).to_have_text('')
                await expect(frame.locator("#notifications .toast").filter(has_text='清理未启用记忆 1 条').last).to_be_visible()
                assert await table_snapshot(memory.database) == baseline
                assert await memory.database.fetchone('SELECT * FROM memory_generations WHERE generation_id=?', (generation_id,)) is None
                assert await memory.get_active_generation(harness.space['space_id']) == active
                assert await sources.database.fetchall('SELECT * FROM source_records ORDER BY rowid') == source_rows
                vectors = await cache.get_vectors('synthetic-delete-probe', harness.space['space_id'], hashes, kind='node', identities=identities)
                assert vectors[0] is None and vectors[1] is not None
                assert generation_id not in plugin.maintenance._pending_reads
                assert generation_id not in plugin.maintenance._read_hints
                assert not await memory.database.fetchall('PRAGMA foreign_key_check')
                assert plugin.context.calls == [] and plugin.context.embedding.calls == []
                # The bulk action uses the real scoped route after explicit confirmation.
                completed = []
                for scope in (harness.space['space_id'], harness.other_space['space_id']):
                    queued = await plugin.jobs.submit('model_test', scope, {}, 'bulk-' + scope)
                    claimed = await plugin.jobs.claim('bulk-ui')
                    assert claimed['job_id'] == queued['job_id']
                    await plugin.jobs.archive_candidate(queued['job_id'], claimed['lease_token'], {'raw':'synthetic'}, 'invalid_json')
                    await plugin.jobs.finish(queued['job_id'], claimed['lease_token'], result={})
                    completed.append(queued['job_id'])
                await frame.locator('#clear-completed-tasks').click()
                assert await plugin.jobs.get(completed[0]) is not None
                async with page.expect_response(lambda response: response.request.method == 'POST'
                                                and response.url.endswith('/tasks/clear-completed')) as pending:
                    await frame.locator('#clear-completed-confirm').click()
                assert (await pending.value).status == 200
                await expect(frame.locator("#notifications .toast").filter(has_text='已清空 1 个').last).to_be_visible()
                assert await plugin.jobs.get(completed[0]) is None
                assert await plugin.jobs.get(completed[1]) is not None
                await frame.locator('#all-tasks').check()
                await frame.locator('#clear-completed-tasks').click()
                await expect(frame.locator('#clear-completed-scope')).to_contain_text('所有空间')
                async with page.expect_response(lambda response: response.request.method == 'POST'
                                                and response.url.endswith('/tasks/clear-completed')) as pending:
                    await frame.locator('#clear-completed-confirm').click()
                assert (await pending.value).status == 200
                await expect(frame.locator('#clear-completed-dialog')).not_to_be_visible()
                await expect(frame.locator("#notifications .toast").filter(has_text='已清空 1 个').last).to_be_visible()
                assert await plugin.jobs.get(completed[1]) is None
                assert await table_snapshot(memory.database) == baseline
                assert await sources.database.fetchall('SELECT * FROM source_records ORDER BY rowid') == source_rows
                await page.close()
                await harness.reload()
                await harness.plugin.maintenance.runner.stop()
                assert await harness.plugin.jobs.get(job_id) is None
                assert await table_snapshot(harness.plugin.memory_store.database) == baseline
                assert await harness.plugin.memory_store.get_active_generation(harness.space['space_id']) == active
                assert await harness.plugin.source_store.database.fetchall('SELECT * FROM source_records ORDER BY rowid') == source_rows
                persisted = await harness.plugin.runtime.cache.get_vectors('synthetic-delete-probe', harness.space['space_id'], hashes, kind='node', identities=identities)
                assert persisted[0] is None and persisted[1] is not None
                assert not errors
                await browser.close()
            print(json.dumps({'ok': True, 'real_host_routes': True, 'persistent_sqlite': True,
                'task_state_before_delete': 'failed', 'synthetic_provider_responses': controlled_calls,
                'candidate_archives_before_delete': 1, 'deleted_unpublished_nodes': 1,
                'confirmation_required': True, 'detail_and_card_cleared': True,
                'bulk_completed_scope_and_all_spaces': True,
                'checkpoint_candidate_generation_and_derived_rows_removed': True,
                'draft_vector_removed_active_vector_retained': True,
                'active_generation_and_sources_unchanged': True, 'persisted_after_runtime_reload': True,
                'external_model_requests': 0, 'parent_page': 'minimal host bridge; not full Vue login'}, ensure_ascii=False))
        finally:
            server.should_exit = True
            await server_task
            sock.close()
            await harness.stop()


if __name__ == '__main__':
    asyncio.run(main())
