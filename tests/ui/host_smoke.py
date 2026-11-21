"""Chromium against real AstrBot asset/auth/bridge routes and temporary stores.

Run with PYTHONPATH pointing at the pinned AstrBot source and this project.
The minimal parent models the Vue GET/POST bridge contract; Vue login, reverse
proxy subpath deployment and live Providers are deliberately not claimed.
"""

import asyncio
from dataclasses import replace
import json
import socket
import sys
import tempfile
from pathlib import Path

import uvicorn
from playwright.async_api import async_playwright, expect

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.host_console_harness import HostConsoleHarness  # noqa: E402
from tests.test_recall_runtime import RuntimeEvent, seed  # noqa: E402


async def await_job(harness, job_id):
    for _ in range(100):
        job = await harness.plugin.jobs.get(job_id)
        if job["state"] == "succeeded":
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"Background task did not finish: {job['state']}")


async def governance_loading_controls(page, frame, harness):
    """Delay actual read requests without mocking their response bodies."""
    for mode in ('success', 'failure', 'scope_change'):
        entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
        intercepted = False
        delayed_url = None
        async def delay(route):
            nonlocal intercepted, delayed_url
            if intercepted:
                await route.continue_()
                return
            intercepted = True
            delayed_url = route.request.url
            entered.set()
            await release.wait()
            if mode == 'failure':
                await route.abort()
            else:
                await route.continue_()
            finished.set()
        await expect(frame.locator('#refresh-nodes')).to_be_enabled()
        await page.route('**/memory/nodes?*', delay)
        try:
            await frame.locator('#refresh-nodes').click()
            await asyncio.wait_for(entered.wait(), 5)
            for control in ('node-select', 'refresh-nodes', 'action', 'node-title', 'content', 'draft-operation', 'governance'):
                await expect(frame.locator('#' + control)).to_be_disabled()
            if mode == 'scope_change':
                await frame.locator('#space').select_option(harness.other_space['space_id'])
                await expect(frame.locator('#content')).to_have_value('Isolated other persona memory')
                await expect(frame.locator('#node-title')).to_be_enabled()
                await frame.locator('#node-title').fill('Synthetic title in new scope')
            if mode == 'failure':
                release.set()
            else:
                async with page.expect_response(lambda response: response.url == delayed_url) as received:
                    release.set()
                await (await received.value).body()
                await frame.locator('#node-title').evaluate('node => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))')
            await asyncio.wait_for(finished.wait(), 5)
            await expect(frame.locator('#refresh-nodes')).to_be_enabled()
            await expect(frame.locator('#node-title')).to_be_enabled()
            if mode == 'scope_change':
                await expect(frame.locator('#space')).to_have_value(harness.other_space['space_id'])
                await expect(frame.locator('#node-title')).to_have_value('Synthetic title in new scope')
            elif mode == 'failure':
                await expect(frame.locator('#notifications .toast.error').last).to_be_visible()
        finally:
            release.set()
            await page.unroute('**/memory/nodes?*', delay)
        if mode == 'scope_change':
            await frame.locator('#space').select_option(harness.space['space_id'])
            await expect(frame.locator('#node-select')).to_be_enabled()
    return {'pending_editor_disabled': True, 'failed_read_unlocks': True, 'stale_scope_response_ignored': True}


async def governance_lifecycle(page, frame, harness):
    """Browser forms commit into isolated stores through real host task routes."""
    plugin = harness.plugin
    memory, jobs = plugin.memory_store, plugin.jobs
    submitted_ids = []
    space, original = await seed(plugin, RuntimeEvent(message_id="synthetic-governance-cycle",
        text="I check the telescope on Monday. I inventory lenses on Friday."),
        content="I check the telescope on Monday. I inventory lenses on Friday.")
    assert space['space_id'] == harness.space['space_id']
    original_sources = await memory.list_node_sources(original['node_id'])
    source_ids = {row['source_id'] for row in original_sources}
    assert len(source_ids) == 1
    await frame.locator('[data-page="maintenance"]').click()
    await frame.locator('[data-maintenance-tab="manual"]').click()
    loading_result = await governance_loading_controls(page, frame, harness)

    async def select(node_id, action):
        await frame.locator('[data-maintenance-tab="manual"]').click()
        await frame.locator('#action').select_option('edit')
        previous_option = await frame.locator('#node-select option').first.element_handle()
        await frame.locator('#refresh-nodes').click()
        await previous_option.evaluate("node => new Promise(resolve => { if (!node.isConnected) return resolve(); const observer = new MutationObserver(() => { if (!node.isConnected) { observer.disconnect(); resolve(); } }); observer.observe(document, {childList:true,subtree:true}); })")
        await expect(frame.locator(f'#node-select option[value="{node_id}"]')).to_have_count(1)
        await frame.locator('#node-select').select_option(node_id)
        await frame.locator('#action').select_option(action)
        if action != 'delete':
            await expect(frame.locator('#governance')).to_be_enabled()

    async def submit(*, wait=True):
        await frame.locator('#draft-operation').click()
        await expect(frame.locator("#notifications .toast").filter(has_text='操作预览已生成').last).to_be_visible()
        try:
            async with page.expect_response(lambda response: response.request.method == 'POST'
                                            and response.url.endswith('/tasks'), timeout=10000) as pending:
                await frame.locator('#governance').click()
        except Exception:
            print(json.dumps({'submission_notifications': await frame.locator('#notifications').inner_text(),
                'availability': await frame.locator('#availability').inner_text(),
                'operation': await frame.locator('#operation').input_value()}, ensure_ascii=False))
            raise
        response = await pending.value
        assert response.status == 200
        value = await response.json()
        submitted = value.get('data', value)
        job_id = submitted['job_id']
        submitted_ids.append(job_id)
        if wait:
            await await_job(harness, job_id)
            await expect(frame.locator('#governance')).to_be_enabled()
        return await jobs.get(job_id)

    # Pause only the isolated worker so queued/running states remain observable.
    await plugin.maintenance.runner.stop()
    await select(original['node_id'], 'edit')
    await frame.locator('#node-title').fill('Synthetic telescope and lenses')
    await frame.locator('#content').fill('I check the telescope each Monday. I inventory lenses each Friday.')
    queued = await submit(wait=False)
    await frame.locator('[data-maintenance-tab="tasks"]').click()
    assert queued['state'] == 'queued'
    await expect(frame.locator('#governance')).to_be_disabled()
    await expect(frame.locator('#extract')).to_be_disabled()
    await expect(frame.locator('#availability')).to_contain_text('暂不可启动')
    # A second browser POST cannot bypass the same-space exclusion.
    from tests.host_console_harness import API_PREFIX
    conflict = await page.request.post(page.url.replace('/host-console', API_PREFIX + '/tasks'),
        headers=harness.headers, data={'kind': 'governance', 'space_id': space['space_id'],
        'idempotency_key': 'synthetic-distinct-conflicting-request',
        'operation': queued['payload']['operation']})
    assert conflict.status == 409
    assert len([j for j in await jobs.list(scope_key=space['space_id'])
                if j['state'] == 'queued']) == 1
    entered, release = asyncio.Event(), asyncio.Event()
    original_apply = plugin.maintenance.governance.apply
    async def gated_apply(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original_apply(*args, **kwargs)
    plugin.maintenance.governance.apply = gated_apply
    try:
        await plugin.maintenance.runner.start()
        await asyncio.wait_for(entered.wait(), 5)
        card = frame.locator('#tasks > article').filter(has_text=queued['job_id'])
        await expect(card).to_contain_text('执行中')
        await expect(card).to_contain_text('应用人工治理')
        await expect(card).to_contain_text('0 / 1')
        await expect(card.get_by_role('progressbar', name='任务进度')).to_have_attribute('value', '0')
        assert await card.get_by_role('button', name='删除任务', exact=True).count() == 0
    finally:
        release.set()
        plugin.maintenance.governance.apply = original_apply
    await await_job(harness, queued['job_id'])
    await expect(card).to_contain_text('人工治理 · 完成')
    await expect(card.get_by_role('progressbar', name='任务进度')).to_have_attribute('value', '100')
    await expect(frame.locator('#governance')).to_be_enabled()
    edited = await memory.get_node(original['node_id'])
    assert edited['content_version'] == 2 and edited['manual_protected']
    assert edited['content_text'] == 'I check the telescope each Monday. I inventory lenses each Friday.'

    await select(original['node_id'], 'split')
    await expect(frame.locator('.split-part')).to_have_count(2)
    for i, (title, content) in enumerate((('Synthetic telescope', 'I check the telescope each Monday.'),
                                         ('Synthetic lenses', 'I inventory lenses each Friday.'))):
        part = frame.locator('.split-part').nth(i)
        await part.locator('.part-title').fill(title)
        await part.locator('.part-content').fill(content)
        assert await part.locator('.sources input:checked').count() == 1
    split_job = await submit()
    split_ids = list(split_job['result']['node_refs'].values())
    assert len(split_ids) == 2
    assert (await memory.get_node(original['node_id']))['status'] == 'superseded'
    for node_id in split_ids:
        node = await memory.get_node(node_id)
        assert node['manual_protected'] and node['content_version'] == 1
        assert {row['source_id'] for row in await memory.list_node_sources(node_id)} == source_ids

    keep_id, second_id = split_ids
    await select(keep_id, 'merge')
    await expect(frame.locator(f'#merge-nodes option[value="{second_id}"]')).to_have_count(1)
    await frame.locator('#merge-nodes').select_option([second_id])
    await frame.locator('#compose-merge').click()
    await frame.locator('#node-title').fill('Synthetic merged checks')
    await submit()
    merged = await memory.get_node(keep_id)
    assert merged['content_version'] == 2 and merged['manual_protected']
    assert 'each Monday' in merged['content_text'] and 'each Friday' in merged['content_text']
    assert (await memory.get_node(second_id))['status'] == 'superseded'
    assert {row['source_id'] for row in await memory.list_node_sources(keep_id)} == source_ids

    for action, status, protected in (('disable', 'disabled', True), ('restore', 'active', True),
                                      ('unprotect', 'active', False), ('protect', 'active', True)):
        await select(keep_id, action)
        await submit()
        node = await memory.get_node(keep_id)
        assert node['status'] == status and node['manual_protected'] is protected
        assert node['content_version'] == merged['content_version']
    await select(keep_id, 'delete')
    await expect(frame.locator('#governance')).to_be_disabled()
    await frame.locator('#delete-confirm').check()
    deleted_job = await submit()
    deleted = await memory.get_node(keep_id)
    assert deleted['status'] == 'deleted' and deleted['deleted_at']
    assert deleted_job['result']['deletion_kind'] == 'derived_tombstone'
    assert {row['source_id'] for row in await memory.list_node_sources(keep_id)} == source_ids
    assert len(await memory.list_node_versions(keep_id)) == 2
    assert await plugin.source_store.get_records(tuple(source_ids))
    assert not await memory.database.fetchall('PRAGMA foreign_key_check')
    assert len(submitted_ids) == len(set(submitted_ids)) == 8
    completed = [await jobs.get(job_id) for job_id in submitted_ids]
    assert all(job['state'] == 'succeeded' for job in completed)
    final_nodes = [await memory.get_node(node_id) for node_id in (original['node_id'], keep_id, second_id)]
    return {'actions': len(submitted_ids), 'committed_jobs': len(completed), 'conflicting_post_status': conflict.status,
            'final_nodes': [{'status': node['status'], 'content_version': node['content_version'],
                             'manual_protected': node['manual_protected']} for node in final_nodes],
            'split_sources_retained': True, 'merge_sources_retained': True,
            'protection_independent_of_status': True, 'delete_preserves_tombstone_and_sources': True,
            'queued_running_completed_progress': True, 'loading_controls': loading_result}


async def text_cleaning_controls(page, frame, harness):
    await frame.locator('[data-page="settings"]').click()
    await frame.get_by_role('tab', name='文本清理').click()
    dialog = frame.locator('#cleaning-dialog')
    rules = frame.locator('#cleaning-list')

    async def save():
        await dialog.get_by_role('button', name='保存', exact=True).click()
        await expect(dialog).to_be_hidden()

    async def add_group(name):
        await frame.get_by_role('button', name='添加分组').click()
        await dialog.get_by_label('名称', exact=True).fill(name)
        await save()
        disclosure = rules.get_by_role('button', name=name, exact=True)
        await expect(disclosure).to_have_attribute('aria-expanded', 'false')

    async def delete_group(group, cascade):
        await group.locator(':scope > .cleaning-row').get_by_role('button', name='删除').click()
        checkbox = dialog.get_by_label('同时删除组内条目')
        await expect(checkbox).not_to_be_checked()
        if cascade:
            await checkbox.check()
        await dialog.get_by_role('button', name='确认删除').click()
        await expect(dialog).to_be_hidden()

    presets = rules.locator('.cleaning-group').filter(has_text='陪伴插件文本清理')
    preset_rows = presets.locator('.cleaning-members .cleaning-row')
    disclosure = presets.get_by_role('button', name='陪伴插件文本清理', exact=True)
    await expect(preset_rows.nth(1)).to_be_hidden()
    header = presets.locator(':scope > .cleaning-row')
    await header.scroll_into_view_if_needed()
    bounds = await header.bounding_box()
    edit = await header.get_by_role('button', name='编辑').bounding_box()
    await page.mouse.move(edit['x'] - 12, bounds['y'] + 4)
    await expect(disclosure).to_have_css('background-color', 'rgba(0, 0, 0, 0)')
    await page.mouse.down()
    await expect(disclosure).to_have_css('background-color', 'rgba(0, 0, 0, 0)')
    await page.mouse.up()
    await expect(preset_rows.nth(1)).to_be_visible()
    await disclosure.locator('small').click()
    await expect(preset_rows.nth(1)).to_be_hidden()
    await disclosure.press('Enter')
    await expect(preset_rows.nth(1)).to_be_visible()
    preset_count = await preset_rows.count()
    preset = preset_rows.first
    await preset.get_by_role('button', name='编辑').click()
    await expect(dialog.get_by_label('规则类型')).to_have_value('regex')
    assert await dialog.get_by_label('匹配内容').input_value()
    await dialog.get_by_label('替换为').fill('合成替换结果')
    await dialog.get_by_label('跳过 Markdown 代码与引用').uncheck()
    await save()
    await preset.get_by_role('button', name='编辑').click()
    await expect(dialog.get_by_label('替换为')).to_have_value('合成替换结果')
    await expect(dialog.get_by_label('跳过 Markdown 代码与引用')).not_to_be_checked()
    await dialog.get_by_role('button', name='取消').click()
    await preset.get_by_role('switch').uncheck()
    await expect(preset).to_have_attribute('data-effective', 'false')
    await expect(preset_rows.nth(1)).to_have_attribute('data-effective', 'true')

    await add_group('合成分组')
    group = rules.locator('.cleaning-group').filter(has_text='合成分组')
    await frame.get_by_role('button', name='添加条目').click()
    await dialog.get_by_label('名称', exact=True).fill('合成替换')
    await expect(dialog.get_by_label('所属分组')).to_have_value('')
    await expect(dialog.get_by_label('执行顺序')).to_have_value(str(preset_count + 1))
    await dialog.get_by_label('执行顺序').fill('1')
    await dialog.get_by_label('规则类型').select_option('regex')
    await dialog.get_by_label('匹配内容').fill('(')
    await dialog.get_by_role('button', name='保存', exact=True).click()
    await expect(dialog.locator('[role="alert"]')).to_be_visible()
    await dialog.get_by_label('匹配内容').fill(r'<tone>(.*?)</tone>')
    await dialog.get_by_label('替换为').fill(r'\1')
    await save()

    root_rule = rules.locator(':scope > .cleaning-row').filter(has_text='合成替换')
    await root_rule.get_by_role('button', name='编辑').click()
    await expect(dialog.get_by_label('执行顺序')).to_have_value('1')
    await dialog.get_by_label('执行顺序').fill('2')
    await expect(dialog.get_by_label('替换为')).to_have_value(r'\1')
    await dialog.get_by_label('所属分组').select_option(label='合成分组')
    await save()
    await group.get_by_role('button', name='合成分组', exact=True).click()
    member = group.locator('.cleaning-members .cleaning-row').filter(has_text='合成替换')
    await expect(member).to_contain_text('执行顺序 2')
    await expect(member).to_have_attribute('data-effective', 'true')
    await group.get_by_role('switch', name='启用分组 合成分组').uncheck()
    await expect(member).to_have_attribute('data-effective', 'false')
    await expect(member.get_by_role('switch')).to_be_checked()
    await delete_group(group, cascade=False)
    await expect(root_rule).to_have_attribute('data-effective', 'true')
    await root_rule.get_by_role('switch').uncheck()
    await expect(root_rule).to_have_attribute('data-effective', 'false')
    await frame.get_by_role('tab', name='高级参数').click()
    await frame.get_by_role('tab', name='文本清理').click()
    await expect(root_rule.get_by_role('switch')).not_to_be_checked()
    await add_group('合成分组')
    await root_rule.get_by_role('button', name='编辑').click()
    await dialog.get_by_label('所属分组').select_option(label='合成分组')
    await save()
    await expect(member).to_have_attribute('data-effective', 'false')
    await delete_group(group, cascade=True)
    await expect(rules).not_to_contain_text('合成替换')
    await expect(rules).not_to_contain_text('合成分组')

    async def drag(handle, target, fraction=.5, *, touch=False):
        await handle.evaluate("node => node.scrollIntoView({block:'center'})")
        bounds = await handle.bounding_box()
        x, y = bounds['x'] + bounds['width'] / 2, bounds['y'] + bounds['height'] / 2
        cdp = await page.context.new_cdp_session(page) if touch else None
        if touch:
            await cdp.send('Input.dispatchTouchEvent', {'type': 'touchStart', 'touchPoints': [{'x': x, 'y': y}]})
        else:
            await page.mouse.move(x, y)
            await page.mouse.down()
        await expect(frame.locator('#cleaning-drop-end')).to_be_visible()
        await target.evaluate("""async node => {
            node.scrollIntoView({block:'center'});
            await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
        }""")
        bounds = await target.bounding_box()
        x, y = bounds['x'] + bounds['width'] / 2, bounds['y'] + bounds['height'] * fraction
        async with page.expect_response(lambda r: '/text-cleaning' in r.url and r.request.method == 'POST' and r.status == 200):
            if touch:
                await cdp.send('Input.dispatchTouchEvent', {'type': 'touchMove', 'touchPoints': [{'x': x, 'y': y}]})
                await cdp.send('Input.dispatchTouchEvent', {'type': 'touchEnd', 'touchPoints': []})
                await cdp.detach()
            else:
                await page.mouse.move(x, y)
                await page.mouse.up()
        await expect(rules).not_to_have_attribute('inert', '')

    await add_group('拖动分组')
    empty = rules.locator('.cleaning-group').filter(has_text='拖动分组')
    empty_header = empty.locator(':scope > .cleaning-row')
    header = presets.locator(':scope > .cleaning-row')
    chosen = preset_rows.last
    child_id = await chosen.get_attribute('data-id')
    sample = '<call:send>{content:正文}</call>'
    assert harness.plugin.source_store.cleaner.clean(sample) == '正文'
    await drag(chosen.locator('.cleaning-grip'), empty_header, touch=True)
    moved = empty.locator('.cleaning-members .cleaning-row')
    await expect(moved).to_be_hidden()
    await expect(moved).to_have_attribute('data-id', child_id)
    await disclosure.click()
    await expect(preset_rows.first).to_be_hidden()
    await drag(empty_header.locator('.cleaning-grip'), header, .35)
    config = harness.plugin.cleaning_settings.config
    assert config['rules'][0]['id'] == child_id
    assert harness.plugin.source_store.cleaner.clean(sample) == '正文}</call>'
    assert config['root_order'][0] == {'kind': 'group', 'id': config['rules'][0]['group_id']}
    await frame.get_by_role('tab', name='高级参数').click()
    await frame.get_by_role('tab', name='文本清理').click()
    await expect(rules.locator(':scope > .cleaning-group').first).to_contain_text('拖动分组')
    await empty.get_by_role('button', name='拖动分组', exact=True).click()
    await drag(moved.locator('.cleaning-grip'), frame.locator('#cleaning-drop-end'))
    assert harness.plugin.cleaning_settings.config['root_order'][-1] == {'kind': 'rule', 'id': child_id}
    assert harness.plugin.cleaning_settings.config['rules'][-1]['group_id'] is None
    assert harness.plugin.source_store.cleaner.clean(sample) == '正文'
    await expect(rules.locator(':scope > .cleaning-row').last).to_have_attribute('data-id', child_id)
    await drag(empty_header.locator('.cleaning-grip'), frame.locator('#cleaning-drop-end'))
    assert harness.plugin.cleaning_settings.config['root_order'][-1]['kind'] == 'group'


async def main():
    with tempfile.TemporaryDirectory(prefix="neomemory-host-ui-") as directory:
        harness = HostConsoleHarness(Path(directory))
        await harness.start()
        memory = harness.plugin.memory_store
        profile = await memory.upsert_profile(space_id=harness.space["space_id"],
            logical_user_id=harness.space["logical_user_id"], tags=["stargazing"], description="Synthetic previous profile")
        await memory.upsert_profile(space_id=harness.space["space_id"],
            logical_user_id=harness.space["logical_user_id"], tags=["stargazing"], description="Synthetic current profile")
        relationship = await memory.upsert_relationship(space_id=harness.space["space_id"],
            logical_user_id=harness.space["logical_user_id"], state_text="Synthetic previous relationship")
        await memory.upsert_relationship(space_id=harness.space["space_id"],
            logical_user_id=harness.space["logical_user_id"], state_text="Synthetic current relationship")
        from core.context_bundle import assemble_context
        from core.relationship_state import relationship_slots
        diagnostic = assemble_context({"relationship": {"state": relationship_slots("我还想听对方细说。" * 60)},
            "profile": {"tags": [], "description": "独立长描述。" * 150}},
            [harness.node], replace(harness.plugin.config, context_relationship_max_chars=320, recall_budget_tokens=2048)).diagnostics
        harness.plugin.runtime.diagnostics["last_context"] = {**diagnostic,
            "space_id": harness.space["space_id"], "request_id": "synthetic-ui-assembly", "recorded_at": 1794787200}
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(harness.app, host="127.0.0.1", port=port, log_level="error", access_log=False))
        server_task = asyncio.create_task(server.serve(sockets=[sock]))
        try:
            for _ in range(100):
                if server.started:
                    break
                await asyncio.sleep(0.02)
            assert server.started
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=True)
                page = await browser.new_page(viewport={"width": 1280, "height": 1000})
                page.set_default_timeout(10_000)
                errors, asset_responses = [], []
                page.on("pageerror", lambda error: errors.append(str(error)))
                page.on("response", lambda response: asset_responses.append((response.url.rsplit("/", 1)[-1].split("?")[0], response.status))
                        if "/_t/" in response.url else None)
                await page.goto(f"http://127.0.0.1:{port}/host-console")
                frame = page.frame_locator("#plugin")
                await frame.locator("#status").filter(has_text="已连接").wait_for()
                assert await page.locator("#plugin").get_attribute("sandbox") == "allow-scripts allow-forms allow-downloads"
                await frame.locator("#space").select_option(harness.space["space_id"])
                await frame.locator("#memory-list").filter(has_text="Synthetic host memory").wait_for()
                await frame.locator("#memory-advanced > summary").click()
                await frame.locator("#memory-emotion").select_option("joy")
                await expect(frame.locator("#memory-list")).to_contain_text("当前条件下没有记录")
                await frame.locator("#memory-emotion").select_option("none")
                await expect(frame.locator("#memory-list")).to_contain_text("Synthetic host memory")
                await frame.locator("#memory-emotion").select_option("")
                await frame.locator("#memory-advanced > summary").click()
                uid = harness.space["logical_user_id"]
                await harness.plugin.source_store.rename_logical_user(uid, "林川")
                await frame.locator("#refresh").click()
                await expect(frame.locator("#space option:checked")).to_contain_text("林川")
                assert uid not in await frame.locator("#space option:checked").inner_text()
                assert await frame.locator("img").count() == 0
                await frame.locator(".memory-inspector > summary").first.click()
                await frame.locator(".memory-sources .memory-source > summary").first.click()
                await frame.locator(".source-content").filter(has_text="Synthetic host conversation").wait_for()
                assert "本次直接引用" in await frame.locator(".memory-detail").inner_text()
                assert "节点累计引用" in await frame.locator(".memory-sources").inner_text()
                for kind, label, entity_id, text in (
                    ("profiles", "用户画像", profile["profile_id"], "profile"),
                    ("relationships", "人格关系", relationship["relationship_id"], "relationship"),
                ):
                    await frame.locator(f'[data-kind="{kind}"]').click()
                    await frame.locator("#memory-list").filter(has_text=f"Synthetic current {text}").wait_for()
                    await expect(frame.locator(".memory-title")).to_have_text(f"{label} · 林川")
                    await frame.locator(".memory-inspector > summary").click()
                    await frame.locator(".memory-detail").filter(has_text=f"Synthetic previous {text}").wait_for()
                    assert await frame.locator(".memory-detail article").count() == 2
                    await expect(frame.locator(".memory-version h3").first).to_contain_text("版本 2")
                    assert "本次未新增原文引用" in await frame.locator(".memory-detail").inner_text()
                    await frame.get_by_role("button", name=f"{label} {entity_id} · 版本 1", exact=True).click()
                    await frame.get_by_role("button", name="返回上一层查阅", exact=True).wait_for()
                    assert await frame.locator(".memory-detail article").count() == 1
                    assert f"Synthetic current {text}" not in await frame.locator(".memory-detail").inner_text()
                    assert await frame.locator(".memory-sources").count() == 0
                # 真实治理任务仅修改标题，正文快照仍保留创建时的空标题。
                before = await memory.get_node(harness.node["node_id"])
                await frame.locator('[data-page="maintenance"]').click()
                await frame.locator('[data-maintenance-tab="status"]').click()
                await expect(frame.locator("#storage-usage")).to_contain_text("memory.db")
                await expect(frame.locator("#context-budget-status")).to_contain_text("synthetic-ui-assembly")
                await expect(frame.locator("#context-budget-status")).to_contain_text(harness.space["space_id"])
                await expect(frame.locator("#context-budget-status")).to_contain_text("UTF-8 字节")
                await expect(frame.locator("#context-budget-status")).to_contain_text("超过该类条目的字符上限")
                await expect(frame.locator("#context-budget-status")).to_contain_text("单项已超过总预算，调整顺序无效")
                await frame.locator('[data-maintenance-tab="manual"]').click()
                await frame.locator('#node-select').select_option(harness.node["node_id"])
                await frame.locator('#node-title').fill('Synthetic host title')
                await frame.locator('#governance').click()
                await frame.locator("#notifications .toast").filter(has_text='任务已提交').last.wait_for()
                title_jobs = [j for j in await harness.plugin.jobs.list(scope_key=harness.space['space_id']) if j['kind'] == 'governance']
                assert len(title_jobs) == 1
                await await_job(harness, title_jobs[0]['job_id'])
                after = await memory.get_node(harness.node['node_id'])
                assert after['title'] == 'Synthetic host title'
                for field in ('content_text', 'content_version', 'importance', 'strength_reset_at', 'strength_reset_round', 'updated_at', 'manual_protected'):
                    assert after[field] == before[field], field
                await frame.locator('[data-page="memory"]').click()
                await frame.locator('[data-kind="nodes"]').click()
                await frame.locator('.memory-title').filter(has_text='Synthetic host title').wait_for()
                await frame.locator('.memory-inspector > summary').click()
                await frame.locator('.memory-detail').filter(has_text='新题：Synthetic host title').wait_for()
                assert await frame.locator('.memory-version').count() == 1
                assert await frame.locator('.version-title').inner_text() == '未记录标题'
                assert '原题：未记录标题' in await frame.locator('.memory-detail').inner_text()

                # 情绪通过实际治理API保存；只改情绪也形成可查阅的新内容版本。
                await frame.locator('[data-page="maintenance"]').click()
                await frame.locator('[data-maintenance-tab="manual"]').click()
                await frame.locator('#node-select').select_option(harness.node['node_id'])
                emotional = frame.locator('#node-emotion-editor')
                await emotional.locator('.emotion-enabled').check()
                await emotional.locator('.emotion-tags').select_option(['joy', 'concern'])
                await emotional.locator('.emotion-intensity[data-tag="joy"]').fill('0.25')
                await emotional.locator('.emotion-intensity[data-tag="concern"]').fill('0.7')
                await emotional.locator('.emotion-description').fill('我对新的安排有些担心。')
                async with page.expect_response(lambda response: response.request.method == 'POST' and response.url.endswith('/tasks')) as submitted:
                    await frame.locator('#governance').click()
                assert (await submitted.value).status == 200
                await frame.locator("#notifications .toast").filter(has_text='任务已提交').last.wait_for()
                emotion_jobs = [j for j in await harness.plugin.jobs.list(scope_key=harness.space['space_id'])
                                if j['kind'] == 'governance' and 'emotion' in j['payload']['operation'].get('patch', {})]
                assert len(emotion_jobs) == 1
                await await_job(harness, emotion_jobs[0]['job_id'])
                after_emotion = await memory.get_node(harness.node['node_id'])
                assert after_emotion['content_version'] == 2
                assert after_emotion['content_text'] == after['content_text']
                assert after_emotion['emotion'] == {'tags': [{'tag': 'joy', 'intensity': 0.25}, {'tag': 'concern', 'intensity': 0.7}], 'description': '我对新的安排有些担心。'}
                await frame.locator('[data-page="memory"]').click()
                await frame.locator('.memory-card > .memory-emotion').filter(has_text='我对新的安排有些担心。').wait_for()
                await frame.locator('.memory-inspector > summary').click()
                await frame.locator('.memory-detail').filter(has_text='我对新的安排有些担心。').wait_for()
                assert await frame.locator('.memory-version').count() == 2
                assert await frame.locator('.memory-version .memory-emotion').filter(has_text='未记录').count() == 1
                # 已有情绪的节点仅修改标题，不能误提交未变化的情绪。
                await frame.locator('[data-page="maintenance"]').click()
                await frame.locator('[data-maintenance-tab="manual"]').click()
                await expect(frame.locator('#node-preview')).to_contain_text('"content_version": 2')
                await frame.locator('#node-select').select_option(harness.node['node_id'])
                await frame.locator('#node-title').fill('Synthetic emotional title')
                await frame.locator('#draft-operation').click()
                assert await frame.locator('#operation').input_value(), await frame.locator('#notifications').inner_text()
                preview = json.loads(await frame.locator('#operation').input_value())
                assert preview['patch'] == {'title': 'Synthetic emotional title'}
                # 拆分表单分别确定情绪，不把整件事的情绪复制给每个部分。
                await frame.locator('#action').select_option('split')
                await expect(frame.locator('.split-part')).to_have_count(2)
                assert not await frame.locator('.split-part .emotion-enabled').first.is_checked()
                await frame.locator('.split-part .part-title').nth(0).fill('合成部分一')
                await frame.locator('.split-part .part-title').nth(1).fill('合成部分二')
                await frame.locator('.split-part .part-content').nth(1).fill('另一项合成说明')
                await frame.locator('.split-part .emotion-enabled').first.check()
                await frame.locator('.split-part .emotion-tags').first.select_option(['joy'])
                await frame.locator('.split-part .emotion-description').first.fill('我期待这部分安排。')
                await frame.locator('#draft-operation').click()
                parts = json.loads(await frame.locator('#operation').input_value())['parts']
                assert parts[0]['emotion']['tags'] == [{'tag': 'joy', 'intensity': 0.5}] and parts[1]['emotion'] is None
                await frame.locator('#action').select_option('edit')

                assert {"app.js", "ui.js", "icons.js", "memory.js", "notes.js", "settings.js", "appearance.js", "maintenance.js", "text-cleaning.js", "cleaning-order.js", "style.css", "select.css"} <= {name for name, status in asset_responses if status == 200}
                assert all(status == 200 for _, status in asset_responses)

                await frame.locator('[data-page="models"]').click()
                original_plugin = harness.plugin
                original_stores = (harness.plugin.source_store, harness.plugin.memory_store, harness.plugin.jobs)
                await frame.locator("#setting-embedding_provider_id").select_option("embedding-test")
                await frame.locator("#setting-embedding_batch_size").fill("8")
                await frame.locator("#models-save").click()
                await frame.locator("#notifications .toast").filter(has_text="配置已保存并生效").last.wait_for()
                assert harness.plugin is original_plugin
                assert (harness.plugin.source_store, harness.plugin.memory_store, harness.plugin.jobs) == original_stores
                assert harness.plugin.config.embedding_batch_size == 8
                assert await frame.locator("#models-state").is_hidden()
                assert json.loads((Path(directory) / "config.json").read_text(encoding="utf-8-sig"))["embedding_batch_size"] == 8

                await frame.locator('[data-page="settings"]').click()
                await frame.locator('[data-settings-group="外观"]').click()
                await frame.locator('#appearance-mode').select_option('dark')
                await frame.locator('#settings-save').click()
                await expect(frame.locator("#settings-draft-state")).to_have_attribute("data-dirty", "false")
                assert any(cookie["name"] == "neomemory_appearance" for cookie in await page.context.cookies())
                await page.reload()
                await expect(frame.locator('html')).to_have_attribute('data-memory-theme', 'dark')
                await frame.locator('[data-page="settings"]').click()
                await frame.locator('[data-settings-group="高级"]').click()
                # The default 15000-byte budget is valid even though the slider hint is 128.
                budget = frame.locator('#setting-recall_budget_tokens')
                assert await budget.input_value() == '15000'
                assert await budget.evaluate('(input) => input.checkValidity()')
                priority = frame.locator("#setting-context_budget_priority")
                assert await priority.input_value() == "recall_first"
                assert await priority.locator("option").all_text_contents() == ["最相关长期记忆优先", "辅助资料优先"]
                assert "记忆注入字节上限" in await frame.locator('label[for="setting-recall_budget_tokens"]').inner_text()
                order = frame.locator("#setting-context_entry_order")
                assert await order.input_value() == "short_term_first"
                await order.select_option("relationship_first")
                await priority.select_option("context_first")
                await frame.locator("#settings-save").click()
                await expect(frame.locator("#settings-draft-state")).to_have_attribute("data-dirty", "false")
                await expect(frame.locator("#settings-save")).to_be_disabled()
                await frame.locator("#notifications .toast").filter(has_text="配置已保存并生效").last.wait_for()
                await expect(priority).to_have_value("context_first")
                assert harness.plugin.config.context_budget_priority == "context_first"
                assert harness.plugin.config.context_entry_order == "relationship_first"
                await frame.get_by_role("button", name="[高级] 辅助资料尝试顺序恢复默认", exact=True).click()
                await frame.get_by_role("button", name="[高级] 记忆注入优先级恢复默认", exact=True).click()
                assert await priority.input_value() == "recall_first"
                await frame.locator("#settings-save").click()
                await expect(frame.locator("#settings-draft-state")).to_have_attribute("data-dirty", "false")
                await expect(frame.locator("#settings-save")).to_be_disabled()
                await frame.locator("#notifications .toast").filter(has_text="配置已保存并生效").last.wait_for()
                assert harness.plugin.config.context_budget_priority == "recall_first"
                assert harness.plugin.config.context_entry_order == "short_term_first"
                assert harness.plugin.config.embedding_batch_size == 8
                assert harness.plugin is original_plugin
                assert (harness.plugin.source_store, harness.plugin.memory_store, harness.plugin.jobs) == original_stores

                old_strength_policy = harness.plugin.runtime.recall.policy.strength_policy
                old_node = await memory.get_node(harness.node["node_id"])
                await frame.locator("#setting-strength_importance_decay_weight").fill("2")
                await frame.locator("#setting-strength_wake_margin").fill("0.02")
                await frame.locator("#settings-save").click()
                await expect(frame.locator("#settings-draft-state")).to_have_attribute("data-dirty", "false")
                await expect(frame.locator("#settings-save")).to_be_disabled()
                await frame.locator("#notifications .toast").filter(has_text="配置已保存并生效").last.wait_for()
                assert harness.plugin.runtime.recall.policy.strength_policy.importance_decay_weight == 2
                assert harness.plugin.runtime.recall.policy.strength_policy.wake_margin == 0.02
                assert old_strength_policy.importance_decay_weight == 1
                await frame.get_by_role("button", name="[高级] 重要性减速系数恢复默认", exact=True).click()
                await frame.locator("#settings-save").click()
                await expect(frame.locator("#settings-draft-state")).to_have_attribute("data-dirty", "false")
                await expect(frame.locator("#settings-save")).to_be_disabled()
                await frame.locator("#notifications .toast").filter(has_text="配置已保存并生效").last.wait_for()
                assert harness.plugin.runtime.recall.policy.strength_policy.importance_decay_weight == 1
                assert harness.plugin.runtime.recall.policy.strength_policy.wake_margin == 0.02
                assert await memory.get_node(harness.node["node_id"]) == old_node
                assert harness.plugin is original_plugin

                await harness.plugin.maintenance.runner.stop()
                await memory.create_note(space_id=harness.space["space_id"], content_text="Initial note")
                await frame.locator('[data-page="notes"]').click()
                await frame.locator("#notes-list").get_by_role("button", name="编辑", exact=True).click()
                await frame.locator("#note-content").fill("Synthetic note after browser close")
                await frame.locator("#note-save").click()
                await frame.locator("#note-job").filter(has_text="queued").wait_for()
                queued = [j for j in await harness.plugin.jobs.list(scope_key=harness.space["space_id"]) if j["kind"] == "note"]
                assert len(queued) == 1 and queued[0]["state"] == "queued"
                await page.close()
                await harness.plugin.maintenance.runner.start()
                await await_job(harness, queued[0]["job_id"])

                page = await browser.new_page(viewport={"width": 1280, "height": 1000})
                page.on("pageerror", lambda error: errors.append(str(error)))
                await page.goto(f"http://127.0.0.1:{port}/host-console")
                frame = page.frame_locator("#plugin")
                await frame.locator("#status").filter(has_text="已连接").wait_for()
                await frame.locator("#space").select_option(harness.space["space_id"])
                await frame.locator('[data-page="notes"]').click()
                await frame.locator("#notes-list").filter(has_text="Synthetic note after browser close").wait_for()
                await harness.plugin.maintenance.runner.stop()
                await frame.locator("#notes-list").get_by_role("button", name="编辑", exact=True).click()
                await frame.locator("#note-content").fill("Synthetic note after runtime reload")
                await frame.locator("#note-save").click()
                await frame.locator("#note-job").filter(has_text="queued").wait_for()
                queued = [j for j in await harness.plugin.jobs.list(scope_key=harness.space["space_id"]) if j["kind"] == "note" and j["state"] == "queued"]
                assert len(queued) == 1
                await page.close()
                await harness.reload()
                await await_job(harness, queued[0]["job_id"])
                assert harness.plugin.config.embedding_batch_size == 8
                assert harness.plugin.runtime.recall.policy.strength_policy.wake_margin == 0.02
                page = await browser.new_page(viewport={"width": 1280, "height": 1000})
                page.on("pageerror", lambda error: errors.append(str(error)))
                await page.goto(f"http://127.0.0.1:{port}/host-console")
                frame = page.frame_locator("#plugin")
                await frame.locator("#status").filter(has_text="已连接").wait_for()
                await frame.locator("#space").select_option(harness.space["space_id"])
                await frame.locator('[data-page="notes"]').click()
                await frame.locator("#notes-list").filter(has_text="Synthetic note after runtime reload").wait_for()
                assert await frame.locator("#notes-list article").count() == 1
                governance_result = await governance_lifecycle(page, frame, harness)
                await page.screenshot(path="/tmp/neomemory-host-console-desktop.png", full_page=True)
                await page.set_viewport_size({"width": 390, "height": 844})
                for name in ("memory", "notes", "models", "settings", "maintenance"):
                    await frame.locator(f'[data-page="{name}"]').click()
                    assert await frame.locator("html").evaluate("node => node.scrollWidth <= window.innerWidth"), name
                await text_cleaning_controls(page, frame, harness)
                assert await frame.locator("html").evaluate("node => node.scrollWidth <= window.innerWidth")
                await page.screenshot(path="/tmp/neomemory-host-console-mobile.png", full_page=True)
                assert errors == []
                assert harness.plugin.context.embedding.calls == []
                assert harness.plugin.context.calls == []
                await browser.close()
            print(json.dumps({"ok": True, "real_host_routes": True, "sandbox": "opaque origin", "asset_modules": 12, "text_cleaning_controls": True,
                              "config_live_apply_same_instance": True, "config_restart_persistence": True, "browser_close_continuation": True,
                              "strength_live_apply_and_single_reset": True,
                              "emotion_governance_versions_and_split_form": True,
                              "budget_priority_save_reset": True, "context_order_save_reset": True, "context_diagnostic_panel": True, "budget_unit_label": "UTF-8 bytes",
                              "version_provenance_node_profile_relationship": True,
                              "title_only_governance_and_historical_snapshot": True,
                              "inherited_version_navigation": True,
                              "queued_job_runtime_reload": True, "external_model_requests": 0,
                              "governance_lifecycle": governance_result,
                              "parent_page": "minimal bridge contract; not complete Vue login"}, ensure_ascii=False))
        finally:
            server.should_exit = True
            await server_task
            sock.close()
            await harness.stop()


if __name__ == "__main__":
    asyncio.run(main())
