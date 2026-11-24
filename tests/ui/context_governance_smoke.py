"""Browser regression for scoped manual context governance with a synthetic bridge."""
import asyncio
import functools
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

from playwright.async_api import async_playwright

from tests.ui.full_smoke import MOCK_BRIDGE, QuietHandler


CONTEXT = r"""
const get = window.AstrBotPluginView.apiGet;
window.AstrBotPluginView.apiGet=async(endpoint,args={})=>{
 if(endpoint==='memory/profile-users')return {items:args.space_id==='group'?[]:[
  {logical_user_id:'user-1',display_name:'小林'},{logical_user_id:'user-2',display_name:'明月'}]};
 if(endpoint!=='memory/context')return get(endpoint,args);
 const base={generation_id:'gen-1',status:'active',content_version:3,updated_at:200,logical_user_name:'小林'};
 const slots={format:'relationship.slots.v1',long_term:{body:'我愿意听{{user:user-1}}慢慢说。'},aftereffect:{body:'我还在惦记这次谈话。'}};
 if(args.kind==='relationships'){
  window.relationshipSpaces.push(args.space_id);
  return {items:[{...base,content_version:2,relationship_id:'relationship-1',state:slots}],has_more:false};
 }
 if(args.kind==='profiles'){
  if(args.space_id==='space-1' && window.deferProfiles)await new Promise(resolve=>window.releaseProfiles=resolve);
  if(args.space_id==='group')return {items:[],has_more:false};
  if(args.logical_user_id==='user-2')return {items:[{...base,logical_user_name:'明月',profile_id:'profile-2',description:'热爱阅读',tags:[]}],has_more:false};
  return {items:[{...base,profile_id:'profile-1',description:'平日徒步',tags:['户外']}],has_more:false};
 }
 return {items:[{...base,state_id:'summary-1',conversation_id:'host.v1:opaque',summary_text:'周末的安排'}],has_more:false};
};
window.relationshipSpaces=[];
"""


async def check_context_governance(page):
    manual = page.locator('[data-maintenance-tab="manual"]')
    operations = 'testCalls.filter(c => c.body.operation?.entity_kind)'

    profile = page.locator('[data-context-governance="profiles"] article').first
    tabs = page.get_by_role('tablist', name='人工记忆治理')
    await tabs.get_by_role('tab', name='用户画像').click()
    chooser = page.get_by_label('选择用户', exact=True)
    await chooser.select_option('user-2')
    await profile.get_by_text('热爱阅读').wait_for()
    await chooser.select_option('user-1')
    await profile.get_by_text('平日徒步').wait_for()
    await profile.get_by_role('button', name='编辑', exact=True).click()
    await profile.get_by_label('画像正文').fill('每周末徒步')
    await profile.get_by_label('标签').fill('户外\n远足')
    await profile.get_by_role('button', name='提交修改').click()
    await page.wait_for_function(operations + '.length === 1')
    body = await page.evaluate(operations + '[0].body')
    assert body['space_id'] == 'space-1'
    assert body['operation'] == {
        'entity_kind': 'profile', 'entity_id': 'profile-1', 'action': 'edit',
        'expected_generation_id': 'gen-1', 'expected_version': 3,
        'patch': {'description': '每周末徒步', 'tags': ['户外', '远足']},
    }
    tasks = page.locator('[data-maintenance-tab="tasks"][aria-selected="true"]')
    await tasks.wait_for()
    await manual.click()

    relationship = page.locator('[data-context-governance="relationships"] article').first
    await tabs.get_by_role('tab', name='人格关系').click()
    await relationship.get_by_role('button', name='编辑', exact=True).click()
    assert await relationship.get_by_label('长期理解').input_value() == '我愿意听{{user:user-1}}慢慢说。'
    await relationship.get_by_label('近期余韵').fill('我很期待。')
    await relationship.get_by_role('button', name='提交修改').click()
    await page.wait_for_function(operations + '.length === 2')
    body = await page.evaluate(operations + '[1].body')
    assert body['operation']['expected_version'] == 2
    assert body['operation']['patch'] == {'long_term': '我愿意听{{user:user-1}}慢慢说。', 'aftereffect': '我很期待。'}
    await tasks.wait_for()
    await manual.click()

    summary = page.locator('[data-context-governance="short_term"] article').first
    await tabs.get_by_role('tab', name='短期记忆').click()
    await summary.get_by_role('button', name='删除', exact=True).click()
    await summary.get_by_role('button', name='取消', exact=True).click()
    assert await page.evaluate(operations + '.length') == 2
    await summary.get_by_role('button', name='删除', exact=True).click()
    await summary.get_by_role('button', name='确认删除', exact=True).click()
    await page.wait_for_function(operations + '.length === 3')
    body = await page.evaluate(operations + '[2].body')
    assert body['operation'] == {'entity_kind': 'short_term', 'entity_id': 'summary-1', 'action': 'delete',
                                 'expected_generation_id': 'gen-1', 'expected_version': 3}
    await tasks.wait_for()
    await manual.click()
    await page.evaluate("window.deferProfiles=true")
    await tabs.get_by_role('tab', name='用户画像').click()
    await manual.click()
    await page.wait_for_function("typeof window.releaseProfiles === 'function'")
    await page.select_option('#space', 'group')
    await page.locator('[data-context-governance="profiles"]').get_by_text('当前空间暂无用户画像。').wait_for()
    await page.evaluate("window.deferProfiles=false; releaseProfiles()")
    assert await profile.count() == 0
    assert not await page.evaluate("relationshipSpaces.includes('group')")
    assert await page.evaluate("document.documentElement.scrollWidth <= innerWidth"), await page.evaluate(
        "[...document.querySelectorAll('body *')].filter(el=>el.getBoundingClientRect().right>innerWidth).map(el=>el.id||el.tagName).slice(-25)")
    assert not await page.locator('[data-context-governance="relationships"]').is_visible()
    await page.select_option('#space', 'space-1')
    await tabs.get_by_role('tab', name='人格关系').click()
    await relationship.wait_for()
    await page.select_option('#space', 'group')
    assert await tabs.get_by_role('tab', name='人格关系').count() == 0
    assert await tabs.get_by_role('tab', name='长期记忆').get_attribute('aria-selected') == 'true'


async def main():
    root = Path(__file__).resolve().parents[2] / 'views/neomemory'
    server = ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(QuietHandler, directory=root))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch()
            page = await browser.new_page(viewport={'width': 390, 'height': 844})
            errors = []
            page.on('pageerror', lambda error: errors.append(str(error)))
            await page.add_init_script(MOCK_BRIDGE + CONTEXT)
            await page.goto(f'http://127.0.0.1:{server.server_port}/index.html')
            await page.locator('#status').filter(has_text='已连接').wait_for()
            await page.locator('[data-page="maintenance"]').click()
            await page.locator('[data-maintenance-tab="manual"]').click()
            await check_context_governance(page)
            assert not errors, errors
            await browser.close()
            print('Scoped governance forms, edit and delete tasks, confirmations, stale responses and mobile layout passed.')
    finally:
        server.shutdown()
        server.server_close()


if __name__ == '__main__':
    asyncio.run(main())
