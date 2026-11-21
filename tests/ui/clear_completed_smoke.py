"""Completed-task confirmation, scope, retries and pending state in Chromium."""
import asyncio
import functools
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

from playwright.async_api import async_playwright, expect

from tests.ui.full_smoke import MOCK_BRIDGE, QuietHandler

TASKS = r"""
const originalGet=window.AstrBotPluginView.apiGet;
const originalPost=window.AstrBotPluginView.apiPost;
window.testJobs=[...Array.from({length:25},(_,i)=>({job_id:'done-'+i,scope_key:'space-1',state:'succeeded'})),
 ...Array.from({length:3},(_,i)=>({job_id:'other-'+i,scope_key:'space-2',state:'succeeded'})),
 ...['failed','cancelled','queued','running','retry_wait'].map(state=>({job_id:state,scope_key:'space-1',state}))
].map(j=>({...j,kind:'model_test',step:'complete',progress_done:1,progress_total:1,attempt:1,max_attempts:3}));
window.failClear=false;window.deferClear=false;
window.AstrBotPluginView.apiGet=async(endpoint,args={})=>{
 if(endpoint==='tasks'){
  const rows=testJobs.filter(j=>!args.scope_key||j.scope_key===args.scope_key);
  return {items:rows.slice(args.offset,args.offset+args.limit),has_more:args.offset+args.limit<rows.length};
 }
 return originalGet(endpoint,args);
};
window.AstrBotPluginView.apiPost=async(endpoint,body)=>{
 if(endpoint!=='tasks/clear-completed')return originalPost(endpoint,body);
 testCalls.push({endpoint,body});
 if(window.failClear)throw Error('合成清理失败，请重试。');
 if(window.deferClear)await new Promise(resolve=>window.releaseClear=resolve);
 const old=testJobs.length;
 window.testJobs=testJobs.filter(j=>j.state!=='succeeded'||(body.scope_key&&j.scope_key!==body.scope_key));
 return {deleted_count:old-testJobs.length,removed_node_count:0};
};
"""


async def main():
    root = Path(__file__).resolve().parents[2]
    artifacts = root/'data/evaluations/clear-completed-webui'
    artifacts.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(QuietHandler, directory=str(root/'views/neomemory')))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch()
            for theme in ('light', 'dark'):
                for width in (1440, 900, 390):
                    page = await browser.new_page(viewport={'width':width,'height':900}, color_scheme=theme)
                    errors = []
                    page.on('pageerror', lambda error: errors.append(str(error)))
                    await page.add_init_script(MOCK_BRIDGE + TASKS)
                    await page.goto(f'http://127.0.0.1:{server.server_port}/index.html')
                    await page.locator('#status').filter(has_text='已连接').wait_for()
                    await page.locator('[data-page=maintenance]').click()
                    await page.locator('#tasks article').first.wait_for()
                    assert await page.locator('#tasks article').count() == 20
                    trigger = page.locator('#clear-completed-tasks')
                    dialog = page.get_by_role('dialog', name='清空已完成任务？')
                    confirm = page.locator('#clear-completed-confirm')
                    await trigger.click()
                    await expect(dialog).to_be_visible()
                    await expect(page.locator('#clear-completed-cancel')).to_be_focused()
                    assert '当前空间' in await dialog.inner_text()
                    assert not await page.evaluate("testCalls.some(c=>c.endpoint==='tasks/clear-completed')")
                    await page.keyboard.press('Escape')
                    await expect(dialog).not_to_be_visible()
                    await expect(trigger).to_be_focused()
                    await trigger.click()
                    await page.locator('#clear-completed-cancel').click()
                    await expect(dialog).not_to_be_visible()
                    assert await page.evaluate('testJobs.length') == 33
                    # Clear from a later page, including successful rows not currently visible.
                    await page.locator('#next-tasks').click()
                    await page.locator('#tasks').filter(has_text='done-20').wait_for()
                    await page.locator('#tasks article').filter(has_text='done-20').get_by_role('radio').check()
                    await page.locator('#task-detail-panel > summary').click()
                    await page.evaluate('window.failClear=true')
                    await trigger.click()
                    await confirm.click()
                    await expect(page.locator('#clear-completed-error')).to_contain_text('合成清理失败')
                    await expect(dialog).to_be_visible()
                    assert await page.evaluate('testJobs.length') == 33
                    await page.evaluate('window.failClear=false;window.deferClear=true')
                    await confirm.click()
                    await page.wait_for_function("typeof releaseClear==='function'")
                    await expect(confirm).to_be_disabled()
                    await expect(page.locator('#clear-completed-cancel')).to_be_disabled()
                    await page.keyboard.press('Escape')
                    await expect(dialog).to_be_visible()
                    assert await page.evaluate("testCalls.filter(c=>c.endpoint==='tasks/clear-completed').length") == 2
                    await page.screenshot(path=str(artifacts/f'{theme}-{width}-pending.png'))
                    assert await page.evaluate('document.documentElement.scrollWidth<=innerWidth')
                    box = await dialog.bounding_box()
                    assert box['x'] >= 0 and box['x']+box['width'] <= width+1
                    await page.evaluate('window.deferClear=false;releaseClear()')
                    await expect(dialog).not_to_be_visible()
                    await expect(page.locator("#notifications .toast").filter(has_text='已清空 25 个').last).to_be_visible()
                    await expect(page.locator('#previous-tasks')).to_be_disabled()
                    await expect(page.locator('#task-detail')).to_have_text('')
                    assert await page.evaluate('testJobs.length') == 8
                    assert await page.evaluate("testCalls.filter(c=>c.endpoint==='tasks/clear-completed').at(-1).body.scope_key") == 'space-1'
                    await page.locator('#all-tasks').check()
                    await trigger.click()
                    assert '范围：所有空间。' in await dialog.inner_text()
                    await confirm.click()
                    await expect(page.locator("#notifications .toast").filter(has_text='已清空 3 个').last).to_be_visible()
                    assert await page.evaluate('testJobs.length') == 5
                    assert await page.evaluate("testCalls.filter(c=>c.endpoint==='tasks/clear-completed').at(-1).body") == {}
                    await trigger.click()
                    await confirm.click()
                    await expect(page.locator("#notifications .toast").filter(has_text='已清空 0 个').last).to_be_visible()
                    assert not errors, errors
                    await page.close()
            await browser.close()
            print('clear completed: 6 viewport/theme cases passed; confirmation, cancel/Escape/focus, scope, all pages, pending lock, retry, detail clearing and zero result')
    finally:
        server.shutdown();server.server_close()


if __name__ == '__main__':
    asyncio.run(main())
