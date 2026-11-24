"""Chromium checks for connection-only status, dismissible toasts and task clocks.

Uses synthetic bridge records and the browser clock; no live model or source data.
"""
import asyncio
from datetime import datetime, timedelta, timezone
import functools
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

from playwright.async_api import async_playwright, expect

from tests.ui.full_smoke import MOCK_BRIDGE, QuietHandler

TASKS = r"""
const now=Date.now()/1000;
window.testJobs=['running','retry_wait','failed','succeeded'].map(state=>({
 job_id:'clock-'+state,kind:'rebuild',scope_key:'space-1',state,step:'topic_model:2/8',
 progress_done:5,progress_total:8,attempt:2,max_attempts:3,
 created_at:now-70,updated_at:now-10,available_at:now+20,
 error_type:'temporary',error_code:state==='retry_wait'?'model_queue_timeout':'extraction_timeout'
}));
const originalGet=window.AstrBotPluginView.apiGet;
window.AstrBotPluginView.apiGet=async(endpoint,args={})=>{
 if(endpoint==='availability')return {available:false,blocking_jobs:[testJobs[0]],automatic:{state:'enabled'}};
 const result=await originalGet(endpoint,args);
 const stamp=Date.now()/1000;
 if(endpoint==='task' && args.job_id==='clock-failed') result.attempts=[
   {ended_at:stamp-30,action:'execute',step:'validate',state:'failed',model_calls:2,commits:1,remaining:4,http_status:503},
   {ended_at:stamp-10,action:'resume',step:'validate',state:'failed',model_calls:0,commits:0,remaining:4,queued_without_call:true}
 ];
 if(endpoint==='task' && args.job_id==='clock-failed') result.validation_errors=[
   {type:'source_budget_exceeded',actual_chars:101,limit_chars:100,actual_tokens:81,limit_tokens:80},
   {type:'prompt_budget_exceeded',actual_bytes:125,limit_bytes:128,reserved_feedback_bytes:8},
   {type:'output_capacity_exceeded',limit_bytes:512}
 ];
 if(endpoint==='ingress') result.health.oldest_pending_at=stamp-65;
 if(endpoint==='health'){
   result.last_source_write_at=stamp-20;
   result.ingress.oldest_unpersisted_at=stamp-120;
   result.companion_delivery_progress={blocked_receipt:'delivery-42',retry_at:stamp+10};
   result.maintenance={automatic_recovery:{total:4,recovered:2,manual:1,pending:1},
     longest_failure_streak:{job_id:'clock-failed',step:'validate',reason:'storage_busy',streak:3}};
 }
 return result;
};
"""


async def notify(page, message, error=False, key=None):
    await page.evaluate("""async ({message,error,key})=>{
      const ui=await import('./ui.js');
      ui.notify(message,error,key ? {key} : {});
    }""", {'message': message, 'error': error, 'key': key})
    return page.locator('#notifications .toast').filter(has_text=message)


async def verify_notifications(page):
    await expect(page.locator('#status')).to_have_text('已连接')
    await expect(page.locator('#notifications .toast')).to_have_count(0)
    await page.mouse.move(0, 0)
    normal = await notify(page, '设置已保存')
    failure = await notify(page, '独立操作失败', True)
    await expect(normal).to_be_visible()
    await expect(failure).to_have_class('toast error')
    await expect(page.locator('#status')).to_have_text('已连接')
    await expect(page.locator('#status')).not_to_have_class('error')
    await failure.get_by_role('button', name='关闭通知').click()
    await expect(failure).to_have_count(0)
    await page.mouse.move(0, 0)
    await page.clock.run_for(6100)
    await expect(normal).to_have_count(0)

    hovered = await notify(page, '鼠标悬停保留通知')
    await page.clock.run_for(2000)
    await hovered.hover()
    await page.clock.run_for(10000)
    await expect(hovered).to_be_visible()
    await page.mouse.move(0, 0)
    await page.clock.run_for(3000)
    await expect(hovered).to_be_visible()
    await page.clock.run_for(1100)
    await expect(hovered).to_have_count(0)

    focused = await notify(page, '键盘阅读保留通知')
    await focused.get_by_role('button', name='关闭通知').focus()
    await page.clock.run_for(10000)
    await expect(focused).to_be_visible()
    await page.locator('#refresh').focus()
    await page.clock.run_for(6100)
    await expect(focused).to_have_count(0)

    keyed = await notify(page, '等待自动恢复', key='retry-status')
    await notify(page, '等待自动恢复', key='retry-status')
    await expect(keyed).to_have_count(1)
    await page.evaluate("async()=>{const ui=await import('./ui.js');ui.dismissNotification('retry-status')}")
    await expect(keyed).to_have_count(0)
    unsafe = await notify(page, '<img src=x onerror=alert(1)>')
    await expect(unsafe).to_contain_text('<img src=x onerror=alert(1)>')
    await expect(unsafe.locator('img')).to_have_count(0)
    await unsafe.get_by_role('button', name='关闭通知').click()
    await expect(page.locator('#status')).to_have_text('已连接')


async def verify_progress(page):
    # Refresh creation times after notification tests have advanced the clock.
    await page.evaluate("""()=>{
      const now=Date.now()/1000;
      testJobs.forEach(job=>Object.assign(job,{created_at:now-70,updated_at:now-10,available_at:now+20}));
    }""")
    await page.locator('[data-page=maintenance]').click()
    cards = {state: page.locator('#tasks article').filter(has_text='clock-'+state)
             for state in ('running', 'retry_wait', 'failed', 'succeeded')}
    for state in ('running', 'retry_wait', 'succeeded'):
        await expect(cards[state].locator('.error')).to_have_count(0)
    await expect(cards['failed'].locator('.error')).to_contain_text('extraction_timeout')
    await expect(cards['retry_wait'].locator('.task-recovery')).to_contain_text('自动重试')
    await expect(cards['retry_wait'].locator('.task-recovery')).to_contain_text('本次排队未发起模型调用')
    await expect(cards['running']).not_to_contain_text('extraction_timeout')
    await expect(cards['succeeded']).not_to_contain_text('extraction_timeout')
    await page.clock.run_for(1000)
    running_before = await cards['running'].locator('.task-timing').inner_text()
    finished_before = await cards['succeeded'].locator('.task-timing').inner_text()
    retry_before = await cards['retry_wait'].locator('.task-recovery').inner_text()
    calls_before = await page.evaluate('testGetCalls.length')
    await page.clock.run_for(1000)
    assert await cards['running'].locator('.task-timing').inner_text() != running_before
    assert await cards['succeeded'].locator('.task-timing').inner_text() == finished_before
    assert await cards['retry_wait'].locator('.task-recovery').inner_text() != retry_before
    # A local timer updates the display without fetching the server each second.
    # Existing polling is every three seconds; at most one ordinary poll may
    # cross this interval, and clocks do not introduce a new API endpoint.
    recent = await page.evaluate('(offset)=>testGetCalls.slice(offset)', calls_before)
    endpoints = [call['endpoint'] for call in recent]
    assert all(endpoint in {'tasks', 'ingress', 'ingress/health'} for endpoint in endpoints), recent
    assert all(endpoints.count(endpoint) <= 1 for endpoint in endpoints), recent
    await page.evaluate("""()=>Object.assign(testJobs.find(j=>j.job_id==='clock-retry_wait'),
      {state:'running',error_type:null,error_code:null})""")
    await page.locator('#refresh').click()
    await expect(cards['retry_wait'].locator('.task-recovery')).to_have_count(0)
    await expect(cards['retry_wait']).to_contain_text('执行中')
    await expect(cards['retry_wait'].locator('.error')).to_have_count(0)
    await cards['failed'].get_by_role('radio').check()
    await page.locator('#task-detail-panel > summary').click()
    for message in ('原文用量 101 / 100 字符', '原文用量 81 / 80 估算 token',
                    '提示词用量 125 / 128 UTF-8 字节', '为纠正反馈预留 8 字节'):
        await expect(page.locator('#task-diagnostics')).to_contain_text(message)
    await expect(page.locator('#task-diagnostics')).not_to_contain_text('模型输出用量')
    await page.locator('#task-attempts > summary').click()
    rows = page.locator('#task-attempt-list li')
    await expect(rows).to_have_count(2)
    for message in ('从断点继续', '模型调用 0 次', '曾等待模型名额超时', '新增提交 0 次', '剩余 4'):
        await expect(rows.first).to_contain_text(message)
    for message in ('模型调用 2 次', '新增提交 1 次', 'HTTP 503'):
        await expect(rows.last).to_contain_text(message)
    await cards['succeeded'].get_by_role('radio').check()
    await expect(rows).to_have_count(0)
    await expect(page.locator('#task-attempts')).to_be_hidden()
    await expect(page.locator('#ingress-summary')).to_contain_text('最久待写 2:00')
    await expect(page.locator('#ingress-summary')).to_contain_text('最近写入')
    await expect(page.locator('#ingress-unpersisted')).to_contain_text('自动按序写入')
    await page.locator('[data-maintenance-tab=status]').click()
    await expect(page.locator('#automatic-recovery')).to_contain_text('50%（2 / 4）')
    await expect(page.locator('#failure-streak')).to_contain_text('storage_busy · 同阶段同类故障连续 3 次')
    await expect(page.locator('#companion-delivery-issue')).to_contain_text('等待重试陪伴回执 delivery-42')
    await expect(page.locator('#status')).to_have_text('已连接')


async def main():
    root = Path(__file__).resolve().parents[2]
    artifacts = root/'data/evaluations/notifications-progress-webui'
    artifacts.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(QuietHandler, directory=str(root/'views/neomemory')))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch()
            for theme in ('light', 'dark'):
                for width in (1440, 390):
                    page = await browser.new_page(viewport={'width':width,'height':900}, color_scheme=theme)
                    errors = []
                    page.on('pageerror', lambda error: errors.append(str(error)))
                    frozen = datetime(2026, 11, 18, tzinfo=timezone.utc)
                    await page.clock.install(time=frozen)
                    await page.clock.pause_at(frozen+timedelta(seconds=1))
                    await page.add_init_script(MOCK_BRIDGE+TASKS)
                    await page.goto(f'http://127.0.0.1:{server.server_port}/index.html')
                    await expect(page.locator('#status')).to_have_text('已连接')
                    await verify_notifications(page)
                    await verify_progress(page)
                    toast = await notify(page, '后台任务已恢复，继续处理剩余内容。')
                    box = await toast.bounding_box()
                    assert box['x'] >= 0 and box['x']+box['width'] <= width+1
                    assert await page.evaluate('document.documentElement.scrollWidth<=innerWidth')
                    await page.screenshot(path=str(artifacts/f'{theme}-{width}.png'))
                    assert not errors, errors
                    await page.close()
            await browser.close()
            print('notifications/progress: 4 viewport/theme cases passed; notifications, clocks, retry recovery, attempt history, queue age, last write and blocked delivery')
    finally:
        server.shutdown()
        server.server_close()


if __name__ == '__main__':
    asyncio.run(main())
