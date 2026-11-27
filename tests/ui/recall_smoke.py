"""Recall inspection, scoped history and late responses in a real browser."""
import asyncio
import functools
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

from playwright.async_api import async_playwright, expect

from tests.ui.full_smoke import MOCK_BRIDGE, QuietHandler

RECALL = r"""
const get = window.AstrBotPluginView.apiGet, post = window.AstrBotPluginView.apiPost;
let recording = false;
const snapshot = (mode, query) => ({
  query, mode, created_at: 1700000000, elapsed_ms: 42, reason: '',
  items: [{node_id:'a', node_type:'event', title:'观星约定', score:0.72}],
  text: '合成记忆正文',
});
window.AstrBotPluginView.apiGet = async (endpoint,args={}) => {
  if (endpoint !== 'recall/history') return get(endpoint,args);
  const sample = {enabled: recording, records: recording && args.space_id === 'space-1' ?
    [...['passive','active','external'].map(mode => snapshot(mode,'第一空间的查询')),
     {...snapshot('passive',''),media_placeholder:'[图片] [表情]', cache_errors:{write:'TimeoutError'}},
     snapshot('passive','')] : []};
  if (window.holdHistory) {
    window.holdHistory = false;
    await new Promise(resolve => window.resumeHistory=resolve);
  }
  return sample;
};
window.AstrBotPluginView.apiPost = async (endpoint,body) => {
  if (endpoint === 'recall/recording') { recording=body.enabled; return {enabled:recording}; }
  if (endpoint !== 'recall/test') return post(endpoint,body);
  if (window.holdTest) await new Promise(resolve => window.resumeTest=resolve);
  return {...snapshot('test',body.query),index_coverage:{usable:3,total:5,truncated:true},
    index_deferred:'space_busy',last_index:{state:'succeeded',progress_done:3,progress_total:3,updated_at:1700000000},
    query_vectors:{total:2,reused:1,provider_calls:1},similarity_threshold:0.45,min_strength:0.1,
    preview_candidates:[{title:'其他安排',node_type:'atomic',similarity:0.1,reason:'低于相似度阈值'}]};
};
"""


async def main():
    root = Path(__file__).resolve().parents[2]
    server = ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(
        QuietHandler, directory=str(root / 'views/neomemory')))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch()
            for width in (1440, 390):
                page = await browser.new_page(viewport={'width': width, 'height': 850})
                errors = []
                page.on('pageerror', lambda error: errors.append(str(error)))
                await page.add_init_script(MOCK_BRIDGE + RECALL)
                await page.goto(f'http://127.0.0.1:{server.server_port}/index.html')
                await expect(page.locator('#space')).to_have_value('space-1')
                await page.locator('[data-page=maintenance]').click()
                await page.get_by_role('tab', name='召回测试').click()
                query, run = page.locator('#recall-query'), page.locator('#recall-run')
                result, history = page.locator('#recall-result'), page.locator('#recall-history')
                toggle = page.get_by_role('switch', name='记录实际召回')
                await expect(history).to_contain_text('已关闭')
                await expect(toggle).not_to_be_checked()
                await expect(run).to_be_disabled()
                await query.fill('今晚在哪里观星')
                await run.click()
                await expect(result).to_contain_text('长期记忆预览')
                await expect(result).to_contain_text('综合分 0.720')
                await expect(result).to_contain_text('本空间可检索向量 3 / 5（已达候选上限）')
                await expect(result).to_contain_text('空间正在执行其他任务')
                await expect(result).to_contain_text('已完成 · 3 / 3 条')
                await expect(result).to_contain_text('查询向量复用 1 / 2')
                await result.get_by_text('未入选的候选', exact=False).click()
                await expect(result.get_by_text('低于相似度阈值', exact=False)).to_be_visible()
                await expect(history).to_contain_text('已关闭')
                await toggle.check()
                await expect(history.locator('details')).to_have_count(5)
                await history.get_by_text('[图片] [表情]', exact=False).click()
                await expect(history.get_by_text('写入向量缓存失败（TimeoutError）')).to_be_visible()
                await expect(history.get_by_text('[无文字]', exact=False)).to_be_visible()
                await history.get_by_text('外部记忆接口', exact=False).click()
                await expect(history.get_by_text('外部接口返回内容')).to_be_visible()
                await page.evaluate('window.holdHistory=true; window.holdTest=true')
                await run.click()
                await page.locator('#recall-refresh').click()
                await page.wait_for_function("typeof window.resumeHistory === 'function' && typeof window.resumeTest === 'function'")
                await page.locator('#space').select_option('space-2')
                await expect(history).to_contain_text('当前空间尚无召回记录')
                await page.evaluate('window.holdTest=false; window.resumeTest(); window.resumeHistory()')
                await expect(result).to_be_empty()
                await expect(query).to_have_value('')
                await expect(history).not_to_contain_text('第一空间')
                await expect(run).to_be_disabled()
                await toggle.uncheck()
                await expect(history).to_contain_text('已关闭')
                await page.locator('#space').select_option('')
                await expect(result).to_contain_text('请先选择记忆空间')
                await expect(toggle).to_be_enabled()
                assert await page.evaluate('document.documentElement.scrollWidth <= innerWidth')
                assert not errors, errors
                await page.close()
            await browser.close()
    finally:
        server.shutdown()
        server.server_close()
    print('Recall inspection passed for desktop and mobile.')


if __name__ == '__main__':
    asyncio.run(main())
