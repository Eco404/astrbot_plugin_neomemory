"""Check scope-specific drafts, model inheritance and collection of changed fields."""

import asyncio
import functools
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

from playwright.async_api import async_playwright, expect

from tests.ui.full_smoke import MOCK_BRIDGE, QuietHandler

POLICY = r"""
const get = window.AstrBotPluginView.apiGet, post = window.AstrBotPluginView.apiPost;
const persona = {shared_recall_enabled:false,allow_group_read_private:false,external_limit:2,external_budget_percent:20};
const policies = {};
function policy(id) {
 return {space_id:id,persona_id:'demo',...persona,exclude_from_shared_recall:false,model_mode:'inherit',
   provider_id:'',topic_provider_id:'',shared_spaces:persona.shared_recall_enabled ? [{space_id:'space-1',display_label:'林川'}] : [],...policies[id]};
}
window.AstrBotPluginView.apiGet = async (endpoint,args) => endpoint==='space/policy' ? policy(args.space_id) : get(endpoint,args);
window.AstrBotPluginView.apiPost = async (endpoint,body) => {
 if(endpoint!=='space/policy')return post(endpoint,body);
 window.savedPolicy=body;
 const local=policies[body.space_id]??={};
 for(const [k,v] of Object.entries(body)) {
   if(k in persona)persona[k]=v;
   else if(k!=='space_id')local[k]=v;
 }
 return policy(body.space_id);
};
"""


async def main():
    root = str(Path(__file__).resolve().parents[2] / 'views' / 'neomemory')
    server = ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(QuietHandler, directory=root))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page(viewport={'width': 390, 'height': 844})
            errors = []
            page.on('pageerror', lambda error: errors.append(str(error)))
            await page.add_init_script(MOCK_BRIDGE + POLICY)
            await page.goto(f'http://127.0.0.1:{server.server_port}/index.html')
            await page.locator('#memory-list > article').first.wait_for()
            await page.locator('[data-page="settings"]').click()
            await page.get_by_role('tab', name='记忆空间').click()
            await expect(page.locator('#space-bar')).to_be_visible()
            await expect(page.locator('#policy-save')).to_be_disabled()
            await page.locator('#policy-shared_recall_enabled').check()
            await page.locator('#policy-model_mode').select_option('fixed')
            await page.locator('#policy-save').click()
            assert not await page.evaluate('window.savedPolicy')
            await page.locator('#policy-provider_id').select_option('llm-1')
            await page.locator('#space').select_option('space-2')
            await page.get_by_role('button', name='留下继续编辑').click()
            await expect(page.locator('#space')).to_have_value('space-1')
            await page.locator('#policy-save').click()
            await expect(page.locator('#policy-save')).to_be_disabled()
            assert await page.evaluate('window.savedPolicy') == {
                'space_id':'space-1', 'model_mode':'fixed', 'provider_id':'llm-1', 'shared_recall_enabled':True,
            }
            await page.locator('#space').select_option('space-2')
            await expect(page.locator('#policy-space-title')).to_contain_text('松子')
            await expect(page.locator('#policy-shared_recall_enabled')).to_be_checked()
            await page.locator('#policy-sources summary').click()
            await expect(page.locator('#policy-source-list')).to_contain_text('林川')
            await expect(page.locator('#policy-model_mode')).to_have_value('inherit')
            await page.locator('#policy-model_mode').select_option('chat')
            await page.locator('[data-page="memory"]').click()
            await page.get_by_role('button', name='放弃并离开').click()
            await expect(page.locator('#page-memory')).to_be_visible()
            assert not errors, errors
            await browser.close()
        print('space settings browser passed')
    finally:
        server.shutdown()
        server.server_close()


if __name__ == '__main__':
    asyncio.run(main())
