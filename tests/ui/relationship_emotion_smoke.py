"""Real Chromium checks for relationship slots and independent emotion editing.

Uses synthetic bridge records only; host persistence has a separate smoke test.
"""
import asyncio
import functools
import json
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

from playwright.async_api import async_playwright

from tests.ui.full_smoke import MOCK_BRIDGE, QuietHandler

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / 'data/evaluations/relationship-emotion-ui'
FIXTURES = r"""
nodes[0].emotion={tags:[{tag:'joy',intensity:0.8},{tag:'concern',intensity:0.2}],description:'这次安排让我期待，也有些担心。'};
nodes[1].emotion={tags:[{tag:'trust',intensity:0.65},{tag:'affection',intensity:0.15}],description:'这次交谈让我更信任对方，也多了些亲近。'};
const slotState={format:'relationship.slots.v1',long_term:{body:'我愿意听{{user:user-1}}慢慢说。',observed_at:0},aftereffect:{body:'刚才没能说完，我心里还有点遗憾。',observed_at:100}};
const shownState={...slotState,long_term:{...slotState.long_term,body:'我愿意听小林慢慢说。'}};
const relationships=[{relationship_id:'relationship-1',logical_user_id:'user-1',content_version:2,state_text:'这个兼容拼接正文不应重复显示',state:slotState,display_state:shownState,updated_at:200},
 {relationship_id:'relationship-old',state_text:'旧版的一段理解。',display_state_text:'旧版已渲染的一段理解。',state:{emotion_tags:['trust'],emotion_intensity:0.4},updated_at:100},
 {relationship_id:'relationship-empty',state_text:'',state:{format:'relationship.slots.v1',long_term:{body:'',observed_at:null},aftereffect:{body:'',observed_at:null}},updated_at:200}];
const priorGet=window.AstrBotPluginView.apiGet;
window.AstrBotPluginView.apiGet=async(endpoint,args={})=>{
 if(endpoint==='memory/context'&&args.kind==='relationships')return {items:relationships,has_more:false};
 if(endpoint==='memory/versions'&&args.kind==='relationship')return {kind:'relationship',entity_id:args.entity_id,items:[
  {version_no:2,created_at:200,state_text:relationships[0].state_text,state:slotState,display_state:shownState,provenance_status:'recorded',derivations:[]},
  {version_no:1,created_at:100,state_text:'历史单段理解。',state:{emotion_tags:['trust'],emotion_intensity:0.4},provenance_status:'unrecorded',derivations:[]}],truncated:false};
 return priorGet(endpoint,args);
};
"""


async def main():
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(QuietHandler, directory=ROOT / 'views/neomemory'))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page(viewport={'width': 1280, 'height': 1000})
            errors = []
            page.on('pageerror', lambda error: errors.append(str(error)))
            await page.add_init_script(MOCK_BRIDGE + FIXTURES)
            await page.goto(f'http://127.0.0.1:{server.server_port}/index.html')
            await page.locator('#status').filter(has_text='已连接').wait_for()
            cards = page.locator('#memory-list > .memory-card')
            await cards.first.locator('.emotion-tag').first.wait_for()
            assert await cards.first.locator('.emotion-tag').all_text_contents() == ['喜悦 · 0.8', '担心 · 0.2']
            assert await cards.nth(1).locator('.emotion-tag').all_text_contents() == ['信任 · 0.65', '亲近 · 0.15']
            await page.locator('[data-kind="relationships"]').click()
            await cards.first.locator('[data-slot="long_term"]').filter(has_text='我愿意听小林慢慢说。').wait_for()
            assert await cards.first.locator('.relationship-slot').count() == 2
            assert '兼容拼接' not in await cards.first.inner_text()
            assert '{{user:' not in await cards.first.inner_text()
            assert 'undefined' not in await cards.first.inner_text()
            assert '未知' not in await cards.first.locator('[data-slot="long_term"] .relationship-observed-at').inner_text()
            assert '旧版已渲染的一段理解。' in await cards.nth(1).inner_text()
            assert '暂无近期余韵。' in await cards.nth(1).inner_text()
            assert await cards.nth(2).locator('.relationship-slot .body').all_text_contents() == ['暂无长期理解。', '暂无近期余韵。']
            await cards.first.locator('.memory-inspector > summary').click()
            await cards.first.locator('.memory-version').first.wait_for()
            assert await cards.first.locator('.memory-version').first.locator('.relationship-slot').count() == 2
            assert '我愿意听小林慢慢说。' in await cards.first.locator('.memory-version').first.inner_text()
            assert '历史单段理解。' in await cards.first.locator('.memory-version').last.inner_text()
            assert await cards.first.locator('.memory-version').last.locator('[data-slot="aftereffect"] .body').inner_text() == '暂无近期余韵。'
            await page.set_viewport_size({'width': 320, 'height': 700})
            assert await page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            await page.screenshot(path=str(ARTIFACTS / 'relationship-mobile.png'), full_page=True)
            await page.set_viewport_size({'width': 1280, 'height': 1000})
            await page.locator('[data-page="maintenance"]').click()
            await page.locator('[data-maintenance-tab="manual"]').click()
            await page.locator('#node-select').select_option('node-1')
            editor = page.locator('#node-emotion-editor')
            assert await editor.locator('.emotion-intensity').count() == 2
            assert await editor.locator('[data-tag="joy"]').input_value() == '0.8'
            assert await editor.locator('[data-tag="concern"]').input_value() == '0.2'
            await editor.locator('[data-tag="joy"]').fill('0.3')
            await editor.locator('[data-tag="concern"]').fill('0.9')
            await editor.locator('.emotion-tags').select_option(['concern'])
            assert await editor.locator('.emotion-intensity').count() == 1
            await editor.locator('.emotion-tags').select_option(['joy', 'concern'])
            assert await editor.locator('[data-tag="joy"]').input_value() == '0.3'
            assert await editor.locator('[data-tag="concern"]').input_value() == '0.9'
            await page.locator('#draft-operation').click()
            operation = json.loads(await page.locator('#operation').input_value())
            assert operation['patch']['emotion'] == {'tags': [{'tag':'joy','intensity':0.3},{'tag':'concern','intensity':0.9}], 'description':'这次安排让我期待，也有些担心。'}
            assert 'intensity' not in operation['patch']['emotion']
            await page.locator('#node-select').select_option('node-2')
            assert await editor.locator('[data-tag="trust"]').input_value() == '0.65'
            assert await editor.locator('[data-tag="affection"]').input_value() == '0.15'
            await page.locator('#node-title').fill('仅修改标题')
            await page.locator('#draft-operation').click()
            operation = json.loads(await page.locator('#operation').input_value())
            assert operation['patch'] == {'title':'仅修改标题'}
            await page.set_viewport_size({'width': 320, 'height': 700})
            assert await page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            await page.screenshot(path=str(ARTIFACTS / 'emotion-editor-mobile.png'), full_page=True)
            assert not errors, errors
            await browser.close()
            result = {'relationship_slots_and_versions': True, 'legacy_and_empty_relationships': True,
                      'rendered_person_reference': True, 'independent_emotion_display_edit': True,
                      'title_edit_preserves_emotion': True, 'mobile_no_overflow': True, 'page_errors': errors}
            (ARTIFACTS / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
            print(json.dumps(result, ensure_ascii=False))
    finally:
        server.shutdown()
        server.server_close()


if __name__ == '__main__':
    asyncio.run(main())
