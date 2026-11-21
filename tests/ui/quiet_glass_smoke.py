"""Quiet Glass layout matrix with synthetic data and real Chromium.

Run with the Conda astrbot Python and PYTHONPATH=. Screenshots and aggregate
results go to ignored data/evaluations/quiet-glass-webui. This does not call
models or validate a deployed AstrBot instance. System color preference is the
product's theme setting; explicit appearance preferences are not introduced.
"""

import asyncio
import functools
import json
import re
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

from playwright.async_api import async_playwright

from tests.ui.full_smoke import MOCK_BRIDGE, QuietHandler

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "data/evaluations/quiet-glass-webui"
VIEWPORTS = [(1440, 1000), (1100, 900), (900, 1000), (720, 1212), (390, 844), (320, 700)]
PAGES = ("memory", "notes", "models", "settings", "maintenance")

SYNTHETIC = r"""
nodes.splice(0, nodes.length,
 {...node('node-1', 'topic'), title:'慢慢找到适合自己的生活节奏', display_title:'慢慢找到适合自己的生活节奏',
  content_text:'林川开始把每天的待办缩减到三件，把傍晚留给散步和阅读。比起把所有空白都填满，我更喜欢这样留一点余地的安排。我们约好，不用因为偶尔没完成计划而责备自己。',
  content_updated_at:1794913200, emotion:{tags:['trust'],intensity:0.6,description:'这份安静的默契让我感到踏实。'}},
 {...node('node-2'), title:'周末书店的约定', display_title:'周末书店的约定',
  content_text:'周六下午去河边的旧书店。林川想找一本介绍植物的书，我想听他讲讲阳台上那盆一直没开花的栀子。下雨就改到下周，不急着把约定变成任务。',
  content_updated_at:1794826800,manual_protected:true},
 {...node('node-3', 'atomic'), title:'喜欢保留一点安静', display_title:'喜欢保留一点安静',
  content_text:'林川习惯在工作结束后独处一会儿。这时候简单陪着就好，不必立刻追问今天发生了什么。',content_updated_at:1794740400}
);
spaces[0].scope_key='人格：青禾 / 私聊：林川';
testNotes.push({note_id:'quiet-note',content_text:'下次聊到阅读时，可以问问那本植物图鉴读得怎么样了。',status:'active',updated_at:1794913200});
const visualGet = window.AstrBotPluginView.apiGet;
window.quietState='normal'; window.quietNodes=nodes; window.quietSpaces=spaces;
window.AstrBotPluginView.apiGet=async(endpoint,args={})=>{
 if(endpoint==='memory/nodes'){
  if(window.quietState==='loading')await new Promise(resolve=>window.releaseQuiet=resolve);
  if(window.quietState==='error')throw Error('合成网络错误：记忆暂不可用，请稍后重试。');
  if(window.quietState==='empty')return {items:[],generation_id:'gen-1',has_more:false};
 }
 const result=await visualGet(endpoint,args);
 if(endpoint==='models')result.llm[1].model='long-model-name-'+('context-window-and-provider-'.repeat(12));
 if(endpoint==='memory/context'){
  if(args.kind==='profiles')result.items[0].description='林川喜欢阅读、散步和照顾植物。面对繁忙的安排，倾向于先理清最重要的事情。';
  if(args.kind==='relationships')result.items[0].state_text='我喜欢我们之间不必急着说话的自在。他愿意把那些还没有想明白的事情告诉我，我也愿意慢慢听。';
  if(args.kind==='short_term')result.items[0].summary_text='正在商量周六去书店的时间，出发前需要确认天气。';
 }
 if(endpoint==='memory/source')result.source.content_text='这是一条独立合成来源，用于检查展开后的正文换行与可读性。';
 return result;
};
"""


async def no_overflow(page, name):
    geometry = await page.evaluate("""() => ({
      viewport:document.documentElement.clientWidth,
      width:document.documentElement.scrollWidth,
      offenders:Array.from(document.querySelectorAll('body *')).filter(el=>{
        const r=el.getBoundingClientRect();
        return r.width && (r.right>document.documentElement.clientWidth+1 || r.left < -1)
          && getComputedStyle(el).position!=='absolute';
      }).slice(0,8).map(el=>el.tagName+'#'+el.id+'.'+el.className)
    })""")
    assert geometry['width'] <= geometry['viewport'] + 1, (name, geometry)
    return geometry


async def visible_navigation(page):
    for destination in PAGES:
        item = page.locator(f'[data-page="{destination}"]')
        assert await item.is_visible(), destination
        box = await item.bounding_box()
        assert box and box['width'] >= 32 and box['height'] >= 32, (destination, box)


async def check_visible_fields(page, destination):
    if destination not in {'settings', 'models'}:
        return
    covered = await page.evaluate("""(destination) => {
      const selectors = destination==='settings' ? '#settings-fields input,#settings-fields select' : '#model-fields input,#model-fields select';
      return Array.from(document.querySelectorAll(selectors)).filter(el=>{
        const r=el.getBoundingClientRect();
        if(!r.width || r.top < 120 || r.bottom > innerHeight)return false;
        const top=document.elementFromPoint(r.x+r.width/2,r.y+r.height/2);
        return top!==el && !el.contains(top);
      }).map(el=>el.id);
    }""", destination)
    assert not covered, ('visible fields covered by another surface', destination, covered)


async def navigate(page, destination):
    await page.evaluate('window.scrollTo(0,0)')
    await page.locator(f'[data-page="{destination}"]').click()
    await page.locator(f'#page-{destination}').wait_for(state='visible')
    await page.evaluate('window.scrollTo(0,0)')
    if destination == 'memory':
        await page.locator('#memory-list > article').first.wait_for()
    elif destination == 'models':
        await page.locator('#setting-llm_provider_id').wait_for()
    elif destination == 'settings':
        await page.locator('#settings-fields .field').first.wait_for()
    elif destination == 'maintenance':
        await page.locator('#tasks article').first.wait_for()
        await page.locator('#ingress-list').filter(has_text='src-one').wait_for()
    else:
        await page.locator('#notes-list .card').first.wait_for()


async def screenshot(page, name, *, full_page=True):
    await page.evaluate("""async () => {
      await Promise.allSettled(document.getAnimations()
        .filter(animation=>animation.effect?.getTiming().iterations!==Infinity)
        .map(animation=>animation.finished));
    }""")
    await page.screenshot(path=str(ARTIFACTS / f'{name}.png'), full_page=full_page)


async def check_inline_details(page):
    await navigate(page, 'memory')
    card = page.locator('#memory-list > article').first
    await card.locator('.memory-inspector > summary').click()
    await card.locator('.memory-version').first.wait_for()
    assert await page.locator('#memory-detail, #source-panel').count() == 0
    assert await card.locator('.memory-detail').count() == 1
    assert not await page.evaluate("testGetCalls.some(call=>call.endpoint==='memory/source')")
    source = card.locator('.memory-source > summary').first
    await source.click()
    await card.locator('.memory-source').filter(has_text='独立合成来源').wait_for()
    await no_overflow(page, 'expanded source and versions')
    await screenshot(page, 'inline-details-phone')
    await card.locator('.memory-inspector > summary').click()
    assert not await card.locator('.memory-version').first.is_visible()


async def check_states(page):
    await page.evaluate("window.quietState='loading'")
    await page.locator('#memory-filter button[type=submit]').click()
    await page.wait_for_function("typeof window.releaseQuiet==='function'")
    assert '读取' in await page.locator('#memory-meta').inner_text()
    await no_overflow(page, 'loading')
    await screenshot(page, 'memory-loading')
    await page.evaluate("window.quietState='empty'; window.releaseQuiet()")
    await page.locator('#memory-list').filter(has_text='没有记录').wait_for()
    await no_overflow(page, 'empty')
    await screenshot(page, 'memory-empty')
    await page.evaluate("window.quietState='error'")
    await page.locator('#memory-filter button[type=submit]').click()
    await page.locator('#notifications .toast.error').filter(has_text='合成网络错误').wait_for()
    assert await page.locator('#status').inner_text() == '已连接'
    await no_overflow(page, 'error')
    await screenshot(page, 'memory-error')
    await page.evaluate("window.quietState='normal'")
    await page.locator('#refresh').click()
    await page.locator('#memory-list > article').first.wait_for()


async def check_focus_and_motion(page):
    await page.emulate_media(reduced_motion='reduce')
    await page.locator('#memory-q').focus()
    await page.keyboard.press('Tab')
    focus = await page.evaluate("""() => {
      const el=document.activeElement, style=getComputedStyle(el);
      return {tag:el.tagName,outline:style.outlineStyle,width:style.outlineWidth,visible:el.matches(':focus-visible')};
    }""")
    assert focus['visible'] and focus['outline'] != 'none' and float(focus['width'].rstrip('px')) > 0, focus
    durations = await page.evaluate("""() => Array.from(document.querySelectorAll('button,input,select,.card')).filter(el=>el.getClientRects().length).map(el=>{
      const s=getComputedStyle(el);return [s.animationDuration,s.transitionDuration];
    })""")
    assert all(float(part.strip().removesuffix('s')) <= 0.001
               for pair in durations for times in pair for part in times.split(',')), durations[:5]
    await page.emulate_media(reduced_motion='no-preference')


async def check_long_content(page):
    await page.evaluate(r"""() => {
      window.quietNodes[0].display_title='一段需要自然换行而不能挤压操作按钮的记忆标题'.repeat(8);
      window.quietNodes[0].content_text='https://example.invalid/'+ 'unbroken-identifier'.repeat(60)+'\n'+ '这是一段用于检查长内容阅读的合成文字。'.repeat(45);
      window.quietSpaces[0].scope_key='persona:'+'long-identity'.repeat(30);
    }""")
    await page.locator('#refresh').click()
    await page.locator('.memory-title').first.filter(has_text='自然换行').wait_for()
    await no_overflow(page, 'long title, identifier and body')
    await screenshot(page, 'memory-long-content-phone')


def luminance(color):
    channels = [int(color[index:index+2], 16)/255 for index in (1, 3, 5)]
    linear = [channel/12.92 if channel <= .04045 else ((channel+.055)/1.055)**2.4 for channel in channels]
    return sum(channel*weight for channel, weight in zip(linear, (.2126, .7152, .0722)))


def contrast(foreground, background):
    low, high = sorted((luminance(foreground), luminance(background)))
    return (high+.05)/(low+.05)


async def token_contrast(page):
    tokens = await page.evaluate(r"""() => {
      const style = getComputedStyle(document.documentElement);
      const ctx = document.createElement('canvas').getContext('2d');
      const hex = () => '#' + Array.from(ctx.getImageData(0,0,1,1).data).slice(0,3)
        .map(v => v.toString(16).padStart(2, '0')).join('');
      const sample = (color, background) => {
        ctx.fillStyle = background; ctx.fillRect(0,0,1,1);
        ctx.fillStyle = color; ctx.fillRect(0,0,1,1);
        return hex();
      };
      const base = style.getPropertyValue('--page-solid');
      const gradient = style.getPropertyValue('--page-gradient');
      const colors = gradient.match(/#[a-f0-9]{6}|hsl\([^)]*\)/gi);
      const primary = getComputedStyle(document.querySelector('button.primary'));
      return { backgrounds: colors.map(c => sample(c, base)),
        texts: ['text','text-secondary','text-muted'].map(key => sample(style.getPropertyValue('--'+key), base)),
        primary: [sample(primary.color, base), sample(primary.backgroundColor, base)],
        selection: [sample(style.getPropertyValue('--accent-strong'), base), sample(style.getPropertyValue('--accent-soft'), base)] };
    }""")
    results = {name: round(min(contrast(color, bg) for bg in tokens['backgrounds']), 2)
               for name, color in zip(('text','text-secondary','text-muted'), tokens['texts'])}
    results['primary'] = round(contrast(*tokens['primary']), 2)
    results['selected'] = round(contrast(*tokens['selection']), 2)
    assert all(value >= 4.5 for value in results.values()), results
    return {'ratios':results, 'method':'WCAG relative luminance for default tokens; page text uses the worst gradient stop. Primary and selected text use their solid token pairs.',
            'limitation':'Token check only; not a full composited-pixel accessibility audit. Disabled controls and placeholder opacity are outside these pairs.'}


async def main():
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(QuietHandler, directory=ROOT/'views/neomemory'))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    report = {'matrix':[], 'theme':'system preference, light and dark resolved outcomes', 'physical_device_test':False, 'token_contrast':{}}
    errors = []
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            for color_scheme in ('light','dark'):
                for width, height in VIEWPORTS:
                    page = await browser.new_page(viewport={'width':width,'height':height}, color_scheme=color_scheme)
                    page.set_default_timeout(10_000)
                    page.on('pageerror', lambda error: errors.append(str(error)))
                    page.on('console', lambda message: errors.append(message.text) if message.type=='error' else None)
                    await page.add_init_script(MOCK_BRIDGE + SYNTHETIC)
                    await page.goto(f'http://127.0.0.1:{server.server_port}/index.html')
                    await page.locator('#status').filter(has_text='已连接').wait_for()
                    await visible_navigation(page)
                    if width == 1440:
                        report['token_contrast'][color_scheme]=await token_contrast(page)
                    for destination in PAGES:
                        await navigate(page, destination)
                        geometry = await no_overflow(page, f'{color_scheme}/{width}/{destination}')
                        await check_visible_fields(page,destination)
                        name=f'{color_scheme}-{width}-{destination}'
                        await screenshot(page,name)
                        if destination=='memory':
                            await screenshot(page,name+'-viewport',full_page=False)
                        report['matrix'].append({'engine':'chromium','theme':color_scheme,'width':width,'page':destination,'geometry':geometry})
                    await page.close()
            page = await browser.new_page(viewport={'width':390,'height':844}, color_scheme='light')
            page.set_default_timeout(10_000)
            page.on('pageerror', lambda error: errors.append(str(error)))
            await page.add_init_script(MOCK_BRIDGE + SYNTHETIC)
            await page.goto(f'http://127.0.0.1:{server.server_port}/index.html')
            await page.locator('#memory-list > article').first.wait_for()
            light_color=await page.locator('body').evaluate('(el)=>getComputedStyle(el).color')
            await page.emulate_media(color_scheme='dark')
            await page.locator('html[data-memory-theme="dark"]').wait_for()
            dark_color=await page.locator('body').evaluate('(el)=>getComputedStyle(el).color')
            assert light_color != dark_color, (light_color,dark_color)
            await page.emulate_media(color_scheme='light')
            await check_inline_details(page)
            await check_states(page)
            await check_focus_and_motion(page)
            await check_long_content(page)
            await page.close()
            # Equivalent CSS viewport for a 1440x1000 display at 200% browser zoom.
            zoom=await browser.new_page(viewport={'width':720,'height':500},device_scale_factor=2)
            await zoom.add_init_script(MOCK_BRIDGE + SYNTHETIC)
            await zoom.goto(f'http://127.0.0.1:{server.server_port}/index.html')
            await zoom.locator('#memory-list > article').first.wait_for()
            await no_overflow(zoom,'200% effective CSS viewport')
            await navigate(zoom,'models')
            await no_overflow(zoom,'200% models')
            await screenshot(zoom,'zoom-200-models')
            await zoom.close()
            assert not errors, errors
            report['checks']=['inline details and source','loading','empty','error and recovery','system theme change','keyboard focus','reduced motion','long content','200% effective CSS viewport']
            report['errors']=errors
            await browser.close()
            webkit = await playwright.webkit.launch(headless=True)
            for color_scheme in ('light','dark'):
                for width, height in ((1440,1000),(390,844)):
                    page = await webkit.new_page(viewport={'width':width,'height':height}, color_scheme=color_scheme)
                    page.set_default_timeout(10_000)
                    page.on('pageerror', lambda error: errors.append(str(error)))
                    page.on('console', lambda message: errors.append(message.text) if message.type=='error' else None)
                    await page.add_init_script(MOCK_BRIDGE + SYNTHETIC)
                    await page.goto(f'http://127.0.0.1:{server.server_port}/index.html')
                    await page.locator('#status').filter(has_text='已连接').wait_for()
                    await visible_navigation(page)
                    if width == 1440:
                        report['token_contrast'][color_scheme]=await token_contrast(page)
                    for destination in PAGES:
                        await navigate(page, destination)
                        geometry=await no_overflow(page,f'webkit/{color_scheme}/{width}/{destination}')
                        await check_visible_fields(page,destination)
                        await screenshot(page,f'webkit-{color_scheme}-{width}-{destination}')
                        report['matrix'].append({'engine':'webkit','theme':color_scheme,'width':width,'page':destination,'geometry':geometry})
                    await page.close()
            await webkit.close()
            assert not errors, errors
    finally:
        server.shutdown()
        server.server_close()
    (ARTIFACTS/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(f'Quiet Glass Chromium/WebKit checks passed: {len(report["matrix"])} page/viewport/theme cases; artifacts: {ARTIFACTS}')


if __name__=='__main__':
    asyncio.run(main())
