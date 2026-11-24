"""Synthetic browser checks for memory filters, floating pagination and notes layout."""
import asyncio
import functools
from http.server import ThreadingHTTPServer
from pathlib import Path
import threading

from playwright.async_api import async_playwright, expect

from tests.ui.full_smoke import MOCK_BRIDGE, QuietHandler

BROWSE_BRIDGE = r"""
window.browseCalls=[];
const browseGet=window.AstrBotPluginView.apiGet;
window.AstrBotPluginView.apiGet=async(endpoint,args={})=>{
 if(endpoint==='memory/context')browseCalls.push({endpoint,args});
 if(endpoint==='memory/nodes'||endpoint==='notes'){
  browseCalls.push({endpoint,args});
  const items=Array.from({length:65},(_,i)=>endpoint==='notes'
    ? {note_id:'note-'+i,content_text:'Synthetic note '+i,status:'active',updated_at:100}
    : {...node('browse-'+i),display_title:'Synthetic memory '+i,content_text:'A distinct synthetic experience '+i});
  const offset=args.offset||0,limit=args.limit||30;
  return {total:items.length,items:items.slice(offset,offset+limit),has_more:offset+limit<items.length};
 }
 return browseGet(endpoint,args);
};
"""


async def check_footer(page, page_id, label_id):
    footer = page.locator(f'#{page_id} > .floating-pagination')
    await expect(footer).to_be_visible()
    box = await footer.bounding_box()
    viewport = page.viewport_size
    assert box['x'] >= 0 and box['x']+box['width'] <= viewport['width']+1
    assert box['y']+box['height'] <= viewport['height']
    assert box['y']+box['height'] >= viewport['height']-24
    assert await footer.evaluate("e=>getComputedStyle(e).position") == 'fixed'
    await page.evaluate('window.scrollTo(0,document.documentElement.scrollHeight)')
    after = await footer.bounding_box()
    assert abs(after['y']-box['y']) < 1
    list_id = 'memory-list' if page_id == 'page-memory' else 'notes-list'
    last = await page.locator(f'#{list_id} > article').last.bounding_box()
    assert last['y']+last['height'] <= after['y'], (last, after)
    assert await page.evaluate('document.documentElement.scrollWidth<=innerWidth')
    await expect(page.locator('#'+label_id)).to_contain_text('第 ')


async def check_page_picker(page, prefix, expected_text):
    trigger = page.locator('#' + prefix + '-page')
    picker = page.locator('#' + prefix + '-picker')
    await trigger.click()
    await expect(picker.get_by_label('第 1 页')).to_be_focused()
    assert await picker.evaluate("""el => getComputedStyle(el).backdropFilter.includes('blur(')
        && getComputedStyle(el.parentElement).backdropFilter === 'none'
        && getComputedStyle(el.parentElement, '::before').backdropFilter.includes('blur(')""")
    controls = await picker.locator('.page-jump input, .page-count, button[type=submit]').evaluate_all(
        "els => els.map(el => el.getBoundingClientRect().toJSON())")
    centers = [box['y'] + box['height'] / 2 for box in controls]
    assert max(centers) - min(centers) <= 1, controls
    assert all(left['right'] <= right['left'] for left, right in zip(controls, controls[1:])), controls
    await picker.get_by_label('第 3 页').click()
    await expect(page.locator('#' + prefix + '-list > article').first).to_contain_text(expected_text)
    await expect(trigger).to_have_text('第 3 页')
    await expect(picker).to_be_hidden()
    await trigger.click()
    target = picker.get_by_role('spinbutton')
    await target.fill('4')
    await target.press('Enter')
    await expect(picker).to_be_visible()
    await target.fill('1')
    await target.press('Enter')
    await expect(trigger).to_have_text('第 1 页')
    await trigger.click()
    await page.keyboard.press('Escape')
    await expect(picker).to_be_hidden()
    await expect(trigger).to_be_focused()


async def check_node_filters(page):
    advanced = page.locator('#memory-advanced')
    await expect(page.locator('#memory-emotion')).to_be_hidden()
    await page.locator('#memory-sort').select_option('importance_desc')
    await page.wait_for_function("browseCalls.at(-1).args.sort==='importance_desc'&&browseCalls.at(-1).args.offset===0")
    await advanced.locator('summary').click()
    await page.locator('#memory-emotion').select_option('joy')
    await page.locator('#memory-date-from').fill('2026-03-08')
    await page.locator('#memory-date-to').fill('2026-03-08')
    await page.locator('#memory-importance-min').fill('0')
    await page.locator('#memory-importance-max').fill('0.35')
    await page.locator('#memory-importance-max').press('Tab')
    await page.wait_for_function("""() => {
        const a=browseCalls.at(-1).args;
        return a.emotion==='joy' && a.importance_min===0 && a.importance_max===0.35
            && a.updated_from===Date.parse('2026-03-08T00:00:00-05:00')/1000
            && a.updated_before===Date.parse('2026-03-09T00:00:00-04:00')/1000;
    }""")
    await page.locator('#memory-importance-min').fill('0.8')
    await page.locator('#memory-importance-min').press('Tab')
    await expect(page.locator('#memory-meta')).to_contain_text('最低重要性不能大于最高重要性')
    await page.locator('#memory-importance-min').fill('')
    await page.locator('#memory-importance-min').press('Tab')
    await expect(page.locator('#memory-meta')).to_contain_text('本页')
    await page.locator('#memory-date-to').fill('2026-03-07')
    await expect(page.locator('#memory-meta')).to_contain_text('开始日期不能晚于结束日期')
    await page.locator('#memory-date-to').fill('2026-03-08')
    await expect(page.locator('#memory-meta')).to_contain_text('本页')
    await advanced.locator('summary').click()
    await expect(advanced.locator('summary')).to_contain_text('（4）')
    await page.locator('[data-kind=profiles]').click()
    await expect(advanced).to_be_hidden()
    await expect(page.locator('#memory-sort')).to_be_hidden()
    await page.wait_for_function("browseCalls.at(-1).endpoint==='memory/context' && !('sort' in browseCalls.at(-1).args)")
    await page.locator('[data-kind=nodes]').click()
    await page.wait_for_function("browseCalls.at(-1).args.emotion==='joy' && browseCalls.at(-1).args.importance_max===0.35")
    await advanced.locator('summary').click()
    await page.locator('#memory-clear-advanced').click()
    await page.wait_for_function("browseCalls.at(-1).args.sort==='importance_desc' && !('updated_from' in browseCalls.at(-1).args) && !('emotion' in browseCalls.at(-1).args) && !('importance_max' in browseCalls.at(-1).args)")
    await expect(page.locator('#memory-clear-advanced')).to_be_disabled()
    assert await page.evaluate('document.documentElement.scrollWidth<=innerWidth')
    await advanced.locator('summary').click()


async def exercise(page, artifacts, name):
    await expect(page.locator('#memory-list > article')).to_have_count(30)
    if page.viewport_size['width'] > 900:
        for offset in (0, 250, 0):
            await page.evaluate('y => window.scrollTo(0, y)', offset)
            sidebar = await page.locator('.sidebar').bounding_box()
            header = await page.locator('.page-header').bounding_box()
            assert header['y'] == sidebar['y'] == 16, (offset, sidebar, header)
    search = await page.locator('#memory-q').bounding_box()
    query = await page.locator('#memory-filter button[type=submit]').bounding_box()
    assert query['x'] >= search['x']+search['width']
    assert abs(query['y']-search['y']) < 2
    await check_footer(page, 'page-memory', 'memory-page')
    await check_page_picker(page, 'memory', 'Synthetic memory 60')
    await page.locator('#memory-next').click()
    await expect(page.locator('#memory-page')).to_have_text('第 2 页')
    await expect(page.locator('#memory-list > article').first).to_contain_text('Synthetic memory 30')
    await page.locator('#memory-type').select_option('event')
    await expect(page.locator('#memory-page')).to_have_text('第 1 页')
    await page.wait_for_function("browseCalls.at(-1).args.node_type==='event'&&browseCalls.at(-1).args.offset===0")
    await page.locator('#memory-next').click()
    await expect(page.locator('#memory-page')).to_have_text('第 2 页')
    await page.locator('#memory-status').select_option('historical')
    await expect(page.locator('#memory-page')).to_have_text('第 1 页')
    await page.wait_for_function("browseCalls.at(-1).args.status==='historical'&&browseCalls.at(-1).args.offset===0")
    await page.locator('#memory-q').fill('search terms')
    await page.locator('#memory-filter button[type=submit]').click()
    await page.wait_for_function("browseCalls.at(-1).args.q==='search terms'")
    await page.locator('#memory-next').click()
    await expect(page.locator('#memory-page')).to_have_text('第 2 页')
    field = page.locator('#memory-q')
    await field.focus()
    count = await page.evaluate('browseCalls.length')
    # Chromium places the native clear button at the right edge of the content box.
    clear = await field.evaluate("""el => ({
        x: el.clientWidth - parseFloat(getComputedStyle(el).paddingRight) - 8,
        y: el.clientHeight / 2
    })""")
    await field.click(position=clear)
    await expect(field).to_have_value('')
    await expect(page.locator('#memory-page')).to_have_text('第 1 页')
    calls = await page.evaluate('count => browseCalls.slice(count)', count)
    assert len(calls) == 1
    args = calls[0]['args']
    assert (args['q'], args['offset'], args['node_type'], args['status']) == ('', 0, 'event', 'historical')
    await check_node_filters(page)
    await page.locator('#memory-next').click()
    await expect(page.locator('#memory-page')).to_have_text('第 2 页')
    await page.locator('#memory-next').click()
    await expect(page.locator('#memory-list > article')).to_have_count(5)
    await expect(page.locator('#memory-page')).to_have_text('第 3 页')
    await expect(page.locator('#memory-next')).to_be_disabled()
    await page.locator('#memory-prev').click()
    await expect(page.locator('#memory-page')).to_have_text('第 2 页')
    await page.evaluate('window.scrollTo(0,0)')
    await page.screenshot(path=str(artifacts/f'{name}-memory.png'))

    await page.locator('[data-page=notes]').click()
    await expect(page.locator('#notes-list > article')).to_have_count(30)
    editor = page.locator('#page-notes > section').nth(0)
    listing = page.locator('#page-notes > section').nth(1)
    await expect(editor).to_be_hidden()
    await listing.get_by_role('button', name='编辑', exact=True).first.click()
    await expect(editor).to_be_visible()
    editor_box, list_box = await editor.bounding_box(), await listing.bounding_box()
    assert editor_box['y']+editor_box['height'] <= list_box['y']
    assert abs(editor_box['x']-list_box['x']) < 1
    assert abs(editor_box['width']-list_box['width']) < 1
    await expect(page.locator('#page-memory .floating-pagination')).not_to_be_visible()
    assert (await page.locator('#note-content').bounding_box())['height'] == 120
    await editor.get_by_role('button', name='取消编辑').click()
    await expect(editor).to_be_hidden()
    await check_footer(page, 'page-notes', 'notes-page')
    await check_page_picker(page, 'notes', 'Synthetic note 60')
    await page.locator('#notes-next').click()
    await expect(page.locator('#notes-page')).to_have_text('第 2 页')
    await expect(page.locator('#notes-list > article').first).to_contain_text('Synthetic note 30')
    await page.evaluate('window.scrollTo(0,0)')
    await page.screenshot(path=str(artifacts/f'{name}-notes.png'))
    await page.locator('[data-page=models]').click()
    await expect(page.locator('.floating-pagination:visible')).to_have_count(0)
    await page.locator('[data-page=maintenance]').click()
    await page.locator('[data-maintenance-tab=manual]').click()
    action = await page.locator('#action').bounding_box()
    title = await page.locator('#node-title').bounding_box()
    assert abs(action['width'] - title['width']) <= 1, (action, title)
    assert action['x'] >= 0 and action['x'] + action['width'] <= page.viewport_size['width']


async def main():
    root = Path(__file__).resolve().parents[2]
    artifacts = root/'data/evaluations/browse-layout-webui'
    artifacts.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer(('127.0.0.1',0), functools.partial(QuietHandler,directory=str(root/'views/neomemory')))
    threading.Thread(target=server.serve_forever,daemon=True).start()
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch()
            for theme in ('light','dark'):
                for width in (1440,900,390,320):
                    page = await browser.new_page(viewport={'width':width,'height':900},color_scheme=theme,
                                                  timezone_id='America/New_York')
                    errors=[]
                    page.on('pageerror',lambda error:errors.append(str(error)))
                    await page.add_init_script(MOCK_BRIDGE+BROWSE_BRIDGE)
                    await page.goto(f'http://127.0.0.1:{server.server_port}/index.html')
                    await exercise(page,artifacts,f'{theme}-{width}')
                    assert not errors, errors
                    await page.close()
            await browser.close()
            print('browse layout: 8 theme/viewport cases passed; adjacent query, immediate filters/reset, fixed unobscured pagination, page boundaries, notes vertical order')
    finally:
        server.shutdown();server.server_close()


if __name__=='__main__':
    asyncio.run(main())
