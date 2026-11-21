"""Check real pointer hit testing on visible controls and adjacent empty space."""
import asyncio
import functools
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

from playwright.async_api import async_playwright, expect

from tests.ui.full_smoke import MOCK_BRIDGE, QuietHandler


async def click_at(page, locator, *, gap=False, text=False, touch=False):
    """Click the visible box or just to its right on the same line."""
    await locator.evaluate("e => e.scrollIntoView({block:'center',inline:'nearest'})")
    point = await locator.evaluate("""(e, options) => {
      let rect = e.getBoundingClientRect();
      if (options.text) {
        const range = document.createRange();
        range.selectNodeContents(e.firstChild);
        rect = range.getClientRects()[0];
      }
      return [options.gap ? rect.right + 4 : (rect.left + rect.right) / 2,
              (rect.top + rect.bottom) / 2];
    }""", {"gap": gap, "text": text})
    # Chromium may redirect a finger tap a few pixels into a nearby control.
    # Use the exact pointer coordinate at boundaries; tap the control on touch screens.
    if touch and not gap:
        await page.touchscreen.tap(*point)
    else:
        await page.mouse.click(*point)


async def verify(page, touch):
    async def click(locator, **kwargs):
        await click_at(page, locator, touch=touch, **kwargs)

    async def check_caption(label, field):
        await field.evaluate("e => e.blur()")
        await click(label, gap=True, text=True)
        await expect(field).not_to_be_focused()
        await click(label, text=True)
        await expect(field).to_be_focused()

    await check_caption(page.locator('label[for=memory-q]'), page.locator('#memory-q'))
    await page.locator('[data-page=notes]').click()
    await check_caption(page.locator('#notes-filter label.grow span'), page.locator('#notes-q'))
    await page.locator('[data-page=models]').click()
    label = page.locator('label[for=setting-llm_concurrency]')
    await check_caption(label, page.locator('#setting-llm_concurrency'))
    await click(page.locator('#setting-llm_concurrency-help small').last)
    await expect(page.locator('#setting-llm_concurrency')).not_to_be_focused()

    await page.locator('[data-page=settings]').click()
    toggle = page.locator('#setting-recall_enabled')
    await expect(toggle).to_be_checked()
    await click(toggle, gap=True)
    await expect(toggle).to_be_checked()
    await click(page.locator('#setting-recall_enabled-help small').first)
    await expect(toggle).to_be_checked()
    await click(toggle)
    await expect(toggle).not_to_be_checked()
    await click(page.locator('label[for=setting-recall_enabled]'), text=True)
    await expect(toggle).to_be_checked()

    await page.locator('[data-page=maintenance]').click()
    summary = page.locator('#task-detail-panel > summary')
    await click(summary, gap=True, text=True)
    await expect(page.locator('#task-detail-panel')).not_to_have_attribute('open', '')
    await click(summary, text=True)
    await expect(page.locator('#task-detail-panel')).to_have_attribute('open', '')
    await page.locator('[data-maintenance-tab=manual]').click()
    confirm = page.locator('#rebuild-confirm')
    await page.locator('#rebuild-confirm').evaluate("e => { e.closest('details').open=true }")
    label = confirm.locator('..')
    await click(label, gap=True)
    await expect(confirm).not_to_be_checked()
    await click(confirm)
    await expect(confirm).to_be_checked()


async def main():
    root = Path(__file__).resolve().parents[2]
    server = ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(QuietHandler, directory=str(root/'views/neomemory')))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch()
            for width, touch in ((1440, False), (390, True)):
                page = await browser.new_page(viewport={'width':width, 'height':900}, has_touch=touch)
                await page.add_init_script(MOCK_BRIDGE)
                await page.goto(f'http://127.0.0.1:{server.server_port}/index.html')
                await verify(page, touch)
                await page.close()
            await browser.close()
            print('All five pages passed pointer boundary checks at desktop and touch widths.')
    finally:
        server.shutdown()
        server.server_close()


if __name__ == '__main__':
    asyncio.run(main())
