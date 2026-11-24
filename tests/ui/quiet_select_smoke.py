"""Browser checks for themed platform selects and dynamic plugin selections."""

import asyncio
import functools
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

from playwright.async_api import async_playwright

from tests.ui.full_smoke import MOCK_BRIDGE, QuietHandler


FIXTURE = """<!doctype html><html lang="zh-CN"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<link rel="stylesheet" href="/style.css"><link rel="stylesheet" href="/select.css">
</head><body style="display:block;padding:16px;box-sizing:border-box">
<main style="margin:0;padding:0;max-width:900px">
<label for="choice">选择测试</label>
<select id="choice" style="width:100%;max-width:300px">
<option value="alpha">Alpha</option><option value="beta">Beta</option>
<option disabled value="disabled">Disabled</option><option value="charlie">Charlie</option>
</select><button id="next" type="button">继续</button>
<label for="unavailable">暂无选项</label>
<select id="unavailable" disabled><option disabled selected>没有可用模型</option></select>
<label for="multiple">多选列表</label>
<select id="multiple" multiple size="3"><option value="one">One</option><option value="two">Two</option><option value="three">Three</option></select>
<div style="height:36vh"></div><label for="long">长列表</label>
<select id="long" style="width:100%;max-width:300px"></select>
</main><script type="module">
import { setupSelectKeys, enhanceSelect } from '/ui.js';
setupSelectKeys(); window.testEnhanceSelect = enhanceSelect;
</script></body></html>"""


async def primitive(page, origin, theme, width, engine):
    await page.emulate_media(color_scheme=theme, reduced_motion="reduce")
    await page.set_viewport_size({"width": width, "height": 844})
    await page.goto(origin + "/select-fixture.html")
    await page.evaluate("theme => document.documentElement.dataset.memoryTheme = theme", theme)
    choice = page.locator("#choice")
    assert await choice.evaluate("s => getComputedStyle(s).appearance") == "base-select"
    await choice.focus()
    # WebKit exposes customizable selects but not HTMLSelectElement.showPicker;
    # its platform opener is Enter/Space, then arrows move within the menu.
    await page.keyboard.press("ArrowDown" if engine == "chromium" else "Enter")
    assert await choice.evaluate("s => s.matches(':open')")
    await page.keyboard.press("ArrowDown")
    await page.keyboard.press("Enter")
    assert await choice.input_value() == "beta"
    assert await choice.evaluate("s => !s.matches(':open') && document.activeElement === s")
    await page.keyboard.press("Space")
    await page.keyboard.press("End")
    await page.keyboard.press("Escape")
    assert await choice.input_value() == "beta"
    assert await choice.evaluate("s => document.activeElement === s")
    await page.keyboard.press("Space")
    await page.keyboard.press("c")
    await page.keyboard.press("Enter")
    assert await choice.input_value() == "charlie"
    await choice.click()
    await page.keyboard.press("Home")
    await page.keyboard.press("Enter")
    assert await choice.input_value() == "alpha"
    await choice.click()
    await page.mouse.click(width - 4, 4)
    assert await choice.evaluate("s => !s.matches(':open')")
    await choice.click()
    await page.keyboard.press("Tab")
    assert await choice.evaluate("s => !s.matches(':open')")
    assert await page.locator("#unavailable").is_disabled()
    assert "没有可用模型" in await page.locator("#unavailable").inner_text()
    await page.select_option("#multiple", ["one", "three"])
    assert await page.locator("#multiple").evaluate("s => [...s.selectedOptions].map(o => o.value)") == ["one", "three"]

    # A short selection in a narrow filter must still expose longer Chinese choices.
    await choice.evaluate("""s => {
      s.style.width = '76px';
      s.replaceChildren(new Option('喜悦'), new Option('尚未设置情绪的记忆'));
      window.testEnhanceSelect(s);
    }""")
    await choice.click()
    options = choice.locator("option")
    short_box = await options.first.bounding_box()
    long_box = await options.last.bounding_box()
    assert long_box["width"] > 76 and long_box["height"] == short_box["height"], long_box
    assert await options.last.evaluate("o => o.scrollWidth <= o.clientWidth")
    await options.last.click()
    assert (await choice.bounding_box())["width"] == 76

    # Platform selection remains compatible with the plugin's existing events.
    await choice.evaluate("s => s.replaceChildren(new Option('Delta', 'delta'), new Option('Echo', 'echo'))")
    await choice.evaluate("s => window.testEnhanceSelect(s)")
    await choice.select_option("echo")
    await choice.click()
    assert await choice.locator("option:checked").count() == 1
    await page.keyboard.press("Escape")
    await page.locator("#long").evaluate("""s => {
      s.replaceChildren(...Array.from({length:60}, (_, i) =>
        new Option(`${i}: ${i % 2 ? 'VeryLongProviderIdentifier'.repeat(7) : '模型'}`, String(i))));
      window.testEnhanceSelect(s);
    }""")
    long = page.locator("#long")
    await long.click()
    first = await long.locator("option").first.bounding_box()
    assert first and first["x"] >= 7 and first["y"] >= 7, first
    assert first["x"] + first["width"] <= width - 7, first
    picker = await long.evaluate("""s => {
      const style=getComputedStyle(s,'::picker(select)');
      return {height:parseFloat(style.height),width:parseFloat(style.width),
        overscroll:style.overscrollBehaviorY,background:style.backgroundColor};
    }""")
    assert picker["height"] <= 320 and picker["width"] <= width - 16, picker
    assert picker["overscroll"] == "contain"
    assert picker["background"] not in ["rgba(0, 0, 0, 0)", "rgb(255, 255, 255)"], picker
    await page.screenshot(path=f"/tmp/neomemory-quiet-select-{engine}-{theme}-{width}.png")
    await page.keyboard.press("End")
    active = await page.evaluate("document.activeElement.getBoundingClientRect().toJSON()")
    assert active["top"] >= 0 and active["bottom"] <= 844, active
    await page.keyboard.press("Enter")
    assert await long.input_value() == "59"
    assert await long.locator("selectedcontent").evaluate("s => s.scrollWidth > s.clientWidth && getComputedStyle(s).textOverflow === 'ellipsis'")
    await page.screenshot(path=f"/tmp/neomemory-quiet-select-closed-{engine}-{theme}-{width}.png")
    assert await page.evaluate("document.documentElement.scrollWidth <= innerWidth + 1")
    return picker["background"]


async def main():
    root = Path(__file__).resolve().parents[2] / "views/neomemory"
    server = ThreadingHTTPServer(("127.0.0.1", 0), functools.partial(QuietHandler, directory=root))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    origin = f"http://127.0.0.1:{server.server_port}"
    try:
        async with async_playwright() as playwright:
            for engine in ("chromium", "webkit"):
                browser = await getattr(playwright, engine).launch(headless=True)
                page = await browser.new_page()
                page.set_default_timeout(10_000)
                errors = []
                page.on("pageerror", lambda error: errors.append(str(error)))
                await page.route("**/select-fixture.html", lambda route: route.fulfill(content_type="text/html", body=FIXTURE))
                backgrounds = {}
                for theme in ("light", "dark"):
                    for width in (1100, 390, 320):
                        backgrounds[theme] = await primitive(page, origin, theme, width, engine)
                assert backgrounds["light"] != backgrounds["dark"]
                await page.add_init_script(MOCK_BRIDGE)
                await page.goto(origin + "/index.html")
                await page.locator("#status").filter(has_text="已连接").wait_for()
                assert await page.locator("#memory-type").evaluate("s => getComputedStyle(s).appearance") == "base-select"
                await page.locator("#memory-type").click()
                await page.get_by_role("option", name="主题概况", exact=True).click()
                assert await page.locator("#memory-type").input_value() == "topic"
                await page.locator('[data-page="models"]').click()
                await page.locator("#setting-llm_provider_id").wait_for()
                await page.locator("#setting-llm_provider_id").select_option("llm-first")
                assert await page.locator("#probe-provider").input_value() == "llm-first"
                await page.locator("#probe-kind").select_option("embedding")
                assert await page.locator("#probe-provider").input_value() == "embed-1"
                assert not errors, errors
                await browser.close()
                print(f"quiet select {engine} passed: themed picker, six theme/viewports, keyboard/typeahead/focus, dynamic values, long list placement, multiselect, plugin model events")
    finally:
        server.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
