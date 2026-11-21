"""Read-only current inline inspectors in the real AstrBot Dashboard shell.

Authentication is read from an ignored local file. Results contain counts and
booleans, never conversation text, screenshots, credentials or browser traces.
"""
import argparse
import asyncio
import json
from pathlib import Path
import re
import traceback
from urllib.parse import parse_qs, urlsplit

from playwright.async_api import async_playwright, expect


def memory_response_matches(response, *, space_id, kind):
    url = urlsplit(response.url)
    query = parse_qs(url.query)
    endpoint = "nodes" if kind == "nodes" else "context"
    return (response.request.method == "GET"
            and url.path.endswith("/memory/" + endpoint)
            and query.get("space_id") == [space_id]
            and query.get("limit") == ["30"]
            and query.get("offset") == ["0"]
            and (kind == "nodes" or query.get("kind") == [kind]))


async def read_memory_page(response, *, space_id, expected_generation=None):
    """Check the actual page response, including every returned item's scope."""
    assert response.ok
    payload = await response.json()
    listing = payload.get("data", payload)
    assert listing["space_id"] == space_id
    generation = listing["generation_id"]
    assert generation
    if expected_generation is not None:
        assert generation == expected_generation
    assert all(item["space_id"] == space_id and item["generation_id"] == generation
               for item in listing["items"])
    return listing


async def run(args):
    auth = json.loads(args.auth_file.read_text())
    result = {"ok": False, "mutations": 0, "page_errors": 0, "inspections": {}, "views": {}, "stage": "login"}
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1440, "height": 1050}, has_touch=True)
        page = await context.new_page()
        page.set_default_timeout(20000)

        async def readonly(route):
            if route.request.method not in {"GET", "HEAD", "OPTIONS"}:
                result["mutations"] += 1
                await route.abort()
            else:
                await route.continue_()

        await context.route("**/*", readonly)
        page.on("pageerror", lambda _: result.update(page_errors=result["page_errors"] + 1))
        try:
            await page.goto(args.base_url + "/#/auth/login", wait_until="domcontentloaded")
            await page.locator('input[name="username"]').wait_for()
            await page.evaluate("a=>{localStorage.setItem('token',a.token);localStorage.setItem('user',a.username)}", auth)
            await page.goto(args.base_url + "/#/plugin-view/astrbot_plugin_neomemory/neomemory", wait_until="domcontentloaded")
            iframe = page.locator("iframe.plugin-view-frame")
            await iframe.wait_for()
            assert await iframe.get_attribute("sandbox") == "allow-scripts allow-forms allow-downloads"
            frame = page.frame_locator("iframe.plugin-view-frame")
            await expect(frame.locator("#status")).to_have_text("已连接")
            notice = page.locator(".v-overlay--active .v-card").filter(
                has=page.locator(".v-card-title span", has_text=re.compile(r"^(首次提示|First Notice)$")))
            async def dismiss_notice():
                await notice.locator(".v-card-title button").click()
            await page.add_locator_handler(notice, dismiss_notice)
            if await notice.count():
                await notice.locator(".v-card-title button").click()
                await expect(notice).to_be_hidden()
            await frame.locator("#space").select_option(args.space_id)
            await expect(frame.locator("#memory-meta")).to_contain_text("本页")
            for kind in ("nodes", "profiles", "relationships"):
                result["stage"] = "inspect_" + kind
                async with page.expect_response(lambda r: memory_response_matches(
                        r, space_id=args.space_id, kind=kind)) as pending:
                    await frame.locator(f'[data-kind="{kind}"]').click()
                listing = await read_memory_page(await pending.value, space_id=args.space_id,
                                                expected_generation=args.expected_generation)
                cards = frame.locator("#memory-list > article")
                await expect(cards).to_have_count(len(listing["items"]))
                await cards.first.wait_for()
                first = cards.first
                async with page.expect_response(lambda r: urlsplit(r.url).path.endswith("/memory/versions")):
                    await first.locator(":scope > .memory-inspector > summary").click()
                await first.locator(".memory-version").first.wait_for()
                info = {"cards": await cards.count(), "versions": await first.locator(".memory-version").count(),
                        "inline": await frame.locator("#memory-detail, #source-panel").count() == 0,
                        "api_generation_checked": True,
                        "expected_generation_matched": args.expected_generation is not None}
                sources = first.locator(".memory-source > summary")
                if await sources.count():
                    async with page.expect_response(lambda r: urlsplit(r.url).path.endswith("/memory/source")):
                        await sources.first.click()
                    await expect(first.locator(".source-content").first).not_to_have_text("正在读取原始内容…")
                    info["source_opened"] = bool(await first.locator(".source-content").first.text_content())
                bases = first.get_by_role("button", name=re.compile(r"^(长期记忆|用户画像|人格关系) .+ · 版本 \d+$"))
                if await bases.count():
                    await bases.first.click()
                    await first.get_by_role("button", name="返回上一层查阅", exact=True).wait_for()
                    info["basis_navigation"] = True
                result["inspections"][kind] = info
            for mobile in (False, True):
                if mobile:
                    await page.set_viewport_size({"width": 390, "height": 844})
                for name in ("memory", "notes", "models", "settings", "maintenance"):
                    result["stage"] = ("mobile_" if mobile else "desktop_") + name
                    await frame.locator("html").evaluate("()=>window.scrollTo(0,0)")
                    await frame.locator(f'[data-page="{name}"]').click()
                    await expect(frame.locator(f"#page-{name}")).to_be_visible()
                    if name == "maintenance":
                        await expect(frame.locator("#availability")).not_to_have_text("等待选择空间。")
                        result["progress_bars"] = await frame.locator("#tasks progress").count()
                    if mobile:
                        assert await frame.locator("html").evaluate("()=>document.documentElement.scrollWidth<=innerWidth")
                    result["views"][result["stage"]] = True
            assert not result["mutations"] and not result["page_errors"]
            result["ok"] = True
        except Exception as exc:
            result["error_type"] = type(exc).__name__
            result["error_lines"] = [entry.lineno for entry in traceback.extract_tb(exc.__traceback__)
                                     if entry.filename == __file__]
        finally:
            await browser.close()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--auth-file", type=Path, required=True)
    parser.add_argument("--space-id", required=True)
    parser.add_argument("--expected-generation")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = asyncio.run(run(args))
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False))
    raise SystemExit(not result["ok"])


if __name__ == "__main__":
    main()
