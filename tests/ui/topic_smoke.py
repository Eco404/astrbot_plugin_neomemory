"""Synthetic topic browsing and maintenance in Chromium; no live data or models."""

import asyncio
import functools
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

from playwright.async_api import async_playwright

from tests.ui.full_smoke import MOCK_BRIDGE, QuietHandler

TOPICS = r"""
testJobs.push({job_id:'parallel-progress',kind:'context_rebuild',scope_key:'space-2',state:'running',
 max_attempts:2,section_progress:{topics:{state:'retry_wait',step:'topic_model',done:2,total:4,attempt:1,available_at:Date.now()/1000+120},context:{state:'running',step:'replay',done:3,total:12}}});
const originalGet = window.AstrBotPluginView.apiGet;
const originalPost = window.AstrBotPluginView.apiPost;
window.topicSubmissionKeys = [];
window.AstrBotPluginView.apiPost = async (endpoint, body) => {
 if(endpoint === 'tasks/model-preview' && body.kind === 'rebuild') {
   const step = {mode:'fixed',provider_id:'llm-1',providers:['llm-1'],basis:'plugin'};
   return {window:step,topic:step};
 }
 if(endpoint === 'tasks/model-preview')return body.kind==='section_rebuild'
   ? {window:{mode:'windows',providers:['llm-first','llm-1'],missing_windows:1,basis:'chat'},topic:{mode:'missing',providers:[]}}
   : {topic:{mode:'fixed',provider_id:'llm-1',providers:['llm-1'],basis:'plugin'}};
 if(endpoint === 'tasks' && body.kind === 'topic_consolidate') {
   window.topicSubmissionKeys.push(body.idempotency_key);
   if(window.topicSubmissionKeys.length === 1) throw new Error('synthetic lost response');
 }
 return originalPost(endpoint, body);
};
window.testGetRelations = [];
testJobs.push({job_id:'job-publish-topic',kind:'topic_rebuild',scope_key:'space-1',state:'running',step:'topic_publish'});
testJobs.push({job_id:'job-rebuild-topic',kind:'rebuild',scope_key:'space-1',state:'succeeded',step:'topic_model:8/16',progress_done:20,progress_total:20});
window.AstrBotPluginView.apiGet = async (endpoint,args={}) => {
 if(endpoint==='topics/rebuild-preview')return {eligible_members:1,protected_members:3,old_topics:2};
 if(endpoint==='availability')return {available:!window.topicBusy,blocking_jobs:[],automatic:{state:'enabled',reason:null,blocking_jobs:[],topic_blocking_jobs:[{job_id:'failed-topic',kind:'topic_consolidate',error_code:'invalid_topic_output'}]}};
 if(endpoint==='memory/nodes' && args.space_id==='space-1' && args.node_type==='topic')
  return {items:[node('topic-1','topic')],generation_id:'gen-1',has_more:false};
 if(endpoint==='memory/relations'){
  testGetRelations.push(args);
  if(window.deferRelations) await new Promise(resolve=>window.releaseRelations=resolve);
  if(args.node_id==='topic-1'){
   const all=Array.from({length:12},(_,i)=>({...node('member-'+i),title:'Member '+i,display_title:'Member '+i}));
   return {items:all.slice(args.offset,args.offset+args.limit),has_more:args.offset+args.limit<all.length,relation:'members'};
  }
  return {items:[node('topic-1','topic')],has_more:false,relation:'topics'};
 }
 return originalGet(endpoint,args);
};
"""


async def main():
    root = str(Path(__file__).resolve().parents[2] / "views" / "neomemory")
    server = ThreadingHTTPServer(("127.0.0.1", 0), functools.partial(QuietHandler, directory=root))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page(viewport={"width": 390, "height": 844})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            await page.add_init_script(MOCK_BRIDGE + TOPICS)
            await page.goto(f"http://127.0.0.1:{server.server_port}/index.html")
            await page.locator("#memory-list > article").first.wait_for()
            event = page.locator("#memory-list > article").first
            await event.locator(":scope > .memory-relations > summary").click()
            await event.locator(".memory-relations-body .badge").filter(has_text="主题").wait_for()
            assert not await page.evaluate("testGetCalls.some(c=>c.endpoint==='memory/versions')")
            await page.select_option("#memory-type", "topic")
            await page.locator("#memory-filter button[type=submit]").click()
            topic = page.locator("#memory-list > article").first
            await topic.locator(":scope > .memory-relations > summary").filter(has_text="展开主题成员").click()
            children = topic.locator(".memory-relations-body > article")
            await children.nth(9).wait_for()
            assert await children.count() == 10
            assert await topic.locator(".memory-relations .memory-relations").count() == 0
            await children.first.locator(".memory-inspector > summary").click()
            await children.first.locator(".memory-version").first.wait_for()
            assert await page.locator("#memory-detail, #source-panel").count() == 0
            await topic.get_by_role("button", name="下一页关联记忆", exact=True).click()
            await children.first.locator(".memory-title").filter(has_text="Member 10").wait_for()
            assert await children.count() == 2
            assert await topic.get_by_role("button", name="上一页关联记忆", exact=True).count() == 1
            await page.evaluate("window.deferRelations=true")
            await topic.get_by_role("button", name="上一页关联记忆", exact=True).click()
            await page.wait_for_function("typeof window.releaseRelations==='function'")
            await page.select_option("#space", "space-2")
            await page.locator("#memory-list").filter(has_text="SECOND SPACE").wait_for()
            await page.evaluate("window.deferRelations=false;window.releaseRelations()")
            await page.wait_for_timeout(50)
            assert "Member 0" not in await page.locator("#memory-list").inner_text()
            assert await page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
            await page.locator('[data-page="maintenance"]').click()
            backlog = page.locator('#extraction-backlog')
            await backlog.filter(has_text='7 条').wait_for()
            await page.select_option('#space', 'space-1')
            await backlog.filter(has_text='0 条').wait_for()
            await page.select_option('#space', 'space-2')
            await backlog.filter(has_text='7 条').wait_for()
            await page.locator('[data-maintenance-tab="manual"]').click()
            await page.locator('#automatic-title').filter(has_text='自动主题整理已暂停').wait_for()
            assert '普通消息抽取' in await page.locator('#automatic-reason').inner_text()
            assert 'failed-topic' in await page.locator('#automatic-jobs').inner_text()
            assert '生成主题概况与成员 8/16' in await page.locator('#tasks').inner_text()
            assert '发布重构后的主题' in await page.locator('#tasks').inner_text()
            await page.locator("#topic-consolidate:not([disabled])").wait_for()
            assert not await page.locator('#request-key, #new-key').count()
            await page.locator("#topic-consolidate").click()
            await page.get_by_role("button", name="启动任务", exact=True).click()
            await page.locator('#notifications').get_by_text('未确认任务是否提交成功。', exact=False).wait_for()
            await page.locator("#topic-consolidate").click()
            await page.get_by_role("button", name="启动任务", exact=True).click()
            await page.wait_for_function("document.querySelector('#topic-consolidate').disabled")
            await page.wait_for_function("testCalls.some(c=>c.endpoint==='tasks'&&c.body.kind==='topic_consolidate')")
            await page.locator('[data-maintenance-tab="tasks"]').click()
            await page.locator("#tasks article").filter(has_text="整理主题").wait_for()
            keys = await page.evaluate('window.topicSubmissionKeys')
            assert len(keys) == 2 and keys[0] and keys[0] == keys[1]
            panel = page.locator('#task-detail-panel')
            assert not await panel.evaluate('element => element.open')
            await page.locator('#tasks article').first.get_by_role('radio').check()
            await panel.locator('summary').first.click()
            assert await panel.evaluate('element => element.open')
            await panel.locator('summary').first.click()
            assert not await panel.evaluate('element => element.open')

            await page.locator('[data-maintenance-tab="manual"]').click()
            rebuild_panel = page.locator("details").filter(has=page.get_by_text("重构记忆", exact=True))
            assert await page.locator("#section-selection").is_hidden()
            assert await page.locator("#rebuild").is_hidden()
            await rebuild_panel.locator("summary").click()
            assert await rebuild_panel.locator("legend").all_text_contents() == ["按板块重构", "全量重构"]
            assert await page.locator("#rebuild").is_disabled()
            await page.locator("#rebuild-confirm").check()
            assert await page.locator("#rebuild").is_enabled()
            await page.locator("#rebuild").click()
            full = page.get_by_role("dialog")
            topic_choice = full.locator("details.task-model-field")
            await topic_choice.get_by_text("主题整理模型 · llm-1", exact=True).wait_for()
            assert not await topic_choice.evaluate("el => el.open")
            await full.get_by_label("记忆 LLM 模型（窗口抽取）").select_option("llm-first")
            await topic_choice.locator("summary").click()
            assert await topic_choice.get_by_label("主题整理模型").input_value() == "llm-1"
            await topic_choice.get_by_label("主题整理模型").select_option("llm-first")
            assert await topic_choice.locator("summary").inner_text() == "主题整理模型 · llm-first"
            await full.get_by_role("button", name="取消").click()
            await page.locator("#rebuild-confirm").uncheck()
            rebuild = page.get_by_role("button", name="启动板块重构", exact=True)
            assert await rebuild.is_disabled()
            for section in ("topics", "profiles", "relationship"):
                await page.locator(f'#section-selection input[value="{section}"]').check()
            await rebuild.click()
            dialog = page.get_by_role("dialog", name="确认重构所选板块？")
            label = await page.locator("#space option:checked").text_content()
            assert label in await dialog.inner_text() and "主题、用户画像、人格关系" in await dialog.inner_text()
            assert "本次将清空现有的 2 个主题" in await dialog.inner_text()
            await dialog.get_by_role("button", name="取消").click()
            assert not await page.evaluate("testCalls.some(c=>c.body.kind==='section_rebuild')")
            await page.evaluate("window.topicBusy=true")
            await page.wait_for_function("document.querySelector('#section-rebuild').disabled")
            await page.evaluate("window.topicBusy=false")
            await rebuild.click()
            await dialog.get_by_role("button", name="确认重构").click()
            assert await dialog.is_visible()
            await dialog.get_by_label("仅用于缺少模型的窗口").select_option("llm-1")
            await dialog.get_by_label("主题整理模型").select_option("llm-first")
            await dialog.get_by_role("button", name="确认重构").click()
            await page.locator("#tasks article").filter(has_text="按板块重构").filter(has_text="完成").wait_for()
            assert await page.locator('[data-maintenance-tab="tasks"]').get_attribute("aria-selected") == "true"
            submitted = await page.evaluate("testCalls.filter(c=>c.body.kind==='section_rebuild').map(c=>c.body)")
            assert len(submitted) == 1 and submitted[0]["space_id"] == "space-2"
            assert submitted[0]["model_selection"] == {"missing_window_provider_id": "llm-1", "topic_provider_id": "llm-first"}
            assert submitted[0]["sections"] == ["topics", "profiles", "relationship"]
            card = page.locator("#tasks article").filter(has_text="parallel-progress")
            assert "第 1 次尝试" in await card.inner_text()
            assert "后继续" in await card.locator(".task-recovery").inner_text()
            assert await card.get_by_role("progressbar", name="主题进度").get_attribute("value") == "50"
            assert await card.get_by_role("progressbar", name="用户画像与人格关系进度").get_attribute("value") == "25"
            assert not errors, errors
            await browser.close()
            print("topic browser passed: distinct views, inline members and sources, parent topics, pagination, scope race, mobile layout, background task submission")
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    asyncio.run(main())
