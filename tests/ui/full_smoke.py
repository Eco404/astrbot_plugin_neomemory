"""Synthetic bridge + real Chromium: five pages, mutations and stale response guards.

Run with the Conda astrbot Python. This is not a live AstrBot authentication or
model quality test. No production data, Provider credentials, or DB is used.
"""

import asyncio
import functools
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from playwright.async_api import async_playwright

MOCK_BRIDGE = r"""
window.testCalls=[]; window.testGetCalls=[]; window.testNotes=[{note_id:'note-1',content_text:'Synthetic user note {{user:person-one}}与{{user:person-two}}',status:'active',updated_at:101}]; window.testDefer=false;
const spaces=[{space_id:'space-1',display_label:'demo · 林川',scope_key:'persona:demo:user:synthetic',space_type:'private'},
 {space_id:'space-2',display_label:'demo · 松子',scope_key:'persona:demo:user:second',space_type:'private'},
 {space_id:'group',display_label:'demo · 示例群',scope_key:'persona:demo:platform:demo:group:test',space_type:'group'}];
const node=(id,kind='event')=>({node_id:id,space_id:'space-1',generation_id:'gen-1',node_type:kind,status:'active',title:id==='node-3'?null:'Original '+id,display_title:id==='node-3'?null:'Displayed <img src=x onerror=alert(9)> '+id,content_text:'<img src=x onerror=alert(1)> Synthetic '+id,content_version:1,updated_at:100,content_updated_at:id==='node-3'?null:id==='node-2'?0:50,importance:0.5});
const nodes=[node('node-1'),node('node-2'),node('node-3','atomic')];
window.testJobs=[{job_id:'job-failed',kind:'extract',scope_key:'space-1',state:'failed',step:'validate',error_code:'invalid_output',progress_done:0,progress_total:5,attempt:2,max_attempts:2,regenerate_window_available:true},
 {job_id:'job-short',kind:'short_term',scope_key:'space-1',state:'cancelled',step:'short_term_model',progress_done:0,progress_total:4,attempt:1,max_attempts:2},
 {job_id:'job-context',kind:'extract',scope_key:'space-1',state:'running',step:'context:256',progress_done:0,progress_total:8,attempt:1,max_attempts:2},
 {job_id:'job-index',kind:'extract',scope_key:'space-1',state:'failed',step:'window_index',progress_done:0,progress_total:8,attempt:2,max_attempts:2,committed_window:{batch_id:'synthetic-batch',indexed:16,total:20}}];
window.testIngress=[{source_id:'src-one',stream_id:'stream-one',state:'failed',attempts:3,max_attempts:3,payload_bytes:1024,error_code:'source_operational_error'}];
window.testUnpersisted=2; window.testOverflow=1;
const health=()=>({pending_count:testIngress.length,max_entries:10000,payload_bytes:1024,max_bytes:67108864,counts:{failed:1}});
const schema={recall_enabled:{type:'bool',default:true,description:'启用召回',hint:'控制采集',group:'功能'},
 external_memory_mode:{type:'string',default:'supplement',description:'陪伴插件记忆归属模式',options:['supplement','owner'],group:'高级'},
 llm_provider_id:{type:'string',default:'',description:'记忆 LLM',group:'model'},short_term_provider_id:{type:'string',default:'',description:'短期摘要模型',group:'model'},embedding_provider_id:{type:'string',default:'',description:'向量模型',group:'model'},
 llm_concurrency:{type:'int',default:2,description:'LLM 并发',slider:{min:1,max:32,step:1},group:'model'},embedding_concurrency:{type:'int',default:2,description:'向量并发',slider:{min:1,max:32,step:1},group:'model'},embedding_batch_size:{type:'int',default:16,description:'向量批次',slider:{min:1,max:256,step:1},group:'model'},
 recall_history_seconds:{type:'int',default:7200,description:'历史时间范围（秒）',hint:'补充检索',slider:{min:0,max:100000,step:1},group:'高级'}};
let values={recall_enabled:true,external_memory_mode:'supplement',llm_provider_id:'llm-1',short_term_provider_id:'',embedding_provider_id:'embed-1',llm_concurrency:2,embedding_concurrency:2,embedding_batch_size:16,recall_history_seconds:7200};
const runtime={...values}; let revision='rev-1'; window.testApplyFailure=false; window.testPendingResponse=false; window.testExternalResponse=false; let applyError=null;
const settings=()=>({schema,values:{...values},runtime_values:runtime,revision,can_save:true,reload_required:false,applied:!applyError&&!Object.keys(values).some(k=>values[k]!==runtime[k]),apply_error:applyError});
window.AstrBotPluginView={ready:async()=>{},apiGet:async(endpoint,args={})=>{
 testGetCalls.push({endpoint,args});
 if(endpoint==='memory/spaces')return {items:spaces,has_more:false};
 if(endpoint==='identities')return {items:[],has_more:false};
 if(endpoint==='memory/nodes'){
  if(window.testDefer&&args.space_id==='space-1')await new Promise(resolve=>window.releaseOld=resolve);
  if(window.testLongList&&args.space_id==='space-1'){const all=Array.from({length:40},(_,i)=>({...node('long-'+i),display_title:'Long title '+i+' '+('标'.repeat(60))}));return {items:all.slice(args.offset||0,(args.offset||0)+(args.limit||30)),generation_id:'gen-1',has_more:(args.offset||0)+(args.limit||30)<all.length};}
  return {items:args.space_id==='space-2'?[{...nodes[0],node_id:'other',content_text:'SECOND SPACE'}]:nodes,generation_id:'gen-1',has_more:false};
 }
 if(endpoint==='memory/versions'){
  if(window.testFailVersions){window.testFailVersions=false;throw Error('Synthetic version failure');}
  if(window.testDeferVersions)await new Promise(resolve=>window.releaseVersions=resolve);
  const kind=args.kind||'node', entityId=args.entity_id||args.node_id;
  const fields=kind==='profile'?{description:'Historical profile',tags:['轻量']}:kind==='relationship'?{state_text:'Historical relationship',state:{feeling:'calm'}}:{content_text:'Version <img src=x onerror=alert(3)>'};
  const items=[{...fields,title:null,version_no:1,created_at:101,content_updated_at:null,provenance_status:'unrecorded',derivations:kind==='node'?[{operation:'title_update',editor_kind:'manual',created_at:100,title_change:{before:null,after:'Later title',display_after:'Later displayed title'},governance_job_id:'title-job'}]:[]},
   {...fields,title:'Snapshot title',display_title:'Snapshot displayed title',version_no:2,created_at:102,content_updated_at:50,provenance_status:'recorded',derivations:[{operation:'update',editor_kind:'model',created_at:101,
    base_versions:[{kind,entity_id:entityId,version_no:1}],sources:[{source_id:'source-version-2',source_kind:'source_record'}]}]}];
  return {kind,entity_id:entityId,node:nodes.find(n=>n.node_id===entityId),items:args.version_no?items.filter(v=>v.version_no===args.version_no):items,
   ...(kind==='node'?{sources:[{source_id:'source-1',source_kind:'source_record'}],sources_scope:'entity_cumulative',participants:[],edges:[]}:{} )};
 }
 if(endpoint==='memory/source'){if(window.testFailSource){window.testFailSource=false;throw Error('Synthetic source failure');}if(window.testDeferSource)await new Promise(resolve=>window.releaseSource=resolve);return {source:{content_text:'PRIVATE SOURCE <img src=x onerror=alert(2)>',source_id:args.source_id}};}
 if(endpoint==='memory/context')return {items:[args.kind==='profiles'?{profile_id:'profile-1',updated_at:600,description:'User profile',tags:['轻量'],logical_user_id:'user-1'}:args.kind==='relationships'?{relationship_id:'relationship-1',updated_at:600,state_text:'Our relationship'}:{updated_at:600,summary_text:'Earlier conversation',conversation_id:'host.v1:opaque',coverage:{source_count:8,to_observed_at:100}}],has_more:false};
 if(endpoint==='notes')return {user_names:{'person-one':'小林','person-two':'小林'},items:testNotes.filter(n=>n.status!=='deleted'&&(args.status==='all'||n.status===args.status)),has_more:false};
 if(endpoint==='appearance')return window.testTheme || {};
 if(endpoint==='settings')return settings();
 if(endpoint==='models')return {llm:[{provider_id:'llm-first',model:'first-listed'},{provider_id:'llm-1',model:'synthetic-llm'}],embedding:[{provider_id:'embed-first',model:'first-listed'},{provider_id:'embed-1',model:'synthetic-embed'}]};
 if(endpoint==='tasks')return {items:testJobs,has_more:false};
 if(endpoint==='task')return {...testJobs.find(j=>j.job_id===args.job_id),candidate:null,source_ids:args.job_id==='job-failed'?['source-failed']:[],restart_required:false};
 if(endpoint==='extraction/backlog')return {pending_count:args.space_id==='space-2'?7:0};
 if(endpoint==='availability')return {available:true,blocking_jobs:[],automatic:{state:'paused',reason:'failed_window',blocking_jobs:[testJobs[0]]}};
 if(endpoint==='health')return {ready:true,ingress:{...health(),unpersisted_count:testUnpersisted,unpersisted_overflow:testOverflow}};
 if(endpoint==='ingress')return {items:args.offset?[]:testIngress,has_more:!args.offset,health:health()};
 if(endpoint==='ingress/record')return {source:{content_text:'PRIVATE INGRESS <img src=x onerror=alert(2)>'}};
 throw Error('unknown endpoint '+endpoint);
},apiPost:async(endpoint,body)=>{
 testCalls.push({endpoint,body});await new Promise(resolve=>setTimeout(resolve,150));
 if(endpoint==='appearance'){window.testTheme={...body};return body;}
 if(endpoint==='settings'){Object.assign(values,body.changes);revision+='x';if(window.testApplyFailure){applyError='settings_apply_failed';throw Error('Settings apply failed');}if(window.testPendingResponse)return settings();Object.assign(runtime,values);applyError=null;const result=settings();if(window.testExternalResponse)Object.assign(result,{external_change:true,can_save:false,applied:false,reload_required:true});return result;}
 if(endpoint==='ingress/retry'){const readmitted=body.source_id?0:testUnpersisted;if(!body.source_id)testUnpersisted=0;return {retried:1,readmitted};}
 if(endpoint==='tasks/delete'){window.testJobs=testJobs.filter(j=>j.job_id!==body.job_id);return {deleted:true,job_id:body.job_id,removed_node_count:0};}
 if(endpoint==='tasks/retry'||endpoint==='tasks/cancel')return testJobs.find(j=>j.job_id===body.job_id);
 let kind=body.kind;
 if(endpoint==='notes'){
  kind='note';
  if(window.testNoteFailure){window.testNoteFailure=false;throw Error('Synthetic note failure');}
  const item=testNotes.find(n=>n.note_id===body.note_id);
  if(body.action==='update')item.content_text=body.content_text;else item.status={archive:'archived',restore:'active',delete:'deleted'}[body.action];item.updated_at++;
 }
 if(endpoint==='models/probe')return {ok:true,kind:body.kind,provider_id:body.provider_id,model:'synthetic',elapsed_ms:42};
 const job={job_id:'job-'+testCalls.length,kind,scope_key:body.space_id,state:'succeeded',step:'complete',progress_done:1,progress_total:1,result:{ok:true}};testJobs.push(job);
 return job;
}};
"""


class QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, *_args):
        pass


async def check_inline_memory(page):
    """卡片独立查阅、历史标题、失败重试及迟到请求使用真实浏览器验证。"""
    cards = page.locator('#memory-list > .memory-card')
    first, second = cards.nth(0), cards.nth(1)
    assert await first.locator('.memory-title').inner_text() == 'Displayed <img src=x onerror=alert(9)> node-1'
    assert await cards.nth(2).locator('.memory-title').inner_text() == '原子 · node-3'
    formatted = await page.evaluate('[50, 0, 600].map(value => new Date(value * 1000).toLocaleString())')
    assert await first.locator(':scope > .memory-updated-at').inner_text() == f'更新时间：{formatted[0]} · 重要性 0.5'
    assert await second.locator(':scope > .memory-updated-at').inner_text() == f'更新时间：{formatted[1]} · 重要性 0.5'
    assert await cards.nth(2).locator(':scope > .memory-updated-at').inner_text() == '更新时间：未知 · 重要性 0.5'
    assert await page.locator('#memory-detail, #source-panel').count() == 0
    assert not await page.evaluate("testGetCalls.some(c=>c.endpoint==='memory/versions')")
    await first.locator('.memory-inspector > summary').click()
    await first.locator('.memory-detail').filter(has_text='此版本未记录独立来源').wait_for()
    assert '本次直接引用' in await first.locator('.memory-detail').inner_text()
    assert '继承的基线版本' in await first.locator('.memory-detail').inner_text()
    assert '节点累计引用' in await first.locator('.memory-sources').inner_text()
    assert await first.locator('.version-title').all_text_contents() == ['未记录标题', 'Snapshot displayed title']
    assert await first.locator('.version-content-time').all_text_contents() == ['内容更新时间：未知', f'内容更新时间：{formatted[0]}']
    assert '版本生成于' in await first.locator('.memory-version h3').first.inner_text()
    assert '操作于' in await first.locator('.version-derivation h4').first.inner_text()
    assert '新题：Later displayed title' in await first.locator('.memory-version').first.inner_text()
    assert not await page.evaluate("testGetCalls.some(c=>c.endpoint==='memory/source')")
    await second.locator('.memory-inspector > summary').click()
    await second.locator('.memory-version').last.wait_for()
    assert await first.locator('.memory-inspector').get_attribute('open') is not None
    calls = await page.evaluate("testGetCalls.filter(c=>c.endpoint==='memory/versions').length")
    await first.locator('.memory-inspector > summary').click()
    assert not await first.locator('.memory-detail').is_visible()
    await first.locator('.memory-inspector > summary').click()
    assert await page.evaluate("testGetCalls.filter(c=>c.endpoint==='memory/versions').length") == calls

    # 原文紧跟所属来源入口，失败不会污染其他卡片。
    await page.evaluate('window.testFailSource=true')
    source = first.locator('.memory-sources .memory-source')
    await source.locator('summary').click()
    await source.get_by_role('button', name='重试读取原文', exact=True).click()
    await source.locator('pre').filter(has_text='PRIVATE SOURCE').wait_for()
    assert 'PRIVATE SOURCE' not in await second.inner_text()
    assert await page.locator('img').count() == 0
    await source.get_by_role('button', name='清除本条原文展示', exact=True).click()
    await page.evaluate('window.testDeferSource=true')
    await source.locator('summary').click()
    await page.wait_for_function("typeof window.releaseSource==='function'")
    await first.get_by_role('button', name='长期记忆 node-1 · 版本 1', exact=True).click()
    await first.get_by_role('button', name='返回上一层查阅', exact=True).wait_for()
    await page.evaluate('window.testDeferSource=false; window.releaseSource()')
    await page.wait_for_timeout(50)
    assert 'PRIVATE SOURCE' not in await first.inner_text()
    assert await first.locator('.memory-version').count() == 1
    assert await second.locator('.memory-version').count() == 2
    call = await page.evaluate("testGetCalls.filter(c=>c.endpoint==='memory/versions').at(-1)")
    assert call['args'] == {'kind': 'node', 'entity_id': 'node-1', 'space_id': 'space-1', 'version_no': 1}
    await first.get_by_role('button', name='返回上一层查阅', exact=True).click()
    assert await first.locator('.memory-version').count() == 2
    assert await page.evaluate("document.activeElement.closest('.memory-card') === document.querySelector('.memory-card')")

    # 本卡片加载失败可在原处重试，刷新使未完成版本响应失效。
    await page.locator('#memory-filter button[type=submit]').click()
    await page.evaluate('window.testFailVersions=true')
    await first.locator('.memory-inspector > summary').click()
    await first.get_by_role('button', name='重试读取版本和来源', exact=True).wait_for()
    await first.get_by_role('button', name='重试读取版本和来源', exact=True).click()
    await first.locator('.memory-version').last.wait_for()
    await page.evaluate('window.testDeferVersions=true')
    await second.locator('.memory-inspector > summary').click()
    await page.wait_for_function("typeof window.releaseVersions==='function'")
    await page.locator('#memory-filter button[type=submit]').click()
    await page.evaluate('window.testDeferVersions=false; window.releaseVersions()')
    await page.wait_for_timeout(50)
    assert await page.locator('.memory-version').count() == 0

    # 长列表分页不预取详情，上一页的异步结果不能渗入下一页。
    await page.evaluate('window.testLongList=true; window.releaseVersions=null')
    await page.locator('#memory-filter button[type=submit]').click()
    await page.locator('#memory-meta').filter(has_text='本页 30 条').wait_for()
    assert await cards.count() == 30
    await page.evaluate('window.testDeferVersions=true')
    await first.locator('.memory-inspector > summary').click()
    await page.wait_for_function("typeof window.releaseVersions==='function'")
    await page.locator('#memory-next').click()
    await page.locator('#memory-meta').filter(has_text='本页 10 条').wait_for()
    await page.evaluate('window.testDeferVersions=false; window.releaseVersions()')
    await page.wait_for_timeout(50)
    assert await page.locator('.memory-version').count() == 0
    await first.locator('.memory-inspector > summary').click()
    await first.locator('.memory-version').last.wait_for()
    await page.set_viewport_size({'width': 390, 'height': 844})
    await first.locator('.memory-sources .memory-source > summary').click()
    await first.locator('.source-content pre').filter(has_text='PRIVATE SOURCE').wait_for()
    assert await page.evaluate('document.documentElement.scrollWidth <= window.innerWidth')
    await page.screenshot(path='/tmp/neomemory-inline-mobile.png', full_page=True)
    await page.set_viewport_size({'width': 1280, 'height': 1000})
    await page.screenshot(path='/tmp/neomemory-inline-desktop.png', full_page=True)
    await page.locator('#memory-prev').click()
    await page.locator('#memory-meta').filter(has_text='本页 30 条').wait_for()
    await first.locator('.memory-inspector > summary').click()
    await first.locator('.memory-version').last.wait_for()
    await page.evaluate('window.testDeferSource=true; window.releaseSource=null')
    await first.locator('.memory-sources .memory-source > summary').click()
    await page.wait_for_function("typeof window.releaseSource==='function'")
    await page.select_option('#space', 'space-2')
    await page.locator('#memory-list').filter(has_text='SECOND SPACE').wait_for()
    await page.evaluate('window.testDeferSource=false; window.releaseSource(); window.testLongList=false')
    await page.wait_for_timeout(50)
    assert 'PRIVATE SOURCE' not in await page.locator('#memory-list').inner_text()
    await page.select_option('#space', 'space-1')
    await page.locator('#memory-list').filter(has_text='Synthetic node-1').wait_for()

    # 画像、关系同样在条目内展开；短期只呈现实际覆盖信息。
    for kind, text in [('profiles', 'User profile'), ('relationships', 'Our relationship'), ('short_term', 'Earlier conversation')]:
        await page.locator(f'[data-kind="{kind}"]').click()
        await page.locator('#memory-list').filter(has_text=text).wait_for()
        assert await cards.first.locator(':scope > .memory-updated-at').inner_text() == f'更新时间：{formatted[2]}'
        if kind != 'short_term':
            await cards.first.locator('.memory-inspector > summary').click()
            await cards.first.locator('.memory-detail').filter(has_text='Historical').wait_for()
            assert '本次直接引用' in await cards.first.locator('.memory-detail').inner_text()
            assert '继承的基线版本' in await cards.first.locator('.memory-detail').inner_text()
            assert await cards.first.locator('.memory-sources').count() == 0
            call = await page.evaluate("testGetCalls.filter(c=>c.endpoint==='memory/versions').at(-1)")
            expected = 'profile' if kind == 'profiles' else 'relationship'
            assert call['args'] == {'kind': expected, 'entity_id': expected + '-1', 'space_id': 'space-1'}
        else:
            assert await cards.first.locator('.memory-inspector').count() == 0
            assert '覆盖消息 8 条' in await cards.first.inner_text()
            assert await cards.first.locator('.memory-title').inner_text() == '短期摘要'
            assert 'host.v1:' not in await cards.first.inner_text()


async def main():
    root = Path(__file__).resolve().parents[2] / "views/neomemory"
    server = ThreadingHTTPServer(("127.0.0.1", 0), functools.partial(QuietHandler, directory=root))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page(viewport={"width": 1280, "height": 1000})
            page.set_default_timeout(10_000)
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            await page.add_init_script(MOCK_BRIDGE)
            await page.goto(f"http://127.0.0.1:{server.server_port}/index.html")
            await page.locator("#status").filter(has_text="已连接").wait_for()
            assert await page.locator(".sidebar nav button").count() == 5
            assert await page.locator("img").count() == 0
            await page.locator("#memory-list").filter(has_text="Synthetic node-1").wait_for()
            assert not await page.evaluate("testGetCalls.some(c=>c.endpoint==='memory/source')")
            await check_inline_memory(page)
            relationships = page.get_by_role('tab', name='人格关系', exact=True)
            await relationships.click()
            await page.select_option('#space', 'group')
            nodes = page.get_by_role('tab', name='长期记忆', exact=True)
            assert await relationships.count() == 0 and await nodes.get_attribute('aria-selected') == 'true'
            await page.get_by_role('tab', name='用户画像').press('ArrowRight')
            assert await page.get_by_role('tab', name='短期记忆').get_attribute('aria-selected') == 'true'
            assert not await page.evaluate("testGetCalls.some(c=>c.endpoint==='memory/context' && c.args.space_id==='group' && c.args.kind==='relationships')")
            await page.select_option('#space', 'space-1')
            assert await relationships.is_visible()
            assert await page.locator('#companion-import').count() == 0
            # Keyboard tab navigation and in-flight scope switching.
            await page.locator('[data-kind="short_term"]').focus()
            await page.keyboard.press("Home")
            await page.locator("#memory-list").filter(has_text="Synthetic").wait_for()
            await page.evaluate("window.testDefer=true")
            await page.locator("#memory-filter button[type=submit]").click()
            await page.wait_for_function("typeof window.releaseOld==='function'")
            await page.select_option("#space", "space-2")
            await page.locator("#memory-list").filter(has_text="SECOND SPACE").wait_for()
            await page.evaluate("window.testDefer=false; window.releaseOld()")
            await page.wait_for_timeout(50)
            assert "Synthetic" not in await page.locator("#memory-list").inner_text()
            await page.select_option("#space", "space-1")

            await page.locator('[data-page="notes"]').click()
            assert await page.locator("#note-editor").is_hidden()
            await page.locator("#notes-list").filter(has_text="Synthetic user note").wait_for()
            await page.locator("#notes-list").filter(has_text="小林与小林").wait_for()
            await page.locator("#notes-list").get_by_role("button", name="编辑", exact=True).click()
            note = page.locator("#note-content")
            assert await note.input_value() == "Synthetic user note 小林与小林"
            await note.press("Home")
            await page.keyboard.insert_text("周三 ")
            await note.press("End")
            await page.keyboard.insert_text(" 和小林")
            await page.locator("#note-save").click()
            await page.locator("#notes-list").filter(has_text="周三 Synthetic user note 小林与小林 和小林").wait_for()
            saved = await page.evaluate("testCalls.filter(c=>c.endpoint==='notes' && c.body.action==='update').at(-1).body.content_text")
            assert saved == "周三 Synthetic user note {{user:person-one}}与{{user:person-two}} 和小林"
            await page.locator("#notes-list").get_by_role("button", name="归档", exact=True).click()
            await page.locator("#notes-list").filter(has_text="没有笔记").wait_for()
            await page.select_option("#notes-status", "archived")
            await page.locator("#notes-filter button[type=submit]").click()
            await page.locator("#notes-list").get_by_role("button", name="恢复", exact=True).click()
            await page.locator("#notes-list").filter(has_text="没有笔记").wait_for()

            await page.select_option("#notes-status", "active")
            await page.locator("#notes-filter button[type=submit]").click()
            await page.locator("#notes-list").get_by_role("button", name="编辑", exact=True).click()
            await page.locator("#note-content").fill("Unsubmitted draft")
            await page.locator("#notes-list").get_by_role("button", name="删除", exact=True).click()
            await page.locator("#notes-list").get_by_role("button", name="取消删除", exact=True).click()
            assert not await page.evaluate("testCalls.some(c=>c.endpoint==='notes' && c.body.action==='delete')")
            await page.locator("#notes-list").get_by_role("button", name="删除", exact=True).click()
            await page.evaluate("window.testNoteFailure=true")
            await page.locator("#notes-list").get_by_role("button", name="确认删除笔记", exact=True).click()
            await page.locator("#notifications .toast").filter(has_text="Synthetic note failure").last.wait_for()
            assert await page.locator("#note-content").input_value() == "Unsubmitted draft"
            await page.locator("#notes-list").get_by_role("button", name="确认删除笔记", exact=True).click()
            await page.locator("#notes-list").filter(has_text="没有笔记").wait_for()
            assert await page.locator("#note-editor").is_hidden()
            delete_calls = await page.evaluate("testCalls.filter(c=>c.endpoint==='notes' && c.body.action==='delete')")
            assert len(delete_calls) == 2
            assert delete_calls[0]["body"] == delete_calls[1]["body"]
            assert delete_calls[0]["body"]["expected_updated_at"] == 104
            await page.select_option("#notes-status", "all")
            await page.locator("#notes-filter button[type=submit]").click()
            await page.locator("#notes-list").filter(has_text="没有笔记").wait_for()

            await page.locator('[data-page="models"]').click()
            await page.locator("#setting-llm_provider_id").wait_for()
            assert await page.locator("#setting-short_term_provider_id").count() == 1
            # The configured model need not be the first Provider in the list.
            assert await page.locator("#setting-llm_provider_id").input_value() == "llm-1"
            assert await page.locator("#probe-provider").input_value() == "llm-1"
            await page.select_option("#setting-llm_provider_id", "llm-first")
            assert await page.locator("#probe-provider").input_value() == "llm-first"
            await page.select_option("#setting-short_term_provider_id", "llm-1")
            await page.locator("#setting-embedding_batch_size").fill("8")
            await page.locator("#models-save").click()
            await page.locator("#settings-result").filter(has_text="配置已保存并生效").wait_for(state="attached")
            assert await page.locator("#models-state").is_hidden()
            saved = await page.evaluate("testCalls.filter(c=>c.endpoint==='settings').at(-1).body.changes")
            assert saved == {"llm_provider_id": "llm-first", "short_term_provider_id": "llm-1", "embedding_batch_size": 8}
            await page.locator("#probe-start").click()
            assert await page.locator("#probe-start").is_disabled()
            await page.locator("#probe-result").filter(has_text='连接成功').wait_for()
            assert await page.evaluate("testCalls.filter(c=>c.endpoint==='models/probe').length") == 1
            assert await page.evaluate("testCalls.filter(c=>c.endpoint==='models/probe').at(-1).body.provider_id") == "llm-first"
            # Explicit test choices survive rendering and do not change settings.
            await page.select_option("#probe-provider", "llm-1")
            await page.locator("#models-refresh").click()
            assert await page.locator("#probe-provider").input_value() == "llm-1"
            await page.locator("#probe-start").click()
            await page.wait_for_function("testCalls.filter(c=>c.endpoint==='models/probe').length===2 && !document.querySelector('#probe-start').disabled")
            assert await page.evaluate("testCalls.filter(c=>c.endpoint==='models/probe').at(-1).body.provider_id") == "llm-1"
            assert await page.locator("#setting-llm_provider_id").input_value() == "llm-first"
            assert await page.evaluate("testCalls.filter(c=>c.endpoint==='settings').length") == 1
            await page.select_option("#probe-kind", "embedding")
            assert await page.locator("#probe-provider").input_value() == "embed-1"
            await page.locator("#probe-start").click()
            await page.wait_for_function("testCalls.filter(c=>c.endpoint==='models/probe').length===3 && !document.querySelector('#probe-start').disabled")
            probe = await page.evaluate("testCalls.filter(c=>c.endpoint==='models/probe').at(-1).body")
            assert probe["kind"] == "embedding" and probe["provider_id"] == "embed-1"
            await page.select_option("#probe-kind", "llm")
            assert await page.locator("#probe-provider").input_value() == "llm-first"
            assert await page.get_by_role("button", name="记忆 LLM恢复默认", exact=True).count() == 0
            await page.select_option("#setting-llm_provider_id", "")
            assert await page.locator("#probe-provider").input_value() == ""
            assert await page.locator("#probe-start").is_disabled()
            await page.select_option("#setting-llm_provider_id", "llm-1")
            assert await page.locator("#probe-start").is_enabled()
            # A disappearing Provider must not silently select another model.
            await page.evaluate("""() => {
const previous=window.AstrBotPluginView.apiGet;
window.restoreModels=()=>{window.AstrBotPluginView.apiGet=previous};
window.AstrBotPluginView.apiGet=async(endpoint,args)=>{
 const result=await previous(endpoint,args);
 if(endpoint==='models')result.llm=result.llm.filter(item=>item.provider_id!=='llm-1');
 return result;
}; }""")
            await page.locator("#models-refresh").click()
            await page.locator("#probe-provider").filter(has_text="当前不可用").wait_for()
            assert await page.locator("#probe-provider").input_value() == "llm-1"
            assert await page.locator("#probe-start").is_disabled()
            await page.evaluate("window.restoreModels()")
            await page.locator("#models-refresh").click()
            await page.locator('[data-page="settings"]').click()
            await page.get_by_role("button", name="留下继续编辑").click()
            assert await page.locator('#page-models').is_visible()
            await page.locator('[data-page="settings"]').click()
            await page.get_by_role("button", name="放弃并离开").click()
            assert await page.locator("#settings-reload").is_hidden()
            assert await page.locator("#settings-save").is_disabled()
            assert await page.locator("#settings-discard").is_hidden()
            await page.locator('[data-settings-group="外观"]').click()
            await page.locator('#appearance-mode').select_option("dark")
            assert await page.locator('html').get_attribute('data-memory-theme') == 'dark'
            await page.locator('#settings-save').click()
            await page.wait_for_function("window.testTheme?.mode==='dark' && document.querySelector('#settings-save').disabled")
            await page.locator('[data-settings-group="功能"]').click()
            await page.locator('#setting-recall_enabled').uncheck()
            await page.locator('#settings-reset-all').click()
            assert await page.locator('#setting-recall_enabled').is_checked()
            assert await page.evaluate("window.testTheme.mode") == 'dark'
            await page.locator('#settings-discard').click()
            assert await page.locator('html').get_attribute('data-memory-theme') == 'dark'
            assert await page.locator("#settings-save").is_disabled()
            await page.locator('[data-settings-group="高级"]').click()
            await page.locator("#setting-recall_history_seconds").fill("100")
            reset = page.get_by_role("button", name="历史时间范围（秒）恢复默认", exact=True)
            assert await reset.is_enabled()
            await page.locator("#setting-recall_history_seconds").fill("7200")
            assert await reset.is_disabled()
            await page.locator("#setting-recall_history_seconds").fill("100")
            await reset.click()
            assert await reset.is_disabled()
            assert await page.locator("#setting-recall_history_seconds").input_value() == "7200"
            # Persistence can succeed before application fails. Refreshing the
            # saved revision enables explicit retry without discarding a draft.
            await page.evaluate("window.testApplyFailure=true")
            await page.locator("#setting-recall_history_seconds").fill("120")
            await page.locator("#settings-save").click()
            await page.locator("#settings-result").filter(has_text="配置已保存但尚未完整应用").wait_for(state="attached")
            await page.locator("#settings-apply").wait_for()
            assert await page.locator("#setting-recall_history_seconds").input_value() == "120"
            assert "运行值：7200" in await page.locator("#settings-fields").inner_text()
            await page.locator("#setting-recall_history_seconds").fill("240")
            await page.evaluate("window.testApplyFailure=false")
            await page.locator("#settings-apply").click()
            await page.locator("#settings-result").filter(has_text="配置已保存并生效").wait_for(state="attached")
            assert await page.evaluate("testCalls.filter(c=>c.endpoint==='settings').at(-1).body.changes") == {}
            assert await page.locator("#settings-reload").is_hidden()
            assert await page.locator("#settings-apply").is_hidden()
            assert await page.locator("#setting-recall_history_seconds").input_value() == "240"
            await page.locator("#settings-refresh").click()
            await page.wait_for_function("document.querySelector('#setting-recall_history_seconds').value==='120'")
            # Enumerated options use a select and successful HTTP responses
            # must still report whether the saved configuration was applied.
            assert await page.locator("#setting-external_memory_mode").evaluate("node=>node.tagName") == "SELECT"
            assert await page.locator("#setting-external_memory_mode option").all_text_contents() == ["supplement", "owner"]
            await page.locator("#setting-external_memory_mode").select_option("owner")
            await page.evaluate("window.testPendingResponse=true")
            await page.locator("#settings-save").click()
            await page.locator("#settings-result").filter(has_text="配置已保存但尚未完整应用").wait_for(state="attached")
            assert await page.locator("#settings-apply").is_visible()
            await page.evaluate("window.testPendingResponse=false")
            await page.locator("#settings-apply").click()
            await page.locator("#settings-result").filter(has_text="配置已保存并生效").wait_for(state="attached")
            assert await page.locator("#setting-external_memory_mode").input_value() == "owner"
            await page.locator("#setting-external_memory_mode").select_option("supplement")
            await page.evaluate("window.testExternalResponse=true")
            await page.locator("#settings-save").click()
            await page.locator("#settings-result").filter(has_text="宿主配置已在其他位置修改").wait_for(state="attached")
            assert await page.locator("#settings-save").is_disabled()
            assert await page.locator("#settings-apply").is_hidden()

            await page.locator('[data-page="maintenance"]').click()
            await page.get_by_role("button", name="重新生成当前窗口", exact=True).click()
            await page.wait_for_function("window.testCalls.some(c=>c.endpoint==='tasks/retry' && c.body.regenerate_window===true && c.body.job_id==='job-failed')")
            assert "短期滚动摘要" in await page.locator("#tasks").text_content()
            assert "查找关联旧记忆，已扫描 256 条" in await page.locator("#tasks").inner_text()
            indexed = page.locator("#tasks article").filter(has_text="job-index")
            assert await indexed.get_by_role("progressbar").count() == 1
            assert "总用时：" in await indexed.inner_text()
            assert "向量准备 16 / 20" not in await indexed.inner_text()
            await indexed.get_by_role("radio").check()
            await page.locator("#task-detail-panel > summary").click()
            assert "向量准备" not in await page.locator("#restart-hint").inner_text()
            await page.locator('[data-maintenance-tab="manual"]').click()
            await page.locator("#automatic-title").filter(has_text="自动记忆更新已暂停").wait_for()
            await page.locator("#automatic-jobs button").click()
            await page.locator("#task-sources button").click()
            await page.locator("#task-source-content").filter(has_text="PRIVATE SOURCE").wait_for()
            call = await page.evaluate("testGetCalls.filter(c=>c.endpoint==='memory/source').at(-1)")
            assert call["args"] == {"space_id": "space-1", "source_id": "source-failed"}
            await page.locator("#tasks article").filter(has_text="job-short").get_by_role("radio").check()
            assert await page.locator("#task-source-panel").is_hidden()
            running_task = page.locator("#tasks article").filter(has_text="job-context")
            assert await running_task.get_by_role("button", name="删除任务", exact=True).count() == 0
            cancel = running_task.locator(".task-heading button.danger")
            dialog = page.get_by_role("dialog", name="取消这项任务？")
            for dismiss in ("继续执行", "Escape", "确认取消任务"):
                await cancel.click()
                assert "job-context" in await dialog.inner_text()
                assert not await page.evaluate("testCalls.some(c=>c.endpoint==='tasks/cancel')")
                if dismiss == "Escape":
                    await page.keyboard.press("Escape")
                else:
                    await dialog.get_by_role("button", name=dismiss).click()
            await page.wait_for_function("testCalls.filter(c=>c.endpoint==='tasks/cancel' && c.body.job_id==='job-context').length===1")
            await page.wait_for_function("document.body.getAttribute('aria-busy') !== 'true'")
            stopped_task = page.locator("#tasks article").filter(has_text="job-short")
            await stopped_task.get_by_role("button", name="删除任务", exact=True).click()
            await stopped_task.get_by_role("button", name="确认删除任务", exact=True).click()
            await page.wait_for_function("testCalls.some(c=>c.endpoint==='tasks/delete'&&c.body.job_id==='job-short')")
            await stopped_task.wait_for(state="detached")
            assert await page.locator("#task-detail").text_content() == ""
            assert "只保存在本次运行的内存" in await page.locator("#ingress-unpersisted").text_content()
            assert not await page.evaluate("testGetCalls.some(c=>c.endpoint==='ingress/record')")
            await page.locator("#ingress-list").get_by_role("button", name="查看原文").click()
            await page.locator("#ingress-detail").filter(has_text="PRIVATE INGRESS").wait_for()
            await page.locator("#clear-ingress-detail").click()
            await page.locator("#retry-ingress").click()
            await page.locator("#notifications .toast").filter(has_text="重新接收 2 条").last.wait_for()
            await page.locator('[data-maintenance-tab="manual"]').click()
            # Title-only edits preserve a separate semantic payload.
            await page.locator("#node-title").fill("Manually revised title")
            await page.locator("#draft-operation").click()
            title_edit = await page.locator("#operation").input_value()
            import json
            title_edit = json.loads(title_edit)
            assert title_edit["patch"] == {"title": "Manually revised title"}
            assert title_edit["expected_titles"] == {"node-1": "Original node-1"}
            assert title_edit["expected_updated_at"] == {"node-1": 100}
            await page.locator("#governance").click()
            await page.wait_for_function("testCalls.some(c=>c.body.operation?.patch?.title==='Manually revised title')")
            await page.wait_for_function("!document.querySelector('#governance').disabled")
            await page.select_option("#action", "split")
            await page.locator("#split-parts > fieldset").nth(1).locator(".part-content").fill("Second part")
            await page.locator("#split-parts > fieldset").nth(1).locator(".part-title").fill("Second title")
            await page.locator("#governance").click()
            await page.wait_for_function("testCalls.some(c=>c.body.operation?.action==='split')")
            await page.wait_for_function("!document.querySelector('#governance').disabled")
            split = await page.evaluate("testCalls.find(c=>c.body.operation?.action==='split').body.operation")
            assert len(split["parts"]) == 2 and split["parts"][1]["source_ids"] == ["source-1"]
            assert split["parts"][1]["title"] == "Second title"
            await page.select_option("#action", "merge")
            assert await page.locator("#merge-nodes option").count() == 1
            await page.select_option("#merge-nodes", ["node-2"])
            await page.locator("#compose-merge").click()
            await page.locator("#governance").click()
            await page.wait_for_function("testCalls.some(c=>c.body.operation?.action==='merge')")
            merged = await page.evaluate("testCalls.find(c=>c.body.operation?.action==='merge').body.operation")
            assert merged["node_ids"] == ["node-1", "node-2"]
            assert merged["keep_id"] == "node-1"
            assert merged["title"] == "Manually revised title"
            assert merged["expected_titles"] == {"node-1": "Original node-1", "node-2": "Original node-2"}
            await page.wait_for_function("!document.querySelector('#governance').disabled")
            await page.select_option("#action", "delete")
            assert await page.locator("#governance").is_disabled()
            await page.locator("#delete-confirm").check()
            assert await page.locator("#governance").is_enabled()
            await page.screenshot(path="/tmp/neomemory-stage6-desktop.png", full_page=True)
            await page.set_viewport_size({"width": 390, "height": 844})
            for name in ["memory", "notes", "models", "settings", "maintenance"]:
                await page.locator(f'[data-page="{name}"]').click()
                assert await page.evaluate("document.documentElement.scrollWidth <= window.innerWidth"), name
            await page.screenshot(path="/tmp/neomemory-stage6-mobile.png", full_page=True)
            # Empty spaces/providers and a host configuration changed elsewhere.
            empty = await browser.new_page(viewport={"width": 390, "height": 844})
            empty.on("pageerror", lambda error: errors.append(str(error)))
            await empty.add_init_script(MOCK_BRIDGE + r"""
const originalGet=window.AstrBotPluginView.apiGet;
window.AstrBotPluginView.apiGet=async(endpoint,args)=>{
 if(endpoint==='memory/spaces')return {items:[],has_more:false};
 if(endpoint==='models')return {llm:[],embedding:[]};
 const result=await originalGet(endpoint,args);
 if(endpoint==='settings'){result.can_save=false;result.external_change=true;result.reload_required=true;}
 return result;
};
""")
            await empty.goto(f"http://127.0.0.1:{server.server_port}/index.html")
            await empty.locator("#memory-list").filter(has_text="暂无记忆空间").wait_for()
            await empty.locator('[data-page="models"]').click()
            await empty.locator("#probe-provider").filter(has_text="没有可用模型").wait_for()
            assert await empty.locator("#probe-start").is_disabled()
            assert await empty.locator("#models-save").is_disabled()
            await empty.locator('[data-page="settings"]').click()
            await empty.locator("#settings-reload").filter(has_text="宿主配置已在其他位置修改").wait_for()
            assert await empty.locator("#settings-save").is_disabled()
            await empty.close()
            # A failed read clears stale rows; the browser also has a useful
            # diagnostic when opened without AstrBot's authenticated bridge.
            await page.locator('[data-page="memory"]').click()
            await page.evaluate("""() => { const previous=window.AstrBotPluginView.apiGet;
window.AstrBotPluginView.apiGet=async(endpoint,args)=>{if(endpoint==='memory/nodes')throw Error('Synthetic service unavailable');return previous(endpoint,args)}; }""")
            await page.locator("#memory-filter button[type=submit]").click()
            await page.locator("#memory-meta").filter(has_text="读取失败").wait_for()
            assert await page.locator("#memory-list article").count() == 0
            unavailable = await browser.new_page()
            unavailable.on("pageerror", lambda error: errors.append(str(error)))
            await unavailable.goto(f"http://127.0.0.1:{server.server_port}/index.html")
            await unavailable.locator("#notifications .toast").filter(has_text="请从 AstrBot 插件页面打开").last.wait_for()
            await unavailable.close()
            assert not errors, errors
            await browser.close()
            print("browser smoke passed: inline titles, version provenance and title changes, per-card sources, retry and navigation, pagination and stale-response guards, title-only governance, five pages, scope races, safe text, explicit sources, note lifecycle, schema defaults, live configuration and apply retry, direct model probes, ingress, visual split/merge, deletion confirmation, keyboard tabs, 390px layout, empty/unavailable services, external configuration changes")
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    asyncio.run(main())
