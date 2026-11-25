"""Synthetic browser coverage for logical-user naming and account confirmation."""
import asyncio
import functools
from http.server import ThreadingHTTPServer
from pathlib import Path
import threading

from playwright.async_api import async_playwright, expect

from tests.ui.full_smoke import MOCK_BRIDGE, QuietHandler

IDENTITIES = r"""
const account=(id,entrances=['test-platform'])=>({platform_instance_id:entrances[0],platform_instance_ids:entrances,account_id:id,binding_status:'active'});
window.identityUsers=[
 {logical_user_id:'lu_1234567aaaaaaaaaaaaaaaaaaaaaaaaa',display_name:'甲',display_name_source:'manual',status:'active',accounts:[account('a',['test-platform','second-entry']),account('a2')]},
 {logical_user_id:'user_1234567bbbbbbbbbbbbbbbbbbbbbbbbb',display_name:'乙',display_name_source:'observed',status:'active',accounts:[account('b')]},
 {logical_user_id:'u-c',display_name:'丙',display_name_source:'observed',status:'active',accounts:[account('c'),account('c2')]}
];
window.identityCalls=[]; window.identityReads=[];window.deferIdentity=false;window.failIdentity=false;
const identityGet=window.AstrBotPluginView.apiGet,identityPost=window.AstrBotPluginView.apiPost;
window.AstrBotPluginView.apiGet=async(endpoint,args={})=>{
 if(endpoint==='identities'){
  identityReads.push(args.space_id);
  const scoped=args.space_id==='space-1'?identityUsers:args.space_id==='space-2'?identityUsers.filter(user=>user.logical_user_id==='u-c'):[];
  const users=scoped.filter(user=>!args.q||user.display_name.includes(args.q)||user.logical_user_id.includes(args.q));
  if(window.deferUserList){window.deferUserList=false;await new Promise(resolve=>window.releaseUserList=resolve);}
  const duplicates=identityUsers.filter(user=>user.accounts.some(account=>account.account_id==='a'));
  const conflicts=duplicates.length>1&&duplicates.some(user=>users.includes(user))?[{account_id:'a',owners:duplicates.map(user=>({
   logical_user_id:user.logical_user_id,display_name:user.display_name,
   platform_instance_ids:user.accounts.find(account=>account.account_id==='a').platform_instance_ids,
   private_spaces:[{space_id:user.logical_user_id,persona_id:'人格甲'}],counts:{nodes:12,notes:3}
  }))}]:[];
  const end=args.offset+args.limit;
  return {conflicts,items:structuredClone(users.slice(args.offset,end)),has_more:end<users.length};
 }
 return identityGet(endpoint,args);
};
window.AstrBotPluginView.apiPost=async(endpoint,body)=>{
 if(!endpoint.startsWith('identities/'))return identityPost(endpoint,body);
 identityCalls.push({endpoint,body});
 const target=identityUsers.find(user=>user.logical_user_id===body.logical_user_id);
 if(endpoint==='identities/rename'){target.display_name=body.display_name;target.display_name_source='manual';return structuredClone(target);}
 const previous=identityUsers.find(user=>user.accounts.some(a=>a.account_id===body.account_id&&a.platform_instance_ids.includes(body.platform_instance_id)));
 const transferring=previous?.accounts.find(a=>a.account_id===body.account_id) || account(body.account_id,[body.platform_instance_id]);
 if(endpoint==='identities/preview'&&window.deferPreview){window.deferPreview=false;await new Promise(resolve=>window.releasePreview=resolve);}
 if(endpoint==='identities/preview')return {
  action:body.action,target_user:structuredClone(target),previous_user:structuredClone(previous||null),
  previous_private_spaces:previous&&body.action==='bind'?[{persona_id:'人格甲'},{persona_id:'人格乙'}]:[],
  account:transferring,
  delete_previous_user:body.action==='bind'&&previous&&previous.accounts.length===1,
  counts:body.action==='bind'&&previous?.accounts.length===1?{private_spaces:2,nodes:12,node_versions:15,profiles:2,profile_versions:3,relationships:2,relationship_versions:4,short_term:1,notes:3,tasks:5}:{},
  raw_records_preserved:true,shared_group_memories_preserved:true,confirmation_token:'preview-'+identityCalls.length
 };
 if(endpoint==='identities/apply'){
  if(deferIdentity)await new Promise(resolve=>window.releaseIdentity=resolve);
  if(failIdentity)throw Error('合成变更提交失败');
  if(body.action==='bind'){
   if(previous){previous.accounts=previous.accounts.filter(a=>a.account_id!==body.account_id);if(!previous.accounts.length)identityUsers=identityUsers.filter(u=>u!==previous);}
   const existing=target.accounts.find(a=>a.account_id===body.account_id);
   if(existing)existing.platform_instance_ids.push(...transferring.platform_instance_ids);
   else target.accounts.push(transferring);
  }else{
   target.accounts=target.accounts.filter(a=>a.account_id!==body.account_id);
   identityUsers.push({logical_user_id:'fresh-'+body.account_id,display_name:'独立账号',display_name_source:'observed',status:'active',accounts:[account(body.account_id)]});
  }
  const job={job_id:'identity-job-'+identityCalls.length,kind:'identity',scope_key:'*',state:'succeeded',step:'complete',progress_done:1,progress_total:1};
  testJobs.push(job);return job;
 }
};
"""


async def run_case(page, artifacts, name):
    await page.evaluate("testIngress.push({...testIngress[0],source_id:'pending-owner',state:'retry_wait',error_code:'identity_binding_conflict'})")
    await page.locator('[data-page=maintenance]').click()
    await expect(page.locator('#ingress-list')).to_contain_text('请在用户管理中处理旧归属')
    await page.locator('[data-maintenance-tab=users]').click()
    guide = page.locator('#identity-guide')
    await expect(guide).to_be_visible()
    assert await guide.evaluate("el => el.nextElementSibling.id === 'identities-panel'")
    details = guide.locator('details')
    assert not await details.evaluate('el => el.open')
    await details.locator('summary').click()
    for example in ('OneBot', '人格', 'Telegram', '旧归属'):
        await expect(details.locator('ul')).to_contain_text(example)
    await expect(details.locator('ul')).to_be_visible()
    await details.locator('summary').click()
    await expect(details.locator('ul')).to_be_hidden()
    await expect(page.locator('#identity-select')).to_have_value('lu_1234567aaaaaaaaaaaaaaaaaaaaaaaaa')
    await expect(page.locator('#identity-id')).to_have_text('逻辑用户标识：lu_1234567')
    await expect(page.locator('#identity-accounts')).to_contain_text('消息入口：test-platform、second-entry')
    await expect(page.locator('#identity-conflicts')).to_be_hidden()
    await expect(page.locator('#identity-select option:checked')).to_have_text('甲 · lu_1234567 · 2 个账号')
    picker = page.locator('#identity-select')
    assert await picker.evaluate('el => Math.abs(el.clientWidth + 2 * el.clientLeft - el.parentElement.clientWidth) <= 1')
    pager = page.locator('#identities-pagination')
    prev_page, next_page = pager.locator('#identities-prev'), pager.locator('#identities-next')
    await expect(pager).to_be_hidden()
    await page.evaluate("""identityUsers.push(...Array.from({length:58}, (_, i) => ({
        logical_user_id:'extra-'+i, display_name:'分页用户'+i, accounts:[]
    })))""")
    await page.locator('#identities-refresh').click()
    await expect(next_page).to_be_visible()
    await expect(prev_page).to_be_hidden()
    await next_page.click()
    await expect(prev_page).to_be_visible()
    await expect(next_page).to_be_visible()
    await next_page.click()
    await expect(page.locator('#identities-page')).to_have_text('第 3 页')
    await expect(next_page).to_be_hidden()
    await prev_page.click()
    await expect(page.locator('#identities-page')).to_have_text('第 2 页')
    await page.locator('#identity-q').fill('无匹配名称')
    await page.locator('#identities-filter button[type=submit]').click()
    await expect(picker.locator('option:checked')).to_have_text('没有匹配的逻辑用户')
    await expect(pager).to_be_hidden()
    await page.evaluate('identityUsers.splice(3)')
    await page.locator('#identity-q').fill('')
    await page.locator('#identities-filter button[type=submit]').click()
    await expect(picker).to_have_value('lu_1234567aaaaaaaaaaaaaaaaaaaaaaaaa')
    spaces=page.locator('#space')
    await page.evaluate('deferUserList=true')
    await page.locator('#identities-refresh').click()
    await page.wait_for_function('window.releaseUserList !== undefined')
    await spaces.select_option('space-2')
    await expect(page.locator('#identity-select')).to_have_value('u-c')
    await page.evaluate('releaseUserList()')
    await expect(page.locator('#identity-select option')).to_have_count(1)
    assert await page.locator('#identity-known-account option').count() == 1
    await spaces.select_option('')
    await expect(page.locator('#identities-status')).to_have_text('请先选择记忆空间。')
    await expect(page.locator('#identity-accounts .identity-account')).to_have_count(0)
    await expect(page.locator('#identity-rename')).to_be_disabled()
    assert await page.evaluate('identityReads.every(Boolean)')
    await spaces.select_option('space-1')
    await expect(page.locator('#identity-select')).to_have_value('lu_1234567aaaaaaaaaaaaaaaaaaaaaaaaa')
    await expect(page.locator('#identity-accounts .identity-account')).to_have_count(2)
    await page.locator('#identity-select').select_option('user_1234567bbbbbbbbbbbbbbbbbbbbbbbbb')
    await expect(page.locator('#identity-accounts button')).to_be_disabled()
    await page.locator('#identity-select').select_option('lu_1234567aaaaaaaaaaaaaaaaaaaaaaaaa')
    await page.locator('#identity-name').fill('新的称呼')
    await page.locator('#identity-rename').click()
    await expect(page.locator('#identity-name')).to_have_value('新的称呼')
    await page.wait_for_function("identityUsers[0].display_name==='新的称呼'")
    assert await page.evaluate("identityCalls.filter(call=>call.endpoint==='identities/apply').length") == 0
    await page.locator('#identity-q').fill('新的称呼')
    await page.locator('#identities-filter button[type=submit]').click()
    await expect(page.locator('#identities-status')).to_have_text('本页 1 位逻辑用户')
    await page.locator('#identity-q').fill('')
    await page.locator('#identities-filter button[type=submit]').click()
    await expect(page.locator('#identities-status')).to_have_text('本页 3 位逻辑用户')
    await page.locator('#identity-known-account').select_option(label='test-platform / b · 乙')
    await expect(page.locator('#identity-platform')).to_have_value('test-platform')
    await expect(page.locator('#identity-account')).to_have_value('b')
    await page.evaluate('window.deferPreview=true')
    await page.locator('#identity-bind').click()
    await page.wait_for_function('window.releasePreview !== undefined')
    await spaces.select_option('space-2')
    await expect(page.locator('#identity-select')).to_have_value('u-c')
    await page.evaluate('releasePreview()')
    await expect(page.locator('#identity-bind')).to_be_enabled()
    await expect(page.locator('#identity-confirm-dialog')).not_to_be_visible()
    await expect(page.locator('#identity-account')).to_have_value('')
    await spaces.select_option('space-1')
    await expect(page.locator('#identity-select')).to_have_value('lu_1234567aaaaaaaaaaaaaaaaaaaaaaaaa')
    await page.locator('#identity-known-account').select_option(label='test-platform / b · 乙')
    await page.locator('#identity-bind').click()
    dialog=page.locator('#identity-confirm-dialog')
    await expect(dialog).to_be_visible()
    await expect(page.locator('#identity-confirm-title')).to_have_text('确认转移账号？')
    await expect(page.locator('#identity-confirm-apply')).to_have_text('确认转移账号')
    await expect(dialog).to_contain_text('将删除以下私聊空间')
    await expect(dialog).to_contain_text('人格乙 · 乙')
    await expect(dialog).to_contain_text('乙（lu_1234567）')
    await expect(dialog).to_contain_text('新的称呼（lu_1234567）')
    await expect(dialog).to_contain_text('长期记忆：12')
    await expect(dialog).to_contain_text('笔记：3')
    await page.screenshot(path=str(artifacts/f'{name}-bind-confirm.png'))
    await page.locator('#identity-confirm-cancel').click()
    await expect(dialog).not_to_be_visible()
    assert await page.evaluate("identityCalls.filter(call=>call.endpoint==='identities/apply').length") == 0
    await page.locator('#identity-bind').click()
    await expect(dialog).to_be_visible()
    await page.keyboard.press('Escape')
    await expect(dialog).not_to_be_visible()
    await page.locator('#identity-bind').click()
    await page.evaluate('deferIdentity=true;failIdentity=true')
    await page.locator('#identity-confirm-apply').click()
    await expect(page.locator('#identity-confirm-cancel')).to_be_disabled()
    await expect(page.locator('#identity-confirm-apply')).to_be_disabled()
    await page.keyboard.press('Escape')
    await expect(dialog).to_be_visible()
    await page.evaluate('releaseIdentity()')
    await expect(page.locator('#identity-confirm-error')).to_contain_text('合成变更提交失败')
    await expect(page.locator('#identity-confirm-apply')).to_be_enabled()
    await page.evaluate('deferIdentity=false;failIdentity=false')
    await page.locator('#identity-confirm-apply').click()
    await expect(dialog).not_to_be_visible()
    await page.wait_for_function("identityUsers.every(u=>u.logical_user_id!=='user_1234567bbbbbbbbbbbbbbbbbbbbbbbbb')")
    sent=await page.evaluate("identityCalls.filter(call=>call.endpoint==='identities/apply').map(call=>call.body)")
    assert len(sent)==2 and sent[0]==sent[1]
    assert await page.evaluate("identityCalls.every(call=>call.body.space_id==='space-1')")
    await expect(page.locator('[data-maintenance-tab="tasks"]')).to_have_attribute('aria-selected','true')
    await page.locator('[data-maintenance-tab="users"]').click()
    await expect(page.locator('#identity-accounts .identity-account')).to_have_count(3)

    await page.locator('#identity-known-account').select_option(label='test-platform / c · 丙')
    await page.locator('#identity-bind').click()
    await expect(dialog).to_contain_text('全部既有记忆保留')
    await expect(dialog).to_contain_text('原逻辑用户保留以下私聊空间')
    await expect(dialog).to_contain_text('人格甲 · 丙')
    await expect(dialog.locator('.identity-removal')).to_have_count(0)
    await page.locator('#identity-confirm-cancel').click()
    b=page.locator('.identity-account').filter(has=page.locator('p').filter(has_text='b'))
    await b.get_by_role('button',name='移除关联').click()
    await expect(dialog).to_contain_text('既有记忆继续归属原逻辑用户')
    await expect(dialog).to_contain_text('不进行重构')
    await page.screenshot(path=str(artifacts/f'{name}-confirm.png'))
    await page.locator('#identity-confirm-apply').click()
    await expect(dialog).not_to_be_visible()
    await page.wait_for_function("identityUsers.some(u=>u.logical_user_id==='fresh-b')")
    await expect(page.locator('[data-maintenance-tab="tasks"]')).to_have_attribute('aria-selected','true')
    await page.locator('[data-maintenance-tab="users"]').click()
    await expect(page.locator('#identity-accounts .identity-account')).to_have_count(2)
    await page.evaluate("""identityUsers.push({logical_user_id:'legacy-user',display_name:'旧归属',
        accounts:[{account_id:'a', platform_instance_id:'legacy-entry-1', platform_instance_ids:['legacy-entry-1','legacy-entry-2'], binding_status:'active'}]})""")
    await page.locator('#identities-refresh').click()
    conflict=page.locator('#identity-conflicts')
    await expect(conflict).to_be_visible()
    await expect(conflict).to_contain_text('旧归属')
    await expect(conflict).to_contain_text('私聊空间：人格甲')
    await expect(conflict).to_contain_text('长期记忆 12')
    await spaces.select_option('space-2')
    await expect(conflict).to_be_hidden()
    await spaces.select_option('space-1')
    transfer=conflict.get_by_role('button',name='预览转入当前用户')
    before=await page.evaluate("identityCalls.filter(c=>c.endpoint==='identities/apply').length")
    await transfer.click()
    await expect(dialog).to_contain_text('本次变更的消息入口：legacy-entry-1、legacy-entry-2')
    await expect(dialog).to_contain_text('将删除以下私聊空间')
    await page.locator('#identity-confirm-cancel').click()
    assert await page.evaluate("identityCalls.filter(c=>c.endpoint==='identities/apply').length") == before
    await transfer.click()
    await page.locator('#identity-confirm-apply').click()
    await expect(dialog).not_to_be_visible()
    await expect(page.locator('[data-maintenance-tab="tasks"]')).to_have_attribute('aria-selected','true')
    await page.locator('[data-maintenance-tab="users"]').click()
    await expect(conflict).to_be_hidden()
    await expect(page.locator('#identity-accounts .identity-account')).to_have_count(2)
    await expect(page.locator('#identity-accounts')).to_contain_text('test-platform、second-entry、legacy-entry-1、legacy-entry-2')
    await page.locator('#identities-panel').scroll_into_view_if_needed()
    assert await page.evaluate('document.documentElement.scrollWidth<=innerWidth')
    await page.screenshot(path=str(artifacts/f'{name}-panel.png'))


async def main():
    root=Path(__file__).resolve().parents[2]
    artifacts=root/'data/evaluations/identities-webui';artifacts.mkdir(parents=True,exist_ok=True)
    server=ThreadingHTTPServer(('127.0.0.1',0),functools.partial(QuietHandler,directory=str(root/'views/neomemory')))
    threading.Thread(target=server.serve_forever,daemon=True).start()
    try:
        async with async_playwright() as playwright:
            browser=await playwright.chromium.launch()
            for theme in ('light','dark'):
                for width in (1440,390):
                    page=await browser.new_page(viewport={'width':width,'height':1000},color_scheme=theme)
                    errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
                    await page.add_init_script(MOCK_BRIDGE+IDENTITIES)
                    await page.goto(f'http://127.0.0.1:{server.server_port}/index.html')
                    await run_case(page,artifacts,f'{theme}-{width}')
                    assert not errors,errors
                    await page.close()
            await browser.close()
            print('identities UI: 4 theme/viewport cases passed; rename/search, last-account constraint, binding preview counts, cancel/Escape, pending lock, idempotent retry, shared-user preservation and detach')
    finally:
        server.shutdown();server.server_close()


if __name__=='__main__':
    asyncio.run(main())
