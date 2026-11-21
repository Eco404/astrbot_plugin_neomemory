import { $, element, notify, on, onSearchForm, newKey, enhanceSelect, shortLogicalUserId } from './ui.js';

export function initIdentities(ui) {
  const state = { items: [], selected: null, offset: 0, hasMore: false, request: 0, loading: false,
    posting: false, pending: null, job: null, polling: false, trigger: null, known: [], conflicts: [] };
  const selected = () => state.items.find(user => user.logical_user_id === state.selected);
  const accounts = user => user?.accounts || [];
  const name = user => user ? `${user.display_name || '未命名用户'}（${shortLogicalUserId(user.logical_user_id)}）` : '尚未关联逻辑用户';
  const entries = account => (account.platform_instance_ids || [account.platform_instance_id]).join('、');
  const countLabels = { private_spaces:'私聊空间', nodes:'长期记忆', node_versions:'节点历史版本', profiles:'用户画像', profile_versions:'画像历史版本', relationships:'人格关系', relationship_versions:'关系历史版本', short_term:'短期摘要', notes:'笔记', tasks:'后台任务' };

  function renderConflicts() {
    const panel = $('identity-conflicts');
    panel.replaceChildren();
    for (const conflict of state.conflicts.filter(item => item.owners.some(owner => owner.logical_user_id === state.selected))) {
      const card = element('div', '', 'notice');
      card.append(element('h3', '账号归属待确认'), element('p', '账号 ' + conflict.account_id + ' 在不同入口关联了多个逻辑用户。'));
      for (const owner of conflict.owners) {
        const current = owner.logical_user_id === state.selected;
        const section = element('div', '', 'identity-conflict-owner');
        section.append(element('strong', name(owner) + (current ? ' · 当前用户' : '')),
          element('p', '消息入口：' + entries(owner)),
          element('p', '私聊空间：' + (owner.private_spaces.map(space => space.persona_id).join('、') || '无')));
        const counts = Object.entries(countLabels).filter(([key]) => key !== 'private_spaces' && owner.counts[key] > 0)
          .map(([key, label]) => label + ' ' + owner.counts[key]);
        section.append(element('p', '已有内容：' + (counts.join(' · ') || '无')));
        if (!current) {
          const transfer = element('button', '预览转入当前用户'); transfer.type = 'button';
          transfer.addEventListener('click', () => preview('bind', {
            platform_instance_id: owner.platform_instance_ids[0], account_id: conflict.account_id
          }, transfer).catch(error => notify(error.message, true)));
          section.append(transfer);
        }
        card.append(section);
      }
      panel.append(card);
    }
    panel.hidden = !panel.childElementCount;
  }
  function controls() {
    const busy = !ui.space() || state.posting || state.loading || Boolean(state.job);
    $('identity-editor').disabled = busy || !selected();
    $('identity-select').disabled = busy;
    $('identities-filter').querySelectorAll('button,input').forEach(node => { node.disabled = busy; });
    $('identities-prev').disabled = busy || state.offset === 0;
    $('identities-next').disabled = busy || !state.hasMore;
    $('identity-confirm-cancel').disabled = $('identity-confirm-apply').disabled = state.posting;
    $('identity-confirm-progress').hidden = !state.posting;
  }
  function renderUser() {
    const user = selected();
    $('identity-id').textContent = user ? '逻辑用户标识：' + shortLogicalUserId(user.logical_user_id) : '请选择逻辑用户。';
    $('identity-name').value = user?.display_name || '';
    $('identity-accounts').replaceChildren();
    const linked = accounts(user);
    for (const account of linked) {
      const row = element('div', '', 'identity-account'), detail = element('div');
      detail.append(element('p', account.account_id), element('p', '消息入口：' + entries(account), 'muted'));
      if (account.binding_status !== 'active') detail.append(element('p', account.binding_status === 'disabled' ? '关联已停用' : '关联存在冲突', 'muted'));
      const remove = element('button', '移除关联'); remove.type = 'button';
      remove.disabled = linked.length < 2;
      if (remove.disabled) remove.title = '逻辑用户必须至少保留一个账号';
      remove.addEventListener('click', () => preview('unbind', account, remove).catch(error => notify(error.message, true)));
      row.append(detail, remove); $('identity-accounts').append(row);
    }
    if (user && linked.length < 2) $('identity-accounts').append(element('p', '仅有一个关联账号，不能移除。', 'muted'));
    renderConflicts();
    state.known = state.items.flatMap(owner => accounts(owner).filter(account => owner.logical_user_id !== state.selected)
      .map(account => ({ ...account, owner })));
    $('identity-known-account').replaceChildren(element('option', '手动填写账号'));
    $('identity-known-account').firstChild.value = '';
    state.known.forEach((account, index) => {
      const option = element('option', `${entries(account)} / ${account.account_id} · ${account.owner.display_name || shortLogicalUserId(account.owner.logical_user_id)}`);
      option.value = String(index); $('identity-known-account').append(option);
    });
    enhanceSelect($('identity-known-account'));
    $('identity-platform').value = ''; $('identity-account').value = '';
    controls();
  }
  async function refresh() {
    const request = ++state.request, previous = state.selected;
    if (!ui.space()) { $('identities-status').textContent = '请先选择记忆空间。'; controls(); return; }
    state.loading = true; controls(); $('identities-status').textContent = '正在读取逻辑用户…';
    try {
      const result = await ui.bridge.apiGet('identities', { space_id: ui.space(), q: $('identity-q').value.trim(), limit: 30, offset: state.offset });
      if (request !== state.request) return;
      state.items = result.items; state.conflicts = result.conflicts || []; state.hasMore = result.has_more;
      state.selected = state.items.some(user => user.logical_user_id === previous) ? previous : state.items[0]?.logical_user_id || null;
      $('identity-select').replaceChildren();
      for (const user of state.items) {
        const option = element('option', `${user.display_name || '未命名用户'} · ${shortLogicalUserId(user.logical_user_id)} · ${accounts(user).length} 个账号`);
        option.value = user.logical_user_id; $('identity-select').append(option);
      }
      if (!state.items.length) { const option = element('option', '没有匹配的逻辑用户'); option.value = ''; $('identity-select').append(option); }
      $('identity-select').value = state.selected || ''; enhanceSelect($('identity-select'));
      $('identities-page').textContent = `第 ${Math.floor(state.offset / 30) + 1} 页`;
      $('identities-status').textContent = state.items.length ? `本页 ${state.items.length} 位逻辑用户` : '没有匹配的逻辑用户；可尝试其他名称或账号。';
      renderUser();
    } catch (error) {
      if (request === state.request) { $('identities-status').textContent = '逻辑用户暂不可用，请刷新重试。'; notify(error.message, true); }
    } finally { if (request === state.request) { state.loading = false; controls(); } }
  }
  async function rename() {
    if (state.posting || !selected()) return;
    const epoch = ui.epoch();
    state.posting = true; controls();
    try {
      await ui.write('identities/rename', { space_id: ui.space(), logical_user_id: state.selected, display_name: $('identity-name').value.trim() });
      if (!ui.current(epoch)) return;
      notify('逻辑用户名称已保存。');
    } finally { state.posting = false; controls(); }
    await ui.refreshSpaces(); await refresh();
  }
  function renderPreview(preview) {
    const binding = preview.action === 'bind';
    const operation = binding ? (preview.previous_user ? '转移账号' : '添加账号') : '移除关联';
    $('identity-confirm-title').textContent = '确认' + operation + '？';
    $('identity-confirm-apply').textContent = '确认' + operation;
    const panel = $('identity-confirm-summary'); panel.replaceChildren();
    panel.append(element('p', `账号：${preview.account.account_id}`),
      element('p', '本次变更的消息入口：' + entries(preview.account)));
    if (binding) {
      panel.append(element('p', '当前归属：' + name(preview.previous_user)), element('p', '新的归属：' + name(preview.target_user)));
      if (preview.previous_private_spaces?.length) {
        panel.append(element('p', preview.delete_previous_user ? '将删除以下私聊空间' : '原逻辑用户保留以下私聊空间'));
        const spaces = element('ul');
        for (const space of preview.previous_private_spaces) {
          spaces.append(element('li', `${space.persona_id} · ${preview.previous_user.display_name || '未命名用户'}`));
        }
        panel.append(spaces);
      }
      if (preview.delete_previous_user) {
        const removal = element('div', '', 'identity-removal');
        removal.append(element('p', '原逻辑用户仅关联此账号，将删除该逻辑用户及其跨人格的私聊记忆，包括版本历史和笔记。此操作不可撤销。'));
        const list = element('ul');
        for (const [key, label] of Object.entries(countLabels)) list.append(element('li', `${label}：${preview.counts[key] || 0}`));
        removal.append(list); panel.append(removal);
      } else panel.append(element('p', preview.previous_user
        ? '原逻辑用户仍有其他关联账号，其全部既有记忆保留，不进行重构；仅更改此账号后续的归属，不将旧记忆合并到目标用户。'
        : '此账号之后使用新的逻辑用户归属。'));
    } else {
      panel.append(element('p', '解除关联的逻辑用户：' + name(preview.target_user)),
        element('p', '该账号将获得独立的逻辑用户。既有记忆继续归属原逻辑用户，不进行重构；后续该账号的会话不再为原逻辑用户提供记忆。'));
    }
    if (preview.raw_records_preserved) panel.append(element('p', '原始会话记录保留。', 'muted'));
    if (preview.shared_group_memories_preserved) panel.append(element('p', '群聊共享记忆保留。', 'muted'));
  }
  async function preview(action, account, trigger) {
    if (state.posting || !selected()) return;
    const epoch = ui.epoch();
    state.posting = true; controls(); state.trigger = trigger;
    const payload = { space_id: ui.space(), action, logical_user_id: state.selected, platform_instance_id: account.platform_instance_id, account_id: account.account_id };
    try {
      const result = await ui.write('identities/preview', payload);
      if (!ui.current(epoch)) return;
      state.pending = { ...payload, confirmation_token: result.confirmation_token, idempotency_key: newKey() };
      renderPreview(result); $('identity-confirm-error').hidden = true;
      $('identity-confirm-dialog').showModal();
    } finally { state.posting = false; controls(); }
  }
  async function apply() {
    if (state.posting || !state.pending || state.pending.space_id !== ui.space()) return;
    const epoch = ui.epoch();
    state.posting = true; controls(); $('identity-confirm-error').hidden = true;
    let job;
    try {
      job = await ui.write('identities/apply', state.pending);
      if (!ui.current(epoch)) return;
      state.job = job; state.pending = null; $('identity-confirm-dialog').close();
      notify('账号变更任务已提交，可在后台任务中查看进度。');
    } catch (error) {
      if (!ui.current(epoch)) return;
      $('identity-confirm-error').textContent = error.message || '提交未完成，请重试；归属发生变化时需取消后重新预览。';
      $('identity-confirm-error').hidden = false;
    } finally { state.posting = false; controls(); }
    if (job) { await refresh(); if (ui.current(epoch)) await ui.showJob(job); }
  }
  async function poll() {
    if (!state.job || state.polling) return;
    const epoch = ui.epoch();
    state.polling = true;
    try {
      const job = await ui.bridge.apiGet('task', { job_id: state.job.job_id });
      if (!ui.current(epoch)) return;
      if (['succeeded','failed','cancelled'].includes(job.state)) {
        state.job = null;
        if (job.state === 'succeeded') notify('账号归属已更新。');
        await ui.refreshSpaces(); await refresh();
      }
    } finally { state.polling = false; controls(); }
  }
  onSearchForm('identities-filter', () => { state.offset = 0; return refresh(); });
  on('identities-refresh', 'click', refresh);
  on('identities-prev', 'click', () => { state.offset = Math.max(0, state.offset - 30); return refresh(); });
  on('identities-next', 'click', () => { state.offset += 30; return refresh(); });
  on('identity-select', 'change', () => { state.selected = $('identity-select').value; renderUser(); });
  on('identity-known-account', 'change', () => {
    const value = $('identity-known-account').value, account = value === '' ? null : state.known[Number(value)];
    $('identity-platform').value = account?.platform_instance_id || ''; $('identity-account').value = account?.account_id || '';
  });
  on('identity-rename-form', 'submit', rename);
  on('identity-bind-form', 'submit', () => preview('bind', { platform_instance_id: $('identity-platform').value.trim(), account_id: $('identity-account').value.trim() }, $('identity-bind')));
  on('identity-confirm-apply', 'click', apply);
  on('identity-confirm-cancel', 'click', () => { if (!state.posting) $('identity-confirm-dialog').close(); });
  $('identity-confirm-dialog').addEventListener('cancel', event => { if (state.posting) event.preventDefault(); });
  $('identity-confirm-dialog').addEventListener('close', () => { state.pending = null; state.trigger?.focus({ preventScroll: true }); });
  function changed() {
    state.request += 1;
    state.items = []; state.conflicts = []; state.selected = null; state.offset = 0; state.hasMore = false;
    state.loading = false; state.pending = null; state.job = null; state.trigger = null;
    $('identity-confirm-dialog').close();
    $('identity-confirm-summary').replaceChildren();
    $('identity-confirm-error').hidden = true;
    $('identity-q').value = '';
    $('identity-select').replaceChildren(); enhanceSelect($('identity-select'));
    $('identities-page').textContent = '第 1 页';
    $('identities-status').textContent = ui.space() ? '正在读取逻辑用户…' : '请先选择记忆空间。';
    renderUser();
  }
  return { refresh, poll, changed };
}
