import { initSpaceSettings } from './space-settings.js';
import { initTextCleaning } from './text-cleaning.js';
import { initAppearance } from './appearance.js';
import { $, element, button, notify, on, setupTabs, enhanceSelect } from './ui.js';

export function initSettings(ui) {
  const state = { data: null, models: { llm: [], embedding: [] }, draft: {}, group: '功能', loading: null, posting: false, probing: false };
  const providerFields = new Set(['llm_provider_id', 'short_term_provider_id', 'embedding_provider_id', 'rerank_provider_id']);
  const modelFields = new Set(['llm_provider_id','short_term_provider_id','embedding_provider_id','embedding_revision','llm_concurrency','embedding_concurrency','embedding_batch_size']);
  const appearance = initAppearance(ui, () => { if (state.data) renderDraftStatus(); });
  const cleaning = initTextCleaning(ui);
  function isModelField(key, meta) {
    return meta.group === 'model' || modelFields.has(key);
  }
  function pageEntries(page = 'settings') {
    return Object.entries(state.data.schema).filter(([key, meta]) => isModelField(key, meta) === (page === 'models'));
  }
  function choices(kind, selected = '', blank = true) {
    const items = []; if (blank) { const option = element('option', '未指定 / 使用默认行为'); option.value = ''; items.push(option); }
    for (const item of state.models[kind] || []) { const option = element('option', `${item.provider_id} · ${item.model || '未知模型'}`); option.value = item.provider_id; items.push(option); }
    if (selected && !items.some((option) => option.value === selected)) { const option = element('option', selected + '（当前不可用）'); option.value = selected; items.push(option); }
    return items;
  }
  const policy = initSpaceSettings(ui, choices);
  function field(key, meta) {
    const row = element('div', '', 'field'), id = 'setting-' + key;
    const label = element('label', meta.description || key); label.htmlFor = id;
    const description = element('div', '', 'field-description');
    const help = element('div'); help.id = id + '-help';
    help.append(element('small', meta.hint || ''), element('small', `${key} · 默认：${JSON.stringify(meta.default)}`));
    description.append(label, help);
    if (state.data.runtime_values && state.data.runtime_values[key] !== state.data.values[key]) help.append(element('small', '运行值：' + JSON.stringify(state.data.runtime_values[key]) + '（保存值尚未应用）'));
    let input;
    if (providerFields.has(key)) { input = element('select'); input.append(...choices(key === 'embedding_provider_id' ? 'embedding' : key === 'rerank_provider_id' ? 'rerank' : 'llm', state.draft[key])); input.value = state.draft[key] ?? ''; }
    else if (meta.options) {
      input = element('select');
      for (const value of meta.options) { const option = element('option', meta.option_labels?.[value] || String(value)); option.value = value; input.append(option); }
      input.value = state.draft[key] ?? ''; input.required = !meta.options.includes('');
    }
    else {
      input = element('input'); input.type = meta.type === 'bool' ? 'checkbox' : ['int','float'].includes(meta.type) ? 'number' : 'text';
      if (input.type === 'checkbox') { input.checked = Boolean(state.draft[key]); input.className = 'toggle'; input.setAttribute('role', 'switch'); } else input.value = state.draft[key] ?? '';
      // Slider increments are UI hints, not divisibility constraints on numeric values.
      if (input.type === 'number') { input.step = meta.type === 'int' ? 1 : 'any'; if (meta.slider?.min !== undefined) input.min = meta.slider.min; if (meta.slider?.max !== undefined) input.max = meta.slider.max; }
    }
    input.id = id; input.setAttribute('aria-describedby', help.id); input.dataset.key = key; input.disabled = !state.data.can_save || state.posting;
    if (input.tagName === 'SELECT') enhanceSelect(input);
    const reset = providerFields.has(key) ? null : button('恢复默认', () => {
      state.draft[key] = meta.default;
      if (key === ($('probe-kind').value === 'embedding' ? 'embedding_provider_id' : 'llm_provider_id')) renderProbe(true);
      render();
    }, 'reset-setting');
    reset?.setAttribute('aria-label', (meta.description || key) + '恢复默认');
    const updateReset = () => { if (reset) reset.disabled = input.disabled || state.draft[key] === meta.default; };
    updateReset();
    input.addEventListener('input', () => {
      state.draft[key] = input.type === 'checkbox' ? input.checked : ['int','float'].includes(meta.type) ? (input.value === '' ? null : Number(input.value)) : input.value;
      updateReset();
      if (key === ($('probe-kind').value === 'embedding' ? 'embedding_provider_id' : 'llm_provider_id')) renderProbe(true);
      renderDraftStatus();
    });
    row.append(description, input);
    if (reset) row.append(reset);
    return row;
  }
  function changedEntries(page) {
    return pageEntries(page).filter(([key]) => state.draft[key] !== state.data.values[key]);
  }
  function dirtyCount(page) {
    return changedEntries(page).length + Number(page === 'settings' && appearance.isDirty());
  }
  function discard(page) {
    if (page === 'settings') appearance.discard();
    for (const [key] of pageEntries(page)) state.draft[key] = state.data.values[key];
    if (page === 'models') renderProbe(true);
    render();
  }
  async function canLeave(page) {
    if (!state.data || !['settings', 'models'].includes(page)) return true;
    if (state.posting || policy.saving()) { notify('正在提交，请稍后切换页面。'); return false; }
    if (!dirtyCount(page) && !(page === 'settings' && policy.dirty())) return true;
    if (!await confirmDiscard()) return false;
    if (page === 'settings') policy.discard();
    discard(page);
    return true;
  }
  async function canChangeSpace() {
    if (policy.saving()) { notify('正在保存空间设置。'); return false; }
    if (!policy.dirty()) return true;
    if (!await confirmDiscard(true)) return false;
    policy.discard(); return true;
  }
  async function confirmDiscard(spaceOnly = false) {
    const dialog = $('settings-leave');
    if (dialog.open) return false;
    $('settings-leave-description').textContent = spaceOnly ? '切换空间会放弃当前空间尚未保存的修改。' : '离开会放弃本页尚未保存的修改。';
    dialog.returnValue = 'stay';
    const closed = new Promise(resolve => dialog.addEventListener('close', resolve, { once: true }));
    dialog.showModal();
    await closed;
    return dialog.returnValue === 'discard';
  }
  window.addEventListener('beforeunload', event => {
    if (policy.dirty() || (state.data && ['settings', 'models'].some(page => dirtyCount(page)))) {
      event.preventDefault();
      event.returnValue = '';
    }
  });
  function renderDraftStatus() {
    const readOnly = !state.data.can_save || state.posting;
    $('settings-reset-all').disabled = readOnly || (appearance.isDefault() && pageEntries().every(([key, meta]) => state.draft[key] === meta.default));
    for (const page of ['settings', 'models']) {
      const changed = dirtyCount(page);
      $(page + '-save').disabled = readOnly || changed === 0;
      if (page === 'settings') {
        $('settings-discard').hidden = changed === 0;
        $('settings-discard').disabled = state.posting;
      }
      const id = page + '-draft-state';
      $(id).textContent = state.posting ? '正在提交…' : changed ? `共 ${changed} 项配置待保存` : state.data.applied === false ? '保存的配置尚未应用' : '没有未保存的改动';
      $(id).dataset.dirty = String(changed > 0);
    }
  }
  function render() {
    if (!state.data) return;
    const query = $('settings-search').value.trim().toLowerCase();
    const settings = [], models = [];
    for (const [key, meta] of Object.entries(state.data.schema)) {
      if (isModelField(key, meta)) models.push(field(key, meta));
      else if (meta.group === state.group && (!query || `${key} ${meta.description} ${meta.hint}`.toLowerCase().includes(query))) settings.push(field(key, meta));
    }
    const showAppearance = state.group === '外观';
    const showCleaning = state.group === '文本清理', showPolicy = state.group === '空间';
    $('space-settings').hidden = !showPolicy;
    if (ui.page() === 'settings') ui.showSpaceBar(showPolicy);
    $('cleaning-panel').hidden = !showCleaning;
    $('settings-draft-state').closest('.save-bar').hidden = $('settings-result').hidden = showCleaning || showPolicy;
    $('appearance-panel').hidden = !showAppearance;
    $('appearance-panel').disabled = state.posting || !state.data.can_save;
    $('settings-search-label').hidden = $('settings-fields').hidden = showAppearance || showCleaning || showPolicy;
    $('settings-fields').replaceChildren(...settings); $('model-fields').replaceChildren(...models);
    if (!settings.length) $('settings-fields').append(element('p', '没有符合当前条件的设置。', 'muted'));
    const pending = state.data.applied === false && !state.data.external_change;
    const pendingMessage = state.data.apply_error ? '配置已保存但尚未完整应用，请查看运行值并重试应用。' : '已保存配置尚未应用，可重试应用。';
    $('settings-reload').hidden = !state.data.external_change && !pending;
    $('settings-reload').textContent = state.data.external_change ? '宿主配置已在其他位置修改，请在 AstrBot 重载插件后继续编辑；仅刷新本页无法更新宿主配置对象。' : pendingMessage;
    for (const id of ['settings-apply', 'models-apply']) { $(id).hidden = !pending || !state.data.can_save; $(id).disabled = state.posting; }
    $('models-state').hidden = state.data.can_save && !state.data.external_change && !pending;
    $('models-state').textContent = state.data.external_change ? '宿主配置已在其他位置修改，请在宿主重载插件后继续编辑。' : !state.data.can_save ? '当前宿主未提供配置保存接口，只能查看模型设置。' : pendingMessage;
    if (!state.data.can_save) $('settings-result').textContent = state.data.external_change ? '宿主配置已在其他位置修改，请在宿主重载插件后继续编辑。' : '当前宿主未提供配置保存接口，只能查看设置。';
    renderProbe();
    renderDraftStatus();
  }
  function renderProbe(followConfigured = false) {
    const kind = $('probe-kind').value;
    const selected = followConfigured ? state.draft[kind === 'embedding' ? 'embedding_provider_id' : 'llm_provider_id'] || '' : $('probe-provider').value;
    const available = state.models[kind] || [];
    $('probe-provider').replaceChildren(...choices(kind, selected));
    $('probe-provider').firstChild.textContent = available.length ? '请选择测试模型' : '没有可用模型，请先在 AstrBot 配置';
    $('probe-provider').value = selected;
    enhanceSelect($('probe-provider'));
    $('probe-start').disabled = state.posting || !available.some((item) => item.provider_id === selected) || state.probing;
  }
  async function refresh(force = false) {
    if (state.data && !force) {
      render();
      if (ui.page() === 'settings' && state.group === '空间') await policy.refresh();
      if (ui.page() === 'settings' && state.group === '文本清理') await cleaning.refresh();
      return;
    }
    if (state.loading) return state.loading;
    state.loading = (async () => {
      const results = await Promise.allSettled([ui.bridge.apiGet('settings'), ui.bridge.apiGet('models')]);
      if (results[0].status === 'fulfilled') { state.data = results[0].value; state.draft = { ...state.data.values }; }
      else throw new Error('设置读取失败，请稍后重新读取。');
      if (results[1].status === 'fulfilled') state.models = results[1].value;
      else { state.models = { llm: [], embedding: [] }; notify('Provider 列表暂不可用，已保存的模型仍保留。', true); }
      if (force) await appearance.load();
      renderProbe(true); render();
      if (ui.page() === 'settings' && state.group === '空间') await policy.refresh();
      if (ui.page() === 'settings' && state.group === '文本清理') await cleaning.refresh();
    })();
    try { await state.loading; } finally { state.loading = null; }
  }
  async function save(page, retry = false) {
    if (state.posting || !state.data?.can_save) return;
    if (!retry) for (const input of $('page-' + page).querySelectorAll('[data-key], #appearance-hex')) if (!input.checkValidity()) { input.reportValidity(); throw new Error('请检查参数范围。'); }
    const changes = retry ? {} : Object.fromEntries(changedEntries(page).map(([key]) => [key, state.draft[key]]));
    const saveAppearance = !retry && page === 'settings' && appearance.isDirty();
    if (!saveAppearance && !Object.keys(changes).length && state.data.applied !== false) { notify('没有待保存的改动。'); return; }
    state.posting = true; render();
    try {
      if (saveAppearance) await appearance.save();
      const result = Object.keys(changes).length || retry
        ? await ui.write('settings', { revision: state.data.revision, changes }) : state.data;
      for (const key of Object.keys(result.values)) {
        if (state.draft[key] === state.data.values[key] || Object.hasOwn(changes, key)) state.draft[key] = result.values[key];
      }
      state.data = result;
      $('settings-result').textContent = result.external_change ? '宿主配置已在其他位置修改，请在 AstrBot 重载插件后继续编辑。' : result.applied ? '配置已保存并生效，无需重载插件。' : '配置已保存但尚未完整应用，请查看运行值并重试应用。';
      notify($('settings-result').textContent, Boolean(result.external_change) || !result.applied);
    } catch (error) {
      let latest = null;
      try { latest = await ui.bridge.apiGet('settings'); } catch { /* Keep the editable draft when the service is unavailable. */ }
      if (latest?.apply_error && !latest.external_change) {
        state.data = latest;
        $('settings-result').textContent = '配置已保存但尚未完整应用，请查看运行值并点击“重试应用已保存配置”；当前草稿仍保留。';
      } else $('settings-result').textContent = (error.message || '保存失败。') + ' 若配置已被其他页面修改，请重新读取后再编辑；当前草稿仍保留。';
      notify($('settings-result').textContent, true);
    }
    finally { state.posting = false; render(); }
  }
  async function probe() {
    const provider = $('probe-provider').value;
    if (!provider || state.probing) return;
    state.probing = true;
    renderProbe();
    $('probe-result').textContent = provider + ' · 正在测试…';
    try {
      const result = await ui.bridge.apiPost('models/probe', { kind: $('probe-kind').value, provider_id: provider });
      const fields = ['连接成功', result.provider_id, result.model, result.elapsed_ms + ' ms'];
      if (result.dimension != null) fields.push(result.dimension + ' 维');
      $('probe-result').textContent = fields.join(' · ');
    } catch (error) {
      $('probe-result').textContent = provider + ' · ' + (error.message || '测试失败，请稍后重试。');
    } finally {
      state.probing = false;
      renderProbe();
    }
  }
  setupTabs('[data-settings-group]', tab => {
    state.group = tab.dataset.settingsGroup;
    render();
    if (state.group === '空间') policy.refresh();
    if (state.group === '文本清理') cleaning.refresh();
  });
  on('settings-search', 'input', render); on('settings-save', 'click', () => save('settings')); on('models-save', 'click', () => save('models'));
  on('settings-discard', 'click', () => discard('settings'));
  on('settings-reset-all', 'click', () => {
    for (const [key, meta] of pageEntries()) state.draft[key] = meta.default;
    appearance.reset();
    render();
  });
  on('settings-apply', 'click', () => save('settings', true)); on('models-apply', 'click', () => save('models', true));
  on('settings-refresh', 'click', () => refresh(true)); on('models-refresh', 'click', async () => { state.models = await ui.bridge.apiGet('models'); render(); });
  on('probe-kind', 'change', () => renderProbe(true)); on('probe-provider', 'change', () => renderProbe()); on('probe-start', 'click', probe);
  return { refresh, canLeave, canChangeSpace, loadAppearance: appearance.load };
}
