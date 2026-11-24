import { $, element, enhanceSelect, notify } from './ui.js';

const bases = { space: '来自空间设置', plugin: '来自插件设置', chat: '来自对话窗口', topic: '来自主题模型设置',
  short_term: '来自短期模型设置', session_config: '按会话配置选择', request_provider: '来自对话请求' };

function modelField(title, step, available, lane, mainField) {
  const block = element('div', '', 'task-model-field');
  const label = element('label'), select = element('select');
  const caption = element('span', title);
  caption.id = 'task-model-' + lane + '-label';
  select.setAttribute('aria-labelledby', caption.id);
  label.append(caption, select); block.append(label);
  const option = (value, name) => { const opt = element('option', name); opt.value = value; return opt; };
  select.append(option('', '请选择模型')); select.required = true;
  const follow = lane === 'window' && (step.mode === 'windows' || (step.missing_windows > 0 && step.providers?.length));
  let byWindow;
  if (follow) {
    byWindow = option('windows', '按各窗口的对话模型');
    select.append(byWindow);
  }
  function providers(input) {
    for (const item of available) input.append(option(item.provider_id, item.model ? item.provider_id + ' · ' + item.model : item.provider_id));
  }
  providers(select);
  if (byWindow) byWindow.selected = true;
  else select.value = available.some(item => item.provider_id === step.provider_id) ? step.provider_id : '';
  const canFollowMain = mainField && step.follows_main;
  if (canFollowMain) {
    select.value = '';
    const sync = () => {
      const resolved = step.mode === 'fixed' || Boolean(mainField.selection().window_provider_id);
      select.options[0].text = resolved ? '未指定：跟随主模型' : '请选择模型';
      select.required = !resolved;
    };
    mainField.select.addEventListener('change', sync);
    sync();
  }
  const hints = [];
  if (bases[step.basis]) hints.push(bases[step.basis]);
  if (follow && step.providers?.length) hints.push('涉及模型 ' + step.providers.join('、'));
  if (lane === 'topic' && step.mode === 'missing') {
    hints.push(canFollowMain ? '来源缺少统一模型时，可指定本次主模型或主题抽取模型。' : '请为本次主题抽取选择模型。');
  } else if (step.missing_windows > 0) hints.push(step.missing_windows + ' 个窗口需要选择模型');
  if (hints.length) block.append(element('small', hints.join(' · '), 'muted'));
  enhanceSelect(select);
  let fallback;
  const following = () => byWindow && select.selectedOptions[0] === byWindow;
  if (follow && step.missing_windows > 0) {
    const missing = element('label'); fallback = element('select');
    fallback.append(option('', '请选择模型')); providers(fallback); enhanceSelect(fallback);
    missing.append(element('span', '仅用于缺少模型的窗口'), fallback); block.append(missing);
    const sync = () => {
      missing.hidden = !following(); fallback.disabled = missing.hidden; fallback.required = !missing.hidden;
    };
    select.addEventListener('change', sync); sync();
  }
  return { block, select, selection() {
    if (canFollowMain && !select.value) return {};
    if (following()) return fallback ? { missing_window_provider_id: fallback.value } : {};
    if (!canFollowMain && select.value === step.provider_id && step.mode === 'fixed') return {};
    return { [lane + '_provider_id']: select.value };
  } };
}

export function initTaskModels(ui) {
  let request = 0;
  return async function confirmTask({ kind, space_id, sections, job_id }, title, description, confirm) {
    const dialog = $('section-rebuild-dialog');
    if (dialog.open) return null;
    const id = ++request, epoch = ui.epoch();
    const body = job_id ? { job_id } : { kind, space_id, ...(sections && { sections }) };
    const preview = await ui.bridge.apiPost('tasks/model-preview', body);
    if (id !== request || !ui.current(epoch) || ui.page() !== 'maintenance') return null;
    if (preview.waiting_message) { notify(preview.waiting_message); return null; }
    const [models, impact] = await Promise.all([
      ui.bridge.apiGet('models'),
      sections?.includes('topics') || kind === 'topic_rebuild' ? ui.bridge.apiGet('topics/rebuild-preview', { space_id }) : null,
    ]);
    if (id !== request || !ui.current(epoch) || ui.page() !== 'maintenance') return null;
    const fields = [];
    if (preview.window) {
      const caption = kind === 'short_term' ? '短期摘要模型' : '主模型';
      fields.push(modelField(caption, preview.window, models.llm, 'window'));
    }
    if (preview.topic) fields.push(modelField('主题抽取模型', preview.topic, models.llm, 'topic', fields[0]));
    const selected = Array.from($('space').options).find(item => item.value === space_id);
    $('section-rebuild-scope').textContent = selected ? '记忆空间 · ' + selected.textContent : '任务 ' + job_id;
    $('section-rebuild-title').textContent = title;
    if (impact) {
      const { eligible_members, protected_members, old_topics } = impact;
      description += `当前可用成员 ${eligible_members} 个，受保护成员 ${protected_members} 个，将替换现有主题 ${old_topics} 个（含人工保护的主题）。`;
      if (eligible_members < 2 && old_topics > 0) description += `可用成员不足两个，本次将清空现有的 ${old_topics} 个主题。`;
    }
    $('section-rebuild-description').textContent = description;
    $('task-model-fields').replaceChildren(...fields.map(field => field.block));
    $('section-rebuild-confirm').textContent = confirm;
    dialog.returnValue = 'cancel';
    const closed = new Promise(resolve => dialog.addEventListener('close', resolve, { once: true }));
    dialog.showModal();
    await closed;
    if (dialog.returnValue !== 'rebuild' || !ui.current(epoch) || ui.page() !== 'maintenance') return null;
    return Object.assign({}, ...fields.map(field => field.selection()));
  };
}
