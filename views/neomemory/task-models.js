import { $, element, enhanceSelect } from './ui.js';

const bases = { space: '来自空间设置', plugin: '来自插件设置', chat: '来自对话窗口', topic: '来自主题模型设置',
  short_term: '来自短期模型设置', session_config: '按会话配置选择', request_provider: '来自对话请求' };

function modelField(title, step, available, lane, { optionalTopic = false } = {}) {
  const block = element(optionalTopic ? 'details' : 'div', '', 'task-model-field');
  const label = element('label'), select = element('select');
  label.append(element('span', title), select); block.append(label);
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
  if (optionalTopic) {
    const summary = element('summary');
    block.prepend(summary);
    const showChoice = () => { summary.textContent = '主题整理模型 · ' + (select.value || '请选择模型'); };
    select.addEventListener('change', showChoice);
    select.addEventListener('invalid', () => { block.open = true; });
    block.open = !select.value;
    showChoice();
  }
  const hints = [];
  if (bases[step.basis]) hints.push(bases[step.basis]);
  if (follow && step.providers?.length) hints.push('涉及模型 ' + step.providers.join('、'));
  if (lane === 'topic' && step.mode === 'missing') {
    hints.push('无法为这些主题来源确定一个可用模型，请为本次主题整理选择模型。');
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
  return { block, selection() {
    if (following()) return fallback ? { missing_window_provider_id: fallback.value } : {};
    if (select.value === step.provider_id && step.mode === 'fixed') return {};
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
    const [preview, models, impact] = await Promise.all([
      ui.bridge.apiPost('tasks/model-preview', body), ui.bridge.apiGet('models'),
      sections?.includes('topics') || kind === 'topic_rebuild' ? ui.bridge.apiGet('topics/rebuild-preview', { space_id }) : null,
    ]);
    if (id !== request || !ui.current(epoch) || ui.page() !== 'maintenance') return null;
    const fields = [];
    for (const [lane, caption] of [['window', kind === 'short_term' ? '短期摘要模型' : '记忆 LLM 模型（窗口抽取）'], ['topic', '主题整理模型']]) {
      if (!preview[lane]) continue;
      const optionalTopic = lane === 'topic' && preview.window && preview.topic.mode === 'fixed';
      const field = modelField(caption, preview[lane], models.llm, lane, { optionalTopic });
      if (lane === 'topic' && preview.window) {
        field.block.append(element('small', '本次更改窗口抽取模型时，主题整理仍使用此处所选的模型。', 'muted'));
      }
      fields.push(field);
    }
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
