import { $, element, enhanceSelect, notify, on } from './ui.js';

const personaFields = [
  ['shared_recall_enabled', '启用同人格跨空间召回', '允许同一人格的其他空间使用已开放的长期记忆。'],
  ['allow_group_read_private', '允许群聊检索私聊记忆', '开启后，私聊中的原子、事件和主题可能用于该人格的群聊回答。'],
  ['external_limit', '外部记忆条数上限', '一次召回中，所有外部空间合计可补充的条数。', 10],
  ['external_budget_percent', '外部记忆预算占比（%）', '外部记忆可使用的共享注入预算上限。', 100],
];
const spaceFields = [
  ['exclude_from_shared_recall', '禁止其他空间检索本空间', '开启后，本空间仍可按人格设置检索其他空间。'],
  ['model_mode', '长期记忆模型', '继承插件设置时，全局记忆 LLM 留空将跟随来源窗口。'],
  ['provider_id', '指定模型', '用于本空间的长期记忆抽取。'],
  ['topic_provider_id', '主题整理模型', '留空时沿用本空间的模型选择。'],
];

export function initSpaceSettings(ui, modelChoices) {
  let saved, draft = {}, saving = false, request = 0;
  const changes = () => Object.fromEntries(Object.entries(draft).filter(([key, value]) => saved && value !== saved[key]));
  const dirty = () => Object.keys(changes()).length > 0;
  function controls() {
    const changed = dirty();
    $('policy-discard').hidden = !changed;
    $('policy-discard').disabled = saving;
    $('policy-save').disabled = saving || !changed;
    $('policy-draft-state').textContent = saving ? '正在保存…' : changed ? '有未保存的修改' : '没有未保存的改动';
  }
  function field([key, name, hint, max]) {
    const row = element('div', '', 'field'), label = element('label', name);
    const description = element('div', '', 'field-description'), help = element('small', hint);
    const select = ['model_mode', 'provider_id', 'topic_provider_id'].includes(key);
    const input = element(select ? 'select' : 'input');
    input.id = 'policy-' + key; label.htmlFor = input.id;
    help.id = input.id + '-help'; input.setAttribute('aria-describedby', help.id);
    if (select) {
      if (key === 'model_mode') {
        for (const [value, text] of [['inherit', '继承插件设置'], ['chat', '跟随对话模型'], ['fixed', '指定模型']]) {
          const option = element('option', text); option.value = value; input.append(option);
        }
      } else {
        input.append(...modelChoices('llm', draft[key]));
        input.firstChild.textContent = key === 'provider_id' ? '请选择模型' : '沿用空间模型';
        input.required = key === 'provider_id' && draft.model_mode === 'fixed';
      }
      input.value = draft[key]; enhanceSelect(input);
    } else if (max !== undefined) {
      input.type = 'number'; input.min = 0; input.max = max; input.step = 1; input.required = true;
      input.value = draft[key] ?? '';
    } else {
      input.type = 'checkbox'; input.className = 'toggle'; input.setAttribute('role', 'switch');
      input.checked = draft[key];
    }
    input.disabled = saving;
    row.hidden = key === 'provider_id' && draft.model_mode !== 'fixed';
    input.disabled ||= row.hidden;
    input.addEventListener('input', () => {
      draft[key] = input.type === 'checkbox' ? input.checked : input.type === 'number' ? (input.value === '' ? null : Number(input.value)) : input.value;
      if (key === 'model_mode') {
        const provider = $('policy-provider_id'), hidden = draft.model_mode !== 'fixed';
        provider.closest('.field').hidden = provider.disabled = hidden;
        provider.required = !hidden;
      }
      controls();
    });
    description.append(label, help); row.append(description, input);
    // Fixed mode requires a provider. Restore inheritance on the model mode row.
    if (key !== 'provider_id') {
      const defaultValue = saved.defaults[key];
      const reset = element('button', '恢复默认', 'reset-setting');
      reset.type = 'button';
      reset.setAttribute('aria-label', name + '恢复默认');
      const updateReset = () => { reset.disabled = input.disabled || draft[key] === defaultValue; };
      updateReset();
      input.addEventListener('input', updateReset);
      reset.addEventListener('click', () => {
        if (input.type === 'checkbox') input.checked = defaultValue;
        else input.value = defaultValue;
        input.dispatchEvent(new Event('input', { bubbles: true }));
      });
      row.append(reset);
    }
    return row;
  }
  function render() {
    $('policy-persona-title').textContent = '人格设置 · ' + saved.persona_id;
    $('policy-space-title').textContent = '当前空间 · ' + $('space').selectedOptions[0].textContent;
    const sources = saved.shared_spaces || [];
    $('policy-source-list').replaceChildren(...sources.map(space => element('li', space.display_label)));
    if (!sources.length) $('policy-source-list').append(element('li', '暂无已开放的来源空间。'));
    $('policy-persona-fields').replaceChildren(...personaFields.map(field));
    $('policy-space-fields').replaceChildren(...spaceFields.map(field));
    controls();
  }
  function discard() { if (saved) { draft = Object.fromEntries([...personaFields, ...spaceFields].map(([key]) => [key, saved[key]])); render(); } }
  async function refresh() {
    const id = ui.space(), sequence = ++request;
    if (saved?.space_id === id && (dirty() || saving)) return;
    saved = null; draft = {}; $('policy-form').hidden = true;
    $('policy-status').textContent = id ? '正在读取空间设置…' : '请选择记忆空间。';
    if (!id) return;
    try {
      const data = await ui.bridge.apiGet('space/policy', { space_id: id });
      if (sequence !== request || id !== ui.space()) return;
      saved = data; discard(); $('policy-form').hidden = false; $('policy-status').textContent = '';
    } catch {
      if (sequence === request && id === ui.space()) $('policy-status').textContent = '读取空间设置失败，请点击空间栏刷新按钮重试。';
    }
  }
  on('policy-discard', 'click', discard);
  on('policy-form', 'submit', async () => {
    if (!saved || saving || !dirty()) return;
    const id = saved.space_id;
    saving = true; render();
    try {
      const data = await ui.write('space/policy', { space_id: id, ...changes() });
      if (id === ui.space()) {
        saved = data; discard(); notify('空间设置已保存。');
        if (!data.shared_spaces) {
          try { saved = await ui.bridge.apiGet('space/policy', { space_id: id }); discard(); }
          catch { notify('已保存；请刷新查看可检索的空间。'); }
        }
      }
    } finally { saving = false; if (saved?.space_id === ui.space()) render(); }
  });
  return { refresh, dirty, discard, saving: () => saving };
}
