import { button, date, element, enhanceSelect, labels, setupTabs, shortLogicalUserId } from './ui.js';

const kinds = {
  profiles: { entity: 'profile', key: 'profile_id', name: '用户画像', fields: [['description', '画像正文'], ['tags', '标签（每行一个）']] },
  relationships: { entity: 'relationship', key: 'relationship_id', name: '人格关系', fields: [['long_term', '长期理解'], ['aftereffect', '近期余韵']] },
  short_term: { entity: 'short_term', key: 'state_id', name: '短期记忆', fields: [['summary_text', '摘要正文']] }
};

function valuesOf(record, display = false) {
  const value = key => display ? record['display_' + key] ?? record[key] : record[key];
  const state = value('state');
  return {
    description: value('description'), tags: (value('tags') || []).join('\n'),
    long_term: state?.format === 'relationship.slots.v1' ? state.long_term.body : value('state_text'),
    aftereffect: state?.aftereffect?.body || '', summary_text: value('summary_text')
  };
}

function recordCard(record, kind, submit) {
  const config = kinds[kind], card = element('article', '', 'card');
  const values = valuesOf(record), display = valuesOf(record, true);
  const name = record.logical_user_name || (record.conversation_id?.startsWith('host.v1:') ? '' : record.conversation_id) || config.name;
  card.append(element('h3', name),
    element('p', [labels[record.status], 'v' + record.content_version, date(record.updated_at)].filter(Boolean).join(' · '), 'muted'));

  const preview = element('div'), form = element('form'), actions = element('div', '', 'row');
  const controls = {};
  for (const [key, label] of config.fields) {
    if (kind === 'relationships') preview.append(element('h4', label));
    if (key === 'tags') {
      if (display.tags) preview.append(element('p', '标签：' + display.tags.split('\n').join('、'), 'muted'));
    } else preview.append(element('p', display[key] || '暂无内容。', 'body'));
    const field = element('label'), input = element('textarea');
    input.value = values[key] || '';
    field.append(element('span', label), input); form.append(field); controls[key] = input;
  }
  function change(action, patch) {
    const operation = { entity_kind: config.entity, entity_id: record[config.key], action,
      expected_generation_id: record.generation_id, expected_version: record.content_version };
    if (patch) operation.patch = patch;
    return submit(operation);
  }
  const save = element('button', '提交修改', 'primary');
  save.type = 'submit'; save.disabled = true;
  form.addEventListener('input', () => {
    save.disabled = !Object.entries(controls).some(([key, input]) => input.value !== (values[key] || ''));
  });
  const edit = button('编辑', () => {
    form.hidden = false; preview.hidden = actions.hidden = true;
    Object.values(controls)[0].focus();
  });
  const cancel = button('取消编辑', () => {
    for (const [key, input] of Object.entries(controls)) input.value = values[key] || '';
    save.disabled = form.hidden = true; preview.hidden = actions.hidden = false;
    edit.focus();
  });
  form.addEventListener('submit', async event => {
    event.preventDefault();
    if (save.disabled) return;
    const patch = Object.fromEntries(Object.entries(controls).map(([key, input]) => [key, input.value.trim()]));
    if ('tags' in patch) patch.tags = patch.tags.split('\n').map(tag => tag.trim()).filter(Boolean);
    save.disabled = true;
    try { await change('edit', patch); } finally { save.disabled = false; }
  });
  const formActions = element('div', '', 'row'); formActions.append(save, cancel);
  form.hidden = true; form.append(formActions);
  const confirmation = element('div', '', 'notice');
  const remove = button('删除', () => {
    confirmation.hidden = false; actions.hidden = true; keep.focus();
  }, 'danger');
  const keep = button('取消', () => {
    confirmation.hidden = true; actions.hidden = false; remove.focus();
  });
  const confirmed = button('确认删除', () => change('delete'), 'danger');
  const confirmActions = element('div', '', 'row'); confirmActions.append(keep, confirmed);
  confirmation.hidden = true;
  const deletionEffect = kind === 'short_term'
    ? '删除摘要后保留已处理进度，后续摘要从新消息继续。'
    : '当前内容和历史版本会一并删除，原始消息保留。';
  confirmation.append(element('p', '确认删除这条' + config.name + '？' + deletionEffect), confirmActions);
  actions.append(edit, remove);
  const editor = element('fieldset', '', 'context-governance-editor');
  editor.setAttribute('aria-label', config.name + '操作');
  editor.append(actions, form, confirmation);
  card.append(preview, editor);
  return card;
}

export function initContextGovernance(ui, submit) {
  const sections = [...document.querySelectorAll('[data-context-governance]')];
  const tabs = [...document.querySelectorAll('[data-governance-tab]')];
  const profileUser = document.getElementById('governance-profile-user');
  profileUser.addEventListener('change', () => load(profileUser.closest('[role="tabpanel"]'), 0, false));
  const offsets = new Map(), requests = new Map(), limit = 10;
  let available = false;
  function setAvailable(value) {
    available = value;
    for (const editor of document.querySelectorAll('.context-governance-editor')) editor.disabled = !available;
  }
  const visible = () => ui.page() === 'maintenance' && document.querySelector('[data-maintenance-tab="manual"]').getAttribute('aria-selected') === 'true';
  const selectTab = setupTabs('[data-governance-tab]', (tab, loadContent = true) => {
    const visibleTabs = tabs.filter(item => !item.hidden);
    tab.parentElement.style.setProperty('--segments', visibleTabs.length);
    tab.parentElement.style.setProperty('--active-segment', visibleTabs.indexOf(tab));
    for (const item of tabs) document.getElementById(item.getAttribute('aria-controls')).hidden = item !== tab;
    const pane = document.getElementById(tab.getAttribute('aria-controls'));
    if (loadContent && pane.dataset.contextGovernance && !pane.querySelector('.context-records').childElementCount) refresh();
  });
  function changed() {
    offsets.clear(); requests.clear();
    profileUser.replaceChildren(); profileUser.disabled = true;
    for (const section of sections) section.querySelector('.context-records').replaceChildren();
    const relationship = tabs.find(tab => tab.dataset.governanceTab === 'relationships');
    relationship.hidden = ui.spaceType() !== 'private';
    selectTab(tabs.find(tab => !tab.hidden && tab.getAttribute('aria-selected') === 'true') || tabs[0], false);
  }
  async function load(section, offset, reloadUsers = true) {
    const kind = section.dataset.contextGovernance, list = section.querySelector('.context-records');
    const epoch = ui.epoch(), space = ui.space(), request = (requests.get(kind) || 0) + 1;
    requests.set(kind, request);
    list.replaceChildren(element('p', space ? '正在读取…' : '请先选择记忆空间。', 'muted'));
    if (!space) return;
    try {
      const args = { kind, space_id: space, offset, limit };
      if (kind === 'profiles') {
        if (reloadUsers) {
          profileUser.disabled = true;
          const { items } = await ui.bridge.apiGet('memory/profile-users', { space_id: space });
          if (!ui.current(epoch) || requests.get(kind) !== request) return;
          const previous = profileUser.value, names = new Map();
          for (const user of items) names.set(user.display_name, (names.get(user.display_name) || 0) + 1);
          profileUser.replaceChildren(...items.map(user => {
            const name = names.get(user.display_name) > 1 ? user.display_name + ' · ' + shortLogicalUserId(user.logical_user_id) : user.display_name;
            return new Option(name, user.logical_user_id);
          }));
          if (items.some(user => user.logical_user_id === previous)) profileUser.value = previous;
          profileUser.disabled = !items.length;
          enhanceSelect(profileUser);
        }
        if (!profileUser.value) {
          list.replaceChildren(element('p', '当前空间暂无用户画像。', 'muted')); return;
        }
        args.logical_user_id = profileUser.value;
      }
      const result = await ui.bridge.apiGet('memory/context', args);
      if (!ui.current(epoch) || requests.get(kind) !== request) return;
      if (!result.items.length && offset) return load(section, Math.max(0, offset - limit));
      offsets.set(kind, offset); list.replaceChildren();
      for (const record of result.items) list.append(recordCard(record, kind, submit));
      if (!result.items.length) list.append(element('p', '当前空间暂无' + kinds[kind].name + '。', 'muted'));
      if (offset || result.has_more) {
        const nav = element('nav', '', 'row');
        nav.setAttribute('aria-label', kinds[kind].name + '治理分页');
        const prev = button('上一页', () => load(section, offset - limit)), next = button('下一页', () => load(section, offset + limit));
        prev.disabled = !offset; next.disabled = !result.has_more;
        nav.append(prev, element('span', '第 ' + (offset / limit + 1) + ' 页', 'muted'), next); list.append(nav);
      }
      setAvailable(available);
    } catch {
      if (!ui.current(epoch) || requests.get(kind) !== request) return;
      list.replaceChildren(element('p', '读取失败。', 'error'), button('重新读取', () => load(section, offset)));
    }
  }
  function refresh() {
    const section = sections.find(item => !item.hidden);
    if (section && visible()) return load(section, offsets.get(section.dataset.contextGovernance) || 0);
  }
  return { refresh, changed, setAvailable };
}
