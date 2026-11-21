import { $, button, element, enhanceSelect, newKey, notify, on } from './ui.js';

import { rootOrder, editedRootOrder, installCleaningDrag } from './cleaning-order.js';

const kindLabels = { literal: '固定文本', regex: '正则表达式' };

export function initTextCleaning(ui) {
  const dialog = $('cleaning-dialog'), list = $('cleaning-list');
  const expandedGroups = new Set();
  let config = null, editing = null, saving = false, readNumber = 0;

  function lock() {
    $('add-cleaning-group').disabled = $('add-cleaning-rule').disabled = !config || saving;
    list.inert = saving;
    $('cleaning-apply').disabled = $('cleaning-cancel').disabled = saving;
  }

  async function save(next) {
    saving = true;
    ++readNumber;
    lock();
    try {
      config = await ui.write('text-cleaning', next);
      render();
    } finally {
      saving = false;
      lock();
    }
  }

  function row(collection, item, detail, groupEnabled = true, label = element('div')) {
    const node = element('div', '', 'cleaning-row');
    node.dataset.id = item.id;
    node.dataset.collection = collection;
    node.dataset.effective = String(groupEnabled && item.enabled);
    label.classList.add('cleaning-label');
    label.append(element('strong', item.name), element('small', detail));
    const edit = button('编辑', () => open(collection, item));
    const remove = button('删除', () => open(collection, item, true), 'danger');
    const toggle = element('input', '', 'toggle');
    toggle.type = 'checkbox';
    toggle.checked = item.enabled;
    toggle.setAttribute('role', 'switch');
    toggle.setAttribute('aria-label', '启用' + (collection === 'groups' ? '分组' : '条目') + ' ' + item.name);
    toggle.addEventListener('change', async () => {
      const enabled = toggle.checked;
      try {
        await save({ ...config, [collection]: config[collection].map(
          entry => entry.id === item.id ? { ...entry, enabled } : entry) });
        const updated = [...list.querySelectorAll('.cleaning-row')].find(
          row => row.dataset.id === item.id && row.dataset.collection === collection);
        updated?.querySelector('.toggle').focus();
      } catch (error) {
        toggle.checked = item.enabled;
        notify(error.message || '开关保存失败，请刷新后重试。', true);
      }
    });
    const grip = element('span', '⠿', 'cleaning-grip');
    grip.title = '拖动排序';
    grip.setAttribute('aria-hidden', 'true');
    node.append(grip, label, edit, remove, toggle);
    return node;
  }

  function render() {
    list.replaceChildren();
    const ruleNodes = new Map(config.rules.map((rule, index) => {
      const group = config.groups.find(entry => entry.id === rule.group_id);
      let detail = '执行顺序 ' + (index + 1) + ' · ' + kindLabels[rule.kind];
      if (group && !group.enabled) detail += ' · 随分组暂停';
      return [rule.id, row('rules', rule, detail, group?.enabled)];
    }));
    const groupNodes = new Map(config.groups.map(group => {
      const block = element('div', '', 'cleaning-group');
      const members = element('div', '', 'cleaning-members');
      for (const rule of config.rules.filter(rule => rule.group_id === group.id)) members.append(ruleNodes.get(rule.id));
      const disclosure = element('button', '', 'cleaning-disclosure');
      disclosure.type = 'button';
      disclosure.setAttribute('aria-label', group.name);
      disclosure.addEventListener('click', () => {
        if (members.hidden) expandedGroups.add(group.id);
        else expandedGroups.delete(group.id);
        updateDisclosure();
      });
      function updateDisclosure() {
        members.hidden = !expandedGroups.has(group.id);
        disclosure.setAttribute('aria-expanded', String(!members.hidden));
      }
      members.id = 'cleaning-members-' + group.id;
      disclosure.setAttribute('aria-controls', members.id);
      updateDisclosure();
      const header = row('groups', group, group.enabled ? '分组' : '分组已关闭', true, disclosure);
      block.append(header, members);
      return [group.id, block];
    }));
    for (const ref of rootOrder(config)) list.append((ref.kind === 'group' ? groupNodes : ruleNodes).get(ref.id));
    $('cleaning-status').textContent = list.childElementCount ? '' : '尚无清理条目或分组。';
  }

  installCleaningDrag(list, $('cleaning-drop-end'), async () => {
    const order = [], groups = [], rules = [];
    const byId = new Map(config.rules.map(rule => [rule.id, rule]));
    function appendRule(row, groupId) { rules.push({...byId.get(row.dataset.id), group_id: groupId}); }
    for (const block of list.children) {
      if (block.classList.contains('cleaning-group')) {
        const id = block.firstElementChild.dataset.id;
        order.push({kind: 'group', id});
        groups.push(config.groups.find(group => group.id === id));
        for (const row of block.querySelector('.cleaning-members').children) appendRule(row, id);
      } else {
        order.push({kind: 'rule', id: block.dataset.id});
        appendRule(block, null);
      }
    }
    try {
      await save({...config, rules, groups, root_order: order});
    } catch (error) {
      render();
      notify(error.message || '排序保存失败，请刷新后重试。', true);
    }
  });

  async function refresh() {
    if (saving || dialog.open) return;
    const read = ++readNumber;
    try {
      const loaded = await ui.bridge.apiGet('text-cleaning');
      if (read !== readNumber) return;
      config = loaded;
      render();
    } catch (error) {
      if (read !== readNumber) return;
      config = null;
      list.replaceChildren();
      $('cleaning-status').textContent = error.message || '读取清理规则失败，请刷新重试。';
    } finally { lock(); }
  }

  function updateKind() {
    const kind = $('cleaning-kind').value;
    $('cleaning-pattern').required = editing?.collection === 'rules' && !editing.removing;
    $('cleaning-regex-hint').hidden = kind !== 'regex';
  }

  function open(collection, item = null, removing = false) {
    ++readNumber;
    editing = { collection, item, removing, base: config };
    const group = collection === 'groups', noun = group ? '分组' : '条目';
    const action = removing ? '删除' : item ? '编辑' : '添加';
    $('cleaning-title').textContent = action + noun;
    $('cleaning-name-field').hidden = removing;
    $('cleaning-name').required = !removing;
    $('cleaning-name').value = item?.name || '';
    $('cleaning-rule-fields').hidden = group || removing;
    $('cleaning-delete-fields').hidden = !removing;
    $('cleaning-cascade-field').hidden = !group;
    $('cleaning-cascade').checked = false;
    if (removing) {
      $('cleaning-delete-summary').textContent = '确定删除“' + item.name + '”？' +
        (group ? '默认将组内条目移至一级，它们将按各自的开关生效。' : '');
    }
    const groups = $('cleaning-group');
    groups.replaceChildren(new Option('无分组', ''));
    for (const entry of config.groups) groups.append(new Option(entry.name, entry.id));
    groups.value = item?.group_id || '';
    enhanceSelect(groups);
    const position = $('cleaning-position');
    position.disabled = group || removing;
    position.required = !position.disabled;
    position.max = config.rules.length + (item ? 0 : 1);
    position.value = item && !group ? config.rules.indexOf(item) + 1 : position.max;
    $('cleaning-kind').value = item?.kind || 'literal';
    $('cleaning-pattern').value = item?.pattern || '';
    $('cleaning-replacement').value = item?.replacement || '';
    $('cleaning-skip-markdown').checked = item?.skip_markdown ?? false;
    $('cleaning-error').hidden = true;
    $('cleaning-apply').textContent = removing ? '确认删除' : '保存';
    $('cleaning-apply').className = removing ? 'danger' : 'primary';
    updateKind();
    dialog.showModal();
    $(removing ? 'cleaning-cancel' : 'cleaning-name').focus();
  }

  on('add-cleaning-group', 'click', () => open('groups'));
  on('add-cleaning-rule', 'click', () => open('rules'));
  on('cleaning-kind', 'change', updateKind);
  on('cleaning-cancel', 'click', () => dialog.close());
  dialog.addEventListener('cancel', event => { if (saving) event.preventDefault(); });
  on('cleaning-form', 'submit', async () => {
    if (saving) return;
    const { base, collection, item, removing } = editing;
    const next = { ...base };
    if (removing) {
      next[collection] = base[collection].filter(entry => entry.id !== item.id);
      if (collection === 'groups') {
        next.rules = $('cleaning-cascade').checked
          ? base.rules.filter(rule => rule.group_id !== item.id)
          : base.rules.map(rule => rule.group_id === item.id ? { ...rule, group_id: null } : rule);
      }
    } else {
      const entry = { id: item?.id || newKey(), enabled: item?.enabled ?? true, name: $('cleaning-name').value.trim() };
      if (collection === 'rules') {
        entry.kind = $('cleaning-kind').value;
        entry.group_id = $('cleaning-group').value || null;
        entry.skip_markdown = $('cleaning-skip-markdown').checked;
        entry.pattern = $('cleaning-pattern').value;
        entry.replacement = $('cleaning-replacement').value;
        next.rules = base.rules.filter(rule => rule.id !== entry.id);
        next.rules.splice($('cleaning-position').valueAsNumber - 1, 0, entry);
      } else {
        next.groups = item
          ? base.groups.map(group => group.id === item.id ? entry : group)
          : [...base.groups, entry];
      }
    }
    try {
      next.root_order = editedRootOrder(base, next, item, collection);
      await save(next);
      dialog.close();
      notify(removing ? '已删除' : '已保存');
    } catch (error) {
      $('cleaning-error').textContent = error.message || '保存失败，请重试。';
      $('cleaning-error').hidden = false;
    }
  });
  return { refresh };
}
