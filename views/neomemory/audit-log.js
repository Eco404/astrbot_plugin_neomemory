import { $, date, element, on, notify } from './ui.js';

const levels = { debug: '调试', info: '信息', warning: '警告', error: '错误' };
const categories = { task: '后台任务', operation: '操作', system: '插件运行' };
const fields = { actor: '操作人', entity_id: '对象', entity_kind: '对象类型', status: 'HTTP 状态', kind: '任务类型', job_id: '任务', account_id: '平台账号', platform_instance_id: '适配器', scope_key: '任务范围', space_id: '记忆空间',
  error_code: '错误码', error_type: '异常类型', step: '阶段', state: '状态', attempt: '尝试次数', action: '动作',
  fields: '设置项', enabled: '记录开关', days: '保留天数', automatic_origin: '自动触发', recovery_mode: '恢复方式',
  available_at: '下次可执行时间', deleted_count: '清除数量', applied: '已应用', retried: '重试数量',
  logical_user_id: '逻辑用户', node_id: '记忆节点', note_id: '笔记', profile_id: '画像', relationship_id: '人格关系',
  state_id: '状态记录', fresh: '重新准备', regenerate_window: '重新生成窗口' };

export function initAuditLog(ui) {
  const enabled = $('audit-enabled'), days = $('audit-days'), save = $('audit-save');
  const dialog = $('audit-confirm-dialog'), status = $('audit-status');
  let saved, request = 0, next = null, cursors = [''], filters = {}, writing = false;
  function dirty() {
    return saved && (enabled.checked !== saved.enabled || days.valueAsNumber !== saved.days);
  }
  function controls() {
    enabled.disabled = days.disabled = !saved || writing;
    save.disabled = writing || !dirty() || !days.validity.valid;
  }
  function render(row) {
    const entry = element('article', '', 'audit-entry');
    const heading = element('div', '', 'audit-heading');
    const level = element('span', levels[row.level], 'audit-level');
    level.dataset.level = row.level;
    heading.append(level, element('strong', row.event), element('small', categories[row.category], 'muted'),
      element('time', date(row.time), 'muted'));
    entry.append(heading);
    const details = Object.entries(row.details);
    if (details.length) {
      const list = element('div', '', 'audit-details');
      for (const [key, value] of details) {
        let label = value;
        if (key === 'available_at') label = date(value);
        else if (typeof value === 'boolean' || key === 'automatic_origin') label = value ? '是' : '否';
        else if (Array.isArray(value)) label = value.join('、');
        list.append(element('span', (fields[key] || key) + ' · ' + label));
      }
      entry.append(list);
    }
    return entry;
  }
  async function load(pages = ['']) {
    const version = ++request;
    status.textContent = '正在读取日志…';
    try {
      const result = await ui.bridge.apiGet('logs', { ...filters, cursor: pages.at(-1) });
      if (request !== version) return;
      if (!dirty()) {
        saved = result.settings;
        enabled.checked = saved.enabled;
        days.value = saved.days;
      }
      cursors = pages;
      next = result.next_cursor;
      $('audit-list').replaceChildren(...result.items.map(render));
      status.textContent = result.items.length ? '' : '没有符合条件的日志。';
      $('audit-pages').hidden = !next && cursors.length === 1;
      $('audit-prev').disabled = cursors.length === 1;
      $('audit-next').disabled = !next;
      $('audit-page').textContent = '第 ' + cursors.length + ' 页';
    } catch {
      if (request !== version) return;
      $('audit-list').replaceChildren();
      $('audit-pages').hidden = true;
      status.textContent = '读取日志失败，请刷新重试。';
    } finally { controls(); }
  }
  function search() {
    filters = { level: $('audit-level').value, category: $('audit-category').value,
      day: $('audit-day').value, keyword: $('audit-keyword').value.trim() };
    return load();
  }
  on('audit-filters', 'submit', search);
  for (const id of ['audit-level', 'audit-category', 'audit-day']) on(id, 'change', search);
  on('audit-keyword', 'input', () => { if (!$('audit-keyword').value && filters.keyword) return search(); });
  on('audit-refresh', 'click', search);
  on('audit-prev', 'click', () => load(cursors.slice(0, -1)));
  on('audit-next', 'click', () => load([...cursors, next]));
  on('audit-preferences', 'input', controls);
  on('audit-preferences', 'submit', async () => {
    writing = true; controls();
    try {
      saved = await ui.write('logs/settings', { enabled: enabled.checked, days: days.valueAsNumber });
      notify('日志设置已保存。');
      await load();
    } finally { writing = false; controls(); }
  });
  on('audit-clear', 'click', () => dialog.showModal());
  on('audit-cancel', 'click', () => dialog.close());
  on('audit-confirm', 'click', async () => {
    const confirm = $('audit-confirm'), cancel = $('audit-cancel');
    confirm.disabled = cancel.disabled = true;
    try {
      await ui.write('logs/clear', { confirm: true });
      dialog.close();
      await load();
      notify('插件日志已清空。');
    } finally { confirm.disabled = cancel.disabled = false; }
  });
  controls();
  return { visible: () => !$('audit-list').closest('section').hidden, refresh: () => load() };
}
