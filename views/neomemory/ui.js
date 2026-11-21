// Shared DOM helpers. Never interpret memory/model output as markup.
import { icon } from './icons.js';
export const $ = (id) => document.getElementById(id);
export function element(tag, text = '', className = '') {
  const node = document.createElement(tag); node.textContent = text; node.className = className; return node;
}
export function button(label, handler, className = '') {
  const node = element('button', label, className); node.type = 'button';
  node.addEventListener('click', async () => { if (node.disabled) return; node.disabled = true; try { await handler(); } catch (error) { notify(error.message || '操作未完成。', true); } finally { node.disabled = false; } });
  return node;
}
export function connectionStatus(connected) {
  $('status').textContent = connected ? '已连接' : '未连接';
  $('status').className = connected ? '' : 'error';
  $('status').dataset.state = connected ? 'ready' : 'error';
}
const notifications = new Map();
export function dismissNotification(key) { notifications.get(key)?.(); }
export function notify(message, error = false, { key = `${error}:${message}` } = {}) {
  if (notifications.has(key)) return notifications.get(key);
  const toast = element('div', '', error ? 'toast error' : 'toast');
  const close = element('button', '×', 'toast-close');
  close.type = 'button'; close.setAttribute('aria-label', '关闭通知');
  toast.append(element('p', message), close);
  let timer, remaining = 6000, started;
  let hovering = false, focused = false;
  function dismiss() {
    clearTimeout(timer); toast.remove(); notifications.delete(key);
  }
  function pause() {
    if (timer == null) return;
    clearTimeout(timer); timer = null;
    remaining = Math.max(0, remaining - (performance.now() - started));
  }
  function resume() {
    if (hovering || focused || timer != null) return;
    started = performance.now(); timer = window.setTimeout(dismiss, remaining);
  }
  toast.addEventListener('pointerenter', () => { hovering = true; pause(); });
  toast.addEventListener('pointerleave', () => { hovering = false; resume(); });
  toast.addEventListener('focusin', () => { focused = true; pause(); });
  toast.addEventListener('focusout', (event) => {
    focused = toast.contains(event.relatedTarget); if (!focused) resume();
  });
  close.addEventListener('click', dismiss);
  notifications.set(key, dismiss); $('notifications').append(toast); resume();
  return dismiss;
}
export function emptyState(message, symbol = 'memory') {
  const node = element('div', '', 'empty-state');
  node.append(icon(symbol), element('p', message));
  return node;
}
export function setupSelectKeys() {
  document.querySelectorAll('select').forEach(enhanceSelect);
  document.addEventListener('keydown', (event) => {
    const select = event.target;
    if (!(select instanceof HTMLSelectElement) || select.multiple || select.disabled
        || !['ArrowDown', 'ArrowUp'].includes(event.key) || !CSS.supports('appearance', 'base-select')
        || select.matches(':open') || !select.showPicker) return;
    try {
      select.showPicker();
      event.preventDefault();
    } catch (error) {
      // A cross-origin host iframe may prohibit showPicker; native keys still work.
      if (error.name !== 'SecurityError' && error.name !== 'NotAllowedError') throw error;
    }
  });
}
export function enhanceSelect(select) {
  if (select.multiple || !CSS.supports('appearance', 'base-select')) return;
  if (select.firstElementChild?.tagName !== 'BUTTON') {
    const trigger = element('button', '', 'select-value');
    trigger.type = 'button';
    trigger.append(document.createElement('selectedcontent'));
    select.prepend(trigger);
  }
}
export function on(id, event, handler) { $(id).addEventListener(event, async (e) => { if (event === 'submit') e.preventDefault(); try { await handler(e); } catch (error) { notify(error.message || '请求失败，请稍后重试。', true); } }); }
export function onSearchForm(id, handler) {
  on(id, 'submit', handler);
  on(id, 'input', event => {
    if (event.target.type === 'search' && event.target.value === '' && !event.isComposing) return handler();
  });
}
export function shortLogicalUserId(id) {
  return 'lu_' + id.replace(/^(lu_|user_)/, '').slice(0, 7);
}

export function pretty(id, value) {
  $(id).textContent = JSON.stringify(value, function (key, item) {
    if (typeof item === 'string' && (key.endsWith('logical_user_id') ||
        (key === 'participant_id' && this.participant_kind === 'logical_user'))) {
      return shortLogicalUserId(item);
    }
    return item;
  }, 2);
}
export function newKey() {
  // getRandomValues also works when AstrBot is served on a plain HTTP LAN URL.
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  return 'web-' + Array.from(bytes, (value) => value.toString(16).padStart(2, '0')).join('');
}
export const labels = { active:'活跃', archived:'已归档', disabled:'禁用', historical:'历史', superseded:'已替代', deleted:'已删除', atomic:'原子', event:'事件', topic:'主题' };
export const date = (value) => value == null ? '未知' : new Date(value * 1000).toLocaleString();
export function setupTabs(selector, selected) {
  const tabs = Array.from(document.querySelectorAll(selector));
  function select(tab, ...args) {
    if (tab.hidden) return;
    tabs.forEach((item) => { const active = item === tab; item.setAttribute('aria-selected', String(active)); item.tabIndex = active ? 0 : -1; });
    selected(tab, ...args);
  }
  tabs.forEach((tab) => {
    tab.addEventListener('click', () => select(tab));
    tab.addEventListener('keydown', (event) => {
      const visible = tabs.filter(item => !item.hidden);
      const index = visible.indexOf(tab);
      let next = null;
      if (event.key === 'ArrowRight') next = (index + 1) % visible.length;
      if (event.key === 'ArrowLeft') next = (index - 1 + visible.length) % visible.length;
      if (event.key === 'Home') next = 0;
      if (event.key === 'End') next = visible.length - 1;
      if (next !== null) { event.preventDefault(); visible[next].focus(); visible[next].click(); }
    });
  });
  return select;
}
export const emotionLabels = { joy: '喜悦', sadness: '难过', anger: '生气', fear: '害怕', surprise: '惊讶', trust: '信任', concern: '担心', affection: '亲近', neutral: '平静' };
export const emotionEntries = (emotion) => emotion?.tags || [];
