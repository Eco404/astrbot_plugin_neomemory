import { $, element, notify, connectionStatus, dismissNotification, on, setupSelectKeys, enhanceSelect } from './ui.js';
import { initMaintenance } from './maintenance.js';
import { initIdentities } from './identities.js';
import { initMemory } from './memory.js';
import { initNotes } from './notes.js';
import { initSettings } from './settings.js';
import { mountIcons } from './icons.js';

mountIcons();
const header = document.querySelector('.page-header');
const sidebar = document.querySelector('.sidebar');
const spaceBar = $('space-bar');
const spaceAnchor = $('space-anchor');
const compact = matchMedia('(max-width: 900px)');
let stickyFrame = 0;

function fadeCoveredBar(bar, anchor, active) {
  const top = parseFloat(getComputedStyle(bar).top) || 0;
  const height = Math.max(1, bar.offsetHeight);
  const covered = active ? Math.max(0, Math.min(1, (top + height - anchor.getBoundingClientRect().top) / height)) : 0;
  bar.style.setProperty('--bar-opacity', String(1 - covered));
  bar.style.setProperty('--bar-blur', covered * 8 + 'px');
  bar.inert = covered >= 1;
}
function updateStickyBars() {
  stickyFrame = 0;
  fadeCoveredBar(sidebar, $('header-anchor'), compact.matches);
  const spaceBarPins = spaceBar.parentElement === $('main') && !spaceBar.hidden;
  fadeCoveredBar(header, spaceAnchor, spaceBarPins);
  document.documentElement.style.setProperty('--pinned-height', (spaceBarPins ? spaceBar : header).offsetHeight + 'px');
}
function scheduleStickyBars() {
  if (!stickyFrame) stickyFrame = requestAnimationFrame(updateStickyBars);
}
const stickySizes = new ResizeObserver(scheduleStickyBars);
for (const bar of [sidebar, header, spaceBar]) stickySizes.observe(bar);
window.addEventListener('scroll', scheduleStickyBars, { passive: true });
window.addEventListener('resize', scheduleStickyBars);
setupSelectKeys();

const bridge = window.AstrBotPluginView;
const state = { page: 'memory', epoch: 0, posting: false, spaces: [], offset: 0, ready: false, spacesRequest: 0 };
const selectedSpaceInfo = () => state.spaces.find(space => space.space_id === $('space').value);
const ui = {
  bridge, space: () => $('space').value, spaceType: () => selectedSpaceInfo()?.space_type,
  spaceOwner: () => selectedSpaceInfo()?.logical_user_id, epoch: () => state.epoch,
  showSpaceBar(visible) { $('space-bar').hidden = !visible; scheduleStickyBars(); },
  current: (epoch) => epoch === state.epoch, page: () => state.page,
  async write(endpoint, body) {
    if (state.posting) throw new Error('另一项操作正在提交，请稍后重试。');
    state.posting = true; document.body.setAttribute('aria-busy', 'true');
    try { return await bridge.apiPost(endpoint, body); }
    finally { state.posting = false; document.body.setAttribute('aria-busy', 'false'); }
  },
  async refreshSpaces() { await loadSpaces(); },
  async showJob(job) { if (await navigate('maintenance')) await maintenance.inspect(job); }
};
const memory = initMemory(ui), notes = initNotes(ui), settings = initSettings(ui), maintenance = initMaintenance(ui);
const identities = initIdentities(ui);
const pages = { memory, notes, models: settings, settings, maintenance: {
  async refresh() { await Promise.all([maintenance.refresh(), identities.refresh()]); }
} };
async function navigate(page) {
  if (page !== state.page && !await settings.canLeave(state.page)) return false;
  if (state.page !== page) window.scrollTo({ top: 0, behavior: 'instant' });
  state.page = page;
  document.querySelectorAll('[data-page]').forEach((node) => node.setAttribute('aria-current', node.dataset.page === page ? 'page' : 'false'));
  document.querySelectorAll('.page').forEach((node) => { node.hidden = node.id !== 'page-' + page; });
  if (page === 'settings') {
    if (spaceBar.parentElement !== $('space-settings')) $('space-settings').prepend(spaceBar);
  } else if (spaceBar.parentElement !== $('main')) {
    spaceAnchor.after(spaceBar);
  }
  spaceBar.hidden = ['settings', 'models'].includes(page);
  $('page-title').textContent = { memory:'记忆', notes:'笔记', models:'模型', settings:'设置', maintenance:'维护' }[page];
  $('page-description').textContent = {
    memory: '查阅经历、了解彼此，让每次对话有所承接。',
    notes: '查阅和管理 Agent 在当前记忆空间写下的笔记。',
    models: '为记忆构建与召回选择模型，并检查连接状态。',
    settings: '按你的相处方式调整功能，也可以进一步微调参数。',
    maintenance: '查看后台进度，整理记忆，处理需要关注的问题。',
  }[page];
  scheduleStickyBars();
  if (state.ready) await pages[page].refresh();
  return true;
}
async function loadSpaces(append = false) {
  const request = ++state.spacesRequest, epoch = state.epoch;
  const previous = ui.space(); if (!append) state.offset = 0;
  const result = await bridge.apiGet('memory/spaces', { limit: 100, offset: state.offset });
  if (request !== state.spacesRequest || epoch !== state.epoch) return;
  state.spaces = append ? [...state.spaces, ...result.items] : result.items;
  $('space').replaceChildren(element('option', '请选择空间')); $('space').firstChild.value = '';
  for (const space of state.spaces) { const option = element('option', space.display_label); option.value = space.space_id; $('space').append(option); }
  $('space').value = state.spaces.some((space) => space.space_id === previous) ? previous : state.spaces[0]?.space_id || '';
  enhanceSelect($('space'));
  $('more-spaces').hidden = !result.has_more; state.offset += result.items.length;
  if (ui.space() !== previous) changed();
  selectedSpace = ui.space();
}
function changed() { state.epoch += 1; memory.changed(); notes.changed(); maintenance.changed(); identities.changed(); }
let selectedSpace = '';
on('space', 'change', async () => {
  const requested = ui.space();
  $('space').value = selectedSpace;
  if (!await settings.canChangeSpace()) return;
  $('space').value = selectedSpace = requested;
  changed(); await pages[state.page].refresh();
});
on('refresh', 'click', async () => { await loadSpaces(); await pages[state.page].refresh(); });
on('more-spaces', 'click', () => loadSpaces(true));
for (const node of document.querySelectorAll('[data-page]')) node.addEventListener('click', () => navigate(node.dataset.page).catch((error) => notify(error.message, true)));
try {
  if (!bridge) throw new Error('请从 AstrBot 插件页面打开本页。');
  await bridge.ready();
  await settings.loadAppearance().catch(() => notify('未能读取已保存的外观。', true));
  state.ready = true; connectionStatus(true); await loadSpaces(); await memory.refresh();
  window.setInterval(() => {
    if (document.hidden || !state.ready) return;
    const task = state.page === 'maintenance' ? Promise.all([maintenance.poll(), identities.poll()]) : state.page === 'notes' ? notes.poll() : Promise.resolve();
    task.then(() => dismissNotification('polling')).catch(() => notify('状态暂不可用，正在自动重试。', false, { key: 'polling' }));
  }, 3000);
} catch (error) { if (!state.ready) connectionStatus(false); notify(error.message || '请从 AstrBot 插件页面打开，并确认插件服务已启动。', true); }
