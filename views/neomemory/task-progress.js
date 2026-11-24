import { date, element } from './ui.js';

const activeStates = new Set(['queued', 'running', 'retry_wait']);
export function formatDuration(seconds) {
  const total = Math.max(0, Math.floor(seconds));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor(total % 3600 / 60);
  const remainder = String(total % 60).padStart(2, '0');
  return hours ? `${hours}:${String(minutes).padStart(2, '0')}:${remainder}` : `${minutes}:${remainder}`;
}
export function taskTiming(job) {
  const node = element('p', '', 'task-timing');
  node.dataset.created = job.created_at ?? '';
  node.dataset.started = date(job.created_at);
  node.dataset.ended = activeStates.has(job.state) ? '' : job.updated_at ?? '';
  node.dataset.active = String(activeStates.has(job.state));
  updateTiming(node);
  return node;
}
function updateTiming(node) {
  const end = node.dataset.active === 'true' ? Date.now() / 1000 : Number(node.dataset.ended);
  const known = node.dataset.created !== '' && (node.dataset.active === 'true' || node.dataset.ended !== '');
  node.textContent = `启动时间：${node.dataset.started} · 总用时：${known ? formatDuration(end - Number(node.dataset.created)) : '未记录'}`;
}
function updateRecovery(node) {
  const seconds = Math.max(0, Math.ceil(Number(node.dataset.available) - Date.now() / 1000));
  node.textContent = seconds > 0 ? `正在自动重试，${formatDuration(seconds)} 后继续，无需操作。`
    : '正在等待后台自动重试，无需操作。';
}
export function taskFeedback(job) {
  if (job.state === 'retry_wait' && job.error_code === 'identity_binding_conflict') {
    return element('p', '等待确认账号归属。请在用户管理中处理旧归属，确认后将自动继续写入。', 'muted');
  }
  if (job.state === 'retry_wait') {
    const node = element('p', '', 'task-recovery');
    node.dataset.available = job.available_at ?? '';
    updateRecovery(node); return node;
  }
  if (job.state === 'failed' && job.error_code === 'model_required') {
    return element('p', '待指定模型。点击“重新准备后重试”选择本次任务的模型。', 'muted');
  }
  if (job.state === 'failed' && (job.error_code || job.error_type)) {
    return element('p', [job.error_type, job.error_code].filter(Boolean).join(' / '), 'error');
  }
  return document.createDocumentFragment();
}
// Update visible clocks without polling the server or replacing focused controls.
window.setInterval(() => {
  if (document.hidden) return;
  document.querySelectorAll('.task-timing').forEach(updateTiming);
  document.querySelectorAll('.task-recovery').forEach(updateRecovery);
}, 1000);
