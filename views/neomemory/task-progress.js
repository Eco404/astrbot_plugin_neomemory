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
const retryReasons = {
  model_queue_timeout: '正在等待模型并发名额，本次排队未发起模型调用。',
  storage_busy: '数据库正被占用。',
  source_database_busy: '原始库正被占用。',
  source_storage_full: '磁盘空间不足，释放空间后可提前重试。'
};
const failureHints = {
  reconstruction_evidence_insufficient: '现有依据不足以重建正文。',
  reconstruction_changed: '条目或依据已经变化，请重新生成草稿。',
  storage_busy: '等待数据库释放写锁，再从断点重试。',
  storage_full: '释放插件数据目录所在磁盘的空间，然后从断点重试。',
  provider_access_denied: '请检查所选模型的凭据和访问权限，确认连通后重试。',
  model_required: '请点击“重新准备后重试”，选择本次任务的模型。',
  prompt_budget_exceeded: '查看详细结果中的提示词用量，核对提示词预算后重新准备。',
  source_budget_exceeded: '单条原文超过窗口容量。查看详细结果中的用量，调整对应窗口上限后重新准备。',
  oversized_source: '单条原文超过窗口容量。请核对窗口的字符和估算 token 上限后重新准备。',
  output_budget_exceeded: '模型输出超过容量。请检查详细结果中的输出用量，再调整模型或输入范围。'
};
export function budgetDiagnostics(errors = []) {
  const labels = { source_budget_exceeded: '原文', prompt_budget_exceeded: '提示词', output_capacity_exceeded: '模型输出' };
  const summaries = [];
  for (const error of errors) {
    const label = labels[error.type];
    if (!label) continue;
    const measurements = [
      [error.actual_bytes, error.limit_bytes, 'UTF-8 字节'],
      [error.actual_chars ?? error.actual_encoded_chars, error.limit_chars, '字符'],
      [error.actual_tokens, error.limit_tokens, '估算 token']
    ];
    for (const [actual, limit, unit] of measurements) {
      if (Number.isFinite(actual) && Number.isFinite(limit)) summaries.push(`${label}用量 ${actual} / ${limit} ${unit}`);
    }
    if (error.reserved_feedback_bytes > 0) summaries.push(`其中为纠正反馈预留 ${error.reserved_feedback_bytes} 字节`);
  }
  return summaries;
}
function updateRecovery(node) {
  const seconds = Math.max(0, Math.ceil(Number(node.dataset.available) - Date.now() / 1000));
  const wait = seconds > 0 ? `正在自动重试，${formatDuration(seconds)} 后继续。` : '正在等待后台自动重试。';
  node.textContent = (retryReasons[node.dataset.reason] || '') + wait;
}
export function taskFeedback(job) {
  if (job.state === 'retry_wait' && job.error_code === 'identity_binding_conflict') {
    return element('p', '等待确认账号归属。请在用户管理中处理旧归属，确认后将自动继续写入。', 'muted');
  }
  if (job.state === 'retry_wait') {
    const node = element('p', '', 'task-recovery');
    node.dataset.available = job.available_at ?? '';
    node.dataset.reason = job.error_code || '';
    updateRecovery(node); return node;
  }
  if (job.state === 'failed' && job.automatic_origin) {
    const hint = failureHints[job.error_code] || '';
    return element('p', `上次失败（${job.error_code || job.error_type}）。${hint}下次自动恢复：${date(job.available_at)}，也可手动重试。`, 'muted');
  }
  if (job.state === 'failed' && job.error_code === 'model_required') {
    return element('p', '待指定模型。' + failureHints.model_required, 'muted');
  }
  if (job.state === 'failed' && (job.error_code || job.error_type)) {
    const code = [job.error_type, job.error_code].filter(Boolean).join(' / ');
    return element('p', [code, failureHints[job.error_code]].filter(Boolean).join(' · '), 'error');
  }
  return document.createDocumentFragment();
}
// Update visible clocks without polling the server or replacing focused controls.
window.setInterval(() => {
  if (document.hidden) return;
  document.querySelectorAll('.task-timing').forEach(updateTiming);
  document.querySelectorAll('.task-recovery').forEach(updateRecovery);
}, 1000);
