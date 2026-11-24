import { initTaskModels } from './task-models.js';
import { initRecallInspector } from './recall.js';
import { formatDuration, taskTiming, taskFeedback } from './task-progress.js';
import { $, date, element, notify, pretty, on, onSearchForm, newKey, emotionLabels, emotionEntries, enhanceSelect, setupTabs } from './ui.js';

function attemptLabel(attempt = 0, limit = 0) {
  return attempt > limit ? `网络恢复第${attempt - limit}次` : `尝试 ${attempt} / ${limit}`;
}

function fieldLabel(caption) {
  const label = element('label');
  label.append(element('span', caption));
  return label;
}

export function initMaintenance(ui) {
  const bridge = ui.bridge;
  const state = { spaces: [], nodes: [], available: false, posting: false, refreshing: false,
    clearingTasks: false, clearScope: null, taskRevision: 0,
    nodeOffset: 0, nodeRequest: 0, nodeLoading: false, taskOffset: 0, spaceOffset: 0, selectedJob: null, detail: null, rawJob: null, rawRequest: 0,
    mergeSelected: new Map(), mergeCandidates: [], mergeOffset: 0, mergeRequest: 0, ingressOffset: 0, ingressRefreshing: false, ingressReady: false, selectedSource: null };
  const pendingSubmissionKeys = new Map();
  let backlogEpoch = null;
  const recall = initRecallInspector(ui);
  const confirmModelTask = initTaskModels(ui);
  const modelJobs = new Set(['extract', 'rebuild', 'section_rebuild', 'topic_consolidate', 'topic_rebuild', 'profile_rebuild', 'relationship_rebuild', 'context_rebuild', 'short_term']);
  setupTabs('[data-maintenance-tab]', tab => {
    for (const section of document.querySelectorAll('[data-maintenance-content]')) {
      section.hidden = section.dataset.maintenanceContent !== tab.dataset.maintenanceTab;
    }
    if (tab.dataset.maintenanceTab === 'tasks') refreshBacklog();
    if (tab.dataset.maintenanceTab === 'recall') recall.refresh();
  });
  function showTasks() { document.querySelector('[data-maintenance-tab="tasks"]').click(); }
  const activeStates = new Set(['queued', 'running', 'retry_wait']);
  const states = { queued: '排队中', running: '执行中', retry_wait: '自动重试中', failed: '失败', succeeded: '完成', cancelled: '已取消' };
  const kinds = { identity: '账号关联管理', note: '笔记更新', model_test: '模型连通测试', extract: '整理抽取', topic_consolidate: '整理主题', topic_rebuild: '主题重构', profile_rebuild: '用户画像重构', relationship_rebuild: '人格关系重构', context_rebuild: '画像与人格关系重构', section_rebuild: '按板块重构', rebuild: '重构记忆', reindex: '重建索引', governance: '人工治理', reject: '放弃失败批次', short_term: '短期滚动摘要' };
  const actionLabels = { active: '活跃', historical: '历史', disabled: '禁用', superseded: '已替代', deleted: '已删除' };
  function stepLabel(step) {
    const labels = { companion_receipts: '按送达时间补录分段', identity_apply: '更新账号关联', topic_prepare: '准备主题归纳材料', topic_model: '生成主题概况与成员', topic_validate: '校验主题候选', topic_index: '准备主题向量', topic_publish: '发布重构后的主题', prepared: '窗口已准备', model: '生成记忆候选', validate: '校验候选', correcting: '自动纠正候选', replay: '回放原始记录', index: '准备记忆向量', window_index: '更新向量索引', switch: '启用构建结果', governance: '应用人工治理', complete: '完成' };
    Object.assign(labels, { short_term_prepared: '短期摘要窗口已准备', short_term_model: '生成短期摘要', short_term_validate: '校验短期摘要', short_term_correcting: '自动纠正短期摘要' });
    if (step?.startsWith('context:')) return '查找关联旧记忆，已扫描 ' + step.slice(8) + ' 条';
    if (step?.startsWith('index:')) return '准备记忆向量 ' + step.slice(6);
    if (step?.startsWith('topic_') && step.includes(':')) {
      const [name, progress] = step.split(':', 2);
      return (labels[name] || name) + ' ' + progress;
    }
    return labels[step] || step || '等待执行';
  }
  const spaceId = () => $('space').value;
  const selectedNode = () => state.nodes.find((node) => node.node_id === $('node-select').value);
  function renderContextBudget(value) {
    const panel = $('context-budget-status'); panel.replaceChildren();
    if (!value) { panel.append(element('p', '尚无装配记录。', 'muted')); return; }
    const names = { short_term: '短期摘要', profile: '用户画像', relationship: '人格关系', relationship_aftereffect: '人格关系 · 近期余韵', notes: '笔记提示', long_term: '长期记忆' };
    const reasons = { budget: '剩余预算不足，整项省略', entry_limit: '超过该类条目的字符上限', node_limit: '已达节点数上限', coverage: '已由完整事件覆盖' };
    panel.append(element('p', `空间：${value.space_id || '未知'} · 装配标识：${value.request_id || '未记录'}`));
    if (value.recorded_at) panel.append(element('p', `记录时间：${new Date(value.recorded_at * 1000).toLocaleString()}`));
    panel.append(element('p', `注入 ${value.injected_bytes} / ${value.budget_limit} UTF-8 字节 · 剩余 ${value.remaining_bytes ?? value.budget_limit - value.injected_bytes} 字节`));
    panel.append(element('p', `辅助资料含封套 ${value.context_bytes ?? '未知'} 字节 · 长期记忆含封套 ${value.long_term_bytes ?? '未知'} 字节 · 块间分隔 ${value.separator_bytes ?? '未知'} 字节`));
    const progress = element('progress'); progress.max = value.budget_limit; progress.value = value.injected_bytes; progress.setAttribute('aria-label', '记忆注入预算占用'); panel.append(progress);
    const table = element('table'), head = element('tr');
    for (const text of ['条目', '渲染字节', '单独装配字节', '结果']) head.append(element('th', text));
    const thead = element('thead'); thead.append(head); const body = element('tbody');
    for (const item of value.entries || []) {
      const row = element('tr'); row.append(element('td', (names[item.section] || item.section) + (item.node_id ? ` · ${item.node_id}` : '')),
        element('td', String(item.rendered_bytes)), element('td', String(item.standalone_bytes ?? '未知')),
        element('td', item.included ? '已注入' : item.reason === 'budget' && item.standalone_bytes > value.budget_limit
          ? '单项已超过总预算，调整顺序无效' : item.reason === 'entry_limit'
          ? `${reasons.entry_limit}（${item.rendered_chars} / ${item.max_chars}）` : reasons[item.reason] || item.reason || '未注入')); body.append(row);
    }
    table.append(thead, body); panel.append(table, element('p', '逐项字节包含昵称展开和条目标记；总量另计共享封套、括号与分隔符。没有候选或功能关闭的项目不列出。', 'muted'));
  }
  function emotionEditor(value = null) {
    const editor = element('fieldset', '', 'emotion-editor');
    editor.append(element('legend', '当时的情绪'));
    const enabledLabel = element('label'), enabled = document.createElement('input');
    enabled.type = 'checkbox'; enabled.className = 'emotion-enabled'; enabled.checked = Boolean(value);
    enabledLabel.append(enabled, document.createTextNode('记录这条记忆中的情绪')); editor.append(enabledLabel);
    const fields = element('div', '', 'emotion-fields'); fields.hidden = !enabled.checked;
    const levels = new Map(emotionEntries(value).map(({ tag, intensity }) => [tag, intensity]));
    const tagsLabel = fieldLabel('情绪标签（最多 3 项）'), tags = document.createElement('select');
    tags.multiple = true; tags.size = 5; tags.className = 'emotion-tags';
    for (const [key, label] of Object.entries(emotionLabels)) { const option = element('option', label); option.value = key; option.selected = levels.has(key); tags.append(option); }
    tagsLabel.append(tags);
    const intensities = element('div', '', 'emotion-intensities');
    function showIntensities() {
      for (const input of intensities.querySelectorAll('input')) levels.set(input.dataset.tag, input.value);
      intensities.replaceChildren();
      for (const option of tags.selectedOptions) {
        const label = fieldLabel(`${emotionLabels[option.value]}强度`), input = document.createElement('input');
        input.type = 'number'; input.className = 'emotion-intensity'; input.dataset.tag = option.value;
        input.min = 0; input.max = 1; input.step = 0.01; input.value = levels.get(option.value) ?? 0.5;
        label.append(input); intensities.append(label);
      }
    }
    tags.addEventListener('change', showIntensities); showIntensities();
    const descriptionLabel = fieldLabel('简短情绪说明'), description = document.createElement('textarea');
    description.className = 'emotion-description'; description.value = value?.description || ''; descriptionLabel.append(description);
    fields.append(tagsLabel, intensities, descriptionLabel, element('p', '每个标签的强度独立填写。描述最多 160 个可读字符，完整用户占位符按一个字符计算。未记录时不推断为平静；请区分谁的情绪。', 'muted'));
    editor.append(fields); enabled.addEventListener('change', () => { fields.hidden = !enabled.checked; });
    return editor;
  }
  function readEmotion(editor) {
    if (!editor.querySelector('.emotion-enabled').checked) return null;
    const tags = Array.from(editor.querySelector('.emotion-tags').selectedOptions, (option) => option.value);
    if (!tags.length || tags.length > 3 || (tags.includes('neutral') && tags.length !== 1)) throw new Error('请选择 1–3 个情绪标签，平静不能与其他情绪并选。');
    const entries = tags.map(tag => ({ tag, intensity: score(editor.querySelector(`.emotion-intensity[data-tag="${tag}"]`), `${emotionLabels[tag]}强度`) }));
    const description = editor.querySelector('.emotion-description').value.trim();
    const readable = description.replace(/\{\{user:([^{}\s]+)\}\}/g, '人');
    if (!description || Array.from(readable).length > 160 || readable.includes('{{user:')) throw new Error('情绪说明必须有内容，且不超过 160 个可读字符。');
    return { tags: entries, description };
  }
  function buttons() {
    const allowed = Boolean(spaceId()) && state.available && !state.posting;
    $('governance-editor').disabled = state.nodeLoading;
    $('governance-editor').setAttribute('aria-busy', String(state.nodeLoading));
    $('node-select').disabled = state.nodeLoading;
    $('refresh-nodes').disabled = state.nodeLoading;
    document.querySelectorAll('.mutation').forEach((button) => { button.disabled = !allowed; });
    const relationship = document.querySelector('#section-selection input[value="relationship"]');
    relationship.disabled = ui.spaceType() !== 'private';
    if (relationship.disabled) relationship.checked = false;
    $('section-rebuild').disabled = !allowed || !document.querySelector('#section-selection input:checked');
    $('section-rebuild-confirm').disabled = state.posting;
    $('rebuild').disabled = !allowed || !$('rebuild-confirm').checked;
    $('reject').disabled = !allowed || !state.detail || state.detail.kind !== 'extract' || state.detail.state !== 'failed'
      || state.detail.scope_key !== spaceId() || !$('reject-confirm').checked || !$('reject-reason').value.trim();
    $('governance').disabled = !allowed || !selectedNode() || ($('action').value === 'delete' && !$('delete-confirm').checked);
    document.querySelectorAll('.ingress-mutation').forEach((button) => { button.disabled = state.posting || !state.ingressReady; });
    $('clear-completed-tasks').disabled = state.posting || state.clearingTasks;
    $('clear-completed-confirm').disabled = $('clear-completed-cancel').disabled = state.clearingTasks;
  }
  async function request(endpoint, body) {
    if (state.posting) throw new Error('已有操作正在提交。');
    state.posting = true; buttons();
    try { return await ui.write(endpoint, body); }
    finally { state.posting = false; buttons(); }
  }
  async function submit(kind, extra = {}) {
    if (state.posting || !state.available || !spaceId()) return;
    const epoch = ui.epoch();
    const body = { kind, space_id: spaceId(), ...extra };
    const signature = JSON.stringify(body);
    const key = pendingSubmissionKeys.get(signature) || newKey();
    pendingSubmissionKeys.set(signature, key);
    try {
      const job = await request('tasks', { ...body, idempotency_key: key });
      pendingSubmissionKeys.delete(signature);
      if (!ui.current(epoch)) return;
      notify('任务已提交：' + job.job_id + (kind === 'governance' ? '。任务完成后请刷新节点，读取最新标题与内容。' : ''));
      if (kind.endsWith('_rebuild')) showTasks();
      state.selectedJob = job.job_id; $('task-detail-panel').open = false;
      await refreshTasks();
    } catch { notify('未确认任务是否提交成功。请先查看后台任务，如未显示，可重试同一操作。', true); }
  }
  function showNode() {
    const node = selectedNode();
    pretty('node-preview', node || '尚未选择节点。');
    $('node-title').value = node?.title || '';
    $('content').value = node?.content_text || ''; $('importance').value = node?.importance ?? 0.5;
    $('node-emotion-editor').replaceChildren(emotionEditor(node?.emotion));
    $('operation').value = ''; $('version-detail').textContent = '点击“查阅版本和来源”。'; state.sources = []; $('split-parts').replaceChildren(); $('delete-confirm').checked = false; $('use-json').checked = false; renderAction(); buttons();
  }
  async function loadNodes() {
    if (!spaceId()) { state.nodes = []; $('node-select').replaceChildren(); $('merge-nodes').replaceChildren(); showNode(); return; }
    const selected = spaceId(), epoch = ui.epoch(), offset = state.nodeOffset, request = ++state.nodeRequest;
    state.nodeLoading = true; buttons();
    try {
      const result = await bridge.apiGet('memory/nodes', { space_id: selected, limit: 50, offset });
      if (request !== state.nodeRequest || selected !== spaceId() || !ui.current(epoch) || offset !== state.nodeOffset) return;
      state.nodes = result.items;
      const previous = $('node-select').value;
      $('node-select').replaceChildren(); $('merge-nodes').replaceChildren();
      for (const node of state.nodes) {
        const title = `${node.node_type} · ${actionLabels[node.status] || node.status} · ${node.display_title || node.title || node.content_text.slice(0, 60)}`;
        const option = element('option', title); option.value = node.node_id; $('node-select').append(option);
      }
      if (state.nodes.some((node) => node.node_id === previous)) $('node-select').value = previous;
      enhanceSelect($('node-select'));
      $('previous-nodes').disabled = state.nodeOffset === 0; $('next-nodes').disabled = !result.has_more; showNode();
      if (['split', 'merge'].includes($('action').value)) await prepareAction();
    } finally {
      if (request === state.nodeRequest) { state.nodeLoading = false; buttons(); }
    }
  }
  async function details(jobId) {
    if (state.rawJob !== jobId) {
      clearTaskSource();
      state.rawJob = jobId;
      state.detail = null;
      $('task-detail').textContent = '正在读取任务结果…';
      $('task-sources').replaceChildren();
      $('task-diagnostics').textContent = $('restart-hint').textContent = $('reject-reason').value = '';
      $('reject-confirm').checked = false;
      buttons();
    }
    for (const radio of $('tasks').querySelectorAll('input[name="task-choice"]')) radio.checked = radio.value === jobId;
    state.selectedJob = jobId; const epoch = ui.epoch();
    const result = await bridge.apiGet('task', { job_id: jobId });
    if (state.selectedJob !== jobId || !ui.current(epoch)) return;
    if (result.hidden_from_task_list) { closeTaskDetails(); return; }
    state.detail = result; pretty('task-detail', result);
    const diagnostic = result.response_diagnostics || {};
    const summary = [`本窗口自动纠正 ${result.automatic_corrections || 0} 次${result.automatic_correction_limit == null ? '' : ` / 上限 ${result.automatic_correction_limit} 次`}`];
    if (diagnostic.response_status) summary.push('模型响应：' + diagnostic.response_status);
    if (diagnostic.incomplete_reason) summary.push('未完成原因：' + diagnostic.incomplete_reason);
    if (diagnostic.queue_seconds !== undefined) summary.push(`模型排队 ${diagnostic.queue_seconds.toFixed(2)} 秒`);
    if (diagnostic.call_started === false) summary.push('尚未取得模型并发名额');
    if (diagnostic.call_seconds !== undefined) summary.push(`调用耗时 ${diagnostic.call_seconds.toFixed(2)} 秒`);
    if (diagnostic.repeated_candidate) summary.push('候选与上次相同');
    if (result.topic_budget) {
      const budget = result.topic_budget;
      summary.push(`主题提示词 ${budget.prompt_bytes ?? budget.base_prompt_bytes} / ${budget.max_prompt_bytes} 字节`);
      if (budget.source_excerpts_retained != null) summary.push(`来源片段 ${budget.source_excerpts_retained} / ${budget.source_excerpts_available} 条`);
      if (budget.seed_count != null) summary.push(`本批审阅 ${budget.seed_count} 条，展开 ${budget.expanded_nodes} 条记忆`);
    }
    $('task-diagnostics').textContent = summary.join(' · ');
    $('task-sources').replaceChildren();
    for (const sourceId of result.source_ids || []) taskAction($('task-sources'), '读取原文 ' + sourceId, () => readTaskSource(result, sourceId));
    $('restart-hint').textContent = result.recovery_action === 'review' && result.kind === 'identity' ? '账号归属或确认的影响范围已变化，请回到用户管理重新预览和确认。' : result.recovery_action === 'review' && result.kind === 'note' ? '笔记冲突需要返回笔记页，重新读取并编辑。断点重试保留原始版本条件，不会覆盖更新后的正文。' : result.restart_required ? '该任务需要按当前配置重新准备。请使用“重新准备后重试”。' : result.recovery_action === 'resize' ? '从断点重试会缩小截断的消息窗口并保留后续原文。' : result.recovery_action === 'regenerate' ? '从断点重试会归档被拒绝的草稿，把校验问题反馈给模型，并继续各个尚未完成的窗口。' : result.regenerate_window_available ? '从断点重试会继续处理已有草稿。选择“重新生成当前窗口”可另行生成候选。' : result.kind === 'short_term' && result.state === 'cancelled' ? '该短期摘要窗口已停止；使用“从断点重试”或“重新准备后重试”恢复处理。' : '';
    buttons();
  }
  function clearTaskSource() { state.rawRequest += 1; $('task-source-content').textContent = ''; $('task-source-panel').hidden = true; }
  async function readTaskSource(job, sourceId) {
    const request = ++state.rawRequest, epoch = ui.epoch();
    $('task-source-panel').hidden = false; $('task-source-panel').open = true; $('task-source-content').textContent = '正在读取任务原文…';
    try {
      const result = await bridge.apiGet('memory/source', { space_id: job.scope_key, source_id: sourceId });
      if (request === state.rawRequest && ui.current(epoch) && state.selectedJob === job.job_id) pretty('task-source-content', result.source);
    } catch { if (request === state.rawRequest && ui.current(epoch)) $('task-source-content').textContent = '原文不可用，可能已被清理或不属于该任务空间。'; }
  }
  function taskAction(card, label, handler, disabled = false, className = '') {
    const button = element('button', label, className); button.disabled = disabled || state.posting;
    button.addEventListener('click', async () => { button.disabled = true; try { await handler(); } catch { notify('操作未完成，请刷新任务详情后重试。', true); } finally { button.disabled = disabled || state.posting; buttons(); } });
    card.append(button);
  }
  async function taskMutation(endpoint, job, extra = {}) {
    if (state.posting) return;
    if (extra.fresh && modelJobs.has(job.kind)) {
      const selection = await confirmModelTask({ kind: job.kind, space_id: job.scope_key, job_id: job.job_id },
        '重新准备' + kinds[job.kind] + '？', '将按当前空间和模型设置重新准备任务。', '重新准备');
      if (!selection) return;
      extra = { ...extra, model_selection: selection };
    }
    await request(endpoint, { job_id: job.job_id, ...extra });
    notify('任务状态已更新。'); await refreshTasks();
  }
  function closeTaskDetails() {
    state.selectedJob = null; state.detail = null; state.rawJob = null;
    clearTaskSource(); $('task-sources').replaceChildren();
    $('task-detail').textContent = ''; $('restart-hint').textContent = ''; $('task-diagnostics').textContent = '';
    $('task-detail-panel').open = false;
  }
  async function confirmCancelTask(job) {
    const dialog = $('cancel-task-dialog'), epoch = ui.epoch();
    $('cancel-task-target').textContent = `${kinds[job.kind] || job.kind} · ${job.job_id}`;
    dialog.returnValue = '';
    const closed = new Promise(resolve => dialog.addEventListener('close', resolve, { once: true }));
    dialog.showModal();
    await closed;
    if (dialog.returnValue === 'cancel-task' && ui.current(epoch)) await taskMutation('tasks/cancel', job);
  }
  async function deleteTask(job) {
    if (state.posting) return;
    const result = await request('tasks/delete', { job_id: job.job_id });
    if (state.selectedJob === job.job_id) {
      closeTaskDetails();
    }
    notify(`任务已删除，清理未启用记忆 ${result.removed_node_count || 0} 条。`);
    await refreshTasks();
  }
  function openClearCompleted() {
    if (state.posting || state.clearingTasks) return;
    state.clearScope = $('all-tasks').checked || !spaceId() ? null : spaceId();
    $('clear-completed-scope').textContent = state.clearScope
      ? '范围：当前空间（' + $('space').selectedOptions[0].textContent + '）。' : '范围：所有空间。';
    $('clear-completed-error').hidden = true;
    $('clear-completed-progress').hidden = true;
    $('clear-completed-dialog').showModal();
    $('clear-completed-cancel').focus();
  }
  async function clearCompleted() {
    if (state.posting || state.clearingTasks) return;
    state.clearingTasks = true; state.taskRevision += 1;
    $('clear-completed-error').hidden = true;
    $('clear-completed-progress').hidden = false;
    $('clear-completed-dialog').setAttribute('aria-busy', 'true');
    buttons();
    try {
      const result = await request('tasks/clear-completed', state.clearScope ? { scope_key: state.clearScope } : {});
      if (state.detail?.state === 'succeeded' && (!state.clearScope || state.detail.scope_key === state.clearScope)) {
        closeTaskDetails();
      }
      state.taskOffset = 0;
      $('clear-completed-dialog').close();
      notify(`已清空 ${result.deleted_count} 个已完成任务。`);
    } catch (error) {
      $('clear-completed-error').textContent = error.message || '清空未完成，请重试。';
      $('clear-completed-error').hidden = false;
    } finally {
      state.clearingTasks = false;
      $('clear-completed-progress').hidden = true;
      $('clear-completed-dialog').setAttribute('aria-busy', 'false');
      buttons();
    }
    if (!$('clear-completed-dialog').open) {
      $('clear-completed-tasks').focus({ preventScroll: true });
      await refreshTasks();
    }
  }
  function automaticStatus(automatic) {
    const panel = $('automatic-status');
    $('automatic-jobs').replaceChildren();
    const topicBlocked = !!automatic?.topic_blocking_jobs?.length;
    panel.hidden = !automatic || (automatic.state === 'enabled' && !topicBlocked);
    if (panel.hidden) return;
    const reasons = {
      failed_window: ['自动记忆更新已暂停', '该空间有尚未解决的失败窗口。新消息仍会记录，已有记忆仍可召回。请查看关联任务，重试、重新准备，或明确放弃失败抽取批次。'],
      automatic_extraction_disabled: ['自动记忆更新已关闭', '可在设置中启用自动抽取，也可以手动整理待处理内容。'],
      llm_not_configured: ['自动记忆更新等待模型配置', '请先选择可用的抽取模型。原始记录会继续保存。'],
      unsupported_persona: ['当前人格已跳过', '默认人格不参与本插件的记忆处理。请选择明确的角色卡。']
    };
    const [title, reason] = automatic.state === 'enabled' && topicBlocked
      ? ['自动主题整理已暂停', '该空间有尚未解决的主题任务。普通消息抽取和已有记忆召回仍可继续，请查看关联任务并重试或重新准备。']
      : reasons[automatic.reason] || ['自动记忆更新暂不可用', '请检查当前配置和后台任务。'];
    $('automatic-title').textContent = title; $('automatic-reason').textContent = reason;
    for (const job of [...(automatic.blocking_jobs || []), ...(automatic.topic_blocking_jobs || [])]) {
      const row = element('div', '', 'row');
      row.append(element('span', `${kinds[job.kind] || job.kind} · ${job.error_code || '需要人工处理'}`, 'muted'));
      taskAction(row, '查看关联任务 ' + job.job_id, async () => {
        showTasks(); $('task-detail-panel').open = true; await details(job.job_id);
        $('task-detail-panel').scrollIntoView({ block: 'center', behavior: 'smooth' });
      });
      $('automatic-jobs').append(row);
    }
  }
  const bytes = (value) => {
    const count = Number.isFinite(value) ? value : 0;
    return count >= 1048576 ? (count / 1048576).toFixed(1) + ' MiB' : count >= 1024 ? (count / 1024).toFixed(1) + ' KiB' : count + ' B';
  };
  async function ingressDetail(sourceId) {
    state.selectedSource = sourceId; $('ingress-detail-panel').open = true;
    $('ingress-detail').textContent = '正在读取所选原文…';
    try {
      const result = await bridge.apiGet('ingress/record', { source_id: sourceId });
      if (state.selectedSource === sourceId) pretty('ingress-detail', result);
    } catch {
      if (state.selectedSource === sourceId) $('ingress-detail').textContent = '该待写记录暂不可读取，可能已经成功入库。请刷新列表。';
    }
  }
  async function retryIngress(sourceId) {
    if (state.posting || !state.ingressReady) return;
    const result = await request('ingress/retry', sourceId ? { source_id: sourceId } : {});
    notify(`已恢复 ${result.retried || 0} 条待写重试，重新接收 ${result.readmitted || 0} 条内存应急记录。`);
    await refreshIngress();
  }
  async function refreshHealth() {
    const health = await bridge.apiGet('health');
    pretty('health', health);
    const deliveryIssue = $('companion-delivery-issue');
    const delivery = health.companion_delivery_progress || {};
    deliveryIssue.textContent = delivery.blocked_receipt
      ? `等待重试陪伴回执 ${delivery.blocked_receipt} · 下次尝试 ${date(delivery.retry_at)}`
      : health.companion_delivery_sync ? '陪伴插件回执同步：' + health.companion_delivery_sync : '';
    deliveryIssue.hidden = !deliveryIssue.textContent;
    renderContextBudget(health.recall?.diagnostics?.last_context);
    const sizes = Object.entries(health.storage_bytes || {});
    $('storage-usage').replaceChildren(...sizes.map(([name, size]) => element('p', name + ' · ' + bytes(size))));
    $('storage-usage').append(element('p', sizes.length ? '合计 ' + bytes(sizes.reduce((total, entry) => total + entry[1], 0)) + '（含 SQLite 辅助文件）' : '暂无存储统计。', 'muted'));
    return health;
  }
  async function refreshIngress() {
    if (state.ingressRefreshing) return;
    state.ingressRefreshing = true;
    try {
      const offset = state.ingressOffset;
      const [result, health] = await Promise.all([
        bridge.apiGet('ingress', { limit: 20, offset }), refreshHealth()
      ]);
      if (offset !== state.ingressOffset) return;
      state.ingressReady = true;
      const summary = result.health;
      const emergency = health.ingress || {};
      $('ingress-count').textContent = `已持久保存 ${summary.pending_count} / ${summary.max_entries} 条待写记录`;
      $('ingress-bytes').textContent = `待写正文 ${bytes(summary.payload_bytes)} / ${bytes(summary.max_bytes)}`;
      $('ingress-entry-progress').max = summary.max_entries || 1;
      $('ingress-entry-progress').value = summary.pending_count || 0;
      $('ingress-byte-progress').max = summary.max_bytes || 1;
      $('ingress-byte-progress').value = summary.payload_bytes || 0;
      const counts = summary.counts || {};
      const pendingTimes = [summary.oldest_pending_at, emergency.oldest_unpersisted_at].filter(Number.isFinite);
      $('ingress-summary').textContent = `排队 ${counts.queued || 0} · 写入 ${counts.running || 0} · 等待重试 ${counts.retry_wait || 0} · 需要人工重试 ${counts.failed || 0}`
        + (pendingTimes.length ? ` · 最久待写 ${formatDuration(Date.now() / 1000 - Math.min(...pendingTimes))}` : '')
        + (health.last_source_write_at ? ` · 最近写入 ${date(health.last_source_write_at)}` : '')
        + (summary.last_error_code ? ` · 最近诊断：${summary.last_error_code}` : '');
      const unpersisted = emergency.unpersisted_count || 0;
      const overflow = emergency.unpersisted_overflow || 0;
      $('ingress-unpersisted').hidden = !unpersisted && !overflow;
      $('ingress-unpersisted').textContent = (unpersisted
        ? `尚有 ${unpersisted} 条记录只保存在本次运行的内存中，重启会丢失。磁盘或待写容量恢复后会自动按序写入。`
        : '当前没有尚未落盘的内存应急记录。')
        + (overflow ? ` 本次运行另有 ${overflow} 条记录超过内存应急容量，未能保存。` : '');
      $('ingress-list').replaceChildren();
      if (!result.items.length) $('ingress-list').append(element('p', '当前页没有待写记录。', 'muted'));
      for (const item of result.items) {
        const card = element('article', '', 'task');
        card.dataset.state = item.state;
        card.append(element('div', `${states[item.state] || item.state} · ${item.source_id}`, 'task-title'));
        card.append(element('p', `会话流：${item.stream_id} · 累计尝试 ${item.total_attempts ?? item.attempts} 次 · ${bytes(item.payload_bytes)}`, 'muted'));
        card.append(taskFeedback(item));
        const actions = element('div', '', 'row');
        taskAction(actions, '查看原文', () => ingressDetail(item.source_id));
        if (['failed', 'retry_wait'].includes(item.state)) {
          taskAction(actions, '重试这条记录', () => retryIngress(item.source_id));
          actions.lastChild.classList.add('ingress-mutation');
        }
        card.append(actions); $('ingress-list').append(card);
      }
      $('previous-ingress').disabled = offset === 0; $('next-ingress').disabled = !result.has_more;
    } catch {
      state.ingressReady = false; $('ingress-summary').textContent = '待写状态暂不可用，稍后会自动重试。';
    } finally { state.ingressRefreshing = false; buttons(); }
  }
  function progressBar(label, done, total, state) {
    const bar = document.createElement('progress');
    bar.setAttribute('aria-label', label);
    bar.max = 100;
    if (state === 'succeeded') bar.value = 100;
    else if (total > 0) bar.value = Math.min(100, 100 * done / total);
    else if (!activeStates.has(state)) bar.value = 0;
    return bar;
  }
  async function refreshBacklog() {
    const count = $('extraction-backlog');
    if (count.closest('section').hidden) return;
    const selected = spaceId(), epoch = ui.epoch();
    if (!selected) {
      count.textContent = '请选择记忆空间。';
      return;
    }
    if (backlogEpoch === epoch) return;
    backlogEpoch = epoch;
    try {
      const result = await bridge.apiGet('extraction/backlog', { space_id: selected });
      if (ui.current(epoch)) count.textContent = `当前空间有 ${result.pending_count} 条原始记录待抽取。`;
    } catch {
      if (ui.current(epoch)) count.textContent = '待抽取数量暂不可用，稍后自动重试。';
    } finally {
      if (backlogEpoch === epoch) backlogEpoch = null;
    }
  }
  async function refreshTasks() {
    if (state.refreshing) return;
    state.refreshing = true;
    try {
      const selected = spaceId(), epoch = ui.epoch(), offset = state.taskOffset, all = $('all-tasks').checked, revision = state.taskRevision;
      const scope = $('all-tasks').checked || !selected ? {} : { scope_key: selected };
      const [result, available] = await Promise.all([
        bridge.apiGet('tasks', { ...scope, limit: 20, offset: state.taskOffset }),
        selected ? bridge.apiGet('availability', { space_id: selected }) : Promise.resolve({ available: false, blocking_jobs: [] })
      ]);
      if (revision !== state.taskRevision || state.clearingTasks || selected !== spaceId() || !ui.current(epoch) || offset !== state.taskOffset || all !== $('all-tasks').checked) return;
      state.available = available.available; automaticStatus(available.automatic);
      $('availability').textContent = !selected ? '尚无可选空间；收到可归属的会话后后台会发现空间。'
        : available.available ? '当前空间可以启动任务。' : '当前空间暂不可启动新任务：' + available.blocking_jobs.map((job) => `${kinds[job.kind] || job.kind} (${job.job_id})`).join('；');
      const confirming = new Set(Array.from($('tasks').querySelectorAll('.task-delete-confirm:not([hidden])'), panel => panel.closest('article').dataset.jobId));
      $('tasks').replaceChildren();
      if (!result.items.length) $('tasks').append(element('p', '暂无任务。', 'muted'));
      for (const job of result.items) {
        const card = element('article', '', 'task');
        card.dataset.state = job.state;
        card.dataset.jobId = job.job_id;
        const choice = element('input');
        choice.type = 'radio';
        choice.name = 'task-choice';
        choice.value = job.job_id;
        choice.checked = job.job_id === state.selectedJob;
        choice.setAttribute('aria-label', '选择任务 ' + job.job_id);
        const title = element('label', '', 'task-title');
        const statusLabel = job.state === 'failed' && job.error_code === 'model_required' ? '待指定模型' : states[job.state] || job.state;
        title.append(choice, `${job.section_progress ? '主题 + ' : ''}${kinds[job.kind] || job.kind} · ${statusLabel}`);
        const heading = element('div', '', 'task-heading');
        const identity = element('div');
        identity.append(title, element('div', job.job_id, 'muted'));
        heading.append(identity);
        if (activeStates.has(job.state) || (job.state === 'failed' && job.automatic_origin)) taskAction(heading, '取消', () => confirmCancelTask(job), false, 'danger');
        card.append(heading);
        if (job.section_progress) {
          for (const [lane, entry] of Object.entries(job.section_progress)) {
            const name = lane === 'topics' ? '主题' : ({
              profile_rebuild: '用户画像', relationship_rebuild: '人格关系', context_rebuild: '用户画像与人格关系'
            }[job.kind] || '会话回放');
            const state = job.state === 'cancelled' && entry.state !== 'succeeded' ? 'cancelled' : entry.state;
            const attempt = entry.attempt ? ' · ' + attemptLabel(entry.attempt, job.max_attempts) : '';
            card.append(element('p', name + ' · ' + (states[state] || state) + ' · ' + stepLabel(entry.step)
              + ' · ' + entry.done + ' / ' + (entry.total ?? '待确定') + attempt));
            card.append(progressBar(name + '进度', entry.done, entry.total, state));
            if (state === 'retry_wait' && entry.available_at) card.append(taskFeedback(entry));
            if (state === 'failed') card.append(taskFeedback(entry));
          }
        } else {
          card.append(progressBar('任务进度', job.progress_done, job.progress_total, job.state));
          card.append(element('p', `当前步骤：${stepLabel(job.step)} · ${job.progress_done || 0} / ${job.progress_total ?? '待确定'} · ${attemptLabel(job.attempt, job.max_attempts)}`));
        }
        card.append(taskTiming(job), taskFeedback(job));
        const actions = element('div', '', 'row');
        if (['failed', 'cancelled'].includes(job.state)) {
          if (!job.restart_required && job.recovery_action !== 'review' && job.error_code !== 'model_required') taskAction(actions, '从断点重试', () => taskMutation('tasks/retry', job));
          if (job.regenerate_window_available) taskAction(actions, '重新生成当前窗口', () => taskMutation('tasks/retry', job, { regenerate_window: true }));
          if (!['note', 'identity'].includes(job.kind)) taskAction(actions, '重新准备后重试', () => taskMutation('tasks/retry', job, { fresh: true }));
        }
        if (job.state !== 'running') {
          const confirmation = element('div', '', 'task-delete-confirm'); confirmation.hidden = !confirming.has(job.job_id);
          confirmation.append(element('p', '删除任务、候选和检查点；未启用的重构结果及其向量也会清理。已生效记忆、来源和笔记保留。尚未处理的来源仍可被自动抽取。'));
          taskAction(confirmation, '确认删除任务', () => deleteTask(job), false, 'danger');
          taskAction(confirmation, '返回', () => { confirmation.hidden = true; });
          taskAction(actions, '删除任务', () => { confirmation.hidden = !confirmation.hidden; }, false, 'danger');
          card.append(confirmation);
        }
        if (actions.childElementCount) card.append(actions);
        $('tasks').append(card);
      }
      $('previous-tasks').disabled = state.taskOffset === 0; $('next-tasks').disabled = !result.has_more;
      if (state.selectedJob) await details(state.selectedJob);
      buttons();
    } catch { state.available = false; buttons(); $('availability').textContent = '维护状态暂不可用，正在自动重试。'; }
    finally { state.refreshing = false; }
  }
  async function loadVersions() {
    const node = selectedNode(); if (!node) return null; const epoch = ui.epoch();
    const result = await bridge.apiGet('memory/versions', { node_id: node.node_id });
    if (selectedNode()?.node_id !== node.node_id || !ui.current(epoch)) return null;
    pretty('version-detail', result); $('version-detail').closest('details').open = true; return result;
  }
  function renderAction() {
    const action = $('action').value, node = selectedNode();
    $('merge-editor').hidden = action !== 'merge'; $('split-editor').hidden = action !== 'split';
    $('delete-confirm-row').hidden = action !== 'delete';
    $('node-title').closest('label').hidden = !['edit', 'merge'].includes(action);
    $('title-hint').hidden = !['edit', 'merge'].includes(action);
    $('content').closest('label').hidden = !['edit', 'merge'].includes(action);
    $('importance').closest('label').hidden = !['edit', 'merge'].includes(action);
    $('node-emotion-editor').hidden = !['edit', 'merge'].includes(action);
    state.mergeSelected.clear(); state.mergeOffset = 0; state.mergeRequest += 1; state.mergeCandidates = []; $('merge-nodes').replaceChildren(); $('merge-selected').replaceChildren();
    $('operation').value = ''; buttons();
  }
  function renderMergeSelection() {
    $('merge-selected').replaceChildren();
    for (const member of state.mergeSelected.values()) {
      const remove = element('button', '移除：' + (member.display_title || member.title || member.content_text.slice(0, 35)));
      remove.addEventListener('click', () => { state.mergeSelected.delete(member.node_id); for (const option of $('merge-nodes').options) if (option.value === member.node_id) option.selected = false; renderMergeSelection(); });
      $('merge-selected').append(remove);
    }
  }
  async function loadMergeCandidates() {
    const node = selectedNode(); if (!node) return;
    const request = ++state.mergeRequest, epoch = ui.epoch(), offset = state.mergeOffset;
    const result = await bridge.apiGet('memory/nodes', { space_id: spaceId(), node_type: node.node_type, q: $('merge-q').value.trim(), limit: 50, offset });
    if (request !== state.mergeRequest || !ui.current(epoch) || selectedNode()?.node_id !== node.node_id || $('action').value !== 'merge') return;
    state.mergeCandidates = result.items.filter((item) => item.node_id !== node.node_id && item.node_type === node.node_type && !['deleted','superseded'].includes(item.status));
    $('merge-nodes').replaceChildren();
    for (const member of state.mergeCandidates) { const option = element('option', member.display_title || member.title || member.content_text.slice(0, 100)); option.value = member.node_id; option.selected = state.mergeSelected.has(member.node_id); $('merge-nodes').append(option); }
    $('merge-prev').disabled = offset === 0; $('merge-next').disabled = !result.has_more;
  }
  function chooseMergeCandidates() {
    for (const member of state.mergeCandidates) {
      if (Array.from($('merge-nodes').selectedOptions).some((option) => option.value === member.node_id)) state.mergeSelected.set(member.node_id, member);
      else state.mergeSelected.delete(member.node_id);
    }
    renderMergeSelection();
  }
  function addPart(content = '', importance = selectedNode()?.importance ?? 0.5, title = '') {
    if ($('split-parts').children.length >= 20) return;
    const part = element('fieldset', '', 'split-part');
    part.append(element('legend', '拆分部分'));
    const titleLabel = fieldLabel('标题'), titleInput = document.createElement('input'); titleInput.type = 'text'; titleInput.className = 'part-title'; titleInput.value = title; titleLabel.append(titleInput);
    const textLabel = fieldLabel('正文'), text = document.createElement('textarea'); text.className = 'part-content'; text.value = content; textLabel.append(text);
    const scoreLabel = fieldLabel('重要性'), score = document.createElement('input'); score.type = 'number'; score.className = 'part-importance'; score.min = 0; score.max = 1; score.step = 0.01; score.value = importance; scoreLabel.append(score);
    const sources = element('div', '', 'sources'); sources.append(element('p', '关联原始来源'));
    for (const sourceId of state.sources || []) {
      const label = element('label'), input = document.createElement('input'); input.type = 'checkbox'; input.value = sourceId; input.checked = true;
      label.append(input, document.createTextNode(' ' + sourceId)); sources.append(label);
    }
    if (!(state.sources || []).length) sources.append(element('p', '未找到可关联的原始来源，暂不能拆分。', 'error'));
    const remove = element('button', '移除本部分'); remove.type = 'button'; remove.addEventListener('click', () => { if ($('split-parts').children.length > 2) part.remove(); else notify('拆分至少保留两部分。', true); });
    part.append(titleLabel, textLabel, scoreLabel, emotionEditor(), sources, remove); $('split-parts').append(part);
  }
  async function prepareAction() {
    renderAction();
    $('node-emotion-editor').replaceChildren(emotionEditor($('action').value === 'edit' ? selectedNode()?.emotion : null));
    if ($('action').value === 'merge') await loadMergeCandidates();
    if ($('action').value === 'split' && selectedNode()) {
      const info = await loadVersions(); if (!info || $('action').value !== 'split') return;
      state.sources = (info.sources || []).filter((source) => source.source_kind === 'source_record').map((source) => source.source_id);
      $('split-parts').replaceChildren(); addPart(selectedNode().content_text, selectedNode().importance, selectedNode().title || ''); addPart();
    }
  }
  function composeMerge() {
    const node = selectedNode(); if (!node) return;
    const members = [node, ...state.mergeSelected.values()];
    $('content').value = members.map((item) => item.content_text).join('\n\n'); $('importance').value = Math.max(...members.map((item) => item.importance));
    $('node-emotion-editor').replaceChildren(emotionEditor());
  }
  function score(input, name = '重要性') { const value = Number(input.value); if (!input.value.trim() || !Number.isFinite(value) || value < 0 || value > 1) throw new Error(`${name}必须为 0 到 1。`); return value; }
  function operation() {
    if ($('use-json').checked) return JSON.parse($('operation').value);
    const node = selectedNode(); if (!node) throw new Error('请先选择节点。');
    const action = $('action').value;
    const result = { action, expected_generation_id: node.generation_id,
      expected_versions: { [node.node_id]: node.content_version }, expected_updated_at: { [node.node_id]: node.updated_at }, node_id: node.node_id };
    if (action === 'edit') {
      if (!$('content').value.trim()) throw new Error('正文不能为空。');
      const title = $('node-title').value.trim(), content = $('content').value.trim(), importance = score($('importance'));
      result.patch = {};
      if (title !== (node.title || '')) { if (!title) throw new Error('标题不能为空。'); result.patch.title = title; result.expected_titles = { [node.node_id]: node.title ?? null }; }
      if ($('content').value !== node.content_text) result.patch.content_text = content;
      if (importance !== node.importance) result.patch.importance = importance;
      const emotion = readEmotion($('node-emotion-editor'));
      const identity = (value) => value ? [emotionEntries(value).sort((a, b) => a.tag.localeCompare(b.tag)), value.description] : null;
      if (JSON.stringify(identity(emotion)) !== JSON.stringify(identity(node.emotion))) result.patch.emotion = emotion;
      if (!Object.keys(result.patch).length) throw new Error('请先修改标题、正文、重要性或情绪。');
    }
    if (action === 'split') {
      result.parts = Array.from($('split-parts').children, (part, i) => ({ ref: 'part_' + (i + 1), title: part.querySelector('.part-title').value.trim(), content_text: part.querySelector('.part-content').value.trim(), importance: score(part.querySelector('.part-importance')), emotion: readEmotion(part.querySelector('.emotion-editor')), source_ids: Array.from(part.querySelectorAll('.sources input:checked'), (input) => input.value) }));
      if (result.parts.length < 2 || result.parts.length > 20 || result.parts.some((part) => !part.title || !part.content_text || !part.source_ids.length)) throw new Error('请填写 2–20 部分标题与正文，并为每部分至少选择一条来源。');
    }
    if (action === 'merge') {
      const members = [node, ...state.mergeSelected.values()];
      if (members.length < 2) throw new Error('请至少选择另一个同类型节点。');
      if (!$('content').value.trim()) throw new Error('合并后的完整正文不能为空。');
      delete result.node_id; result.node_ids = members.map((item) => item.node_id); result.keep_id = node.node_id;
      result.expected_versions = Object.fromEntries(members.map((item) => [item.node_id, item.content_version]));
      result.expected_updated_at = Object.fromEntries(members.map((item) => [item.node_id, item.updated_at]));
      result.expected_titles = Object.fromEntries(members.map((item) => [item.node_id, item.title ?? null]));
      result.title = $('node-title').value.trim(); if (!result.title) throw new Error('合并后的标题不能为空。');
      result.content_text = $('content').value.trim(); result.importance = score($('importance'));
      result.emotion = readEmotion($('node-emotion-editor'));
    }
    if (action === 'delete' && !$('delete-confirm').checked) throw new Error('请先确认删除语义。');
    return result;
  }
  function draftOperation() { $('operation').value = JSON.stringify(operation(), null, 2); notify('操作预览已生成，正常提交直接使用当前表单。'); }
  on('tasks', 'change', event => details(event.target.value));
  on('task-detail-panel', 'toggle', () => {
    if (!$('task-detail-panel').open || state.selectedJob) return;
    const first = $('tasks').querySelector('input[name="task-choice"]');
    if (first) return details(first.value);
  });
  on('task-source-clear', 'click', clearTaskSource);
  on('refresh-ingress', 'click', refreshIngress);
  on('retry-ingress', 'click', () => retryIngress());
  on('next-ingress', 'click', () => { state.ingressOffset += 20; return refreshIngress(); });
  on('previous-ingress', 'click', () => { state.ingressOffset = Math.max(0, state.ingressOffset - 20); return refreshIngress(); });
  on('clear-ingress-detail', 'click', () => { state.selectedSource = null; $('ingress-detail').textContent = '已清除本页原文展示。'; $('ingress-detail-panel').open = false; });
  on('all-tasks', 'change', () => { state.taskOffset = 0; return refreshTasks(); });
  on('next-tasks', 'click', () => { state.taskOffset += 20; return refreshTasks(); });
  on('previous-tasks', 'click', () => { state.taskOffset = Math.max(0, state.taskOffset - 20); return refreshTasks(); });
  on('section-selection', 'change', buttons);
  on('section-rebuild', 'click', async () => {
    const inputs = [...document.querySelectorAll('#section-selection input:checked')];
    const sections = inputs.map(input => input.value);
    const names = inputs.map(input => input.parentElement.textContent.trim()).join('、');
    const description = ['所选板块：' + names + '。'];
    if (sections.some(section => section !== 'topics')) description.push('画像或人格关系按原始会话顺序回放。');
    description.push('各分支成功后替换对应旧内容和版本历史，从 v1 重新计数。没有形成新内容时，对应板块将为空。');
    const selection = await confirmModelTask({ kind: 'section_rebuild', space_id: spaceId(), sections },
      '确认重构所选板块？', description.join(''), '确认重构');
    if (selection) await submit('section_rebuild', { sections, model_selection: selection });
  });
  async function submitWithModels(kind, description = '') {
    const selection = await confirmModelTask({ kind, space_id: spaceId() }, kinds[kind] + ' · 本次使用的模型', description, '启动任务');
    if (selection) await submit(kind, { model_selection: selection });
  }
  on('topic-consolidate', 'click', () => submitWithModels('topic_consolidate'));
  on('extract', 'click', () => submitWithModels('extract')); on('reindex', 'click', () => submit('reindex'));
  on('rebuild', 'click', () => $('rebuild-confirm').checked && submitWithModels('rebuild', '成功后替换当前空间的旧记忆和版本历史。'));
  for (const id of ['rebuild-confirm', 'reject-confirm', 'reject-reason', 'operation']) on(id, 'input', buttons);
  on('reject', 'click', () => state.detail && submit('reject', { failed_job_id: state.detail.job_id, reason: $('reject-reason').value }));
  on('clear-completed-tasks', 'click', openClearCompleted);
  on('clear-completed-confirm', 'click', clearCompleted);
  on('clear-completed-cancel', 'click', () => $('clear-completed-dialog').close());
  $('clear-completed-dialog').addEventListener('cancel', event => { if (state.clearingTasks) event.preventDefault(); });
  on('refresh-nodes', 'click', loadNodes); on('node-select', 'change', async () => { showNode(); await prepareAction(); });
  on('next-nodes', 'click', () => { state.nodeOffset += 50; return loadNodes(); });
  on('previous-nodes', 'click', () => { state.nodeOffset = Math.max(0, state.nodeOffset - 50); return loadNodes(); });
  on('versions', 'click', loadVersions); on('draft-operation', 'click', draftOperation);
  on('governance', 'click', () => submit('governance', { operation: operation(), reason: $('governance-reason').value }));
  on('action', 'change', prepareAction); on('add-part', 'click', () => addPart()); on('compose-merge', 'click', composeMerge); on('merge-nodes', 'change', chooseMergeCandidates);
  onSearchForm('merge-filter', () => { state.mergeOffset = 0; return loadMergeCandidates(); });
  on('merge-next', 'click', () => { state.mergeOffset += 50; return loadMergeCandidates(); });
  on('merge-prev', 'click', () => { state.mergeOffset = Math.max(0, state.mergeOffset - 50); return loadMergeCandidates(); }); on('delete-confirm', 'input', buttons);
  return {
    async refresh() { await Promise.all([recall.visible() && recall.refresh(), loadNodes(), refreshTasks(), refreshBacklog(), refreshIngress()]); },
    async poll() { if (!recall.visible() && !state.clearingTasks) await Promise.all([refreshTasks(), refreshBacklog(), refreshIngress()]); },
    changed() { recall.changed(); $('task-detail-panel').open = false; $('extraction-backlog').textContent = '正在读取当前空间的待抽取数量…'; state.nodeRequest += 1; state.nodeLoading = false; clearTaskSource(); state.rawJob = null; $('task-sources').replaceChildren(); state.available = false; state.nodeOffset = state.taskOffset = 0; state.selectedJob = state.detail = null; state.nodes = []; $('node-select').replaceChildren(); $('merge-nodes').replaceChildren(); $('node-preview').textContent = ''; $('version-detail').textContent = ''; $('tasks').replaceChildren(); $('automatic-status').hidden = true; $('task-detail').textContent = '请选择要查看的任务。'; $('restart-hint').textContent = ''; $('task-diagnostics').textContent = ''; $('operation').value = ''; $('node-title').value = ''; $('content').value = ''; $('split-parts').replaceChildren(); $('availability').textContent = '正在读取空间状态…'; buttons(); },
    async inspect(job) { showTasks(); if (job.scope_key === '*') $('all-tasks').checked = true; state.selectedJob = job.job_id; $('task-detail-panel').open = false; await refreshTasks(); }
  };
}
