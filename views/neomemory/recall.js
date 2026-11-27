import { $, date, element, labels, on } from './ui.js';

function nodeList(items) {
  const list = element('ol', '', 'recall-nodes');
  for (const item of items) {
    const row = element('li');
    row.append(element('strong', item.title || item.node_id));
    const fields = [labels[item.node_type] || item.node_type];
    if (item.source_scene) fields.push('其他空间 · ' + item.source_scene);
    if (item.score != null) fields.push('综合分 ' + item.score.toFixed(3));
    if (item.similarity != null) fields.push('相似度 ' + item.similarity.toFixed(3));
    if (item.reason) fields.push(item.reason);
    row.append(element('div', fields.join(' · '), 'muted'));
    list.append(row);
  }
  return list;
}

function renderSnapshot(snapshot) {
  const content = document.createDocumentFragment();
  const items = snapshot.items;
  const summary = [`选中 ${items.length} 条`, `耗时 ${Math.round(snapshot.elapsed_ms)} 毫秒`];
  if (snapshot.similarity_threshold != null) summary.push('相似度阈值 ' + snapshot.similarity_threshold);
  if (snapshot.min_strength != null) summary.push('最低强度 ' + snapshot.min_strength);
  if (snapshot.selection) {
    const selection = snapshot.selection;
    const method = {selector: '模型选取证据', rerank: '模型重排', local: '本地筛选'}[selection.evidence_selection];
    summary.push(selection.recall_route === 'overview' ? '多事件回顾' : '具体查询',
      '混合候选 ' + selection.hybrid_candidates + ' 条', method);
    if (selection.contextual_query) summary.push('已结合上文');
    if (selection.selection_fallback) summary.push('已使用回退策略');
  }
  content.append(element('p', summary.join(' · '), 'muted'));
  if (snapshot.index_coverage) {
    const {usable, total, truncated} = snapshot.index_coverage;
    const cap = truncated ? '（已达候选上限）' : '';
    content.append(element('p', `本空间可检索向量 ${usable} / ${total}${cap}`, 'muted'));
  }
  if (snapshot.index_deferred) {
    const reason = {capacity: '索引并发已满', cooldown: '等待下次检查', space_busy: '空间正在执行其他任务'};
    content.append(element('p', '等待后台补建 · ' + reason[snapshot.index_deferred], 'muted'));
  }
  if (snapshot.last_index) {
    const {state, progress_done, progress_total, updated_at, error_code} = snapshot.last_index;
    const states = {queued: '排队中', running: '运行中', retry_wait: '等待重试', succeeded: '已完成', failed: '失败', cancelled: '已取消'};
    const details = [date(updated_at), states[state] || state];
    if (progress_total) details.push(`${progress_done} / ${progress_total} 条`);
    if (snapshot.last_index.repaired != null) details.push(`补齐 ${snapshot.last_index.repaired} 条版本向量`);
    if (state === 'failed' && error_code) details.push(error_code);
    content.append(element('p', '本空间最近的索引任务 · ' + details.join(' · '), 'muted'));
  }
  if (snapshot.query_vectors) {
    const {reused, total, provider_calls} = snapshot.query_vectors;
    content.append(element('p', `查询向量复用 ${reused} / ${total} · 向量模型调用 ${provider_calls} 次`, 'muted'));
  }
  if (snapshot.reason) content.append(element('p', snapshot.reason));
  const cacheOperations = {read: '读取向量缓存', write: '写入向量缓存', touch: '更新缓存访问时间'};
  for (const [operation, errorType] of Object.entries(snapshot.cache_errors || {})) {
    content.append(element('p', `${cacheOperations[operation]}失败（${errorType}）`));
  }
  if (items.length) content.append(nodeList(items));
  if (snapshot.preview_candidates?.length) {
    const excluded = element('details');
    excluded.append(element('summary', `未入选的候选（${snapshot.preview_candidates.length}）`), nodeList(snapshot.preview_candidates));
    content.append(excluded);
  }
  if (snapshot.text) {
    const title = { test: '长期记忆预览', active: '工具返回内容', passive: '注入内容', external: '外部接口返回内容' }[snapshot.mode];
    content.append(element('h3', title), element('p', snapshot.text, 'recall-text'));
  }
  return content;
}

export function initRecallInspector(ui) {
  const query = $('recall-query'), result = $('recall-result'), history = $('recall-history');
  const toggle = $('recall-recording'), refreshButton = $('recall-refresh');
  let testRequest = 0, historyRequest = 0, testing = false, saving = false;
  const visible = () => ui.page() === 'maintenance' && !$('recall-test-form').closest('section').hidden;

  function updateTestButton() {
    $('recall-run').disabled = testing || !ui.space() || !query.value.trim();
  }

  function showHistory(enabled, records) {
    toggle.checked = enabled;
    history.replaceChildren();
    for (const record of records) {
      const entry = element('details', '', 'card');
      const type = { active: '主动回忆', passive: '被动召回', external: '外部记忆接口' }[record.mode];
      const queryLabel = record.query?.trim() || record.media_placeholder || '[无文字]';
      entry.append(element('summary', `${date(record.created_at)} · ${type} · ${queryLabel}`), renderSnapshot(record));
      history.append(entry);
    }
    if (!records.length) {
      const empty = ui.space() ? '当前空间尚无召回记录。' : '请选择记忆空间查看记录。';
      history.append(element('p', enabled ? empty : '召回记录已关闭。', 'muted'));
    }
  }

  async function refresh() {
    if (saving) return;
    const request = ++historyRequest, epoch = ui.epoch();
    toggle.disabled = refreshButton.disabled = true;
    try {
      const space = ui.space();
      if (!space) result.textContent = '请先选择记忆空间。';
      const data = await ui.bridge.apiGet('recall/history', space ? { space_id: space } : {});
      if (request !== historyRequest || !ui.current(epoch)) return;
      showHistory(data.enabled, data.records);
      toggle.disabled = false;
    } catch (error) {
      if (request === historyRequest && ui.current(epoch)) history.textContent = error.message || '读取召回记录失败，请刷新重试。';
    } finally {
      if (request === historyRequest) refreshButton.disabled = false;
    }
  }

  on('recall-recording', 'change', async () => {
    const enabled = toggle.checked;
    ++historyRequest;
    saving = true;
    toggle.disabled = refreshButton.disabled = true;
    try {
      await ui.write('recall/recording', { enabled });
      if (!enabled) showHistory(false, []);
    } catch (error) {
      toggle.checked = !enabled;
      throw error;
    } finally {
      saving = false;
      await refresh();
    }
  });
  on('recall-refresh', 'click', refresh);
  on('recall-query', 'input', updateTestButton);
  on('recall-test-form', 'submit', async () => {
    const text = query.value.trim(), space = ui.space();
    if (testing || !space || !text) return;
    const request = ++testRequest;
    testing = true;
    updateTestButton();
    result.textContent = '正在检索当前空间的记忆…';
    try {
      const snapshot = await ui.bridge.apiPost('recall/test', { space_id: space, query: text });
      if (request === testRequest) {
        result.replaceChildren(element('h3', '查询结果'), element('p', snapshot.query, 'recall-text'), renderSnapshot(snapshot));
      }
    } catch (error) {
      if (request === testRequest) result.textContent = error.message || '召回测试失败，请重试。';
    } finally {
      if (request === testRequest) {
        testing = false;
        updateTestButton();
      }
    }
  });

  return {
    visible, refresh,
    changed() {
      ++testRequest;
      ++historyRequest;
      testing = false;
      query.value = '';
      result.textContent = ui.space() ? '' : '请先选择记忆空间。';
      history.replaceChildren();
      toggle.disabled = true;
      updateTestButton();
    },
  };
}
