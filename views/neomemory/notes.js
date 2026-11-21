import { initPagination, PAGE_SIZE } from './pagination.js';
import { $, element, button, notify, on, onSearchForm, newKey, date, labels, emptyState } from './ui.js';

// Track each reference separately so equal display names can be edited independently.
function editableNote(text, names) {
  const spans = [];
  let delta = 0;
  let display = text.replace(/\{\{user:([^{}\s]+)\}\}/g, (reference, id, index) => {
    const name = Object.hasOwn(names, id) ? names[id] : '未命名用户';
    const start = index + delta;
    spans.push({ start, end: start + name.length, reference });
    delta += name.length - reference.length;
    return name;
  });
  function edit(value, cursor) {
    if (display === value) return;
    const shift = value.length - display.length;
    let end = display.length, nextEnd = value.length;
    // Stop at the caret to distinguish edits on either side of repeated names.
    while (end > 0 && nextEnd > cursor && display[end - 1] === value[nextEnd - 1]) { end--; nextEnd--; }
    let start = 0;
    while (start < end && start < nextEnd && display[start] === value[start]) start++;
    for (let i = spans.length - 1; i >= 0; i--) {
      const span = spans[i];
      if (span.end <= start) continue;
      if (span.start < end) spans.splice(i, 1);
      else { span.start += shift; span.end += shift; }
    }
    display = value;
  }
  function canonical() {
    let text = display;
    for (let index = spans.length - 1; index >= 0; index--) {
      const { start, end, reference } = spans[index];
      text = text.slice(0, start) + reference + text.slice(end);
    }
    return text;
  }
  return { display, edit, canonical };
}

export function initNotes(ui) {
  const state = { offset: 0, request: 0, selected: null, posting: false, key: newKey(), signature: null, job: null, polling: false };
  let editor = null;
  const input = $('note-content');
  input.addEventListener('input', () => editor?.edit(input.value, input.selectionEnd));
  function closeEditor() {
    editor = null;
    state.selected = null;
    state.key = newKey();
    input.value = '';
    $('note-editor').hidden = true;
  }
  async function write(action, item) {
    if (state.posting || !ui.space() || !item) return;
    const epoch = ui.epoch();
    const payload = { space_id: ui.space(), idempotency_key: state.key, action };
    payload.note_id = item.note_id;
    payload.expected_updated_at = item.updated_at;
    if (action === 'update') {
      payload.content_text = editor.canonical().trim();
      if (!payload.content_text) throw new Error('笔记正文不能为空。');
    }
    state.posting = true;
    $('note-save').disabled = true;
    const signature = JSON.stringify({ ...payload, idempotency_key: undefined });
    if (state.signature && state.signature !== signature) { state.key = newKey(); payload.idempotency_key = state.key; }
    state.signature = signature;
    try {
      const job = await ui.write('notes', payload); state.key = newKey();
      if (!ui.current(epoch)) return;
      state.job = job; $('note-job').textContent = '笔记任务已提交：' + job.job_id;
      if (state.selected?.note_id === item.note_id) closeEditor();
      await poll();
    } catch (error) { notify((error.message || '笔记提交未完成。') + ' 请刷新任务后再试，重复提交沿用原请求标识。', true); }
    finally { state.posting = false; $('note-save').disabled = !ui.space(); }
  }
  const updatePagination = initPagination('notes', async offset => {
    state.offset = offset;
    await refresh();
    $('notes-list').scrollIntoView({ block: 'start' });
  });
  async function refresh() {
    const request = ++state.request, epoch = ui.epoch();
    updatePagination(state.offset);
    $('note-save').disabled = !ui.space() || state.posting;
    if (!ui.space()) { $('notes-list').replaceChildren(emptyState('请先选择记忆空间。', 'notes')); return; }
    try {
      const result = await ui.bridge.apiGet('notes', { space_id: ui.space(), status: $('notes-status').value, q: $('notes-q').value.trim(), limit: PAGE_SIZE, offset: state.offset });
      if (!ui.current(epoch) || request !== state.request) return;
      if (state.offset > 0 && state.offset >= result.total) {
        state.offset = Math.max(0, Math.ceil(result.total / PAGE_SIZE) - 1) * PAGE_SIZE;
        return refresh();
      }
      $('notes-list').replaceChildren();
      for (const item of result.items) {
        const note = editableNote(item.content_text, result.user_names || {});
        const card = element('article', '', 'card'); card.append(element('span', labels[item.status] || item.status, 'badge'), element('p', note.display, 'body'), element('p', '更新时间：' + date(item.updated_at), 'muted'));
        const actions = element('div', '', 'row');
        actions.append(button('编辑', () => {
          state.selected = item;
          state.key = newKey();
          editor = editableNote(item.content_text, result.user_names || {});
          input.value = editor.display;
          $('note-editor').hidden = false;
          input.focus();
        }));
        actions.append(button(item.status === 'archived' ? '恢复' : '归档', () => write(item.status === 'archived' ? 'restore' : 'archive', item)));
        const confirmation = element('div', '', 'note-delete-confirm'); confirmation.hidden = true;
        confirmation.append(element('p', '删除后不再显示或参与查询，不能通过“恢复”找回。暂时收起请使用归档。'));
        confirmation.append(button('确认删除笔记', () => write('delete', item), 'danger'), button('取消删除', () => { confirmation.hidden = true; }));
        actions.append(button('删除', () => { confirmation.hidden = false; }, 'danger'));
        card.append(actions, confirmation); $('notes-list').append(card);
      }
      if (!result.items.length) $('notes-list').append(emptyState('当前条件下没有笔记。', 'notes'));
      updatePagination(state.offset, result);
    } catch (error) { if (ui.current(epoch) && request === state.request) { $('notes-list').replaceChildren(element('p', '笔记暂不可用，请刷新重试。', 'error')); notify(error.message, true); } }
  }
  async function poll() {
    if (!state.job || state.polling) return;
    const jobId = state.job.job_id, epoch = ui.epoch(); state.polling = true;
    try {
      const job = await ui.bridge.apiGet('task', { job_id: jobId }); if (!ui.current(epoch) || state.job?.job_id !== jobId) return;
      $('note-job').replaceChildren(element('span', `笔记任务：${job.state} · ${job.step || '等待执行'} `), button('查看任务', () => ui.showJob(job)));
      if (['succeeded','failed','cancelled'].includes(job.state)) { state.job = null; await refresh(); }
    } finally { state.polling = false; }
  }
  onSearchForm('notes-filter', () => { state.offset = 0; return refresh(); });
  on('note-cancel', 'click', closeEditor);
  on('note-form', 'submit', () => write('update', state.selected));
  return { refresh, poll, changed() { state.request += 1; state.offset = 0; updatePagination(0); state.job = null; closeEditor(); $('notes-list').replaceChildren(); $('note-job').textContent = ''; } };
}
