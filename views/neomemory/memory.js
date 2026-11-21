import { initPagination, PAGE_SIZE } from './pagination.js';
import { $, element, button, notify, on, onSearchForm, labels, emotionLabels, emotionEntries, date, setupTabs, emptyState, enhanceSelect } from './ui.js';

export function initMemory(ui) {
  const state = { kind: 'nodes', offset: 0, request: 0 };
  const kinds = { node: '长期记忆', profile: '用户画像', relationship: '人格关系' };
  const operations = { create: '创建', update: '更新', confirm: '补充引用', edit: '人工编辑', title_update: '修改标题', membership: '关联成员', split: '拆分', merge: '合并' };
  const editors = { system: '系统', model: '模型', manual: '人工', rebuild: '重构' };
  const prose = (item) => item.display_content_text ?? item.content_text ?? item.display_description ?? item.description ?? item.display_state_text ?? item.state_text ?? item.display_summary_text ?? item.summary_text ?? '';
  const target = (item) => ({ kind: item.kind || (item.node_id ? 'node' : item.profile_id ? 'profile' : 'relationship'), entity_id: item.entity_id || item.node_id || item.profile_id || item.relationship_id, ...(item.version_no ? { version_no: item.version_no } : {}) });
  function emotionalContext(item) {
    const emotion = item.display_emotion ?? item.emotion;
    if (!emotion) return element('p', '当时的情绪：未记录', 'muted memory-emotion');
    const section = element('div', '', 'memory-emotion');
    const tags = element('div', '', 'emotion-tag-list');
    for (const { tag, intensity } of emotionEntries(emotion)) tags.append(element('span', `${emotionLabels[tag] || tag} · ${intensity}`, 'emotion-tag'));
    section.append(element('p', '当时的情绪', 'muted'), tags, element('p', emotion.description, 'body'));
    return section;
  }
  function relationshipContent(item, heading = 'h3') {
    const wrapper = element('div', '', 'relationship-content');
    const saved = item.display_state ?? item.state;
    const slots = saved?.format === 'relationship.slots.v1' ? saved : {
      long_term: { body: item.display_state_text ?? item.state_text ?? '', observed_at: null },
      aftereffect: { body: '', observed_at: null }
    };
    for (const [slot, label] of [['long_term', '长期理解'], ['aftereffect', '近期余韵']]) {
      const part = element('section', '', 'relationship-slot'); part.dataset.slot = slot;
      part.append(element(heading, label), element('p', slots[slot].body || `暂无${label}。`, 'body'));
      if (slots[slot].body) part.append(element('p', `原互动依据时间：${date(slots[slot].observed_at)}`, 'muted relationship-observed-at'));
      wrapper.append(part);
    }
    return wrapper;
  }

  // 每张卡片独立持有版本导航和原文，离开本页后旧请求不能回写。
  function inspector(item, card) {
    const revision = state.request, epoch = ui.epoch(), spaceId = ui.space();
    const panel = element('details', '', 'memory-inspector');
    const summary = element('summary', '查阅版本和来源');
    const body = element('div', '', 'memory-detail');
    const live = () => revision === state.request && ui.current(epoch) && card.isConnected;
    const cache = new Map(), history = [];
    let current = target(item), loading = false, loaded = false, navigation = 0;
    panel.append(summary, body);

    function sourceLinks(container, records) {
      for (const record of records) {
        if (record.source_kind !== 'source_record') {
          container.append(element('span', `${record.source_kind}：${record.source_id}`, 'muted card-id'));
          continue;
        }
        const raw = element('details', '', 'memory-source');
        const label = element('summary', '读取原文 ' + record.source_id);
        const content = element('div', '', 'source-content');
        let busy = false, ready = false;
        async function read() {
          if (busy || !live()) return;
          busy = true; content.replaceChildren(element('p', '正在读取原始内容…', 'muted'));
          try {
            const result = await ui.bridge.apiGet('memory/source', { space_id: spaceId, source_id: record.source_id });
            if (!live() || !raw.isConnected) return;
            content.replaceChildren(element('pre', JSON.stringify(result.display_source ?? { content_text: result.source.content_text }, null, 2)), button('清除本条原文展示', () => { content.replaceChildren(); ready = false; raw.open = false; label.focus(); }));
            ready = true;
          } catch {
            if (live() && raw.isConnected) content.replaceChildren(element('p', '原文不可用，可能已清理或不属于当前记忆。', 'error'), button('重试读取原文', read));
          } finally { busy = false; }
        }
        raw.append(label, content);
        raw.addEventListener('toggle', () => { if (raw.open && !ready) read(); });
        container.append(raw);
      }
    }
    function versionLink(ref) {
      const label = `${kinds[ref.kind] || ref.kind} ${ref.entity_id} · 版本 ${ref.version_no}`;
      return ref.available === false
        ? element('span', label + '（未保留）', 'muted')
        : button(label, () => navigate(ref));
    }
    function versionCard(version, kind) {
      const versionCard = element('article', '', 'card memory-version');
      versionCard.append(element('h3', `版本 ${version.version_no} · ${kind === 'node' ? '版本生成于 ' : ''}${date(version.created_at)}`));
      if (kind === 'node') versionCard.append(element('p', `内容更新时间：${date(version.content_updated_at)}`, 'muted version-content-time'));
      if (kind === 'node') versionCard.append(element('h4', version.display_title || version.title || '未记录标题', 'version-title'));
      versionCard.append(kind === 'relationship' ? relationshipContent(version, 'h4') : element('p', prose(version) || '当前没有正文。', 'body'));
      if (kind === 'node') versionCard.append(emotionalContext(version));
      const tags = version.display_tags ?? version.tags;
      if (Array.isArray(tags) && tags.length) versionCard.append(element('p', '标签：' + tags.join('、'), 'muted'));
      if (version.state && version.state.format !== 'relationship.slots.v1' && Object.keys(version.state).length) {
        const structure = element('details'); structure.append(element('summary', '结构化关系信息'), element('pre', JSON.stringify(version.display_state ?? {}, null, 2))); versionCard.append(structure);
      }
      if (version.provenance_status !== 'recorded') {
        versionCard.append(element('p', '此版本未记录独立来源，无法确认本次引用。累计引用不能用来推断本版本来源。', 'notice'));
      }
      for (const entry of version.derivations || []) {
        const record = element('section', '', 'version-derivation');
        record.append(element('h4', `${operations[entry.operation] || entry.operation} · ${editors[entry.editor_kind] || entry.editor_kind} · 操作于 ${date(entry.created_at)}`));
        if (entry.operation === 'membership') {
          record.append(element('p', '本次仅关联成员，不代表主题正文已重新生成或完整覆盖成员。', 'muted'));
          const members = element('div', '', 'row');
          for (const member of entry.semantic?.member_versions || []) members.append(versionLink(member));
          record.append(members);
          if (entry.sources?.length) { const links = element('div'); sourceLinks(links, entry.sources); record.append(links); }
          if (entry.batch_id) record.append(element('p', '抽取批次：' + entry.batch_id, 'muted card-id'));
          versionCard.append(record); continue;
        }
        if (entry.operation === 'title_update') {
          const change = entry.title_change || {};
          record.append(element('p', '原题：' + (change.display_before ?? change.before ?? '未记录标题')), element('p', '新题：' + (change.display_after ?? change.after ?? '未记录标题')));
          if (entry.governance_job_id) record.append(element('p', '治理任务：' + entry.governance_job_id, 'muted card-id'));
          versionCard.append(record); continue;
        }
        record.append(element('p', '本次直接引用', 'task-title'));
        if (entry.sources?.length) { const links = element('div'); sourceLinks(links, entry.sources); record.append(links); }
        else record.append(element('p', '本次未新增原文引用。', 'muted'));
        if (entry.context_source_ids?.length) {
          record.append(element('p', '同窗上下文（不等同于直接证据）', 'task-title'));
          const links = element('div');
          sourceLinks(links, entry.context_source_ids.map(source_id => ({ source_id, source_kind: 'source_record' })));
          record.append(links);
        }
        if (entry.semantic && Object.keys(entry.semantic).length) {
          const declaration = element('details');
          declaration.append(element('summary', '模型的语义声明（未经事实认证）'), element('pre', JSON.stringify(entry.display_semantic ?? {}, null, 2)));
          record.append(declaration);
        }
        record.append(element('p', '继承的基线版本', 'task-title'));
        if (entry.base_versions?.length) {
          const bases = element('div', '', 'row');
          for (const base of entry.base_versions) bases.append(versionLink(base));
          record.append(bases);
        } else record.append(element('p', '无已记录的基线版本。', 'muted'));
        if (entry.batch_id) record.append(element('p', '抽取批次：' + entry.batch_id, 'muted card-id'));
        if (entry.governance_job_id) record.append(element('p', '治理任务：' + entry.governance_job_id, 'muted card-id'));
        versionCard.append(record);
      }
      return versionCard;
    }
    async function navigate(next, back = false) {
      if (next) { if (!back) history.push(current); current = target(next); }
      const request = ++navigation, selected = current;
      const key = JSON.stringify(selected);
      loading = true; loaded = false;
      body.replaceChildren(element('p', '正在读取版本和来源…', 'muted'));
      try {
        const result = cache.get(key) || await ui.bridge.apiGet('memory/versions', { ...selected, space_id: spaceId });
        if (!live() || request !== navigation) return;
        cache.set(key, result); loaded = true;
        body.replaceChildren();
        if (history.length) body.append(button('返回上一层查阅', () => navigate(history.pop(), true), 'memory-back'));
        body.append(element('p', `${kinds[selected.kind]} · ${selected.entity_id}`, 'muted card-id'));
        if (result.earliest_retained_version > 1) body.append(element('p', `当前可查阅的版本从 v${result.earliest_retained_version} 开始。`, 'notice'));
        body.append(...result.items.map(version => versionCard(version, selected.kind)));
        if (!result.items.length) body.append(element('p', '没有可查阅的版本。', 'muted'));
        if (result.sources?.length) {
          const sources = element('div', '', 'memory-sources');
          sources.append(element('h3', '节点累计引用'), element('p', '汇总该节点保留的来源关系，不表示这些来源被每个版本直接引用。', 'muted'));
          sourceLinks(sources, result.sources); body.append(sources);
        }
        if (result.participants?.length || result.edges?.length) {
          const related = element('details'); related.append(element('summary', '参与者与关联节点'), element('pre', JSON.stringify(result.display_relations ?? {}, null, 2))); body.append(related);
        }
      } catch {
        if (live() && request === navigation) {
          body.replaceChildren(element('p', '版本和来源读取失败。', 'error'), button('重试读取版本和来源', () => navigate()));
          if (history.length) body.append(button('返回上一层查阅', () => navigate(history.pop(), true)));
        }
      } finally {
        if (request === navigation) loading = false;
        // 基线导航替换了按钮，焦点回到本卡片入口，保持键盘操作位置。
        if (next && live() && request === navigation) { summary.focus({ preventScroll: true }); summary.scrollIntoView({ block: 'nearest' }); }
      }
    }
    panel.addEventListener('toggle', () => { if (panel.open && !loaded && !loading) navigate(); });
    return panel;
  }
  function topicRelations(item, card) {
    const revision = state.request, epoch = ui.epoch(), spaceId = ui.space();
    const members = item.node_type === 'topic';
    const panel = element('details', '', 'memory-relations');
    const summary = element('summary', members ? '展开主题成员' : '所属主题');
    const body = element('div', '', 'memory-relations-body');
    panel.append(summary, body);
    let loaded = false, loading = false, offset = 0;
    const live = () => revision === state.request && ui.current(epoch) && card.isConnected;
    async function read(nextOffset = 0) {
      if (loading || !live()) return;
      loading = true;
      body.replaceChildren(element('p', '正在读取关联记忆…', 'muted'));
      try {
        const result = await ui.bridge.apiGet('memory/relations', { space_id: spaceId, node_id: item.node_id, limit: 10, offset: nextOffset });
        if (!live()) return;
        offset = nextOffset; loaded = true;
        body.replaceChildren(...result.items.map(member => render(member, false)));
        if (!result.items.length) body.append(element('p', members ? '尚未记录主题成员。' : '尚未归入主题；独立经历可以单独保存。', 'muted'));
        const pages = element('div', '', 'row');
        if (offset) pages.append(button('上一页关联记忆', () => read(Math.max(0, offset - 10))));
        if (result.has_more) pages.append(button('下一页关联记忆', () => read(offset + 10)));
        body.append(pages);
      } catch {
        if (live()) body.replaceChildren(element('p', '关联记忆读取失败。', 'error'), button('重试读取关联记忆', () => read(offset)));
      } finally { loading = false; }
    }
    panel.addEventListener('toggle', () => { if (panel.open && !loaded) read(); });
    return panel;
  }
  function render(item, withRelations = true) {
    const card = element('article', '', 'card memory-card');
    const type = item.node_type || { profiles: '用户画像', relationships: '人格关系', short_term: '短期摘要' }[state.kind];
    const typeName = labels[type] || type;
    const conversationName = item.conversation_id?.startsWith('host.v1:') ? '' : item.conversation_id;
    const name = item.node_id ? item.display_title || item.title || `${typeName} · ${item.node_id.slice(0, 8)}` : `${typeName}${item.logical_user_id ? ' · ' + (item.logical_user_name || '未命名用户') : conversationName ? ' · ' + conversationName : ''}`;
    card.append(element('h2', name, 'memory-title'));
    const badges = element('div'), stateBadge = element('span', labels[item.status] || item.status || '活跃', 'badge');
    stateBadge.dataset.state = item.status || 'active';
    badges.append(element('span', typeName, 'badge'), stateBadge);
    if (item.manual_protected) { const badge = element('span', '人工保护', 'badge'); badge.dataset.state = 'protected'; badges.append(badge); }
    if (item.metadata?.summary_stale) { const badge = element('span', '概括待修订', 'badge'); badge.dataset.state = 'stale'; badges.append(badge); }
    card.append(badges);
    if (conversationName) card.append(element('p', '会话：' + conversationName, 'muted card-id'));
    card.append(item.relationship_id ? relationshipContent(item) : element('p', prose(item) || '当前没有正文。', 'body'));
    if (item.node_id) card.append(emotionalContext(item));
    const tags = item.display_tags ?? item.tags;
    if (Array.isArray(tags) && tags.length) card.append(element('p', '标签：' + tags.join('、'), 'muted'));
    if (item.coverage) card.append(element('p', `覆盖消息 ${item.coverage.source_count ?? '未知'} 条 · 截至 ${date(item.coverage.to_observed_at)}`, 'muted'));
    card.append(element('p', `更新时间：${date(item.node_id ? item.content_updated_at : item.updated_at)}` + (item.importance !== undefined ? ` · 重要性 ${item.importance}` : ''), 'muted memory-updated-at'));
    if (item.node_id && withRelations) card.append(topicRelations(item, card));
    if (item.node_id || item.profile_id || item.relationship_id) card.append(inspector(item, card));
    return card;
  }
  const updatePagination = initPagination('memory', async offset => {
    state.offset = offset;
    await refresh();
    $('memory-list').scrollIntoView({ block: 'start' });
  });
  const emotion = $('memory-emotion'), start = $('memory-date-from'), end = $('memory-date-to');
  const minimum = $('memory-importance-min'), maximum = $('memory-importance-max');
  const advanced = [emotion, start, end, minimum, maximum];
  for (const [tag, label] of Object.entries(emotionLabels)) emotion.add(new Option(label, tag));
  emotion.add(new Option('未记录情绪', 'none'));
  enhanceSelect(emotion);
  function setNodeFilters(args) {
    args.node_type = $('memory-type').value;
    args.sort = $('memory-sort').value;
    const activeCount = advanced.filter(input => input.value || !input.validity.valid).length;
    $('memory-filter-count').textContent = activeCount ? `（${activeCount}）` : '';
    $('memory-clear-advanced').disabled = activeCount === 0;
    if (start.validity.badInput || end.validity.badInput) return '请输入完整的内容更新日期。';
    if (start.value && end.value && start.valueAsNumber > end.valueAsNumber) return '开始日期不能晚于结束日期。';
    if (!minimum.validity.valid || !maximum.validity.valid) return '重要性须为 0 到 1 之间的数值。';
    if (minimum.valueAsNumber > maximum.valueAsNumber) return '最低重要性不能大于最高重要性。';
    if (emotion.value) args.emotion = emotion.value;
    if (minimum.value) args.importance_min = minimum.valueAsNumber;
    if (maximum.value) args.importance_max = maximum.valueAsNumber;
    if (start.value) args.updated_from = new Date(start.value + 'T00:00:00').getTime() / 1000;
    if (end.value) {
      const next = new Date(end.value + 'T00:00:00');
      next.setDate(next.getDate() + 1);
      args.updated_before = next.getTime() / 1000;
    }
    return '';
  }
  async function refresh() {
    const request = ++state.request, epoch = ui.epoch();
    updatePagination(state.offset);
    $('memory-list').replaceChildren();
    if (!ui.space()) { $('memory-list').append(emptyState('暂无记忆空间。指定角色的会话进入系统后会建立空间。')); $('memory-meta').textContent = ''; return; }
    $('memory-meta').textContent = '正在读取…';
    const args = { space_id: ui.space(), limit: PAGE_SIZE, offset: state.offset, q: $('memory-q').value.trim(), status: $('memory-status').value };
    if (state.kind === 'nodes') {
      const error = setNodeFilters(args);
      if (error) { $('memory-meta').textContent = error; return; }
    } else args.kind = state.kind;
    try {
      const result = await ui.bridge.apiGet(state.kind === 'nodes' ? 'memory/nodes' : 'memory/context', args);
      if (request !== state.request || !ui.current(epoch)) return;
      if (state.offset > 0 && state.offset >= result.total) {
        state.offset = Math.max(0, Math.ceil(result.total / PAGE_SIZE) - 1) * PAGE_SIZE;
        return refresh();
      }
      $('memory-list').replaceChildren(...result.items.map(item => render(item)));
      if (!result.items.length) $('memory-list').append(emptyState(state.kind === 'short_term' ? '当前条件下没有记录。短期摘要由实际会话的滚动任务生成；长期重构不会生成短期摘要。' : '当前条件下没有记录。'));
      $('memory-meta').textContent = `本页 ${result.items.length} 条 · 第 ${Math.floor(state.offset / PAGE_SIZE) + 1} 页`;
      updatePagination(state.offset, result);
    } catch (error) { if (request !== state.request || !ui.current(epoch)) return; $('memory-meta').textContent = '读取失败，可点击刷新重试。'; $('memory-list').replaceChildren(); notify(error.message || '记忆服务暂不可用。', true); }
  }
  const selectTab = setupTabs('[data-kind]', (tab, reload = true) => {
    state.kind = tab.dataset.kind; state.offset = 0; $('memory-type-label').hidden = state.kind !== 'nodes';
    $('memory-sort-label').hidden = $('memory-advanced').hidden = state.kind !== 'nodes';
    const statuses = state.kind === 'nodes' ? ['all','active','historical','disabled','superseded','deleted'] : state.kind === 'short_term' ? ['all','active','archived','disabled'] : ['all','active','disabled','historical'];
    $('memory-status').replaceChildren(...statuses.map((value) => { const option = element('option', labels[value] || '全部状态'); option.value = value; return option; }));
    enhanceSelect($('memory-status'));
    if (reload) refresh();
  });
  function applyFilters() { state.offset = 0; return refresh(); }
  onSearchForm('memory-filter', applyFilters);
  for (const id of ['memory-type', 'memory-status', 'memory-sort', ...advanced.map(input => input.id)]) on(id, 'change', applyFilters);
  on('memory-clear-advanced', 'click', () => {
    for (const input of advanced) input.value = '';
    enhanceSelect(emotion);
    return applyFilters();
  });
  function changed() {
    const relationships = document.querySelector('[data-kind="relationships"]');
    relationships.hidden = ui.spaceType() === 'group';
    if (relationships.hidden && state.kind === 'relationships') {
      selectTab(document.querySelector('[data-kind="nodes"]'), false);
    }
    state.request += 1;
    state.offset = 0;
    updatePagination(0);
    $('memory-list').replaceChildren();
  }
  return { refresh, changed };
}
