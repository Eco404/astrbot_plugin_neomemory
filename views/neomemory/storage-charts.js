import { $, element } from './ui.js';

const databases = {
  'source.db': '保存会话原文、外部事件、账号归属及原文来源。',
  'memory.db': '保存长期记忆、用户画像、人格关系、短期摘要、笔记，以及后台任务和历史版本。',
  'cache.db': '缓存消息和记忆节点的向量，供索引与召回复用。',
  'ingress.db': '暂存等待写入原文库的记录，包含排队、失败与重试状态。'
};
const colors = ['var(--accent)', '#668aba', '#ca9061', '#a57cad'];
function color(index) {
  return colors[index] || `hsl(${Math.round(index * 137.508) % 360} 42% 56%)`;
}

export function bytes(count = 0) {
  for (const [unit, divisor] of [['GiB', 2 ** 30], ['MiB', 2 ** 20], ['KiB', 1024]]) {
    if (count >= divisor) return (count / divisor).toFixed(1) + ' ' + unit;
  }
  return count + ' B';
}

function svgNode(name, attrs) {
  const node = document.createElementNS('http://www.w3.org/2000/svg', name);
  for (const [attr, value] of Object.entries(attrs)) node.setAttribute(attr, value);
  return node;
}

function segment(start, end) {
  const attrs = { fill: 'none', 'stroke-width': 17 };
  if (end - start === 1) return svgNode('circle', { ...attrs, cx: 60, cy: 60, r: 49 });
  function point(turn) {
    const angle = turn * 2 * Math.PI - Math.PI / 2;
    return [60 + 49 * Math.cos(angle), 60 + 49 * Math.sin(angle)].join(' ');
  }
  return svgNode('path', { ...attrs, d: `M ${point(start)} A 49 49 0 ${end - start > .5 ? 1 : 0} 1 ${point(end)}` });
}

function renderChart(container, entries, { showSizes }) {
  const total = entries.reduce((sum, item) => sum + item.size, 0);
  const layout = element('div', '', 'storage-plot');
  const ring = element('div', '', 'storage-ring');
  const svg = svgNode('svg', { viewBox: '0 0 120 120', 'aria-hidden': 'true' });
  svg.append(svgNode('circle', { cx: 60, cy: 60, r: 49, fill: 'none', stroke: 'var(--line-soft)', 'stroke-width': 17 }));
  ring.append(svg);
  if (showSizes) {
    const center = element('div', '', 'storage-center');
    center.append(element('strong', bytes(total)), element('small', '磁盘合计'));
    ring.append(center);
  }
  const legend = element('div', '', 'storage-legend');
  const tip = element('div', '', 'storage-tip');
  tip.setAttribute('role', 'tooltip');
  tip.hidden = true;
  let used = 0;
  for (const [index, item] of entries.entries()) {
    const share = total ? item.size / total : 0;
    const percent = share > 0 && share < .001 ? '<0.1%' : (100 * share).toLocaleString('zh-CN', { maximumFractionDigits: 1 }) + '%';
    const legendValue = showSizes ? bytes(item.size) : percent;
    const text = showSizes ? legendValue + ' · ' + percent : percent;
    const button = element('button', '', 'storage-key');
    button.type = 'button';
    button.setAttribute('aria-label', [item.name, text, item.description].filter(Boolean).join('，'));
    const marker = element('span', '', 'storage-dot');
    marker.style.backgroundColor = color(index);
    button.append(marker, element('span', item.name), element('small', legendValue));
    legend.append(button);

    function show() {
      tip.replaceChildren(element('strong', item.name), element('span', text));
      if (item.description) tip.append(element('span', item.description));
      tip.hidden = false;
    }
    const hide = () => { tip.hidden = true; };
    for (const event of ['pointerenter', 'focus', 'click']) button.addEventListener(event, show);
    for (const event of ['pointerleave', 'blur']) button.addEventListener(event, hide);
    if (item.size > 0) {
      const arc = segment(used / total, (used + item.size) / total);
      used += item.size;
      arc.setAttribute('stroke', color(index));
      arc.addEventListener('pointerenter', show);
      arc.addEventListener('pointerleave', hide);
      arc.addEventListener('click', () => { button.focus({ preventScroll: true }); show(); });
      svg.append(arc);
    }
  }
  layout.addEventListener('keydown', event => { if (event.key === 'Escape') tip.hidden = true; });
  layout.append(ring, legend, tip);
  container.replaceChildren(layout);
}

export function initStorageCharts(bridge) {
  const button = $('calculate-space-usage'), status = $('space-usage-status');
  let lastSizes;
  button.addEventListener('click', async () => {
    button.disabled = true;
    status.textContent = '正在统计所有记忆空间…';
    try {
      const { items } = await bridge.apiGet('storage/spaces');
      const entries = items.map(item => ({ name: item.display_label, size: item.size_bytes }));
      renderChart($('storage-spaces'), entries, { showSizes: false });
      button.textContent = '重新计算';
      const time = new Date().toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
      status.textContent = entries.some(item => item.size)
        ? `计算于 ${time}`
        : '暂无可归属到记忆空间的内容。';
    } catch {
      status.textContent = '本次计算失败，请重新计算。';
    } finally { button.disabled = false; }
  });
  return {
    databases(sizes) {
      const key = JSON.stringify(sizes || {});
      if (lastSizes === key) return;
      lastSizes = key;
      const entries = Object.entries(sizes || {}).map(([name, size]) => ({ name, size, description: databases[name] }));
      if (entries.length) renderChart($('storage-usage'), entries, { showSizes: true });
      else $('storage-usage').replaceChildren(element('p', '暂无存储统计。', 'muted'));
    }
  };
}
