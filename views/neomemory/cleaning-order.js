export function rootOrder(config) {
  return config.root_order ?? [
    ...config.groups.map(group => ({ kind: 'group', id: group.id })),
    ...config.rules.filter(rule => rule.group_id === null).map(rule => ({ kind: 'rule', id: rule.id }))
  ];
}

// Keep unaffected rows in place when an editor creates, moves or deletes an item.
export function editedRootOrder(base, next, item, collection) {
  const order = rootOrder(base).flatMap(ref => {
    if (ref.kind === 'group' && !next.groups.some(group => group.id === ref.id)) {
      return base.rules.filter(rule => rule.group_id === ref.id &&
        next.rules.some(entry => entry.id === rule.id && entry.group_id === null))
        .map(rule => ({kind: 'rule', id: rule.id}));
    }
    if (ref.kind === 'rule' && !next.rules.some(rule => rule.id === ref.id && rule.group_id === null)) return [];
    return [ref];
  });
  for (const group of next.groups) {
    if (!order.some(ref => ref.kind === 'group' && ref.id === group.id)) order.push({kind: 'group', id: group.id});
  }
  const changedPosition = item && collection === 'rules' &&
    next.rules.findIndex(rule => rule.id === item.id) !== base.rules.indexOf(item);
  for (const [i, rule] of next.rules.entries()) {
    if (rule.group_id !== null) continue;
    const at = order.findIndex(ref => ref.kind === 'rule' && ref.id === rule.id);
    if (at >= 0) {
      if (!changedPosition || rule.id !== item.id) continue;
      order.splice(at, 1);
    }
    const after = next.rules[i + 1];
    const anchor = after ? order.findIndex(ref => after.group_id === null
      ? ref.kind === 'rule' && ref.id === after.id : ref.kind === 'group' && ref.id === after.group_id) : -1;
    order.splice(anchor < 0 ? order.length : anchor, 0, {kind: 'rule', id: rule.id});
  }
  return order;
}

export function installCleaningDrag(list, endZone, reordered) {
  let drag = null;
  function clearTarget() {
    if (drag.target) delete drag.target.node.dataset.drop;
    drag.target = null;
  }
  function targetAtPointer() {
    clearTarget();
    const hit = document.elementFromPoint(drag.x, drag.y);
    let node = hit?.closest('.cleaning-row');
    if (hit === endZone) { drag.target = {node: endZone, side: 'end'}; }
    else if (node && list.contains(node) && !drag.node.contains(node)) {
      const bounds = node.getBoundingClientRect();
      const fraction = (drag.y - bounds.top) / bounds.height;
      let side = fraction < .5 ? 'before' : 'after';
      const group = node.closest('.cleaning-group');
      if (node.dataset.collection === 'groups') {
        if (!drag.group && fraction >= .25 && fraction <= .75) side = 'inside';
        node = group;
      } else if (drag.group && group) {
        node = group;
        side = drag.y < (group.getBoundingClientRect().top + group.getBoundingClientRect().bottom) / 2 ? 'before' : 'after';
      }
      drag.target = {node, side};
    }
    if (drag.target) drag.target.node.dataset.drop = drag.target.side;
  }
  function scroll() {
    if (!drag) return;
    const edge = 64;
    const dy = drag.y < edge ? -Math.ceil((edge - drag.y) / 5)
      : drag.y > innerHeight - edge ? Math.ceil((drag.y - innerHeight + edge) / 5) : 0;
    if (dy) { window.scrollBy(0, dy); targetAtPointer(); }
    drag.frame = requestAnimationFrame(scroll);
  }
  list.addEventListener('pointerdown', event => {
    const handle = event.target.closest('.cleaning-grip');
    if (!handle || drag || event.button !== 0) return;
    const row = handle.closest('.cleaning-row');
    const group = row.dataset.collection === 'groups';
    drag = {node: group ? row.parentNode : row, group, id: event.pointerId, x:event.clientX, y:event.clientY};
    handle.setPointerCapture(event.pointerId);
    drag.node.classList.add('cleaning-dragging');
    endZone.hidden = false;
    drag.frame = requestAnimationFrame(scroll);
  });
  list.addEventListener('pointermove', event => {
    if (drag?.id !== event.pointerId) return;
    drag.x = event.clientX; drag.y = event.clientY;
    targetAtPointer();
  });
  function finish(event) {
    if (drag?.id !== event.pointerId) return;
    if (event.type === 'pointerup') {
      drag.x = event.clientX; drag.y = event.clientY;
      targetAtPointer();
    }
    const {node, target} = drag;
    cancelAnimationFrame(drag.frame);
    clearTarget();
    drag = null;
    node.classList.remove('cleaning-dragging');
    endZone.hidden = true;
    if (!target || event.type !== 'pointerup') return;
    if (target.side === 'inside') target.node.querySelector('.cleaning-members').append(node);
    else if (target.side === 'end') list.append(node);
    else if (target.side === 'before') target.node.before(node);
    else target.node.after(node);
    reordered();
  }
  for (const name of ['pointerup', 'pointercancel', 'lostpointercapture']) list.addEventListener(name, finish);
}
