import { $, button, element } from './ui.js';

export const PAGE_SIZE = 30;

export function initPagination(prefix, navigate) {
  const trigger = $(prefix + '-page'), picker = $(prefix + '-picker');
  const previous = $(prefix + '-prev'), next = $(prefix + '-next');
  const options = picker.querySelector('.page-options'), input = picker.querySelector('input');
  let current = 1, last = 1;

  function close() {
    picker.hidden = true;
    trigger.setAttribute('aria-expanded', 'false');
  }

  async function select(page) {
    close();
    if (page !== current) await navigate((page - 1) * PAGE_SIZE);
    trigger.focus({ preventScroll: true });
  }

  trigger.addEventListener('click', () => {
    if (!picker.hidden) { close(); return; }
    const pages = new Set([1, last]);
    for (let page = Math.max(1, current - 2); page <= Math.min(last, current + 2); page++) pages.add(page);
    options.replaceChildren();
    let prior = 0;
    for (const page of [...pages].sort((a, b) => a - b)) {
      if (page > prior + 1) options.append(element('span', '…'));
      const choice = button(String(page), () => select(page));
      choice.setAttribute('aria-label', '第 ' + page + ' 页');
      if (page === current) choice.setAttribute('aria-current', 'page');
      options.append(choice);
      prior = page;
    }
    input.max = last;
    input.value = current;
    picker.querySelector('.page-count').textContent = '共 ' + last + ' 页';
    picker.hidden = false;
    trigger.setAttribute('aria-expanded', 'true');
    options.querySelector('[aria-current]').focus({ preventScroll: true });
  });
  picker.querySelector('form').addEventListener('submit', event => {
    event.preventDefault();
    select(input.valueAsNumber);
  });
  document.addEventListener('pointerdown', event => {
    if (!picker.hidden && !trigger.contains(event.target) && !picker.contains(event.target)) close();
  });
  trigger.parentElement.addEventListener('keydown', event => {
    if (event.key === 'Escape' && !picker.hidden) { close(); trigger.focus(); }
  });

  previous.addEventListener('click', () => select(current - 1));
  next.addEventListener('click', () => select(current + 1));
  return function update(offset, result) {
    close();
    current = Math.floor(offset / PAGE_SIZE) + 1;
    last = Math.max(1, Math.ceil((result?.total ?? 0) / PAGE_SIZE));
    trigger.textContent = '第 ' + current + ' 页';
    trigger.disabled = !result || last <= 1;
    previous.disabled = !result || current === 1;
    next.disabled = !result?.has_more;
  };
}
