// Small local outline icons; no font, CDN, or runtime asset request.
const paths = {
  storage: ['M20 6c0 2-3.6 3-8 3S4 8 4 6s3.6-3 8-3 8 1 8 3Z', 'M4 6v12c0 2 3.6 3 8 3s8-1 8-3V6', 'M4 12c0 2 3.6 3 8 3s8-1 8-3'],
  layers: ['M12 3 3 8l9 5 9-5-9-5Z', 'm3 12 9 5 9-5', 'm3 16 9 5 9-5'],
  memory: ['M4 5h6a2 2 0 0 1 2 2v14a3 3 0 0 0-3-3H4V5Z', 'M20 5h-6a2 2 0 0 0-2 2v14a3 3 0 0 1 3-3h5V5Z'],
  notes: ['M14 3H5v18h14V8l-5-5Z', 'M14 3v5h5', 'M8 12h8', 'M8 16h5'],
  models: ['M8 3v3m8-3v3M8 18v3m8-3v3M3 8h3m-3 8h3m12-8h3m-3 8h3', 'M6 6h12v12H6z', 'M10 10h4v4h-4z'],
  settings: ['M4 7h9m4 0h3M4 17h3m4 0h9', 'M13 4h4v6h-4z', 'M7 14h4v6H7z'],
  maintenance: ['M3 12h4l3-8 4 16 3-8h4'],
  pipette: ['m15 7 2 2 3-3a2.1 2.1 0 0 0-3-3l-3 3', 'm12 5 7 7', 'm14 8-9 9-1 4 4-1 9-9'],
  refresh: ['M20 4v5h-5', 'M4 20v-5h5', 'M4.7 9a8 8 0 0 1 13-3L20 9', 'M19.3 15a8 8 0 0 1-13 3L4 15'],
  space: ['M4 4h6v6H4z', 'M14 4h6v6h-6z', 'M4 14h6v6H4z', 'M14 14h6v6h-6z'],
  search: ['M15.5 15.5 21 21', 'M17 10a7 7 0 1 1-14 0 7 7 0 0 1 14 0Z'],
  arrowLeft: ['m12 5-7 7 7 7', 'M5 12h14'],
  arrowRight: ['m12 5 7 7-7 7', 'M5 12h14'],
  clock: ['M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z', 'M12 7v5l3 2'],
};
export function icon(name) {
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.setAttribute('viewBox', '0 0 24 24');
  svg.setAttribute('fill', 'none');
  svg.setAttribute('stroke', 'currentColor');
  svg.setAttribute('stroke-width', '1.7');
  svg.setAttribute('stroke-linecap', 'round');
  svg.setAttribute('stroke-linejoin', 'round');
  svg.setAttribute('aria-hidden', 'true');
  svg.classList.add('icon');
  for (const d of paths[name] || []) {
    const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    path.setAttribute('d', d); svg.append(path);
  }
  return svg;
}
export function mountIcons() {
  for (const node of document.querySelectorAll('[data-icon]')) node.replaceChildren(icon(node.dataset.icon));
}
