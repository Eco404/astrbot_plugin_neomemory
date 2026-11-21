import { $, button, enhanceSelect, on } from './ui.js';

const defaults = { mode: 'system', hue: 168, saturation: 38, lightness: 50, opacity: 68 };
const colors = [[168, '青绿'], [130, '叶绿'], [195, '湖蓝'], [216, '雾蓝'], [345, '蔷薇'], [35, '麦金']];

function rgbToHsl(r, g, b, previousHue) {
  const high = Math.max(r, g, b), low = Math.min(r, g, b), delta = high - low;
  const l = (high + low) / 2;
  let hue = previousHue;
  if (delta) {
    const sector = high === r ? (g - b) / delta : high === g ? (b - r) / delta + 2 : (r - g) / delta + 4;
    hue = (sector * 60 + 360) % 360;
  }
  return { hue, saturation: delta ? 100 * delta / (1 - Math.abs(2 * l - 1)) : 0, lightness: l * 100 };
}

// WCAG relative luminance for a selected HSL color on the light/dark selection surface.
function luminance(h, s, l) {
  const light = l / 100, chroma = (1 - Math.abs(2 * light - 1)) * s / 100;
  const channels = [0, 8, 4].map(offset => {
    const k = (offset + h / 30) % 12;
    const channel = light - chroma / 2 * Math.max(-1, Math.min(1, k - 3, 9 - k));
    return channel <= .04045 ? channel / 12.92 : ((channel + .055) / 1.055) ** 2.4;
  });
  return .2126 * channels[0] + .7152 * channels[1] + .0722 * channels[2];
}

export function initAppearance(ui, changed) {
  let values = { ...defaults }, saved = { ...defaults };
  const scheme = matchMedia('(prefers-color-scheme: dark)');
  const canvas = $('appearance-canvas');
  const hexInput = $('appearance-hex');
  const root = document.documentElement;
  const different = baseline => Object.keys(defaults).some(key => values[key] !== baseline[key]);
  function paint() {
    const { mode, hue, saturation, lightness, opacity } = values;
    const dark = mode === 'system' ? scheme.matches : mode === 'dark';
    root.dataset.memoryTheme = dark ? 'dark' : 'light';
    const raw = 'hsl(' + hue + ' ' + saturation + '% ' + lightness + '%)';
    const background = luminance(hue, dark ? 18 : saturation, dark ? 30 : 85);
    let accent = dark ? Math.max(62, lightness) : Math.min(30, lightness);
    while (accent > 0 && accent < 100) {
      const foreground = luminance(hue, saturation, accent);
      if ((Math.max(background, foreground) + .05) / (Math.min(background, foreground) + .05) >= 4.6) break;
      accent += dark ? 1 : -1;
    }
    root.style.setProperty('--theme-h', hue);
    root.style.setProperty('--theme-s', saturation + '%');
    root.style.setProperty('--accent-l', accent + '%');
    root.style.setProperty('--strong-l', (dark ? Math.max(72, accent) : Math.min(26, accent)) + '%');
    root.style.setProperty('--glass-opacity', opacity + '%');
    $('appearance-mode').value = mode;
    const context = document.createElement('canvas').getContext('2d');
    context.fillStyle = raw;
    $('appearance-color').value = hexInput.value = context.fillStyle;
    hexInput.setCustomValidity('');
    for (const key of ['saturation', 'opacity']) {
      $('appearance-' + key).value = Math.round(values[key]);
      $('appearance-' + key + '-value').value = Math.round(values[key]) + '%';
    }
    for (const swatch of $('appearance-swatches').children) {
      swatch.setAttribute('aria-pressed', String(+swatch.dataset.hue === hue && lightness === 50));
    }
    $('appearance-reset').disabled = !different(defaults);
  }
  function update(patch) { Object.assign(values, patch); paint(); changed(); }
  for (const [hue, label] of colors) {
    const swatch = button(label, () => update({ hue, lightness: 50 }), 'theme-swatch');
    swatch.style.setProperty('--swatch', 'hsl(' + hue + ' 42% 50%)');
    swatch.dataset.hue = hue;
    $('appearance-swatches').append(swatch);
  }
  function setHex(input) {
    let hex = input.trim().replace(/^#/, '');
    if (/^[0-9a-f]{3}$/i.test(hex)) hex = Array.from(hex, digit => digit + digit).join('');
    if (!/^[0-9a-f]{6}$/i.test(hex)) {
      hexInput.setCustomValidity('请输入三位或六位 HEX 色值。');
      hexInput.reportValidity();
      return;
    }
    update(rgbToHsl(...hex.match(/../g).map(pair => parseInt(pair, 16) / 255), values.hue));
  }
  on('appearance-color', 'input', () => setHex($('appearance-color').value));
  on('appearance-hex', 'change', () => setHex(hexInput.value));
  on('appearance-mode', 'input', () => update({ mode: $('appearance-mode').value }));
  for (const key of ['saturation', 'opacity']) on('appearance-' + key, 'input', () => update({ [key]: +$('appearance-' + key).value }));
  on('appearance-reset', 'click', () => update(defaults));
  on('appearance-pick', 'click', async () => {
    if (window.EyeDropper && window.isSecureContext) {
      try { setHex((await new EyeDropper().open()).sRGBHex); return; }
      catch (error) { if (error.name === 'AbortError') return; }
    }
    $('appearance-file').click();
  });
  on('appearance-file', 'change', async () => {
    const file = $('appearance-file').files[0];
    if (!file) return;
    const bitmap = await createImageBitmap(file);
    const scale = Math.min(1, 900 / Math.max(bitmap.width, bitmap.height));
    canvas.width = Math.max(1, Math.round(bitmap.width * scale));
    canvas.height = Math.max(1, Math.round(bitmap.height * scale));
    canvas.getContext('2d').drawImage(bitmap, 0, 0, canvas.width, canvas.height);
    bitmap.close(); $('appearance-file').value = ''; canvas.hidden = false; canvas.focus();
  });
  on('appearance-canvas', 'click', event => {
    const rect = canvas.getBoundingClientRect();
    const x = Math.floor((event.clientX - rect.left) * canvas.width / rect.width);
    const y = Math.floor((event.clientY - rect.top) * canvas.height / rect.height);
    const [r, g, b] = canvas.getContext('2d').getImageData(x, y, 1, 1).data;
    update(rgbToHsl(r / 255, g / 255, b / 255, values.hue));
    canvas.hidden = true;
  });
  scheme.addEventListener('change', paint);
  paint();
  enhanceSelect($('appearance-mode'));
  async function load() {
    const stored = await ui.bridge.apiGet('appearance');
    values = { ...defaults, ...stored }; saved = { ...values }; paint();
  }
  async function save() {
    const snapshot = { ...values };
    await ui.write('appearance', snapshot);
    const stored = await ui.bridge.apiGet('appearance');
    if (Object.keys(defaults).some(key => stored[key] !== snapshot[key])) throw new Error('浏览器未保存主题，请检查 Cookie 设置。');
    saved = snapshot;
  }
  return { load, save, isDirty: () => different(saved), isDefault: () => !different(defaults),
    reset() { update(defaults); }, discard() { canvas.hidden = true; update(saved); } };
}
