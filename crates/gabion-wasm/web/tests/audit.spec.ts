import { inflateSync } from 'node:zlib';
import { expect, test, type Locator, type Page } from '@playwright/test';
import { BAND_ALPHA, BEAM_ALPHA, readTheme, type Theme } from '../src/lib/theme';

// The automated half of the audit checklist in `../README.md` ("Design rules"):
// A1, A2, A3 (DOM text), A7 and A8. Each audit project fixes a width and a
// colour scheme; this walks every preset to a fixed virtual time with the
// transport paused, checks it, then checks it again with `data-theme` forcing
// the other theme. Full-page PNGs land in `audit-shots/` for the visual audit.

const STEPS = 10;

type Findings = Record<'A1' | 'A2' | 'A3' | 'A7' | 'A8', string[]>;

test('every preset passes the automated design audit', async ({ page }, info) => {
  test.setTimeout(180_000);
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  page.on('console', (message) => {
    if (message.type() === 'error') errors.push(message.text());
  });
  page.on('requestfailed', (request) => errors.push(`request failed: ${request.url()}`));
  page.on('response', (response) => {
    if (response.status() >= 400) errors.push(`HTTP ${response.status()}: ${response.url()}`);
  });

  await page.goto('/');
  const canvas = page.locator('.stage canvas');
  await expect(canvas).toBeVisible({ timeout: 30_000 });
  // Below 640 px the rail is a closed "Controls" disclosure; it stays open across rebuilds.
  const controls = page.locator('.controls > summary');
  if (await controls.isVisible()) await controls.click();

  const forced = info.project.use.colorScheme === 'dark' ? 'light' : 'dark';
  const presets = page.getByRole('group', { name: 'Scenario preset' }).getByRole('button');
  const count = await presets.count();
  expect(count, 'no presets to audit').toBeGreaterThan(0);
  let converged = 0;
  for (let i = 0; i < count; i++) {
    const preset = presets.nth(i);
    const name = (await preset.innerText()).trim();
    // The rail remounts only once a rebuild has finished, so its aria-current
    // marks the new cluster as live (a stale tick readout could not).
    if ((await preset.getAttribute('aria-current')) !== 'true') await preset.click();
    await expect(preset).toHaveAttribute('aria-current', 'true');
    await expect(canvas).toBeVisible();
    for (let s = 0; s < STEPS; s++) {
      await page.getByRole('button', { name: 'Step forward one tick' }).click();
    }
    await expect(
      page.locator('.readout-item', { hasText: 'tick' }).locator('.readout-value'),
    ).toHaveText(String(STEPS));
    converged += await page.locator('.headline.converged').count();

    let unforced: Theme | null = null;
    for (const theme of [null, forced]) {
      // Colour transitions (hover fades) start on the theme flip, once styles are
      // recalculated; wait them out so the audit never reads a half-way colour.
      await page.evaluate(async (t) => {
        if (t === null) delete document.documentElement.dataset.theme;
        else document.documentElement.dataset.theme = t;
        getComputedStyle(document.documentElement).color;
        await Promise.all(document.getAnimations().map((animation) => animation.finished));
      }, theme);
      const colours = await page.evaluate(readTheme);
      const where = `${name}, data-theme ${theme ?? 'unset'}`;
      const slug = name.toLowerCase().replace(/[^a-z0-9]+/g, '-');
      await page.screenshot({
        path: `audit-shots/${info.project.name}/${slug}-${theme ?? 'scheme'}.png`,
        fullPage: true,
      });
      if (unforced === null) unforced = colours;
      else expect(colours.bg, `${where}: data-theme did not switch the theme`).not.toBe(unforced.bg);
      const findings = await page.evaluate(audit, { theme: colours, beam: BEAM_ALPHA, band: BAND_ALPHA });
      expect(findings, where).toEqual({
        A1: [],
        A2: [],
        A3: [],
        A7: [],
        A8: [],
      });
      await expect
        .poll(() => stagePixel(page, canvas), {
          message: `${where}: the stage does not draw readTheme().bg (A7)`,
        })
        .toBe(colours.bg);
    }
  }
  // Text shown only once every node agrees (the converged badge) is checked too.
  expect(converged, 'no preset was audited in a converged state (A3)').toBeGreaterThan(0);
  expect(errors, 'page errors, console errors or failed requests (A1)').toEqual([]);
});

/** The stage's pixel 4 px in from its top-left corner, as `0xRRGGBB`: bare
 *  background, and clear of the fractional edge where a neighbour's border can
 *  bleed in. A 1 × 1 PNG stores its one pixel as is under every filter, so
 *  inflating the IDAT data is enough. */
async function stagePixel(page: Page, stage: Locator): Promise<number> {
  await stage.scrollIntoViewIfNeeded();
  const box = await stage.boundingBox();
  if (box === null) throw new Error('the stage has no box to read a pixel from');
  const png = await page.screenshot({
    clip: { x: box.x + 4, y: box.y + 4, width: 1, height: 1 },
  });
  const idat: Buffer[] = [];
  for (let at = 8; at < png.length; at += 12 + png.readUInt32BE(at)) {
    if (png.toString('latin1', at + 4, at + 8) === 'IDAT') {
      idat.push(png.subarray(at + 8, at + 8 + png.readUInt32BE(at)));
    }
  }
  const [, r, g, b] = inflateSync(Buffer.concat(idat));
  return (r << 16) | (g << 8) | b;
}

/** Runs in the page: findings for A1 (sideways scroll), A2, A3 (DOM text), A7 and
 *  A8, one sentence each, given the canvas colours and the alphas of the
 *  translucent canvas marks. Self-contained: Playwright ships it to the page as source. */
function audit({ theme, beam, band }: { theme: Theme; beam: number; band: number }): Findings {
  type Rgba = [number, number, number, number];
  type Box = { left: number; right: number; top: number; bottom: number };
  const found: Findings = { A1: [], A2: [], A3: [], A7: [], A8: [] };

  // Colour maths: paint a CSS colour into one pixel, so no colour string is parsed by hand.
  const ctx = new OffscreenCanvas(1, 1).getContext('2d', { willReadFrequently: true });
  if (ctx === null) return { ...found, A7: ['no 2D canvas to read colours with'] };
  const paint = (css: string): Rgba => {
    ctx.clearRect(0, 0, 1, 1);
    ctx.fillStyle = css;
    ctx.fillRect(0, 0, 1, 1);
    const [r, g, b, a] = ctx.getImageData(0, 0, 1, 1).data;
    return [r, g, b, a / 255];
  };
  const unpack = (rgb: number, alpha = 1): Rgba => [(rgb >> 16) & 0xff, (rgb >> 8) & 0xff, rgb & 0xff, alpha];
  const pack = ([r, g, b]: Rgba): number => (r << 16) | (g << 8) | b;
  const over = (top: Rgba, under: Rgba): Rgba => {
    const mix = (i: number): number => top[3] * top[i] + (1 - top[3]) * under[i];
    return [mix(0), mix(1), mix(2), 1];
  };
  const luminance = ([r, g, b]: Rgba): number => {
    const linear = (v: number): number => {
      const c = v / 255;
      return c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
    };
    return 0.2126 * linear(r) + 0.7152 * linear(g) + 0.0722 * linear(b);
  };
  const contrast = (a: Rgba, b: Rgba): number => {
    const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x);
    return (hi + 0.05) / (lo + 0.05);
  };
  /** The colour behind `el`: its ancestors' backgrounds up to the first opaque one. */
  const backdrop = (el: Element): Rgba => {
    const layers: Rgba[] = [];
    for (let e: Element | null = el; e !== null; e = e.parentElement) {
      const layer = paint(getComputedStyle(e).backgroundColor);
      if (layer[3] > 0) layers.push(layer);
      if (layer[3] >= 1) break;
    }
    return layers.reduceRight((under, layer) => over(layer, under), [255, 255, 255, 1] as Rgba);
  };
  const label = (text: string | null): string => (text ?? '').trim().replace(/\s+/g, ' ').slice(0, 40);

  // A1: no sideways page scroll.
  const root = document.documentElement;
  if (root.scrollWidth > innerWidth) {
    found.A1.push(`the page scrolls sideways: ${root.scrollWidth} px wide in a ${innerWidth} px window`);
  }

  // Text walk: A3 clipping, A3 overlaps (boxes collected here) and A7 text contrast.
  const boxes: { text: Text; box: Box }[] = [];
  const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  for (let node = walker.nextNode(); node !== null; node = walker.nextNode()) {
    const text = node as Text;
    const el = text.parentElement;
    // `select` option text is not laid out; `el.checkVisibility` drops hidden text.
    if (el === null || (text.textContent ?? '').trim() === '' || el.closest('select')) continue;
    if (!el.checkVisibility({ opacityProperty: true, visibilityProperty: true })) continue;
    const name = label(text.textContent);
    const range = document.createRange();
    range.selectNodeContents(text);
    const lineHeight = parseFloat(getComputedStyle(el).lineHeight);
    let cutOff = false;
    for (const rect of range.getClientRects()) {
      if (rect.width <= 0 || rect.height <= 0) continue;
      // A glyph box taller than a `line-height: 1` line box would overlap its
      // neighbours by leading alone, so trim it to the line box.
      const inset = lineHeight > 0 && rect.height > lineHeight ? (rect.height - lineHeight) / 2 : 0;
      let box: Box = {
        left: rect.left,
        right: rect.right,
        top: rect.top + inset,
        bottom: rect.bottom - inset,
      };
      // A scroller brings text back into view, so only a hidden box before the
      // first scroller counts as clipping; text a scroller has scrolled away is
      // trimmed from the overlap check.
      let scrolls = false;
      for (let a: Element | null = el; a !== null && a !== document.body; a = a.parentElement) {
        const style = getComputedStyle(a);
        const overflow = `${style.overflowX} ${style.overflowY}`;
        if (overflow === 'visible visible') continue;
        const clip = a.getBoundingClientRect();
        if (
          !scrolls &&
          /hidden|clip/.test(overflow) &&
          (box.left < clip.left - 1 ||
            box.right > clip.right + 1 ||
            box.top < clip.top - 1 ||
            box.bottom > clip.bottom + 1)
        ) {
          cutOff = true;
        }
        if (/auto|scroll/.test(overflow)) scrolls = true;
        box = {
          left: Math.max(box.left, clip.left),
          right: Math.min(box.right, clip.right),
          top: Math.max(box.top, clip.top),
          bottom: Math.min(box.bottom, clip.bottom),
        };
      }
      if (box.right - box.left >= 1 && box.bottom - box.top >= 1) boxes.push({ text, box });
    }
    if (cutOff) found.A3.push(`"${name}" is cut off by an overflow: hidden box`);

    // A7 text contrast. Disabled controls are exempt (WCAG 2.5.8's own exception);
    // the stage labels sit on canvas discs and are checked as the `--color-fg on
    // the stage` pair below.
    if (el.closest(':disabled, .stage-labels') === null) {
      let opacity = 1;
      for (let e: Element | null = el; e !== null; e = e.parentElement) {
        opacity *= Number(getComputedStyle(e).opacity);
      }
      const ink = paint(getComputedStyle(el).color);
      ink[3] *= opacity;
      const under = backdrop(el);
      const ratio = contrast(over(ink, under), under);
      if (ratio < 4.5) found.A7.push(`"${name}" has ${ratio.toFixed(2)}:1 contrast, below 4.5:1`);
    }
  }

  // A3 overlaps: two text nodes whose boxes share at least 1 px both ways.
  for (let i = 0; i < boxes.length; i++) {
    for (let j = i + 1; j < boxes.length; j++) {
      const [a, b] = [boxes[i], boxes[j]];
      if (a.text === b.text) continue;
      const w = Math.min(a.box.right, b.box.right) - Math.max(a.box.left, b.box.left);
      const h = Math.min(a.box.bottom, b.box.bottom) - Math.max(a.box.top, b.box.top);
      if (w >= 1 && h >= 1) {
        found.A3.push(
          `"${label(a.text.textContent)}" overlaps "${label(b.text.textContent)}" by ${w.toFixed(1)} × ${h.toFixed(1)} px`,
        );
      }
    }
  }

  // A7 targets: at least 24 × 24 px. The exemptions are WCAG 2.5.8's: targets
  // inline in a sentence (the `InfoTip` terms), disabled controls, and the stage
  // `×`, whose function the rail's Remove control repeats at full size.
  for (const el of document.querySelectorAll('a[href], button, input, select, summary')) {
    if (!el.checkVisibility() || el.matches(':disabled, .node-delete')) continue;
    if (getComputedStyle(el).display === 'inline') continue;
    const { width, height } = el.getBoundingClientRect();
    if (width < 24 || height < 24) {
      const name = label(el.getAttribute('aria-label') ?? el.textContent);
      found.A7.push(`the target "${name}" is ${width.toFixed(0)} × ${height.toFixed(0)} px, below 24 × 24`);
    }
  }

  // A7: readTheme() agrees with what the page paints.
  const rootStyle = getComputedStyle(root);
  if (pack(paint(rootStyle.backgroundColor)) !== theme.bg) {
    found.A7.push('readTheme().bg is not the page background');
  }
  if (pack(paint(rootStyle.color)) !== theme.fg) found.A7.push('readTheme().fg is not the page text colour');

  // A7 canvas pairs (graphics 3:1, text 4.5:1): the stage draws on `readTheme().bg`
  // and the charts on the detail rail's panel, so each is checked on its own
  // surface, a translucent mark composited over it first.
  const check = (surface: Rgba, where: string, pairs: [string, Rgba, number][]): void => {
    for (const [token, colour, minimum] of pairs) {
      const ratio = contrast(over(colour, surface), surface);
      if (ratio < minimum) {
        found.A7.push(`${token} on the ${where} has ${ratio.toFixed(2)}:1 contrast, below ${minimum}:1`);
      }
    }
  };
  check(unpack(theme.bg), 'stage', [
    ['--color-fg', unpack(theme.fg), 4.5],
    ['--chart-muted beam', unpack(theme.muted, beam), 3],
    ['--color-ok', unpack(theme.ok), 3],
    ['--color-warn', unpack(theme.warn), 3],
  ]);
  const plot = document.querySelector('.plot');
  if (plot === null) found.A7.push('no chart to check the chart colours against');
  else {
    const panel = backdrop(plot);
    check(panel, 'charts', [
      ['--color-fg', unpack(theme.fg), 4.5],
      ['--color-fg-muted', unpack(theme.fgMuted), 4.5],
      ['--color-warn', unpack(theme.warn), 4.5],
      ['--chart-muted', unpack(theme.muted), 3],
    ]);
    // "REJECTING" is warn text drawn inside its own translucent warn band.
    const warnBand = over(unpack(theme.warn, band), panel);
    check(warnBand, 'REJECTING band', [['--color-warn', unpack(theme.warn), 4.5]]);
  }

  // A2 and A8: page-text scans. "CI" is case-sensitive so "ci" in words cannot match.
  const pageText = document.body.innerText;
  const scan = (into: string[], patterns: RegExp[]): void => {
    for (const pattern of patterns) {
      for (const match of pageText.matchAll(pattern)) into.push(`page text contains "${match[0]}"`);
    }
  };
  scan(found.A2, [/\bNaN\b|\bundefined\b|\bnull\b|\bInfinity\b|\[object|\de\+\d/g]);
  scan(found.A8, [/confidence|significant|p-value|standard error/gi, /\bCI\b/g, /\b\d{1,2}:\d{2}\b/g]);
  return found;
}
