// Canvas colours and type. PixiJS and uPlot take numbers and strings, not CSS
// variables, so `readTheme` resolves the design tokens in `tokens.css` by
// painting each colour and reading the pixel back: no colour string is parsed by
// hand, whatever form the browser serialises it in.

/** Resolve the canvas tokens from the page's current styles. Self-contained (no
 *  module-level references), because the design audit runs it in the page too. */
export function readTheme() {
  const ctx = new OffscreenCanvas(1, 1).getContext('2d', { willReadFrequently: true });
  if (ctx === null) throw new Error('readTheme: no 2D canvas to resolve the theme colours with');
  const probe = document.createElement('span');
  probe.hidden = true;
  document.body.append(probe);
  const read = (token: string): number => {
    probe.style.color = `var(${token})`;
    ctx.clearRect(0, 0, 1, 1);
    ctx.fillStyle = getComputedStyle(probe).color;
    ctx.fillRect(0, 0, 1, 1);
    const [r, g, b, a] = ctx.getImageData(0, 0, 1, 1).data;
    // The number has no alpha channel: a translucent token would read as opaque.
    if (a < 255) console.error(`readTheme: ${token} is translucent; canvas tokens must be opaque`);
    return (r << 16) | (g << 8) | b;
  };
  const size = (token: string): number => {
    probe.style.fontSize = `var(${token})`;
    return Number.parseFloat(getComputedStyle(probe).fontSize);
  };
  const theme = {
    bg: read('--color-bg'),
    fg: read('--color-fg'),
    fgMuted: read('--color-fg-muted'),
    grid: read('--chart-grid'),
    muted: read('--chart-muted'),
    reference: read('--chart-reference'),
    panel: read('--color-panel'),
    ok: read('--color-ok'),
    warn: read('--color-warn'),
    /** Type: the page's family and the two caption sizes, in px. */
    font: getComputedStyle(document.documentElement).fontFamily,
    captionPx: size('--font-size-caption'),
    captionSmPx: size('--font-size-caption-sm'),
  };
  probe.remove();
  return theme;
}

/** The tokens the stage and the charts draw with: colours as packed `0xRRGGBB`,
 *  and the canvas type. */
export type Theme = ReturnType<typeof readTheme>;

/** The peak alpha of every stage beam, delivered or dropped, and the REJECTING
 *  band's fill: the design audit composites each over its surface before
 *  checking contrast. */
export const BEAM_ALPHA = 0.8;
export const BAND_ALPHA = 0.15;
/** The share of its alpha a beam keeps when a selection makes it context: the
 *  one named exemption from the graphics contrast rule. */
export const FADED_ALPHA = 0.25;
/** A landed route's alpha, drawn 1 px with no head: history, not the event in
 *  focus. Exempt by name from the graphics contrast rule, as `FADED_ALPHA` is. */
export const LANDED_ALPHA = 0.15;

/** Call `onChange` when the OS colour scheme or `<html data-theme>` changes;
 *  returns the unsubscribe. */
export function watchTheme(onChange: () => void): () => void {
  const scheme = window.matchMedia('(prefers-color-scheme: dark)');
  const observer = new MutationObserver(onChange);
  scheme.addEventListener('change', onChange);
  observer.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
  return () => {
    scheme.removeEventListener('change', onChange);
    observer.disconnect();
  };
}

/** `color` as a CSS hex colour with `alpha` in [0, 1], for uPlot. */
export function cssColor(color: number, alpha = 1): string {
  return `#${(color * 256 + Math.round(alpha * 255)).toString(16).padStart(8, '0')}`;
}
