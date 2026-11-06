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
    bad: read('--color-bad'),
    axis: read('--chart-axis'),
    level: read('--color-level'),
    /** Type: the page's family, the two caption sizes and the count's, in px. */
    font: getComputedStyle(document.documentElement).fontFamily,
    captionPx: size('--font-size-caption'),
    captionSmPx: size('--font-size-caption-sm'),
    countPx: size('--font-size-count'),
  };
  probe.remove();
  return theme;
}

/** The tokens the stage and the charts draw with: colours as packed `0xRRGGBB`,
 *  and the canvas type. */
export type Theme = ReturnType<typeof readTheme>;

/** The longest the canvas waits for the faces before it draws with a
 *  fallback: a missing face is a typographic loss, not a functional one. */
export const FONT_GATE_MS = 3000;

/** The faces the canvas and the charts measure with. */
const FACES = ['400 1em "Source Sans 3"', '600 1em "Source Sans 3"', '400 1em "Source Serif 4"', '400 1em "Source Code Pro"'];

let faces: Promise<unknown> | null = null;

/** Settles once every face has loaded or failed, or the gate has passed,
 *  whichever is first; it never rejects, so a face that fails to load fails
 *  open. Asked for once, the first time something measures text. */
export function fontsReady(): Promise<unknown> {
  faces ??= Promise.race([
    Promise.allSettled(FACES.map((face) => document.fonts.load(face))),
    new Promise((resolve) => setTimeout(resolve, FONT_GATE_MS)),
  ]);
  return faces;
}

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
