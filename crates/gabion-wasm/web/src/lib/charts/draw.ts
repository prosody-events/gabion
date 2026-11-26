// The detail pane's charts, drawn on 2D canvases in CSS px: R1 the selected
// node's window by bucket against the limit, R2 the requests admitted and
// rejected, L who is behind and for how long, S the share of data bytes that
// crossed a zone. Every chart shares one frame: a left gutter for ticks and zone
// names, a right margin for direct labels, and one x axis under the last time
// chart. Colour has one role each: ink for counts and the limit, red for
// rejected or over the brim, amber for behind.
//
// Every label goes through `Labels`, which halos it in the panel colour and
// records its box on the canvas (`data-label-boxes`), so the design audit checks
// canvas text the way it checks DOM text.

import { VESSEL_SCALE } from '../limit';
import { cssColor, type Theme } from '../theme';
import { ChartHistory, SLOTS, slotIndex } from './history';
import type { RequestLog } from '../requests';

/** The plot's left edge and the direct labels' room at its right, px. */
export const GUTTER = 40;
export const MARGIN = 88;
/** Room above a plot for a label centred on its top edge, and below it for a
 *  row of tick labels, px. */
export const PAD = 10;
export const AXIS = 20;
/** Plot heights, px: R1, R2 (requests in order, and time bins), S. */
export const R1_PLOT = 120;
export const R2_ORDER = 52;
export const R2_TIME = 64;
export const S_PLOT = 140;
/** L's rows: one per node, a gap between nodes and a wider one between zones. */
export const ROW = 5;
export const ROW_GAP = 1;
export const ZONE_GAP = 5;
/** R2's bars in order: width and pitch, px. */
const BAR = 10;
export const BAR_PITCH = 24;
/** Above this many non-empty bins, R2's time bins merge into one per second
 *  of a 10 s window. */
const BINS_MAX = 50;
const MERGE = 10;
const HALO = 2;
const LINE = 1.25;
const LABEL_GAP = 4;
// Line dashes, made once: a reference, the truth's outline, and solid.
const DASH = [6, 4];
const OUTLINE = [3, 2];
const SOLID: number[] = [];

/** The colours and type a chart draws with, as canvas strings. */
export interface Palette {
  fg: string;
  muted: string;
  level: string;
  bad: string;
  warn: string;
  axis: string;
  track: string;
  panel: string;
  font: string;
  size: number;
}

export function palette(theme: Theme): Palette {
  return {
    fg: cssColor(theme.fg),
    muted: cssColor(theme.fgMuted),
    level: cssColor(theme.level),
    bad: cssColor(theme.bad),
    warn: cssColor(theme.warn),
    axis: cssColor(theme.axis),
    track: cssColor(theme.grid),
    panel: cssColor(theme.panel),
    font: `${theme.captionSmPx}px ${theme.font}`,
    size: theme.captionSmPx,
  };
}

/** One label's box in CSS px of its canvas, halo included. */
interface LabelBox {
  text: string;
  x: number;
  y: number;
  w: number;
  h: number;
  size: number;
}

/** The labels one canvas draws, recorded for the audit. */
export class Labels {
  readonly boxes: LabelBox[] = [];

  /** A label's height with its halo, px. */
  static height(pal: Palette): number {
    return pal.size * LINE + 2 * HALO;
  }

  /** Draw `text` with its halo, `align`ed to `x` and centred on `y`. */
  draw(ctx: CanvasRenderingContext2D, pal: Palette, text: string, x: number, y: number, align: CanvasTextAlign, colour = pal.fg): void {
    ctx.font = pal.font;
    ctx.textAlign = align;
    ctx.textBaseline = 'middle';
    ctx.lineJoin = 'round';
    ctx.lineWidth = 2 * HALO;
    ctx.strokeStyle = pal.panel;
    ctx.strokeText(text, x, y);
    ctx.fillStyle = colour;
    ctx.fillText(text, x, y);
    const w = ctx.measureText(text).width;
    const left = align === 'left' ? x : align === 'right' ? x - w : x - w / 2;
    const h = Labels.height(pal);
    this.boxes.push({ text, x: left - HALO, y: y - h / 2, w: w + 2 * HALO, h, size: pal.size });
  }
}

/** Clear `canvas` for a frame `width` × `height` CSS px and return its context
 *  in CSS px, or null when it has none. */
export function begin(canvas: HTMLCanvasElement, width: number, height: number): CanvasRenderingContext2D | null {
  const dpr = window.devicePixelRatio || 1;
  const w = Math.round(width * dpr);
  const h = Math.round(height * dpr);
  if (canvas.width !== w) canvas.width = w;
  if (canvas.height !== h) canvas.height = h;
  const ctx = canvas.getContext('2d');
  if (ctx === null) return null;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, width, height);
  return ctx;
}

/** The plot's width in a canvas `width` px wide. */
export const plotWidth = (width: number): number => Math.max(width - GUTTER - MARGIN, 1);

/** A hollow block with one slash: a rejection, by shape as well as hue. */
function slashed(ctx: CanvasRenderingContext2D, pal: Palette, x: number, y: number, w: number, h: number): void {
  if (h < 0.5) return;
  ctx.strokeStyle = pal.bad;
  ctx.lineWidth = 1.5;
  ctx.strokeRect(x + 0.75, y + 0.75, w - 1.5, h - 1.5);
  ctx.beginPath();
  ctx.moveTo(x + 0.75, y + h - 0.75);
  ctx.lineTo(x + w - 0.75, y + 0.75);
  ctx.stroke();
}

/** A vertical hairline at `x` across a plot: the readout's cursor. */
function cursorLine(ctx: CanvasRenderingContext2D, pal: Palette, x: number, top: number, height: number): void {
  ctx.fillStyle = pal.muted;
  ctx.fillRect(Math.round(x) - 0.5, top, 1, height);
}

/** Y ticks at 0 and the midpoint, in the gutter. */
function yTicks(ctx: CanvasRenderingContext2D, pal: Palette, labels: Labels, mid: string, y0: number, yMid: number): void {
  labels.draw(ctx, pal, '0', GUTTER - LABEL_GAP, y0, 'right', pal.muted);
  labels.draw(ctx, pal, mid, GUTTER - LABEL_GAP, yMid, 'right', pal.muted);
}

/** The shared time axis's ticks under a plot whose bottom is `y`: the
 *  window's start, its middle and now, from the window alone. */
export function timeAxis(ctx: CanvasRenderingContext2D, pal: Palette, labels: Labels, width: number, y: number, windowMs: number): void {
  const pw = plotWidth(width);
  const at = y + AXIS / 2;
  ctx.fillStyle = pal.axis;
  ctx.fillRect(GUTTER, y, pw, 1);
  const start = `${windowMs / 1000} s ago`;
  const middle = `${windowMs / 2000} s ago`;
  labels.draw(ctx, pal, start, GUTTER, at, 'left', pal.muted);
  // The middle tick only where it clears the start's label on a narrow plot.
  ctx.font = pal.font;
  if (ctx.measureText(start).width + ctx.measureText(middle).width / 2 + 4 * HALO + LABEL_GAP < pw / 2) {
    labels.draw(ctx, pal, middle, GUTTER + pw / 2, at, 'center', pal.muted);
  }
  labels.draw(ctx, pal, 'now', GUTTER + pw, at, 'right', pal.muted);
}

/** What R1 draws: the selected node's count in each live bucket, oldest
 *  first, at `row` of `window` (B to a node), the cluster's truth by bucket,
 *  the limit, and a rejection standing on the current bucket. */
export interface WindowData {
  window: readonly number[];
  row: number;
  oracle: readonly number[];
  limit: number;
  rejected: number;
  windowMs: number;
}

/** R1's height, px. */
export const R1_HEIGHT = PAD + R1_PLOT + AXIS;

/** R1, the window by bucket: each bucket's hits float on the older buckets'
 *  (a waterfall), so the top at "now" is the node's window total, on the
 *  vessel's scale with the limit at 0.8 of the plot. Where the cluster counted
 *  more than the node in a bucket, a dashed outline shows the difference. */
export function drawWindow(ctx: CanvasRenderingContext2D, pal: Palette, labels: Labels, width: number, d: WindowData, cursor: number | null): void {
  const pw = plotWidth(width);
  const top = PAD;
  const bottom = PAD + R1_PLOT;
  const scale = VESSEL_SCALE * d.limit;
  const y = (v: number): number => bottom - (Math.min(v, scale) / scale) * R1_PLOT;
  const buckets = d.oracle.length;
  const cw = pw / buckets;
  const gap = Math.min(2, cw / 4);
  let own = 0;
  let truth = 0;
  for (let k = 0; k < buckets; k++) {
    const hits = d.window[d.row * buckets + k] ?? 0;
    const x = GUTTER + k * cw + gap / 2;
    const below = Math.min(own + hits, d.limit);
    if (below > own) {
      ctx.fillStyle = pal.level;
      ctx.fillRect(x, y(below), cw - gap, y(own) - y(below));
    }
    if (own + hits > d.limit) {
      ctx.fillStyle = pal.bad;
      const from = Math.max(own, d.limit);
      ctx.fillRect(x, y(own + hits), cw - gap, y(from) - y(own + hits));
    }
    own += hits;
    truth += d.oracle[k];
    if (truth > own) {
      ctx.strokeStyle = pal.fg;
      ctx.lineWidth = 1;
      ctx.setLineDash(OUTLINE);
      ctx.strokeRect(x + 0.5, y(truth) + 0.5, cw - gap - 1, y(own) - y(truth) - 1);
      ctx.setLineDash(SOLID);
    }
  }
  ctx.fillStyle = pal.axis;
  ctx.fillRect(GUTTER, bottom, pw, 1);
  const brim = y(d.limit);
  ctx.fillStyle = pal.fg;
  ctx.fillRect(GUTTER, brim - 0.75, pw, 1.5);
  if (d.rejected > 0) {
    const x = GUTTER + (buckets - 1) * cw + gap / 2;
    const from = y(own);
    const to = y(own + d.rejected);
    slashed(ctx, pal, x, to, cw - gap, from - to);
  }
  if (cursor !== null) cursorLine(ctx, pal, GUTTER + (cursor + 0.5) * cw, top, R1_PLOT);
  yTicks(ctx, pal, labels, String(Math.round(d.limit / 2)), bottom, y(d.limit / 2));
  const right = GUTTER + pw + LABEL_GAP;
  labels.draw(ctx, pal, `limit ${d.limit}`, right, brim, 'left');
  if (d.rejected > 0) {
    const h = Labels.height(pal);
    const mid = (y(own) + y(own + d.rejected)) / 2;
    labels.draw(ctx, pal, `${d.rejected} rejected`, right, Math.abs(mid - brim) < h ? brim - h : mid, 'left', pal.bad);
  }
  const at = bottom + AXIS / 2;
  labels.draw(ctx, pal, `${d.windowMs / 1000} s ago`, GUTTER, at, 'left', pal.muted);
  labels.draw(ctx, pal, 'now', GUTTER + pw, at, 'right', pal.muted);
}

/** R2's height, px: in order, with a row of node ids; in time, with the shared
 *  axis's room when it is the last time chart. */
export const r2Height = (inOrder: boolean, axis: boolean): number =>
  PAD + (inOrder ? R2_ORDER : R2_TIME) + (inOrder || axis ? AXIS : PAD);

/** The first Send R2 draws: as many of the latest as fit the plot. */
export const firstBar = (log: RequestLog, width: number): number =>
  Math.max(log.length - Math.floor(plotWidth(width) / BAR_PITCH), 0);

/** R2's axis, the share of its plot above it, and the direct labels. */
function r2Frame(ctx: CanvasRenderingContext2D, pal: Palette, labels: Labels, width: number, plot: number): { axis: number; above: number; below: number } {
  const above = Math.round(plot * 0.6);
  const axis = PAD + above;
  const pw = plotWidth(width);
  ctx.fillStyle = pal.axis;
  ctx.fillRect(GUTTER, axis, pw, 1);
  const right = GUTTER + pw + LABEL_GAP;
  labels.draw(ctx, pal, 'admitted', right, PAD + above / 2, 'left');
  labels.draw(ctx, pal, 'rejected', right, axis + (plot - above) / 2, 'left', pal.bad);
  return { axis, above, below: plot - above };
}

/** R2 in held scenarios: one bar per Send, in order, its height the requests;
 *  admitted above the axis, rejected below as a hollow slashed block, the
 *  node's id under each. */
export function drawRequests(ctx: CanvasRenderingContext2D, pal: Palette, labels: Labels, width: number, log: RequestLog, cursor: number | null): void {
  const { axis, above, below } = r2Frame(ctx, pal, labels, width, R2_ORDER);
  const first = firstBar(log, width);
  let most = 1;
  for (let i = first; i < log.length; i++) most = Math.max(most, log.admitted(i), log.rejected(i));
  const ids = PAD + R2_ORDER + AXIS / 2;
  for (let i = first; i < log.length; i++) {
    const x = GUTTER + (i - first) * BAR_PITCH + (BAR_PITCH - BAR) / 2;
    const up = (log.admitted(i) / most) * (above - 1);
    if (up > 0) {
      ctx.fillStyle = pal.level;
      ctx.fillRect(x, axis - up, BAR, up);
    }
    slashed(ctx, pal, x, axis + 1, BAR, (log.rejected(i) / most) * (below - 1));
    labels.draw(ctx, pal, String(log.node(i)), x + BAR / 2, ids, 'center', pal.muted);
  }
  if (cursor !== null) cursorLine(ctx, pal, GUTTER + (cursor - first + 0.5) * BAR_PITCH, PAD, R2_ORDER);
}

/** The x of virtual time `t` on the shared domain ending at `now`. */
const xOf = (t: number, now: number, windowMs: number, pw: number): number => GUTTER + ((t - (now - windowMs)) / windowMs) * pw;

/** The bins R2 draws in time: one per slot, or one per `MERGE` slots when
 *  more than `BINS_MAX` would show. */
export function binSlots(h: ChartHistory): number {
  let filled = 0;
  for (let i = 0; i < SLOTS; i++) if (h.admitted[i] > 0 || h.rejected[i] > 0) filled += 1;
  return filled > BINS_MAX ? MERGE : 1;
}

/** R2 in feed scenarios: requests admitted and rejected per time bin on the
 *  shared axis. */
export function drawBins(ctx: CanvasRenderingContext2D, pal: Palette, labels: Labels, width: number, h: ChartHistory, now: number, axisBelow: boolean, cursor: number | null): void {
  const { axis, above, below } = r2Frame(ctx, pal, labels, width, R2_TIME);
  const pw = plotWidth(width);
  const per = binSlots(h);
  const oldest = h.head - SLOTS + 1;
  // Bins align to whole multiples of `per` slots, so a merged bin keeps its
  // place as the window slides.
  const start = Math.floor(oldest / per) * per;
  let most = 1;
  for (let b = start; b <= h.head; b += per) {
    let a = 0;
    let r = 0;
    for (let j = Math.max(b, oldest); j < b + per && j <= h.head; j++) {
      a += h.admitted[slotIndex(j)];
      r += h.rejected[slotIndex(j)];
    }
    most = Math.max(most, a, r);
  }
  ctx.save();
  ctx.beginPath();
  ctx.rect(GUTTER, 0, pw, PAD + R2_TIME);
  ctx.clip();
  for (let b = start; b <= h.head; b += per) {
    let a = 0;
    let r = 0;
    for (let j = Math.max(b, oldest); j < b + per && j <= h.head; j++) {
      a += h.admitted[slotIndex(j)];
      r += h.rejected[slotIndex(j)];
    }
    const x0 = xOf(b * h.slotMs, now, h.windowMs, pw);
    const w = Math.max((per * h.slotMs * pw) / h.windowMs - 1, 1);
    const up = (a / most) * (above - 1);
    if (up > 0) {
      ctx.fillStyle = pal.level;
      ctx.fillRect(x0, axis - up, w, up);
    }
    const down = (r / most) * (below - 1);
    if (w >= 6) slashed(ctx, pal, x0, axis + 1, w, down);
    else if (down > 0) {
      ctx.fillStyle = pal.bad;
      ctx.fillRect(x0, axis + 1, w, down);
    }
  }
  ctx.restore();
  if (cursor !== null) cursorLine(ctx, pal, xOf((oldest + cursor + 0.5) * h.slotMs, now, h.windowMs, pw), PAD, R2_TIME);
  if (axisBelow) timeAxis(ctx, pal, labels, width, PAD + R2_TIME, h.windowMs);
}

/** L's rows: each live node in (zone, id) order, and where each zone starts. */
export interface Lanes {
  nodes: readonly { id: number; zone: number }[];
  zoneNames: readonly string[];
}

/** L's plot height for `nodes` in (zone, id) order, px. */
export function lanesHeight(nodes: readonly { zone: number }[]): number {
  let h = 0;
  for (let i = 0; i < nodes.length; i++) {
    if (i > 0) h += nodes[i].zone === nodes[i - 1].zone ? ROW_GAP : ZONE_GAP;
    h += ROW;
  }
  return h;
}

/** L's height, px. */
export const lHeight = (nodes: readonly { zone: number }[], axis: boolean): number =>
  PAD + lanesHeight(nodes) + (axis ? AXIS : PAD);

/** L, who is behind: one row per node, an amber run in every slot it was
 *  not caught up, zone names in the gutter. */
export function drawBehind(ctx: CanvasRenderingContext2D, pal: Palette, labels: Labels, width: number, h: ChartHistory, lanes: Lanes, now: number, axisBelow: boolean, cursor: number | null): void {
  const pw = plotWidth(width);
  const oldest = h.head - SLOTS + 1;
  const slotW = (h.slotMs * pw) / h.windowMs;
  const nodes = lanes.nodes;
  const plot = lanesHeight(nodes);
  ctx.save();
  ctx.beginPath();
  ctx.rect(GUTTER, 0, pw, PAD + plot);
  ctx.clip();
  let y = PAD;
  for (let i = 0; i < nodes.length; i++) {
    if (i > 0) y += nodes[i].zone === nodes[i - 1].zone ? ROW_GAP : ZONE_GAP;
    ctx.fillStyle = pal.track;
    ctx.fillRect(GUTTER, y, pw, ROW);
    const row = h.behind(nodes[i].id);
    if (row !== undefined) {
      ctx.fillStyle = pal.warn;
      for (let j = oldest; j <= h.head; j++) {
        if (Number.isNaN(row[slotIndex(j)])) continue;
        const x0 = xOf(j * h.slotMs, now, h.windowMs, pw);
        ctx.fillRect(x0, y, slotW + 0.5, ROW);
      }
    }
    y += ROW;
  }
  ctx.restore();
  // Each zone's name, centred on its rows, in the gutter.
  y = PAD;
  let zoneTop = y;
  for (let i = 0; i < nodes.length; i++) {
    if (i > 0) y += nodes[i].zone === nodes[i - 1].zone ? ROW_GAP : ZONE_GAP;
    if (i === 0 || nodes[i].zone !== nodes[i - 1].zone) zoneTop = y;
    y += ROW;
    if (i === nodes.length - 1 || nodes[i + 1].zone !== nodes[i].zone) {
      labels.draw(ctx, pal, lanes.zoneNames[nodes[i].zone] ?? '', GUTTER - LABEL_GAP, (zoneTop + y) / 2, 'right', pal.muted);
    }
  }
  if (cursor !== null) cursorLine(ctx, pal, xOf((oldest + cursor + 0.5) * h.slotMs, now, h.windowMs, pw), PAD, plot);
  if (axisBelow) timeAxis(ctx, pal, labels, width, PAD + plot, h.windowMs);
}

/** The two predictions S draws: the share of data bytes that should cross
 *  zones with zones, and with none. */
export interface References {
  zones: number;
  nozone: number;
}

/** A dashed reference line across the plot at `y`. */
function reference(ctx: CanvasRenderingContext2D, pal: Palette, pw: number, y: number): void {
  ctx.strokeStyle = pal.axis;
  ctx.lineWidth = 1;
  ctx.setLineDash(DASH);
  ctx.beginPath();
  ctx.moveTo(GUTTER, Math.round(y) + 0.5);
  ctx.lineTo(GUTTER + pw, Math.round(y) + 0.5);
  ctx.stroke();
  ctx.setLineDash(SOLID);
}

/** S's height, px. */
export const sHeight = (axis: boolean): number => PAD + S_PLOT + (axis ? AXIS : PAD);

/** S, the share of the window's data bytes that crossed a zone, at the end of
 *  each slot, over its two predictions. */
export function drawShare(ctx: CanvasRenderingContext2D, pal: Palette, labels: Labels, width: number, h: ChartHistory, refs: References | null, now: number, axisBelow: boolean, cursor: number | null): void {
  const pw = plotWidth(width);
  const bottom = PAD + S_PLOT;
  const y = (v: number): number => bottom - v * S_PLOT;
  const right = GUTTER + pw + LABEL_GAP;
  ctx.fillStyle = pal.axis;
  ctx.fillRect(GUTTER, bottom, pw, 1);
  if (refs !== null) {
    reference(ctx, pal, pw, y(refs.zones));
    reference(ctx, pal, pw, y(refs.nozone));
  }
  ctx.strokeStyle = pal.fg;
  ctx.lineWidth = 2;
  ctx.beginPath();
  let drawing = false;
  for (let j = h.head - SLOTS + 1; j <= h.head; j++) {
    const v = h.shareAt(j);
    if (v === null) {
      drawing = false;
      continue;
    }
    const x = xOf(Math.min((j + 1) * h.slotMs, now), now, h.windowMs, pw);
    if (x < GUTTER) continue;
    if (drawing) ctx.lineTo(x, y(v));
    else ctx.moveTo(x, y(v));
    drawing = true;
  }
  ctx.stroke();
  if (cursor !== null) cursorLine(ctx, pal, xOf((h.head - SLOTS + 1 + cursor + 1) * h.slotMs, now, h.windowMs, pw), PAD, S_PLOT);
  yTicks(ctx, pal, labels, '50 %', bottom, y(0.5));
  if (refs !== null) {
    const a = y(refs.zones);
    const b = y(refs.nozone);
    // Two predictions within one label height of each other share a label.
    if (Math.abs(a - b) < Labels.height(pal)) labels.draw(ctx, pal, 'both predictions', right, (a + b) / 2, 'left');
    else {
      labels.draw(ctx, pal, 'with zones', right, a, 'left');
      labels.draw(ctx, pal, 'without zones', right, b, 'left');
    }
  }
  if (axisBelow) timeAxis(ctx, pal, labels, width, bottom, h.windowMs);
}
