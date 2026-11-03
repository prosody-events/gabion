// uPlot option builders for the dashboard's charts. Pure functions: each returns
// a complete `uPlot.Options`. The thin `Chart.svelte` wrapper owns the
// lifecycle; the pedagogy lives here.
//
// Conventions enforced across the charts (the design rubric is explicit):
//   • one shared cursor-sync key, so a hover crosshair tracks the same x on
//     every panel at once;
//   • the detached legend is OFF: reference lines are labelled in the right
//     margin, where no data line reaches (Tufte: label the line, don't make the
//     eye round-trip to a key);
//   • references are drawn first and recede (dashed `--chart-reference`), so the
//     data reads over them;
//   • every label this file draws goes through `drawLabel`, which halos it in
//     the surface colour and records its measured box on the canvas
//     (`data-label-boxes`), so the design audit checks canvas text for overlap
//     and legibility the way it checks DOM text. uPlot's own axis ticks are laid
//     out by the library and drawn in the caption-sm size.
//
// The Aggregate-vs-Limit panel (`aggregateLimitOptions`) is shown only under the
// overload preset, whose sustained feed climbs the aggregate across a low limit
// — the one regime where it is legible. See `lib/presets.ts`.

import uPlot from 'uplot';
import { BAND_ALPHA, cssColor, type Theme } from '../theme';

// One sync group ties every chart's cursor to the same virtual-time x.
const SYNC_KEY = 'gabion-dashboard';
// A label's halo, px each side, and its line box as a multiple of its font size.
const HALO = 2;
const LINE = 1.25;
// Gap between a plot and the label beside it, px.
const LABEL_GAP = 4;
// The room an x axis's ticks take below the plot, with a title and without,
// and the title's own row, px.
const X_AXIS_SIZE = 38;
const X_AXIS_BARE = 28;
const X_TITLE_SIZE = 30;
/** All the room a titled x axis takes below its plot, px: what a stacked
 *  panel that shows one must add to its height to keep its plot's. */
export const X_AXIS_ROOM = X_AXIS_SIZE + X_TITLE_SIZE;
const Y_AXIS_SIZE = 44;

/** One label's box in CSS px of the chart canvas, halo included. */
interface LabelBox {
  text: string;
  x: number;
  y: number;
  w: number;
  h: number;
  size: number;
}

/** The fitted range panels of one chart family share, so one axis serves them
 *  all: the dashboard rewrites `max` with each sample. */
export interface YRange {
  max: number;
}

/** The two predictions chart 1 draws, as the share of data bytes that should
 *  cross zones with zones and with none. */
export interface References {
  zones: number;
  nozone: number;
}

let measuring: OffscreenCanvasRenderingContext2D | null = null;

/** The width of `text` in `theme`'s caption-sm type, in px. */
function measure(theme: Theme, text: string): number {
  measuring ??= new OffscreenCanvas(1, 1).getContext('2d');
  if (measuring === null) return text.length * theme.captionSmPx;
  measuring.font = font(theme);
  return measuring.measureText(text).width;
}

function font(theme: Theme, scale = 1): string {
  return `${theme.captionSmPx * scale}px ${theme.font}`;
}

/** A label's height with its halo, px. */
function labelHeight(theme: Theme): number {
  return theme.captionSmPx * LINE + 2 * HALO;
}

/** Device-pixel ratio uPlot drew at: hook coordinates and fonts are in canvas
 *  (device) pixels, so static sizes must scale by it to stay crisp. */
function pxr(u: uPlot): number {
  return (u as { pxRatio?: number }).pxRatio ?? (window.devicePixelRatio || 1);
}

/** The labels one chart draws: `draw` paints a label with its halo and records
 *  its box, `reset` empties the record before a redraw and `publish` writes it
 *  to the canvas. */
function labelSink(theme: Theme) {
  const boxes: LabelBox[] = [];
  return {
    reset: (): void => {
      boxes.length = 0;
    },
    /** Draw `text` at (`x`, `y`) in CSS px of the canvas, `align`ed to `x`,
     *  vertically centred on `y`. */
    draw: (u: uPlot, text: string, x: number, y: number, align: 'left' | 'right'): void => {
      const dpr = pxr(u);
      const ctx = u.ctx;
      ctx.save();
      ctx.font = font(theme, dpr);
      ctx.textAlign = align;
      ctx.textBaseline = 'middle';
      ctx.lineJoin = 'round';
      ctx.lineWidth = 2 * HALO * dpr;
      ctx.strokeStyle = cssColor(theme.panel);
      ctx.strokeText(text, x * dpr, y * dpr);
      ctx.fillStyle = cssColor(theme.fg);
      ctx.fillText(text, x * dpr, y * dpr);
      const w = ctx.measureText(text).width / dpr;
      ctx.restore();
      const h = labelHeight(theme);
      boxes.push({
        text,
        x: (align === 'left' ? x : x - w) - HALO,
        y: y - h / 2,
        w: w + 2 * HALO,
        h,
        size: theme.captionSmPx,
      });
    },
    publish: (u: uPlot): void => {
      u.ctx.canvas.dataset.labelBoxes = JSON.stringify(boxes);
    },
  };
}

/** The y of the last non-null value of series `idx` in canvas CSS px, or `null`
 *  when the series has none. */
function lastY(u: uPlot, idx: number): number | null {
  const ys = u.data[idx];
  let i = ys.length - 1;
  while (i >= 0 && ys[i] == null) i--;
  return i < 0 ? null : u.valToPos(ys[i] as number, 'y', true) / pxr(u);
}

/** The right edge of the plot in CSS px of the canvas. */
function plotRight(u: uPlot): number {
  return (u.bbox.left + u.bbox.width) / pxr(u);
}

/** Draw a dashed horizontal reference line across the plot at `y` (CSS px of
 *  the canvas), behind the series. */
function drawReference(u: uPlot, y: number, theme: Theme): void {
  const dpr = pxr(u);
  const ctx = u.ctx;
  ctx.save();
  ctx.strokeStyle = cssColor(theme.reference);
  ctx.lineWidth = dpr;
  ctx.setLineDash([6 * dpr, 4 * dpr]);
  ctx.beginPath();
  ctx.moveTo(u.bbox.left, y * dpr);
  ctx.lineTo(u.bbox.left + u.bbox.width, y * dpr);
  ctx.stroke();
  ctx.restore();
}

/** Shared axis styling: a faint grid, caption-sm ink, minimal tick ink,
 *  maximizing the data-ink ratio. `xTitle` labels the x axis; `showX` draws it
 *  at all (the stacked panels show it on the last only). */
function axes(
  theme: Theme,
  yTitle: string | undefined,
  xTitle: string | undefined,
  showX = true,
  yValues?: uPlot.Axis.Values,
): uPlot.Axis[] {
  const grid = cssColor(theme.grid);
  const type = font(theme);
  const common = {
    stroke: cssColor(theme.fgMuted),
    font: type,
    labelFont: type,
    grid: { stroke: grid, width: 1 },
    ticks: { stroke: grid, width: 1, size: 4 },
  };
  return [
    {
      ...common,
      show: showX,
      label: xTitle,
      labelGap: 4,
      labelSize: X_TITLE_SIZE,
      size: xTitle === undefined ? X_AXIS_BARE : X_AXIS_SIZE,
      // x is virtual milliseconds; the axis reads seconds.
      values: (_u, ticks) => ticks.map((t) => t / 1000),
    },
    { ...common, label: yTitle, labelGap: 4, size: Y_AXIS_SIZE, values: yValues },
  ];
}

function baseCursor(): uPlot.Cursor {
  return { sync: { key: SYNC_KEY }, points: { size: 6 }, focus: { prox: 12 } };
}

/** Plot padding in CSS px: room above and below for a label centred on the
 *  plot's edge, and `right` for labels in the margin. */
function padding(theme: Theme, right: number): uPlot.Padding {
  const half = Math.ceil(labelHeight(theme) / 2);
  return [half, right, half, 0];
}

/** Chart 1: the share of data bytes that crossed zones, σ, on a fixed 0–100 %
 *  axis, over two dashed references the design predicts, labelled in the right
 *  margin where the data line never reaches. `refs` is read at every draw, so
 *  the references follow the cluster without rebuilding the chart. */
export function shareOptions(theme: Theme, refs: () => References | null): uPlot.Options {
  const sink = labelSink(theme);
  const labels = ['with zones', 'with no zone', 'both predictions'];
  const margin = Math.ceil(Math.max(...labels.map((l) => measure(theme, l)))) + LABEL_GAP;
  return {
    width: 1,
    height: 1,
    padding: padding(theme, margin + LABEL_GAP),
    cursor: baseCursor(),
    legend: { show: false },
    scales: { x: { time: false }, y: { range: () => [0, 1] } },
    axes: axes(theme, 'share of data bytes, %', 'virtual time, s', true, (_u, ticks) =>
      ticks.map((t) => t * 100),
    ),
    series: [{}, { stroke: cssColor(theme.fg), width: 2, points: { show: false }, spanGaps: false }],
    hooks: {
      drawClear: [
        (u) => {
          sink.reset();
          const r = refs();
          if (r === null) return;
          for (const v of [r.zones, r.nozone]) drawReference(u, u.valToPos(v, 'y', true) / pxr(u), theme);
        },
      ],
      draw: [
        (u) => {
          const r = refs();
          if (r !== null) {
            const ys = [r.zones, r.nozone].map((v) => u.valToPos(v, 'y', true) / pxr(u));
            const x = plotRight(u) + LABEL_GAP;
            // Two predictions within one label height of each other share a label.
            if (Math.abs(ys[0] - ys[1]) < labelHeight(theme)) {
              sink.draw(u, 'both predictions', x, (ys[0] + ys[1]) / 2, 'left');
            } else {
              sink.draw(u, 'with zones', x, ys[0], 'left');
              sink.draw(u, 'with no zone', x, ys[1], 'left');
            }
          }
          sink.publish(u);
        },
      ],
    },
  };
}

/** Chart 2, one zone's panel: the band from its members' least to most
 *  informed view, over the true total drawn first as a dashed reference. Series:
 *  x, true total, max, min and the hovered node's own line (null unless one is
 *  hovered in this zone). The first panel labels the reference. */
export function agreementPanel(theme: Theme, first: boolean, last: boolean, y: YRange): uPlot.Options {
  const sink = labelSink(theme);
  const muted = cssColor(theme.muted);
  const edge = { stroke: muted, width: 1, points: { show: false }, spanGaps: false };
  return {
    width: 1,
    height: 1,
    padding: padding(theme, Math.ceil(measure(theme, 'true total')) + 2 * LABEL_GAP),
    cursor: baseCursor(),
    legend: { show: false },
    scales: { x: { time: false }, y: { range: () => [0, y.max] } },
    axes: axes(theme, undefined, last ? 'virtual time, s' : undefined, last),
    series: [
      {},
      {
        stroke: cssColor(theme.reference),
        width: 1.5,
        dash: [6, 4],
        points: { show: false },
        spanGaps: false,
      },
      edge,
      edge,
      { stroke: cssColor(theme.fg), width: 2, points: { show: false }, spanGaps: false },
    ],
    bands: [{ series: [2, 3], fill: cssColor(theme.muted, 0.25) }],
    hooks: {
      drawClear: [() => sink.reset()],
      draw: [
        (u) => {
          const at = lastY(u, 1);
          if (first && at !== null) sink.draw(u, 'true total', plotRight(u) + LABEL_GAP, at, 'left');
          sink.publish(u);
        },
      ],
    },
  };
}

/** Chart 3, one panel: max − min view, an area over a zero reference. */
export function spreadPanel(theme: Theme, last: boolean, y: YRange): uPlot.Options {
  const sink = labelSink(theme);
  return {
    width: 1,
    height: 1,
    padding: padding(theme, 0),
    cursor: baseCursor(),
    legend: { show: false },
    scales: { x: { time: false }, y: { range: () => [0, y.max] } },
    axes: axes(theme, undefined, last ? 'virtual time, s' : undefined, last),
    series: [
      {},
      {
        stroke: cssColor(theme.warn),
        fill: cssColor(theme.warn, 0.2),
        width: 1.5,
        points: { show: false },
        spanGaps: false,
      },
    ],
    hooks: {
      drawClear: [
        (u) => {
          sink.reset();
          drawReference(u, u.valToPos(0, 'y', true) / pxr(u), theme);
        },
      ],
      draw: [(u) => sink.publish(u)],
    },
  };
}

/** Paint the REJECTING band (limit → top of plot) and the dashed limit line,
 *  behind the series. Device-pixel coordinates. */
function drawRejectBand(u: uPlot, limit: number, fill: string, stroke: string): void {
  const dpr = pxr(u);
  const ctx = u.ctx;
  const x = u.bbox.left;
  const w = u.bbox.width;
  const yTop = u.bbox.top;
  const yLimit = u.valToPos(limit, 'y', true);
  if (yLimit <= yTop) return;
  ctx.save();
  ctx.fillStyle = fill;
  ctx.fillRect(x, yTop, w, yLimit - yTop);
  ctx.strokeStyle = stroke;
  ctx.lineWidth = 1.5 * dpr;
  ctx.setLineDash([6 * dpr, 4 * dpr]);
  ctx.beginPath();
  ctx.moveTo(x, yLimit);
  ctx.lineTo(x + w, yLimit);
  ctx.stroke();
  ctx.restore();
}

/** Chart 4: Aggregate vs Limit. The cluster's true total (the aggregate every
 *  node converges on) climbing across a dashed limit line, the regime above it
 *  shaded as the REJECTING band: *why gabion exists*. Shown only for the
 *  overload preset, whose sustained feed actually drives the aggregate across
 *  the limit. */
export function aggregateLimitOptions(limit: number, theme: Theme): uPlot.Options {
  // Fixed range so the limit line keeps its height as the aggregate ramps (no
  // auto-fit crawl). `2×limit` seats the limit at mid-height — a tall enough
  // REJECTING band to label — and leaves headroom above the feed's 1.6×limit
  // plateau so the line settles inside the band, not against the ceiling.
  const top = Math.max(limit * 2, 1);
  const sink = labelSink(theme);
  const warn = cssColor(theme.warn);
  return {
    width: 1,
    height: 1,
    cursor: baseCursor(),
    legend: { show: false },
    scales: { x: { time: false }, y: { range: () => [0, top] } },
    axes: axes(theme, 'hits in the window', 'virtual time, s'),
    series: [
      {},
      { stroke: cssColor(theme.fg), fill: cssColor(theme.fg, 0.08), width: 2, points: { show: false } },
    ],
    hooks: {
      drawClear: [
        (u) => {
          sink.reset();
          drawRejectBand(u, limit, cssColor(theme.warn, BAND_ALPHA), warn);
        },
      ],
      // The band's name sits at its top-left corner and the limit's just under
      // the dashed line, over the series; both on the left so neither meets the
      // line's right end.
      draw: [
        (u) => {
          const left = u.bbox.left / pxr(u) + LABEL_GAP;
          const half = labelHeight(theme) / 2;
          sink.draw(u, 'REJECTING', left, u.bbox.top / pxr(u) + half, 'left');
          sink.draw(u, 'limit', left, u.valToPos(limit, 'y', true) / pxr(u) + half + LABEL_GAP, 'left');
          sink.publish(u);
        },
      ],
    },
  };
}
