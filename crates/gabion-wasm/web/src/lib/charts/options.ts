// uPlot option builders for the dashboard's charts. Pure functions — given the
// node count (for the fan's per-node series), each returns a complete
// `uPlot.Options`. The thin `Chart.svelte` wrapper owns the lifecycle; the
// pedagogy lives here.
//
// The Aggregate-vs-Limit panel (`aggregateLimitOptions`) is shown only under the
// overload preset, whose sustained feed climbs the aggregate across a low limit
// — the one regime where it is legible. A single burst can't tell that story:
// approaching the limit trips the eager threshold flush, collapsing the spread
// the other scenarios exist to show. See `lib/presets.ts`.
//
// Conventions enforced across the charts (the design rubric is explicit):
//   • one shared cursor-sync key, so a hover crosshair tracks the same x on
//     every panel at once;
//   • the detached legend is OFF — series are direct-labelled in `draw` hooks
//     (Tufte: label the line, don't make the eye round-trip to a key);
//   • the oracle / true-total line dominates (dark, heavy, dashed); per-node
//     lines recede (thin muted lines), so the macro reading is the oracle and
//     the micro reading is the spread around it.

import uPlot from 'uplot';
import { BAND_ALPHA, cssColor, type Theme } from '../theme';

const FONT = "12px ui-sans-serif, system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif";

// One sync group ties every chart's cursor to the same virtual-time x.
const SYNC_KEY = 'gabion-dashboard';

/** Device-pixel ratio uPlot drew at: hook coordinates and fonts are in canvas
 *  (device) pixels, so static sizes must scale by it to stay crisp. */
function pxr(u: uPlot): number {
  return (u as { pxRatio?: number }).pxRatio ?? (window.devicePixelRatio || 1);
}

/** Shared axis styling: a faint grid, soft ink labels, minimal tick ink —
 *  maximizing the data-ink ratio. */
function axes(theme: Theme, xLabel?: string): uPlot.Axis[] {
  const grid = cssColor(theme.grid);
  const common = {
    stroke: cssColor(theme.fgMuted),
    font: FONT,
    grid: { stroke: grid, width: 1 },
    ticks: { stroke: grid, width: 1, size: 4 },
  };
  return [
    { ...common, label: xLabel, labelFont: FONT, labelGap: 4, size: xLabel ? 38 : 28 },
    { ...common, size: 44 },
  ];
}

function baseCursor(): uPlot.Cursor {
  return { sync: { key: SYNC_KEY }, points: { size: 6 }, focus: { prox: 12 } };
}

/** Direct-label series `seriesIdx` at the plot's right margin, riding at the
 *  line's current height — the label that replaces a detached legend entry.
 *  Pinned to the right edge (not the last data point) so a single sample at
 *  x = 0 can't push the right-anchored text off the left edge. */
function labelSeriesEnd(seriesIdx: number, text: string, color: string) {
  return (u: uPlot): void => {
    const ys = u.data[seriesIdx];
    let i = ys.length - 1;
    while (i >= 0 && ys[i] == null) i--;
    if (i < 0) return;
    const dpr = pxr(u);
    const x = u.bbox.left + u.bbox.width - 4 * dpr;
    const y = u.valToPos(ys[i] as number, 'y', true);
    const ctx = u.ctx;
    ctx.save();
    ctx.font = `${dpr * 11}px ui-sans-serif, system-ui, sans-serif`;
    ctx.fillStyle = color;
    ctx.textAlign = 'right';
    ctx.textBaseline = 'bottom';
    ctx.fillText(text, x, y - 4 * dpr);
    ctx.restore();
  };
}

/** A1 — the Convergence Fan. N recessive per-node lines racing a dominant,
 *  heavy dashed ground-truth oracle. The hero panel; never hidden. */
export function fanOptions(nodeCount: number, theme: Theme): uPlot.Options {
  const muted = cssColor(theme.muted);
  const fg = cssColor(theme.fg);
  const nodeSeries: uPlot.Series[] = [];
  for (let i = 0; i < nodeCount; i++) {
    nodeSeries.push({ stroke: muted, width: 1, points: { show: false } });
  }
  const oracleIdx = nodeCount + 1;
  return {
    width: 1,
    height: 1,
    cursor: baseCursor(),
    legend: { show: false },
    scales: { x: { time: false } },
    axes: axes(theme),
    series: [
      {},
      ...nodeSeries,
      // The oracle: dark, heavy, dashed — the line the fan chases.
      { stroke: fg, width: 2.5, dash: [7, 4], points: { show: false } },
    ],
    hooks: { draw: [labelSeriesEnd(oracleIdx, 'true total', fg)] },
  };
}

/** Paint the REJECTING band (limit → top of plot) and the dashed limit line,
 *  behind the series. Device-pixel coordinates, like `labelSeriesEnd`. */
function drawRejectBand(limit: number, fill: string, stroke: string) {
  return (u: uPlot): void => {
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
  };
}

/** Label the band "REJECTING" (top-left, inside the band) and the threshold
 *  "limit" (left, just under the dashed line), over the series. Kept on the left
 *  so neither collides with the "aggregate" end-label riding the line's right. */
function drawRejectLabels(limit: number, color: string) {
  return (u: uPlot): void => {
    const dpr = pxr(u);
    const ctx = u.ctx;
    const x = u.bbox.left;
    const yTop = u.bbox.top;
    const yLimit = u.valToPos(limit, 'y', true);
    ctx.save();
    ctx.font = `${dpr * 11}px ui-sans-serif, system-ui, sans-serif`;
    ctx.fillStyle = color;
    ctx.textAlign = 'left';
    ctx.textBaseline = 'top';
    ctx.fillText('REJECTING', x + 6 * dpr, yTop + 4 * dpr);
    ctx.fillText('limit', x + 6 * dpr, yLimit + 3 * dpr);
    ctx.restore();
  };
}

/** B1 — Aggregate vs Limit. The cluster's true total (the aggregate every node
 *  converges on) climbing across a dashed limit line, the regime above it shaded
 *  as the REJECTING band — *why gabion exists*. Shown only for the overload
 *  preset, whose sustained feed actually drives the aggregate across the limit. */
export function aggregateLimitOptions(limit: number, theme: Theme): uPlot.Options {
  // Fixed range so the limit line keeps its height as the aggregate ramps (no
  // auto-fit crawl). `2×limit` seats the limit at mid-height — a tall enough
  // REJECTING band to label — and leaves headroom above the feed's 1.6×limit
  // plateau so the line settles inside the band, not against the ceiling.
  const top = Math.max(limit * 2, 1);
  const fg = cssColor(theme.fg);
  const warn = cssColor(theme.warn);
  return {
    width: 1,
    height: 1,
    cursor: baseCursor(),
    legend: { show: false },
    scales: { x: { time: false }, y: { range: () => [0, top] } },
    axes: axes(theme),
    series: [{}, { stroke: fg, fill: cssColor(theme.fg, 0.08), width: 2, points: { show: false } }],
    hooks: {
      drawClear: [drawRejectBand(limit, cssColor(theme.warn, BAND_ALPHA), warn)],
      draw: [drawRejectLabels(limit, warn), labelSeriesEnd(1, 'aggregate', fg)],
    },
  };
}

/** A2 — Disagreement → 0. The per-node spread (max − min) as a filled area
 *  decaying to zero after the burst: the textbook anti-entropy curve. */
export function disagreementOptions(theme: Theme): uPlot.Options {
  return {
    width: 1,
    height: 1,
    cursor: baseCursor(),
    legend: { show: false },
    scales: { x: { time: false }, y: { range: (_u, _min, max) => [0, Math.max(max, 1)] } },
    axes: axes(theme, 'virtual ms'),
    series: [
      {},
      {
        stroke: cssColor(theme.warn),
        fill: cssColor(theme.warn, 0.2),
        width: 1.5,
        points: { show: false },
      },
    ],
  };
}
