import { inflateSync } from 'node:zlib';
import { expect, test, type Locator, type Page } from '@playwright/test';
import { ChartHistory } from '../src/lib/charts/history';
import { CARRIES_CELLS, missingFor, type ClusterState, type EventBatch, type PacketKind, type StepUntil, type Tier } from '../src/lib/sim/types';
import {
  ARC_GAP,
  ARC_WIDTH,
  CONTROL,
  COUNT_FIT,
  CROSS,
  DROPPED,
  LABEL_GAP,
  PORT_GAP,
  RELAY,
  R_MAX,
  SELECTION_GAP,
  SELECTION_WIDTH,
  STRATA_MIN_INNER,
  beamPath,
  pointOn,
  portHalf,
  type Point,
  type QuadPath,
  type Spot,
} from '../src/lib/stage/layout';
import { MARK, RING_BEHIND, RING_CAUGHT_UP, type GlyphTable } from '../src/lib/stage/renderer';
import { VESSEL_SCALE } from '../src/lib/limit';
import { zoneAt } from '../src/lib/format';
import { PRESETS } from '../src/lib/presets';
import { BAND_ALPHA, BEAM_ALPHA, readTheme, type Theme } from '../src/lib/theme';
import timings from './phase-timings.json' with { type: 'json' };
import { PERIOD_MS, WATCH, period, refusal, send, sendButton, setSpeed, step, stepUntil, still, virtualMs } from './drive';

// How far a node's ring and its selection outline reach outside its disc,
// summed from the gap and stroke each is drawn with rather than read from the
// layout's own sums, so a sum that loses a term shows.
const RING_REACH = ARC_GAP + ARC_WIDTH / 2;
const SELECTION_REACH = SELECTION_GAP + SELECTION_WIDTH / 2;

// The automated half of the audit checklist in `../README.md` ("Design rules"):
// A1 to A9. Each audit project fixes a width and a colour scheme; the main test
// walks every preset a number of heartbeat periods by Step, at 4×, checks it,
// then checks it again with `data-theme` forcing the other theme. Full-page
// PNGs land in `audit-shots/` for the visual audit. More tests check chart 1's
// window and placement, the stage's geometry for every zone count, the beams,
// the interactions, and how every preset opens, goes quiet and fails over.
//
// A4 recomputes what the page prints from the engine's own snapshot, in this
// file's code and with its own formatting, never the page's, and A5 checks that
// the page prints it in (zone, id) order, never sorted by a value.

// Heartbeat periods each preset is walked: past its first waves, and short,
// to keep CI timely.
const STEPS = 4;
// The rule window every preset runs with (`SimConfig::default`, which no preset
// overrides).
const WINDOW_MS = 10_000;

type Findings = Record<'A1' | 'A2' | 'A3' | 'A7' | 'A8' | 'A9', string[]>;

// -- Formatting, written out here so the page's formatters are not reused --------

const int = (n: number): string => n.toLocaleString('en-US');
const percent = (x: number): string => `${(x * 100).toFixed(1)}%`;
const duration = (ms: number): string => {
  const whole = Math.round(ms);
  return whole < 1000 ? `${whole} ms` : `${(ms / 1000).toFixed(1)} s`;
};

// -- Page helpers --------------------------------------------------------------

const cell = (page: Page, id: number): Locator => page.locator(`[role="gridcell"][data-node="${id}"]`);
const field = (page: Page, name: string): Locator => page.locator(`[data-field="${name}"]`);

/** The page's engine, snapshotted: every node's state, or with `inspect` that
 *  node's rows too. */
const snapshot = (page: Page, inspect: number | null = null): Promise<ClusterState> =>
  page.evaluate(([id, watch]) => window.sim!.snapshot(id, watch), [inspect, WATCH] as const);

/** Load the page, wait for the stage, and open the rail where it folds away. */
async function open(page: Page): Promise<Locator> {
  await page.goto('/');
  const canvas = page.locator('.stage canvas');
  await expect(canvas).toBeVisible({ timeout: 30_000 });
  // Below 640 px the rail is a closed "Controls" disclosure; it stays open across rebuilds.
  const controls = page.locator('.controls > summary');
  if (await controls.isVisible()) await controls.click();
  return canvas;
}

/** Select a scenario by its label and wait for the rebuild to finish. The rail's
 *  `aria-current` marks the new cluster as live. */
async function loadPreset(page: Page, name: string): Promise<Locator> {
  const preset = page.getByRole('group', { name: 'Scenario preset' }).getByRole('button', { name, exact: true });
  if ((await preset.getAttribute('aria-current')) !== 'true') await preset.click();
  await expect(preset).toHaveAttribute('aria-current', 'true');
  await expect(page.locator('.overlay.rebuilding')).toHaveCount(0);
  return preset;
}

/** Open or close "Show as table" and wait for the stage layout to follow its
 *  shrinking or growing box, so no geometry is read mid-resize. */
async function setTable(page: Page, open: boolean): Promise<void> {
  const details = page.locator('details.node-table');
  if ((await details.evaluate((el: HTMLDetailsElement) => el.open)) !== open) {
    await details.locator('summary').click();
  }
  await expect(page.locator('.node-table table')).toHaveCount(open ? 1 : 0);
  await settled(page);
}

/** Wait until the layout the renderer published is for the stage's current box. */
async function settled(page: Page): Promise<void> {
  await expect
    .poll(() =>
      page.locator('.stage').evaluate((stage: HTMLElement) => {
        const centre = JSON.parse(stage.querySelector('canvas')?.dataset.centre ?? '{"x":0,"y":0}');
        return Math.abs(centre.x * 2 - stage.clientWidth) < 1 && Math.abs(centre.y * 2 - stage.clientHeight) < 1;
      }),
    )
    .toBe(true);
}

// -- The main audit ------------------------------------------------------------

test('every preset passes the automated design audit', async ({ page }, info) => {
  test.setTimeout(240_000);
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  page.on('console', (message) => {
    if (message.type() === 'error') errors.push(message.text());
  });
  page.on('requestfailed', (request) => errors.push(`request failed: ${request.url()}`));
  page.on('response', (response) => {
    if (response.status() >= 400) errors.push(`HTTP ${response.status()}: ${response.url()}`);
  });

  const canvas = await open(page);
  await setSpeed(page, 4);
  const forced = info.project.use.colorScheme === 'dark' ? 'light' : 'dark';
  const presets = page.getByRole('group', { name: 'Scenario preset' }).getByRole('button');
  const names = (await presets.allInnerTexts()).map((name) => name.trim());
  expect(names.length, 'no presets to audit').toBeGreaterThan(0);
  let converged = 0;
  let charted = 0;
  let inspected = 0;
  for (const name of names) {
    await loadPreset(page, name);
    await expect(canvas).toBeVisible();
    // Sandbox and Rate limiting open empty: a Send by hand at node 0 gives
    // their converged pass something to converge on, with no feed to make it
    // nondeterministic.
    if (name === 'Sandbox' || name === 'Rate limiting') await send(page, 0);
    // The engine's own snapshot after every Step, which A4 recomputes from.
    const series: ClusterState[] = [await snapshot(page)];
    for (let s = 0; s < STEPS; s++) await period(page, async () => void series.push(await snapshot(page)));
    converged += await page.locator('[data-template]', { hasText: /^✓/ }).count();
    charted += await page.locator('.plot').count();
    inspected += await page.locator('.inspector').count();

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
      const findings = await page.evaluate(audit, {
        theme: colours,
        beam: BEAM_ALPHA,
        band: BAND_ALPHA,
      });
      expect(findings, where).toEqual({ A1: [], A2: [], A3: [], A7: [], A8: [], A9: [] });
      await checkGlyphs(page, where, name);
      await expect
        .poll(() => stagePixel(page, canvas), {
          message: `${where}: the stage does not draw readTheme().bg (A7)`,
        })
        .toBe(colours.bg);
      await checkTooltips(page, where);
    }

    // A4 and A5 read the table, which shrinks the stage: they run after the
    // audit and the screenshots, which record the table closed. A5 runs first:
    // A4 reads a row per node and the chart panels in the order A5 checks, so a
    // missing row or a misplaced panel is reported as what it is.
    await setTable(page, true);
    await checkOrder(page, name);
    await checkData(page, name, series);
    await setTable(page, false);
  }
  // Text shown only once every node agrees (the converged badge) is checked too.
  expect(converged, 'no preset was audited in a converged state (A3)').toBeGreaterThan(0);
  expect(charted, 'no preset showed the charts, so none were checked (A3, A7)').toBeGreaterThan(0);
  expect(inspected, 'no preset opened with its node selected, so Gossip targets were never checked (A4)').toBeGreaterThan(0);
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

/** A3: a tooltip stays inside the stage. Hover the right-most and the
 *  bottom-most node, where it has the least room, then park the pointer outside
 *  so none leaks into the next audit. */
async function checkTooltips(page: Page, where: string): Promise<void> {
  const stage = page.locator('.stage');
  await stage.scrollIntoViewIfNeeded();
  const cells = await page.locator('[role="gridcell"]').evaluateAll((els) =>
    els.map((el) => {
      const r = el.getBoundingClientRect();
      return { x: r.x + r.width / 2, y: r.y + r.height / 2 };
    }),
  );
  const box = await stage.boundingBox();
  if (box === null) throw new Error('the stage has no box');
  const right = cells.reduce((a, b) => (b.x > a.x ? b : a));
  const bottom = cells.reduce((a, b) => (b.y > a.y ? b : a));
  for (const at of [right, bottom]) {
    await page.mouse.move(at.x, at.y);
    const tooltip = page.locator('.stage-box .tooltip');
    const tip = await tooltip.boundingBox();
    expect(tip, `${where}: no tooltip over the node at ${at.x}, ${at.y}`).not.toBeNull();
    if (tip === null) continue;
    // A4, rule 12: a node that lacks a change says how long, at least.
    const id = Number(await text(tooltip.locator('[data-field="node"]')));
    const node = (await snapshot(page)).nodes.find((n) => n.id === id);
    const missing = node === undefined ? undefined : missingFor(node.lag);
    const printed = tooltip.locator('[data-field="missing-for"]');
    if (missing === undefined) await expect(printed, `${where}: node ${id} lacks nothing`).toHaveCount(0);
    else await expect(printed, `${where}: node ${id} not caught up`).toHaveText(duration(missing));
    expect(tip.x, `${where}: the tooltip crosses the stage's left edge`).toBeGreaterThanOrEqual(box.x - 1);
    expect(tip.y, `${where}: the tooltip crosses the stage's top edge`).toBeGreaterThanOrEqual(box.y - 1);
    expect(tip.x + tip.width, `${where}: the tooltip crosses the stage's right edge`).toBeLessThanOrEqual(
      box.x + box.width + 1,
    );
    expect(tip.y + tip.height, `${where}: the tooltip crosses the stage's bottom edge`).toBeLessThanOrEqual(
      box.y + box.height + 1,
    );
  }
  await page.mouse.move(0, 0);
  await expect(page.locator('.stage-box .tooltip')).toHaveCount(0);
}

// -- A4: the printed numbers, recomputed ---------------------------------------

interface Totals {
  t: number;
  inData: number;
  crossData: number;
  own: Map<number, number>;
}

const totals = (s: ClusterState): Totals => ({
  t: s.virtual_ms,
  inData: s.bytes_by_tier.in_zone.data,
  crossData: s.bytes_by_tier.cross_zone.data,
  own: new Map(s.nodes.map((n) => [n.id, n.own_data_bytes])),
});

/** The cumulative totals the window ending at the last of `series` starts from.
 *  `series` holds a snapshot at every instant the page applied one. A snapshot
 *  is kept as a mark once `W / 128` has passed since the last kept one, and the
 *  window starts at the newest mark at or before `t − W`, or at the session
 *  start while the window reaches back past every mark. */
function windowStart(series: ClusterState[]): Totals {
  const now = series[series.length - 1].virtual_ms;
  let start: ClusterState | undefined;
  let kept: number | undefined;
  for (const snap of series) {
    if (kept !== undefined && snap.virtual_ms - kept < WINDOW_MS / 128) continue;
    kept = snap.virtual_ms;
    if (kept <= now - WINDOW_MS) start = snap;
  }
  return now <= WINDOW_MS || start === undefined
    ? { t: 0, inData: 0, crossData: 0, own: new Map() }
    : totals(start);
}

/** σ, the share of the window's data bytes that crossed zones, or null. */
function share(series: ClusterState[]): number | null {
  const now = series[series.length - 1];
  const from = windowStart(series);
  const cross = now.bytes_by_tier.cross_zone.data - from.crossData;
  const all = cross + now.bytes_by_tier.in_zone.data - from.inData;
  return all > 0 ? cross / all : null;
}

/** The two predictions of chart 1 from the placement and the window's bytes. */
function predictions(series: ClusterState[]): { zones: number; nozone: number } | null {
  const now = series[series.length - 1];
  const n = now.nodes.length;
  if (n < 2) return null;
  const from = windowStart(series);
  const live = now.zones.filter((z) => z.live > 0).length;
  let weight = 0;
  let weighted = 0;
  let plain = 0;
  for (const node of now.nodes) {
    const across = (n - now.zones[node.zone].live) / (n - 1);
    const bytes = node.own_data_bytes - (from.own.get(node.id) ?? 0);
    weight += bytes;
    weighted += bytes * across;
    plain += across;
  }
  return { zones: (live - 1) / (n - 1), nozone: weight > 0 ? weighted / weight : plain / n };
}

/** The spread of the views of `nodes`: most informed minus least. */
const spread = (nodes: { window_total: number }[]): number =>
  Math.max(...nodes.map((n) => n.window_total)) - Math.min(...nodes.map((n) => n.window_total));

const text = async (locator: Locator): Promise<string> => ((await locator.textContent()) ?? '').trim();

/** A4: every number the page prints, against the engine's own snapshot. `series`
 *  is the snapshot after every Step, so the windowed share can be recomputed. */
async function checkData(page: Page, name: string, series: ClusterState[]): Promise<void> {
  const where = `${name} (A4)`;
  const snap = series[series.length - 1];
  const nodes = snap.nodes;
  const oracle = snap.oracle_total;

  // The status sentence's counts.
  await expect(field(page, 'status-nodes'), where).toHaveText(String(nodes.length));
  await expect(field(page, 'status-zones'), where).toHaveCount(name === 'No zone configured' ? 0 : 1);
  if ((await field(page, 'status-zones').count()) === 1) {
    await expect(field(page, 'status-zones'), where).toHaveText(
      String(snap.zones.filter((z) => z.live > 0).length),
    );
  }
  await checkHeading(page, where, snap);

  // Each node's behind-by, view and the origins it is a gateway for, from every
  // node's own peer rows inverted.
  const gatewayFor = new Map<number, number>();
  for (const node of nodes) {
    const rows = (await snapshot(page, node.id)).inspected?.peers ?? [];
    for (const peer of rows) {
      if (peer.role === 'gateway' && peer.id != null) gatewayFor.set(peer.id, (gatewayFor.get(peer.id) ?? 0) + 1);
    }
  }
  for (const node of nodes) {
    const row = page.locator(`.node-table tr[data-node="${node.id}"]`);
    await expect(row.locator('[data-field="view"]'), `${where}: node ${node.id} view`).toHaveText(
      int(node.window_total),
    );
    if (node.lag.kind !== 'Stranded') {
      expect(node.window_total, `${where}: node ${node.id} sees no more than the truth`).toBeLessThanOrEqual(oracle);
    }
    await expect(row.locator('[data-field="behind"]'), `${where}: node ${node.id} behind`).toHaveText(
      int(oracle - node.window_total),
    );
    const want = gatewayFor.get(node.id) ?? 0;
    const printed = row.locator('[data-field="gateway-for"]');
    if (want === 0) await expect(printed, `${where}: node ${node.id} is a gateway for nothing`).toHaveCount(0);
    else await expect(printed, `${where}: node ${node.id} gateway for`).toHaveText(int(want));
  }

  // The charts (shown while no node is selected).
  if ((await page.locator('.dashboard').count()) > 0) {
    await expect(field(page, 'control-share'), where).toHaveText(
      percent(
        (() => {
          const { in_zone: a, cross_zone: b } = snap.bytes_by_tier;
          const control = a.control + b.control;
          return control / (control + a.data + b.data);
        })(),
      ),
    );
    const sigma = share(series);
    const seen = series.some((_, i) => share(series.slice(0, i + 1)) !== null);
    if (snap.zones.length > 1 && seen) {
      await expect(field(page, 'share'), `${where}: σ`).toHaveText(sigma === null ? '—' : percent(sigma));
    }
    const spreads = page.locator('section.block', { hasText: /^Disagreement by zone/ }).locator('figure');
    const live = snap.zones.flatMap((z, i) => (z.live > 0 ? [i] : []));
    await expect(spreads, where).toHaveCount(live.length + 1);
    await expect(
      page.locator('section.block', { hasText: /^Agreement by zone/ }).locator('figure'),
      `${where}: agreement panels`,
    ).toHaveCount(live.length);
    for (const [i, zone] of live.entries()) {
      await expect(spreads.nth(i).locator('[data-field="spread"]'), `${where}: zone ${zone} spread`).toHaveText(
        int(spread(nodes.filter((n) => n.zone === zone))),
      );
    }
    await expect(spreads.nth(live.length).locator('[data-field="spread"]'), `${where}: whole cluster`).toHaveText(
      int(spread(nodes)),
    );
  }

  // The selected origin's Gossip targets: each remote zone's gateway, unacked
  // positions, wait and the counters.
  if ((await page.locator('.inspector').count()) > 0) {
    const origin = Number(await text(page.locator('.pane-title [data-field="node"]')));
    const inspected = (await snapshot(page, origin)).inspected;
    const state = nodes.find((n) => n.id === origin)!;
    const peers = inspected?.peers ?? [];
    const remote = snap.zones.flatMap((z, i) => (i !== state.zone && z.live > 0 ? [i] : []));
    const configured = peers.some((p) => p.zone != null);
    const rows = page.locator('.inspector tr.remote');
    await expect(rows, `${where}: remote rows`).toHaveCount(configured || peers.length === 0 ? remote.length : 0);
    if (configured) {
      for (const zone of remote) {
        const row = page.locator(`.inspector tr.remote[data-zone="${zone}"]`);
        const gateway = peers.find((p) => p.role === 'gateway' && p.zone === zone);
        if (gateway === undefined) {
          await expect(row, `${where}: zone ${zone} has no gateway`).toContainText('no gateway yet');
          continue;
        }
        await expect(row.locator('[data-field="gateway"]'), `${where}: zone ${zone} gateway`).toHaveText(
          String(gateway.id),
        );
        await expect(row.locator('[data-field="unacked"]'), `${where}: zone ${zone} unacked`).toHaveText(
          int(gateway.own_unacked),
        );
        const waiting = row.locator('[data-field="waiting"]');
        if (gateway.unanswered_ms == null) await expect(waiting).toHaveCount(0);
        else await expect(waiting, `${where}: zone ${zone} waiting`).toHaveText(duration(gateway.unanswered_ms));
      }
    }
    const counters = {
      failovers: state.gateway_failovers,
      'gap-reports': state.gap_reports_sent,
      retransmissions: state.rto_rewinds_own + state.rto_rewinds_relay,
      sweeps: state.sweeps_served,
    };
    for (const [key, want] of Object.entries(counters)) {
      await expect(page.locator(`.inspector tr.counters [data-field="${key}"]`), `${where}: ${key}`).toHaveText(
        int(want),
      );
    }
  }
}

/** A18: the stage heading, recomputed from the engine's snapshot: its form,
 *  the truth it prints, how many nodes are up, the gap, the reopening
 *  countdown and rule 12's censored "≥". */
async function checkHeading(page: Page, where: string, snap: ClusterState): Promise<void> {
  const h = page.locator('[data-template]');
  const id = await h.getAttribute('data-template');
  const nodes = snap.nodes;
  const t = snap.oracle_total;
  if (id === 'hd-feed') return;
  if (id === 'rl-4') {
    expect(t, `${where}: the window is empty`).toBe(0);
    return;
  }
  const claims = async (noun: string): Promise<string[]> =>
    (await h.locator(`[data-noun="${noun}"]`).allTextContents()).map((v) => v.trim());
  const view = await claims('view');
  if (view.length > 0) expect(view[0], `${where}: the heading's truth`).toBe(int(t));
  const up = nodes.filter((n) => n.lag.kind === 'CaughtUp').length;
  const stranded = nodes.filter((n) => n.lag.kind === 'Stranded').length;
  if (id === 'hd-stranded') {
    expect(up, `${where}: every reachable node is up`).toBe(nodes.length - stranded);
    return;
  }
  const printed = ((await h.textContent()) ?? '').replace(/\s+/g, ' ').trim();
  if (up < nodes.length) {
    expect(printed.startsWith(`${up} of ${nodes.length} `), `${where}: "${printed}" counts ${up} up`).toBe(true);
    const gap = Math.max(...nodes.filter((n) => n.lag.kind !== 'Stranded').map((n) => t - n.window_total));
    expect((await claims('behind'))[0], `${where}: the gap`).toBe(int(gap));
  } else {
    expect(printed.includes(`All ${nodes.length} nodes`), `${where}: "${printed}" says every node`).toBe(true);
  }
  const selected = Number(await text(page.locator('.pane-title [data-field="node"]')));
  const next = (await snapshot(page, selected)).inspected?.next_send;
  const reopens = await claims('reopens');
  if (id === 'rl-3' && next?.kind === 'Rejected' && next.reopens.kind === 'In') {
    expect(reopens, `${where}: the countdown`).toEqual([`${(Math.floor(next.reopens.ms / 100) / 10).toFixed(1)} s`]);
  } else {
    expect(reopens, `${where}: no countdown while node ${selected} admits`).toEqual([]);
  }
  let longest = 0;
  for (const node of nodes) if (node.lag.kind === 'Behind') longest = Math.max(longest, node.lag.missing_for_ms);
  const late = h.locator('[data-noun="behind"]', { hasText: /\d+(\.\d)? m?s/ });
  if (longest >= PERIOD_MS) await expect(late, `${where}: not caught up`).toHaveText(duration(longest));
  else await expect(late, `${where}: rule 12 prints from one period up`).toHaveCount(0);
}

/** A42 (static): every node mark as the stage published it, against the
 *  layout, the DOM's counts and names, and the engine's snapshot. */
async function checkGlyphs(page: Page, where: string, name: string): Promise<void> {
  const snap = await snapshot(page);
  const feed = PRESETS.find((p) => p.label === name)?.traffic !== undefined;
  // Each node's name: its id, and the fault the engine was given that picks it out.
  const names = new Map(snap.nodes.map((n) => [n.id, `node ${n.id}`]));
  for (const cut of await page.evaluate(() => window.sim!.cuts())) {
    if (cut.kind === 'silenced') names.set(cut.node, `node ${cut.node} · silenced`);
    else if (cut.kind === 'unheard-in-zone') names.set(cut.node, `node ${cut.node} · can't reach ${zoneAt(snap.zones, cut.zone)}`);
  }
  const findings = await page.evaluate(
    ({ k, nodes, grace, names }) => {
      const found: string[] = [];
      const c = document.querySelector('.stage canvas') as HTMLCanvasElement & { glyphs: GlyphTable };
      const g = c.glyphs;
      const rings = JSON.parse(c.dataset.rings ?? '[]') as { x: number; y: number }[];
      const labels = (JSON.parse(c.dataset.labelBoxes ?? '[]') as { text: string; x: number; y: number; w: number; h: number; size: number }[]).filter((l) => l.text.startsWith('node'));
      const at = c.getBoundingClientRect();
      const zoneOf = new Map<number, number>();
      for (const row of document.querySelectorAll<HTMLElement>('[role="grid"] [role="row"]')) {
        for (const cell of row.querySelectorAll<HTMLElement>('[role="gridcell"]')) zoneOf.set(Number(cell.dataset.node), Number(row.dataset.zone));
      }
      const byId = new Map(nodes.map((n) => [n.id, n]));
      const slots: number[] = [];
      for (let s = 0; s < g.len; s++) if (g.id[s] >= 0) slots.push(s);
      // The DOM counts, as boxes of their width and figure height, in canvas px.
      const counts = [...document.querySelectorAll<HTMLElement>('.count')];
      const ctx = new OffscreenCanvas(1, 1).getContext('2d')!;
      const boxes = counts.map((el) => {
        const r = el.getBoundingClientRect();
        // Not the computed `font` shorthand: it serializes empty, and the
        // canvas would keep its 10 px default.
        const style = getComputedStyle(el);
        ctx.font = `${style.fontWeight} ${style.fontSize} ${style.fontFamily}`;
        const cap = ctx.measureText('0').actualBoundingBoxAscent;
        const cx = r.left + r.width / 2 - at.left;
        const cy = r.top + r.height / 2 - at.top;
        return { id: Number(el.closest<HTMLElement>('[data-node]')?.dataset.node), x0: cx - r.width / 2, x1: cx + r.width / 2, y0: cy - cap / 2, y1: cy + cap / 2, w: r.width, cap, size: parseFloat(getComputedStyle(el).fontSize) };
      });
      if (counts.length !== 0 && counts.length !== slots.length) found.push(`${counts.length} of ${slots.length} counts shown: all or none`);
      if (innerWidth >= 1100 && counts.length === 0) found.push('no count shown at 1100 px or wider');
      const overlaps = (a: { x0: number; x1: number; y0: number; y1: number }, b: { x0: number; x1: number; y0: number; y1: number }): boolean =>
        a.x0 < b.x1 && b.x0 < a.x1 && a.y0 < b.y1 && b.y0 < a.y1;
      for (const s of slots) {
        const id = g.id[s];
        const [x, y, r] = [g.x[s], g.y[s], g.r[s]];
        const node = byId.get(id);
        const box = boxes.find((b) => b.id === id);
        if (box !== undefined) {
          if ((box.w / 2) ** 2 + (box.cap / 2) ** 2 > (k.COUNT_FIT * r) ** 2 + 0.5) found.push(`node ${id}'s count does not fit its disc`);
          if (Math.abs((box.x0 + box.x1) / 2 - x) > 1 || Math.abs((box.y0 + box.y1) / 2 - y) > 1.5) found.push(`node ${id}'s count is off its disc's centre`);
        }
        // The vessel: the fill's top and the brim, on the 1.25-limit scale.
        if (g.onScale === 1 && node !== undefined) {
          const inner = 2 * (r - 1);
          const full = k.VESSEL_SCALE * g.limit;
          const top = r - 1 - (inner * Math.min(node.window_total, full)) / full;
          const brim = r - 1 - inner / k.VESSEL_SCALE;
          if (Math.abs(g.fillTop[s] - top) > 0.5) found.push(`node ${id}'s fill tops at ${g.fillTop[s].toFixed(1)}, not ${top.toFixed(1)}`);
          if (Math.abs(g.brimY[s] - brim) > 0.5) found.push(`node ${id}'s brim at ${g.brimY[s].toFixed(1)}, not ${brim.toFixed(1)}`);
          if ((g.cap[s] === 1) !== (node.brim.kind === 'Rejected')) found.push(`node ${id}'s cap does not follow its brim decision`);
        } else if (g.onScale === 0 && !Number.isNaN(g.brimY[s])) {
          found.push(`node ${id} draws a brim though the limit is off the scale`);
        }
        // The ring's colour role, from the lag past the feed's grace.
        if (node !== undefined) {
          const behind = node.lag.kind !== 'CaughtUp' && node.lag.missing_for_ms >= grace;
          if (g.ring[s] !== (behind ? k.RING_BEHIND : k.RING_CAUGHT_UP)) found.push(`node ${id}'s ring role does not follow its lag`);
        }
        // A port: on its circle, toward its own zone's centre, clear of its disc,
        // of every ring and of every count.
        if (!Number.isNaN(g.portX[s])) {
          const ring = rings[zoneOf.get(id) ?? -1];
          const want = Math.atan2(ring.y - y, ring.x - x);
          const got = Math.atan2(g.portY[s] - y, g.portX[s] - x);
          const off = Math.abs(((got - want + 3 * Math.PI) % (2 * Math.PI)) - Math.PI);
          if (off > Math.PI / 180) found.push(`node ${id}'s port points ${((off * 180) / Math.PI).toFixed(1)}° off its zone's centre`);
          const half = Math.max(k.PORT_MIN, r / 3);
          const d = Math.hypot(g.portX[s] - x, g.portY[s] - y);
          if (Math.abs(d - (r + k.RING_REACH + half + k.PORT_GAP)) > 0.5) found.push(`node ${id}'s port is not on its port circle`);
          if (d - half <= r) found.push(`node ${id}'s port covers its own disc`);
          const port = { x0: g.portX[s] - half, x1: g.portX[s] + half, y0: g.portY[s] - half, y1: g.portY[s] + half };
          for (const t of slots) {
            const dt = Math.hypot(g.portX[s] - g.x[t], g.portY[s] - g.y[t]);
            if (Math.abs(dt - (g.r[t] + k.ARC_GAP)) < k.ARC_WIDTH / 2 + half - 1e-6) found.push(`node ${id}'s port crosses node ${g.id[t]}'s ring`);
          }
          for (const b of boxes) if (overlaps(port, b)) found.push(`node ${id}'s port covers node ${b.id}'s count`);
        }
      }
      // The selection outline: clear of every other node's ring.
      if (g.selected >= 0) {
        const s = g.selected;
        for (const t of slots) {
          if (t === s) continue;
          const d = Math.hypot(g.x[s] - g.x[t], g.y[s] - g.y[t]);
          if (d < g.r[s] + g.r[t] + k.SELECTION_REACH + k.RING_REACH - 1e-6) found.push(`the selection outline crosses node ${g.id[t]}'s ring`);
        }
      }
      // The names: what they say, smaller than the counts (A3 keeps them inside
      // the canvas), apart from one another and clear of every ring and count.
      for (const [i, l] of labels.entries()) {
        const id = Number(/^node (\d+)/.exec(l.text)?.[1]);
        if (l.text !== names[id]) found.push(`the name "${l.text}" is not node ${id}'s name, "${names[id]}"`);
        if (boxes.length > 0 && l.size >= boxes[0].size) found.push(`the name "${l.text}" is not smaller than the counts`);
        const box = { x0: l.x, x1: l.x + l.w, y0: l.y, y1: l.y + l.h };
        for (const o of labels.slice(i + 1)) {
          const vertical = l.y < o.y + o.h && o.y < l.y + l.h;
          const apart = Math.max(o.x - (l.x + l.w), l.x - (o.x + o.w));
          // Twelve px apart where the stage has room for its names (1100 px
          // and wider); clear of each other everywhere.
          const least = innerWidth >= 1100 ? 12 : 1;
          if (vertical && apart < least - 1e-6) found.push(`"${l.text}" and "${o.text}" stand ${apart.toFixed(1)} px apart`);
        }
        for (const t of slots) {
          const nx = Math.max(box.x0, Math.min(g.x[t], box.x1));
          const ny = Math.max(box.y0, Math.min(g.y[t], box.y1));
          const reach = g.r[t] + (t === g.selected ? k.SELECTION_REACH : k.RING_REACH);
          if (Math.hypot(g.x[t] - nx, g.y[t] - ny) <= reach) found.push(`the name "${l.text}" touches node ${g.id[t]}'s marks`);
        }
        for (const b of boxes) if (overlaps(box, b)) found.push(`the name "${l.text}" covers node ${b.id}'s count`);
        // A name stands against its own disc: next to it, overlapping the
        // disc's span across or down the stage, so it names the node it
        // stands by.
        const own = slots.find((t) => g.id[t] === id);
        if (own !== undefined) {
          const [x, y, r] = [g.x[own], g.y[own], g.r[own]];
          const spans = (box.x0 < x + r && x - r < box.x1) || (box.y0 < y + r && y - r < box.y1);
          const gap = Math.hypot(Math.max(box.x0 - x, 0, x - box.x1), Math.max(box.y0 - y, 0, y - box.y1));
          if (!spans || gap > r + k.RING_REACH + k.LABEL_GAP + 0.5) found.push(`the name "${l.text}" does not stand against node ${id}'s disc`);
        }
      }
      if (labels.length !== slots.length) found.push(`${labels.length} names for ${slots.length} nodes`);
      if (g.onScale === 0 && (g.marks & k.FAR) === 0) found.push('the limit is off the scale but the key does not say so');
      return found;
    },
    {
      k: { COUNT_FIT, VESSEL_SCALE, RING_REACH, LABEL_GAP, SELECTION_REACH, PORT_GAP, PORT_MIN: portHalf(0), ARC_GAP, ARC_WIDTH, RING_BEHIND, RING_CAUGHT_UP, FAR: MARK.FAR },
      nodes: snap.nodes.map((n) => ({ id: n.id, lag: n.lag, window_total: n.window_total, brim: n.brim })),
      grace: feed ? PERIOD_MS + 5 : 0,
      names: Object.fromEntries(names),
    },
  );
  expect(findings, `${where} (A42)`).toEqual([]);
}

// -- A5: fixed order -----------------------------------------------------------

/** A5: the grid, the table, the chart panels and the Gossip-targets rows are in
 *  (zone, id) order, never sorted by a value. */
async function checkOrder(page: Page, name: string): Promise<void> {
  const where = `${name} (A5)`;
  const grid = await page.evaluate(() =>
    Array.from(document.querySelectorAll('[role="grid"] [role="row"]')).map((row) => ({
      zone: Number((row as HTMLElement).dataset.zone),
      ids: Array.from(row.querySelectorAll('[role="gridcell"]')).map((c) => Number((c as HTMLElement).dataset.node)),
    })),
  );
  const ascending = (xs: number[]): boolean => xs.every((x, i) => i === 0 || xs[i - 1] < x);
  expect(ascending(grid.map((r) => r.zone)), `${where}: grid rows by zone`).toBe(true);
  for (const row of grid) expect(ascending(row.ids), `${where}: zone ${row.zone} cells by id`).toBe(true);

  const table = await page.evaluate(() =>
    Array.from(document.querySelectorAll('.node-table tr[data-node]')).map((tr) => [
      Number((tr as HTMLElement).dataset.zone),
      Number((tr as HTMLElement).dataset.node),
    ]),
  );
  expect(
    table.every(([z, id], i) => i === 0 || z > table[i - 1][0] || (z === table[i - 1][0] && id > table[i - 1][1])),
    `${where}: table rows by (zone, id)`,
  ).toBe(true);
  expect(table.map(([, id]) => id).sort((a, b) => a - b), `${where}: the table lists the grid's nodes`).toEqual(
    grid.flatMap((r) => r.ids).sort((a, b) => a - b),
  );

  for (const title of ['Agreement by zone', 'Disagreement by zone']) {
    const panels = await page
      .locator('section.block', { hasText: new RegExp(`^${title}`) })
      .locator('figure')
      .evaluateAll((els) => els.map((el) => (el as HTMLElement).dataset.zone ?? null));
    if (panels.length === 0) continue;
    const zones = panels.filter((z) => z !== null).map(Number);
    expect(ascending(zones), `${where}: ${title} panels by zone`).toBe(true);
    // Chart 3 ends with the whole cluster, which has no zone.
    if (title.startsWith('Disagreement')) expect(panels.at(-1), `${where}: whole cluster last`).toBeNull();
  }

  const remote = await page
    .locator('.inspector tr.remote')
    .evaluateAll((els) => els.map((el) => Number((el as HTMLElement).dataset.zone)));
  expect(ascending(remote), `${where}: Gossip targets rows by zone`).toBe(true);
}

// -- In the page ---------------------------------------------------------------

/** Runs in the page: findings for A1 (sideways scroll), A2, A3 (DOM text, canvas
 *  labels, legibility and chart marks), A7, A8 and A9, one sentence each, given
 *  the canvas colours and the alphas of the translucent canvas marks.
 *  Self-contained: Playwright ships it to the page as source. */
function audit({ theme, beam, band }: { theme: Theme; beam: number; band: number }): Findings {
  type Rgba = [number, number, number, number];
  type Box = { left: number; right: number; top: number; bottom: number };
  const found: Findings = { A1: [], A2: [], A3: [], A7: [], A8: [], A9: [] };

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

  /** Clip `box` through the overflow boxes between `el` and the body, as the
   *  browser does. A scroller brings content back into view, so only a hidden box
   *  before the first scroller counts as clipping; content a scroller has scrolled
   *  away is trimmed. Returns the visible part, or null when none is. */
  const clip = (el: Element, from: Box): { box: Box | null; cutOff: boolean } => {
    let box = from;
    let scrolls = false;
    let cutOff = false;
    for (let a: Element | null = el; a !== null && a !== document.body; a = a.parentElement) {
      const style = getComputedStyle(a);
      const overflow = `${style.overflowX} ${style.overflowY}`;
      if (overflow === 'visible visible') continue;
      const edge = a.getBoundingClientRect();
      if (
        !scrolls &&
        /hidden|clip/.test(overflow) &&
        (box.left < edge.left - 1 || box.right > edge.right + 1 || box.top < edge.top - 1 || box.bottom > edge.bottom + 1)
      ) {
        cutOff = true;
      }
      if (/auto|scroll/.test(overflow)) scrolls = true;
      box = {
        left: Math.max(box.left, edge.left),
        right: Math.min(box.right, edge.right),
        top: Math.max(box.top, edge.top),
        bottom: Math.min(box.bottom, edge.bottom),
      };
    }
    return { box: box.right - box.left >= 1 && box.bottom - box.top >= 1 ? box : null, cutOff };
  };

  // The smallest type the page uses: every text node and canvas label is at least this.
  const floor = theme.captionSmPx;

  // Text walk: A3 clipping, A3 overlaps (boxes collected here), A3 legibility and
  // A7 text contrast.
  const boxes: { key: object; name: string; box: Box }[] = [];
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
      const clipped = clip(el, {
        left: rect.left,
        right: rect.right,
        top: rect.top + inset,
        bottom: rect.bottom - inset,
      });
      cutOff ||= clipped.cutOff;
      if (clipped.box !== null) boxes.push({ key: text, name, box: clipped.box });
    }
    if (cutOff) found.A3.push(`"${name}" is cut off by an overflow: hidden box`);

    // A3 legibility: no text below the smallest type token.
    const size = parseFloat(getComputedStyle(el).fontSize);
    if (size < floor - 0.01) found.A3.push(`"${name}" is ${size.toFixed(1)} px, below the ${floor.toFixed(1)} px floor`);

    // A7 text contrast. Disabled controls are exempt (WCAG 2.5.8's own exception).
    if (el.closest(':disabled') === null) {
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

  // A3 canvas labels: the boxes the stage and every chart publish join the text
  // boxes in the overlap check, clipped through their scrollers the same way.
  for (const canvas of document.querySelectorAll<HTMLCanvasElement>('canvas[data-label-boxes]')) {
    if (!canvas.checkVisibility()) continue;
    const at = canvas.getBoundingClientRect();
    const labels = JSON.parse(canvas.dataset.labelBoxes ?? '[]') as {
      text: string;
      x: number;
      y: number;
      w: number;
      h: number;
      size: number;
    }[];
    for (const l of labels) {
      if (l.size < floor - 0.01) {
        found.A3.push(`canvas label "${l.text}" is ${l.size.toFixed(1)} px, below the ${floor.toFixed(1)} px floor`);
      }
      if (l.x < 0 || l.y < 0 || l.x + l.w > canvas.clientWidth || l.y + l.h > canvas.clientHeight) {
        found.A3.push(`canvas label "${l.text}" leaves its canvas`);
      }
      const clipped = clip(canvas, {
        left: at.left + l.x,
        right: at.left + l.x + l.w,
        top: at.top + l.y,
        bottom: at.top + l.y + l.h,
      });
      if (clipped.cutOff) found.A3.push(`canvas label "${l.text}" is cut off by an overflow: hidden box`);
      if (clipped.box !== null) boxes.push({ key: l, name: `canvas "${l.text}"`, box: clipped.box });
    }
    // The stage names every node it draws: a node the labeller found no room
    // for leaves its disc unnamed.
    if (canvas.closest('.stage') !== null) {
      const drawn = new Set(labels.map((l) => /^node (\d+)/.exec(l.text)?.[1]));
      for (const c of document.querySelectorAll<HTMLElement>('[role="gridcell"][data-node]')) {
        const id = c.dataset.node ?? '';
        if (!drawn.has(id)) found.A3.push(`the stage draws node ${id} with no name`);
      }
    }
  }

  // A3 overlaps: two text boxes that share at least 1 px both ways.
  for (let i = 0; i < boxes.length; i++) {
    for (let j = i + 1; j < boxes.length; j++) {
      const [a, b] = [boxes[i], boxes[j]];
      if (a.key === b.key) continue;
      const w = Math.min(a.box.right, b.box.right) - Math.max(a.box.left, b.box.left);
      const h = Math.min(a.box.bottom, b.box.bottom) - Math.max(a.box.top, b.box.top);
      if (w >= 1 && h >= 1) {
        found.A3.push(`${a.name.startsWith('canvas') ? a.name : `"${a.name}"`} overlaps ${b.name.startsWith('canvas') ? b.name : `"${b.name}"`} by ${w.toFixed(1)} × ${h.toFixed(1)} px`);
      }
    }
  }

  // A3 chart marks: every value of every series lies inside its plot's y range,
  // and every y axis shown labels at least two values, so the scale reads.
  for (const host of document.querySelectorAll<HTMLElement & { uplot?: any }>('.plot')) {
    const u = host.uplot;
    if (u === undefined || !host.checkVisibility()) continue;
    const ticks = (u.axes[1]._splits ?? []).length;
    if (u.axes[1].show !== false && ticks < 2) {
      found.A3.push(`a chart's y axis (zone ${host.closest('figure')?.dataset.zone}) labels ${ticks} value(s), not two or more`);
    }
    const { min, max } = u.scales.y;
    for (let s = 1; s < u.data.length; s++) {
      for (const v of u.data[s] as (number | null)[]) {
        if (v !== null && v !== undefined && (v < min - 1e-9 || v > max + 1e-9)) {
          found.A3.push(`a chart value ${v} (series ${s} of ${u.data.length - 1}, zone ${host.closest('figure')?.dataset.zone}, ${u.data[0].length} samples) lies outside its plot's range ${min} to ${max}`);
          break;
        }
      }
    }
  }

  // A7 targets: at least 24 × 24 px. The exemptions are WCAG 2.5.8's: targets
  // inline in a sentence (links in prose, and the `InfoTip` terms, which are
  // buttons the browser lays out as inline blocks) and disabled controls.
  for (const el of document.querySelectorAll('a[href], button, input, select, summary')) {
    if (!el.checkVisibility() || el.matches(':disabled, .term')) continue;
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
  // surface, a translucent mark composited over it first. Marks drawn faded as
  // context for a selection (`FADED_ALPHA`) and the age fade of a line
  // are exempt by name (the README's Beams paragraph); the ring
  // guides repeat what position and the ring title say (structure, not data) and
  // are exempt too.
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
    ['--color-fg cross-zone beam', unpack(theme.fg, beam), 3],
    ['--color-ok', unpack(theme.ok), 3],
    ['--color-warn', unpack(theme.warn), 3],
  ]);
  const plot = document.querySelector('.plot');
  if (plot !== null) {
    const panel = backdrop(plot);
    check(panel, 'charts', [
      ['--color-fg', unpack(theme.fg), 4.5],
      ['--color-fg-muted', unpack(theme.fgMuted), 4.5],
      ['--color-warn', unpack(theme.warn), 4.5],
      ['--chart-muted', unpack(theme.muted), 3],
      ['--chart-reference', unpack(theme.reference), 3],
    ]);
    // "REJECTING" is text drawn inside its own translucent warn band.
    const warnBand = over(unpack(theme.warn, band), panel);
    check(warnBand, 'REJECTING band', [['--color-fg', unpack(theme.fg), 4.5]]);
  }

  // A2 and A8: page-text scans. "CI" is case-sensitive so "ci" in words cannot match.
  const pageText = document.body.innerText;
  const scan = (into: string[], patterns: RegExp[]): void => {
    for (const pattern of patterns) {
      for (const match of pageText.matchAll(pattern)) into.push(`page text contains "${match[0]}"`);
    }
  };
  scan(found.A2, [/\bNaN\b|\bundefined\b|\bnull\b|\bInfinity\b|\[object|\de\+\d/g]);
  // A3: glyphs that carry meaning are drawn, never set from a font.
  scan(found.A3, [/[◆◇●▸❙❚↺]/g]);
  scan(found.A8, [/confidence|significant|p-value|standard error/gi, /\bCI\b/g, /\b\d{1,2}:\d{2}\b/g]);

  // A9: no digit outside a data-bound element, visible or not. "level 0" and
  // "level 1" are the glossary's names for the two tiers, not numbers.
  const all = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  for (let node = all.nextNode(); node !== null; node = all.nextNode()) {
    const el = node.parentElement;
    if (el === null || el.closest('script, style, [data-field]') !== null) continue;
    const bare = (node.textContent ?? '').replace(/\blevel [01]\b/g, '');
    if (/\d/.test(bare)) found.A9.push(`"${label(node.textContent)}" prints a number outside a data-field element`);
  }
  return found;
}

// -- A4: chart 1 across the window, and unequal zones ---------------------------

test('chart 1 prints the windowed share and follows the placement', async ({ page }, info) => {
  test.skip(
    !['audit-1440-light', 'audit-400-light'].includes(info.project.name),
    'the numbers are the same at every width and theme: checked at the widest and the narrowest',
  );
  test.setTimeout(240_000);
  await open(page);
  await setSpeed(page, 4);

  // Steady state after one heartbeat period: σ is printed, not the empty
  // state. A snapshot after every Step: the page applies one at each Step's
  // instant, and the window's start depends on which of them it kept.
  await loadPreset(page, 'Steady state');
  const series: ClusterState[] = [await snapshot(page)];
  const record = async (): Promise<void> => void series.push(await snapshot(page));
  await period(page, record);
  const first = share(series);
  expect(first, 'one heartbeat period of Steady state sends data bytes').not.toBeNull();
  await expect(field(page, 'share')).toHaveText(percent(first!));
  await expect(page.getByText('No traffic yet')).toHaveCount(0);
  // The axes read seconds of virtual time and percent: each tick's value
  // from the split it labels (milliseconds, and a share of 1).
  const ticks = await page
    .locator('section.block', { hasText: 'Share of data bytes' })
    .locator('.plot')
    .evaluate((host: HTMLElement & { uplot?: any }) => host.uplot.axes.map((a: { _splits: number[]; _values: number[] }) => [a._splits, a._values]));
  const [[xSplits, xValues], [ySplits, yValues]] = ticks as [number[], number[]][];
  expect(xValues, 'the x axis reads seconds').toEqual(xSplits.map((t) => t / 1000));
  expect(yValues, 'the y axis reads percent').toEqual(ySplits.map((t) => t * 100));
  expect([yValues[0], yValues.at(-1)], 'the y axis runs from 0 to 100 %').toEqual([0, 100]);

  // Thirty periods on, the window (ten seconds) has slid past the start: σ is
  // the share of the last window's bytes, not the whole run's.
  for (let i = 0; i < 30; i++) await period(page, record);
  const now = series[series.length - 1];
  expect(now.virtual_ms, 'the run is past one window').toBeGreaterThan(WINDOW_MS);
  await expect(field(page, 'share')).toHaveText(percent(share(series)!));
  const refs = predictions(series)!;
  const figure = page.locator('figure[data-ref-zones]');
  expect(Math.abs(Number(await figure.getAttribute('data-ref-zones')) - refs.zones)).toBeLessThan(1e-9);
  expect(Math.abs(Number(await figure.getAttribute('data-ref-nozone')) - refs.nozone)).toBeLessThan(1e-9);

  // Sandbox with unequal zones: Add node in zone a (5, 4, 4), Sends at nodes 0
  // and 1, then 26 heartbeat periods (past one window) with another Send at
  // node 0 four periods from the end. Both references follow the placement and
  // the bytes each origin sent in the window, which opens at a kept mark, not
  // the session start, so the mark's per-origin bytes count.
  await loadPreset(page, 'Sandbox');
  // Every snapshot the page takes is recorded as it takes it: a Send and an
  // Add each run until the cluster settles, and the chart's marks are kept
  // from every snapshot of that run.
  await page.evaluate(() => {
    const w = window as unknown as { taken: ClusterState[] };
    w.taken = [];
    const sim = window.sim! as typeof window.sim & { recorded?: boolean };
    if (sim.recorded === true) return;
    sim.recorded = true;
    const take = sim.snapshot.bind(sim);
    sim.snapshot = async (...args) => {
      const snap = await take(...args);
      w.taken.push(snap);
      return snap;
    };
  });
  await page.getByRole('button', { name: 'Add node' }).click();
  await expect(page.locator('[data-field="status-nodes"]')).toHaveText('13');
  await expect(page.locator('.stage-pane')).toHaveAttribute('data-clock', 'paused', { timeout: 30_000 });
  await send(page, 0);
  await send(page, 1);
  for (let i = 0; i < 26; i++) {
    if (i === 22) await send(page, 0);
    await period(page);
  }
  const sandbox = (await page.evaluate(() => (window as unknown as { taken: ClusterState[] }).taken)).filter(
    (snap, i, all) => i === 0 || snap.virtual_ms >= all[i - 1].virtual_ms,
  );
  expect(sandbox[0].zones.map((z) => z.live), 'zones of 5, 4 and 4').toEqual([5, 4, 4]);
  const opened = windowStart(sandbox);
  expect(opened.t, 'the window opens at a kept mark').toBeGreaterThan(0);
  expect(
    Math.max(...opened.own.values()),
    "the window's opening mark holds own bytes already sent, so its per-origin bytes count",
  ).toBeGreaterThan(0);
  const unequal = predictions(sandbox)!;
  const chart = page.locator('figure[data-ref-zones]');
  expect(Math.abs(Number(await chart.getAttribute('data-ref-zones')) - unequal.zones)).toBeLessThan(1e-9);
  expect(Math.abs(Number(await chart.getAttribute('data-ref-nozone')) - unequal.nozone)).toBeLessThan(1e-9);
  expect(unequal.nozone, 'unequal zones move the no-zone prediction off (N − N/Z) / (N − 1)').not.toBeCloseTo(
    (13 - 13 / 3) / 12,
    6,
  );
  await setTable(page, true);
  await checkOrder(page, 'Sandbox after add');

  // The same run with one node left: no reference line, attribute or label.
  // The pane's Remove takes the selected node, and the selection moves on.
  const remove = page.locator('.pane-foot').getByRole('button', { name: /^Remove node/ });
  for (let left = 12; left >= 1; left--) {
    await remove.click();
    // Each removal lands before the next click, which would otherwise pick the
    // node just removed.
    await expect(page.locator('[data-field="status-nodes"]')).toHaveText(String(left));
  }
  // The status sentence counts the zones with members, not the zones built.
  const left = await snapshot(page);
  expect(left.zones.filter((z) => z.live > 0).length, 'one node leaves one live zone of three').toBe(1);
  await expect(field(page, 'status-zones')).toHaveText('1');
  const lone = page.locator('section.block', { hasText: 'Share of data bytes' }).locator('figure');
  await expect(lone).toHaveCount(1);
  await expect(lone).not.toHaveAttribute('data-ref-zones', /.*/);
  await expect(lone).not.toHaveAttribute('data-ref-nozone', /.*/);
  const labels = await lone.locator('canvas[data-label-boxes]').getAttribute('data-label-boxes');
  expect(JSON.parse(labels ?? '[]'), 'no reference label with one node').toEqual([]);
  await checkOrder(page, 'Sandbox after remove');
});

// -- A4: the share window at any snapshot rate ----------------------------------

test('the share window holds at any snapshot rate', async ({}, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'pure arithmetic, checked once');
  // Twenty seconds at half the data bytes crossing zones, then twenty at a
  // tenth: a window of the last ten seconds lies wholly in the second phase, so
  // σ is 10 % however finely play slices virtual time. 17 ms is a 60 Hz frame
  // at 1x, 67 ms at 4x, and 500 ms a Step. The page's own runs Step from wave
  // to wave and jump to the window's edge, too coarse at the window's start to
  // tell one mark spacing from another, so the spacing is checked here: every
  // window opens within `W / 128` (plus one snapshot) of `t − W`.
  for (const stepMs of [17, 67, 500]) {
    const history = new ChartHistory();
    history.reset(WINDOW_MS);
    let inData = 0;
    let crossData = 0;
    for (let t = 0; t <= 40_000; t += stepMs) {
      const cross = t < 20_000 ? 0.5 : 0.1;
      inData += stepMs * (1 - cross);
      crossData += stepMs * cross;
      history.push({
        virtual_ms: t,
        nodes: [],
        zones: [{ name: 'zone-a', live: 0 }],
        bytes_by_tier: { in_zone: { data: inData, control: 0 }, cross_zone: { data: crossData, control: 0 } },
        oracle_total: 0,
      });
      if (t > WINDOW_MS) {
        const late = t - WINDOW_MS - history.windowStart(t).t;
        const where = `at ${stepMs} ms snapshots, the window ending at ${t}: ms it opens before t − W`;
        expect(late, where).toBeGreaterThanOrEqual(0);
        expect(late, where).toBeLessThan(WINDOW_MS / 128 + stepMs);
      }
    }
    expect(history.share.at(-1), `σ at ${stepMs} ms snapshots`).toBeCloseTo(0.1, 9);
  }
});

// -- A3: the stage's geometry for every zone count -----------------------------

test('stage geometry holds for every zone count', async ({ page }, info) => {
  test.skip(
    !['audit-1440-light', 'audit-400-light'].includes(info.project.name),
    'the geometry is checked at the widest and the narrowest stage',
  );
  test.setTimeout(240_000);
  await open(page);
  await page.getByText('Tune the cluster').click();
  const stepper = page.locator('#knob-zones');
  const max = Number(await stepper.getAttribute('max'));
  expect(max, 'twelve nodes allow the engine ceiling of six zones').toBe(6);

  for (let zones = 1; zones <= max; zones++) {
    await stepper.fill(String(zones));
    await page.getByRole('button', { name: 'Rebuild cluster' }).click();
    await expect
      .poll(() => page.locator('.stage canvas').evaluate((c: HTMLCanvasElement) => JSON.parse(c.dataset.rings ?? '[]').length))
      .toBe(zones);
    await expect(page.locator('.overlay.rebuilding')).toHaveCount(0);
    await settled(page);
    await page.locator('.stage').scrollIntoViewIfNeeded();

    const stage = await page.locator('.stage canvas').evaluate((c: HTMLCanvasElement) => {
      const at = c.getBoundingClientRect();
      return {
        w: at.width,
        h: at.height,
        left: at.left,
        top: at.top,
        centre: JSON.parse(c.dataset.centre ?? '{}') as Point,
        rings: JSON.parse(c.dataset.rings ?? '[]') as { x: number; y: number; rz: number; r: number }[],
        crossWidth: Number(c.dataset.crossWidth),
        labels: JSON.parse(c.dataset.labelBoxes ?? '[]') as { text: string; x: number; y: number; w: number; h: number }[],
      };
    });
    const where = `${zones} zone(s) on a ${stage.w.toFixed(0)} × ${stage.h.toFixed(0)} px stage`;

    // Every ring's node circle, grown by the largest disc, and every label lie
    // inside the stage.
    for (const [z, ring] of stage.rings.entries()) {
      const reach = ring.rz + R_MAX;
      expect(ring.x - reach, `${where}: zone ${z} crosses the left edge`).toBeGreaterThanOrEqual(-1e-6);
      expect(ring.y - reach, `${where}: zone ${z} crosses the top edge`).toBeGreaterThanOrEqual(-1e-6);
      expect(ring.x + reach, `${where}: zone ${z} crosses the right edge`).toBeLessThanOrEqual(stage.w + 1e-6);
      expect(ring.y + reach, `${where}: zone ${z} crosses the bottom edge`).toBeLessThanOrEqual(stage.h + 1e-6);
    }
    for (const l of stage.labels) {
      expect(l.x, `${where}: label "${l.text}" crosses the left edge`).toBeGreaterThanOrEqual(0);
      expect(l.y, `${where}: label "${l.text}" crosses the top edge`).toBeGreaterThanOrEqual(0);
      expect(l.x + l.w, `${where}: label "${l.text}" crosses the right edge`).toBeLessThanOrEqual(stage.w);
      expect(l.y + l.h, `${where}: label "${l.text}" crosses the bottom edge`).toBeLessThanOrEqual(stage.h);
    }
    if (zones < 2) continue;

    // No cross-zone route, drawn with the renderer's own `beamPath`, comes within
    // a disc and half a stroke of a third ring's nodes. This all-pairs check
    // covers the clearance; the drawn-beam check below pins that the renderer
    // routes every live beam through `beamPath` with each node's current zone.
    const spots: Spot[] = await page.evaluate(
      ({ left, top }) =>
        Array.from(document.querySelectorAll('[role="grid"] [role="row"]')).flatMap((row) =>
          Array.from(row.querySelectorAll('[role="gridcell"]')).map((c) => {
            const at = c.getBoundingClientRect();
            return {
              id: Number((c as HTMLElement).dataset.node),
              zone: Number((row as HTMLElement).dataset.zone),
              x: at.left + at.width / 2 - left,
              y: at.top + at.height / 2 - top,
              r: 0,
            };
          }),
        ),
      { left: stage.left, top: stage.top },
    );
    expect(spots, where).toHaveLength(12);

    // No two nodes of different rings come within each other's ring, nor
    // within the selection outline either might wear.
    const touching: string[] = [];
    for (const a of spots) {
      for (const b of spots) {
        if (a.zone >= b.zone) continue;
        const room =
          Math.hypot(a.x - b.x, a.y - b.y) - (stage.rings[a.zone].r + SELECTION_REACH) - (stage.rings[b.zone].r + RING_REACH);
        if (room < 0) touching.push(`node ${a.id} and node ${b.id} overlap by ${(-room).toFixed(1)} px`);
      }
    }
    expect(touching, `${where}: nodes of neighbouring rings that touch`).toEqual([]);

    let nearest = Number.POSITIVE_INFINITY;
    const meets: string[] = [];
    const at: Point = { x: 0, y: 0 };
    const route: QuadPath = { from: { x: 0, y: 0 }, control: { x: 0, y: 0 }, to: { x: 0, y: 0 } };
    for (const a of spots) {
      for (const b of spots) {
        if (a.zone >= b.zone) continue;
        const path = beamPath(a, b, stage.centre, route);
        for (let i = 0; i <= 128; i++) {
          pointOn(path, i / 128, at);
          for (const [k, ring] of stage.rings.entries()) {
            if (k === a.zone || k === b.zone) continue;
            const gap = Math.abs(Math.hypot(at.x - ring.x, at.y - ring.y) - ring.rz);
            const clear = gap - (ring.r + stage.crossWidth / 2);
            nearest = Math.min(nearest, clear);
            if (clear <= 0) meets.push(`node ${a.id} to node ${b.id} meets zone ${k}'s discs`);
          }
        }
      }
    }
    expect(meets, `${where}: cross-zone routes that meet a third ring's discs`).toEqual([]);
    console.log(`${where}: the nearest route clears a third ring's discs by ${nearest.toFixed(1)} px`);

    // Every beam the renderer drew bends through the centre exactly when its
    // nodes' zones differ. The zones come from the grid rows, the engine's
    // truth, never from the renderer's own flags. Each beam is recorded as it
    // enters the pool, with its route and the centre the renderer published at
    // that moment, so neither a beam that has already faded nor a Step that
    // reflowed the stage can hide or skew one.
    type Drawn = { src: number; dst: number; route: number[]; centre: string };
    await page.locator('.stage canvas').evaluate((c: HTMLCanvasElement) => {
      type Push = (path: QuadPath, start: number, src: number, dst: number, flags: number) => void;
      const canvas = c as HTMLCanvasElement & {
        beams?: { push: Push; recorded?: boolean };
        drawn?: Drawn[];
      };
      const pool = canvas.beams;
      if (pool === undefined) throw new Error('the stage publishes no beam pool');
      canvas.drawn = [];
      if (pool.recorded === true) return;
      pool.recorded = true;
      const push = pool.push.bind(pool);
      pool.push = (path, start, src, dst, flags) => {
        const { from, control, to } = path;
        canvas.drawn?.push({
          src,
          dst,
          route: [from.x, from.y, control.x, control.y, to.x, to.y],
          centre: canvas.dataset.centre ?? '',
        });
        push(path, start, src, dst, flags);
      };
    });
    const drawn = (): Promise<Drawn[]> =>
      page
        .locator('.stage canvas')
        .evaluate((c: HTMLCanvasElement & { drawn?: Drawn[] }) => c.drawn ?? []);
    const zoneOf = new Map(spots.map((s) => [s.id, s.zone]));
    const crosses = (b: { src: number; dst: number }): boolean => zoneOf.get(b.src) !== zoneOf.get(b.dst);
    // Rate limiting opens quiet (no seed, no feed), so a Send flushes a wave
    // that crosses zones at once, and its run draws it.
    await send(page, 0);
    await stepUntil(page, async () => (await drawn()).some(crosses), 40, `${where}: a cross-zone beam was drawn`);
    const beams = await drawn();
    for (const b of beams) {
      const [fx, fy, cx, cy, tx, ty] = b.route;
      const centre = JSON.parse(b.centre) as Point;
      const want = crosses(b) ? centre : { x: (fx + tx) / 2, y: (fy + ty) / 2 };
      expect(
        Math.hypot(cx - want.x, cy - want.y),
        `${where}: the beam from node ${b.src} to node ${b.dst} bends ${crosses(b) ? 'off the centre' : 'off its own segment'}`,
      ).toBeLessThan(0.01);
    }
    console.log(`${where}: checked the route of ${beams.length} drawn beam(s)`);
  }
});

// -- The stage's beams, one per packet, styled by its kind ---------------------

test("every drawn beam carries its packet's kind, tier and fate", async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', "the renderer's beam flags, checked once");
  await open(page);
  await loadPreset(page, 'Sandbox');

  // What each packet should wear, derived here from the packet kinds rather
  // than read from the renderer's `kindFlags`: an acknowledgement or a sweep
  // request is a dot, a relay is marked so its gateway's beams stay the
  // selected origin's.
  const KIND: Record<PacketKind, number> = { Ack: CONTROL, SweepRequest: CONTROL, Own: 0, Relay: RELAY, Sweep: 0 };
  type Beam = { src: number; dst: number; flags: number };
  type Log = { beams: Beam[] };

  // Record the packets the engine reports, and every beam the stage draws, as
  // it enters the beam pool, so a beam that has already faded and gone is
  // still counted.
  await logPackets(page);
  await page.evaluate(() => {
    const log = window as unknown as Log;
    log.beams = [];
    type Push = (path: unknown, start: number, src: number, dst: number, flags: number) => void;
    const pool = document.querySelector<HTMLCanvasElement & { beams?: { push: Push } }>('.stage canvas')?.beams;
    if (pool === undefined) throw new Error('the stage publishes no beam pool');
    const push = pool.push.bind(pool);
    pool.push = (path, start, src, dst, flags) => {
      log.beams.push({ src, dst, flags });
      push(path, start, src, dst, flags);
    };
  });

  // The packets and beams logged since the last call, taken at once, so no
  // async tail lands between the two.
  const take = (): Promise<{ packets: Packet[]; beams: Beam[] }> =>
    page.evaluate(() => {
      const w = window as unknown as Log & { packets: Packet[] };
      return { packets: w.packets.splice(0), beams: w.beams.splice(0) };
    });

  // One Step: the beams it drew are its packets, one beam each, each with its
  // packet's flags. Returns the kinds of the packets drawn.
  const checkStep = async (): Promise<PacketKind[]> => {
    await take();
    await step(page);
    const { packets, beams } = await take();
    const route = ({ src, dst, flags }: Beam): string => `node ${src} to node ${dst} with flags ${flags}`;
    const sent = packets.map((p) =>
      route({
        src: p.src,
        dst: p.dst,
        flags: KIND[p.kind] | (p.tier === 'CrossZone' ? CROSS : 0) | (p.dropped ? DROPPED : 0),
      }),
    );
    expect(beams.map(route).sort(), 'the beams drawn are the packets sent, one each').toEqual(sent.sort());
    return packets.map((p) => p.kind);
  };

  // A Send's waves are own streams, relays and acknowledgements. Once every
  // store holds its hits, a node added to the live cluster asks its zone for a
  // join sweep and is sent one.
  await send(page, 0, true);
  await period(page);
  await period(page);
  // Add runs until the newcomer has caught up: the join's packets, a sweep
  // request and the sweep, are each drawn once as they are sent.
  const seen = new Set<PacketKind>();
  await take();
  await page.getByRole('button', { name: 'Add node' }).click();
  await expect(field(page, 'status-nodes')).toHaveText('13');
  await expect(page.locator('.stage-pane')).toHaveAttribute('data-clock', 'paused', { timeout: 30_000 });
  await still(page);
  const joined = await take();
  const name = ({ src, dst }: { src: number; dst: number }): string => `node ${src} to node ${dst}`;
  expect(joined.beams.map(name).sort(), "the join's beams are its packets, one each").toEqual(joined.packets.map(name).sort());
  for (const p of joined.packets) seen.add(p.kind);
  await send(page, 0, true);
  for (let i = 0; i < 40 && seen.size < Object.keys(KIND).length; i++) {
    for (const kind of await checkStep()) seen.add(kind);
  }
  expect([...seen].sort(), 'every packet kind was sent and drawn').toEqual([
    'Ack',
    'Own',
    'Relay',
    'Sweep',
    'SweepRequest',
  ]);
});

// -- The stage's labels, laid out once per layout -------------------------------

test('a paused stage lays its labels out once, not per snapshot', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'a count of layouts, checked once');
  await open(page);
  await loadPreset(page, 'Sandbox');
  // Run a few periods first, so the annotation line settles.
  for (let i = 0; i < 4; i++) await period(page);
  await settled(page);
  await page.locator('.stage canvas').evaluate((c: HTMLCanvasElement) => {
    const w = window as unknown as { labelWrites: number };
    w.labelWrites = 0;
    new MutationObserver((records) => {
      w.labelWrites += records.length;
    }).observe(c, { attributeFilter: ['data-label-boxes'] });
  });
  // Each Step hands the stage a fresh snapshot with the same members and size.
  for (let i = 0; i < 3; i++) await period(page);
  const writes = await page.evaluate(() => (window as unknown as { labelWrites: number }).labelWrites);
  expect(writes, 'the labels were rebuilt for snapshots that moved no node').toBe(0);
});

// -- How every preset opens, goes quiet and fails over ---------------------------

/** A packet the engine reported sent or lost, at its instant. */
type Packet = { t: number; src: number; dst: number; kind: PacketKind; tier: Tier; dropped: boolean };

/** Log every packet in the batches of the page's engine, its steps and its
 *  submits alike, from now on. Installed once per engine. */
async function logPackets(page: Page): Promise<void> {
  await page.evaluate(() => {
    const w = window as unknown as { packets: Packet[] };
    w.packets = [];
    const sim = window.sim! as typeof window.sim & { logged?: boolean };
    if (sim.logged === true) return;
    sim.logged = true;
    const record = (batch: EventBatch): EventBatch => {
      for (const { virtual_ms: t, kind } of batch.events) {
        if (kind.type === 'PacketSent' || kind.type === 'PacketDropped') {
          const { src, dst, tier } = kind;
          w.packets.push({ t, src, dst, kind: kind.kind, tier, dropped: kind.type === 'PacketDropped' });
        }
      }
      return batch;
    };
    const step = sim.step.bind(sim);
    sim.step = async (ms: number, until?: StepUntil) => record(await step(ms, until));
    const submit = sim.submitRequest.bind(sim);
    sim.submitRequest = async (node: number, key: number, hits: number) => record(await submit(node, key, hits));
  });
}

/** The packets logged since the last call. */
const takePackets = (page: Page): Promise<Packet[]> =>
  page.evaluate(() => (window as unknown as { packets: Packet[] }).packets.splice(0));

/** Whether every node holds every in-window hit. */
const agreed = async (page: Page): Promise<boolean> =>
  (await snapshot(page)).nodes.every((n) => n.lag.kind === 'CaughtUp');

test('every preset opens warmed and settled', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the opening state, checked once');
  test.setTimeout(240_000);
  await open(page);
  await setSpeed(page, 4);
  const gateways = page.locator('.inspector tr.remote [data-field="gateway"]');

  // Propagation's seed has spread and been acknowledged before the first
  // paint, and node 0's gateways were found while the cluster warmed up.
  await loadPreset(page, 'Propagation');
  const settledSeed = await snapshot(page);
  expect(settledSeed.nodes.map((n) => n.window_total), 'every node holds the seed').toEqual(
    settledSeed.nodes.map(() => 50),
  );
  expect(
    await page.locator('.stage canvas').evaluate((c: HTMLCanvasElement & { beams?: { len: number } }) => c.beams?.len),
    'no beam at load',
  ).toBe(0);
  await expect(gateways, "node 0's gateway into each other zone, named at load").toHaveCount(2);

  // Every other zoned preset: each seeded node's first wave reaches one gateway
  // in each other zone, not every peer, as a warm cluster's does.
  const names = (await page.getByRole('group', { name: 'Scenario preset' }).getByRole('button').allInnerTexts())
    .map((name) => name.trim())
    .filter((name) => name !== 'Propagation' && name !== 'No zone configured');
  for (const name of names) {
    await loadPreset(page, name);
    if ((await page.locator('.inspector').count()) > 0) await expect(gateways, `${name}: gateways at load`).toHaveCount(2);
    const load = await snapshot(page);
    const seeded = load.nodes.filter((n) => n.admitted_hits > 0).map((n) => n.id);
    await logPackets(page);
    const log: Packet[] = [];
    await stepUntil(
      page,
      async () => {
        log.push(...(await takePackets(page)));
        return seeded.every((id) => log.some((p) => p.src === id && p.kind === 'Own'));
      },
      4,
      `${name}: every seeded node sent its hits`,
    );
    for (const id of seeded) {
      const own = log.filter((p) => p.src === id && p.kind === 'Own');
      const across = own.filter((p) => p.t === own[0].t && p.tier === 'CrossZone').length;
      expect(across, `${name}: node ${id}'s first wave crossed a zone ${across} times`).toBe(load.zones.length - 1);
    }
    console.log(`${name}: ${seeded.length} seeded node(s) checked`);
  }
});

test('runs go quiet after agreement', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the quiet after a run, checked once');
  test.setTimeout(240_000);
  await open(page);
  await setSpeed(page, 4);

  // Propagation: after the relays land, only the acknowledgements fly, each at
  // its node's next heartbeat, so the period after that sends nothing at all.
  await loadPreset(page, 'Propagation');
  await logPackets(page);
  // Propagation's Send stops before its first wave; Steps fly the waves.
  await send(page, 0);
  await step(page);
  await stepUntil(page, () => agreed(page), 4, 'Propagation agreed');
  const at = await virtualMs(page);
  await takePackets(page);
  for (let i = 0; i < 5; i++) await period(page);
  const after = await takePackets(page);
  expect(after.filter((p) => CARRIES_CELLS[p.kind]), 'Propagation: data after agreement').toEqual([]);
  expect(
    after.filter((p) => p.t >= at + PERIOD_MS && p.t < at + 2 * PERIOD_MS),
    'Propagation: a packet in the second period after agreement',
  ).toEqual([]);

  // Zone partition, healed: the heal's run reconciles and pauses itself,
  // not at its cap, then the network sends no data.
  await loadPreset(page, 'Zone partition');
  await page.getByRole('button', { name: 'Heal network' }).click();
  await expect(field(page, 'status-cut')).toHaveCount(0);
  await expect(page.locator('.stage-pane'), 'Zone partition: the heal ran to agreement, not its cap').toHaveAttribute('data-clock', 'paused', { timeout: 30_000 });
  await expect(page.locator('.narration')).toHaveText(/every node has it$/);
  await logPackets(page);
  await stepUntil(page, () => agreed(page), 16, 'Zone partition agreed');
  await takePackets(page);
  for (let i = 0; i < 5; i++) await period(page);
  expect((await takePackets(page)).filter((p) => CARRIES_CELLS[p.kind]), 'Zone partition: data after agreement').toEqual([]);

  // Packet loss: its resends are not asserted away, but once agreed it stays so.
  await loadPreset(page, 'Packet loss');
  await stepUntil(page, () => agreed(page), 20, 'Packet loss agreed');
  for (let i = 0; i < 5; i++) {
    await period(page);
    expect(await agreed(page), `Packet loss: agreed ${i + 1} period(s) on`).toBe(true);
  }
});

// A46: one time model. A refused Send runs no time; an admitted Send's run
// stops by itself one period after its last landing, its acknowledgements
// sent, so the period of Play after it sends nothing; in Zone partition it
// stops at the cut zone, long before its cap.
test('a Send runs only the time it needs (A46)', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the time model, checked once');
  test.setTimeout(240_000);
  await open(page);
  await setSpeed(page, 4);
  const pane = page.locator('.stage-pane');
  const hits = page.locator('#burst-hits');
  await hits.fill('100');
  await hits.press('Enter');
  await logPackets(page);
  await send(page, 0);
  await expect(pane).toHaveAttribute('data-clock', 'paused');
  const end = await virtualMs(page);
  await takePackets(page);
  await page.getByRole('button', { name: 'Play' }).click();
  await expect.poll(() => virtualMs(page), { timeout: 30_000 }).toBeGreaterThanOrEqual(end + PERIOD_MS);
  await page.getByRole('button', { name: 'Pause' }).click();
  await still(page);
  expect((await takePackets(page)).filter((p) => p.t < end + PERIOD_MS), 'a packet in the period after the run').toEqual([]);

  // Node 0 sees the whole limit: its next Send is refused, and the page
  // stays paused at the same instant.
  const at = await virtualMs(page);
  await refusal(page, 0, 'A46');
  expect(await virtualMs(page), 'a refused Send ran time').toBe(at);
  await expect(pane).toHaveAttribute('data-playing', '0');
  expect(await takePackets(page), 'a refused Send sent a packet').toEqual([]);

  await loadPreset(page, 'Zone partition');
  const from = await virtualMs(page);
  await send(page, 0);
  await expect(page.locator('.narration')).toHaveText(/^paused: zone c is cut off/);
  // Its flush lands at once and its relays within a heartbeat; the run ends a
  // period later. Four periods leave room and stay far below the cap of 16.
  expect((await virtualMs(page)) - from, 'Zone partition: the run stopped at the cut').toBeLessThanOrEqual(4 * PERIOD_MS);

  // Steady state's feed never lets every node be caught up for long: a paused
  // Send's run still ends, at the latest at its cap of 16 periods, overrun by
  // at most one advance of the play loop (500 ms).
  await loadPreset(page, 'Steady state');
  await page.evaluate(() => {
    const w = window as unknown as { clocks: string[] };
    const el = document.querySelector('.stage-pane')!;
    w.clocks = [el.getAttribute('data-clock') ?? ''];
    new MutationObserver(() => {
      const c = el.getAttribute('data-clock') ?? '';
      if (w.clocks.at(-1) !== c) w.clocks.push(c);
    }).observe(el, { attributes: true, attributeFilter: ['data-clock'] });
  });
  const steady = await virtualMs(page);
  await sendButton(page).click();
  await expect
    .poll(() => page.evaluate(() => (window as unknown as { clocks: string[] }).clocks.join(' ')), { timeout: 90_000 })
    .toMatch(/^paused settled (paused|capped)$/);
  const ran = (await virtualMs(page)) - steady;
  console.log(`Steady state: a paused Send's run lasted ${ran} ms`);
  expect(ran, "Steady state: the Send's run ended within its cap").toBeLessThanOrEqual(16 * PERIOD_MS + 500);
});

test('both gateway presets fail over within the window', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the failover, checked once');
  test.setTimeout(240_000);
  await open(page);
  await setSpeed(page, 4);
  for (const [name, record, trickle] of [
    ['Gateway failure', timings.gateway_failure, 0],
    // The trickle's hits in the last period may not have reached zone b yet.
    ['Gateway cut', timings.gateway_cut, Math.ceil((2 * PERIOD_MS) / 1000)],
  ] as const) {
    await loadPreset(page, name);
    const load = await snapshot(page, 0);
    const faulted = load.inspected?.peers.find((p) => p.role === 'gateway' && p.zone === 1)?.id;
    expect(faulted, `${name}: node 0's gateway into zone b at load`).toBeDefined();
    const failovers = async (): Promise<number> =>
      (await snapshot(page)).nodes.find((n) => n.id === 0)?.gateway_failovers ?? 0;
    await stepUntil(page, async () => (await failovers()) >= 1, WINDOW_MS / PERIOD_MS, `${name}: node 0 failed over`);
    const failedOver = await virtualMs(page);
    // `stepUntil` checks its deadline before each Step, so the last may overrun it.
    expect(failedOver - load.virtual_ms, `${name}: node 0 failed over within the window`).toBeLessThan(WINDOW_MS);
    const zoneB = load.nodes.filter((n) => n.zone === 1 && n.id !== faulted).map((n) => n.id);
    await stepUntil(
      page,
      async () => {
        const snap = await snapshot(page);
        return zoneB.every((id) => snap.nodes.find((n) => n.id === id)!.window_total >= snap.oracle_total - trickle);
      },
      3,
      `${name}: zone b caught up through its new gateway`,
    );
    console.log(`${name}: failed over ${failedOver - load.virtual_ms} ms after load (the record: ${record.failover_ms} ms)`);
  }
});

test('the transport stays put when the heartbeat reading appears', async ({ page }, info) => {
  await open(page);
  await page.evaluate(() => document.fonts.ready.then(() => undefined));
  const bar = page.locator('.transport');
  const time = page.locator('.readout-item.time');
  const reading = page.locator('.readout-item.heartbeat');
  // Kept, but hidden, until a period has played: it never reads a time not
  // measured.
  await expect(reading, 'the heartbeat reading before a period played').toBeHidden();
  const before = (await bar.boundingBox())!.height;
  const at = (await time.boundingBox())!;
  const row = (await page.locator('.readout').boundingBox())!;
  expect(at.x + at.width, "the simulated time stands at the bar's right edge").toBeCloseTo(row.x + row.width, 0);
  await page.getByRole('button', { name: 'Play' }).click();
  await expect(field(page, 'heartbeat-wall')).toHaveAttribute('data-ms', /\d/, { timeout: 30_000 });
  await page.getByRole('button', { name: 'Pause' }).click();
  await expect(reading, 'the heartbeat reading once a period played').toBeVisible();
  expect((await bar.boundingBox())!.height, 'the bar changed height when the reading appeared').toBe(before);
  const after = (await time.boundingBox())!;
  expect(after.x + after.width, 'the simulated time moved when the reading appeared').toBeCloseTo(at.x + at.width, 0);
  // From 1100 px up, Play, Step, Send, Speed and both readouts share one row.
  const play = (await page.getByRole('button', { name: 'Play' }).boundingBox())!.height;
  if ((page.viewportSize()?.width ?? 0) >= 1100) expect(before, 'the bar wrapped').toBeLessThan(2 * play);
  await bar.screenshot({ path: `audit-shots/${info.project.name}/transport-heartbeat.png` });
});

// -- A6: interaction -----------------------------------------------------------

test.describe('interaction', () => {
  test.use({ hasTouch: true });

  test('the controls, the keyboard grid, focus, tooltips and the study link work', async ({ page }) => {
    test.setTimeout(240_000);
    const errors: string[] = [];
    page.on('pageerror', (error) => errors.push(error.message));
    await open(page);
    const lede = page.locator('.lede');
    const title = page.locator('.pane-title [data-field="node"]');
    const rings = (): Promise<number> =>
      page.locator('.stage canvas').evaluate((c: HTMLCanvasElement) => JSON.parse(c.dataset.rings ?? '[]').length);

    // The zone stepper and Rebuild: four zones.
    await page.getByText('Tune the cluster').click();
    await page.locator('#knob-zones').fill('4');
    await page.getByRole('button', { name: 'Rebuild cluster' }).click();
    await expect(field(page, 'status-zones')).toHaveText('4');
    await expect.poll(rings).toBe(4);

    // Steady state: cut zone c off, then heal. The lede names the cut.
    await loadPreset(page, 'Steady state');
    await page.locator('#cut-zone').selectOption({ label: 'zone c' });
    await page.getByRole('button', { name: 'Cut zone c off' }).click();
    await expect(lede).toContainText('zone c cut off');
    await page.getByRole('button', { name: 'Heal network' }).click();
    await expect(lede).not.toContainText('cut off');

    // Propagation, with node 0 selected: silence one of its gateways, then cut
    // another off from its zone. The cluster warmed up before it opened, so
    // the gateways are already found.
    await loadPreset(page, 'Propagation');
    await expect(page.getByRole('heading', { name: "node 0's gateways" })).toBeVisible();
    const silence = page.getByRole('button', { name: /^Silence node \d+$/ }).first();
    const silenced = (await silence.locator('[data-field="node"]').textContent()) ?? '';
    await silence.click();
    await expect(lede).toContainText(`node ${silenced} silenced`);
    await page.getByRole('button', { name: 'Heal network' }).click();
    await expect(lede).not.toContainText('silenced');
    await page.getByRole('button', { name: /^Cut node \d+ off from zone b$/ }).click();
    await expect(lede).toContainText('unheard by zone b');
    await page.getByRole('button', { name: 'Heal network' }).click();
    await expect(lede).not.toContainText('unheard');

    // The keyboard: Shift+Tab from "Show as table" lands on the grid's one tab
    // stop. Right moves within the zone; Down to the same rank in the next zone;
    // End and Home to the zone's ends; Enter selects; Escape returns the
    // selection to the scenario's node.
    const focused = (): Promise<string | undefined> => page.evaluate(() => (document.activeElement as HTMLElement).dataset.node);
    await page.getByText('Show as table').focus();
    await page.keyboard.press('Shift+Tab');
    expect(await focused(), 'the first node of the first zone is the tab stop').toBe('0');
    await page.keyboard.press('ArrowRight');
    expect(await focused(), 'Right: the next node of zone a').toBe('3');
    await page.keyboard.press('ArrowDown');
    expect(await focused(), 'Down: the same rank in zone b').toBe('4');
    await page.keyboard.press('End');
    expect(await focused(), 'End: the last node of zone b').toBe('10');
    await page.keyboard.press('Home');
    expect(await focused(), 'Home: the first node of zone b').toBe('1');
    await expect(page.locator('.stage-box .tooltip'), 'focus shows the tooltip').toContainText('select node 1');
    await page.keyboard.press('Enter');
    await expect(title).toHaveText('1');
    await page.keyboard.press('Escape');
    await expect(title, "Escape returns the selection to the scenario's node").toHaveText('0');
    await expect(page.locator('.stage-box .tooltip')).toHaveCount(0);

    // Focus survives a rebuild: Restart, clicked without moving focus, leaves it
    // on the same node.
    await cell(page, 5).focus();
    // Mark the engine, so the rebuild is seen as a new one rather than as the
    // overlay not having appeared yet.
    await page.evaluate(() => Object.assign(window.sim!, { old: true }));
    await page.getByRole('button', { name: 'Restart Propagation' }).dispatchEvent('click');
    await expect.poll(() => page.evaluate(() => window.sim !== undefined && !('old' in window.sim))).toBe(true);
    await expect(page.locator('.overlay.rebuilding')).toHaveCount(0);
    expect(await focused(), 'focus stayed on node 5 through the rebuild').toBe('5');

    // The tooltip on hover and on tap. Focus is released first: a focused node
    // keeps its tooltip once the pointer leaves.
    await page.evaluate(() => (document.activeElement as HTMLElement).blur());
    await cell(page, 7).scrollIntoViewIfNeeded();
    const box = await cell(page, 7).boundingBox();
    if (box === null) throw new Error('node 7 has no box');
    const [x, y] = [box.x + box.width / 2, box.y + box.height / 2];
    await page.mouse.move(x, y);
    await expect(page.locator('.stage-box .tooltip')).toContainText('select node 7');
    await page.mouse.move(0, 0);
    await expect(page.locator('.stage-box .tooltip')).toHaveCount(0);
    await page.touchscreen.tap(x, y);
    await expect(title).toHaveText('7');
    await expect(page.locator('.stage-box .tooltip')).toContainText('select node 7');

    // Removing a cut node takes its cut with it.
    const cutFrom = page.getByRole('button', { name: /^Cut node \d+ off from zone \w$/ }).first();
    await expect(cutFrom, 'node 7 found its gateways while the cluster warmed up').toBeVisible();
    const cutNode = Number((await cutFrom.locator('[data-field="node"]').textContent()) ?? '');
    await cutFrom.click();
    await expect(lede).toContainText(`node ${cutNode} unheard`);
    await page.locator(`[role="gridcell"][data-node="${cutNode}"]`).press('Enter');
    await expect(title).toHaveText(String(cutNode));
    await page.locator('.pane-foot').getByRole('button', { name: `Remove node ${cutNode}` }).click();
    await expect(field(page, 'status-nodes')).toHaveText('11');
    await expect(lede).not.toContainText('unheard');

    // The study link: the base URL's form, not that it resolves (`study/` joins
    // the site only in the Pages build).
    await expect(page.getByRole('link', { name: 'measured results in the study' })).toHaveAttribute('href', '/study/dashboard/');
    expect(errors, 'page errors').toEqual([]);
  });
});

// -- A42, A16: the node glyph in motion ------------------------------------------

/** The relative luminance of a packed `0xRRGGBB` and the contrast of two. */
const lum = (rgb: number): number => {
  const c = [(rgb >> 16) & 0xff, (rgb >> 8) & 0xff, rgb & 0xff].map((v) => {
    const x = v / 255;
    return x <= 0.04045 ? x / 12.92 : ((x + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * c[0] + 0.7152 * c[1] + 0.0722 * c[2];
};
const contrastOf = (a: number, b: number): number => {
  const [hi, lo] = [lum(a), lum(b)].sort((x, y) => y - x);
  return (hi + 0.05) / (lo + 0.05);
};

/** The stage's glyph table, read in the page. */
const glyphs = <T>(page: Page, read: (g: GlyphTable) => T): Promise<T> =>
  page.locator('.stage canvas').evaluate((c: HTMLCanvasElement & { glyphs: GlyphTable }, src) => {
    // The reader arrives as source: Playwright ships functions as text.
    return (0, eval)(src)(c.glyphs);
  }, read.toString()) as Promise<T>;

/** Each decision stands right of its ring, and on its left only when
 *  the right would leave the stage or touch a name. */
async function checkDecisions(page: Page, where: string): Promise<void> {
  const found = await page.evaluate(() => {
    const out: string[] = [];
    const names: { x: number; y: number; w: number; h: number }[] = JSON.parse(document.querySelector<HTMLElement>('.stage canvas')?.dataset.labelBoxes ?? '[]');
    for (const el of document.querySelectorAll<HTMLElement>('.decision')) {
      const cell = document.querySelector<HTMLElement>(`[role="gridcell"][data-node="${el.dataset.node}"]`)!;
      const cx = parseFloat(cell.style.left) + parseFloat(cell.style.width) / 2;
      const cy = parseFloat(cell.style.top) + parseFloat(cell.style.height) / 2;
      const left = parseFloat(el.style.left);
      const w = el.getBoundingClientRect().width;
      const flipped = el.classList.contains('flipped');
      if (flipped !== left < cx) out.push(`node ${el.dataset.node}'s decision sits ${left < cx ? 'left' : 'right'} but says it is ${flipped ? '' : 'not '}flipped`);
      // The right slot: where the text stands unflipped, or its mirror.
      const x = flipped ? 2 * cx - left - w : left;
      const crosses = names.some((b) => x < b.x + b.w && b.x < x + w && cy - 10 < b.y + b.h && b.y < cy + 10);
      const overflows = x + w > (el.offsetParent as HTMLElement).clientWidth;
      if (flipped !== (crosses || overflows)) {
        out.push(`node ${el.dataset.node}'s decision is ${flipped ? 'left though its right is free' : 'right though it ' + (crosses ? 'touches a name' : 'leaves the stage')}`);
      }
    }
    return out;
  });
  expect(found, `${where}: the decisions`).toEqual([]);
}

test('the node glyph tells level, strata, cap, refusal and ring (A42, A16)', async ({ page }, info) => {
  test.setTimeout(240_000);
  await open(page);
  await loadPreset(page, 'Rate limiting');
  const wide = (page.viewportSize()?.width ?? 0) >= 1100;
  const where = info.project.name;
  const theme = await page.evaluate(readTheme);
  expect(contrastOf(theme.fg, theme.level), `${where}: a count over the fill (A16)`).toBeGreaterThanOrEqual(4.5);
  expect(contrastOf(theme.fg, theme.panel), `${where}: a count over the empty inside (A16)`).toBeGreaterThanOrEqual(4.5);

  // Every ring opens closed and caught up: nothing sweeps on load.
  const opened = await page.locator('.stage canvas').evaluate((c: HTMLCanvasElement & { glyphs: GlyphTable }) => {
    const g = c.glyphs;
    const out: string[] = [];
    for (let s = 0; s < g.len; s++) if (g.id[s] >= 0) out.push(`${g.ring[s]}|${g.arc[s]}`);
    return out;
  });
  expect(opened, `${where}: every ring at load`).toEqual(opened.map(() => `${RING_CAUGHT_UP}|1`));

  // In every frame of the first Send's run a ring is closed exactly where the
  // vessel has the hits, and its count says so: each line drew with its count.
  // Before the Send, every ring is closed over an empty window. Every audit
  // project reduces motion, so an arc moves at once. The sampler runs from
  // before the press until the run has ended; a vessel has hits when its fill
  // rises off the bottom.
  type Sample = { id: number; ring: number; arc: number; strata: number; hits: boolean; count: string | null }[];
  await page.locator('[role="gridcell"][data-node="0"]').press('Enter');
  await page.evaluate(() => {
    const w = window as unknown as { samples: Sample[]; sampling: boolean };
    w.samples = [];
    w.sampling = true;
    const take = (): void => {
      if (!w.sampling) return;
      const g = (document.querySelector('.stage canvas') as HTMLCanvasElement & { glyphs: GlyphTable }).glyphs;
      const nodes: Sample = [];
      for (let s = 0; s < g.len; s++) {
        if (g.id[s] < 0) continue;
        const count = document.querySelector(`[role="gridcell"][data-node="${g.id[s]}"] .count`)?.textContent?.trim() ?? null;
        nodes.push({ id: g.id[s], ring: g.ring[s], arc: g.arc[s], strata: g.strata[s], hits: g.fillTop[s] < g.r[s] - 1 - 1e-3, count });
      }
      w.samples.push(nodes);
      requestAnimationFrame(take);
    };
    requestAnimationFrame(take);
  });
  const inner = await glyphs(page, (g) => 2 * (g.r[0] - 1));
  await sendButton(page).click();
  // The admitted text stands while its run plays, and goes when it ends.
  if (wide) await expect(page.locator('.decision'), `${where}: node 0's decision`).toHaveCount(1);
  await checkDecisions(page, `${where}, admitted`);
  await still(page, 60_000);
  const samples = await page.evaluate(() => {
    const w = window as unknown as { samples: Sample[]; sampling: boolean };
    w.sampling = false;
    return w.samples;
  });
  await expect(page.locator('.decision'), `${where}: the admitted text once its run ended`).toHaveCount(0);
  // Strata show where the inside is tall enough: one per non-empty bucket.
  const tall = inner >= STRATA_MIN_INNER;
  expect(samples.some((nodes) => nodes.some((n) => n.id === 0 && n.hits)), `${where}: the Send was seen`).toBe(true);
  for (const [frame, nodes] of samples.entries()) {
    const sent = nodes.some((n) => n.hits);
    for (const { id, ring, arc, strata, hits, count } of nodes) {
      const at = `${where}: node ${id} in frame ${frame} of the Send's run`;
      expect(ring === RING_CAUGHT_UP, `${at}: its ring is closed exactly when its arc is whole`).toBe(arc === 1);
      expect(ring === RING_CAUGHT_UP, `${at}: its ring is closed exactly where its vessel has the hits`).toBe(hits || !sent);
      if (count !== null) expect(count, `${at}: its count`).toBe(hits ? '25' : '0');
      if (tall) expect(strata, `${at}: its strata`).toBe(hits ? 1 : 0);
    }
  }

  // Sends at nodes 1, 2 and 2, each in a bucket of its own where strata show.
  for (const node of [1, 2, 2]) {
    if (wide) {
      const bucket = (await snapshot(page)).bucket_epoch_now;
      await page.getByRole('button', { name: 'Play' }).click();
      await expect.poll(async () => (await snapshot(page)).bucket_epoch_now, { timeout: 30_000 }).toBeGreaterThan(bucket);
      await page.getByRole('button', { name: 'Pause' }).click();
      await still(page);
    }
    await send(page, node);
  }
  const snap = await snapshot(page);
  await checkGlyphs(page, `${where}, at the limit`, 'Rate limiting');
  const strata = await glyphs(page, (g) => Array.from({ length: g.len }, (_, s) => [g.id[s], g.strata[s]] as [number, number]));
  const buckets = snap.window_buckets.length / snap.nodes.length;
  for (const [i, node] of snap.nodes.entries()) {
    const row = snap.window_buckets.slice(i * buckets, (i + 1) * buckets);
    const want = tall ? row.filter((v) => v > 0).length : 0;
    expect(strata.find(([id]) => id === node.id)?.[1], `${where}: node ${node.id}'s strata`).toBe(want);
    // Every count is the engine's where counts fit; from 1100 px they always do.
    const count = page.locator(`[role="gridcell"][data-node="${node.id}"] .count`);
    if (wide || (await count.count()) > 0) await expect(count, `${where}: node ${node.id}'s count`).toHaveText(int(node.window_total));
  }
  if (tall) expect(Math.max(...strata.map(([, n]) => n)), `${where}: four Sends in four buckets`).toBe(4);

  // The fifth Send at node 2 is refused: its band plays in node 2's headroom,
  // inside its disc, above its brim and clear of its count; no line is drawn
  // and no count changes.
  const counts = await page.locator('.count').allTextContents();
  await page.evaluate(() => {
    const w = window as unknown as { pushed: number };
    w.pushed = 0;
    const pool = document.querySelector<HTMLCanvasElement & { beams: { push: (...a: unknown[]) => void } }>('.stage canvas')!.beams;
    const push = pool.push.bind(pool);
    pool.push = (...a: unknown[]) => {
      w.pushed += 1;
      push(...a);
    };
  });
  const frames = await refusal(page, 2, where);
  await checkDecisions(page, `${where}, refused`);
  // Every audit project reduces motion (the motion project plays the band).
  // A relayout the test did not force may still land mid-refusal and reshape
  // the band with its disc, so the band rests at one top per disc radius.
  const tops = new Map<number, Set<number>>();
  for (const f of frames) tops.set(f.r, (tops.get(f.r) ?? new Set()).add(f.top));
  for (const [r, at] of tops) expect(at.size, `${where}: under reduced motion the band rests (r ${r})`).toBe(1);
  expect(await page.evaluate(() => (window as unknown as { pushed: number }).pushed), `${where}: a refusal sends nothing`).toBe(0);
  expect(await page.locator('.count').allTextContents(), `${where}: a refusal counts nothing`).toEqual(counts);

  // A relayout that resizes the disc mid-bounce reshapes the band: it still
  // rests on the brim. At 0.2× the band stands three seconds, long past the
  // resize's debounce. Checked where the stage fills the height and where it
  // keeps its aspect.
  if (!['audit-1280-light', 'audit-400-dark'].includes(where)) return;
  await setSpeed(page, 0.2);
  const resized = await refusal(page, 2, `${where}, resized`, true);
  await page.locator('.stage').evaluate((el: HTMLElement) => el.removeAttribute('style'));
  expect(new Set(resized.map((f) => f.r)).size, `${where}: the disc resized mid-bounce`).toBeGreaterThan(1);
  for (const f of resized) expect(Math.abs(f.bottom - f.brim), `${where}: the resized band rests on its brim`).toBeLessThan(1e-3);
});

// -- A10: the faces ---------------------------------------------------------------

test('every visible text is set in the three faces, from the page itself (A10)', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the faces, checked once');
  const origins = new Set<string>();
  page.on('request', (request) => origins.add(new URL(request.url()).origin));
  await open(page);
  await page.evaluate(() => document.fonts.ready.then(() => undefined));
  const loaded = await page.evaluate(() =>
    ['400 16px "Source Sans 3"', '600 16px "Source Sans 3"', '400 16px "Source Serif 4"', '400 16px "Source Code Pro"'].map(
      (f) => document.fonts.check(f) && [...document.fonts].some((face) => face.status === 'loaded' && f.includes(face.family.replace(/"/g, ''))),
    ),
  );
  expect(loaded, 'each face is loaded').toEqual([true, true, true, true]);
  const cdp = await page.context().newCDPSession(page);
  await cdp.send('DOM.enable');
  await cdp.send('CSS.enable');
  const { root } = await cdp.send('DOM.getDocument', { depth: -1 });
  const families = new Set<string>();
  const parents: number[] = [];
  const walk = (node: { nodeId: number; nodeType: number; nodeValue?: string; children?: unknown[] }, parent: number): void => {
    if (node.nodeType === 3 && (node.nodeValue ?? '').trim() !== '') parents.push(parent);
    for (const child of (node.children ?? []) as (typeof node)[]) walk(child, node.nodeId);
  };
  walk(root as never, root.nodeId);
  for (const id of new Set(parents)) {
    const visible = await cdp
      .send('DOM.resolveNode', { nodeId: id })
      .then(({ object }) =>
        cdp.send('Runtime.callFunctionOn', {
          objectId: object.objectId!,
          functionDeclaration: 'function () { return this.checkVisibility ? this.checkVisibility() : false; }',
          returnByValue: true,
        }),
      )
      .then((r) => r.result.value === true)
      .catch(() => false);
    if (!visible) continue;
    const { fonts } = await cdp.send('CSS.getPlatformFontsForNode', { nodeId: id }).catch(() => ({ fonts: [] }));
    for (const f of fonts) families.add(f.familyName);
  }
  // An instanced variable face reports its family with its default style.
  const ours = (f: string): boolean => ['Source Sans 3', 'Source Serif 4', 'Source Code Pro'].some((face) => f.startsWith(face));
  expect([...families].filter((f) => !ours(f)), 'faces outside the three').toEqual([]);
  expect([...origins], 'every request goes to the page itself').toEqual([new URL(page.url()).origin]);
});

test('the stage draws though a face fails to load', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the font gate, checked once');
  const failed: string[] = [];
  page.on('requestfailed', (request) => failed.push(request.url()));
  await page.route('**/SourceSans3*.woff2', (route) => route.abort());
  await page.goto('/');
  await expect(page.locator('.stage canvas')).toBeVisible({ timeout: 30_000 });
  await expect
    .poll(() => page.locator('.stage canvas').evaluate((c: HTMLCanvasElement) => JSON.parse(c.dataset.labelBoxes ?? '[]').length))
    .toBeGreaterThan(0);
  expect(failed.length, 'the aborted face is the only failure').toBeGreaterThan(0);
  expect(failed.every((url) => /SourceSans3.*\.woff2/.test(url)), 'only the aborted face failed').toBe(true);
});

// -- A5: the key lists what is drawn ----------------------------------------------

test('the key lists exactly the marks drawn, in its fixed order (A5)', async ({ page }, info) => {
  test.skip(!['audit-1440-light', 'audit-400-dark'].includes(info.project.name), 'the key, checked at the widest and narrowest');
  await open(page);
  const presets = page.getByRole('group', { name: 'Scenario preset' }).getByRole('button');
  const names = (await presets.allInnerTexts()).map((name) => name.trim());
  for (const name of names) {
    await loadPreset(page, name);
    await expect
      .poll(async () => {
        const marks = Number(await page.locator('.stage canvas').getAttribute('data-marks'));
        const items = await page.locator('.key:not(.sizer) li').evaluateAll((els) => els.map((el) => Number((el as HTMLElement).dataset.mark)));
        const bits = Object.values(MARK).filter((bit) => (marks & bit) !== 0);
        return JSON.stringify(items) === JSON.stringify(bits);
      }, { message: `${name}: the key lists the marks drawn, in order` })
      .toBe(true);
    if (info.project.name === 'audit-1440-light') {
      const lines = await page.locator('.key:not(.sizer) li').evaluateAll((els) => new Set(els.map((el) => Math.round(el.getBoundingClientRect().top))).size);
      expect(lines, `${name}: the key takes at most two lines at 1440`).toBeLessThanOrEqual(2);
    }
  }
});
