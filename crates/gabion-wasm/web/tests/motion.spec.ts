import { readFileSync } from 'node:fs';
import { expect, test, type Page } from '@playwright/test';
import { ORIGIN } from '../src/lib/presets';
import type { ClusterState } from '../src/lib/sim/types';
import { CONTROL, CROSS, DROPPED, FADE_MS, RELAY } from '../src/lib/stage/layout';
import { BOUNCE_MS, RING_CAUGHT_UP, type BeamPool, type GlyphTable } from '../src/lib/stage/renderer';
import { BEAM_ALPHA, FADED_ALPHA } from '../src/lib/theme';
import { PERIOD_MS, WATCH, recordData, refusal, send, sendButton, setRange, setSpeed, step, still, virtualMs } from './drive';
import { sourceOf } from './sourcemap';

// The packets in motion (README "Tests"), run once at 1440 px in the light
// theme with motion on: every count changes in the frame its line appears, in
// every preset; a Send's eager flush past the emit floor; Step drawing
// Propagation's waves; Play's pace at the chosen speed; a line's fade, scaled
// by Speed and held by Pause; the stage's allocations; a ring's sweep and a
// refusal's band. Every check reads the engine (`window.sim`), the line pool
// the stage publishes (`canvas.beams`) or the DOM, never the page's
// formatters; every wait is a condition with a deadline, in simulated time
// where one is meant. A check of how long something takes to watch runs the
// page on Playwright's fake clock, so the page's wall time moves only as the
// test runs it, in frames of the same length on every machine; real wall
// times are printed, never asserted.

// The flag bits the stage's line pool packs, passed into the page's functions.
const BITS = { CROSS, CONTROL, RELAY, DROPPED } as const;
// The animation frames in each stretch of the allocation audit.
const FRAMES = 300;
// One frame of the fake clock, ms.
const FRAME_MS = 16;
// How far the test runs the fake clock between checks: a few frames.
const RUN_MS = 100;
// The longest one tick of Pixi's ticker advances, ms (its `maxElapsedMS`).
const MAX_TICK_MS = 100;
// The Hits per Send every scenario opens with.
const BURST = 25;

/** The page's engine, snapshotted, with node `inspect`'s rows. */
const snapshot = (page: Page, inspect: number | null = null): Promise<ClusterState> =>
  page.evaluate(([id, watch]) => window.sim!.snapshot(id, watch), [inspect, WATCH] as const);

/** Load the page, open "Show as table" (the DOM's counts) and wait for the
 *  stage's layout to follow the smaller box, so no line is drawn mid-resize. */
async function open(page: Page): Promise<void> {
  await page.goto('/');
  await expect(page.locator('.stage canvas')).toBeVisible({ timeout: 30_000 });
  await page.getByText('Show as table').click();
  await expect(page.locator('.node-table tr[data-node]')).toHaveCount(12);
  await expect
    .poll(() =>
      page.locator('.stage').evaluate((stage: HTMLElement) => {
        const centre = JSON.parse(stage.querySelector('canvas')?.dataset.centre ?? '{"x":0,"y":0}');
        return Math.abs(centre.x * 2 - stage.clientWidth) < 1 && Math.abs(centre.y * 2 - stage.clientHeight) < 1;
      }),
    )
    .toBe(true);
}

/** Select a scenario by its label and wait for the rebuild to finish. */
async function loadPreset(page: Page, name: string): Promise<void> {
  const preset = page.getByRole('group', { name: 'Scenario preset' }).getByRole('button', { name, exact: true });
  if ((await preset.getAttribute('aria-current')) !== 'true') await preset.click();
  await expect(preset).toHaveAttribute('aria-current', 'true');
  await expect(page.locator('.overlay.rebuilding')).toHaveCount(0);
}

/** Rebuild the running scenario with rule limit `limit` and, if given, a
 *  heartbeat period of `heartbeatMs`. */
async function rebuildAtLimit(page: Page, limit: number, heartbeatMs?: number): Promise<void> {
  await page.getByText('Tune the cluster').click();
  await page.locator('#knob-limit').fill(String(limit));
  if (heartbeatMs !== undefined) await setRange(page.locator('#knob-gossip'), heartbeatMs);
  await page.getByRole('button', { name: 'Rebuild cluster' }).click();
  await expect(page.locator('.overlay.rebuilding')).toHaveCount(0);
  await expect(page.locator('.tune summary')).not.toContainText('staged');
}

/** Select node `node` and press Send, waiting for neither: the page's queue
 *  keeps their order however its clock runs. */
async function pressSend(page: Page, node: number): Promise<void> {
  await page.locator(`[role="gridcell"][data-node="${node}"]`).press('Enter');
  await sendButton(page).click();
}

/** The stage's canvas, with the line pool it publishes. */
type Canvas = HTMLCanvasElement & { beams: BeamPool };

/** Evaluate `read` against the stage's canvas in the page. */
function pool<T>(page: Page, read: (c: Canvas) => T): Promise<T> {
  return page.locator('.stage canvas').evaluate(read);
}

/** Put the page on Playwright's fake clock, which runs on its own until
 *  `stopClock`, and load it. */
async function openOnFakeClock(page: Page): Promise<void> {
  await page.clock.install();
  await open(page);
}

/** Stop the page's fake clock: from here the page's time moves only as the
 *  test runs it (`runUntil`), until `page.clock.resume()`. The clock runs on
 *  while the test reads it, so on a loaded machine the instant read may have
 *  passed by the time the pause reaches the page: each retry reads it again,
 *  with twice the margin. */
async function stopClock(page: Page): Promise<void> {
  for (let margin = FRAME_MS; ; margin *= 2) {
    const at = await page.evaluate((m) => Date.now() + m, margin);
    try {
      await page.clock.pauseAt(at);
      return;
    } catch (e) {
      if (margin >= 1024 * FRAME_MS || !String(e).includes('Cannot fast-forward to the past')) throw e;
    }
  }
}

/** Run the page's fake clock until `done` holds, at most `maxMs` of the page's
 *  time, and return the time it ran: a deadline in page time, so the outcome is
 *  the same however loaded the machine. */
async function runUntil(page: Page, done: () => Promise<boolean>, maxMs: number, what: string): Promise<number> {
  let ran = 0;
  while (!(await done())) {
    if (ran >= maxMs) throw new Error(`${what}: not within ${maxMs} ms of the page's time`);
    await page.clock.runFor(RUN_MS);
    ran += RUN_MS;
  }
  return ran;
}

/** Each line Graphics now shown, as its layer's paint position relative to
 *  the labels' layer: above it when positive. Null when the stage has no
 *  label layer. */
function paintOrder(page: Page): Promise<number[] | null> {
  type Node = { parent: Node | null; children: Node[]; visible: boolean };
  return page.locator('.stage canvas').evaluate((c: HTMLCanvasElement & { beamGfx: Node[] }) => {
    const root = c.beamGfx[0].parent?.parent;
    if (root == null) return null;
    const labels = root.children.findIndex((layer) => layer.children.some((child) => 'text' in child));
    if (labels < 0) return null;
    return c.beamGfx.filter((g) => g.visible).map((g) => root.children.indexOf(g.parent as Node) - labels);
  });
}

/** Wait `frames` animation frames in the page: a count of frames, not a wall
 *  time. */
async function frames(page: Page, count: number): Promise<void> {
  await page.evaluate(
    (n) =>
      new Promise<void>((done) => {
        let left = n;
        const tick = (): void => {
          left -= 1;
          if (left <= 0) done();
          else requestAnimationFrame(tick);
        };
        requestAnimationFrame(tick);
      }),
    count,
  );
}

// -- The causality log ---------------------------------------------------------

/** What the causality log found. */
interface Causality {
  violations: string[];
  /** Waves stopped before for Step whose DOM counts were compared with the
   *  engine. */
  checked: number;
  /** Count rises a line into the node accounted for. */
  rises: number;
}

/** Install the causality log in the page, or reset it: idempotent per line
 *  pool and per engine, so it survives a rebuild once reinstalled. A frame
 *  counter ticks on every animation frame. It records (a) a node's count
 *  rising in a frame in which no data line into it appeared and it admitted
 *  no request of its own, and (d) the DOM counts, while Propagation's run
 *  stands before a wave, differing from the engine's, which holds the wave
 *  undelivered. */
async function installCausality(page: Page): Promise<void> {
  await page.evaluate(({ watch, bits }) => {
    type Log = {
      violations: string[];
      checked: number;
      rises: number;
      frame: number;
      lined: Map<number, number>;
      credit: Map<number, number>;
      counts: Map<number, number>;
    };
    type P = BeamPool & { causal?: boolean };
    const w = window as unknown as { causality: Log; causalObserver?: boolean };
    const readCounts = (): Map<number, number> => {
      const counts = new Map<number, number>();
      for (const row of document.querySelectorAll<HTMLElement>('.node-table tr[data-node]')) {
        const view = row.querySelector('[data-field="view"]');
        if (view !== null) counts.set(Number(row.dataset.node), Number((view.textContent ?? '').replace(/,/g, '')));
      }
      return counts;
    };
    w.causality = {
      violations: [],
      checked: 0,
      rises: 0,
      frame: w.causality?.frame ?? 0,
      lined: new Map(),
      credit: new Map(),
      counts: readCounts(),
    };
    const p = (document.querySelector('.stage canvas') as HTMLCanvasElement & { beams: P }).beams;
    if (p.causal !== true) {
      p.causal = true;
      const push = p.push.bind(p);
      p.push = (path, sent, src, dst, flags) => {
        if ((flags & (bits.CONTROL | bits.DROPPED)) === 0) w.causality.lined.set(dst, w.causality.frame);
        push(path, sent, src, dst, flags);
      };
    }
    const sim = window.sim as unknown as {
      submitRequest: (...args: unknown[]) => Promise<{ events: { kind: { type: string; node?: number; admission?: { kind: string } } }[] }>;
      causal?: boolean;
    };
    if (sim.causal !== true) {
      sim.causal = true;
      const submit = sim.submitRequest.bind(sim);
      sim.submitRequest = async (...args) => {
        const batch = await submit(...args);
        for (const { kind } of batch.events) {
          if (kind.type === 'Decided' && kind.admission?.kind === 'Admitted') w.causality.credit.set(kind.node!, w.causality.frame);
        }
        return batch;
      };
    }
    if (w.causalObserver === true) return;
    w.causalObserver = true;
    new MutationObserver(() => {
      const log = w.causality;
      const counts = readCounts();
      for (const [node, count] of counts) {
        const before = log.counts.get(node);
        if (before === undefined || count <= before) continue;
        if (log.lined.get(node) === log.frame) log.rises += 1;
        else if (log.credit.get(node) !== log.frame) {
          log.violations.push(`(a) node ${node} rose from ${before} to ${count} with no line into it in that frame`);
        }
      }
      log.counts = counts;
    }).observe(document.querySelector('.node-table')!, { subtree: true, childList: true, characterData: true });
    const pane = document.querySelector<HTMLElement>('.stage-pane')!;
    let last = pane.dataset.clock;
    const sample = (): void => {
      const log = w.causality;
      log.frame += 1;
      const clock = pane.dataset.clock;
      if (clock === 'before-wave' && last !== 'before-wave') {
        const shown = readCounts();
        void window.sim!.snapshot(null, watch).then((snap) => {
          // Compared only if the run still stands before the wave: the engine
          // then cannot have delivered it before it answered.
          if (pane.dataset.clock !== 'before-wave') return;
          for (const node of snap.nodes) {
            if (shown.get(node.id) !== node.window_total) {
              log.violations.push(`(d) node ${node.id} shows ${shown.get(node.id)} while the engine holds ${node.window_total}`);
            }
          }
          log.checked += 1;
        });
      }
      last = clock;
      requestAnimationFrame(sample);
    };
    requestAnimationFrame(sample);
  }, { watch: WATCH, bits: BITS });
}

/** What the causality log has found so far. */
const readCausality = (page: Page): Promise<Causality> =>
  page.evaluate(() => {
    const { violations, checked, rises } = (window as unknown as { causality: Causality }).causality;
    return { violations, checked, rises };
  });

/** Play at `speed` until simulated time has advanced `ms` past where it stood,
 *  then pause. */
async function playFor(page: Page, speed: number, ms: number, timeout: number): Promise<void> {
  const from = await virtualMs(page);
  await setSpeed(page, speed);
  await page.getByRole('button', { name: 'Play' }).click();
  await expect.poll(() => virtualMs(page), { timeout }).toBeGreaterThanOrEqual(from + ms);
  await page.getByRole('button', { name: 'Pause' }).click();
}

// -- Cause and effect in one frame --------------------------------------------------

test('every preset changes each count only in the frame its line appears', async ({ page }) => {
  test.setTimeout(300_000);
  await open(page);
  const names = (await page.getByRole('group', { name: 'Scenario preset' }).getByRole('button').allInnerTexts()).map((name) => name.trim());
  for (const name of names) {
    await loadPreset(page, name);
    await installCausality(page);
    const wall = Date.now();
    await setSpeed(page, 4);
    await send(page, 0);
    await playFor(page, 4, 2000, 60_000);
    const found = await readCausality(page);
    console.log(`${name}: ${found.checked} checked, ${found.rises} rise(s), ${Date.now() - wall} ms of wall time`);
    expect(found.violations, name).toEqual([]);
    expect(found.rises, `${name}: no count rose with a line into it`).toBeGreaterThanOrEqual(1);
    // Propagation's Send stops before wave 1.
    if (name === 'Propagation') {
      expect(found.checked, `${name}: no wave stopped before was compared with the engine`).toBeGreaterThanOrEqual(1);
    }
  }

  // A feed's eager flush is drawn with its counts too. A node flushes at once
  // only when its unsent hits pass ε, which is 1 at limit 100, so it needs two
  // hits in one heartbeat period: Gateway cut's trickle of two a second at
  // node 0 puts two in every period of a second.
  const name = 'Gateway cut';
  await loadPreset(page, name);
  await rebuildAtLimit(page, 100, 1000);
  await installCausality(page);
  await page.evaluate(() => {
    const w = window as unknown as { feedFlushes: number };
    w.feedFlushes = 0;
    const sim = window.sim!;
    const submit = sim.submitRequest.bind(sim);
    sim.submitRequest = async (...args) => {
      const batch = await submit(...args);
      if (batch.events.some((e) => e.kind.type === 'PacketSent' && e.kind.kind === 'Own')) w.feedFlushes += 1;
      return batch;
    };
  });
  const wall = Date.now();
  await playFor(page, 4, 10_000, 90_000);
  const found = await readCausality(page);
  const flushes = await page.evaluate(() => (window as unknown as { feedFlushes: number }).feedFlushes);
  console.log(`${name} at limit 100: ${flushes} feed flush(es), ${found.rises} rise(s), ${Date.now() - wall} ms`);
  expect(flushes, `${name} at limit 100: the feed flushed no wave`).toBeGreaterThanOrEqual(1);
  expect(found.violations, `${name} at limit 100`).toEqual([]);
  expect(found.rises, `${name} at limit 100: no count rose with a line into it`).toBeGreaterThanOrEqual(1);
});

// -- A Send's eager flush, past the emit floor ------------------------------------

test('back-to-back Sends each flush at once, past the emit floor', async ({ page }) => {
  test.setTimeout(120_000);
  await openOnFakeClock(page);
  await loadPreset(page, 'Sandbox');
  await rebuildAtLimit(page, 100);
  await installCausality(page);
  // Each submit's own data packets: an eager flush sends them in the submit's
  // batch.
  await page.evaluate(() => {
    const w = window as unknown as { own: number[] };
    w.own = [];
    const sim = window.sim!;
    const submit = sim.submitRequest.bind(sim);
    sim.submitRequest = async (...args) => {
      const batch = await submit(...args);
      w.own.push(batch.events.filter((e) => e.kind.type === 'PacketSent' && e.kind.kind === 'Own').length);
      return batch;
    };
  });
  // Clicked back to back, with the page's clock standing still, so no frame
  // moves time between them: each Send first runs the one before past its
  // relays and gabion's emit floor, so it meets no floor.
  await stopClock(page);
  for (const node of [0, 1, 2, 2]) await pressSend(page, node);
  const ran = await runUntil(
    page,
    async () => (await page.evaluate(() => (window as unknown as { own: number[] }).own.length)) === 4,
    10 * FRAME_MS,
    'the four Sends were decided',
  );
  console.log(`four back-to-back Sends flushed in ${ran} ms of the page's time`);
  await page.clock.resume();
  await still(page);
  expect(await page.evaluate(() => (window as unknown as { own: number[] }).own), 'Own packets per Send').toEqual([5, 5, 5, 5]);
  expect((await readCausality(page)).violations).toEqual([]);
});

// -- Step by event ------------------------------------------------------------------

test("Step draws Propagation's waves one at a time, each with its counts", async ({ page }, info) => {
  test.setTimeout(120_000);
  await open(page);
  await loadPreset(page, 'Propagation');
  await setSpeed(page, 1);
  await installCausality(page);
  const data = await recordData(page);
  const view = (node: number) => page.locator(`.node-table tr[data-node="${node}"] [data-field="view"]`);
  const pane = page.locator('.stage-pane');
  const stage = page.locator('.stage');
  // Propagation's Send plays to node 0's heartbeat and stops before its first
  // wave, which the engine holds: only node 0 counts the Send.
  await send(page, 0);
  await expect(pane).toHaveAttribute('data-clock', 'before-wave');
  expect(await data(), "Propagation's Send stopped before its first wave").toEqual([]);
  const held = await snapshot(page);
  expect(held.nodes.map((n) => n.window_total), 'the engine holds wave 1').toEqual(held.nodes.map((n) => (n.id === 0 ? 75 : 50)));
  for (const n of held.nodes) await expect(view(n.id)).toHaveText(String(n.window_total));
  const gateways = ((await snapshot(page, 0)).inspected?.peers ?? []).filter((p) => p.role === 'gateway').map((p) => p.id);
  const zones = held.zones.length;

  let wall = Date.now();
  await step(page);
  const wave1 = await data();
  console.log(`Step 1 took ${Date.now() - wall} ms`);
  expect(wave1.length, 'Step 1 drew node 0 to its zone and one gateway per other zone').toBe(5);
  expect(wave1.every((b) => b.src === 0 && (b.flags & RELAY) === 0), 'Step 1: node 0, no relay').toBe(true);
  expect(wave1.filter((b) => (b.flags & CROSS) !== 0).length, 'Step 1 crossed a zone Z − 1 times').toBe(zones - 1);
  const reached = await snapshot(page);
  for (const { dst } of wave1) {
    expect(reached.nodes.find((n) => n.id === dst)?.window_total, `node ${dst} counted wave 1`).toBe(75);
    await expect(view(dst)).toHaveText('75');
  }
  // Wave 1 delivered, the run plays on to the relays, which it stops before.
  await expect(pane, 'the relays wait for Step').toHaveAttribute('data-clock', 'before-wave');
  await stage.screenshot({ path: `audit-shots/${info.project.name}/propagation-wave1.png` });

  wall = Date.now();
  await step(page);
  const wave2 = await data();
  console.log(`Step 2 took ${Date.now() - wall} ms`);
  expect(wave2.length, 'Step 2 drew the relays').toBe(6);
  expect(wave2.every((b) => (b.flags & RELAY) !== 0 && gateways.includes(b.src)), "Step 2: relays from node 0's gateways").toBe(true);
  const agreed = await snapshot(page);
  expect(agreed.nodes.map((n) => n.window_total), 'every count is the truth').toEqual(agreed.nodes.map(() => agreed.oracle_total));
  expect(agreed.oracle_total).toBe(75);
  await stage.screenshot({ path: `audit-shots/${info.project.name}/propagation-wave2.png` });

  // The next event is the window's edge: the seed's 50 leave (its bucket is
  // older than the Send's), with no packet.
  const edge = agreed.next_expiry_ms;
  await step(page);
  expect(await data(), 'Step 3 drew no data').toEqual([]);
  const after = await snapshot(page);
  expect(after.virtual_ms, 'Step 3 stopped at the window edge').toBe(edge);
  expect(after.nodes.map((n) => n.window_total), "the seed's 50 left every node at once").toEqual(after.nodes.map(() => 25));

  await step(page);
  expect((await snapshot(page)).nodes.map((n) => n.window_total), "the Send's 25 left too").toEqual(after.nodes.map(() => 0));
  const found = await readCausality(page);
  expect(found.violations).toEqual([]);
  expect(found.checked, 'both waves stopped before were compared with the engine').toBeGreaterThanOrEqual(2);
});

// -- Play's pace ---------------------------------------------------------------------

// The page's time Play runs for in each scenario.
const PACE_MS = 2000;

// Owner Q16 holds for a Send that flushes at once too: at a limit of 100 the
// error budget is one hit, so the Send's own wave leaves in its own batch, and
// Propagation's Send still stops before it, and Step draws it with its counts.
test("Propagation's Send stops before its wave when it flushes at once", async ({ page }) => {
  await open(page);
  await loadPreset(page, 'Propagation');
  await rebuildAtLimit(page, 100);
  const data = await recordData(page);
  const pane = page.locator('.stage-pane');
  const before = await snapshot(page);
  await send(page, 0);
  await expect(pane).toHaveAttribute('data-clock', 'before-wave');
  expect(await data(), 'the Send drew none of its wave').toEqual([]);
  const plus = (id: number, from: ClusterState): number => (from.nodes.find((n) => n.id === id)?.window_total ?? Number.NaN) + BURST;
  const held = await snapshot(page);
  expect(held.nodes.map((n) => n.window_total), 'only node 0 counted the Send').toEqual(before.nodes.map((n) => (n.id === 0 ? plus(0, before) : n.window_total)));
  await step(page);
  const wave = await data();
  expect(wave.length, "Step drew node 0's wave: its zone and one gateway per other zone").toBe(5);
  expect(wave.every((b) => b.src === 0 && (b.flags & RELAY) === 0), 'Step: node 0, no relay').toBe(true);
  const reached = await snapshot(page);
  for (const { dst } of wave) expect(reached.nodes.find((n) => n.id === dst)?.window_total, `node ${dst} counted the Send`).toBe(plus(dst, before));
});

test('Play runs at the chosen speed in every scenario, never waiting for a packet', async ({ page }) => {
  test.setTimeout(240_000);
  await openOnFakeClock(page);
  const names = (await page.getByRole('group', { name: 'Scenario preset' }).getByRole('button').allInnerTexts()).map((name) => name.trim());
  for (const name of names) {
    await loadPreset(page, name);
    await setSpeed(page, 1);
    // The most lines on stage at once.
    await page.evaluate(() => {
      const w = window as unknown as { peak: number };
      w.peak = 0;
      const p = (document.querySelector('.stage canvas') as Canvas & { beams: { paced?: boolean } }).beams;
      if (p.paced === true) return;
      p.paced = true;
      const push = p.push.bind(p);
      p.push = (...args) => {
        push(...args);
        w.peak = Math.max(w.peak, p.len);
      };
    });
    await stopClock(page);
    const from = await virtualMs(page);
    await page.getByRole('button', { name: 'Play' }).click();
    await page.clock.runFor(PACE_MS);
    await page.getByRole('button', { name: 'Pause' }).click();
    const pace = ((await virtualMs(page)) - from) / PACE_MS;
    const peak = await page.evaluate(() => (window as unknown as { peak: number }).peak);
    console.log(`${name}: ${pace.toFixed(3)} s simulated per second at 1×, peak ${peak} lines`);
    expect(pace, `${name} plays at the speed`).toBeGreaterThanOrEqual(name === 'Steady state' ? 0.95 : 0.9);
    await page.clock.resume();
  }
});

// -- A line's fade -------------------------------------------------------------------

// The slow speed the fade is checked at.
const SLOW = 0.2;

test('a line fades over one simulated second, and Speed scales it', async ({ page }) => {
  test.setTimeout(180_000);
  await openOnFakeClock(page);
  await loadPreset(page, 'Rate limiting');
  // A data line's life, from the pool to its removal, in the page's ms.
  await page.evaluate((control) => {
    const w = window as unknown as { lives: number[]; born: Map<string, number> };
    w.lives = [];
    w.born = new Map();
    const p = (document.querySelector('.stage canvas') as Canvas).beams;
    const push = p.push.bind(p);
    p.push = (path, sent, src, dst, flags) => {
      if ((flags & control) === 0) w.born.set(`${src}:${dst}:${sent}`, performance.now());
      push(path, sent, src, dst, flags);
    };
    const removeAt = p.removeAt.bind(p);
    p.removeAt = (i) => {
      const born = w.born.get(`${p.src[i]}:${p.dst[i]}:${p.sent[i]}`);
      if (born !== undefined) w.lives.push(performance.now() - born);
      removeAt(i);
    };
  }, BITS.CONTROL);
  await stopClock(page);
  const lifeAt = async (speed: number, node: number): Promise<number> => {
    await page.evaluate(() => {
      const w = window as unknown as { lives: number[]; born: Map<string, number> };
      w.lives = [];
      w.born.clear();
    });
    await setSpeed(page, speed);
    await page.getByRole('button', { name: 'Play' }).click();
    // Past Play's first frame, which runs a sixth of a heartbeat at once.
    await page.clock.runFor(RUN_MS);
    await pressSend(page, node);
    await runUntil(
      page,
      async () => (await page.evaluate(() => (window as unknown as { lives: number[] }).lives.length)) >= 11,
      (3 * FADE_MS) / speed,
      `node ${node}'s two waves faded at ${speed}×`,
    );
    await page.getByRole('button', { name: 'Pause' }).click();
    const lives = await page.evaluate(() => (window as unknown as { lives: number[] }).lives.slice(0, 11));
    return lives.reduce((a, b) => a + b, 0) / lives.length;
  };
  // A line's first age is up to a frame of simulated time, and its removal
  // up to a frame late, so its life is within two frames of the fade over
  // the speed. The speed slider moves in tenths, so a fifth of the speed is
  // the slowest round fraction it holds.
  const full = await lifeAt(1, 0);
  const fifth = await lifeAt(SLOW, 1);
  console.log(`a line's life: ${full.toFixed(0)} ms at 1×, ${fifth.toFixed(0)} ms at ${SLOW}×, ${(fifth / full).toFixed(2)} times as long`);
  expect(Math.abs(full - FADE_MS), 'a line at 1× lives the fade').toBeLessThanOrEqual(2 * FRAME_MS);
  expect(Math.abs(fifth - FADE_MS / SLOW), `a line at ${SLOW}× lives the fade over the speed`).toBeLessThanOrEqual(2 * FRAME_MS);
});

// Its own test, so a fault that also cuts a line's life short still meets
// these checks.
test('Pause holds the lines still, each at its ink times its fade left, under the labels', async ({ page }, info) => {
  test.setTimeout(60_000);
  await open(page);
  await loadPreset(page, 'Rate limiting');
  // Paused mid-fade, the lines hold still: their opacities, the pool and the
  // stage's pixels. The watcher pauses the first frame a line of node 2's
  // Send is half faded, after the rings' sweeps (a quarter second of the
  // presentation clock, 1.25 s of wall time at a fifth of the speed) have
  // ended.
  await setSpeed(page, SLOW);
  await page.getByRole('button', { name: 'Play' }).click();
  await pressSend(page, 2);
  await page.evaluate((bits) => {
    const p = (document.querySelector('.stage canvas') as Canvas).beams;
    const watch = (): void => {
      for (let i = 0; i < p.len; i++) {
        if (p.src[i] === 2 && (p.flags[i] & (bits.CONTROL | bits.DROPPED)) === 0 && p.alpha[i] > 0.3 && p.alpha[i] < 0.5) {
          [...document.querySelectorAll<HTMLButtonElement>('.transport button')].find((b) => b.textContent?.trim() === 'Pause')?.click();
          return;
        }
      }
      requestAnimationFrame(watch);
    };
    requestAnimationFrame(watch);
  }, BITS);
  await expect(page.getByRole('button', { name: 'Play' })).toBeVisible({ timeout: 30_000 });
  await frames(page, 2);
  type Still = { len: number; redraws: number; alpha: number[]; sent: number[]; gfx: [boolean, number][]; hidden: boolean; now: number };
  const read = (): Promise<Still> =>
    page.locator('.stage canvas').evaluate((c: Canvas & { beamGfx: { visible: boolean; alpha: number }[] }) => {
      const p = c.beams;
      const ms = Number(document.querySelector<HTMLElement>('[data-field="virtual-time"]')?.dataset.ms);
      return {
        len: p.len,
        redraws: p.redraws,
        alpha: [...p.alpha.subarray(0, p.len)],
        sent: [...p.sent.subarray(0, p.len)],
        gfx: c.beamGfx.slice(0, p.len).map((g): [boolean, number] => [g.visible, g.alpha]),
        hidden: c.beamGfx.slice(p.len).every((g) => !g.visible),
        now: ms,
      };
    });
  const before = await read();
  expect(before.len, 'paused with lines on the stage').toBeGreaterThan(0);
  const stage = page.locator('.stage');
  const pixels = await stage.screenshot();
  await stage.screenshot({ path: `audit-shots/${info.project.name}/lines-paused.png` });
  await frames(page, 60);
  expect(await read(), 'the lines held still').toEqual(before);
  expect((await stage.screenshot()).equals(pixels), 'the stage held still').toBe(true);
  // Each line is drawn at its opacity, its ink times what is left of its fade.
  expect(before.hidden, 'every free slot is hidden').toBe(true);
  for (let i = 0; i < before.len; i++) {
    expect(before.gfx[i], `line ${i} is shown at its opacity`).toEqual([true, before.alpha[i]]);
    const left = 1 - (before.now - before.sent[i]) / FADE_MS;
    const off = Math.min(...[BEAM_ALPHA, BEAM_ALPHA * FADED_ALPHA].map((peak) => Math.abs(before.alpha[i] - peak * left)));
    expect(off, `line ${i}'s opacity is its ink times its fade left`).toBeLessThanOrEqual(1e-9);
  }
  const order = await paintOrder(page);
  expect(order, 'the stage has a label layer').not.toBeNull();
  expect(order!.length, 'the lines are drawn').toBe(before.len);
  expect(order!.filter((d) => d >= 0), 'a line painted over the labels').toEqual([]);
});

test('the transport says how long a heartbeat takes to watch', async ({ page }) => {
  await openOnFakeClock(page);
  await stopClock(page);
  // Propagation opens settled, so its periods pass with nothing held.
  await setSpeed(page, 4);
  const wallMs = page.locator('[data-field="heartbeat-wall"]');
  await page.getByRole('button', { name: 'Play' }).click();
  await runUntil(page, async () => (await wallMs.getAttribute('data-ms')) !== null, 4 * PERIOD_MS, 'a heartbeat reading');
  const measured = Number(await wallMs.getAttribute('data-ms'));
  await page.getByRole('button', { name: 'Pause' }).click();
  console.log(`a heartbeat took ${measured.toFixed(0)} ms to watch at 4×`);
  // The play loop sees a period end only at a frame, so the reading is up to
  // a frame off the period over the speed.
  expect(Math.abs(measured - PERIOD_MS / 4), 'the reading is the period over the speed').toBeLessThanOrEqual(FRAME_MS);
});

/** The functions the stage runs every frame, by source file: the ticker, the
 *  lines' fade, and every stage function they call; and the path that draws
 *  a packet's line, which runs in every frame that sends one. */
const PER_FRAME = {
  'renderer.ts': [
    'push',
    'setRoute',
    'removeAt',
    'oldest',
    'show',
    '#beam',
    '#routeOf',
    '#drawLine',
    '#drop',
    '#fadeLines',
    '#isContext',
    '#tick',
    '#route',
    '#tangentAt',
    '#arrowhead',
    '#stopBar',
    '#dropStop',
    '#tickRings',
    '#tickRefusal',
    '#syncNode',
    '#drawRing',
    '#drawVessel',
    'levelY',
  ],
  'layout.ts': ['pointOn', 'reachT', 'beamPath'],
} as const;

/** Each per-frame function's 1-based line range in its source file, read from
 *  the source, keyed by file. Throws naming a function it cannot find, so a
 *  rename cannot quietly empty the check. */
function perFrameLines(): Map<string, [number, number][]> {
  const ranges = new Map<string, [number, number][]>();
  for (const [file, names] of Object.entries(PER_FRAME)) {
    const lines = readFileSync(`src/lib/stage/${file}`, 'utf8').split('\n');
    ranges.set(
      file,
      names.map((name) => {
        // A method opens indented two spaces and closes at that indent; a
        // function opens and closes at the margin.
        const heads = [`  ${name}(`, `function ${name}(`, `export function ${name}(`];
        const start = lines.findIndex((line) => heads.some((head) => line.startsWith(head)));
        if (start < 0) throw new Error(`${file} has no function ${name}`);
        const close = lines[start].startsWith(' ') ? '  }' : '}';
        const end = lines.findIndex((line, i) => i > start && line === close);
        if (end < 0) throw new Error(`${file}'s ${name} has no closing brace`);
        return [start + 1, end + 1];
      }),
    );
  }
  return ranges;
}

/** The parts of a heap snapshot taken while allocations were tracked that the
 *  allocation audit reads. */
interface TrackedSnapshot {
  snapshot: { meta: { trace_function_info_fields: string[] } };
  trace_function_infos: number[];
  trace_tree: unknown[];
  strings: string[];
}

/** One function's allocations at one call stack: its name, script, 1-based
 *  start line and column, and the objects it allocated and their bytes. */
type Allocations = { name: string; script: string; line: number; column: number; count: number; size: number };

/** The function a call stack's allocations are charged to. */
type Owner = Omit<Allocations, 'count' | 'size'>;

/** Each call stack's allocations in a tracked heap snapshot, charged to the
 *  function on top, or, when that is one of V8's builtins (a frame of no
 *  script: `slice`, `map`, a spread), to the function that called it, whose
 *  code asked for the object. Its trace tree nests nodes of five fields (`id,
 *  function_info_index, count, size, children`); a function's info is
 *  `function_id, name, script_name, script_id, line, column`. */
function* allocations(snapshot: TrackedSnapshot, nodes = snapshot.trace_tree, caller?: Owner): Generator<Allocations> {
  const width = snapshot.snapshot.meta.trace_function_info_fields.length;
  const info = snapshot.trace_function_infos;
  const text = snapshot.strings;
  for (let i = 0; i < nodes.length; i += 5) {
    const [fn, count, size, children] = nodes.slice(i + 1, i + 5) as [number, number, number, unknown[]];
    const at = fn * width;
    const builtin = info[at + 3] === 0;
    const owner: Owner =
      builtin && caller !== undefined ? caller : { name: text[info[at + 1]], script: text[info[at + 2]], line: info[at + 4], column: info[at + 5] };
    yield { ...owner, count, size };
    yield* allocations(snapshot, children, owner);
  }
}

// The allocation audit's own browser runs V8's interpreter only. Nothing is
// inlined, so an allocation is charged to the function whose code made it
// (inlined, Pixi's Graphics calls would charge their own allocations to the
// stage's code), and nothing tiers up or deoptimizes, so on the fake clock the
// same frames allocate the same objects on every run. It is also the strictest
// reading: an object the code makes counts even where an optimizer would elide it.
const INTERPRETED = '--js-flags=--no-opt --no-maglev --no-sparkplug';
// The untracked stretches of `FRAMES` frames, each opened by a Send, run
// before tracking starts. V8 makes objects of its own once, not per frame: each
// function's bytecode and feedback on its first runs, a property access's
// feedback as it first meets another shape, and an object's new shape when a
// field first holds a fraction. On the fake clock's whole-millisecond frames
// the last of these comes in the third stretch.
const WARM_STRETCHES = 3;

/** Each stretch of the allocation audit: a preset played with node 0
 *  selected, and what each stretch presses. Packet loss draws stop bars and
 *  resends; Rate limiting redraws vessels, sweeps
 *  rings and, three Sends of 50 a stretch against its limit of 100, plays a
 *  refusal; Propagation sweeps the selection toward node 0's heartbeat. */
// The rule window every preset runs with.
const WINDOW_MS = 10_000;

const STRETCHES: readonly { name: string; hits?: number; sends: number; warm: number }[] = [
  { name: 'Packet loss', sends: 1, warm: WARM_STRETCHES },
  // Each stretch that opens on an empty window admits two Sends and refuses
  // the third, so the tracked one repeats a warm one.
  { name: 'Rate limiting', hits: 50, sends: 3, warm: 2 * WARM_STRETCHES },
  { name: 'Propagation', sends: 1, warm: WARM_STRETCHES },
];

for (const stretchOf of STRETCHES) {
  test(`the stage allocates nothing per frame: ${stretchOf.name}`, async ({ playwright, baseURL, viewport, colorScheme, reducedMotion }) => {
    test.setTimeout(120_000);
    const browser = await playwright.chromium.launch({ args: [INTERPRETED] });
    try {
      const page = await browser.newPage({ baseURL, viewport, colorScheme, reducedMotion });
      await openOnFakeClock(page);
      await stopClock(page);
      await loadPreset(page, stretchOf.name);
      if (stretchOf.hits !== undefined) {
        await page.locator('#burst-hits').fill(String(stretchOf.hits));
        await page.locator('#burst-hits').press('Enter');
      }
      const title = page.locator('.pane-title [data-field="node"]');
      await page.locator(`[role="gridcell"][data-node="${ORIGIN}"]`).press('Enter');
      await expect(title).toHaveText(String(ORIGIN));
      // The relays node 0's gateways send, by their senders, as each line is
      // drawn: lines fade within a stretch, so the pool at its end is no
      // evidence.
      await page.evaluate((relay) => {
        const w = window as unknown as { relays: number[] };
        w.relays = [];
        const p = (document.querySelector('.stage canvas') as Canvas).beams;
        const push = p.push.bind(p);
        p.push = (path, sent, src, dst, flags) => {
          if ((flags & relay) !== 0) w.relays.push(src);
          push(path, sent, src, dst, flags);
        };
      }, RELAY);
      await page.getByRole('button', { name: 'Play' }).click();
      // Sends while playing leave time alone: the stretch's frames are the
      // play loop's.
      const stretch = async (): Promise<void> => {
        for (let i = 0; i < stretchOf.sends; i++) await sendButton(page).click();
        await page.clock.runFor(FRAMES * FRAME_MS);
      };
      for (let warm = 0; warm < stretchOf.warm; warm++) await stretch();
      // The tracked stretch opens on an empty window, as the first warm one
      // did, so it admits two Sends and refuses the third.
      if (stretchOf.name === 'Rate limiting') {
        await runUntil(page, async () => (await snapshot(page)).oracle_total === 0, 4 * WINDOW_MS, 'the window emptied');
      }
      const decidedFrom = await snapshot(page);
      const perFrame = perFrameLines();
      const cdp = await page.context().newCDPSession(page);
      const chunks: string[] = [];
      cdp.on('HeapProfiler.addHeapSnapshotChunk', ({ chunk }) => void chunks.push(chunk));
      await cdp.send('HeapProfiler.enable');
      // Tracking allocations counts every one, with its call stack. A double the
      // interpreter computes is boxed (a HeapNumber): the engine's, not an object
      // the code made. A box's size is read from a function that does nothing
      // else, so the per-frame functions' bytes are that size times their count
      // exactly, and any object of another size leaves bytes over.
      await cdp.send('HeapProfiler.startTrackingHeapObjects', { trackAllocations: true });
      await page.evaluate(() => {
        const field: { v: unknown } = { v: '' };
        function boxDoubles(): void {
          for (let i = 0; i < 2000; i++) field.v = i + 0.5;
        }
        boxDoubles();
      });
      const redrawsFrom = await pool(page, (c) => c.beams.redraws);
      await page.evaluate(() => {
        (window as unknown as { relays: number[] }).relays = [];
      });
      await stretch();
      const redraws = (await pool(page, (c) => c.beams.redraws)) - redrawsFrom;
      await cdp.send('HeapProfiler.stopTrackingHeapObjects', { reportProgress: false });
      const tracked = JSON.parse(chunks.join('')) as TrackedSnapshot;
      await expect(title, 'node 0 stayed selected').toHaveText(String(ORIGIN));
      if (stretchOf.name === 'Rate limiting') {
        const after = await snapshot(page);
        expect(after.admitted_hits, 'the tracked stretch admitted a Send').toBeGreaterThan(decidedFrom.admitted_hits);
        expect(after.rejected_hits, 'the tracked stretch refused a Send').toBeGreaterThan(decidedFrom.rejected_hits);
      }
      if (stretchOf.name === 'Packet loss') {
        // The relays of node 0's gateways: the lines only a selection's roles
        // single out.
        const gateways = (await snapshot(page, ORIGIN)).inspected!.peers.flatMap((p) =>
          p.role === 'gateway' && p.id !== undefined ? [p.id] : [],
        );
        const relays = await page.evaluate(() => (window as unknown as { relays: number[] }).relays);
        const ours = relays.filter((src) => gateways.includes(src)).length;
        expect(ours, "the stretch drew a relay from one of node 0's gateways").toBeGreaterThan(0);
      }

      const box = { count: 0, size: 0 };
      for (const a of allocations(tracked)) {
        if (a.name !== 'boxDoubles') continue;
        box.count += a.count;
        box.size += a.size;
      }
      const boxed = Math.floor(box.size / box.count);
      let count = 0;
      let over = 0;
      const where: [string, number][] = [];
      for (const a of allocations(tracked)) {
        const bundle = a.script.match(/\/assets\/([^/?#]+\.js)$/)?.[1];
        if (a.count === 0 || bundle === undefined || a.line === 0) continue;
        // The function's opening line in its source.
        const source = sourceOf(`dist/assets/${bundle}.map`, a.line - 1, a.column - 1);
        if (source === null || !source.file.includes('src/lib/stage/')) continue;
        const ranges = perFrame.get(source.file.slice(source.file.lastIndexOf('/') + 1)) ?? [];
        if (!ranges.some(([from, to]) => source.line >= from && source.line <= to)) continue;
        // Unsigned, so one call stack's shortfall cannot hide another's excess.
        const extra = Math.abs(a.size - boxed * a.count);
        count += a.count;
        over += extra;
        if (extra !== 0) where.push([`${source.file}:${source.line}`, extra]);
      }
      console.log(
        `${stretchOf.name}: ${count} allocations in the per-frame functions over ${redraws} redraws, ` +
          `boxed doubles of ${boxed} bytes, ${over} bytes over: ${JSON.stringify(where)}`,
      );
      expect(boxed, "a boxed double's size was measured").toBeGreaterThan(0);
      expect(redraws, 'the line layer faded while lines lived').toBeGreaterThan(0);
      expect(count, 'the source map placed no allocation in the per-frame functions').toBeGreaterThan(0);
      expect(over, `bytes over boxed doubles in the per-frame functions over ${FRAMES} frames`).toBe(0);
    } finally {
      await browser.close();
    }
  });
}

// -- The ring's sweep ----------------------------------------------------------------

test('a ring sweeps up to its new share in the frame its line appears (A13)', async ({ page }) => {
  test.setTimeout(60_000);
  // Every ring's arc, each time it changes, from the page's first frame.
  await page.addInitScript(() => {
    const w = window as unknown as { arcs: Map<number, number[]> };
    w.arcs = new Map();
    const sample = (): void => {
      const g = (document.querySelector('.stage canvas') as (HTMLCanvasElement & { glyphs?: GlyphTable }) | null)?.glyphs;
      for (let s = 0; g !== undefined && s < g.len; s++) {
        if (g.id[s] < 0) continue;
        const seen = w.arcs.get(g.id[s]) ?? [];
        if (seen.at(-1) !== g.arc[s]) seen.push(g.arc[s]);
        w.arcs.set(g.id[s], seen);
      }
      requestAnimationFrame(sample);
    };
    requestAnimationFrame(sample);
  });
  await open(page);
  await loadPreset(page, 'Rate limiting');
  await setSpeed(page, 1);
  // Every ring opens closed and caught up: nothing sweeps on load.
  const opened = await page.locator('.stage canvas').evaluate((c: HTMLCanvasElement & { glyphs: GlyphTable }) => {
    const g = c.glyphs;
    const out: string[] = [];
    for (let s = 0; s < g.len; s++) if (g.id[s] >= 0) out.push(`${g.ring[s]}|${g.arc[s]}`);
    return out;
  });
  expect(opened, 'every ring at load').toEqual(opened.map(() => `${RING_CAUGHT_UP}|1`));
  const loaded = await page.evaluate(() => [...(window as unknown as { arcs: Map<number, number[]> }).arcs]);
  expect(loaded.length, 'every ring was sampled from the first frame').toBe(12);
  for (const [id, seen] of loaded) expect(seen, `node ${id}'s ring since the first frame`).toEqual([1]);
  await page.evaluate(() => (window as unknown as { arcs: Map<number, number[]> }).arcs.clear());
  // The nodes the Send's eager flush reaches, wave 1.
  await page.evaluate((bits) => {
    const w = window as unknown as { wave1: number[] };
    w.wave1 = [];
    const p = (document.querySelector('.stage canvas') as Canvas).beams;
    const push = p.push.bind(p);
    p.push = (path, sent, src, dst, flags) => {
      if (src === 0 && (flags & (bits.CONTROL | bits.RELAY)) === 0) w.wave1.push(dst);
      push(path, sent, src, dst, flags);
    };
  }, BITS);
  await send(page, 0);
  const wave1 = await page.evaluate(() => (window as unknown as { wave1: number[] }).wave1);
  expect(wave1.length, "the Send's eager flush reached five nodes").toBe(5);
  const arcs = await page.evaluate((origin) => [...(window as unknown as { arcs: Map<number, number[]> }).arcs].filter(([id]) => id !== origin), ORIGIN);
  expect(arcs.length, 'every receiver was sampled').toBe(11);
  for (const [id, seen] of arcs) {
    // Wave 1's receivers count the hits in the frame the truth does, so their
    // rings never open.
    if (wave1.includes(id)) {
      expect(seen, `node ${id}'s ring, reached by wave 1`).toEqual([1]);
      continue;
    }
    const low = seen.indexOf(Math.min(...seen));
    const rise = seen.slice(low);
    expect(rise.every((v, i) => i === 0 || v >= rise[i - 1]), `node ${id}'s ring only rises once its line appears: ${seen}`).toBe(true);
    expect(rise.some((v) => v > rise[0] && v < rise.at(-1)!), `node ${id}'s ring sweeps through its way up: ${seen}`).toBe(true);
  }
});

// -- The refusal in motion -----------------------------------------------------------

test("a refusal's band drops, rebounds and fades in the headroom", async ({ page }) => {
  // The stage at its full size, the table closed, so node 0 shows its count.
  await page.goto('/');
  await expect(page.locator('.stage canvas')).toBeVisible({ timeout: 30_000 });
  await loadPreset(page, 'Rate limiting');
  // A request of the whole limit, then one more: refused at node 0.
  const field = page.locator('#burst-hits');
  await field.fill('100');
  await field.press('Enter');
  await send(page, 0);
  await expect(page.locator('[role="gridcell"][data-node="0"] .count')).toHaveText('100');
  await setSpeed(page, 1);
  const frames = await refusal(page, 0, 'motion');
  expect(new Set(frames.map((f) => f.top)).size, 'the band moves').toBeGreaterThan(1);
  // It drops from the disc's top: its bottom never rises above the disc's
  // inner edge, and its first frame is at most one tick of the presentation
  // clock in (Pixi caps a tick at `MAX_TICK_MS`), so it has fallen at most
  // that tick's share of its drop, eased in, (2u)² of it.
  const [first] = frames;
  const disc = -(first.r - 1);
  const drop = first.brim - disc;
  const u = MAX_TICK_MS / BOUNCE_MS;
  for (const f of frames) expect(f.bottom, "the band never rises above the disc's top").toBeGreaterThanOrEqual(disc - 1e-3);
  expect(first.bottom - disc, "the band starts at the disc's top").toBeLessThanOrEqual(drop * (2 * u) ** 2 + 1e-3);
});
