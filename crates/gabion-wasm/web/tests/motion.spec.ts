import { readFileSync } from 'node:fs';
import { expect, test, type Page } from '@playwright/test';
import { ORIGIN } from '../src/lib/presets';
import type { ClusterState } from '../src/lib/sim/types';
import { BEAM_FLIGHT_MS, CONTROL, CROSS, DROPPED, LANDED, RELAY } from '../src/lib/stage/layout';
import type { BeamPool } from '../src/lib/stage/renderer';
import { LANDED_ALPHA } from '../src/lib/theme';
import { PERIOD_MS, WATCH, refusal, send, sendButton, setRange, setSpeed, step, virtualMs } from './drive';
import { sourceOf } from './sourcemap';

// The waves in motion (README "Tests"), run once at 1440 px in the light theme
// with motion on: cause before effect in every held preset, a Send's eager
// flush past the emit floor, Step by event, the presentation clock, the stage's
// allocations, a refusal's band and the landed routes. Every check reads the engine
// (`window.sim`), the beam pool the stage publishes (`canvas.beams`) or the
// DOM, never the page's formatters; every wait is a condition with a deadline,
// in simulated time where one is meant. A check of how long something takes to
// watch runs the page on Playwright's fake clock, so the page's wall time moves
// only as the test runs it, in frames of the same length on every machine;
// real wall times are printed, never asserted.

// The flag bits the stage's beam pool packs, passed into the page's functions.
const BITS = { CROSS, CONTROL, RELAY, DROPPED, LANDED } as const;
// The animation frames in each stretch of the allocation audit.
const FRAMES = 300;
// One frame of the fake clock, ms.
const FRAME_MS = 16;
// How far the test runs the fake clock between checks: a few frames.
const RUN_MS = 100;

/** The page's engine, snapshotted, with node `inspect`'s rows. */
const snapshot = (page: Page, inspect: number | null = null): Promise<ClusterState> =>
  page.evaluate(([id, watch]) => window.sim!.snapshot(id, watch), [inspect, WATCH] as const);

/** Load the page, open "Show as table" (the DOM's counts) and wait for the
 *  stage's layout to follow the smaller box, so no beam is drawn mid-resize. */
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

/** Wait until no wave is held and no beam is in flight. */
async function still(page: Page): Promise<void> {
  await expect(page.locator('.stage-pane')).toHaveAttribute('data-held', '0', { timeout: 30_000 });
  await expect.poll(() => pool(page, (c) => c.beams.inFlight), { timeout: 30_000 }).toBe(0);
}

/** The stage's canvas, with the beam pool it publishes. */
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
 *  test runs it (`runUntil`), until `page.clock.resume()`. */
async function stopClock(page: Page): Promise<void> {
  await page.clock.pauseAt(await page.evaluate((frame) => Date.now() + frame, FRAME_MS));
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

/** Each beam Graphics now drawing a mark, as its layer's paint position
 *  relative to the labels' layer: above it when positive. Null when the stage
 *  has no label layer. */
function paintOrder(page: Page): Promise<number[] | null> {
  type Node = { parent: Node | null; children: Node[] };
  type Gfx = Node & { context: { instructions: unknown[] } };
  return page.locator('.stage canvas').evaluate((c: HTMLCanvasElement & { beamGfx: Gfx[] }) => {
    const root = c.beamGfx[0].parent?.parent;
    if (root == null) return null;
    const labels = root.children.findIndex((layer) => layer.children.some((child) => 'text' in child));
    if (labels < 0) return null;
    return c.beamGfx
      .filter((g) => g.context.instructions.length > 0)
      .map((g) => root.children.indexOf(g.parent as Node) - labels);
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
  /** Holds whose DOM counts were compared with the frozen engine. */
  checked: number;
  /** Holds seen. */
  holds: number;
  /** Count rises a landed packet accounted for. */
  rises: number;
}

/** Install the causality log in the page, or reset it: idempotent per beam
 *  pool and per engine, so it survives a rebuild once reinstalled. It records
 *  (a) a node's count rising before a beam into it landed or its own admitted
 *  request, (b) a wave's beams leaving while another wave's were in flight,
 *  (c) simulated time moving while one wave is held, and (d) a held wave's DOM
 *  counts differing from the engine's, frozen while it is held. */
async function installCausality(page: Page): Promise<void> {
  await page.evaluate(({ watch, bits }) => {
    type Log = {
      violations: string[];
      checked: number;
      holds: number;
      rises: number;
      inflight: number;
      lastStart: number | null;
      landed: Set<number>;
      credit: Set<number>;
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
      holds: 0,
      rises: 0,
      inflight: 0,
      lastStart: null,
      landed: new Set(),
      credit: new Set(),
      counts: readCounts(),
    };
    const p = (document.querySelector('.stage canvas') as HTMLCanvasElement & { beams: P }).beams;
    if (p.causal !== true) {
      p.causal = true;
      const push = p.push.bind(p);
      p.push = (path, start, src, dst, flags) => {
        const log = w.causality;
        if ((flags & bits.CONTROL) === 0) {
          if (start !== log.lastStart) {
            if (log.inflight > 0) {
              log.violations.push(`(b) a wave left node ${src} while ${log.inflight} data beam(s) of another were in flight`);
            }
            log.lastStart = start;
          }
          log.inflight += 1;
        }
        push(path, start, src, dst, flags);
      };
      const land = p.land.bind(p);
      p.land = (i) => {
        const log = w.causality;
        const flags = p.flags[i];
        if ((flags & bits.CONTROL) === 0) {
          log.inflight -= 1;
          if ((flags & bits.DROPPED) === 0) log.landed.add(p.dst[i]);
        }
        land(i);
      };
    }
    const sim = window.sim as unknown as { submitRequest: (n: number, k: number, h: number) => Promise<{ events: { kind: { type: string; node?: number; admission?: { kind: string } } }[] }>; causal?: boolean };
    if (sim.causal !== true) {
      sim.causal = true;
      const submit = sim.submitRequest.bind(sim);
      sim.submitRequest = async (n, k, h) => {
        const batch = await submit(n, k, h);
        for (const { kind } of batch.events) {
          if (kind.type === 'Decided' && kind.admission?.kind === 'Admitted') w.causality.credit.add(kind.node!);
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
        const byPacket = log.landed.delete(node);
        const byRequest = log.credit.delete(node);
        if (byPacket) log.rises += 1;
        else if (!byRequest) log.violations.push(`(a) node ${node} rose from ${before} to ${count} before a beam into it landed`);
      }
      log.counts = counts;
    }).observe(document.querySelector('.node-table')!, { subtree: true, childList: true, characterData: true });
    const pane = document.querySelector<HTMLElement>('.stage-pane')!;
    const readout = document.querySelector<HTMLElement>('[data-field="virtual-time"]')!;
    let last = { held: pane.dataset.held ?? '0', ms: readout.dataset.ms ?? '' };
    const sample = (): void => {
      const now = { held: pane.dataset.held ?? '0', ms: readout.dataset.ms ?? '' };
      const log = w.causality;
      // A delivered wave's counts are on the page by now (the snapshot applies
      // before `held` clears): a credit it left unused must not excuse a later rise.
      if (now.held === '0' && last.held !== '0') {
        log.landed.clear();
        log.credit.clear();
      }
      if (now.held !== '0' && now.held === last.held && now.ms !== last.ms) {
        log.violations.push(`(c) simulated time moved from ${last.ms} to ${now.ms} while wave ${now.held} was held`);
      }
      if (now.held !== '0' && now.held !== last.held) {
        log.holds += 1;
        const shown = readCounts();
        const hold = now.held;
        void window.sim!.snapshot(null, watch).then((snap) => {
          // Compared only if the wave is still held and has not landed: the
          // engine then cannot have delivered it before it answered.
          const p = (document.querySelector('.stage canvas') as Canvas).beams;
          if (pane.dataset.held !== hold || p.due === null) return;
          for (const node of snap.nodes) {
            if (shown.get(node.id) !== node.window_total) {
              log.violations.push(`(d) node ${node.id} shows ${shown.get(node.id)} while the engine holds ${node.window_total}`);
            }
          }
          log.checked += 1;
        });
      }
      last = now;
      requestAnimationFrame(sample);
    };
    requestAnimationFrame(sample);
  }, { watch: WATCH, bits: BITS });
}

/** What the causality log has found so far. */
const readCausality = (page: Page): Promise<Causality> =>
  page.evaluate(() => {
    const { violations, checked, holds, rises } = (window as unknown as { causality: Causality }).causality;
    return { violations, checked, holds, rises };
  });

/** Play at `speed` until simulated time has advanced `ms` past where it stood,
 *  then pause and land any wave the pause froze. */
async function playFor(page: Page, speed: number, ms: number, timeout: number): Promise<void> {
  const from = await virtualMs(page);
  await setSpeed(page, speed);
  await page.getByRole('button', { name: 'Play' }).click();
  await expect.poll(() => virtualMs(page), { timeout }).toBeGreaterThanOrEqual(from + ms);
  await page.getByRole('button', { name: 'Pause' }).click();
  if ((await page.locator('.stage-pane').getAttribute('data-held')) !== '0') await step(page);
}

// -- Cause before effect ------------------------------------------------------------

test('every held preset changes each count only when its packet lands', async ({ page }) => {
  test.setTimeout(300_000);
  await open(page);
  const names = (await page.getByRole('group', { name: 'Scenario preset' }).getByRole('button').allInnerTexts())
    .map((name) => name.trim())
    .filter((name) => name !== 'Sustained overload');
  for (const name of names) {
    await loadPreset(page, name);
    await installCausality(page);
    const wall = Date.now();
    await setSpeed(page, 4);
    await send(page, 0);
    await playFor(page, 4, 2000, 60_000);
    const found = await readCausality(page);
    console.log(`${name}: ${found.holds} hold(s), ${found.checked} checked, ${found.rises} rise(s), ${Date.now() - wall} ms of wall time`);
    expect(found.violations, name).toEqual([]);
    expect(found.checked, `${name}: no held wave was compared with the engine`).toBeGreaterThanOrEqual(1);
    expect(found.rises, `${name}: no count rose by a landed packet`).toBeGreaterThanOrEqual(1);
  }

  // A feed's eager flush is held too. A node flushes at once only when its
  // unsent hits pass ε, which is 1 at limit 100, so it needs two hits in one
  // heartbeat period: Gateway cut's trickle of two a second at node 0 puts two
  // in every period of a second.
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
  console.log(`${name} at limit 100: ${flushes} feed flush(es), ${found.holds} hold(s), ${found.checked} checked, ${found.rises} rise(s), ${Date.now() - wall} ms`);
  expect(flushes, `${name} at limit 100: the feed flushed no wave`).toBeGreaterThanOrEqual(1);
  expect(found.violations, `${name} at limit 100`).toEqual([]);
  expect(found.checked, `${name} at limit 100: too few holds compared`).toBeGreaterThanOrEqual(3);
  expect(found.rises, `${name} at limit 100: no count rose by a landed packet`).toBeGreaterThanOrEqual(1);
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
  // Clicked back to back, with the page's clock standing still: each Send
  // fast-forwards the one before past its relays, so it meets no emit floor.
  // At a tenth of the speed a wave flown at the speed takes `slowFlightMs` of
  // the page's time, and three of the Sends each land the wave before theirs,
  // so the four flush within two such flights only when each fast-forward ran
  // fast.
  await stopClock(page);
  await setSpeed(page, 0.1);
  const slowFlightMs = BEAM_FLIGHT_MS / 0.1;
  for (const node of [0, 1, 2, 2]) await pressSend(page, node);
  const ran = await runUntil(
    page,
    async () => (await page.evaluate(() => (window as unknown as { own: number[] }).own.length)) === 4,
    2 * slowFlightMs,
    'the four Sends flushed within two flights at a tenth of the speed',
  );
  console.log(`four back-to-back Sends flushed in ${ran} ms of the page's time`);
  await page.clock.resume();
  await still(page);
  expect(await page.evaluate(() => (window as unknown as { own: number[] }).own), 'Own packets per Send').toEqual([5, 5, 5, 5]);
  expect((await readCausality(page)).violations).toEqual([]);
});

// -- Step by event ------------------------------------------------------------------

for (const speed of [1, 0.1]) {
  test(`Step flies Propagation's waves one at a time at ${speed}×`, async ({ page }) => {
    test.setTimeout(120_000);
    await open(page);
    await loadPreset(page, 'Propagation');
    await setSpeed(page, speed);
    await page.evaluate(() => {
      const w = window as unknown as { pushed: { src: number; dst: number; flags: number }[] };
      w.pushed = [];
      const p = (document.querySelector('.stage canvas') as Canvas).beams;
      const push = p.push.bind(p);
      p.push = (path, start, src, dst, flags) => {
        w.pushed.push({ src, dst, flags });
        push(path, start, src, dst, flags);
      };
    });
    const data = async (): Promise<{ src: number; dst: number; flags: number }[]> =>
      page.evaluate((control) => {
        const w = window as unknown as { pushed: { src: number; dst: number; flags: number }[] };
        const drawn = w.pushed.filter((b) => (b.flags & control) === 0);
        w.pushed = [];
        return drawn;
      }, BITS.CONTROL);
    // Propagation's Send plays to node 0's heartbeat and stops there, its
    // first wave held for Step.
    await send(page, 0);
    await expect(page.locator('.node-table tr[data-node="0"] [data-field="view"]')).toHaveText('75');
    await expect(page.locator('.stage-pane')).toHaveAttribute('data-flight', 'waiting');
    expect(await data(), "Propagation's Send stopped before its first wave").toEqual([]);
    const gateways = ((await snapshot(page, 0)).inspected?.peers ?? [])
      .filter((p) => p.role === 'gateway')
      .map((p) => p.id);
    const zones = (await snapshot(page)).zones.length;

    let wall = Date.now();
    await step(page);
    const wave1 = await data();
    console.log(`${speed}×: Step 1 took ${Date.now() - wall} ms`);
    expect(wave1.length, 'Step 1 flew node 0 to its zone and one gateway per other zone').toBe(5);
    expect(wave1.every((b) => b.src === 0 && (b.flags & RELAY) === 0), 'Step 1: node 0, no relay').toBe(true);
    expect(wave1.filter((b) => (b.flags & CROSS) !== 0).length, 'Step 1 crossed a zone Z − 1 times').toBe(zones - 1);

    wall = Date.now();
    await step(page);
    const wave2 = await data();
    console.log(`${speed}×: Step 2 took ${Date.now() - wall} ms`);
    expect(wave2.length, 'Step 2 flew the relays').toBe(6);
    expect(wave2.every((b) => (b.flags & RELAY) !== 0 && gateways.includes(b.src)), "Step 2: relays from node 0's gateways").toBe(true);
    const agreed = await snapshot(page);
    expect(agreed.nodes.map((n) => n.window_total), 'every count is the truth').toEqual(agreed.nodes.map(() => agreed.oracle_total));
    expect(agreed.oracle_total).toBe(75);

    // The next event is the window's edge: the seed's 50 leave (its bucket is
    // older than the Send's), with no packet.
    const edge = agreed.next_expiry_ms;
    wall = Date.now();
    await step(page);
    console.log(`${speed}×: Step 3 took ${Date.now() - wall} ms`);
    expect(await data(), 'Step 3 flew no data').toEqual([]);
    const after = await snapshot(page);
    expect(after.virtual_ms, 'Step 3 stopped at the window edge').toBe(edge);
    expect(after.nodes.map((n) => n.window_total), "the seed's 50 left every node at once").toEqual(after.nodes.map(() => 25));

    await step(page);
    expect((await snapshot(page)).nodes.map((n) => n.window_total), "the Send's 25 left too").toEqual(after.nodes.map(() => 0));
  });
}

// -- The presentation clock ---------------------------------------------------------

test('flights scale with speed and Pause freezes them', async ({ page }, info) => {
  test.setTimeout(180_000);
  await openOnFakeClock(page);
  await stopClock(page);

  // A data beam's lifetime, pool entry to landing, in the page's ms, over node
  // 0's two waves played at `speed`.
  await loadPreset(page, 'Propagation');
  const lifetime = async (speed: number): Promise<number> => {
    await page.getByRole('button', { name: 'Restart Propagation' }).click();
    await runUntil(page, async () => (await page.locator('.overlay.rebuilding').count()) === 0, 10_000, 'the rebuild');
    await page.evaluate((control) => {
      const w = window as unknown as { lives: number[]; born: Map<string, number> };
      w.lives = [];
      w.born = new Map();
      const p = (document.querySelector('.stage canvas') as HTMLCanvasElement & { beams: BeamPool & { timed?: boolean } }).beams;
      if (p.timed === true) return;
      p.timed = true;
      const push = p.push.bind(p);
      p.push = (path, start, src, dst, flags) => {
        if ((flags & control) === 0) w.born.set(`${src}:${dst}:${start}`, performance.now());
        push(path, start, src, dst, flags);
      };
      const land = p.land.bind(p);
      p.land = (i) => {
        const born = w.born.get(`${p.src[i]}:${p.dst[i]}:${p.start[i]}`);
        if (born !== undefined) w.lives.push(performance.now() - born);
        land(i);
      };
    }, BITS.CONTROL);
    // The reader's Pause ends the Send's own run; Play then plays freely.
    await pressSend(page, 0);
    await page.getByRole('button', { name: 'Pause' }).click();
    await setSpeed(page, speed);
    await page.getByRole('button', { name: 'Play' }).click();
    await runUntil(
      page,
      async () => (await page.evaluate(() => (window as unknown as { lives: number[] }).lives.length)) >= 11,
      30 * BEAM_FLIGHT_MS,
      `node 0's two waves at ${speed}×`,
    );
    await page.getByRole('button', { name: 'Pause' }).click();
    const lives = await page.evaluate(() => (window as unknown as { lives: number[] }).lives.slice(0, 11));
    return lives.reduce((a, b) => a + b, 0) / lives.length;
  };
  // The speed slider moves in tenths, so a fifth of the speed is the slowest
  // round fraction it holds. A lifetime is whole frames, so it is up to a frame
  // off the flight over the speed.
  for (const speed of [1, 0.2]) {
    const life = await lifetime(speed);
    console.log(`a beam's lifetime at ${speed}×: ${life.toFixed(0)} ms`);
    expect(Math.abs(life - BEAM_FLIGHT_MS / speed), `a flight at ${speed}× lasts the flight over the speed`).toBeLessThanOrEqual(FRAME_MS);
  }
  await page.clock.resume();

  // Pause mid-flight freezes the wave: the pool, its clock and the stage's
  // pixels hold still. Captured for the visual audit.
  const freezeAt = async (relay: boolean, shot: string): Promise<void> => {
    await page.evaluate(
      ({ bits, flight, wantRelay }) => {
        const p = (document.querySelector('.stage canvas') as Canvas).beams;
        const watch = (): void => {
          for (let i = 0; i < p.len; i++) {
            const f = p.flags[i];
            if ((f & (bits.CONTROL | bits.LANDED)) !== 0 || ((f & bits.RELAY) !== 0) !== wantRelay) continue;
            if (p.clock - p.start[i] >= flight / 2) {
              [...document.querySelectorAll<HTMLButtonElement>('.transport button')].find((b) => b.textContent?.trim() === 'Pause')?.click();
              return;
            }
          }
          requestAnimationFrame(watch);
        };
        requestAnimationFrame(watch);
      },
      { bits: BITS, flight: BEAM_FLIGHT_MS, wantRelay: relay },
    );
    await page.getByRole('button', { name: 'Play' }).click();
    await expect(page.getByRole('button', { name: 'Play' })).toBeVisible({ timeout: 30_000 });
    expect(await pool(page, (c) => c.beams.inFlight), 'paused with a wave in flight').toBeGreaterThan(0);
    const before = await pool(page, (c) => [c.beams.len, c.beams.clock]);
    const stage = page.locator('.stage');
    const pixels = await stage.screenshot();
    await frames(page, 60);
    expect(await pool(page, (c) => [c.beams.len, c.beams.clock]), 'the pool and its clock held still').toEqual(before);
    expect((await stage.screenshot()).equals(pixels), 'the stage held still').toBe(true);
    await stage.screenshot({ path: `audit-shots/${info.project.name}/${shot}` });
  };
  // A packet in flight, the figure, passes over a zone title (a ring's
  // diameter runs under it) rather than vanishing under its halo.
  const inFlightAboveLabels = async (): Promise<void> => {
    const order = await paintOrder(page);
    expect(order, 'the stage has a label layer').not.toBeNull();
    expect(order!.length, 'a beam in flight is drawn').toBeGreaterThan(0);
    expect(order!.filter((d) => d <= 0), 'a beam in flight painted under the labels').toEqual([]);
  };
  await page.getByRole('button', { name: 'Restart Propagation' }).click();
  await expect(page.locator('.overlay.rebuilding')).toHaveCount(0);
  await pressSend(page, 0);
  await page.getByRole('button', { name: 'Pause' }).click();
  await setSpeed(page, 0.2);
  await freezeAt(false, 'propagation-wave1.png');
  await inFlightAboveLabels();
  await step(page);
  await freezeAt(true, 'propagation-wave2.png');
  await step(page);
});

test('the transport says how long a heartbeat takes to watch', async ({ page }) => {
  await openOnFakeClock(page);
  await stopClock(page);
  // Propagation opens settled, so its periods pass with no wave held.
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
 *  beam layer's redraw and every stage function it calls. */
const PER_FRAME = {
  'renderer.ts': [
    'land',
    'removeAt',
    '#isContext',
    '#tick',
    '#drawBeams',
    '#route',
    '#tangentAt',
    '#arrowhead',
    '#stopBar',
    'easeInOutQuad',
    '#tickRings',
    '#tickRefusal',
    '#syncNode',
    '#drawRing',
    '#drawVessel',
  ],
  'layout.ts': ['pointOn'],
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
 *  resends, and its landed routes stay; Rate limiting redraws vessels, sweeps
 *  rings and, two Sends of 50 a stretch against its limit of 100, plays
 *  refusals; Propagation sweeps the selection toward node 0's heartbeat. */
const STRETCHES: readonly { name: string; hits?: number; sends: number; warm: number }[] = [
  { name: 'Packet loss', sends: 1, warm: WARM_STRETCHES },
  // Its stretches repeat every three, admitted, refused, refused, so the
  // tracked one repeats a warm one.
  { name: 'Rate limiting', hits: 50, sends: 2, warm: 2 * WARM_STRETCHES },
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
      await page.getByRole('button', { name: 'Play' }).click();
      // Sends while playing leave time alone: the stretch's frames are the
      // play loop's.
      const stretch = async (): Promise<void> => {
        for (let i = 0; i < stretchOf.sends; i++) await sendButton(page).click();
        await page.clock.runFor(FRAMES * FRAME_MS);
      };
      for (let warm = 0; warm < stretchOf.warm; warm++) await stretch();
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
      await stretch();
      const redraws = (await pool(page, (c) => c.beams.redraws)) - redrawsFrom;
      await cdp.send('HeapProfiler.stopTrackingHeapObjects', { reportProgress: false });
      const tracked = JSON.parse(chunks.join('')) as TrackedSnapshot;
      await expect(title, 'node 0 stayed selected').toHaveText(String(ORIGIN));
      if (stretchOf.name === 'Packet loss') {
        // The relays of node 0's gateways: the beams only a selection's roles
        // single out.
        const gateways = (await snapshot(page, ORIGIN)).inspected!.peers.flatMap((p) =>
          p.role === 'gateway' && p.id !== undefined ? [p.id] : [],
        );
        const relays = await page.locator('.stage canvas').evaluate(
          (c: Canvas, [relay, ids]) => {
            let n = 0;
            for (let i = 0; i < c.beams.len; i++) if ((c.beams.flags[i] & relay) !== 0 && ids.includes(c.beams.src[i])) n += 1;
            return n;
          },
          [RELAY, gateways] as const,
        );
        expect(relays, "the stretch drew a relay from one of node 0's gateways").toBeGreaterThan(0);
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
      expect(redraws, 'the beam layer redrew while the waves flew').toBeGreaterThan(0);
      expect(count, 'the source map placed no allocation in the per-frame functions').toBeGreaterThan(0);
      expect(over, `bytes over boxed doubles in the per-frame functions over ${FRAMES} frames`).toBe(0);
    } finally {
      await browser.close();
    }
  });
}

// -- The ring's sweep ----------------------------------------------------------------

test('a ring sweeps up to its new share as its packet lands (A13)', async ({ page }) => {
  test.setTimeout(60_000);
  await open(page);
  await loadPreset(page, 'Rate limiting');
  await setSpeed(page, 1);
  await page.evaluate(() => {
    const w = window as unknown as { arcs: Map<number, number[]> };
    w.arcs = new Map();
    const c = document.querySelector('.stage canvas') as HTMLCanvasElement & { glyphs: { len: number; id: Int32Array; arc: Float32Array } };
    const sample = (): void => {
      const g = c.glyphs;
      for (let s = 0; s < g.len; s++) {
        if (g.id[s] <= 0) continue;
        const seen = w.arcs.get(g.id[s]) ?? [];
        if (seen.at(-1) !== g.arc[s]) seen.push(g.arc[s]);
        w.arcs.set(g.id[s], seen);
      }
      requestAnimationFrame(sample);
    };
    requestAnimationFrame(sample);
  });
  await send(page, 0);
  const arcs = await page.evaluate(() => [...(window as unknown as { arcs: Map<number, number[]> }).arcs]);
  expect(arcs.length, 'every receiver was sampled').toBe(11);
  for (const [id, seen] of arcs) {
    const low = seen.indexOf(Math.min(...seen));
    const rise = seen.slice(low);
    expect(rise.every((v, i) => i === 0 || v >= rise[i - 1]), `node ${id}'s ring only rises once its packet lands: ${seen}`).toBe(true);
    expect(rise.some((v) => v > rise[0] && v < rise.at(-1)!), `node ${id}'s ring sweeps through its way up: ${seen}`).toBe(true);
  }
});

// -- The refusal in motion -----------------------------------------------------------

test("a refusal's band drops, rebounds and fades in the headroom (C37)", async ({ page }) => {
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
  const frames = await refusal(page, 0, 'motion');
  expect(new Set(frames.map((f) => f.top)).size, 'the band moves').toBeGreaterThan(1);
});

// -- Landed routes -------------------------------------------------------------------

// Run at 1440 px only: the pool holds whatever the stage draws, and the design
// audit checks that every width lays out every node, so a narrower stage holds
// the same routes; ten more runs would only lengthen CI.

for (const reducedMotion of ['no-preference', 'reduce'] as const) {
  test.describe(`motion ${reducedMotion}`, () => {
    test.use({ reducedMotion });

    test('landed routes stay, quiet, until the next command', async ({ page }) => {
      test.setTimeout(120_000);
      await open(page);
      const landed = (): Promise<number> =>
        page.locator('.stage canvas').evaluate(
          (c: Canvas, bits) => {
            let n = 0;
            for (let i = 0; i < c.beams.len; i++) {
              if ((c.beams.flags[i] & (bits.LANDED | bits.CONTROL)) === bits.LANDED) n += 1;
            }
            return n;
          },
          BITS,
        );
      await loadPreset(page, 'Propagation');
      // Propagation's Send stops before each wave; Step flies it.
      await send(page, 0);
      await step(page);
      expect(await landed(), "wave 1's routes stay").toBe(5);

      // From here until wave 2 leaves, the pool never empties.
      await page.evaluate(() => {
        const w = window as unknown as { least: number; watching: boolean };
        const p = (document.querySelector('.stage canvas') as Canvas).beams;
        w.least = p.len;
        w.watching = true;
        const watch = (): void => {
          if (!w.watching) return;
          w.least = Math.min(w.least, p.len);
          requestAnimationFrame(watch);
        };
        requestAnimationFrame(watch);
        const push = p.push.bind(p);
        p.push = (...args) => {
          w.watching = false;
          push(...args);
        };
      });
      await step(page);
      expect(await page.evaluate(() => (window as unknown as { least: number }).least), 'the pool read 0 between the waves').toBeGreaterThan(0);
      // The run plays on until the acknowledgements have flown and left; the
      // routes stay.
      await still(page);
      expect(await landed(), "both waves' routes stay").toBe(11);
      expect(await pool(page, (c) => c.beams.len), 'nothing but the landed routes').toBe(11);
      // Every mark drawn is a landed route's: a stroke 1 px wide at no more than
      // `LANDED_ALPHA`, with no head (heads and dots are fills), painted under
      // the labels, whose halos knock out the ground.
      type Marks = { context: { instructions: { action: string; data: { style: { width?: number; alpha: number } } }[] } };
      const marks = await page.locator('.stage canvas').evaluate((c: HTMLCanvasElement & { beamGfx: Marks[] }) =>
        c.beamGfx.flatMap((g) => g.context.instructions.map(({ action, data }) => ({ action, width: data.style.width, alpha: data.style.alpha }))),
      );
      expect(marks.length, 'the landed routes are drawn').toBeGreaterThan(0);
      expect(marks.filter((m) => m.action !== 'stroke' || m.width !== 1 || m.alpha > LANDED_ALPHA), 'a landed route drawn other than 1 px and quiet').toEqual([]);
      const order = await paintOrder(page);
      expect(order, 'the stage has a label layer').not.toBeNull();
      expect(order!.filter((d) => d >= 0), 'a landed route painted over the labels').toEqual([]);

      const redraws = await pool(page, (c) => c.beams.redraws);
      await frames(page, 60);
      expect(await pool(page, (c) => c.beams.redraws), 'a still stage redraws nothing').toBe(redraws);

      // A command clears them.
      await sendButton(page).click();
      await expect.poll(landed).toBe(0);
    });
  });
}
