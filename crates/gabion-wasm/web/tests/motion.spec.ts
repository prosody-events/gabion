import { expect, test, type Page } from '@playwright/test';
import type { ClusterState } from '../src/lib/sim/types';
import { PERIOD_MS, WATCH, setSpeed, step, virtualMs } from './drive';
import { sourceOf } from './sourcemap';

// The waves in motion (README "Tests"), run once at 1440 px in the light theme
// with motion on: cause before effect in every held preset (A13), a Send's
// eager flush past the emit floor, Step by event (A14), the presentation clock
// (A15) and the landed routes (A23). Every check reads the engine
// (`window.sim`), the beam pool the stage publishes (`canvas.beams`) or the
// DOM, never the page's formatters; every wait is a condition with a deadline,
// in simulated time where one is meant, and wall times are printed, never
// asserted beyond bounds only a broken mechanism can miss.

// The flag bits the stage's beam pool packs, written out here.
const BITS = { CROSS: 1, CONTROL: 2, RELAY: 4, DROPPED: 8, LANDED: 16 } as const;
const { CROSS, RELAY } = BITS;
// The renderer's flight time on the presentation clock.
const BEAM_FLIGHT_MS = 520;
// The animation frames the allocation audit samples.
const FRAMES = 300;

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

/** Rebuild the running scenario with rule limit `limit`. */
async function rebuildAtLimit(page: Page, limit: number): Promise<void> {
  await page.getByText('Tune the cluster').click();
  await page.locator('#knob-limit').fill(String(limit));
  await page.getByRole('button', { name: 'Rebuild cluster' }).click();
  await expect(page.locator('.overlay.rebuilding')).toHaveCount(0);
  await expect(page.locator('.tune-status')).toContainText('match');
}

/** The control rail's Send at `node`. */
async function railSend(page: Page, node: number): Promise<void> {
  await page.getByLabel('Node', { exact: true }).selectOption(String(node));
  await page.locator('.rail').getByRole('button', { name: 'Send burst' }).click();
}

/** Wait until no wave is held and no beam is in flight. */
async function still(page: Page): Promise<void> {
  await expect(page.locator('.stage-pane')).toHaveAttribute('data-held', '0', { timeout: 30_000 });
  await expect.poll(() => pool(page, (c) => c.beams.inFlight), { timeout: 30_000 }).toBe(0);
}

/** What the page publishes on its beam pool, as the tests read it. */
interface Pool {
  len: number;
  clock: number;
  redraws: number;
  inFlight: number;
  due: number | null;
  start: Float64Array;
  src: Int32Array;
  dst: Int32Array;
  flags: Uint8Array;
  push: (path: unknown, start: number, src: number, dst: number, flags: number) => void;
  land: (i: number) => void;
}

/** The stage's canvas, with the beam pool it publishes. */
type Canvas = HTMLCanvasElement & { beams: Pool };

/** Evaluate `read` against the stage's canvas in the page. */
function pool<T>(page: Page, read: (c: Canvas) => T): Promise<T> {
  return page.locator('.stage canvas').evaluate(read);
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
      inflight: number;
      lastStart: number | null;
      landed: Set<number>;
      credit: Set<number>;
      counts: Map<number, number>;
    };
    type P = Pool & { causal?: boolean };
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
        if (log.landed.has(node) || log.credit.has(node)) {
          log.landed.delete(node);
          log.credit.delete(node);
        } else {
          log.violations.push(`(a) node ${node} rose from ${before} to ${count} before a beam into it landed`);
        }
      }
      log.counts = counts;
    }).observe(document.querySelector('.node-table')!, { subtree: true, childList: true, characterData: true });
    const pane = document.querySelector<HTMLElement>('.stage-pane')!;
    const readout = document.querySelector<HTMLElement>('[data-field="virtual-time"]')!;
    let last = { held: pane.dataset.held ?? '0', ms: readout.dataset.ms ?? '' };
    const sample = (): void => {
      const now = { held: pane.dataset.held ?? '0', ms: readout.dataset.ms ?? '' };
      const log = w.causality;
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
    const { violations, checked, holds } = (window as unknown as { causality: Causality }).causality;
    return { violations, checked, holds };
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

// -- A13: cause before effect ----------------------------------------------------

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
    await railSend(page, 0);
    await playFor(page, 4, 2000, 60_000);
    const found = await readCausality(page);
    console.log(`${name}: ${found.holds} hold(s), ${found.checked} checked, ${Date.now() - wall} ms of wall time`);
    expect(found.violations, name).toEqual([]);
    expect(found.checked, `${name}: no held wave was compared with the engine`).toBeGreaterThanOrEqual(1);
  }

  // Steady state at limit 100: ε is 1, so the one-hit feed flushes at once
  // whenever a node's pending hits pass it, and those submits are held too.
  await loadPreset(page, 'Steady state');
  await rebuildAtLimit(page, 100);
  await installCausality(page);
  const wall = Date.now();
  await playFor(page, 4, 10_000, 90_000);
  const found = await readCausality(page);
  console.log(`Steady state at limit 100: ${found.holds} hold(s), ${found.checked} checked, ${Date.now() - wall} ms`);
  expect(found.violations, 'Steady state at limit 100').toEqual([]);
  expect(found.checked, 'Steady state at limit 100: too few holds compared').toBeGreaterThanOrEqual(3);
});

// -- A Send's eager flush, past the emit floor ------------------------------------

test('back-to-back Sends each flush at once, past the emit floor', async ({ page }) => {
  test.setTimeout(120_000);
  await open(page);
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
  // Clicked back to back: each Send fast-forwards the one before past its
  // relays, so it meets no emit floor, whatever the click cadence.
  for (const node of [0, 1, 2, 2]) await railSend(page, node);
  await expect.poll(() => page.evaluate(() => (window as unknown as { own: number[] }).own.length)).toBe(4);
  await still(page);
  expect(await page.evaluate(() => (window as unknown as { own: number[] }).own), 'Own packets per Send').toEqual([5, 5, 5, 5]);
  expect((await readCausality(page)).violations).toEqual([]);
});

// -- A14: Step by event ------------------------------------------------------------

for (const speed of [1, 0.1]) {
  test(`Step flies Propagation's waves one at a time at ${speed}×`, async ({ page }) => {
    test.setTimeout(120_000);
    await open(page);
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
    await railSend(page, 0);
    await expect(page.locator('.node-table tr[data-node="0"] [data-field="view"]')).toHaveText('75');
    await data();
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

// -- A15: the presentation clock ----------------------------------------------------

test('flights scale with speed and Pause freezes them', async ({ page }, info) => {
  test.setTimeout(180_000);
  await open(page);

  // A data beam's lifetime, pool entry to landing, in wall ms, over node 0's
  // two waves played at `speed`.
  const lifetime = async (speed: number): Promise<number> => {
    await page.getByRole('button', { name: 'Reset' }).click();
    await expect(page.locator('.overlay.rebuilding')).toHaveCount(0);
    await page.evaluate((control) => {
      const w = window as unknown as { lives: number[]; born: Map<string, number> };
      w.lives = [];
      w.born = new Map();
      const p = (document.querySelector('.stage canvas') as HTMLCanvasElement & { beams: Pool & { timed?: boolean } }).beams;
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
    await railSend(page, 0);
    await setSpeed(page, speed);
    await page.getByRole('button', { name: 'Play' }).click();
    await expect
      .poll(() => page.evaluate(() => (window as unknown as { lives: number[] }).lives.length), { timeout: 30_000 })
      .toBeGreaterThanOrEqual(11);
    await page.getByRole('button', { name: 'Pause' }).click();
    const lives = await page.evaluate(() => (window as unknown as { lives: number[] }).lives.slice(0, 11));
    return lives.reduce((a, b) => a + b, 0) / lives.length;
  };
  const l1 = await lifetime(1);
  const l4 = await lifetime(0.25);
  console.log(`a beam's lifetime: ${l1.toFixed(0)} ms at 1×, ${l4.toFixed(0)} ms at 0.25× (ratio ${(l4 / l1).toFixed(2)}, nominally 4)`);
  expect(l4 / l1, 'a flight at a quarter speed lasts longer').toBeGreaterThan(2);

  // Pause mid-flight freezes the wave: the pool, its clock and the stage's
  // pixels hold still. Captured for the visual audit as V7.
  const freezeAt = async (relay: boolean, shot: string): Promise<void> => {
    await page.evaluate(
      ({ bits, flight, wantRelay }) => {
        const p = (document.querySelector('.stage canvas') as Canvas).beams;
        const watch = (): void => {
          for (let i = 0; i < p.len; i++) {
            const f = p.flags[i];
            if ((f & (bits.CONTROL | bits.LANDED)) !== 0 || ((f & bits.RELAY) !== 0) !== wantRelay) continue;
            if (p.clock - p.start[i] >= flight / 2) {
              document.querySelector<HTMLButtonElement>('button[aria-label="Pause"]')?.click();
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
  await page.getByRole('button', { name: 'Reset' }).click();
  await expect(page.locator('.overlay.rebuilding')).toHaveCount(0);
  await railSend(page, 0);
  await setSpeed(page, 0.25);
  await freezeAt(false, 'propagation-wave1.png');
  await step(page);
  await freezeAt(true, 'propagation-wave2.png');
  await step(page);
});

test('the transport says how long a heartbeat takes to watch', async ({ page }) => {
  await open(page);
  // Propagation opens settled, so its periods pass with no wave held.
  await setSpeed(page, 4);
  const wallMs = page.locator('[data-field="heartbeat-wall"]');
  await page.getByRole('button', { name: 'Play' }).click();
  await expect(wallMs).toHaveCount(1);
  const first = await wallMs.getAttribute('data-ms');
  await expect(wallMs).not.toHaveAttribute('data-ms', first ?? '');
  const measured = Number(await wallMs.getAttribute('data-ms'));
  await page.getByRole('button', { name: 'Pause' }).click();
  console.log(`a heartbeat took ${measured.toFixed(0)} ms to watch at 4× (nominally ${PERIOD_MS / 4} ms)`);
  // Frames quantise the reading by up to one frame either way, so the bound is
  // half the nominal: a reading divided by the speed once too often is a
  // quarter of it.
  expect(measured, 'the reading is the period over the speed').toBeGreaterThanOrEqual(PERIOD_MS / 4 / 2);
});

test('the stage allocates nothing per frame', async ({ page }) => {
  test.setTimeout(120_000);
  await open(page);
  // Packet loss: no feed, so landed routes stay; loss draws stop bars; resends
  // keep waves coming.
  await loadPreset(page, 'Packet loss');
  await railSend(page, 0);
  await page.getByRole('button', { name: 'Play' }).click();
  await frames(page, 60);
  const cdp = await page.context().newCDPSession(page);
  await cdp.send('HeapProfiler.enable');
  await cdp.send('HeapProfiler.startSampling', {
    samplingInterval: 16,
    includeObjectsCollectedByMajorGC: true,
    includeObjectsCollectedByMinorGC: true,
  });
  // V8 boxes a double that is not yet in optimised code (a HeapNumber): an
  // artefact of the engine's tiers, not an object the code made. Its size is
  // read here, from a function that does nothing else, so it can be told apart.
  await page.evaluate(() => {
    const field: { v: unknown } = { v: '' };
    function boxDoubles(): void {
      for (let i = 0; i < 2000; i++) field.v = i + 0.5;
    }
    boxDoubles();
  });
  await frames(page, FRAMES);
  const { profile } = await cdp.send('HeapProfiler.stopSampling');
  await page.getByRole('button', { name: 'Pause' }).click();

  // Each sample's top frame, mapped through the bundle's hidden source map.
  type Node = {
    id: number;
    callFrame: { url: string; lineNumber: number; columnNumber: number; functionName: string };
    children: Node[];
  };
  const byId = new Map<number, Node>();
  const index = (node: Node): void => {
    byId.set(node.id, node);
    for (const child of node.children) index(child);
  };
  index(profile.head as Node);
  const sizes = new Map<number, number>();
  for (const sample of profile.samples) {
    if (byId.get(sample.nodeId)?.callFrame.functionName !== 'boxDoubles') continue;
    sizes.set(sample.size, (sizes.get(sample.size) ?? 0) + 1);
  }
  const boxed = [...sizes].reduce((a, b) => (b[1] > a[1] ? b : a), [0, 0])[0];
  expect(boxed, "a boxed double's size was measured").toBeGreaterThan(0);
  const stage = new Map<string, number>();
  let app = 0;
  let objects = 0;
  for (const sample of profile.samples) {
    const frame = byId.get(sample.nodeId)?.callFrame;
    const file = frame?.url.match(/\/assets\/([^/?#]+\.js)$/)?.[1];
    if (frame === undefined || file === undefined) continue;
    app += 1;
    const source = sourceOf(`dist/assets/${file}.map`, frame.lineNumber, frame.columnNumber);
    if (source === null || !source.includes('src/lib/stage/') || sample.size === boxed) continue;
    objects += 1;
    const where = `${frame.functionName || '(anonymous)'} in ${source}, ${sample.size} bytes`;
    stage.set(where, (stage.get(where) ?? 0) + 1);
  }
  console.log(
    `${profile.samples.length} samples, ${app} in the app's bundle, ${objects} in src/lib/stage/ other than ` +
      `boxed doubles (${boxed} bytes): ${JSON.stringify([...stage])}`,
  );
  // What remains are the engine's one-off allocations (a function's feedback,
  // an aligned box): far fewer than one per frame. Code that builds an object
  // per frame, or per beam, makes at least one each frame.
  expect(objects, 'objects the stage allocated over the played frames').toBeLessThan(FRAMES);
});

// -- A23: landed routes ----------------------------------------------------------

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
      await railSend(page, 0);
      await step(page);
      await still(page);
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
      // The acknowledgements fly on the next Step and leave; the routes stay.
      await step(page);
      await still(page);
      expect(await landed(), "both waves' routes stay").toBe(11);
      expect(await pool(page, (c) => c.beams.len), 'nothing but the landed routes').toBe(11);

      const redraws = await pool(page, (c) => c.beams.redraws);
      await frames(page, 60);
      expect(await pool(page, (c) => c.beams.redraws), 'a still stage redraws nothing').toBe(redraws);

      // A command clears them.
      await railSend(page, 0);
      await expect.poll(landed).toBe(0);
    });
  });
}
