// Driving the page the way a reader does, shared by the specs: one Step at a
// time, Steps by the heartbeat period, Steps until a condition holds (with
// deadlines in simulated time), a selection through the keyboard grid, a Send
// that returns once its run has ended, a refused Send's band, frame by frame,
// and the data lines the stage draws.

import { expect, type Locator, type Page } from '@playwright/test';
import { WATCHED_KEY } from '../src/lib/presets';
import type { Watch } from '../src/lib/sim/types';
import { CONTROL } from '../src/lib/stage/layout';
import type { BeamPool, GlyphTable } from '../src/lib/stage/renderer';
import timings from './phase-timings.json' with { type: 'json' };

/** What the specs' snapshots read: the watched key, deciding the default Send
 *  of 25 hits. */
export const WATCH: Watch = { key: WATCHED_KEY, probe_hits: 25 };

/** The heartbeat period every preset runs with, from the engine's timing
 *  record. */
export const PERIOD_MS = timings.heartbeat_ms;

/** The longest a command's run takes to stop, in wall ms, with room to
 *  spare. */
const LANDING_MS = 15_000;

/** The simulated-time readout's value. */
export async function virtualMs(page: Page): Promise<number> {
  return Number(await page.locator('[data-field="virtual-time"]').getAttribute('data-ms'));
}

/** The page's run state, as the stage pane publishes it. */
async function runState(page: Page): Promise<string> {
  return page.evaluate(() => {
    const pane = document.querySelector<HTMLElement>('.stage-pane');
    const ms = document.querySelector<HTMLElement>('[data-field="virtual-time"]')?.dataset.ms;
    return `${ms}|${pane?.dataset.clock}`;
  });
}

/** Wait until the page stands still: paused, stopped at a cap, or stopped
 *  before a wave Step draws, polled each animation frame. */
export async function still(page: Page, timeout = LANDING_MS): Promise<void> {
  await page.waitForFunction(() => document.querySelector<HTMLElement>('.stage-pane')?.dataset.playing === '0', undefined, { timeout });
}

/** One Step to the next event, returning once it is over: something moved
 *  (simulated time, or the clock's state) and the page stands still again,
 *  so the step's snapshot is on the page. */
export async function step(page: Page): Promise<void> {
  const before = await runState(page);
  await page.getByRole('button', { name: 'Step to the next event' }).click();
  await page.waitForFunction(
    (was) => {
      const pane = document.querySelector<HTMLElement>('.stage-pane');
      const ms = document.querySelector<HTMLElement>('[data-field="virtual-time"]')?.dataset.ms;
      return `${ms}|${pane?.dataset.clock}` !== was && pane?.dataset.playing === '0';
    },
    before,
    { timeout: LANDING_MS },
  );
}

/** Select node `node` through its grid cell, as a keyboard reader does, and
 *  wait until the detail pane names it. */
export async function select(page: Page, node: number): Promise<void> {
  const title = page.locator('.pane-title [data-field="node"]');
  if ((await title.textContent()) === String(node)) return;
  await page.locator(`[role="gridcell"][data-node="${node}"]`).press('Enter');
  await expect(title).toHaveText(String(node));
}

/** The transport's Send. */
export const sendButton = (page: Page): Locator =>
  page.locator('.transport').getByRole('button', { name: /^Send/ });

/** Pause if the page is playing, checked and clicked in one task, so a run that
 *  ends by itself cannot leave a click waiting for a Pause that is gone. */
export const pauseIfPlaying = (page: Page): Promise<void> =>
  page.evaluate(() => {
    [...document.querySelectorAll<HTMLButtonElement>('.transport button')].find((b) => b.textContent?.trim() === 'Pause')?.click();
  });

/** Send at node `node` and return once the node has decided and, if the page
 *  was paused, the run the Send started has ended: the page stands still. With
 *  `pause`, the reader pauses the run as soon as it starts, so later Steps
 *  draw its waves one at a time. */
export async function send(page: Page, node: number, pause = false): Promise<void> {
  await select(page, node);
  const decided = async (): Promise<number> =>
    page.evaluate(async (w) => {
      const s = await window.sim!.snapshot(null, w);
      return s.admitted_hits + s.rejected_hits;
    }, WATCH);
  const before = await decided();
  const playing = (await page.locator('.stage-pane').getAttribute('data-playing')) === '1';
  await sendButton(page).click();
  await expect.poll(decided, { timeout: LANDING_MS }).toBeGreaterThan(before);
  // The run may already have stopped by itself.
  if (pause && !playing) await pauseIfPlaying(page);
  if (!playing) await still(page, 4 * LANDING_MS);
}

/** The most Steps one heartbeat period may take: a period holds a handful of
 *  waves, so a deadline this far out means a Step that no longer advances. */
const PERIOD_STEPS_MAX = 200;

/** Step until simulated time has advanced by at least one heartbeat period,
 *  calling `each` after every Step. A Step that reaches the window's edge may
 *  run several periods at once. */
export async function period(page: Page, each?: () => Promise<void>): Promise<void> {
  const from = await virtualMs(page);
  for (let steps = 0; (await virtualMs(page)) - from < PERIOD_MS; steps++) {
    if (steps === PERIOD_STEPS_MAX) throw new Error(`one heartbeat period took more than ${PERIOD_STEPS_MAX} Steps`);
    await step(page);
    await each?.();
  }
}

/** Step until `done` holds, checked after every Step, at most `maxPeriods`
 *  heartbeat periods of simulated time: the cap is a deadline in simulated
 *  time, so the outcome is the same however loaded the machine, and the test's
 *  own timeout is the wall-clock backstop. Throws naming `what`. */
export async function stepUntil(
  page: Page,
  done: () => Promise<boolean>,
  maxPeriods: number,
  what: string,
): Promise<void> {
  const deadline = (await virtualMs(page)) + maxPeriods * PERIOD_MS;
  while (!(await done())) {
    if ((await virtualMs(page)) >= deadline) throw new Error(`${what}: not within ${maxPeriods} heartbeat periods`);
    await step(page);
  }
}

/** Set range input `range` to `value`: a range input can't be `fill`ed, so set
 *  it and fire `input`. */
export async function setRange(range: Locator, value: number): Promise<void> {
  await range.evaluate((el: HTMLInputElement, v: number) => {
    el.value = String(v);
    el.dispatchEvent(new Event('input', { bubbles: true }));
  }, value);
}

/** Set the playback speed slider. */
export const setSpeed = (page: Page, value: number): Promise<void> => setRange(page.getByLabel('Playback speed'), value);

/** One frame of a refusal's band, as the stage published it, with its
 *  vessel's brim and its disc's radius. */
export interface BandFrame {
  readonly top: number;
  readonly bottom: number;
  readonly brim: number;
  readonly r: number;
}

/** Press Send at the selected node `node`, which refuses it, and return every
 *  frame of the refusal's band, checking each: it never passes the brim
 *  nor reaches the count where one is shown. The recorder starts before the
 *  press, so a band however short is seen from its first frame. With
 *  `relayout`, the stage's box shrinks to half its height in the band's first
 *  frame, so the discs resize mid-bounce. */
export async function refusal(page: Page, node: number, where: string, relayout = false): Promise<BandFrame[]> {
  const recording = page.evaluate(
    (relayout) =>
      new Promise<BandFrame[]>((done) => {
        const c = document.querySelector('.stage canvas') as HTMLCanvasElement & { glyphs: GlyphTable };
        const out: BandFrame[] = [];
        let started = false;
        const tick = (): void => {
          const g = c.glyphs;
          if (g.bounce >= 0) {
            if (!started && relayout) {
              const stage = document.querySelector<HTMLElement>('.stage')!;
              stage.style.flex = 'none';
              stage.style.aspectRatio = 'auto';
              stage.style.height = `${stage.clientHeight / 2}px`;
            }
            started = true;
            out.push({ top: g.bandTop, bottom: g.bandBottom, brim: g.brimY[g.bounce], r: g.r[g.bounce] });
          } else if (started) {
            done(out);
            return;
          }
          requestAnimationFrame(tick);
        };
        tick();
      }),
    relayout,
  );
  await sendButton(page).click();
  const frames = await recording;
  expect(frames.length, `${where}: the refusal played`).toBeGreaterThan(0);
  // The count's cap height, where the node shows its count.
  // Measured in the count's own computed weight, size and family: a canvas
  // reads a rem against its own 10 px default, and keeps that default when
  // handed the computed `font` shorthand, which serializes empty.
  const cap = await page.locator(`[role="gridcell"][data-node="${node}"]`).evaluate((el) => {
    const count = el.querySelector('.count');
    if (count === null) return null;
    const ctx = new OffscreenCanvas(1, 1).getContext('2d')!;
    const style = getComputedStyle(count);
    ctx.font = `${style.fontWeight} ${style.fontSize} ${style.fontFamily}`;
    return ctx.measureText('0').actualBoundingBoxAscent;
  });
  for (const f of frames) {
    expect(f.bottom, `${where}: the band never passes the brim`).toBeLessThanOrEqual(f.brim + 1e-3);
    if (cap !== null) expect(f.bottom, `${where}: the band never reaches the count`).toBeLessThanOrEqual(-cap / 2);
  }
  return frames;
}

/** A line the stage drew: its ends and flags. */
export type Drawn = { src: number; dst: number; flags: number };

/** Record every line the stage draws from now on. The function returned takes
 *  the data lines drawn since its last call. */
export async function recordData(page: Page): Promise<() => Promise<Drawn[]>> {
  await page.evaluate(() => {
    const w = window as unknown as { pushed: Drawn[] };
    w.pushed = [];
    const p = (document.querySelector('.stage canvas') as HTMLCanvasElement & { beams: BeamPool }).beams;
    const push = p.push.bind(p);
    p.push = (path, sent, src, dst, flags) => {
      w.pushed.push({ src, dst, flags });
      push(path, sent, src, dst, flags);
    };
  });
  return () =>
    page.evaluate((control) => {
      const w = window as unknown as { pushed: Drawn[] };
      const drawn = w.pushed.filter((b) => (b.flags & control) === 0);
      w.pushed = [];
      return drawn;
    }, CONTROL);
}
