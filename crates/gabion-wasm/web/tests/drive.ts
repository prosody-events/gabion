// Driving the page the way a reader does, shared by the specs: one Step at a
// time, Steps by the heartbeat period, Steps until a condition holds (with
// deadlines in simulated time), a selection through the keyboard grid, and a
// Send that returns once its run has ended.

import { expect, type Locator, type Page } from '@playwright/test';
import { WATCHED_KEY } from '../src/lib/presets';
import type { Watch } from '../src/lib/sim/types';
import timings from './phase-timings.json' with { type: 'json' };

/** What the specs' snapshots read: the watched key, deciding the default Send
 *  of 25 hits. */
export const WATCH: Watch = { key: WATCHED_KEY, probe_hits: 25 };

/** The heartbeat period every preset runs with, from the engine's timing
 *  record. */
export const PERIOD_MS = timings.heartbeat_ms;

/** The longest a held wave takes to land, in wall ms: a Send's decision and its
 *  flight at the slowest speed, with room to spare. */
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
    return `${ms}|${pane?.dataset.held}|${pane?.dataset.flight}`;
  });
}

/** Wait until the page stands still: paused, with no wave flying (one held
 *  unflown for Step may wait), polled each animation frame. */
export async function still(page: Page, timeout = LANDING_MS): Promise<void> {
  await page.waitForFunction(
    () => {
      const pane = document.querySelector<HTMLElement>('.stage-pane');
      return pane?.dataset.playing === '0' && (pane.dataset.flight === 'none' || pane.dataset.flight === 'waiting');
    },
    undefined,
    { timeout },
  );
}

/** One Step to the next event, returning once it is over: something moved
 *  (simulated time, the held wave or its flight) and the page stands still
 *  again, so the step's snapshot is on the page. */
export async function step(page: Page): Promise<void> {
  const before = await runState(page);
  await page.getByRole('button', { name: 'Step to the next event' }).click();
  await page.waitForFunction(
    (was) => {
      const pane = document.querySelector<HTMLElement>('.stage-pane');
      const ms = document.querySelector<HTMLElement>('[data-field="virtual-time"]')?.dataset.ms;
      const now = `${ms}|${pane?.dataset.held}|${pane?.dataset.flight}`;
      return (
        now !== was &&
        pane?.dataset.playing === '0' &&
        (pane.dataset.flight === 'none' || pane.dataset.flight === 'waiting')
      );
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
  if (pause && !playing) {
    // The run may already have stopped by itself; a Pause that freezes a wave
    // mid-flight is followed by a Step that lands it.
    const pauseButton = page.getByRole('button', { name: 'Pause' });
    if (await pauseButton.isVisible()) await pauseButton.click();
    if ((await page.locator('.stage-pane').getAttribute('data-flight')) === 'frozen') await step(page);
  }
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
