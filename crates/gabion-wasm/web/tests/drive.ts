// Driving the page the way a reader does, shared by the specs: one Step at a
// time, Steps by the heartbeat period, and Steps until a condition holds, with
// deadlines in simulated time.

import type { Locator, Page } from '@playwright/test';
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

/** One Step to the next event, returning once it is over: simulated time moved
 *  or the wave held before it landed, and no wave is held any more, so its
 *  snapshot is on the page. Polled each animation frame. */
export async function step(page: Page): Promise<void> {
  const before = await page.evaluate(() => [
    document.querySelector<HTMLElement>('[data-field="virtual-time"]')?.dataset.ms,
    document.querySelector<HTMLElement>('.stage-pane')?.dataset.held,
  ]);
  await page.getByRole('button', { name: 'Step to the next event' }).click();
  await page.waitForFunction(
    ([ms, held]) => {
      const now = document.querySelector<HTMLElement>('[data-field="virtual-time"]')?.dataset.ms;
      const holding = document.querySelector<HTMLElement>('.stage-pane')?.dataset.held;
      return (now !== ms || holding !== held) && holding === '0';
    },
    before,
    { timeout: LANDING_MS },
  );
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
