// Driving the page the way a reader does, shared by the audit and screenshot
// specs: one Step at a time, and a Step loop that ends on a condition rather
// than a count.

import { expect, type Page } from '@playwright/test';
import { WATCHED_KEY } from '../src/lib/presets';
import type { Watch } from '../src/lib/sim/types';

/** What the specs' snapshots read: the watched key, deciding the default Send
 *  of 25 hits. */
export const WATCH: Watch = { key: WATCHED_KEY, probe_hits: 25 };

/** One Step, returning once its snapshot is on the page: the simulated-time
 *  readout moved. */
export async function step(page: Page): Promise<void> {
  const readout = page.locator('[data-field="virtual-time"]');
  const before = (await readout.getAttribute('data-ms')) ?? '';
  await page.getByRole('button', { name: 'Step one heartbeat' }).click();
  await expect(readout).not.toHaveAttribute('data-ms', before);
}

/** Step until `done` holds, at most `maxSteps` times: the cap is a deadline in
 *  simulated time, so the outcome is the same however loaded the machine, and
 *  the test's own timeout is the wall-clock backstop. Throws naming `what`. */
export async function stepUntil(
  page: Page,
  done: () => Promise<boolean>,
  maxSteps: number,
  what: string,
): Promise<void> {
  for (let steps = 0; !(await done()); steps++) {
    if (steps === maxSteps) throw new Error(`${what}: not within ${maxSteps} Steps`);
    await step(page);
  }
}
