import { expect, test } from '@playwright/test';

const COUNTS = '.stage-labels .node-count';
const SEED_TOTAL = '50';
const NODES = 12;

// One end-to-end check that doubles as the visual-quality screenshot source:
// the page boots the real gabion core in the browser, the PixiJS stage renders
// the ring from a snapshot, and — when played — gossips the seeded burst out to
// every node as light-beam packets. The hard assertions read the DOM label
// overlay (robust regardless of WebGL pixels); the screenshots are for eyeballing
// the canvas itself (discs, beams, convergence pulse).
test('boots, renders the ring, and gossips to convergence in-browser', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });
  await page.screenshot({ path: 'screenshots/ring-initial.png' });

  // Before any gossip, only the seeded node carries the burst total.
  const initial = await page.locator(COUNTS).allTextContents();
  expect(initial.filter((t) => t.trim() === SEED_TOTAL)).toHaveLength(1);

  // Play; the burst propagates as beams until every node agrees on the total.
  await page.getByRole('button', { name: 'Play' }).click();

  // Catch the stage mid-gossip: beams in flight, arcs partly filled. Best-effort
  // — a snapshot for eyeballing, not an assertion (timing the burst exactly is
  // racy at 12 nodes).
  await page.waitForTimeout(250);
  await page.screenshot({ path: 'screenshots/ring-gossip.png' });

  await expect
    .poll(
      async () => {
        const counts = await page.locator(COUNTS).allTextContents();
        return counts.filter((t) => t.trim() === SEED_TOTAL).length;
      },
      { timeout: 15_000, message: 'cluster did not converge on the seeded total' },
    )
    .toBe(NODES);
  // Grab the converged frame immediately, while the convergence pulse may still
  // be expanding.
  await page.screenshot({ path: 'screenshots/ring-converged.png' });

  await page.getByRole('button', { name: 'Pause' }).click();

  // Reset tears the engine down and rebuilds: back to a single seeded node.
  // Guards both the bootstrap shutdown path (a leak otherwise piles up an engine
  // per click) and the renderer's reset-on-tick-regress beam teardown.
  await page.getByRole('button', { name: 'Reset' }).click();
  await expect
    .poll(
      async () => {
        const counts = await page.locator(COUNTS).allTextContents();
        return counts.filter((t) => t.trim() === SEED_TOTAL).length;
      },
      { timeout: 10_000, message: 'Reset did not rebuild a freshly-seeded cluster' },
    )
    .toBe(1);

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});
