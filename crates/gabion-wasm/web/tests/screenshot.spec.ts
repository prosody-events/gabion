import { expect, test } from '@playwright/test';

const STAGE = 'svg.stage';
const COUNTS = 'svg.stage .node-count';
const SEED_TOTAL = '50';
const NODES = 12;

// One end-to-end check that doubles as the visual-quality screenshot source:
// the page boots the real gabion core in the browser, renders the ring from a
// snapshot, and — when played — gossips the seeded burst out to every node.
test('boots, renders the ring, and gossips to convergence in-browser', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await page.goto('/');
  await page.waitForSelector(STAGE, { timeout: 30_000 });
  await page.screenshot({ path: 'screenshots/ring-initial.png' });

  // Before any gossip, only the seeded node carries the burst total. SVG
  // `<text>` has no `innerText`, so read `textContent`.
  const initial = await page.locator(COUNTS).allTextContents();
  expect(initial.filter((t) => t.trim() === SEED_TOTAL)).toHaveLength(1);

  // Play; the burst should propagate until every node agrees on the total.
  await page.getByRole('button', { name: 'Play' }).click();
  await expect
    .poll(
      async () => {
        const counts = await page.locator(COUNTS).allTextContents();
        return counts.filter((t) => t.trim() === SEED_TOTAL).length;
      },
      { timeout: 15_000, message: 'cluster did not converge on the seeded total' },
    )
    .toBe(NODES);

  await page.getByRole('button', { name: 'Pause' }).click();
  await page.screenshot({ path: 'screenshots/ring-converged.png' });

  // Reset tears the engine down and rebuilds: back to a single seeded node.
  // Guards the bootstrap shutdown path (a leak otherwise piles up an engine
  // per click).
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
