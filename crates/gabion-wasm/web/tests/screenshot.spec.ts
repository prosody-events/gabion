import { expect, test } from '@playwright/test';

const COUNTS = '.stage-labels .node-count';
const SEED_TOTAL = '50';
const NODES = 12;
// Mirrors `CLICK_HITS` in `App.svelte`: the burst a single node click injects.
const CLICK_HITS = '25';

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

  // Before any gossip, only the seeded node carries the burst total, so the
  // cluster maximally disagrees: one node at 50, the rest at 0. (The default
  // limit is high enough that the burst stays below gabion's threshold flush,
  // so it spreads only when played — no eager pre-spread.)
  const initial = await page.locator(COUNTS).allTextContents();
  expect(initial.filter((t) => t.trim() === SEED_TOTAL)).toHaveLength(1);
  await expect(page.locator('.headline-value')).toHaveText(SEED_TOTAL);

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

  // The pinned headline reaches zero and latches the round count — the dashboard
  // tells the same convergence story the stage just animated.
  await expect(page.locator('.headline.converged .headline-value')).toHaveText('0');
  await expect(page.locator('.badge')).toHaveText(/converged in \d+ rounds?/);

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
  // The dashboard resets with the engine: headline back to the full burst
  // spread, the converged badge cleared.
  await expect(page.locator('.headline-value')).toHaveText(SEED_TOTAL);
  await expect(page.locator('.headline.converged')).toHaveCount(0);

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// Click-a-node: a pointer click on a disc injects a burst at that node, at the
// current (paused) virtual time. The label overlay is `pointer-events: none`, so
// clicking its center falls through to the stage's hit-test; we read the geometry
// off the same overlay the canvas hides behind.
test('clicking a node injects a burst at the current virtual time', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });

  // The page boots paused, with only node 0 seeded. Pick an empty node to poke.
  const target = page.locator('.node-label[data-index="3"]');
  await expect(target.locator('.node-count')).toHaveText('0');

  const box = await target.boundingBox();
  if (box === null) throw new Error('node 3 label has no bounding box to click');
  await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);

  // The clicked node now carries the burst. The click is a pure inject — it does
  // not advance time, and the burst is far below the threshold-AE budget, so it
  // does not spread: node 0 keeps its seed and the rest stay at zero.
  await expect(target.locator('.node-count')).toHaveText(CLICK_HITS);
  await expect(page.locator('.node-label[data-index="0"] .node-count')).toHaveText(SEED_TOTAL);
  await expect(page.locator('.node-label[data-index="5"] .node-count')).toHaveText('0');

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// The control rail is the keyboard/AT-accessible equivalent of click-a-node:
// type a node index, press Send, and the same burst lands — no pointer geometry
// involved. Drives the labelled inputs and the button by their accessible names.
test('the control rail sends a burst to the chosen node', async ({ page }) => {
  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });

  const target = page.locator('.node-label[data-index="7"]');
  await expect(target.locator('.node-count')).toHaveText('0');

  await page.getByLabel('Node', { exact: true }).fill('7');
  await page.getByRole('button', { name: 'Send burst' }).click();

  await expect(target.locator('.node-count')).toHaveText(CLICK_HITS);
});
