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

// Scenario presets rebuild the cluster from a fresh config + opening seed.
// Switching from the default Traffic burst to Steady state should tear down and
// rebuild with the steady seed (a light burst scattered across four nodes).
test('selecting a scenario preset rebuilds the cluster with its seed', async ({ page }) => {
  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });

  // Default scenario: node 0 carries the 50-hit burst, the rest start at zero.
  await expect(page.locator('.node-label[data-index="0"] .node-count')).toHaveText(SEED_TOTAL);

  await page.getByRole('button', { name: 'Steady state' }).click();

  // Steady state seeds 10 on nodes 0, 3, 6, 9 and nothing elsewhere; at t=0
  // (paused) each carries only its own seed, so the cluster maximally disagrees
  // before any gossip. node 0's old 50 is gone — this is a fresh cluster.
  await expect(page.locator('.node-label[data-index="3"] .node-count')).toHaveText('10');
  await expect(page.locator('.node-label[data-index="0"] .node-count')).toHaveText('10');
  await expect(page.locator('.node-label[data-index="1"] .node-count')).toHaveText('0');
});

// The rebuild knobs live behind a collapsed disclosure (Hick's law). Opening it
// and moving the packet-loss slider updates its readout and rebuilds the cluster
// with the new value — the cluster still boots (no errors) and stays at N nodes.
test('a rebuild knob updates its readout and rebuilds the cluster', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });
  await expect(page.locator(COUNTS)).toHaveCount(NODES);

  await page.getByText('Tune the cluster').click();
  // A range input can't be `fill`ed: set the value, fire `input` so Svelte's
  // bind reads it, then `change` to commit (which rebuilds with the new config).
  await page.getByLabel('Packet loss').evaluate((el: HTMLInputElement) => {
    el.value = '0.5';
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
  });

  await expect(page.locator('label[for="knob-loss"] .val')).toHaveText('50%');
  // The rebuild left a healthy cluster of the same size.
  await expect(page.locator(COUNTS)).toHaveCount(NODES);
  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// The sustained-overload scenario feeds the cluster at a steady rate against a
// low limit (400). Played, the aggregate every node converges on climbs past the
// limit into the REJECTING band — the "why gabion exists" story, and the one
// scenario that surfaces the Aggregate-vs-Limit chart.
test('sustained overload climbs the aggregate past the limit', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });

  await page.getByRole('button', { name: 'Sustained overload' }).click();
  // The overload preset has no opening burst — the feed is the story — so every
  // node starts at zero until played.
  await expect(page.locator('.node-label[data-index="0"] .node-count')).toHaveText('0');

  await page.getByRole('button', { name: 'Play' }).click();
  // The watched node's view (the aggregate it converges on) climbs past the
  // limit of 400 as the steady feed accumulates cluster-wide.
  await expect
    .poll(
      async () => {
        const text = await page.locator('.node-label[data-index="0"] .node-count').textContent();
        return Number(text ?? '0');
      },
      { timeout: 15_000, message: 'aggregate did not climb past the limit under sustained load' },
    )
    .toBeGreaterThan(400);
  await page.getByRole('button', { name: 'Pause' }).click();
  await page.screenshot({ path: 'screenshots/ring-overload.png' });

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// The network-partition scenario severs the cluster in two, bursts one half, and
// the halves disagree until the user heals the link — the eventual-consistency
// story end to end. Also confirms Heal is disclosed only for network scenarios.
test('network partition splits the cluster until healed', async ({ page }) => {
  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });

  await page.getByRole('button', { name: 'Network partition' }).click();
  const heal = page.getByRole('button', { name: 'Heal network' });
  await expect(heal).toBeVisible();

  // Group A (nodes 0–5) holds the burst; group B (6–11) is cut off from it.
  const groupA = page.locator('.node-label[data-index="1"] .node-count');
  const groupB = page.locator('.node-label[data-index="6"] .node-count');

  await page.getByRole('button', { name: 'Play' }).click();
  // Group A converges on the burst among itself; the severed half never hears it.
  await expect(groupA).toHaveText(SEED_TOTAL);
  await expect(groupB).toHaveText('0');
  await page.screenshot({ path: 'screenshots/ring-partition.png' });

  // Heal the link (still playing): the cut-off half catches up by gossip — it
  // kept its CRDT state, so this is reconciliation, not a cold restart.
  await heal.click();
  await expect(groupB).toHaveText(SEED_TOTAL, { timeout: 15_000 });
  await page.getByRole('button', { name: 'Pause' }).click();
  await page.screenshot({ path: 'screenshots/ring-healed.png' });
});
