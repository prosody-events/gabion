import { expect, test, type Page } from '@playwright/test';

const COUNTS = '.stage-labels .node-count';
const SEED_TOTAL = '50';
const NODES = 12;
// Mirrors `burstHits` in `App.svelte`: the size of a burst the inspector's or
// the rail's Send injects (a stage click now *selects*, it does not burst).
const BURST_HITS = '25';

/** Set the playback speed slider (a range input can't be `fill`ed). */
async function setSpeed(page: Page, value: number): Promise<void> {
  await page.getByLabel('Playback speed').evaluate((el: HTMLInputElement, v: number) => {
    el.value = String(v);
    el.dispatchEvent(new Event('input', { bubbles: true }));
  }, value);
}

/** Read the transport bar's virtual-time readout, in seconds. */
async function virtualSeconds(page: Page): Promise<number> {
  const text = await page
    .locator('.readout-item', { hasText: 'virtual time' })
    .locator('.readout-value')
    .textContent();
  return Number.parseFloat((text ?? '0').replace('s', ''));
}

/** How many of the on-stage node counts are currently non-zero. */
async function nonZeroCount(page: Page): Promise<number> {
  const counts = await page.locator(COUNTS).allTextContents();
  return counts.filter((t) => Number(t.trim()) > 0).length;
}

// One end-to-end check that doubles as the visual-quality screenshot source:
// the page boots the real gabion core in the browser, the PixiJS stage renders
// the ring from a snapshot, and — when played — gossips the seeded burst out to
// every node as light-beam packets. The narrative presets now carry a faint
// uncapped background feed, so the cluster never settles on a single clean total
// and never falls silent — the burst spreads on top of a living hum, and gossip
// continues indefinitely. The hard assertions read the DOM label overlay (robust
// regardless of WebGL pixels); the screenshots are for eyeballing the canvas.
test('boots, gossips the burst out, and keeps gossiping past the window', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });
  await page.screenshot({ path: 'screenshots/ring-initial.png' });

  // Before any gossip (paused, so the background feed has not yet run): only the
  // seeded node carries the burst total — one node at 50, the rest at 0, maximal
  // disagreement. The narrative limit keeps the burst below the threshold flush,
  // so it spreads only when played.
  const initial = await page.locator(COUNTS).allTextContents();
  expect(initial.filter((t) => t.trim() === SEED_TOTAL)).toHaveLength(1);
  await expect(page.locator('.headline-value')).toHaveText(SEED_TOTAL);

  // Play at speed so the burst gossips out and virtual time crosses the window.
  await setSpeed(page, 4);
  await page.getByRole('button', { name: 'Play' }).click();

  // Catch the stage mid-gossip: beams in flight, arcs partly filled. Best-effort
  // — a snapshot for eyeballing, not an assertion.
  await page.waitForTimeout(250);
  await page.screenshot({ path: 'screenshots/ring-gossip.png' });

  // The burst (and the background hum) reach every node — no node sits at zero.
  await expect
    .poll(async () => nonZeroCount(page), {
      timeout: 15_000,
      message: 'the burst did not gossip out to every node',
    })
    .toBe(NODES);
  await page.screenshot({ path: 'screenshots/ring-converged.png' });

  // Perpetual gossip — the regression this guards. Well past the old ~11 s
  // silence point (where every store emptied and beams stopped), the windowed
  // background feed keeps live cells on every node, so the cluster never falls
  // quiet. Drive virtual time past 13 s and confirm it is still carrying traffic.
  await expect.poll(async () => virtualSeconds(page), { timeout: 15_000 }).toBeGreaterThan(13);
  expect(await nonZeroCount(page), 'the cluster fell silent past the window').toBe(NODES);

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
  // The dashboard resets with the engine: headline back to the full burst spread.
  await expect(page.locator('.headline-value')).toHaveText(SEED_TOTAL);

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// Click-a-node now *selects* it and opens the inspector (the burst gesture moved
// into the inspector and the rail). The label overlay is `pointer-events: none`,
// so clicking its center falls through to the stage's hit-test; we read geometry
// off the same overlay the canvas hides behind.
test('clicking a node selects it; the inspector sends a burst', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });

  // The page boots paused, with only node 0 seeded and the charts in the right
  // rail. Pick an empty node to inspect.
  const target = page.locator('.node-label[data-id="3"]');
  await expect(target.locator('.node-count')).toHaveText('0');

  const box = await target.boundingBox();
  if (box === null) throw new Error('node 3 label has no bounding box to click');
  await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);

  // The click selects node 3: the inspector replaces the charts and names it.
  const inspector = page.locator('.inspector');
  await expect(inspector).toBeVisible();
  await expect(inspector).toContainText('Node 3');

  // The inspector's Send burst injects at the selected node, at the current
  // (paused) virtual time — a pure inject below the threshold-AE budget, so it
  // does not spread: node 3 carries the burst, node 0 keeps its seed, others 0.
  await inspector.getByRole('button', { name: 'Send burst' }).click();
  await expect(target.locator('.node-count')).toHaveText(BURST_HITS);
  await expect(page.locator('.node-label[data-id="0"] .node-count')).toHaveText(SEED_TOTAL);
  await expect(page.locator('.node-label[data-id="5"] .node-count')).toHaveText('0');

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// Clicking the bare stage (the ring's empty hub) deselects: the inspector closes
// and the charts dashboard returns.
test('clicking the bare stage deselects and restores the charts', async ({ page }) => {
  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });

  const target = page.locator('.node-label[data-id="3"]');
  const box = await target.boundingBox();
  if (box === null) throw new Error('node 3 label has no bounding box to click');
  await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
  await expect(page.locator('.inspector')).toBeVisible();

  // The stage's centre sits inside the ring, off every disc — a click there
  // misses all nodes and deselects.
  const stage = page.locator('.stage');
  const sb = await stage.boundingBox();
  if (sb === null) throw new Error('stage has no bounding box');
  await page.mouse.click(sb.x + sb.width / 2, sb.y + sb.height / 2);
  await expect(page.locator('.inspector')).toHaveCount(0);
  await expect(page.locator('.dashboard')).toBeVisible();
});

// Removing the selected node closes its inspector — the App clears the selection
// the moment its id is no longer live, so the inspector can't show a gone node.
test('removing the selected node closes its inspector', async ({ page }) => {
  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });

  const target = page.locator('.node-label[data-id="5"]');
  const box = await target.boundingBox();
  if (box === null) throw new Error('node 5 label has no bounding box to click');
  await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
  await expect(page.locator('.inspector')).toContainText('Node 5');

  await page.locator('.node-label[data-id="5"] .node-delete').click();
  await expect(page.locator('.inspector')).toHaveCount(0);
  await expect(page.locator('.dashboard')).toBeVisible();
});

// The pinned headline metric stays above whichever the right rail shows — the
// charts dashboard or the node inspector.
test('the pinned headline stays visible in both rail modes', async ({ page }) => {
  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });

  // Charts mode: node 0 seeded at 50, rest 0, so the headline (max − min) is 50.
  await expect(page.locator('.headline-value')).toHaveText(SEED_TOTAL);
  await expect(page.locator('.dashboard')).toBeVisible();

  const target = page.locator('.node-label[data-id="3"]');
  const box = await target.boundingBox();
  if (box === null) throw new Error('node 3 label has no bounding box to click');
  await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);

  // Inspector mode: the headline is still pinned above it.
  await expect(page.locator('.inspector')).toBeVisible();
  await expect(page.locator('.headline-value')).toHaveText(SEED_TOTAL);
});

// The Sandbox preset is the user-driven complement to the continuous-feed
// presets: a blank cluster with no background traffic. It is the one scenario
// that shows the full state-driven lifecycle honestly — inject a cell by hand,
// Step it out round by round to convergence, then watch it age back to quiet
// once the window slides past it (nothing replenishes it). It also carries the
// Strata honesty invariant (Σ over the rendered slots equals the node's
// aggregate) in a deterministic, feed-free setting.
test('Sandbox: inject, Step it out to convergence, then age out to quiet', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });

  await page.getByRole('button', { name: 'Sandbox' }).click();
  // Starts quiet — no seed, no background feed, every node at zero.
  await expect(page.locator('.node-label[data-id="0"] .node-count')).toHaveText('0');
  await expect(page.locator('.node-label[data-id="6"] .node-count')).toHaveText('0');

  // Select node 0 and inject a burst (paused — a pure inject, no spread yet).
  const target = page.locator('.node-label[data-id="0"]');
  const box = await target.boundingBox();
  if (box === null) throw new Error('node 0 label has no bounding box to click');
  await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
  await page.locator('.inspector').getByRole('button', { name: 'Send burst' }).click();
  await expect(target.locator('.node-count')).toHaveText(BURST_HITS);

  // Honesty invariant: the Strata Σ equals this node's aggregate (the strip
  // shows exactly the cells the node holds).
  const strata = page.locator('.strata');
  await expect(strata).toBeVisible();
  await expect(strata.locator('.sigma-value')).toHaveText(BURST_HITS);
  // It has not spread yet — a different node is still empty.
  await expect(page.locator('.node-label[data-id="6"] .node-count')).toHaveText('0');

  // Step gossips the cell out round by round; within a handful of ticks every
  // node agrees on the injected total.
  for (let i = 0; i < 12; i++) {
    await page.getByRole('button', { name: 'Step forward one tick' }).click();
  }
  await expect
    .poll(
      async () => {
        const counts = await page.locator(COUNTS).allTextContents();
        return counts.filter((t) => t.trim() === BURST_HITS).length;
      },
      { timeout: 10_000, message: 'the cell did not gossip out to every node under Step' },
    )
    .toBe(NODES);

  // Advance past the 10 s window: with no feed to replenish it, the bucket ages
  // out everywhere, the Σ strip empties, and the cluster falls quiet.
  await setSpeed(page, 4);
  await page.getByRole('button', { name: 'Play' }).click();
  await expect(strata.locator('.no-traffic')).toBeVisible({ timeout: 20_000 });
  await page.getByRole('button', { name: 'Pause' }).click();
  await expect(target.locator('.node-count')).toHaveText('0');

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// The Strata is a fixed-width conveyor belt: the time channel is the track's
// leftward scroll, the data channel is bar height — kept separate so a window
// slide reads as motion, never as bars "growing in place". Its column count is
// fixed for the whole session and never grows, shrinks, or stretches as virtual
// time advances — the regression behind the old "bars stretch / overlap / scroll
// off screen" report (a variable slot count re-distributing `flex:1` widths).
// Default window is 10 s → liveBuckets 10 → 12 columns (the 11-bucket window plus
// the emerging bucket scrolling in under "now"). Select the seeded node, then
// drive time forward and confirm the column count is pinned throughout.
test('the Strata keeps a fixed column count as the window scrolls', async ({ page }) => {
  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });

  const target = page.locator('.node-label[data-id="0"]');
  const box = await target.boundingBox();
  if (box === null) throw new Error('node 0 label has no bounding box to click');
  await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);

  // One key (the watched key) → one strip; its track holds liveBuckets + 2 = 12
  // fixed-width columns from the first render.
  const cells = page.locator('.strata .strip .track .bar-cell');
  await expect(cells).toHaveCount(12);

  // Drive virtual time well past several bucket boundaries (the window scrolls,
  // the background feed keeps the strip populated). The column count must not
  // move — no insert, no re-distribution.
  await setSpeed(page, 4);
  await page.getByRole('button', { name: 'Play' }).click();
  await expect.poll(async () => virtualSeconds(page), { timeout: 15_000 }).toBeGreaterThan(6);
  await page.getByRole('button', { name: 'Pause' }).click();
  await expect(cells).toHaveCount(12);
});

// The control rail is the keyboard/AT-accessible equivalent of click-a-node:
// pick a live node id, press Send, and the same burst lands — no pointer
// geometry involved. The picker is a select of live stable ids (ids gap under
// churn, so a free-typed number could miss a live node). Drives the labelled
// control and the button by their accessible names.
test('the control rail sends a burst to the chosen node', async ({ page }) => {
  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });

  const target = page.locator('.node-label[data-id="7"]');
  await expect(target.locator('.node-count')).toHaveText('0');

  await page.getByLabel('Node', { exact: true }).selectOption('7');
  await page.getByRole('button', { name: 'Send burst' }).click();

  await expect(target.locator('.node-count')).toHaveText(BURST_HITS);
});

// Live join: "+ Add node" spawns a fresh cold-start member into a converged
// cluster — no rebuild. It takes the next stable id (12, never reused) and joins
// by gossip, then catches up to the settled total by anti-entropy. The
// screenshots are the proof the join *reads* right: the newcomer fades/scales in
// at its ring slot while the survivors glide to re-space around it.
test('adding a node joins it live and it catches up by gossip', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });

  // Spread the burst across the living cluster first, so the newcomer joins an
  // actively-gossiping cluster — the clearest "catch up by gossip" story.
  await setSpeed(page, 4);
  await page.getByRole('button', { name: 'Play' }).click();
  await expect
    .poll(async () => nonZeroCount(page), {
      timeout: 15_000,
      message: 'the cluster was not carrying traffic before the add',
    })
    .toBe(NODES);
  await page.getByRole('button', { name: 'Pause' }).click();
  // The dashboard before the join: the fan and disagreement curves over the
  // elapsed window. Pair with `dash-after-add` to eyeball that the join threads
  // through this same window — the time axis keeps running, no restart at x = 0.
  const dashboard = page.locator('.dashboard');
  await dashboard.screenshot({ path: 'screenshots/dash-before-add.png' });

  // Add a fresh node: it takes id 12 and joins cold (its view starts at zero).
  await page.getByRole('button', { name: 'Add node' }).click();
  const newcomer = page.locator('.node-label[data-id="12"]');
  await expect(newcomer.locator('.node-count')).toHaveText('0');
  await expect(page.locator(COUNTS)).toHaveCount(NODES + 1);
  await page.screenshot({ path: 'screenshots/ring-node-added.png' });
  // The same dashboard right after the join: the newcomer adds a fan line that
  // starts *here* (a gap before it, since it did not exist earlier in the
  // window) — and crucially the x-axis is unbroken from `dash-before-add`,
  // proving the join did not reset the charts.
  await dashboard.screenshot({ path: 'screenshots/dash-after-add.png' });

  // Play on: the newcomer catches up from zero by anti-entropy — the survivors
  // push it their cells until its view climbs to match the living cluster.
  await page.getByRole('button', { name: 'Play' }).click();
  await expect
    .poll(async () => Number((await newcomer.locator('.node-count').textContent()) ?? '0'), {
      timeout: 15_000,
      message: 'the newcomer did not catch up by gossip',
    })
    .toBeGreaterThan(0);
  await page.getByRole('button', { name: 'Pause' }).click();
  await page.screenshot({ path: 'screenshots/ring-node-added-caughtup.png' });

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// Live leave via the stage "×": removing a mid-ring node drops exactly that one.
// Its stable id leaves a *gap* — neighbours 4 and 6 keep their ids, no renumber —
// and the survivors re-space to close the ring. The "×" is a pointer-only
// affordance whose `stopPropagation` keeps the click off the burst hit-test.
test('removing a node via its stage × leaves a stable-id gap and re-spaces', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });
  await expect(page.locator(COUNTS)).toHaveCount(NODES);

  await page.locator('.node-label[data-id="5"] .node-delete').click();

  // Node 5 is gone, 12 → 11 nodes; 4 and 6 remain (a gap at 5, not a renumber).
  await expect(page.locator('.node-label[data-id="5"]')).toHaveCount(0);
  await expect(page.locator('.node-label[data-id="4"]')).toHaveCount(1);
  await expect(page.locator('.node-label[data-id="6"]')).toHaveCount(1);
  await expect(page.locator(COUNTS)).toHaveCount(NODES - 1);
  // Node 5 was empty (only node 0 seeded), so removing it loses no count — node 0
  // still holds the burst.
  await expect(page.locator('.node-label[data-id="0"] .node-count')).toHaveText(SEED_TOTAL);
  await page.screenshot({ path: 'screenshots/ring-node-removed.png' });

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// The rail's Cluster control is the keyboard/AT-accessible equivalent of the
// stage "×": pick a live id, press Remove, and that node leaves.
test('the rail Cluster control removes the chosen node', async ({ page }) => {
  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });
  await expect(page.locator(COUNTS)).toHaveCount(NODES);

  await page.getByLabel('Remove', { exact: true }).selectOption('8');
  // `exact` so this rail button isn't ambiguous with the stage "×" buttons,
  // whose accessible name is "Remove node 5", "Remove node 6", …
  await page.getByRole('button', { name: 'Remove node', exact: true }).click();

  await expect(page.locator('.node-label[data-id="8"]')).toHaveCount(0);
  await expect(page.locator(COUNTS)).toHaveCount(NODES - 1);
});

// Scenario presets rebuild the cluster from a fresh config + opening seed.
// Switching from the default Traffic burst to Steady state should tear down and
// rebuild with the steady seed (a light burst scattered across four nodes).
test('selecting a scenario preset rebuilds the cluster with its seed', async ({ page }) => {
  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });

  // Default scenario: node 0 carries the 50-hit burst, the rest start at zero.
  await expect(page.locator('.node-label[data-id="0"] .node-count')).toHaveText(SEED_TOTAL);

  await page.getByRole('button', { name: 'Steady state' }).click();

  // Steady state seeds 10 on nodes 0, 3, 6, 9 and nothing elsewhere; at t=0
  // (paused) each carries only its own seed, so the cluster maximally disagrees
  // before any gossip. node 0's old 50 is gone — this is a fresh cluster.
  await expect(page.locator('.node-label[data-id="3"] .node-count')).toHaveText('10');
  await expect(page.locator('.node-label[data-id="0"] .node-count')).toHaveText('10');
  await expect(page.locator('.node-label[data-id="1"] .node-count')).toHaveText('0');
});

// The rebuild knobs live behind a collapsed disclosure (Hick's law). They are
// build-time settings, so editing one only *stages* it: the readout updates and
// a "staged" cue appears, but nothing rebuilds and the section stays open until
// the explicit Rebuild. This guards the regression where a knob change flashed
// "Loading…", reset the running sim, and collapsed the disclosure.
test('a rebuild knob stages, keeps the section open, and applies on Rebuild', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });
  await expect(page.locator(COUNTS)).toHaveCount(NODES);

  const details = page.locator('details.tune');
  await page.getByText('Tune the cluster').click();
  await expect(details).toHaveAttribute('open', '');
  // Nothing staged yet → Rebuild is disabled.
  const rebuild = page.getByRole('button', { name: 'Rebuild cluster' });
  await expect(rebuild).toBeDisabled();

  // A range input can't be `fill`ed: set the value and fire `input` so Svelte's
  // bind reads it. With the new model there is no `change`-to-rebuild.
  await page.getByLabel('Packet loss').evaluate((el: HTMLInputElement) => {
    el.value = '0.5';
    el.dispatchEvent(new Event('input', { bubbles: true }));
  });

  // The readout updated and the knob is marked staged; the section stayed open
  // and no rebuild fired (no loading overlay, Rebuild now enabled).
  await expect(page.locator('label[for="knob-loss"] .val')).toHaveText('50%');
  await expect(page.locator('.knob.changed label[for="knob-loss"]')).toBeVisible();
  await expect(page.locator('.tune-status')).toContainText('staged');
  await expect(page.locator('.overlay', { hasText: 'Loading the gossip engine' })).toHaveCount(0);
  await expect(details).toHaveAttribute('open', '');
  await expect(rebuild).toBeEnabled();

  // Rebuild applies the staged set: the cluster rebuilds (same size, no errors),
  // the section is still open, and nothing is staged any more.
  await rebuild.click();
  await expect(page.locator(COUNTS)).toHaveCount(NODES);
  await expect(details).toHaveAttribute('open', '');
  await expect(page.locator('.tune-status')).toContainText('match');
  await expect(rebuild).toBeDisabled();
  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// The rule knobs (Slice 2) live in the same disclosure: a gossip-interval and
// window range plus a limit number input. Moving the window updates its derived
// bucket-count readout (the same `buckets.ts` math the Strata draws bars from);
// the explicit Rebuild then applies it — proof the readout and the rebuild are
// wired and the engine still accepts the new window/bucket pairing (window stays
// a whole number of 1 s buckets, so `SimConfig::validate` passes).
test('the window knob updates its bucket-count readout and rebuilds', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });
  await expect(page.locator(COUNTS)).toHaveCount(NODES);

  await page.getByText('Tune the cluster').click();
  // Default window is 10 s → 11 buckets (10 nominal + the partial oldest bucket
  // the engine retains; see `buckets.ts`).
  await expect(page.locator('label[for="knob-window"] .val')).toHaveText('10 s · 11 buckets');

  // Move the window to 5 s → 6 buckets. The readout updates immediately (the
  // staged value), before any rebuild.
  await page.getByLabel('Window').evaluate((el: HTMLInputElement) => {
    el.value = '5000';
    el.dispatchEvent(new Event('input', { bubbles: true }));
  });
  await expect(page.locator('label[for="knob-window"] .val')).toHaveText('5 s · 6 buckets');

  // Apply it. The rebuild left a healthy cluster of the same size, no errors.
  await page.getByRole('button', { name: 'Rebuild cluster' }).click();
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
  await expect(page.locator('.node-label[data-id="0"] .node-count')).toHaveText('0');

  await page.getByRole('button', { name: 'Play' }).click();
  // The watched node's view (the aggregate it converges on) climbs past the
  // limit of 400 as the steady feed accumulates cluster-wide.
  await expect
    .poll(
      async () => {
        const text = await page.locator('.node-label[data-id="0"] .node-count').textContent();
        return Number(text ?? '0');
      },
      { timeout: 15_000, message: 'aggregate did not climb past the limit under sustained load' },
    )
    .toBeGreaterThan(400);
  await page.getByRole('button', { name: 'Pause' }).click();
  await page.screenshot({ path: 'screenshots/ring-overload.png' });
  // A dashboard-only frame, large enough to eyeball that the REJECTING band
  // (the shaded regime above the dashed limit line) actually reads against the
  // white panel — the full-page shot is too small to judge a pale fill.
  await page.locator('.dashboard').screenshot({ path: 'screenshots/dash-overload.png' });

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

  // Group A (nodes 0–5) holds the 50-hit burst; group B (6–11) is severed from
  // it. Both halves carry their share of the faint background hum, but only A
  // ever sees the burst — so A sits well above B while the link is cut.
  const groupACount = page.locator('.node-label[data-id="1"] .node-count');
  const groupBCount = page.locator('.node-label[data-id="6"] .node-count');

  await page.getByRole('button', { name: 'Play' }).click();
  // Group A picks up the burst (≥ 50); the severed half never hears it, so it
  // stays well below — only its sliver of background traffic.
  await expect
    .poll(async () => Number((await groupACount.textContent()) ?? '0'), {
      timeout: 15_000,
      message: 'group A did not pick up the burst',
    })
    .toBeGreaterThanOrEqual(50);
  expect(
    Number((await groupBCount.textContent()) ?? '0'),
    'the severed half should not have the burst',
  ).toBeLessThan(50);
  await page.screenshot({ path: 'screenshots/ring-partition.png' });

  // Heal the link (still playing): the cut-off half catches up by gossip — it
  // kept its CRDT state, so this reconciles rather than cold-restarts. It now
  // holds the burst too, climbing past where it sat while severed.
  await heal.click();
  await expect
    .poll(async () => Number((await groupBCount.textContent()) ?? '0'), {
      timeout: 15_000,
      message: 'the healed half did not reconcile',
    })
    .toBeGreaterThanOrEqual(50);
  await page.getByRole('button', { name: 'Pause' }).click();
  await page.screenshot({ path: 'screenshots/ring-healed.png' });
});

// The redesigned node-detail panel surfaces the full AdminSnapshot in grouped,
// labeled sections. This asserts each section renders with live values: the
// §2 convergence headline (honestly "This node's view", not "Cluster total"),
// §4 cadence, §5 I/O & queues, §6 storage gauges, §7 peer table, §8 identity.
test('the node inspector renders every detail section', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });

  // Spread some traffic so cadence/peers/storage carry non-trivial values.
  await setSpeed(page, 3);
  await page.getByRole('button', { name: 'Play' }).click();
  await expect.poll(async () => nonZeroCount(page), { timeout: 15_000 }).toBe(NODES);
  await page.getByRole('button', { name: 'Pause' }).click();

  const target = page.locator('.node-label[data-id="0"]');
  const box = await target.boundingBox();
  if (box === null) throw new Error('node 0 label has no bounding box to click');
  await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);

  const inspector = page.locator('.inspector');
  await expect(inspector).toBeVisible();

  // §2 — the headline is this node's view, with its two references inline.
  await expect(inspector).toContainText("This node's view");
  await expect(inspector.locator('.hl-refs')).toContainText('true');
  await expect(inspector.locator('.hl-refs')).toContainText('limit');
  // A convergence badge is always present (one of the three states).
  await expect(inspector.locator('.badge[role="status"]').first()).toBeVisible();

  // §4–§7 section headings.
  await expect(inspector.locator('.section-head', { hasText: 'Gossip cadence' })).toBeVisible();
  await expect(inspector.locator('.section-head', { hasText: 'Gossip I/O' })).toBeVisible();
  await expect(inspector.locator('.section-head', { hasText: 'Storage' })).toBeVisible();
  await expect(inspector.locator('.section-head', { hasText: 'Peers' })).toBeVisible();

  // §4 — the cadence status word, live tick sparkline, the adaptive-fanout meter
  // (floor → peak with a coverage badge), and the surfaced error budget.
  await expect(inspector.locator('.cadence .status-word')).toBeVisible();
  await expect(inspector.locator('.cadence .spark')).toBeVisible();
  await expect(inspector.locator('.cadence .fanout-now')).toContainText('peers');
  await expect(inspector.locator('.cadence .fanout-foot')).toContainText('floor');
  await expect(inspector.locator('.cadence .budget')).toContainText('error budget');

  // §6 — three occupancy gauges + the §5 send-queue meter + the §4 fanout meter
  // = five meters; the calm "holding" badge at zero rejects.
  await expect(inspector.locator('[role="meter"]')).toHaveCount(5);
  await expect(inspector.locator('.holding')).toContainText('All capacities holding');

  // §7 — the peer table summary and a resolved peer row.
  await expect(inspector.locator('.peer-summary')).toContainText('tracked');
  await expect(inspector.locator('table.peers tbody tr').first()).toContainText('known');

  // §8 — the collapsed identity footer with its hex preview.
  await expect(inspector.locator('details.identity summary')).toContainText('Identity');

  // Honesty invariant (guards the §2/§3 rewrite): the headline "this node's
  // view" equals the Strata Σ for the single watched key — the strip shows
  // exactly the cells the node's aggregate counts. (Strip commas from the
  // localized hero before comparing.)
  const hero = Number(
    (await inspector.locator('.hl-value').textContent())?.replace(/,/g, '') ?? '0',
  );
  const sigma = Number(
    (await inspector.locator('.strata .sigma-value').first().textContent()) ?? '0',
  );
  expect(sigma, "Strata Σ should equal this node's aggregate view").toBe(hero);

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// Two orthogonal states on independent channels. Convergence: a fresh node lags
// the origin, then catches up (deterministic on Sandbox — no background feed).
// Admission: on the low-limit overload preset the aggregate climbs past the
// limit, recoloring the headline and showing the honest "over limit" pill
// (the visualizer counts past the cap; it never claims to have rejected).
test('the inspector shows convergence lag and the over-limit state', async ({ page }) => {
  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });

  // --- Convergence channel, on Sandbox ---
  await page.getByRole('button', { name: 'Sandbox' }).click();
  // Inject at node 0 (the origin): its view equals the oracle → caught up.
  const origin = page.locator('.node-label[data-id="0"]');
  let box = await origin.boundingBox();
  if (box === null) throw new Error('node 0 has no bounding box');
  await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
  await page.locator('.inspector').getByRole('button', { name: 'Send burst' }).click();
  await expect(page.locator('.inspector .convergence .badge')).toContainText('caught up');

  // Switch to an empty node: it has none of the origin's burst yet → lagging.
  const other = page.locator('.node-label[data-id="6"]');
  box = await other.boundingBox();
  if (box === null) throw new Error('node 6 has no bounding box');
  await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
  await expect(page.locator('.inspector .convergence .badge')).toContainText('lagging');
  // Step until it catches up by gossip.
  for (let i = 0; i < 12; i++) {
    await page.getByRole('button', { name: 'Step forward one tick' }).click();
  }
  await expect(page.locator('.inspector .convergence .badge')).toContainText('caught up');

  // --- Admission channel, on the overload preset ---
  await page.getByRole('button', { name: 'Sustained overload' }).click();
  await setSpeed(page, 4);
  await page.getByRole('button', { name: 'Play' }).click();
  await expect
    .poll(
      async () => Number((await page.locator('.node-label[data-id="0"] .node-count').textContent()) ?? '0'),
      { timeout: 15_000 },
    )
    .toBeGreaterThan(400);
  await page.getByRole('button', { name: 'Pause' }).click();
  box = await page.locator('.node-label[data-id="0"]').boundingBox();
  if (box === null) throw new Error('node 0 has no bounding box');
  await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
  // The headline recolors (the `.over` class) and the over-limit pill appears.
  await expect(page.locator('.inspector .hl-value.over')).toBeVisible();
  await expect(page.locator('.inspector .pill')).toContainText('over limit');

  // The pill sits at the panel's right edge; its tooltip must grow inward, not
  // clip against the inspector's overflow box (it is right-aligned for exactly
  // this reason).
  await page.locator('.inspector .pill .term').hover();
  await page.waitForTimeout(200);
  const bubble = page.locator('.inspector .pill .bubble');
  await expect(bubble).toBeVisible();
  const bb = await bubble.boundingBox();
  const ib = await page.locator('.inspector').boundingBox();
  if (bb === null || ib === null) throw new Error('missing tooltip/inspector box');
  expect(bb.x + bb.width, 'the tooltip clipped past the inspector right edge').toBeLessThanOrEqual(
    ib.x + ib.width + 1,
  );
});

// The adaptive-decision metrics must be *visible and responsive* — the whole
// point of §4. The coverage fanout (`config.fanout.max(⌈ln(peers)+c⌉).min(peers)`)
// sits above the configured floor and is stable for the cluster size; the
// eager-flush share was a frozen lifetime ratio. So §4 shows two things: the
// fanout meter fills past its floor for coverage, and a concentrated burst
// crosses the (now tiny) error budget, raising the windowed threshold share —
// the genuinely burst-driven knob — off zero.
test('the node inspector shows the coverage fanout and a responsive eager-flush share', async ({
  page,
}) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });
  await page.getByRole('button', { name: 'Sustained overload' }).click();
  await setSpeed(page, 4);
  await page.getByRole('button', { name: 'Play' }).click();
  await expect
    .poll(
      async () =>
        Number((await page.locator('.node-label[data-id="0"] .node-count').textContent()) ?? '0'),
      { timeout: 15_000 },
    )
    .toBeGreaterThan(400);

  const box = await page.locator('.node-label[data-id="0"]').boundingBox();
  if (box === null) throw new Error('node 0 has no bounding box');
  await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
  const inspector = page.locator('.inspector');
  await expect(inspector).toBeVisible();

  // The coverage fanout sits above the configured floor: the coverage segment
  // carries width, and the foot says so. (`⌈ln(peers)+c⌉` exceeds the floor at
  // this cluster size, independent of load.)
  await expect
    .poll(
      async () =>
        Number(
          ((await inspector.locator('.cadence .seg.fan-coverage').getAttribute('style')) ?? '')
            .replace(/[^0-9.]/g, '') || '0',
        ),
      { timeout: 10_000 },
    )
    .toBeGreaterThan(0);
  await expect(inspector.locator('.cadence .coverage-note')).toContainText('sized for coverage');

  // The eager-flush (threshold) share is windowed, so a concentrated burst on
  // this node — many hits at once across the ε=1 budget — moves it off zero.
  const send = inspector.getByRole('button', { name: /send/i }).first();
  for (let i = 0; i < 10; i++) {
    await send.click();
    await page.waitForTimeout(80);
  }
  await expect
    .poll(
      async () =>
        Number(
          ((await inspector.locator('.bar-row .bar .seg.threshold').getAttribute('style')) ?? '')
            .replace(/[^0-9.]/g, '') || '0',
        ),
      { timeout: 10_000 },
    )
    .toBeGreaterThan(0);

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// Regression for two linked bugs: in isolation/heal mode a cut-off node tracks
// peers it has never heard from, whose gossip id is None — and Option::None
// crosses the wasm boundary as `undefined`, not `null`. A strict `!== null`
// guard let `undefined` through to `shortHex`, throwing during the inspector's
// render. That surfaced as "clicking a node does nothing" (the inspector never
// mounted), and — because an uncaught render error wedges Svelte's scheduler —
// a subsequent Reset then hung forever on "Loading the gossip engine…". The fix
// is the loose `!= null` peer guards; this guards the whole path.
test('isolation/heal: nodes stay clickable and Reset still rebuilds', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });
  await page.getByRole('button', { name: 'Node isolation & heal' }).click();
  await setSpeed(page, 4);
  await page.getByRole('button', { name: 'Play' }).click();
  await page.waitForTimeout(1000);
  await page.getByRole('button', { name: 'Pause' }).click();

  // The isolated node (highest id) has only pending peers — the exact case that
  // used to crash. Clicking it must open the inspector, not silently fail.
  const isolated = page.locator(`.node-label[data-id="${NODES - 1}"]`);
  let box = await isolated.boundingBox();
  if (box === null) throw new Error('isolated node has no bounding box');
  await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
  await expect(page.locator('.inspector')).toContainText(`Node ${NODES - 1}`);
  // It really is the unresolved-peer case (pending rows present).
  await expect(page.locator('.inspector .state.pending').first()).toBeVisible();

  // Heal, then the same node is still clickable.
  await page.getByRole('button', { name: 'Heal network' }).click();
  await page.waitForTimeout(300);
  box = await isolated.boundingBox();
  if (box === null) throw new Error('isolated node has no bounding box after heal');
  await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
  await expect(page.locator('.inspector')).toContainText(`Node ${NODES - 1}`);

  // With a node selected, Reset must clear the loading overlay and rebuild the
  // cluster — not wedge on "Loading the gossip engine…".
  await page.getByRole('button', { name: 'Reset' }).click();
  await expect(page.locator('.overlay', { hasText: 'Loading the gossip engine' })).toHaveCount(0, {
    timeout: 10_000,
  });
  await expect(page.locator(COUNTS)).toHaveCount(NODES);

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});
