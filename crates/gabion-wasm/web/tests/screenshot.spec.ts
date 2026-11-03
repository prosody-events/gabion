import { expect, test, type Page } from '@playwright/test';

const SEED_TOTAL = '50';
const NODES = 12;
// Mirrors `burstHits` in `App.svelte`: the size of a burst the inspector's or
// the rail's Send injects (a stage click *selects*, it does not burst).
const BURST_HITS = '25';

// Every node is read from the page's own DOM: its grid cell (`gridcell`) for
// geometry and its "Show as table" row for numbers, because the canvas is opaque
// and the table is the in-DOM numeric source. The table stays open across
// rebuilds, so a test opens it once, right after the page loads.
const cell = (page: Page, id: number) => page.locator(`[role="gridcell"][data-node="${id}"]`);
const view = (page: Page, id: number) =>
  page.locator(`.node-table tr[data-node="${id}"] [data-field="view"]`);
const COUNTS = '.node-table [data-field="view"]';

/** Wait until the stage's layout follows its box: the table opening shrinks the
 *  stage, and the layout (and with it every grid cell) follows after a short
 *  debounce, so a click before then lands where a node used to be. The renderer
 *  publishes the centre of the layout it drew. */
async function settled(page: Page): Promise<void> {
  await expect
    .poll(async () => {
      const at = await page.locator('.stage').evaluate((stage: HTMLElement) => {
        const canvas = stage.querySelector('canvas');
        const centre = JSON.parse(canvas?.dataset.centre ?? '{"x":0,"y":0}') as { x: number; y: number };
        return { x: centre.x * 2 - stage.clientWidth, y: centre.y * 2 - stage.clientHeight };
      });
      return Math.abs(at.x) < 1 && Math.abs(at.y) < 1;
    })
    .toBe(true);
}

/** Load the page and open "Show as table". The default preset (Traffic burst)
 *  opens with node 0 selected. */
async function boot(page: Page): Promise<void> {
  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });
  await page.getByText('Show as table').click();
  await expect(page.locator(COUNTS)).toHaveCount(NODES);
  await settled(page);
}

/** Parse a printed count ("1,234"). */
const num = (text: string | null): number => Number((text ?? '0').replace(/,/g, ''));

/** Set the playback speed slider (a range input can't be `fill`ed). */
async function setSpeed(page: Page, value: number): Promise<void> {
  await page.getByLabel('Playback speed').evaluate((el: HTMLInputElement, v: number) => {
    el.value = String(v);
    el.dispatchEvent(new Event('input', { bubbles: true }));
  }, value);
}

/** The transport bar's virtual-time readout, in seconds ("5.0 s" or "200 ms"). */
async function virtualSeconds(page: Page): Promise<number> {
  const text = (await page.locator('[data-field="virtual-time"]').textContent()) ?? '0 ms';
  return text.endsWith(' ms') ? Number.parseFloat(text) / 1000 : Number.parseFloat(text);
}

/** One Step, returning once its snapshot is on the page (the tick readout moved). */
async function step(page: Page): Promise<void> {
  const tick = page.locator('[data-field="tick"]');
  const before = num(await tick.textContent());
  await page.getByRole('button', { name: 'Step forward one tick' }).click();
  await expect(tick).toHaveText(String(before + 1));
}

/** Click the middle of node `id`'s disc. The grid cell has no pointer events, so
 *  the click reaches the stage's own hit test, as a user's would. */
async function clickNode(page: Page, id: number): Promise<void> {
  const box = await cell(page, id).boundingBox();
  if (box === null) throw new Error(`node ${id} has no grid cell box to click`);
  await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
}

/** Click the middle of the stage, which no disc covers, to deselect. */
async function deselect(page: Page): Promise<void> {
  const box = await page.locator('.stage').boundingBox();
  if (box === null) throw new Error('the stage has no box');
  await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
}

/** How many of the table's node views are currently non-zero. */
async function nonZeroCount(page: Page): Promise<number> {
  const counts = await page.locator(COUNTS).allTextContents();
  return counts.filter((t) => num(t) > 0).length;
}

// One end-to-end check that doubles as the visual-quality screenshot source:
// the page boots the real gabion core in the browser, the PixiJS stage renders
// the rings from a snapshot, and — when played — gossips the seeded burst out to
// every node as beams. The narrative presets carry a faint uncapped background
// feed, so the cluster never settles on a single clean total and never falls
// silent — the burst spreads on top of a living hum, and gossip continues
// indefinitely. The hard assertions read the table (robust regardless of WebGL
// pixels); the screenshots are for eyeballing the canvas.
test('boots, gossips the burst out, and keeps gossiping past the window', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await boot(page);
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

// Click-a-node *selects* it and opens the inspector (the burst gesture lives in
// the inspector and the rail). The grid cell has no pointer events, so clicking
// its centre falls through to the stage's hit test.
test('clicking a node selects it; the inspector sends a burst', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await boot(page);

  // The page boots paused with Traffic burst, which selects its origin, node 0.
  // Pick an empty node to inspect.
  await expect(view(page, 3)).toHaveText('0');
  await clickNode(page, 3);

  // The click selects node 3: the inspector names it.
  const inspector = page.locator('.inspector');
  await expect(inspector).toBeVisible();
  await expect(inspector).toContainText('Node 3');

  // The inspector's Send burst injects at the selected node, at the current
  // (paused) virtual time — a pure inject below the threshold-AE budget, so it
  // does not spread: node 3 carries the burst, node 0 keeps its seed, others 0.
  await inspector.getByRole('button', { name: 'Send burst' }).click();
  await expect(view(page, 3)).toHaveText(BURST_HITS);
  await expect(view(page, 0)).toHaveText(SEED_TOTAL);
  await expect(view(page, 5)).toHaveText('0');

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// Clicking the bare stage (the rings' empty centre) deselects: the inspector
// closes and the charts dashboard returns.
test('clicking the bare stage deselects and restores the charts', async ({ page }) => {
  await boot(page);

  await clickNode(page, 3);
  await expect(page.locator('.inspector')).toContainText('Node 3');

  // The stage's centre sits in the hub between the rings, off every disc.
  await deselect(page);
  await expect(page.locator('.inspector')).toHaveCount(0);
  await expect(page.locator('.dashboard')).toBeVisible();
});

// Removing the selected node closes its inspector — the App clears the selection
// the moment its id is no longer live, so the inspector can't show a gone node.
test('removing the selected node closes its inspector', async ({ page }) => {
  await boot(page);

  await clickNode(page, 5);
  await expect(page.locator('.inspector')).toContainText('Node 5');

  // The rail's Remove takes the picked node, which is not the selected one by
  // default: pick node 5 there.
  await page.getByLabel('Remove', { exact: true }).selectOption('5');
  await page.getByRole('button', { name: 'Remove node', exact: true }).click();
  await expect(page.locator('.inspector')).toHaveCount(0);
  await expect(page.locator('.dashboard')).toBeVisible();
});

// The pinned headline metric stays above whichever the right rail shows — the
// charts dashboard or the node inspector.
test('the pinned headline stays visible in both rail modes', async ({ page }) => {
  await boot(page);

  // Inspector mode (Traffic burst selects node 0): node 0 seeded at 50, rest 0,
  // so the headline (max − min) is 50.
  await expect(page.locator('.inspector')).toBeVisible();
  await expect(page.locator('.headline-value')).toHaveText(SEED_TOTAL);

  // Charts mode: the headline is still pinned above the dashboard.
  await deselect(page);
  await expect(page.locator('.dashboard')).toBeVisible();
  await expect(page.locator('.headline-value')).toHaveText(SEED_TOTAL);
});

// The Sandbox preset is the user-driven complement to the continuous-feed
// presets: a blank cluster with no background traffic. It is the one scenario
// that shows the full state-driven lifecycle honestly — inject a cell by hand,
// Step it out to convergence, then watch it age back to quiet
// once the window slides past it (nothing replenishes it). It also carries the
// Strata honesty invariant (Σ over the rendered slots equals the node's
// aggregate) in a deterministic, feed-free setting.
test('Sandbox: inject, Step it out to convergence, then age out to quiet', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await boot(page);

  await page.getByRole('button', { name: 'Sandbox' }).click();
  // Starts quiet — no seed, no background feed, every node at zero.
  await expect(view(page, 0)).toHaveText('0');
  await expect(view(page, 6)).toHaveText('0');

  // Select node 0 and inject a burst (paused — a pure inject, no spread yet).
  await clickNode(page, 0);
  await page.locator('.inspector').getByRole('button', { name: 'Send burst' }).click();
  await expect(view(page, 0)).toHaveText(BURST_HITS);

  // Honesty invariant: the Strata Σ equals this node's aggregate (the strip
  // shows exactly the cells the node holds).
  const strata = page.locator('.strata');
  await expect(strata).toBeVisible();
  await expect(strata.locator('.sigma-value')).toHaveText(BURST_HITS);
  // It has not spread yet — a different node is still empty.
  await expect(view(page, 6)).toHaveText('0');

  // Step gossips the cell out: the next heartbeat sends it to the zone and a
  // gateway in each other zone, whose relays follow, so within a handful of
  // ticks every node agrees on the injected total.
  for (let i = 0; i < 12; i++) await step(page);
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
  await expect(view(page, 0)).toHaveText('0');

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
  await boot(page);

  await clickNode(page, 0);

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
  await boot(page);

  await expect(view(page, 7)).toHaveText('0');

  await page.getByLabel('Node', { exact: true }).selectOption('7');
  await page.locator('.rail').getByRole('button', { name: 'Send burst' }).click();

  await expect(view(page, 7)).toHaveText(BURST_HITS);
});

// Live join: "+ Add node" spawns a fresh cold-start member into a converged
// cluster — no rebuild. It takes the next stable id (12, never reused) in the
// zone the picker offers (the fewest live members, the lowest index on a tie:
// zone a) and joins by gossip, then catches up to the settled total by
// anti-entropy. The screenshots are the proof the join *reads* right: the
// newcomer fades/scales in at its ring slot while the survivors glide to
// re-space around it.
test('adding a node joins it live and it catches up by gossip', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await boot(page);

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
  // The dashboard before the join: the share, agreement and disagreement charts
  // over the elapsed window. Pair with `dash-after-add` to eyeball that the join
  // threads through this same window — the time axis keeps running, no restart
  // at x = 0.
  await deselect(page);
  const dashboard = page.locator('.dashboard');
  await dashboard.screenshot({ path: 'screenshots/dash-before-add.png' });

  // Add a fresh node: it takes id 12, joins zone a and starts cold (view zero).
  await expect(page.getByLabel('Add to')).toHaveValue('0');
  await page.getByRole('button', { name: 'Add node' }).click();
  await expect(view(page, 12)).toHaveText('0');
  await expect(page.locator(COUNTS)).toHaveCount(NODES + 1);
  await expect(page.locator('.node-table tr[data-node="12"]')).toHaveAttribute('data-zone', '0');
  await page.screenshot({ path: 'screenshots/ring-node-added.png' });
  // The same dashboard right after the join: the time axis is unbroken from
  // `dash-before-add`, proving the join did not reset the charts.
  await dashboard.screenshot({ path: 'screenshots/dash-after-add.png' });

  // Play on: the newcomer catches up from zero by anti-entropy — the survivors
  // push it their cells until its view climbs to match the living cluster.
  await page.getByRole('button', { name: 'Play' }).click();
  await expect
    .poll(async () => num(await view(page, 12).textContent()), {
      timeout: 15_000,
      message: 'the newcomer did not catch up by gossip',
    })
    .toBeGreaterThan(0);
  await page.getByRole('button', { name: 'Pause' }).click();
  await page.screenshot({ path: 'screenshots/ring-node-added-caughtup.png' });

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// Live leave through the inspector: removing the selected node drops exactly
// that one. Its stable id leaves a *gap* — neighbours 4 and 6 keep their ids, no
// renumber — and the survivors re-space to close the ring.
test('the inspector removes the selected node', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await boot(page);
  await expect(cell(page, 5)).toHaveCount(1);

  await clickNode(page, 5);
  const inspector = page.locator('.inspector');
  await expect(inspector).toContainText('Node 5');
  await inspector.getByRole('button', { name: 'Remove this node' }).click();

  // Node 5 is gone, 12 → 11 nodes; 4 and 6 remain (a gap at 5, not a renumber).
  await expect(cell(page, 5)).toHaveCount(0);
  await expect(cell(page, 4)).toHaveCount(1);
  await expect(cell(page, 6)).toHaveCount(1);
  await expect(page.locator('[role="gridcell"]')).toHaveCount(NODES - 1);
  await expect(page.locator('.inspector')).toHaveCount(0);
  // Node 5 was empty (only node 0 seeded), so removing it loses no count — node 0
  // still holds the burst.
  await expect(view(page, 0)).toHaveText(SEED_TOTAL);
  await page.screenshot({ path: 'screenshots/ring-node-removed.png' });

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// The rail's Cluster control is the keyboard/AT-accessible equivalent of the
// inspector's Remove: pick a live id, press Remove, and that node leaves.
test('the rail Cluster control removes the chosen node', async ({ page }) => {
  await boot(page);

  await page.getByLabel('Remove', { exact: true }).selectOption('8');
  // `exact` so this rail button isn't ambiguous with the inspector's "Remove this
  // node".
  await page.getByRole('button', { name: 'Remove node', exact: true }).click();

  await expect(cell(page, 8)).toHaveCount(0);
  await expect(page.locator(COUNTS)).toHaveCount(NODES - 1);
});

// Scenario presets rebuild the cluster from a fresh config + opening seed.
// Switching from the default Traffic burst to Steady state should tear down and
// rebuild with the steady seed (a light burst at one node in each zone).
test('selecting a scenario preset rebuilds the cluster with its seed', async ({ page }) => {
  await boot(page);

  // Default scenario: node 0 carries the 50-hit burst, the rest start at zero.
  await expect(view(page, 0)).toHaveText(SEED_TOTAL);

  await page.getByRole('button', { name: 'Steady state' }).click();

  // Steady state seeds 10 on nodes 0, 4 and 8 (one per zone) and nothing
  // elsewhere; at t=0 (paused) each carries only its own seed, so the cluster
  // maximally disagrees before any gossip. node 0's old 50 is gone — this is a
  // fresh cluster.
  await expect(view(page, 4)).toHaveText('10');
  await expect(view(page, 0)).toHaveText('10');
  await expect(view(page, 8)).toHaveText('10');
  await expect(view(page, 1)).toHaveText('0');
});

// The rebuild knobs live behind a collapsed disclosure (Hick's law). They are
// build-time settings, so editing one only *stages* it: the readout updates and
// a "staged" cue appears, but nothing rebuilds and the section stays open until
// the explicit Rebuild. This guards the regression where a knob change flashed
// "Loading…", reset the running sim, and collapsed the disclosure.
test('a rebuild knob stages, keeps the section open, and applies on Rebuild', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await boot(page);

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
  // and no rebuild fired (no overlay, Rebuild now enabled).
  await expect(page.locator('label[for="knob-loss"] .val')).toHaveText('50%');
  await expect(page.locator('.knob.changed label[for="knob-loss"]')).toBeVisible();
  await expect(page.locator('.tune-status')).toContainText('staged');
  await expect(page.locator('.overlay')).toHaveCount(0);
  await expect(details).toHaveAttribute('open', '');
  await expect(rebuild).toBeEnabled();

  // Rebuild applies the staged set: the cluster rebuilds (same size, no errors),
  // the section is still open, and nothing is staged any more.
  await rebuild.click();
  await expect(page.locator('.overlay')).toHaveCount(0);
  await expect(page.locator(COUNTS)).toHaveCount(NODES);
  await expect(details).toHaveAttribute('open', '');
  await expect(page.locator('.tune-status')).toContainText('match');
  await expect(rebuild).toBeDisabled();
  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// The rule knobs live in the same disclosure: a gossip-interval and window range
// plus a limit number input. Moving the window updates its derived bucket-count
// readout (the same `buckets.ts` math the Strata draws bars from); the explicit
// Rebuild then applies it — proof the readout and the rebuild are wired and the
// engine still accepts the new window/bucket pairing (window stays a whole
// number of 1 s buckets, so `SimConfig::validate` passes). The zone stepper
// stages the same way and stops at the engine's own ceiling.
test('the window knob updates its bucket-count readout and rebuilds', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await boot(page);

  await page.getByText('Tune the cluster').click();
  // The stepper's ceiling is the engine's `max_zones` (six), below the twelve nodes.
  await expect(page.locator('#knob-zones')).toHaveAttribute('max', '6');
  // Default window is 10 s → 11 buckets (10 nominal + the partial oldest bucket
  // the engine retains; see `buckets.ts`).
  await expect(page.locator('label[for="knob-window"] .val')).toHaveText('10 s · 11 buckets');

  // Move the window to 5 s → 6 buckets. The readout updates immediately (the
  // staged value), before any rebuild.
  await page.locator('#knob-window').evaluate((el: HTMLInputElement) => {
    el.value = '5000';
    el.dispatchEvent(new Event('input', { bubbles: true }));
  });
  await expect(page.locator('label[for="knob-window"] .val')).toHaveText('5 s · 6 buckets');

  // Apply it. The rebuild left a healthy cluster of the same size, no errors.
  await page.getByRole('button', { name: 'Rebuild cluster' }).click();
  await expect(page.locator('.overlay')).toHaveCount(0);
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

  await boot(page);

  await page.getByRole('button', { name: 'Sustained overload' }).click();
  // The overload preset has no opening burst — the feed is the story — so every
  // node starts at zero until played.
  await expect(view(page, 0)).toHaveText('0');

  await page.getByRole('button', { name: 'Play' }).click();
  // The watched node's view (the aggregate it converges on) climbs past the
  // limit of 400 as the steady feed accumulates cluster-wide.
  await expect
    .poll(async () => num(await view(page, 0).textContent()), {
      timeout: 15_000,
      message: 'aggregate did not climb past the limit under sustained load',
    })
    .toBeGreaterThan(400);
  await page.getByRole('button', { name: 'Pause' }).click();
  await page.screenshot({ path: 'screenshots/ring-overload.png' });
  // A dashboard-only frame, large enough to eyeball that the REJECTING band
  // (the shaded regime above the dashed limit line) actually reads against the
  // white panel — the full-page shot is too small to judge a pale fill.
  await page.locator('.dashboard').screenshot({ path: 'screenshots/dash-overload.png' });

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// The zone-partition scenario cuts the last zone off from the rest, bursts both
// sides, and the sides disagree until the user heals the links — the
// eventual-consistency story end to end. Driven by Step (which still feeds the
// background hum), so the outcome is exact rather than timed.
test('zone partition splits zone c off until healed', async ({ page }) => {
  await boot(page);

  await page.getByRole('button', { name: 'Zone partition' }).click();
  const heal = page.getByRole('button', { name: 'Heal network' });
  await expect(heal).toBeVisible();
  await expect(page.locator('[data-field="status-cut"]')).toHaveText('32');

  // Node 0 (zone a) burst 50 and node 2 (zone c) burst 50. While the cut holds
  // node 1 (zone b) hears only node 0's burst and node 5 (zone c) only node 2's,
  // so both sit well under the 100 the whole cluster holds.
  for (let i = 0; i < 6; i++) await step(page);
  await expect.poll(async () => num(await view(page, 1).textContent())).toBeGreaterThanOrEqual(50);
  expect(num(await view(page, 1).textContent()), 'zone b should not have zone c burst').toBeLessThan(100);
  expect(num(await view(page, 5).textContent()), 'zone c should not have zone a burst').toBeLessThan(100);
  await page.screenshot({ path: 'screenshots/ring-partition.png' });

  // Heal the links: the cut-off zone kept its CRDT state, so the sides reconcile
  // rather than cold-restart. Both now hold both bursts.
  await heal.click();
  await expect(page.locator('[data-field="status-cut"]')).toHaveText('0');
  for (let i = 0; i < 8; i++) await step(page);
  await expect
    .poll(async () => num(await view(page, 1).textContent()), { message: 'zone b did not reconcile' })
    .toBeGreaterThanOrEqual(100);
  await expect
    .poll(async () => num(await view(page, 5).textContent()), { message: 'zone c did not reconcile' })
    .toBeGreaterThanOrEqual(100);
  await page.screenshot({ path: 'screenshots/ring-healed.png' });
});

// The node-detail panel surfaces the runtime's admin snapshot in grouped,
// labeled sections. This asserts each section renders with live values: the
// convergence headline (honestly "This node's view", not "Cluster total"), the
// bucket window, the gossip targets table and the identity footer.
test('the node inspector renders every detail section', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await boot(page);

  // Spread some traffic so the targets and counters carry non-trivial values.
  await setSpeed(page, 3);
  await page.getByRole('button', { name: 'Play' }).click();
  await expect.poll(async () => nonZeroCount(page), { timeout: 15_000 }).toBe(NODES);
  await page.getByRole('button', { name: 'Pause' }).click();

  await clickNode(page, 0);
  const inspector = page.locator('.inspector');
  await expect(inspector).toBeVisible();

  // The headline is this node's view, with its two references inline.
  await expect(inspector).toContainText("This node's view");
  await expect(inspector.locator('.hl-refs')).toContainText('true');
  await expect(inspector.locator('.hl-refs')).toContainText('limit');
  // A convergence badge is always present (one of the three states).
  await expect(inspector.locator('.badge[role="status"]').first()).toBeVisible();

  // The section headings that remain, and those that were deleted with the
  // machinery they described.
  await expect(inspector.locator('.section-head', { hasText: 'Bucket window' })).toBeVisible();
  await expect(inspector.locator('.section-head', { hasText: 'Gossip targets' })).toBeVisible();
  await expect(inspector.locator('.section-head')).toHaveCount(2);

  // Gossip targets: own zone, zone peers, one row per remote zone, counters and
  // the error budget.
  await expect(inspector.locator('tr.own-zone')).toContainText('zone a');
  await expect(inspector.locator('tr.zone-peers')).toBeVisible();
  await expect(inspector.locator('tr.remote')).toHaveCount(2);
  await expect(inspector.locator('tr.counters')).toBeVisible();
  await expect(inspector.locator('tr.budget')).toBeVisible();

  // Identity: the collapsed footer with its hex preview, and the cells held.
  const identity = inspector.locator('details.identity');
  await expect(identity.locator('summary')).toContainText('Identity');
  await identity.locator('summary').click();
  await expect(identity).toContainText('Cells held');
  expect(num(await identity.locator('[data-field="cells-held"]').textContent())).toBeGreaterThan(0);

  // Honesty invariant (guards the hero/Strata rewrite): the headline "this node's
  // view" equals the Strata Σ for the single watched key — the strip shows
  // exactly the cells the node's aggregate counts.
  const hero = num(await inspector.locator('.hl-value').textContent());
  const sigma = num(await inspector.locator('.strata .sigma-value').first().textContent());
  expect(sigma, "Strata Σ should equal this node's aggregate view").toBe(hero);

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// Two orthogonal states on independent channels. Convergence: a fresh node lags
// the origin, then catches up (deterministic on Sandbox — no background feed).
// Admission: on the low-limit overload preset the aggregate climbs past the
// limit, recoloring the headline and showing the honest "over limit" pill
// (the visualizer counts past the cap; it never claims to have rejected).
test('the inspector shows convergence lag and the over-limit state', async ({ page }) => {
  await boot(page);

  // --- Convergence channel, on Sandbox ---
  await page.getByRole('button', { name: 'Sandbox' }).click();
  // Inject at node 0 (the origin): its view equals the oracle → caught up.
  await clickNode(page, 0);
  await page.locator('.inspector').getByRole('button', { name: 'Send burst' }).click();
  await expect(page.locator('.inspector .convergence .badge')).toContainText('caught up');

  // Switch to an empty node: it has none of the origin's burst yet → lagging.
  await clickNode(page, 6);
  await expect(page.locator('.inspector .convergence .badge')).toContainText('lagging');
  // Step until it catches up by gossip.
  for (let i = 0; i < 12; i++) await step(page);
  await expect(page.locator('.inspector .convergence .badge')).toContainText('caught up');

  // --- Admission channel, on the overload preset ---
  await page.getByRole('button', { name: 'Sustained overload' }).click();
  await setSpeed(page, 4);
  await page.getByRole('button', { name: 'Play' }).click();
  await expect
    .poll(async () => num(await view(page, 0).textContent()), { timeout: 15_000 })
    .toBeGreaterThan(400);
  await page.getByRole('button', { name: 'Pause' }).click();
  await clickNode(page, 0);
  // The headline recolors (the `.over` class) and the over-limit pill appears.
  await expect(page.locator('.inspector .hl-value.over')).toBeVisible();
  await expect(page.locator('.inspector .pill')).toContainText('over limit');

  // The pill sits at the panel's right edge; its tooltip must grow inward, not
  // clip against the inspector's overflow box (it is right-aligned for exactly
  // this reason).
  await page.locator('.inspector .pill .term').hover();
  const bubble = page.locator('.inspector .pill .bubble');
  await expect(bubble).toBeVisible();
  const bb = await bubble.boundingBox();
  const ib = await page.locator('.inspector').boundingBox();
  if (bb === null || ib === null) throw new Error('missing tooltip/inspector box');
  expect(bb.x + bb.width, 'the tooltip clipped past the inspector right edge').toBeLessThanOrEqual(
    ib.x + ib.width + 1,
  );
});

// The inspector tells a newcomer what the origin's runtime chose: one row per
// remote zone naming the gateway it sends that zone's copy through, and the
// error budget it holds before an eager flush. The page reads both from the
// runtime's own admin rows, so the test compares the named gateway with the
// engine's, not just its zone.
test('the inspector shows gateways per remote zone and the error budget', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await boot(page);
  const inspector = page.locator('.inspector');
  await expect(inspector).toContainText('Node 0');
  const rows = inspector.locator('tr.remote');

  // Step until both remote rows name a gateway, at most ten Steps.
  const named = async (): Promise<boolean> =>
    (await inspector.locator('tr.remote [data-field="gateway"]').count()) === 2;
  for (let i = 0; i < 10 && !(await named()); i++) await step(page);
  await expect(inspector.locator('tr.remote [data-field="gateway"]')).toHaveCount(2);

  await expect(inspector.locator('tr.own-zone')).toContainText('zone a');
  await expect(inspector.locator('tr.own-zone [data-field="members"]')).toHaveText('4');
  await expect(inspector.locator('tr.zone-peers [data-field="count"]')).toHaveText('3');
  await expect(rows).toHaveCount(2);
  await expect(rows.nth(0)).toHaveAttribute('data-zone', '1');
  await expect(rows.nth(1)).toHaveAttribute('data-zone', '2');

  // Each row names the runtime's own gateway for that zone, and that node sits
  // in the zone.
  const gateways = await page.evaluate(async () => {
    const snap = await window.sim!.snapshot(0);
    return (snap.inspected?.peers ?? [])
      .filter((p) => p.role === 'gateway')
      .map((p) => ({ zone: p.zone, id: p.id }));
  });
  for (const zone of [1, 2]) {
    const row = inspector.locator(`tr.remote[data-zone="${zone}"]`);
    const id = Number(await row.locator('[data-field="gateway"]').textContent());
    expect(gateways, `the runtime's gateway into zone ${zone}`).toContainEqual({ zone, id });
    await expect(page.locator(`.node-table tr[data-node="${id}"]`)).toHaveAttribute(
      'data-zone',
      String(zone),
    );
  }

  // The error budget the first burst set is a number above zero.
  const budget = inspector.locator('tr.budget [data-field="error-budget"]');
  await expect(budget).toHaveCount(1);
  expect(num(await budget.textContent())).toBeGreaterThan(0);

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// A gateway going silent is the failure the two-tier design exists to survive.
// The preset silences the origin's gateway into zone b without telling anyone;
// after the suspect timeout the origin names another gateway, the stage says so,
// and the zone catches up through it. Bounded Steps, no sleeps: 12 Steps of
// virtual time outlast the failover, so a green run has headroom and a red one
// cannot pass by waiting longer. A silent node's peers include ones it never
// heard from (`Option::None` crosses as `undefined`), the case that once crashed
// the inspector, so the test opens it too and then checks Reset still rebuilds.
test('gateway failure fails over and the zone catches up', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await boot(page);
  await page.getByRole('button', { name: 'Gateway failure' }).click();
  const inspector = page.locator('.inspector');
  await expect(inspector).toContainText('Node 0');
  await expect(page.locator('[data-field="status-cut"]')).toHaveText('11');

  const gateway = inspector.locator('tr.remote[data-zone="1"] [data-field="gateway"]');
  const g = Number(await gateway.textContent());

  // Step until the stage names a zone b failover and the origin has moved off g.
  const annotation = page.locator('.annotation');
  const moved = async (): Promise<boolean> =>
    ((await annotation.textContent()) ?? '').includes('zone b') &&
    Number(await gateway.textContent()) !== g;
  for (let i = 0; i < 12 && !(await moved()); i++) await step(page);
  await expect(annotation).toContainText('zone b');
  await expect(gateway).not.toHaveText(String(g));

  // Six more Steps: every zone b node but the silenced one is caught up, and the
  // silenced one is not.
  for (let i = 0; i < 6; i++) await step(page);
  const behind = (id: number) =>
    page.locator(`.node-table tr[data-node="${id}"] [data-field="behind"]`);
  for (const id of [1, 4, 7, 10].filter((id) => id !== g)) {
    await expect(behind(id), `node ${id} in zone b`).toHaveText('0');
  }
  expect(num(await behind(g).textContent()), 'the silenced gateway hears nothing').toBeGreaterThan(0);

  // The silenced node is still inspectable.
  await clickNode(page, g);
  await expect(inspector).toContainText(`Node ${g}`);

  // With a node selected, Reset must clear the rebuilding overlay and rebuild the
  // cluster with a node selected, not wedge.
  await page.getByRole('button', { name: 'Reset' }).click();
  await expect(page.locator('.overlay.rebuilding')).toHaveCount(0, { timeout: 10_000 });
  await expect(page.locator(COUNTS)).toHaveCount(NODES);
  await expect(page.locator('.inspector')).toBeVisible();

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});
