import { expect, test, type Page } from '@playwright/test';
import { CARRIES_CELLS, missingFor, type AnswerState, type ClusterState, type EventBatch } from '../src/lib/sim/types';
import { CONTROL } from '../src/lib/stage/layout';
import { WATCH, setRange, setSpeed, stepUntil, virtualMs } from './drive';

// Every node holds the default preset's settled seed at load.
const SEED_TOTAL = '50';
const NODES = 12;
// Mirrors `burstHits` in `App.svelte`: the size of the request the inspector's
// or the rail's Send submits (a stage click *selects*, it does not send).
const BURST_HITS = '25';
// A node's count after one Send on top of the seed.
const SENT_TOTAL = String(Number(SEED_TOTAL) + Number(BURST_HITS));

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

/** Load the page, open "Show as table" and play at 4×, so a held wave flies in
 *  a quarter of its time. The default preset (Propagation) opens with node 0
 *  selected. */
async function boot(page: Page): Promise<void> {
  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });
  await page.getByText('Show as table').click();
  await expect(page.locator(COUNTS)).toHaveCount(NODES);
  await settled(page);
  await setSpeed(page, 4);
}

/** Parse a printed count ("1,234"). */
const num = (text: string | null): number => Number((text ?? '0').replace(/,/g, ''));

/** The page's engine, snapshotted, with node `inspect`'s rows. */
const snapshot = (page: Page, inspect: number | null = null): Promise<ClusterState> =>
  page.evaluate(([id, watch]) => window.sim!.snapshot(id, watch), [inspect, WATCH] as const);

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

// One end-to-end check that doubles as the visual-quality screenshot source:
// the page boots the real gabion core in the browser on a settled cluster, the
// PixiJS stage renders the rings from a snapshot, and, played, a Send spreads
// to every node as beams, then leaves the window with the seed. The hard
// assertions read the table (robust regardless of WebGL pixels); the
// screenshots are for eyeballing the canvas.
test('boots settled, spreads a Send, and empties at the window edge', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await boot(page);
  await page.screenshot({ path: 'screenshots/ring-initial.png' });

  // Propagation settles its seed before the first paint: every node holds it.
  await expect(page.locator(COUNTS)).toHaveText(Array(NODES).fill(SEED_TOTAL));
  await expect(page.locator('.headline-value')).toHaveText('0');

  // A Send at node 0, then Play.
  await page.getByLabel('Node', { exact: true }).selectOption('0');
  await page.locator('.rail').getByRole('button', { name: 'Send burst' }).click();
  await expect(view(page, 0)).toHaveText(SENT_TOTAL);
  await page.getByRole('button', { name: 'Play' }).click();

  // Catch the stage mid-gossip once beams are in flight. Best-effort — a
  // snapshot for eyeballing, not an assertion.
  await expect
    .poll(() =>
      page.locator('.stage canvas').evaluate((c: HTMLCanvasElement & { beams?: { inFlight: number } }) => c.beams?.inFlight ?? 0),
    )
    .toBeGreaterThan(0);
  await page.screenshot({ path: 'screenshots/ring-gossip.png' });

  // The Send reaches every node.
  await expect(page.locator(COUNTS), 'the Send did not reach every node').toHaveText(Array(NODES).fill(SENT_TOTAL), {
    timeout: 15_000,
  });
  await page.screenshot({ path: 'screenshots/ring-converged.png' });

  // With no feed, the seed and the Send leave the window, every node at once,
  // and nothing flies.
  await expect(page.locator(COUNTS), 'the hits did not leave the window').toHaveText(Array(NODES).fill('0'), {
    timeout: 30_000,
  });
  await expect
    .poll(() => page.locator('.stage canvas').evaluate((c: HTMLCanvasElement & { beams?: { inFlight: number } }) => c.beams?.inFlight))
    .toBe(0);
  await page.getByRole('button', { name: 'Pause' }).click();

  // Reset tears the engine down and rebuilds: back to the settled seed. Guards
  // both the bootstrap shutdown path (a leak otherwise piles up an engine per
  // click) and the stage's reset of its beams.
  await page.getByRole('button', { name: 'Reset' }).click();
  await expect(page.locator(COUNTS), 'Reset did not rebuild a freshly seeded cluster').toHaveText(
    Array(NODES).fill(SEED_TOTAL),
    { timeout: 10_000 },
  );
  await expect(page.locator('.headline-value')).toHaveText('0');

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// Click-a-node *selects* it and opens the inspector (the burst gesture lives in
// the inspector and the rail). The grid cell has no pointer events, so clicking
// its centre falls through to the stage's hit test.
test('clicking a node selects it; the inspector sends a burst', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await boot(page);

  // The page boots paused with Propagation, which selects node 0. Pick another
  // node to inspect.
  await expect(view(page, 3)).toHaveText(SEED_TOTAL);
  await clickNode(page, 3);

  // The click selects node 3: the inspector names it.
  const inspector = page.locator('.inspector');
  await expect(inspector).toBeVisible();
  await expect(inspector).toContainText('Node 3');

  // The inspector's Send submits at the selected node, at the current (paused)
  // virtual time — below the threshold-AE budget, so it waits for node 3's
  // heartbeat: node 3 counts the Send, the rest keep the seed.
  await inspector.getByRole('button', { name: 'Send burst' }).click();
  await expect(view(page, 3)).toHaveText(SENT_TOTAL);
  await expect(view(page, 0)).toHaveText(SEED_TOTAL);
  await expect(view(page, 5)).toHaveText(SEED_TOTAL);

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

  // Inspector mode (Propagation selects node 0): every node holds the settled
  // seed, so the headline (max − min) is 0.
  await expect(page.locator('.inspector')).toBeVisible();
  await expect(page.locator('.headline-value')).toHaveText('0');

  // Charts mode: the headline is still pinned above the dashboard.
  await deselect(page);
  await expect(page.locator('.dashboard')).toBeVisible();
  await expect(page.locator('.headline-value')).toHaveText('0');
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

  // Select node 0 and send it a request (paused: it waits for node 0's
  // heartbeat).
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

  // Step gossips the cell out: node 0's next heartbeat sends it to the zone and
  // a gateway in each other zone, whose relays follow, so within a few
  // heartbeat periods every node agrees on the total.
  await stepUntil(
    page,
    async () => {
      const texts = await page.locator(COUNTS).allTextContents();
      return texts.length === NODES && texts.every((t) => t.trim() === BURST_HITS);
    },
    12,
    'the cell gossiped out to every node under Step',
  );

  // Advance past the 10 s window: with no feed to replenish it, the bucket ages
  // out everywhere, the Σ strip empties, and the cluster falls quiet.
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
// Default window is 10 s → liveBuckets 10 → 11 columns (the 10-bucket window
// admission counts plus the emerging bucket scrolling in under "now"). Select the seeded node, then
// drive time forward and confirm the column count is pinned throughout.
test('the Strata keeps a fixed column count as the window scrolls', async ({ page }) => {
  await boot(page);

  await clickNode(page, 0);

  // One key (the watched key) → one strip; its track holds liveBuckets + 1 = 11
  // fixed-width columns from the first render.
  const cells = page.locator('.strata .strip .track .bar-cell');
  await expect(cells).toHaveCount(11);

  // Drive virtual time past several bucket boundaries (the window scrolls).
  // The column count must not move — no insert, no re-distribution.
  const from = await virtualMs(page);
  await page.getByRole('button', { name: 'Play' }).click();
  await expect.poll(async () => virtualMs(page), { timeout: 15_000 }).toBeGreaterThan(from + 4000);
  await page.getByRole('button', { name: 'Pause' }).click();
  await expect(cells).toHaveCount(11);
});

// The control rail is the keyboard/AT-accessible equivalent of click-a-node:
// pick a live node id, press Send, and the same request lands — no pointer
// geometry involved. The picker is a select of live stable ids (ids gap under
// churn, so a free-typed number could miss a live node). Drives the labelled
// control and the button by their accessible names.
test('the control rail sends a burst to the chosen node', async ({ page }) => {
  await boot(page);

  await expect(view(page, 7)).toHaveText(SEED_TOTAL);

  await page.getByLabel('Node', { exact: true }).selectOption('7');
  await page.locator('.rail').getByRole('button', { name: 'Send burst' }).click();

  await expect(view(page, 7)).toHaveText(SENT_TOTAL);
});

// Live join: "+ Add node" spawns a fresh cold-start member into a settled
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

  // Step on: the newcomer catches up from zero by anti-entropy, the survivors'
  // join sweep bringing it the seed.
  await stepUntil(page, async () => (await view(page, 12).textContent()) === SEED_TOTAL, 6, 'the newcomer caught up by gossip');
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
  // Node 5 held the settled seed, like every node; removing it loses no count —
  // node 0 still holds the seed.
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
// Switching from the default Propagation to Steady state should tear down and
// rebuild with the steady seed (a light request at one node in each zone).
test('selecting a scenario preset rebuilds the cluster with its seed', async ({ page }) => {
  await boot(page);

  // Default scenario: every node holds the settled 50-hit seed.
  await expect(view(page, 0)).toHaveText(SEED_TOTAL);

  await page.getByRole('button', { name: 'Steady state' }).click();

  // Steady state seeds 10 on nodes 0, 4 and 8 (one per zone) and nothing
  // elsewhere; at load (paused) each carries only its own seed, so the cluster
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

  await setRange(page.getByLabel('Packet loss'), 0.5);

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
  // Default window is 10 s → 10 buckets, the ones admission counts (see
  // `buckets.ts`).
  await expect(page.locator('label[for="knob-window"] .val')).toHaveText('10 s · 10 buckets');

  // Move the window to 5 s → 5 buckets. The readout updates immediately (the
  // staged value), before any rebuild.
  await setRange(page.locator('#knob-window'), 5000);
  await expect(page.locator('label[for="knob-window"] .val')).toHaveText('5 s · 5 buckets');

  // Apply it. The rebuild left a healthy cluster of the same size, no errors,
  // and the Strata draws the rebuilt window: 5 buckets plus the one scrolling
  // in under "now".
  await page.getByRole('button', { name: 'Rebuild cluster' }).click();
  await expect(page.locator('.overlay')).toHaveCount(0);
  await expect(page.locator(COUNTS)).toHaveCount(NODES);
  await expect(page.locator('.strata .strip .track .bar-cell')).toHaveCount(6);
  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// The sustained-overload scenario feeds the cluster at a steady rate against a
// low limit (400). Stepped, the nodes admit hits until their window holds the
// limit and reject the rest — the "why gabion exists" story, and the one
// scenario that surfaces the Aggregate-vs-Limit chart.
test('sustained overload rejects what passes the limit', async ({ page }) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (err) => pageErrors.push(err.message));

  await boot(page);

  await page.getByRole('button', { name: 'Sustained overload' }).click();
  // The overload preset has no opening request — the feed is the story — so
  // every node starts at zero until played.
  await expect(view(page, 0)).toHaveText('0');

  // The REJECTING band fills the plot from its top down to the limit line and no
  // further. Read from the drawn pixels of three rows across the plot's right
  // half, 5 px clear of the top and of the dashed line, where no label (both sit
  // at the left) and, before Play, no series reaches; the vertical grid lines
  // cross all three rows alike.
  const plot = page.locator('section.block', { hasText: 'Aggregate vs. limit' }).locator('.plot');
  await expect
    .poll(() =>
      plot.evaluate((host: HTMLElement & { uplot?: any }, limit) => {
        const u = host.uplot;
        if (u === undefined) return null;
        const gap = 5 * (u.pxRatio ?? devicePixelRatio);
        const [x, w] = [Math.round(u.bbox.left + u.bbox.width / 2), Math.floor(u.bbox.width / 2)];
        const line = u.valToPos(limit, 'y', true);
        const at = (y: number): string => u.ctx.getImageData(x, Math.round(y), w, 1).data.join();
        const [top, above, below] = [at(u.bbox.top + gap), at(line - gap), at(line + gap)];
        return { 'one fill from the plot top to the limit': above === top, 'ends at the limit line': below !== above };
      }, 400),
    )
    .toEqual({ 'one fill from the plot top to the limit': true, 'ends at the limit line': true });

  // The feed offers 40 hits a Step, so the window fills to the limit of 400
  // within a dozen Steps and the next requests are rejected; a node rejects
  // only once its own window holds the limit, so the cluster has admitted at
  // least that many by then.
  await stepUntil(page, async () => (await snapshot(page)).rejected_hits > 0, 40, 'a node rejected a request');
  expect((await snapshot(page)).admitted_hits, 'the window filled before any rejection').toBeGreaterThanOrEqual(400);
  await page.screenshot({ path: 'screenshots/ring-overload.png' });
  // A dashboard-only frame, large enough to eyeball that the REJECTING band
  // (the shaded regime above the dashed limit line) actually reads against the
  // white panel — the full-page shot is too small to judge a pale fill.
  await page.locator('.dashboard').screenshot({ path: 'screenshots/dash-overload.png' });

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// On a display with 2 device px per CSS px every chart label is still drawn at
// its CSS size: each published box spans its text, set in the page's font at
// the box's size in device px and measured back in CSS px, plus a 2 px halo each
// side. (Measured at the device size, as drawn, because a system font's optical
// sizes set 2 × 10 px text narrower than twice 10 px text.) Every other project
// draws at 1 device px, where a label sized in device px looks the same.
test.describe('on a 2× display', () => {
  test.use({ deviceScaleFactor: 2 });

  test('chart labels keep their CSS size', async ({ page }) => {
    await boot(page);
    await page.getByRole('button', { name: 'Sustained overload' }).click();
    const canvas = page.locator('section.block', { hasText: 'Aggregate vs. limit' }).locator('canvas[data-label-boxes]');
    await expect
      .poll(() =>
        canvas.evaluate((c: HTMLCanvasElement) => {
          const ctx = document.createElement('canvas').getContext('2d')!;
          const family = getComputedStyle(document.documentElement).fontFamily;
          const dpr = window.devicePixelRatio;
          const labels = JSON.parse(c.dataset.labelBoxes ?? '[]') as { text: string; w: number; size: number }[];
          return {
            dpr,
            labels: labels.map((l) => {
              ctx.font = `${l.size * dpr}px ${family}`;
              const text = ctx.measureText(l.text).width / dpr;
              return { text: l.text, spansItsText: Math.abs(l.w - text - 4) < 0.5 };
            }),
          };
        }),
      )
      .toEqual({
        dpr: 2,
        labels: [
          { text: 'REJECTING', spansItsText: true },
          { text: 'limit', spansItsText: true },
        ],
      });
  });
});

// Against the overload preset's low limit a Send crosses its node's error
// budget, so it flushes at once: its packets leave in the Send's own batch,
// and the stage draws each as a beam as it is sent, not a Step later. Paused,
// so no feed or Step adds a packet of its own. Played on, every data packet of
// the feed's submits and of the steps is drawn, one beam each.
test('a Send that flushes at once draws its packets as it sends', async ({ page }) => {
  await boot(page);
  await page.getByRole('button', { name: 'Sustained overload' }).click();
  await expect(view(page, 0)).toHaveText('0');

  type Route = { src: number; dst: number; data: boolean };
  type Log = { sent: Route[]; drawn: Route[] };
  await page.evaluate(({ carries, control }) => {
    const log = window as unknown as Log;
    log.sent = [];
    log.drawn = [];
    const sim = window.sim!;
    const record = (batch: EventBatch): EventBatch => {
      for (const { kind } of batch.events) {
        if (kind.type === 'PacketSent' || kind.type === 'PacketDropped') {
          log.sent.push({ src: kind.src, dst: kind.dst, data: carries[kind.kind] });
        }
      }
      return batch;
    };
    const submit = sim.submitRequest.bind(sim);
    sim.submitRequest = async (...args) => record(await submit(...args));
    const step = sim.step.bind(sim);
    sim.step = async (...args) => record(await step(...args));
    type Push = (path: unknown, start: number, src: number, dst: number, flags: number) => void;
    const pool = document.querySelector<HTMLCanvasElement & { beams?: { push: Push } }>('.stage canvas')?.beams;
    if (pool === undefined) throw new Error('the stage publishes no beam pool');
    const push = pool.push.bind(pool);
    pool.push = (path, start, src, dst, flags) => {
      log.drawn.push({ src, dst, data: (flags & control) === 0 });
      push(path, start, src, dst, flags);
    };
  }, { carries: CARRIES_CELLS, control: CONTROL });

  await page.getByLabel('Node', { exact: true }).selectOption('0');
  await page.locator('.rail').getByRole('button', { name: 'Send burst' }).click();
  await expect(view(page, 0)).toHaveText(BURST_HITS);
  const routes = (): Promise<{ sent: string[]; drawn: string[] }> =>
    page.evaluate(() => {
      const { sent, drawn } = window as unknown as Log;
      const name = ({ src, dst }: Route): string => `node ${src} to node ${dst}`;
      return { sent: sent.map(name).sort(), drawn: drawn.map(name).sort() };
    });
  const { sent } = await routes();
  expect(sent.length, 'the Send flushed packets in its own batch').toBeGreaterThan(0);
  await expect.poll(async () => (await routes()).drawn, { message: 'the beams drawn are the packets sent' }).toEqual(sent);

  // Played on, the feed's eager flushes and the steps' waves: the data beams
  // drawn are the data packets sent.
  const from = await virtualMs(page);
  await page.getByRole('button', { name: 'Play' }).click();
  await expect.poll(async () => virtualMs(page), { timeout: 30_000 }).toBeGreaterThan(from + 6000);
  await page.getByRole('button', { name: 'Pause' }).click();
  const data = await page.evaluate(() => {
    const { sent, drawn } = window as unknown as Log;
    return { sent: sent.filter((r) => r.data).length, drawn: drawn.filter((r) => r.data).length };
  });
  console.log(`over 6 s of Sustained overload: ${data.sent} data packets sent, ${data.drawn} data beams drawn`);
  expect(data.drawn, 'the data beams drawn are the data packets sent').toBe(data.sent);
});

// The zone-partition scenario cuts the last zone off from the rest, seeds both
// sides, and the sides disagree until the user heals the links — the
// eventual-consistency story end to end. Driven by Step, so the outcome is
// exact rather than timed.
test('zone partition splits zone c off until healed', async ({ page }) => {
  await boot(page);

  await page.getByRole('button', { name: 'Zone partition' }).click();
  const heal = page.getByRole('button', { name: 'Heal network' });
  await expect(heal).toBeVisible();
  await expect(page.locator('[data-field="status-cut"]')).toHaveText('32');

  // Node 0 (zone a) and node 2 (zone c) each counted 50. While the cut holds
  // node 1 (zone b) hears only node 0's and node 5 (zone c) only node 2's, so
  // both sit well under the 100 the whole cluster holds.
  await stepUntil(page, async () => num(await view(page, 1).textContent()) >= 50, 12, 'zone b heard zone a');
  expect(num(await view(page, 1).textContent()), 'zone b should not have zone c burst').toBeLessThan(100);
  expect(num(await view(page, 5).textContent()), 'zone c should not have zone a burst').toBeLessThan(100);
  // Rule 12: before Heal the headline says how long a node has lacked the other
  // side's changes, at least, as a censored "≥": the longest any node reports,
  // which is more than nothing and no more than the run so far.
  const cut = await snapshot(page);
  const longest = Math.max(...cut.nodes.map((n) => missingFor(n.lag) ?? -1));
  expect(longest, 'a node across the cut lacks a change it cannot hear').toBeGreaterThan(0);
  expect(longest, 'the bound never exceeds the run').toBeLessThanOrEqual(cut.virtual_ms);
  const printed = longest < 999.5 ? `${Math.round(longest)} ms` : `${(longest / 1000).toFixed(1)} s`;
  await expect(page.locator('.headline .badge')).toHaveText(`not caught up (≥ ${printed})`);
  await page.screenshot({ path: 'screenshots/ring-partition.png' });

  // Heal the links: the cut-off zone kept its CRDT state, so the sides reconcile
  // rather than cold-restart. Both now hold both seeds.
  await heal.click();
  await expect(page.locator('[data-field="status-cut"]')).toHaveText('0');
  await stepUntil(
    page,
    async () =>
      num(await view(page, 1).textContent()) >= 100 && num(await view(page, 5).textContent()) >= 100,
    16,
    'zones b and c reconciled',
  );
  // Healed, with no feed, no node lacks anything: shorter than anything the
  // cut held back.
  const healed = await snapshot(page);
  const after = Math.max(...healed.nodes.map((n) => missingFor(n.lag) ?? -1));
  expect(after, 'the heal delivered what the cut held back').toBeLessThan(longest);
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
// Admission: on the low-limit overload preset a node's window fills to the
// limit, so it rejects even one more hit: the headline recolors and the "at
// limit" pill appears.
test('the inspector shows convergence lag and the at-limit state', async ({ page }) => {
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
  const badge = page.locator('.inspector .convergence .badge');
  await stepUntil(page, async () => ((await badge.textContent()) ?? '').includes('caught up'), 12, 'node 6 caught up');

  // --- Admission channel, on the overload preset ---
  await page.getByRole('button', { name: 'Sustained overload' }).click();
  await stepUntil(
    page,
    async () => (await snapshot(page)).nodes[0].brim.kind === 'Rejected',
    40,
    "node 0's window holds the limit",
  );
  await clickNode(page, 0);
  // The headline recolors (the `.over` class) and the at-limit pill appears.
  await expect(page.locator('.inspector .hl-value.over')).toBeVisible();
  await expect(page.locator('.inspector .pill')).toContainText('at limit');

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

  // Both remote rows name a gateway at load: the cluster warmed up first.
  await expect(inspector.locator('tr.remote [data-field="gateway"]')).toHaveCount(2);

  await expect(inspector.locator('tr.own-zone')).toContainText('zone a');
  await expect(inspector.locator('tr.own-zone [data-field="members"]')).toHaveText('4');
  await expect(inspector.locator('tr.zone-peers [data-field="count"]')).toHaveText('3');
  await expect(rows).toHaveCount(2);
  await expect(rows.nth(0)).toHaveAttribute('data-zone', '1');
  await expect(rows.nth(1)).toHaveAttribute('data-zone', '2');

  // Each row names the runtime's own gateway for that zone, and that node sits
  // in the zone.
  const gateways = ((await snapshot(page, 0)).inspected?.peers ?? [])
    .filter((p) => p.role === 'gateway')
    .map((p) => ({ zone: p.zone, id: p.id }));
  for (const zone of [1, 2]) {
    const row = inspector.locator(`tr.remote[data-zone="${zone}"]`);
    const id = Number(await row.locator('[data-field="gateway"]').textContent());
    expect(gateways, `the runtime's gateway into zone ${zone}`).toContainEqual({ zone, id });
    await expect(page.locator(`.node-table tr[data-node="${id}"]`)).toHaveAttribute(
      'data-zone',
      String(zone),
    );
  }

  // The error budget the seed request set is a number above zero.
  const budget = inspector.locator('tr.budget [data-field="error-budget"]');
  await expect(budget).toHaveCount(1);
  expect(num(await budget.textContent())).toBeGreaterThan(0);

  expect(pageErrors, `unexpected page errors: ${pageErrors.join('; ')}`).toEqual([]);
});

// A gateway going silent is the failure the two-tier design exists to survive.
// The preset silences the origin's gateway into zone b without telling anyone;
// after the suspect timeout the origin names another gateway, the stage says so,
// and the zone catches up through it. Bounded Steps, no sleeps: 12 heartbeat
// periods of virtual time outlast the failover, so a green run has headroom and
// a red one cannot pass by waiting longer. A silent node's peers include ones it never
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

  // Step until the stage names a failover off g into zone b and the origin has
  // moved off g. The sentence names the new gateway first, then g.
  const annotation = page.locator('.annotation');
  const failover = /^node (\d+) now reaches zone b through node (\d+), after node (\d+)$/;
  const sentence = async (): Promise<RegExpExecArray | null> =>
    failover.exec(((await annotation.textContent()) ?? '').replace(/\s+/g, ' ').trim());
  const moved = async (): Promise<boolean> =>
    Number((await sentence())?.[3]) === g && Number(await gateway.textContent()) !== g;
  // Each Step, the zone b row wears the glyph and word of the answer state the
  // origin's runtime reports for its gateway: ◆ "ok" while settled or owed, ◇
  // "suspected" or "cannot relay" otherwise.
  const shown: Record<AnswerState, readonly [string, string]> = {
    settled: ['◆', 'ok'],
    owed: ['◆', 'ok'],
    suspect: ['◇', 'suspected'],
    cannot_relay: ['◇', 'cannot relay'],
  };
  const row = inspector.locator('tr.remote[data-zone="1"] td');
  const answers = new Set<AnswerState>();
  const checkRow = async (): Promise<void> => {
    const answer = (await snapshot(page, 0)).inspected?.peers.find(
      (p) => p.role === 'gateway' && p.zone === 1,
    )?.answer;
    if (answer === undefined) return;
    const [glyph, label] = shown[answer];
    const text = ((await row.textContent()) ?? '').replace(/\s+/g, ' ').trim();
    expect(text.startsWith(`${glyph} gateway node `), `a ${answer} gateway's glyph in "${text}"`).toBe(true);
    expect(text.endsWith(` · ${label}`), `a ${answer} gateway's word in "${text}"`).toBe(true);
    answers.add(answer);
  };
  await stepUntil(
    page,
    async () => {
      await checkRow();
      return moved();
    },
    12,
    'node 0 left its gateway into zone b',
  );
  expect([...answers].sort(), 'the zone b row was checked settled, then owed').toEqual(['owed', 'settled']);
  const [, origin, to, from] = (await sentence()) ?? [];
  expect(Number(from), `the annotation names g = ${g} as the gateway it left`).toBe(g);
  expect([1, 4, 7, 10], `the annotation's new gateway, node ${to}, is in zone b`).toContain(Number(to));
  expect(Number(to), 'the annotation names a new gateway').not.toBe(g);
  expect([1, 4, 7, 10], `the annotation's origin, node ${origin}, is outside zone b`).not.toContain(Number(origin));
  await expect(gateway).not.toHaveText(String(g));

  // Step until every zone b node but the silenced one is caught up; the
  // silenced one is not.
  const behind = (id: number) =>
    page.locator(`.node-table tr[data-node="${id}"] [data-field="behind"]`);
  const zoneB = [1, 4, 7, 10].filter((id) => id !== g);
  const caughtUp = async (): Promise<boolean> => {
    for (const id of zoneB) if ((await behind(id).textContent())?.trim() !== '0') return false;
    return true;
  };
  await stepUntil(page, caughtUp, 12, 'zone b caught up through its new gateway');
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
