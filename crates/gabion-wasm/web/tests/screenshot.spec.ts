import { expect, test as base, type Page } from '@playwright/test';
import { CARRIES_CELLS, type AnswerState, type ClusterState, type EventBatch } from '../src/lib/sim/types';
import { WATCHED_KEY } from '../src/lib/presets';
import { CONTROL } from '../src/lib/stage/layout';
import { pauseIfPlaying, send, sendButton, setRange, setSpeed, step, stepUntil, virtualMs } from './drive';

/** Every test fails on an error the page throws. */
const test = base.extend<{ page: Page }>({
  page: async ({ page }, use) => {
    const errors: string[] = [];
    page.on('pageerror', (e) => errors.push(e.message));
    await use(page);
    expect(errors, 'page errors').toEqual([]);
  },
});

// Every node holds Propagation's settled seed at load.
const SEED_TOTAL = '50';
const NODES = 12;
// Requests per Send every scenario opens with, one hit each.
const BURST_REQUESTS = '25';
// A node's count after one Send on top of the seed.
const SENT_TOTAL = String(Number(SEED_TOTAL) + Number(BURST_REQUESTS));

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

/** Load the page, open "Show as table" and play at 4×, so a run ends in a
 *  quarter of its time. The default scenario, Rate limiting, opens at 0 with
 *  node 0 selected. */
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
  page.evaluate(([id, key]) => window.sim!.snapshot(id, key), [inspect, WATCHED_KEY] as const);

/** Click the middle of node `id`'s disc. The grid cell has no pointer events, so
 *  the click reaches the stage's own hit test, as a user's would. */
async function clickNode(page: Page, id: number): Promise<void> {
  const box = await cell(page, id).boundingBox();
  if (box === null) throw new Error(`node ${id} has no grid cell box to click`);
  await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
}

/** Click the middle of the stage, which no disc covers. */
async function clickBare(page: Page): Promise<void> {
  const box = await page.locator('.stage').boundingBox();
  if (box === null) throw new Error('the stage has no box');
  await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
}

/** Select a scenario by its label and wait for its build. */
async function loadPreset(page: Page, name: string): Promise<void> {
  const preset = page.getByRole('group', { name: 'Scenario preset' }).getByRole('button', { name, exact: true });
  if ((await preset.getAttribute('aria-current')) !== 'true') await preset.click();
  await expect(preset).toHaveAttribute('aria-current', 'true');
  await expect(page.locator('.overlay.rebuilding')).toHaveCount(0);
}

const title = (page: Page) => page.locator('.pane-title [data-field="node"]');
const lede = (page: Page) => page.locator('.lede');

// One end-to-end check that doubles as the visual-quality screenshot source:
// the page boots the real gabion core in the browser on a settled cluster, the
// PixiJS stage renders the rings from a snapshot, and, played, a Send spreads
// to every node as beams, then leaves the window with the seed. The hard
// assertions read the table (robust regardless of WebGL pixels); the
// screenshots are for eyeballing the canvas.
test('boots settled, spreads a Send, and empties at the window edge', async ({ page }) => {

  await boot(page);
  await expect(page.locator('[data-template]')).toHaveAttribute('data-template', 'rl-0');
  await loadPreset(page, 'Propagation');
  await page.screenshot({ path: 'screenshots/ring-initial.png' });

  // Propagation settles its seed before the first paint: every node holds it.
  await expect(page.locator(COUNTS)).toHaveText(Array(NODES).fill(SEED_TOTAL));
  await expect(page.locator('[data-template]')).toHaveText(`✓ All ${NODES} nodes see all ${SEED_TOTAL} hits`);

  // A Send at node 0 stops before its first wave; Play draws the waves on.
  await send(page, 0);
  await expect(view(page, 0)).toHaveText(SENT_TOTAL);
  await page.getByRole('button', { name: 'Play' }).click();

  // Catch the stage mid-gossip once lines are on it. Best-effort — a
  // snapshot for eyeballing, not an assertion.
  await expect
    .poll(() => page.locator('.stage canvas').evaluate((c: HTMLCanvasElement & { beams?: { len: number } }) => c.beams?.len ?? 0))
    .toBeGreaterThan(0);
  await page.screenshot({ path: 'screenshots/ring-gossip.png' });

  // The Send reaches every node.
  await expect(page.locator(COUNTS), 'the Send did not reach every node').toHaveText(Array(NODES).fill(SENT_TOTAL), {
    timeout: 15_000,
  });
  await page.screenshot({ path: 'screenshots/ring-converged.png' });

  // With no feed, the seed and the Send leave the window, every node at once,
  // and every line has faded.
  await expect(page.locator(COUNTS), 'the hits did not leave the window').toHaveText(Array(NODES).fill('0'), {
    timeout: 30_000,
  });
  await expect
    .poll(() => page.locator('.stage canvas').evaluate((c: HTMLCanvasElement & { beams?: { len: number } }) => c.beams?.len))
    .toBe(0);
  await page.getByRole('button', { name: 'Pause' }).click();

  // Restart tears the engine down and rebuilds: back to the settled seed.
  // Guards both the shutdown path (a leak otherwise piles up an engine per
  // click) and the stage's reset of its beams.
  await page.getByRole('button', { name: 'Restart Propagation' }).click();
  await expect(page.locator(COUNTS), 'Restart did not rebuild a freshly seeded cluster').toHaveText(
    Array(NODES).fill(SEED_TOTAL),
    { timeout: 10_000 },
  );

});

// A17: a click selects, and the transport's Send goes to the selection. From
// a paused Rate limiting the target decides in the frame of the press, the
// run plays its waves until every node has the hits and pauses itself, and
// every receiver rises by exactly the hits admitted.
test('the transport Send sends to the selection', async ({ page }) => {
  await boot(page);
  // The counts in the discs: the table closed, the stage at its full size. At
  // half speed the run lasts long enough to be seen playing.
  await page.getByText('Show as table').click();
  await setSpeed(page, 0.5);
  await clickNode(page, 3);
  await expect(title(page)).toHaveText('3');
  await expect(sendButton(page)).toHaveText(/Send\s*25\s*requests to node\s*3/);

  await sendButton(page).click();
  await expect(page.locator('[role="gridcell"][data-node="3"] .count')).toHaveText(BURST_REQUESTS);
  await expect(page.locator('.decision[data-node="3"]')).toHaveText('+25 admitted');
  await expect(page.getByRole('button', { name: 'Pause' })).toBeVisible();
  await expect(page.locator('.count')).toHaveText(Array(NODES).fill(BURST_REQUESTS), { timeout: 15_000 });
  await expect(page.getByRole('button', { name: 'Play' })).toBeVisible({ timeout: 15_000 });
  await expect(page.locator('.stage-pane')).toHaveAttribute('data-clock', 'paused');
});

// A click on the bare stage selects nothing and keeps the selection: the
// selection never clears.
test('a click off the discs keeps the selection', async ({ page }) => {
  await boot(page);
  await clickNode(page, 3);
  await expect(title(page)).toHaveText('3');
  await clickBare(page);
  await expect(title(page)).toHaveText('3');
  await expect(page.locator('.charts')).toBeVisible();
});

// Removing the selected node moves the selection to the scenario's fallback:
// node 0's zone's lowest live node. The truth keeps the departed node's hits
// until they leave the window.
test("removing the selected node selects the scenario's fallback", async ({ page }) => {
  await boot(page);
  await loadPreset(page, 'Sandbox');
  await send(page, 0);
  await expect(page.locator(COUNTS)).toHaveText(Array(NODES).fill(BURST_REQUESTS), { timeout: 15_000 });
  await page.locator('.pane-foot').getByRole('button', { name: 'Remove node 0' }).click();
  await expect(cell(page, 0)).toHaveCount(0);
  await expect(title(page)).toHaveText('3');
  await expect(sendButton(page)).toHaveText(/node\s*3$/);
  const heading = page.locator('[data-template] [data-noun="view"]').first();
  await expect(heading, 'the departed node\'s hits still count').toHaveText(BURST_REQUESTS);
});

// The stage heading states the cluster at load: every node at 0 of the limit.
test('the stage heading states the cluster', async ({ page }) => {
  await boot(page);
  const heading = page.locator('[data-template]');
  await expect(heading).toHaveAttribute('data-template', 'rl-0');
  await expect(heading).toHaveText(`All ${NODES} nodes see 0 of the 100 limit`);
  await expect(page.locator('.charts')).toBeVisible();
});

// Sandbox: a Send runs its waves and pauses itself, every node agreeing; a
// Send the reader pauses at once is then drawn by Step one wave at a time. The
// requests chart counts what was admitted, and with no feed the hits age out
// to quiet.
test('a Send runs its waves and pauses itself; Step after Send goes one wave at a time', async ({ page }) => {
  await boot(page);
  await loadPreset(page, 'Sandbox');
  await expect(view(page, 0)).toHaveText('0');

  await send(page, 0);
  await expect(page.locator(COUNTS)).toHaveText(Array(NODES).fill(BURST_REQUESTS));
  await expect(page.getByRole('button', { name: 'Play' })).toBeVisible();
  await expect(page.locator('figure[data-chart="requests"] [data-field="admitted"]')).toHaveText(BURST_REQUESTS);

  // Paused at once, the next Send waits for node 0's heartbeat; each Step
  // then lands one wave, so node 6 (a zone peer) counts it before node 1
  // (behind a gateway).
  await send(page, 0, true);
  await expect(view(page, 0)).toHaveText('50');
  await stepUntil(page, async () => (await view(page, 6).textContent()) === '50', 4, 'wave 1 reached node 6');
  expect(await view(page, 1).textContent(), 'the relays wait for their own Step').toBe(BURST_REQUESTS);
  await step(page);
  await expect(view(page, 1)).toHaveText('50');

  // With no feed to replenish it, the window empties and the cluster falls
  // quiet.
  await page.getByRole('button', { name: 'Play' }).click();
  await expect(view(page, 0)).toHaveText('0', { timeout: 20_000 });
  await pauseIfPlaying(page);
});

// The Rate limiting script on the charts (A30): Sends at nodes 0, 1, 2 and 2
// fill the window, and a fifth at node 2 is refused whole. R1 draws node 2's
// window, whose buckets sum to its window total, with the refusal standing on
// it, labelled; the table lists the five Sends in order, the fifth rejected.
test('the charts tell the Rate limiting script', async ({ page }) => {
  await boot(page);
  for (const node of [0, 1, 2, 2, 2]) await send(page, node);
  const r1 = page.locator('figure[data-chart="window"]');
  await expect(r1.locator('[data-field="r1-total"]')).toHaveText('100');
  const canvas = r1.locator('canvas');
  await expect
    .poll(() => canvas.evaluate((c: HTMLCanvasElement) => (c.dataset.columns ?? '').split(',').reduce((a, v) => a + Number(v), 0)))
    .toBe(100);
  await expect
    .poll(() => canvas.evaluate((c: HTMLCanvasElement) => (JSON.parse(c.dataset.labelBoxes ?? '[]') as { text: string }[]).map((l) => l.text)))
    .toEqual(expect.arrayContaining(['limit 100', '25 rejected']));
  const r2 = page.locator('figure[data-chart="requests"]');
  await expect(r2.locator('[data-field="admitted"]')).toHaveText('100');
  await expect(r2.locator('[data-field="rejected"]')).toHaveText('25');
  await page.getByText('Show the charts as tables').click();
  const rows = page.locator('details.table table').last().locator('tbody tr');
  await expect(rows.locator('[data-field="node"]')).toHaveText(['0', '1', '2', '2', '2']);
  await expect(rows.locator('[data-field="admitted"]')).toHaveText(['25', '25', '25', '25', '0']);
  await expect(rows.locator('[data-field="rejected"]')).toHaveText(['0', '0', '0', '0', '25']);
  await page.screenshot({ path: 'screenshots/charts-rate-limiting.png' });
});

// Live join: "+ Add node" spawns a fresh cold-start member into a settled
// cluster — no rebuild. It takes the next stable id (12, never reused) in the
// zone the picker offers (the fewest live members, the lowest index on a tie:
// zone a) and joins by gossip, then catches up to the settled total by
// anti-entropy. The screenshots are the proof the join *reads* right: the
// newcomer fades/scales in at its ring slot while the survivors glide to
// re-space around it.
test('adding a node joins it live and it catches up by gossip', async ({ page }) => {

  await boot(page);
  await loadPreset(page, 'Propagation');

  // The charts before the join. Pair with `dash-after-add` to eyeball that the
  // join threads through this same window: the time axis keeps running.
  const dashboard = page.locator('.charts');
  await dashboard.screenshot({ path: 'screenshots/dash-before-add.png' });

  // Add a fresh node: it takes id 12, joins zone a and starts cold (view zero).
  // The run Add starts may catch it up before a read, so its first count is
  // recorded as its row appears.
  await expect(page.getByLabel('Add to')).toHaveValue('0');
  await page.evaluate(() => {
    const w = window as unknown as { joined: string | null };
    w.joined = null;
    const table = document.querySelector('.node-table')!;
    const watch = new MutationObserver(() => {
      const view = table.querySelector('tr[data-node="12"] [data-field="view"]');
      if (view === null) return;
      w.joined = view.textContent;
      watch.disconnect();
    });
    watch.observe(table, { subtree: true, childList: true, characterData: true });
  });
  await page.getByRole('button', { name: 'Add node' }).click();
  await expect
    .poll(() => page.evaluate(() => (window as unknown as { joined: string | null }).joined), { message: 'node 12 starts cold' })
    .toBe('0');
  await expect(page.locator(COUNTS)).toHaveCount(NODES + 1);
  await expect(page.locator('.node-table tr[data-node="12"]')).toHaveAttribute('data-zone', '0');
  await page.screenshot({ path: 'screenshots/ring-node-added.png' });
  // The same dashboard right after the join: the time axis is unbroken from
  // `dash-before-add`, proving the join did not reset the charts.
  await dashboard.screenshot({ path: 'screenshots/dash-after-add.png' });

  // Add runs until the newcomer has caught up from zero by anti-entropy, the
  // survivors' join sweep bringing it the seed, then pauses.
  await expect(view(page, 12)).toHaveText(SEED_TOTAL, { timeout: 15_000 });
  await expect(page.locator('.stage-pane')).toHaveAttribute('data-clock', 'paused', { timeout: 15_000 });
  await page.screenshot({ path: 'screenshots/ring-node-added-caughtup.png' });

});

// Live leave through the detail pane: removing the selected node drops exactly
// that one. Its stable id leaves a *gap* — neighbours 4 and 6 keep their ids, no
// renumber — and the survivors re-space to close the ring.
test('the pane removes the selected node', async ({ page }) => {

  await boot(page);
  await loadPreset(page, 'Propagation');
  await expect(cell(page, 5)).toHaveCount(1);

  await clickNode(page, 5);
  await expect(title(page)).toHaveText('5');
  await page.locator('.pane-foot').getByRole('button', { name: 'Remove node 5' }).click();

  // Node 5 is gone, 12 → 11 nodes; 4 and 6 remain (a gap at 5, not a renumber).
  await expect(cell(page, 5)).toHaveCount(0);
  await expect(cell(page, 4)).toHaveCount(1);
  await expect(cell(page, 6)).toHaveCount(1);
  await expect(page.locator('[role="gridcell"]')).toHaveCount(NODES - 1);
  // The selection moves to node 0's zone's lowest live node, the scenario's.
  await expect(title(page)).toHaveText('0');
  await expect(view(page, 0)).toHaveText(SEED_TOTAL);
  await page.screenshot({ path: 'screenshots/ring-node-removed.png' });

});

// Scenario presets rebuild the cluster from a fresh config + opening seed.
// Switching from the default Rate limiting to Steady state should tear down and
// rebuild with the steady seed (a light request at one node in each zone).
test('selecting a scenario preset rebuilds the cluster with its seed', async ({ page }) => {
  await boot(page);

  // Default scenario: Rate limiting opens at 0.
  await expect(view(page, 0)).toHaveText('0');

  await page.getByRole('button', { name: 'Steady state' }).click();

  // Steady state seeds 10 on nodes 0, 4 and 8 (one per zone) and nothing
  // elsewhere; at load (paused) each carries only its own seed, so the cluster
  // maximally disagrees before any gossip: a fresh cluster.
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
  await expect(details.locator('summary')).toContainText('1 staged');
  await expect(page.locator('.overlay')).toHaveCount(0);
  await expect(details).toHaveAttribute('open', '');
  await expect(rebuild).toBeEnabled();

  // Rebuild applies the staged set: the cluster rebuilds (same size, no errors),
  // the section is still open, and nothing is staged any more.
  await rebuild.click();
  await expect(page.locator('.overlay')).toHaveCount(0);
  await expect(page.locator(COUNTS)).toHaveCount(NODES);
  await expect(details).toHaveAttribute('open', '');
  await expect(details.locator('summary')).not.toContainText('staged');
  await expect(rebuild).toBeDisabled();
  await expect(page.locator('#rebuild-reason')).toHaveText('nothing staged');
});

// The rule knobs live in the same disclosure: a gossip-interval and window range
// plus a limit number input. Moving the window updates its derived bucket-count
// readout (the `buckets.ts` math); the explicit
// Rebuild then applies it — proof the readout and the rebuild are wired and the
// engine still accepts the new window/bucket pairing (window stays a whole
// number of 1 s buckets, so `SimConfig::validate` passes). The zone stepper
// stages the same way and stops at the engine's own ceiling.
test('the window knob updates its bucket-count readout and rebuilds', async ({ page }) => {

  await boot(page);
  await loadPreset(page, 'Propagation');

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
  // and the time charts span the rebuilt window.
  await page.getByRole('button', { name: 'Rebuild cluster' }).click();
  await expect(page.locator('.overlay')).toHaveCount(0);
  await expect(page.locator(COUNTS)).toHaveCount(NODES);
  await expect
    .poll(() =>
      page
        .locator('figure[data-chart="behind"] canvas')
        .evaluate((c: HTMLCanvasElement) => (JSON.parse(c.dataset.labelBoxes ?? '[]') as { text: string }[]).map((l) => l.text)),
    )
    .toContain('5\u202fs ago');
});

// The sustained-overload scenario feeds the cluster at a steady rate against a
// low limit (400). Stepped, the nodes admit hits until their window holds the
// limit and reject the rest: the "why gabion exists" story.
test('sustained overload rejects what passes the limit', async ({ page }) => {
  // About 20 s alone, each Step deciding the feed's requests one by one: the
  // default 30 s left too little room on a loaded runner.
  test.setTimeout(90_000);
  await boot(page);

  await page.getByRole('button', { name: 'Sustained overload' }).click();
  // The overload preset has no opening request — the feed is the story — so
  // every node starts at zero until played.
  await expect(view(page, 0)).toHaveText('0');

  // The feed offers 40 hits a Step, so the window fills to the limit of 400
  // within a dozen Steps and the next requests are rejected; a node rejects
  // only once its own window holds the limit, so the cluster has admitted at
  // least that many by then.
  await stepUntil(page, async () => (await snapshot(page)).rejected_hits > 0, 40, 'a node rejected a request');
  expect((await snapshot(page)).admitted_hits, 'the window filled before any rejection').toBeGreaterThanOrEqual(400);
  // Stepped and still, the requests chart prints the engine's counters.
  const counted = await snapshot(page);
  const r2 = page.locator('figure[data-chart="requests"]');
  await expect(r2.locator('[data-field="admitted"]')).toHaveText(String(counted.admitted_hits));
  await expect(r2.locator('[data-field="rejected"]')).toHaveText(String(counted.rejected_hits));
  await page.screenshot({ path: 'screenshots/ring-overload.png' });
  await page.locator('.charts').screenshot({ path: 'screenshots/dash-overload.png' });

});

// On a display with 2 device px per CSS px every chart label is still drawn at
// its CSS size: the charts draw in CSS px through a device-pixel transform, so
// each published box spans its text set in the page's font at the box's size,
// measured the same way, plus a 2 px halo each side, and the canvas holds 2
// device px per CSS px.
test.describe('on a 2× display', () => {
  test.use({ deviceScaleFactor: 2 });

  test('chart labels keep their CSS size', async ({ page }) => {
    await boot(page);
    const canvas = page.locator('figure[data-chart="window"] canvas[data-label-boxes]');
    await expect
      .poll(() =>
        canvas.evaluate((c: HTMLCanvasElement) => {
          const ctx = document.createElement('canvas').getContext('2d')!;
          const family = getComputedStyle(document.documentElement).fontFamily;
          const labels = JSON.parse(c.dataset.labelBoxes ?? '[]') as { text: string; w: number; size: number }[];
          return {
            dpr: c.width / c.clientWidth,
            labels: labels.map((l) => {
              ctx.font = `${l.size}px ${family}`;
              const text = ctx.measureText(l.text).width;
              return { text: l.text, spansItsText: Math.abs(l.w - text - 4) < 0.5 };
            }),
          };
        }),
      )
      .toEqual({
        dpr: 2,
        labels: ['0', '50', 'limit 100', '10\u202fs ago', 'now'].map((text) => ({ text, spansItsText: true })),
      });
  });
});

// Against the overload preset's low limit a Send crosses its node's error
// budget, so it flushes at once: its packets leave in the Send's own batch,
// and the stage draws each as a line as it is sent, not a Step later. The
// feed runs with the Send's run, so no count is read exactly: node 0 holds
// at least the Send's hits. Played on, every packet of the feed's submits
// and of the steps is drawn, one line each, once the queue has shown them.
test('a Send that flushes at once draws its packets as it sends', async ({ page }) => {
  await boot(page);
  await page.getByRole('button', { name: 'Sustained overload' }).click();
  await expect(view(page, 0)).toHaveText('0');

  type Route = { src: number; dst: number; data: boolean };
  type Log = { sent: Route[]; drawn: Route[]; send: Route[] };
  await page.evaluate(({ carries, control }) => {
    const log = window as unknown as Log;
    log.sent = [];
    log.drawn = [];
    log.send = [];
    const sim = window.sim!;
    const routesOf = (batch: EventBatch): Route[] =>
      batch.events.flatMap(({ kind }) =>
        kind.type === 'PacketSent' || kind.type === 'PacketDropped' ? [{ src: kind.src, dst: kind.dst, data: carries[kind.kind] }] : [],
      );
    const record = (batch: EventBatch): EventBatch => {
      log.sent.push(...routesOf(batch));
      return batch;
    };
    const submit = sim.submitRequest.bind(sim);
    sim.submitRequest = async (node, key, requests, until) => {
      const batch = await submit(node, key, requests, until);
      // The reader's Send, not one of the feed's one-hit requests.
      if (requests > 1) log.send = routesOf(batch);
      return record(batch);
    };
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

  await sendButton(page).click();
  await expect
    .poll(async () => Number(await view(page, 0).textContent()), { message: "node 0 holds the Send's hits" })
    .toBeGreaterThanOrEqual(Number(BURST_REQUESTS));
  await pauseIfPlaying(page);
  const routes = (): Promise<{ sent: string[]; drawn: string[]; send: string[] }> =>
    page.evaluate(() => {
      const { sent, drawn, send } = window as unknown as Log;
      const name = ({ src, dst }: Route): string => `node ${src} to node ${dst}`;
      return { sent: sent.map(name).sort(), drawn: drawn.map(name).sort(), send: send.map(name) };
    });
  const { send } = await routes();
  expect(send.length, 'the Send flushed packets in its own batch').toBeGreaterThan(0);
  const drawnAll = async (): Promise<boolean> => {
    const { sent, drawn } = await routes();
    return drawn.join() === sent.join() && send.every((r) => drawn.includes(r));
  };
  await expect.poll(drawnAll, { message: 'the lines drawn are the packets sent, the Send\'s among them' }).toBe(true);

  // Played on, the feed's eager flushes and the steps' waves: the data lines
  // drawn are the data packets sent.
  const from = await virtualMs(page);
  await page.getByRole('button', { name: 'Play' }).click();
  await expect.poll(async () => virtualMs(page), { timeout: 30_000 }).toBeGreaterThan(from + 6000);
  await page.getByRole('button', { name: 'Pause' }).click();
  const data = (): Promise<{ sent: number; drawn: number }> =>
    page.evaluate(() => {
      const { sent, drawn } = window as unknown as Log;
      return { sent: sent.filter((r) => r.data).length, drawn: drawn.filter((r) => r.data).length };
    });
  const shown = async (): Promise<boolean> => {
    const { sent, drawn } = await data();
    return drawn === sent;
  };
  await expect.poll(shown, { message: 'the data lines drawn are the data packets sent' }).toBe(true);
  const counted = await data();
  console.log(`over 6 s of Sustained overload: ${counted.sent} data packets sent, ${counted.drawn} data lines drawn`);
});

// Zone partition: zone c is cut off at load. A Send at node 0 reaches zones a
// and b and never zone c, until Heal reconnects it; a node that joins zone c
// is cut off with it.
test('zone partition splits zone c off until healed', async ({ page }) => {
  await boot(page);
  await loadPreset(page, 'Zone partition');
  await expect(lede(page)).toContainText('zone c cut off');
  // The same fault made again is listed once: the Send below runs after it.
  await page.locator('#cut-zone').selectOption({ label: 'zone c' });
  await page.getByRole('button', { name: 'Cut zone c off' }).click();

  await send(page, 0);
  for (const id of [0, 3, 1, 4]) await expect(view(page, id), `node ${id} heard node 0`).toHaveText(BURST_REQUESTS);
  for (const id of [2, 5, 8, 11]) await expect(view(page, id), `node ${id} across the cut`).toHaveText('0');
  await expect(page.locator('.stage-heading .repair')).toHaveText('Reconnect zone c');
  expect((await lede(page).textContent())?.split('zone c cut off').length, 'the lede names the cut once').toBe(2);
  await page.screenshot({ path: 'screenshots/ring-partition.png' });

  // Heal runs until every node has the hits.
  await page.getByRole('button', { name: 'Heal network' }).click();
  await expect(page.locator(COUNTS)).toHaveText(Array(NODES).fill(BURST_REQUESTS), { timeout: 15_000 });
  await expect(lede(page)).not.toContainText('cut off');
  await page.screenshot({ path: 'screenshots/ring-healed.png' });
});

test('a node that joins a cut zone is cut off with it (A6)', async ({ page }) => {
  await boot(page);
  await loadPreset(page, 'Zone partition');
  await page.getByLabel('Add to').selectOption({ label: 'zone c' });
  await page.getByRole('button', { name: 'Add node' }).click();
  await expect(page.locator('.node-table tr[data-node="12"]')).toHaveAttribute('data-zone', '2');
  await expect(page.locator('.stage-pane')).toHaveAttribute('data-clock', 'paused', { timeout: 15_000 });
  // The joiner's own hits reach its zone and stop at the cut.
  await send(page, 12);
  for (const id of [2, 5, 8, 11]) await expect(view(page, id), `node ${id} heard the joiner`).toHaveText(BURST_REQUESTS);
  for (const id of [0, 1, 3, 4, 6, 7, 9, 10]) await expect(view(page, id), `node ${id} across the cut from the joiner`).toHaveText('0');
  // Nor do node 0's hits reach the joiner's zone: the run ends with them
  // spread on node 0's side only.
  await send(page, 0);
  for (const id of [0, 1, 3, 4, 6, 7, 9, 10]) await expect(view(page, id), `node ${id} heard node 0`).toHaveText(BURST_REQUESTS);
  for (const id of [2, 5, 8, 11, 12]) await expect(view(page, id), `node ${id} in the cut zone`).toHaveText(BURST_REQUESTS);
  await expect(lede(page)).toContainText('zone c cut off');
});

// The detail pane surfaces the runtime's admin snapshot under the charts: the
// gossip targets with its route sentence and the identity footer.
test('the detail pane renders every section', async ({ page }) => {

  await boot(page);
  await loadPreset(page, 'Propagation');
  const inspector = page.locator('.inspector');
  await expect(inspector).toBeVisible();
  await expect(page.locator('.pane-title')).toContainText('selected · zone a');

  await expect(inspector.locator('.section-head', { hasText: 'Gossip targets' })).toBeVisible();
  await expect(inspector.locator('.section-head')).toHaveCount(1);

  // Gossip targets: the route sentence names exactly the gateways the stage
  // marks, then own zone, zone peers, one row per remote zone, counters and
  // the error budget.
  const gateways = await inspector.locator('tr.remote [data-field="gateway"]').allTextContents();
  await expect(inspector.locator('.route')).toContainText(`through node ${gateways[0]}`);
  await expect(inspector.locator('.route')).toContainText(`through node ${gateways[1]}`);
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
});

// The pane tells a newcomer what node 0's runtime chose: one row per
// remote zone naming the gateway it sends that zone's copy through, and the
// error budget it holds before an eager flush. The page reads both from the
// runtime's own admin rows, so the test compares the named gateway with the
// engine's, not just its zone.
test('the inspector shows gateways per remote zone and the error budget', async ({ page }) => {

  await boot(page);
  await loadPreset(page, 'Propagation');
  const inspector = page.locator('.inspector');
  await expect(title(page)).toHaveText('0');
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

});

// A gateway going silent is the failure the two-tier design exists to survive.
// The preset silences the origin's gateway into zone b without telling anyone;
// after the suspect timeout the origin names another gateway, the stage says so,
// and the zone catches up through it. Bounded Steps, no sleeps: 12 heartbeat
// periods of virtual time outlast the failover, so a green run has headroom and
// a red one cannot pass by waiting longer. A silent node's peers include ones it never
// heard from (`Option::None` crosses as `undefined`), the case that once crashed
// the inspector, so the test opens it too and then checks Restart still rebuilds.
test('gateway failure fails over and the zone catches up', async ({ page }) => {

  await boot(page);
  await loadPreset(page, 'Gateway failure');
  const inspector = page.locator('.inspector');
  await expect(title(page)).toHaveText('0');

  const gateway = inspector.locator('tr.remote[data-zone="1"] [data-field="gateway"]');
  const g = Number(await gateway.textContent());
  await expect(lede(page)).toContainText(`node ${g} silenced`);

  // Step until the narration names a failover off g into zone b and node 0
  // has moved off g. The sentence names the new gateway first, then g.
  const annotation = page.locator('.narration');
  const failover = /^node (\d+) now reaches zone b through node (\d+), after node (\d+)$/;
  const sentence = async (): Promise<RegExpExecArray | null> =>
    failover.exec(((await annotation.textContent()) ?? '').replace(/\s+/g, ' ').trim());
  const moved = async (): Promise<boolean> =>
    Number((await sentence())?.[3]) === g && Number(await gateway.textContent()) !== g;
  // Each Step, the zone b row wears the glyph and word of the answer state
  // node 0's runtime reports for its gateway: a port, "ok" while settled or
  // owed; a hollow port, "suspected" or "cannot relay" otherwise.
  const shown: Record<AnswerState, readonly [string, string]> = {
    settled: ['port', 'ok'],
    owed: ['port', 'ok'],
    suspect: ['suspect', 'suspected'],
    cannot_relay: ['suspect', 'cannot relay'],
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
    await expect(row.locator('svg[data-glyph]'), `a ${answer} gateway's glyph`).toHaveAttribute('data-glyph', glyph);
    expect(text.startsWith('gateway node '), `the row "${text}" names its gateway`).toBe(true);
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
  await expect(title(page)).toHaveText(String(g));

  // Restart must clear the rebuilding overlay and rebuild the cluster with the
  // scenario's node selected, not wedge.
  await page.getByRole('button', { name: 'Restart Gateway failure' }).click();
  await expect(page.locator('.overlay.rebuilding')).toHaveCount(0, { timeout: 10_000 });
  await expect(page.locator(COUNTS)).toHaveCount(NODES);
  await expect(title(page)).toHaveText('0');

});
