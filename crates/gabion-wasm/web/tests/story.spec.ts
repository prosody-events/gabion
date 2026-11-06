import { expect, test, type Locator, type Page } from '@playwright/test';
import type { ClusterState } from '../src/lib/sim/types';
import type { GlyphTable } from '../src/lib/stage/renderer';
import { CONTROL, CROSS, RELAY, STRATA_MIN_INNER } from '../src/lib/stage/layout';
import { readTheme } from '../src/lib/theme';
import { WATCH, pauseIfPlaying, sendButton, still } from './drive';
import { WATCHES } from '../src/lib/next';
import { PRESETS, presetById } from '../src/lib/presets';
import { COMMANDS, clauses } from './next-actions';

// Each scenario's story, walked the way a newcomer walks it: pressing only the
// one filled control the caption's action line names, and waiting while the
// line says to wait (README "Tests", "The next action"). In every state the
// page shows exactly one next action (A49), a wait shows its progress (A54),
// the goal never commands (A56) and one live region speaks (A19). Runs in the
// ten audit projects (motion reduced) and the motion project.

const line = (page: Page): Locator => page.locator('#next-action');
const primary = (page: Page): Locator => page.locator('[data-primary]');
const squash = (s: string | null): string => (s ?? '').replace(/\s+/g, ' ').trim();

/** The page's engine, snapshotted, with node `inspect`'s rows. */
const snapshot = (page: Page, inspect: number | null = null): Promise<ClusterState> =>
  page.evaluate(([id, watch]) => window.sim!.snapshot(id, watch), [inspect, WATCH] as const);

async function open(page: Page): Promise<void> {
  await page.goto('/');
  await expect(page.locator('.stage canvas')).toBeVisible({ timeout: 30_000 });
  const controls = page.locator('.controls > summary');
  if (await controls.isVisible()) await controls.click();
}

/** Scenario `name`'s button in the preset list. */
const presetButton = (page: Page, name: string): Locator =>
  page.getByRole('group', { name: 'Scenario preset' }).getByRole('button', { name, exact: true });

async function loadPreset(page: Page, name: string): Promise<void> {
  const preset = presetButton(page, name);
  if ((await preset.getAttribute('aria-current')) !== 'true') await preset.click();
  await expect(preset).toHaveAttribute('aria-current', 'true');
  await expect(page.locator('.overlay.rebuilding')).toHaveCount(0);
}

/** The action line's tag: Next, Wait or Watch. */
const tagOf = async (page: Page): Promise<string> => squash(await line(page).locator('.tag').textContent());

/** A49, A54, A56 and A19 in the state the page stands in now; returns the
 *  primary's visible label, or null in a wait or a watch. */
async function checkState(page: Page, where: string): Promise<string | null> {
  // The pointer off every control, so no hover tints what is read.
  await page.mouse.move(0, 0);
  const tag = await tagOf(page);
  const found = await page.evaluate(
    () => {
      const out: string[] = [];
      const ink = getComputedStyle(document.documentElement).color;
      // A49.2: only the primary wears the ink fill, the scenario list included.
      for (const el of document.querySelectorAll<HTMLElement>('button, .preset')) {
        if (!el.checkVisibility()) continue;
        const filled = getComputedStyle(el).backgroundColor === ink;
        if (filled !== el.hasAttribute('data-primary')) out.push(`"${el.textContent?.trim()}" ${filled ? 'is filled but not the next action' : 'is the next action but not filled'}`);
      }
      // A19: one live region, no status role, nothing focusable inside it.
      if (document.querySelectorAll('[aria-live]').length !== 1) out.push('not exactly one live region');
      if (document.querySelectorAll('[role="status"]').length !== 0) out.push('a role="status" element');
      if (document.querySelector('[aria-live] button, [aria-live] a, [aria-live] input') !== null) out.push('a control inside the live region');
      // A19: no claim said twice: each number a voice tags (whose, what,
      // how much) appears in one voice, the heading, the narration or a
      // decision.
      const voices = new Map<string, string>();
      for (const el of document.querySelectorAll<HTMLElement>('[data-noun]')) {
        if (!el.checkVisibility()) continue;
        const voice = el.closest('[data-template]') !== null ? 'heading' : el.closest('.narration') !== null ? 'narration' : 'decision';
        const said = `${el.dataset.subject} ${el.dataset.noun} ${el.textContent?.trim()}`;
        const first = voices.get(said);
        if (first !== undefined && first !== voice) out.push(`"${said}" is said in the ${first} and the ${voice}`);
        voices.set(said, first ?? voice);
      }
      // A19: the narration takes at most two lines where the stage has its
      // measure (from 1100 px; the narrower layouts come in a later phase).
      const narration = innerWidth >= 1100 ? document.querySelector('.narration') : null;
      if (narration !== null) {
        const range = document.createRange();
        range.selectNodeContents(narration);
        const lines = new Set([...range.getClientRects()].map((r) => Math.round(r.bottom)));
        if (lines.size > 2) out.push(`the narration takes ${lines.size} lines`);
      }
      return out;
    },
  );
  expect(found, where).toEqual([]);
  // A56: the goal explains and never commands, nor quotes a control.
  const goal = squash(await page.locator('.scenario').textContent());
  for (const clause of clauses(goal)) {
    expect(COMMANDS as readonly string[], `${where}: the goal "${goal}" commands in "${clause}"`).not.toContain(clause.split(/\s+/)[0].toLowerCase());
  }
  for (const label of (await page.locator('button:visible').allInnerTexts()).map(squash).filter((l) => l !== '')) {
    const quoted = new RegExp(`\\b${label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
    expect(quoted.test(goal), `${where}: the goal "${goal}" quotes "${label}"`).toBe(false);
  }
  const lineText = squash(await line(page).textContent());
  if (tag !== 'Next') {
    await expect(primary(page), `${where}: a ${tag} fills nothing`).toHaveCount(0);
    if (tag === 'Wait') {
      // A54: the line holds one meter, and its tag is ink.
      await expect(line(page).getByRole('progressbar'), `${where}: the wait's meter`).toHaveCount(1);
      const colours = await page.evaluate(readTheme);
      const tagColour = await line(page).locator('.tag').evaluate((el) => getComputedStyle(el).color);
      const ink = await page.evaluate(() => getComputedStyle(document.documentElement).color);
      expect(tagColour, `${where}: the tag is ink (${colours.fg})`).toBe(ink);
    }
    return null;
  }
  await expect(primary(page), `${where}: one filled control`).toHaveCount(1);
  const p = primary(page);
  const label = squash(await p.innerText());
  expect(await p.isDisabled(), `${where}: the next action is never disabled`).toBe(false);
  if ((await p.evaluate((el) => el.closest('#next-action') === null)) === true) {
    expect(squash(await line(page).locator('strong').textContent()), `${where}: the line "${lineText}" quotes "${label}"`).toBe(label);
  }
  const name = await p.evaluate((el) => (el.getAttribute('aria-label') ?? el.textContent ?? '').replace(/\s+/g, ' ').trim());
  expect(name.startsWith(label), `${where}: its accessible name "${name}" begins with its label`).toBe(true);
  expect(await line(page).getByRole('button').count(), `${where}: at most one button in the line`).toBeLessThanOrEqual(1);
  // A49.6: in view once the line is scrolled to the top, where the page does
  // not scroll (from 1100 px; the scrolling layouts' sticky bar comes in a
  // later phase).
  if ((page.viewportSize()?.width ?? 0) >= 1100) {
    await line(page).evaluate((el) => el.scrollIntoView({ block: 'start' }));
    const box = await p.boundingBox();
    const vh = page.viewportSize()?.height ?? 0;
    expect(box !== null && box.y < vh && box.y + box.height > 0, `${where}: "${label}" is in view with its line`).toBe(true);
  }
  return label;
}

/** Wait until the line names something to press, checking each wait's meter
 *  never falls (A54). */
async function untilNext(page: Page, where: string, timeout = 60_000): Promise<void> {
  const deadline = Date.now() + timeout;
  let last = -1;
  let reason = '';
  while ((await tagOf(page)) !== 'Next') {
    if (Date.now() > deadline) throw new Error(`${where}: still "${squash(await line(page).textContent())}"`);
    // Read at once: the line may change between two reads.
    const [clause, now] = await line(page).evaluate((el) => [
      (el.textContent ?? '').replace(/\s+/g, ' ').trim(),
      Number(el.querySelector('[role="progressbar"]')?.getAttribute('aria-valuenow') ?? -1),
    ] as const);
    if (clause !== reason) {
      reason = clause;
      last = -1;
    }
    if (now >= 0) {
      expect(now, `${where}: the meter never falls in "${clause}"`).toBeGreaterThanOrEqual(last);
      last = now;
    }
    await page.waitForTimeout(100);
  }
}

/** Record every text `selector` shows from now on, repeats collapsed, into
 *  `window[key]`; `recorded` reads the record and starts it afresh. */
async function record(page: Page, key: string, selector: string): Promise<void> {
  await page.evaluate(([key, selector]) => {
    const w = window as unknown as Record<string, string[]>;
    w[key] = [];
    const take = (): void => {
      const text = (document.querySelector(selector)?.textContent ?? '').replace(/\s+/g, ' ').trim();
      if (w[key].at(-1) !== text) w[key].push(text);
    };
    take();
    new MutationObserver(take).observe(document.body, { subtree: true, childList: true, characterData: true });
  }, [key, selector] as const);
}

const recorded = (page: Page, key: string): Promise<string[]> =>
  page.evaluate((key) => {
    const w = window as unknown as Record<string, string[]>;
    const out = w[key];
    w[key] = out.slice(-1);
    return out;
  }, key);

/** Record every heading the page shows, from now on, as its template and
 *  text, repeats collapsed. */
async function recordTemplates(page: Page): Promise<void> {
  await page.evaluate(() => {
    const w = window as unknown as { templates: [string, string][] };
    w.templates = [];
    const take = (): void => {
      const h = document.querySelector('[data-template]');
      const id = h?.getAttribute('data-template') ?? '';
      if (w.templates.at(-1)?.[0] !== id) w.templates.push([id, (h?.textContent ?? '').replace(/\s+/g, ' ').trim()]);
    };
    take();
    new MutationObserver(take).observe(document.querySelector('.stage-pane')!, { subtree: true, attributes: true, attributeFilter: ['data-template'], childList: true, characterData: true });
  });
}

test('Rate limiting, state by state (A40, A49, A18, A54, A56, A19)', async ({ page }, info) => {
  test.setTimeout(240_000);
  await open(page);
  // Rate limiting is the page's default: it opens there, at nothing counted.
  await expect(presetButton(page, 'Rate limiting')).toHaveAttribute('aria-current', 'true');
  await expect(page.locator('[data-template]')).toHaveAttribute('data-template', 'rl-0');
  // At 1×: the sliding-window act wants its Sends a bucket apart, and the
  // checks between presses then cost the story next to nothing in simulated
  // time.
  await recordTemplates(page);
  // Every narration and readout from the first press on, so a sentence is
  // read whatever the playing clock says after it.
  await record(page, 'narrations', '.narration');
  await record(page, 'readouts', '.readout .time');
  const pressed: string[] = [];
  let refusals = 0;
  // A57: the buckets the Sends admitted after Play were pressed in.
  const paced: number[] = [];
  for (let step = 0; step < 60; step++) {
    const where = `${info.project.name}, state ${step}`;
    await untilNext(page, where);
    const label = await checkState(page, where);
    if (label === null) continue;
    pressed.push(label.startsWith('Send') ? 'Send' : label);
    if (label.startsWith('Go to')) break;
    const before = await snapshot(page);
    await recorded(page, 'narrations');
    await primary(page).click();
    if (label.startsWith('Send')) {
      await expect.poll(async () => {
        const s = await snapshot(page);
        return s.admitted_hits + s.rejected_hits;
      }).toBeGreaterThan(before.admitted_hits + before.rejected_hits);
      const after = await snapshot(page);
      if (after.rejected_hits > before.rejected_hits) {
        refusals += 1;
        // The one rejection template, naming the nodes that filled the window,
        // among the sentences said since the press.
        const said = await recorded(page, 'narrations');
        const narration = said.find((t) => / rejected all 25/.test(t)) ?? said.join(' | ');
        expect(narration, `${where}: the rejection`).toMatch(/^node \d+ rejected all 25: it sees 100/);
        if (refusals === 1) expect(narration, `${where}: who filled the window`).toContain('admitted at nodes 0, 1 and 2');
      } else if (pressed.includes('Play')) {
        // The bucket before the press: the Send decides in it or a later one,
        // and the page waits past the last Send's bucket before naming Send,
        // so a playing clock cannot make two readings meet.
        paced.push(before.bucket_epoch_now);
      }
    } else if (label.startsWith('Select node')) {
      await expect(page.locator('.pane-title [data-field="node"]')).toHaveText(label.replace('Select node ', ''));
    }
  }
  // Paused, the presses are the script's own; once Play runs the clock, the
  // sliding-window act asks for a Send in each new bucket until one is
  // refused (four in the window, one more on a slow runner whose checks let
  // the oldest leave first), then for the Send after the fall-off.
  const played = pressed.indexOf('Play') + 1;
  expect(pressed.slice(0, played), `${info.project.name}: the presses while paused`).toEqual([
    'Send',
    'Select node 1',
    'Send',
    'Select node 2',
    'Send',
    'Send',
    'Send',
    'Play',
  ]);
  const after = pressed.slice(played);
  expect(after.at(-1), `${info.project.name}: the end`).toBe('Go to Zone partition');
  expect(after.slice(0, -1), `${info.project.name}: the presses after Play`).toEqual(after.slice(0, -1).map(() => 'Send'));
  expect(after.length - 1, `${info.project.name}: four admitted, one refused, one after the fall-off`).toBeGreaterThanOrEqual(6);
  expect(refusals, 'two refusals: at the limit and in the sliding window').toBe(2);
  // A46: the readout said while a paused Send's run bounded it, and while its
  // packets flew.
  const readouts = await recorded(page, 'readouts');
  expect(readouts.some((t) => t.endsWith('simulated · playing until every node has it · packets in flight')), `the readouts: ${readouts.slice(0, 6).join(' | ')}`).toBe(true);
  expect(readouts.some((t) => t.endsWith('simulated · playing until every node has it')), 'a bounded run between its waves').toBe(true);
  // A18: the heading opens on rl-0; every Send's spreading (rl-1) ends in
  // agreement, with the badge (rl-2, two hops: the flush and the relays) or
  // the countdown (rl-3) where the selected node now refuses; the window's
  // sliding shows the counts it leaves (rl-5) or an empty window (rl-4).
  const templates = await page.evaluate(() => (window as unknown as { templates: [string, string][] }).templates);
  console.log(`${info.project.name}: ${templates.map(([id]) => id).join(' ')}`);
  const ids = templates.map(([id]) => id).filter((id) => id !== '');
  expect(ids[0], 'the heading at load').toBe('rl-0');
  for (const [i, id] of ids.entries()) {
    if (id === 'rl-1' && i + 1 < ids.length) expect(['rl-2', 'rl-3'], `after ${ids.slice(0, i + 2).join(' ')}`).toContain(ids[i + 1]);
  }
  expect(ids.filter((id) => id === 'rl-1').length, 'one spreading per admitted Send').toBe(9);
  expect(ids.filter((id) => id === 'rl-3').length, 'the countdown at each limit').toBeGreaterThanOrEqual(2);
  expect(ids.some((id) => id === 'rl-4' || id === 'rl-5'), 'the window slid').toBe(true);
  expect(templates.some(([, text]) => text.includes('see 75 of the 100 limit')), 'the partial fall-off').toBe(true);
  expect(paced.length, 'A57: the Sends admitted after Play').toBeGreaterThanOrEqual(5);
  expect(new Set(paced).size, `A57: each Send after Play in a bucket of its own: ${paced}`).toBe(paced.length);
  // A paused Send's run is its own two waves: the flush and the relays. (A
  // Send while playing may share its run with other waves.)
  const paused = templates.slice(0, templates.findIndex(([id]) => id === 'rl-3'));
  for (const [id, text] of paused) if (id === 'rl-2') expect(text, 'the badge').toContain('agreed in 2 hops');
  // A57: the sliding-window act's Sends fell in buckets of their own: the
  // vessels hold their strata wherever the inside is tall enough to show them.
  const [strata, inner] = await page.locator('.stage canvas').evaluate((c: HTMLCanvasElement & { glyphs: GlyphTable }) => [
    Math.max(...c.glyphs.strata),
    2 * (c.glyphs.r[0] - 1),
  ]);
  if (inner >= STRATA_MIN_INNER) expect(strata, 'the sliding window left its strata').toBeGreaterThanOrEqual(2);

  // A6: the end hands over. Go to Zone partition loads it paused at its
  // start with node 0 selected; Undo restores the finished Rate limiting run.
  // Checked once; paused first, so the run Undo restores is the one read here.
  if (info.project.name !== 'audit-1440-light') return;
  await pauseIfPlaying(page);
  await still(page);
  const left = await snapshot(page);
  const selection = page.locator('.pane-title [data-field="node"]');
  const selected = squash(await selection.textContent());
  await primary(page).click();
  await expect(presetButton(page, 'Zone partition')).toHaveAttribute('aria-current', 'true');
  await expect(page.locator('.narration')).toContainText('Paused at the start of Zone partition');
  await expect(selection).toHaveText('0');
  await page.getByRole('button', { name: 'Undo' }).click();
  await expect(presetButton(page, 'Rate limiting')).toHaveAttribute('aria-current', 'true');
  await expect(selection).toHaveText(selected);
  const back = await snapshot(page);
  expect(back.virtual_ms, 'Undo: the same instant').toBe(left.virtual_ms);
  expect(back.nodes.map((n) => n.window_total), 'Undo: the same counts').toEqual(left.nodes.map((n) => n.window_total));
});

test('Propagation stops before each wave and Step flies it (A55)', async ({ page }, info) => {
  test.skip(!['audit-1440-light-motion', 'audit-1440-light', 'audit-400-dark'].includes(info.project.name), 'the waves, checked where they move and at the widest and narrowest');
  test.setTimeout(120_000);
  await open(page);
  await loadPreset(page, 'Propagation');
  await page.evaluate(() => {
    const w = window as unknown as { pushed: { src: number; dst: number; flags: number }[] };
    w.pushed = [];
    const p = (document.querySelector('.stage canvas') as HTMLCanvasElement & { beams: { push: (...a: unknown[]) => void } }).beams;
    const push = p.push.bind(p);
    p.push = (path, start, src, dst, flags) => {
      w.pushed.push({ src: src as number, dst: dst as number, flags: flags as number });
      push(path, start, src, dst, flags);
    };
  });
  const data = (): Promise<{ src: number; dst: number; flags: number }[]> =>
    page.evaluate((control) => {
      const w = window as unknown as { pushed: { src: number; dst: number; flags: number }[] };
      const out = w.pushed.filter((b) => (b.flags & control) === 0);
      w.pushed = [];
      return out;
    }, CONTROL);
  const where = info.project.name;
  expect(await checkState(page, `${where}: load`)).toMatch(/^Send/);
  // (1) Until the run pauses before wave 1, the line waits and fills nothing:
  // every state the page shows from the press to the first Next.
  await page.evaluate(() => {
    const w = window as unknown as { states: string[] };
    w.states = [];
    const take = (): void => {
      const state = `${(document.querySelector('#next-action .tag')?.textContent ?? '').trim()}|${document.querySelectorAll('[data-primary]').length}`;
      if (w.states.at(-1) !== state) w.states.push(state);
    };
    new MutationObserver(take).observe(document.body, { subtree: true, childList: true, characterData: true, attributes: true, attributeFilter: ['data-primary'] });
  });
  await primary(page).click();
  await untilNext(page, `${where}: before wave 1`);
  const states = await page.evaluate(() => (window as unknown as { states: string[] }).states);
  // From the first state that is not the Send's own Next to the first Next after it.
  const from = states.findIndex((s) => !s.startsWith('Next|'));
  const waits = from < 0 ? [] : states.slice(from, states.findIndex((s, i) => i > from && s.startsWith('Next|')));
  expect(waits.length, `${where}: the line waited before wave 1: ${states}`).toBeGreaterThan(0);
  expect(waits, `${where}: before wave 1`).toEqual(waits.map(() => 'Wait|0'));
  // (2) Step, to send wave 1.
  expect(await checkState(page, `${where}: wave 1 held`)).toBe('Step');
  await expect(line(page)).toContainText('to send wave 1');
  // A46: halted before the wave, the readout claims neither a playing run
  // nor packets in flight.
  await expect(page.locator('.readout .time')).toHaveText(/simulated$/);
  expect(await data(), 'nothing flew before Step').toEqual([]);
  await record(page, 'narrations', '.narration');
  // (3) Wave 1 alone: node 0's own packets, Z − 1 of them across zones.
  await primary(page).click();
  await untilNext(page, `${where}: before wave 2`);
  const wave1 = await data();
  expect(wave1.length, 'wave 1: node 0 to its zone and a gateway per other zone').toBe(5);
  expect(wave1.every((b) => b.src === 0 && (b.flags & RELAY) === 0)).toBe(true);
  expect(wave1.filter((b) => (b.flags & CROSS) !== 0).length).toBe(2);
  expect(await checkState(page, `${where}: wave 2 held`)).toBe('Step');
  await expect(line(page)).toContainText('for wave 2, the relays');
  const one = await recorded(page, 'narrations');
  expect(
    one.some((t) => /^node 0 sent them to 3 nodes of zone a and to gateways node \d+ \(zone b\) and node \d+ \(zone c\)$/.test(t)),
    `wave 1, told on its Step: ${one.join(' | ')}`,
  ).toBe(true);
  // (4) Wave 2: the relays, from node 0's gateways; then every count is the truth.
  const gateways = ((await snapshot(page, 0)).inspected?.peers ?? []).filter((p) => p.role === 'gateway').map((p) => p.id);
  await primary(page).click();
  await untilNext(page, `${where}: after the relays`);
  const wave2 = await data();
  expect(wave2.length, 'wave 2: the six relays').toBe(6);
  // Wave 2's sentence counts each data packet once: 5 in wave 1, 6 relays.
  const told = (await recorded(page, 'narrations')).find((t) => t.startsWith('gateways '));
  expect(told, 'wave 2, told on its Step').toMatch(/passed them on to 6 nodes · 2 of the 11 packets crossed a zone$/);
  expect(wave2.every((b) => (b.flags & RELAY) !== 0 && gateways.includes(b.src))).toBe(true);
  const agreed = await snapshot(page);
  expect(agreed.nodes.every((n) => n.window_total === agreed.oracle_total)).toBe(true);
  // (5) Step until the window empties, with no data packet.
  for (let i = 0; i < 6 && (await snapshot(page)).oracle_total > 0; i++) {
    expect(await checkState(page, `${where}: to the window's edge`)).toBe('Step');
    await primary(page).click();
    await still(page);
    await untilNext(page, `${where}: at the window's edge`);
  }
  expect((await snapshot(page)).nodes.every((n) => n.window_total === 0), 'the window emptied').toBe(true);
  expect(await data(), "the window's edge flew no data").toEqual([]);
  expect(await checkState(page, `${where}: the end`)).toBe('Go to Rate limiting');
});

test('a Send while a wave waits for Step lands it first (A55)', async ({ page }, info) => {
  test.skip(!['audit-1440-light-motion', 'audit-1440-light'].includes(info.project.name), 'the waiting wave, with motion on and reduced');
  test.setTimeout(60_000);
  await open(page);
  await loadPreset(page, 'Propagation');
  await primary(page).click();
  await untilNext(page, `${info.project.name}: before wave 1`);
  expect(await checkState(page, `${info.project.name}: wave 1 held`)).toBe('Step');
  // Send again instead: the waiting wave lands, then node 0 decides the
  // second request, and its run stops before its own first wave.
  const before = await snapshot(page);
  await sendButton(page).click();
  await expect
    .poll(async () => {
      const s = await snapshot(page);
      return s.admitted_hits + s.rejected_hits;
    }, { timeout: 15_000 })
    .toBeGreaterThan(before.admitted_hits + before.rejected_hits);
  await untilNext(page, `${info.project.name}: after the second Send`);
  expect(await checkState(page, `${info.project.name}: the second Send's wave held`)).toBe('Step');
  const held = await snapshot(page);
  // Node 0 and wave 1's five receivers hold the first Send.
  expect(held.nodes.filter((n) => n.window_total >= before.oracle_total).length, 'the first wave landed').toBeGreaterThanOrEqual(6);
});

test('Zone partition admits on each side, then counts all of it (A48)', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the partition story, checked once');
  test.setTimeout(240_000);
  await open(page);
  await loadPreset(page, 'Zone partition');
  const pressed: string[] = [];
  for (let step = 0; step < 30; step++) {
    const where = `Zone partition, state ${step}`;
    await untilNext(page, where);
    const label = await checkState(page, where);
    if (label === null) continue;
    pressed.push(label.startsWith('Send') ? 'Send' : label);
    if (label.startsWith('Reconnect')) {
      // Before the repair: both sides at the limit, 200 against 100.
      await expect(page.locator('[data-template]')).toContainText('· 200 admitted against the limit of 100');
      await primary(page).click();
      await expect(page.locator('.stage-pane')).toHaveAttribute('data-run', 'off', { timeout: 30_000 });
      await still(page);
      break;
    }
    await primary(page).click();
    await still(page);
  }
  expect(pressed).toEqual(['Send', 'Send', 'Send', 'Send', 'Send', 'Select node 2', 'Send', 'Send', 'Send', 'Send', 'Send', 'Reconnect zone c']);
  const healed = await snapshot(page);
  expect(healed.nodes.map((n) => n.window_total), 'every node counts both sides').toEqual(healed.nodes.map(() => 200));
  const over = await page.locator('.stage canvas').evaluate((c: HTMLCanvasElement & { glyphs: GlyphTable }) => {
    const g = c.glyphs;
    const out: boolean[] = [];
    for (let s = 0; s < g.len; s++) if (g.id[s] >= 0) out.push(g.fillTop[s] < g.brimY[s]);
    return out;
  });
  expect(over, 'every vessel draws over its brim').toEqual(healed.nodes.map(() => true));
});

test('Hits per Send is checked when committed, not while typed (A59, A50)', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the field, checked once');
  await open(page);
  const field = page.locator('#burst-hits');
  const send = sendButton(page);
  await field.click();
  await field.press('ControlOrMeta+a');
  for (const key of ['5', 'Backspace', '5', '0']) {
    await field.press(key);
    await expect(primary(page), `after ${key}`).toHaveCount(1);
    await expect(primary(page)).toHaveAttribute('id', 'burst-hits');
    await expect(page.getByRole('button', { name: /^Use \d+ hits/ })).toHaveCount(0);
    await expect(send).toBeEnabled();
  }
  await field.press('Enter');
  await expect(send).toHaveText(/Send a request of\s*50\s*hits/);
  await field.fill('');
  await field.blur();
  await expect(send).toHaveText('Send');
  await expect(send).toBeDisabled();
  const reason = page.locator('#send-reason');
  await expect(reason).toBeVisible();
  const [a, b] = [await send.boundingBox(), await reason.boundingBox()];
  expect(Math.max(b!.x - (a!.x + a!.width), a!.x - (b!.x + b!.width), b!.y - (a!.y + a!.height), a!.y - (b!.y + b!.height)), 'the reason stands beside Send').toBeLessThanOrEqual(48);
  const use = page.getByRole('button', { name: /^Use 25 hits/ });
  await expect(use).toHaveAttribute('data-primary', '');
  await use.click();
  await expect(send).toBeEnabled();
  await expect(send).toHaveText(/25\s*hits/);
  // Restart keeps a committed Hits per Send (A6).
  await field.fill('30');
  await field.press('Enter');
  await page.getByRole('button', { name: 'Restart Rate limiting' }).click();
  await expect(page.locator('.narration')).toContainText('Paused at the start of Rate limiting');
  await expect(send).toHaveText(/Send a request of\s*30\s*hits/);
  await expect(field).toHaveValue('30');
  // A request heavier than the limit is never admitted: the fix is Use 25
  // hits, until the reader types, when the field is the next action.
  await field.fill('200');
  await field.press('Enter');
  await send.click();
  await expect(page.getByRole('button', { name: /^Use 25 hits/ })).toHaveAttribute('data-primary', '');
  await field.press('Backspace');
  await expect(primary(page)).toHaveAttribute('id', 'burst-hits');
  await expect(page.getByRole('button', { name: /^Use \d+ hits/ })).toHaveCount(0);
});

test('while the simulator starts, the transport says so beside its controls (A50)', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the starting reason, checked once');
  // Hold the engine's module until the reason has been read.
  const { promise: release, resolve } = Promise.withResolvers<void>();
  await page.route('**/*.wasm', async (route) => {
    await release;
    await route.continue();
  });
  await page.goto('/');
  const reason = page.locator('.transport #transport-reason');
  await expect(reason).toHaveText('starting the simulator…');
  // One reason for the group, beside its last control and describing each.
  for (const button of await page.locator('.transport button').all()) {
    await expect(button).toBeDisabled();
    expect((await button.getAttribute('aria-describedby')) ?? '', 'the reason describes it').toContain('transport-reason');
  }
  const [a, b] = [await sendButton(page).boundingBox(), await reason.boundingBox()];
  expect(Math.max(b!.x - (a!.x + a!.width), a!.x - (b!.x + b!.width), b!.y - (a!.y + a!.height), a!.y - (b!.y + b!.height)), 'the reason stands beside the transport').toBeLessThanOrEqual(48);
  resolve();
  await expect(page.locator('.stage canvas')).toBeVisible({ timeout: 30_000 });
  await expect(reason).toHaveCount(0);
});

test('a rebuild that fails keeps the running cluster, and the active scenario is inert (A6)', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the rebuilds, checked once');
  await open(page);
  // A Send moves the run on, so a rebuild would show.
  await sendButton(page).click();
  await still(page);
  const before = await snapshot(page);
  expect(before.oracle_total).toBe(25);
  await presetButton(page, 'Rate limiting').click();
  await expect(page.locator('.overlay.rebuilding')).toHaveCount(0);
  const inert = await snapshot(page);
  expect([inert.virtual_ms, inert.oracle_total], 'pressing the active scenario rebuilt it').toEqual([before.virtual_ms, before.oracle_total]);
  // The next build fails: the running cluster stays, live, and says why.
  await page.evaluate(() => {
    (window.sim!.constructor as unknown as { create: () => Promise<never> }).create = () => Promise.reject(new Error('the test refused the build.'));
  });
  await page.getByRole('button', { name: 'Restart Rate limiting' }).click();
  await expect(page.getByText(/could not be rebuilt, so the running one stays/)).toBeVisible();
  const kept = await snapshot(page);
  expect([kept.virtual_ms, kept.oracle_total], 'the running cluster').toEqual([before.virtual_ms, before.oracle_total]);
  await expect(page.locator('.stage canvas')).toBeVisible();
  await sendButton(page).click();
  await expect.poll(async () => (await snapshot(page)).oracle_total).toBe(50);
});

test('the next step is reachable from the keyboard (A52, A21)', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the keyboard, checked once');
  await open(page);
  // The page opens with the scenario's node, node 0, selected (A12).
  await expect(page.locator('.pane-title [data-field="node"]')).toHaveText('0');
  await page.keyboard.press('Tab');
  await expect(page.locator(':focus')).toHaveText('Skip to the next step');
  await page.keyboard.press('Enter');
  await expect(page.locator(':focus')).toHaveAttribute('data-primary', '');
  const description = await page.locator(':focus').evaluate((el) => document.getElementById(el.getAttribute('aria-describedby')?.split(' ')[0] ?? '')?.textContent ?? '');
  expect(squash(description)).toBe(squash(await line(page).textContent()));
  // Space on a focused Send sends; it does not toggle Play.
  const before = (await snapshot(page)).admitted_hits;
  await page.keyboard.press('Space');
  await expect.poll(async () => (await snapshot(page)).admitted_hits).toBeGreaterThan(before);
  await still(page);
  await expect(page.getByRole('button', { name: 'Play' })).toBeVisible();
  // Space on the page toggles Play; Escape on the page returns the selection.
  await page.locator('[role="gridcell"][data-node="5"]').press('Enter');
  await expect(page.locator('.pane-title [data-field="node"]')).toHaveText('5');
  await page.evaluate(() => (document.activeElement as HTMLElement).blur());
  await page.keyboard.press('Escape');
  await expect(page.locator('.pane-title [data-field="node"]')).toHaveText('0');
  await page.keyboard.press('Space');
  await expect(page.getByRole('button', { name: 'Pause' })).toBeVisible();
  // A50: Step stays enabled while playing; it pauses and steps.
  await expect(page.getByRole('button', { name: 'Step to the next event' })).toBeEnabled();
  await page.keyboard.press('Space');
  await expect(page.getByRole('button', { name: 'Play' })).toBeVisible();
  // → on a select changes its option and not the time.
  const time = await page.locator('[data-field="virtual-time"]').getAttribute('data-ms');
  await page.locator('#cut-zone').focus();
  await page.keyboard.press('ArrowRight');
  await expect(page.locator('[data-field="virtual-time"]')).toHaveAttribute('data-ms', time ?? '');
});

test('what can be pressed looks pressable, and a named node is previewed (A51, owner note)', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the controls, checked once');
  await open(page);
  const found = await page.evaluate(() => {
    const out: string[] = [];
    const ctx = new OffscreenCanvas(1, 1).getContext('2d')!;
    const rgb = (css: string): number[] => {
      ctx.clearRect(0, 0, 1, 1);
      ctx.fillStyle = css;
      ctx.fillRect(0, 0, 1, 1);
      return [...ctx.getImageData(0, 0, 1, 1).data].slice(0, 3);
    };
    const lum = (c: number[]): number => {
      const [r, g, b] = c.map((v) => (v / 255 <= 0.04045 ? v / 255 / 12.92 : ((v / 255 + 0.055) / 1.055) ** 2.4));
      return 0.2126 * r + 0.7152 * g + 0.0722 * b;
    };
    const ratio = (a: number[], b: number[]): number => {
      const [hi, lo] = [lum(a), lum(b)].sort((x, y) => y - x);
      return (hi + 0.05) / (lo + 0.05);
    };
    const bg = (el: Element | null): string => {
      for (let e = el; e !== null; e = e.parentElement) {
        const c = getComputedStyle(e).backgroundColor;
        if (!c.endsWith(', 0)') && c !== 'transparent') return c;
      }
      return getComputedStyle(document.documentElement).backgroundColor;
    };
    for (const el of document.querySelectorAll<HTMLElement>('[role="button"]')) {
      if (el.tagName !== 'BUTTON' && !(el.tagName === 'A' && el.hasAttribute('href'))) out.push(`${el.tagName} acts as a button`);
    }
    for (const b of document.querySelectorAll<HTMLButtonElement>('button')) {
      if (!b.checkVisibility() || b.disabled) continue;
      const style = getComputedStyle(b);
      if (style.textDecorationLine.includes('underline') && style.textDecorationStyle === 'solid') out.push(`"${b.textContent?.trim()}" is underlined`);
      if (b.hasAttribute('data-primary') || b.matches('.btn-tertiary, .term, .preset, .skip-link')) continue;
      const border = rgb(style.borderTopColor);
      if (parseFloat(style.borderTopWidth) < 1 || ratio(border, rgb(bg(b))) < 3 || ratio(border, rgb(bg(b.parentElement))) < 3) {
        out.push(`"${b.textContent?.trim()}" has no 3:1 boundary`);
      }
    }
    return out;
  });
  expect(found).toEqual([]);
  await expect(page.getByRole('button', { name: 'Restart Rate limiting' })).toBeVisible();
  const glyphs = (): Promise<{ preview: number; id: number; previewDash: number }> =>
    page.locator('.stage canvas').evaluate((c: HTMLCanvasElement & { glyphs: GlyphTable }) => ({
      preview: c.glyphs.preview,
      id: c.glyphs.preview < 0 ? -1 : c.glyphs.id[c.glyphs.preview],
      previewDash: c.glyphs.previewDash.length,
    }));
  expect((await glyphs()).previewDash, 'the preview is dashed').toBeGreaterThan(0);
  // Hovering a disc previews it; leaving clears it.
  const box = (await page.locator('[role="gridcell"][data-node="7"]').boundingBox())!;
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
  await expect.poll(async () => (await glyphs()).id).toBe(7);
  await page.mouse.move(0, 0);
  await expect.poll(async () => (await glyphs()).preview).toBe(-1);
  // Every control that names a node previews that node: the rail's gateway
  // buttons, under a heading naming whose gateways they are, and the pane's
  // Remove, also after the cluster grows to 24 nodes.
  const check = async (): Promise<void> => {
    await expect(page.getByRole('heading', { name: /^node \d+'s gateways$/ })).toBeVisible();
    const named = page.locator('.rail').getByRole('button', { name: /^(Silence node|Cut node) \d+/ });
    for (let i = 0; i < (await named.count()); i++) {
      const button = named.nth(i);
      const id = Number(await button.locator('[data-field="node"]').textContent());
      await button.hover();
      await expect.poll(async () => (await glyphs()).id, `"${squash(await button.textContent())}" previews node ${id}`).toBe(id);
    }
    const remove = page.locator('.pane-foot').getByRole('button', { name: /^Remove node/ });
    await remove.hover();
    const id = Number(await remove.locator('[data-field="node"]').textContent());
    expect(id).toBe(Number(await page.locator('.pane-title [data-field="node"]').textContent()));
    // Remove names the selection, whose solid outline already marks it: two
    // frames on, no other node is previewed.
    await page.evaluate(() => new Promise((done) => requestAnimationFrame(() => requestAnimationFrame(done))));
    expect((await glyphs()).preview, `"${squash(await remove.textContent())}" previews another node`).toBe(-1);
    await page.mouse.move(0, 0);
  };
  await check();
  for (let i = 0; i < 12; i++) {
    await page.getByRole('button', { name: 'Add node' }).click();
    await expect(page.locator('[data-field="status-nodes"]')).toHaveText(String(13 + i));
  }
  await check();
});

test('every watched scenario and Sandbox names its next action, at load and once playing (A49)', async ({ page }, info) => {
  test.skip(!['audit-1440-light', 'audit-400-dark'].includes(info.project.name), 'the other scenarios, at the widest and narrowest');
  test.setTimeout(180_000);
  await open(page);
  for (const preset of PRESETS) {
    const story = preset.story;
    if (story.kind !== 'watch' && story.kind !== 'sandbox') continue;
    const where = `${info.project.name}, ${preset.label}`;
    await loadPreset(page, preset.label);
    if (story.kind === 'sandbox') {
      expect(await checkState(page, `${where}: load`)).toMatch(/^Send/);
      continue;
    }
    expect(await checkState(page, `${where}: load`)).toBe('Play');
    await primary(page).click();
    // Once playing, between the waits for its waves, the line watches what
    // this scenario shows and offers the next one, read in one go.
    const deadline = Date.now() + 30_000;
    let said = squash(await line(page).textContent());
    while (!said.startsWith('Watch')) {
      if (Date.now() > deadline) throw new Error(`${where}: still "${said}"`);
      await page.waitForTimeout(100);
      said = squash(await line(page).textContent());
    }
    expect(said, `${where}: the watch`).toBe(`Watch ${WATCHES[story.watched]} Go to ${presetById(story.next).label}`);
    expect(await checkState(page, `${where}: playing`)).toBeNull();
  }
});

test('the page says what it is (A20, A22)', async ({ page }, info) => {
  await open(page);
  const wide = (page.viewportSize()?.width ?? 0) >= 1100;
  if (wide) {
    const header = (await page.locator('header').boundingBox())!;
    expect(header.height, `${info.project.name}: the header is one row`).toBeLessThanOrEqual(56);
  }
  // The longest lede, with its cut listed after a spaced separator, keeps
  // the header one row.
  await loadPreset(page, 'Gateway cut');
  await expect(page.locator('.lede')).toContainText(/share one rate limit · node \d+ unheard by zone b/);
  if (wide) {
    const header = (await page.locator('header').boundingBox())!;
    expect(header.height, `${info.project.name}: the header with a cut is one row`).toBeLessThanOrEqual(56);
  }
  await loadPreset(page, 'Rate limiting');
  expect(await page.title()).toBe('Gabion rate limiter');
  await expect(page.locator('h1')).toHaveText('Gabion · a distributed rate limiter');
  const visible = await page.evaluate(() => document.body.innerText);
  expect(visible).not.toMatch(/Gossip Visualizer/);
  await page.getByRole('button', { name: 'How to read this' }).click();
  const help = await page.evaluate(() => document.body.innerText);
  expect(help, 'no visible "origin"').not.toMatch(/\borigin/i);
  const outside = await page.evaluate(() => {
    const vh = innerHeight;
    return [...document.querySelectorAll<HTMLElement>('.lede, .scenario, [data-template]')]
      .filter((el) => el.getBoundingClientRect().top < vh)
      .map((el) => el.textContent ?? '')
      .join(' ');
  });
  expect(outside, '"rate limit" in the first viewport').toMatch(/rate limit/);
});

test('the pane is one column that never swaps (A5)', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the pane, checked once');
  await open(page);
  const order = (): Promise<string[]> =>
    page.locator('.rail-body').evaluate((body) =>
      [...body.querySelectorAll(':scope > .pane-title, .dashboard h3, .inspector .section-head, :scope > .pane-foot')].map((el) => (el.textContent ?? '').replace(/\s+/g, ' ').trim()),
    );
  const before = await order();
  expect(before[0]).toMatch(/^node 0 selected · zone a$/);
  expect(before.slice(1, 5)).toEqual(['Aggregate vs. limit', 'Share of data bytes that crossed zones', 'Agreement by zone', 'Disagreement by zone']);
  expect(before.at(-1)).toMatch(/^Remove node 0$/);
  await page.locator('[role="gridcell"][data-node="4"]').press('Enter');
  await expect(page.locator('.pane-title [data-field="node"]')).toHaveText('4');
  const after = await order();
  expect(after.slice(1, -1), 'selecting another node swaps nothing').toEqual(before.slice(1, -1));
});
