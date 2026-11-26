import { expect, test, type Locator, type Page } from '@playwright/test';
import type { BeamPool, GlyphTable } from '../src/lib/stage/renderer';
import { CROSS, RELAY, STRATA_MIN_INNER } from '../src/lib/stage/layout';
import { readTheme } from '../src/lib/theme';
import { breakGateway, loadPreset, openControls, pauseIfPlaying, recordData, send, sendButton, setRange, setSpeed, snapshot, step, still } from './drive';
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

async function open(page: Page): Promise<void> {
  await page.goto('/');
  await expect(page.locator('.stage canvas')).toBeVisible({ timeout: 30_000 });
  await openControls(page);
}

/** Scenario `name`'s button in the preset list. */
const presetButton = (page: Page, name: string): Locator =>
  page.getByRole('group', { name: 'Scenario preset' }).getByRole('button', { name, exact: true });

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
      // A58: one control per action: no two visible, enabled buttons share
      // an accessible name.
      const names = new Set<string>();
      for (const b of document.querySelectorAll<HTMLButtonElement>('button')) {
        if (!b.checkVisibility() || b.disabled) continue;
        const name = (b.getAttribute('aria-label') ?? b.textContent ?? '').replace(/\s+/g, ' ').trim();
        if (names.has(name)) out.push(`two buttons are named "${name}"`);
        names.add(name);
      }
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
  let admitted = 0;
  // A57: the buckets the Sends admitted after Play were pressed in.
  const paced: number[] = [];
  // A57: the most strata a vessel held once Play ran the clock, read as each
  // state settles, while the act's Sends are still in the window.
  const strataOf = (): Promise<number> =>
    page.locator('.stage canvas').evaluate((c: HTMLCanvasElement & { glyphs: GlyphTable }) => Math.max(...c.glyphs.strata));
  let strata = 0;
  for (let step = 0; step < 60; step++) {
    const where = `${info.project.name}, state ${step}`;
    await untilNext(page, where);
    if (pressed.includes('Play')) strata = Math.max(strata, await strataOf());
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
        const narration = said.find((t) => / rejected all 25 requests/.test(t)) ?? said.join(' | ');
        expect(narration, `${where}: the rejection`).toMatch(/^node \d+ rejected all 25 requests: it sees 100/);
        if (refusals === 1) expect(narration, `${where}: who filled the window`).toContain('admitted at nodes 0, 1 and 2');
      } else {
        admitted += 1;
        // The bucket before the press: the Send decides in it or a later one,
        // and the page waits past the last Send's bucket before naming Send,
        // so a playing clock cannot make two readings meet.
        if (pressed.includes('Play')) paced.push(before.bucket_epoch_now);
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
  // A46: the readout said while a paused Send's run bounded it.
  const readouts = await recorded(page, 'readouts');
  expect(readouts.some((t) => t.endsWith('simulated · playing until every node has it')), `the readouts: ${readouts.slice(0, 6).join(' | ')}`).toBe(true);
  // A18: the heading opens on rl-0; every Send's spreading (rl-1) ends in
  // agreement, with the badge (rl-2, two hops: the flush and the relays) or
  // the countdown (rl-3) where the selected node now refuses; the window's
  // sliding shows the counts it leaves (rl-5) or an empty window (rl-4). Once
  // Play runs the clock, a spreading can agree in the frame hits leave the
  // window, which drops the badge (rl-5); the paused Sends' cannot.
  const templates = await page.evaluate(() => (window as unknown as { templates: [string, string][] }).templates);
  console.log(`${info.project.name}: ${templates.map(([id]) => id).join(' ')}`);
  const ids = templates.map(([id]) => id).filter((id) => id !== '');
  expect(ids.slice(0, 7), 'the heading at load and over the first three Sends').toEqual(['rl-0', 'rl-1', 'rl-2', 'rl-1', 'rl-2', 'rl-1', 'rl-2']);
  for (const [i, id] of ids.entries()) {
    if (id === 'rl-1' && i + 1 < ids.length) expect(['rl-2', 'rl-3', 'rl-5'], `after ${ids.slice(0, i + 2).join(' ')}`).toContain(ids[i + 1]);
  }
  // How many Sends the playing clock admits before the refusal depends on the
  // runner's pace, so each is counted as the walk saw it decided.
  expect(ids.filter((id) => id === 'rl-1').length, 'one spreading per admitted Send').toBe(admitted);
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
  // vessels held their strata wherever the inside is tall enough to show them.
  // Read as the act went, not at its end, where a slow runner's playing clock
  // has let all but the last bucket leave.
  const inner = await page.locator('.stage canvas').evaluate((c: HTMLCanvasElement & { glyphs: GlyphTable }) => 2 * (c.glyphs.r[0] - 1));
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

test('Propagation stops before each wave and Step draws it (A55)', async ({ page }, info) => {
  test.skip(!['audit-1440-light-motion', 'audit-1440-light', 'audit-400-dark'].includes(info.project.name), 'the waves, checked where they move and at the widest and narrowest');
  test.setTimeout(120_000);
  await open(page);
  await loadPreset(page, 'Propagation');
  const data = await recordData(page);
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
  // A46: halted before the wave, the readout claims no playing run.
  await expect(page.locator('.readout .time')).toHaveText(/simulated$/);
  expect(await data(), 'nothing was drawn before Step').toEqual([]);
  await record(page, 'narrations', '.narration');
  // (3) Wave 1 alone: node 0's own packets, Z − 1 of them across zones.
  await primary(page).click();
  await untilNext(page, `${where}: before wave 2`);
  // Wave 1's lines drew with their counts, and the run played on by itself to
  // the relays, held for Step.
  await expect(page.locator('.stage-pane'), `${where}: the relays wait for Step`).toHaveAttribute('data-clock', 'before-wave');
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
  expect(await data(), "the window's edge drew no data").toEqual([]);
  expect(await checkState(page, `${where}: the end`)).toBe('Go to Rate limiting');
});

test('a Send while a wave waits for Step sends it first (A55)', async ({ page }, info) => {
  test.skip(!['audit-1440-light-motion', 'audit-1440-light'].includes(info.project.name), 'the waiting wave, with motion on and reduced');
  test.setTimeout(60_000);
  await open(page);
  await loadPreset(page, 'Propagation');
  await primary(page).click();
  await untilNext(page, `${info.project.name}: before wave 1`);
  expect(await checkState(page, `${info.project.name}: wave 1 held`)).toBe('Step');
  // Send again instead: the waiting wave is sent, drawn with its counts, then
  // node 0 decides the second request, and its run stops before its own first
  // wave.
  const data = await recordData(page);
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
  expect(held.nodes.filter((n) => n.window_total >= before.oracle_total).length, 'the first wave was delivered').toBeGreaterThanOrEqual(6);
  // The page drew it: wave 1's five lines from node 0, and no line of the
  // second Send's wave, which waits for Step.
  const wave1 = (await data()).filter((b) => b.src === 0 && (b.flags & RELAY) === 0);
  expect(wave1.length, 'the waiting wave was drawn: node 0 to its zone and a gateway per other zone').toBe(5);
  expect(wave1.filter((b) => (b.flags & CROSS) !== 0).length, 'two of its lines cross a zone').toBe(2);
});

test('a cut while a wave waits for Step sends it, and the run goes on (A55, A49)', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the waiting wave under a command, checked once');
  test.setTimeout(120_000);
  await open(page);
  await loadPreset(page, 'Propagation');
  await setSpeed(page, 4);
  // Faults are offered once the story has ended: play it to its Go, pressing
  // each primary the line names.
  await primary(page).click();
  for (let presses = 0; ; presses++) {
    await untilNext(page, `Propagation, press ${presses}`);
    const label = await checkState(page, `Propagation, press ${presses}`);
    if (label === 'Go to Rate limiting') break;
    expect(presses, 'Propagation reached its end').toBeLessThan(20);
    await primary(page).click();
  }
  await sendButton(page).click();
  await untilNext(page, 'before wave 1');
  expect(await checkState(page, 'wave 1 held')).toBe('Step');
  await record(page, 'narrations', '.narration');
  await page.locator('#cut-zone').selectOption({ label: 'zone c' });
  await page.getByRole('button', { name: 'Cut zone c off' }).click();
  // The cut sends the waiting wave first, and says so; the run then goes on
  // to stop before its next wave, or pauses itself once every node it can
  // reach has the hits. Either way the line names a press again.
  await untilNext(page, 'after the cut', 30_000);
  expect(await checkState(page, 'after the cut')).toBe('Step');
  expect(await recorded(page, 'narrations'), 'the cut over the waiting wave').toContain(
    'zone c is cut off · the wave waiting for Step was sent first',
  );
});

test('a run that reaches its cap stops there, and Play plays on (A49, A46)', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the cap, checked once');
  test.setTimeout(120_000);
  await open(page);
  await loadPreset(page, 'Sandbox');
  // A 50 ms heartbeat puts the cap, 16 heartbeats, at 800 ms: well inside
  // gabion's 2 s floor on suspecting a silent gateway, so the zone behind it
  // is still behind when the run reaches its cap.
  await page.getByText('Tune the cluster').click();
  await setRange(page.locator('#knob-gossip'), 50);
  await page.getByRole('button', { name: 'Rebuild cluster' }).click();
  await expect(page.locator('.overlay.rebuilding')).toHaveCount(0);
  await expect(page.locator('.tune summary')).not.toContainText('staged');
  await page.locator('.inspector details.faults summary').click();
  await page.locator('.inspector button', { hasText: /^Silence node \d+$/ }).first().click();
  await expect(page.locator('[data-field="status-cut"]')).toHaveCount(1);
  await setSpeed(page, 4);
  const pane = page.locator('.stage-pane');
  expect(await checkState(page, 'silenced')).toMatch(/^Send/);
  await primary(page).click();
  await expect(pane).toHaveAttribute('data-clock', 'capped', { timeout: 60_000 });
  await expect(page.locator('.narration')).toHaveText(/^paused after 16 heartbeats: node \d+ is still behind$/);
  await expect(pane).toHaveAttribute('data-playing', '0');
  expect(await checkState(page, 'capped')).toBe('Play');
  await primary(page).click();
  await expect(pane).toHaveAttribute('data-clock', 'playing');
  await expect(pane).toHaveAttribute('data-playing', '1');
  expect(await checkState(page, 'playing after the cap')).toMatch(/^Send/);
  await pauseIfPlaying(page);
});

test('a scenario change pauses, and says so once over a held wave (A46)', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the scenario change, checked once');
  test.setTimeout(60_000);
  await open(page);
  const pane = page.locator('.stage-pane');
  const narration = page.locator('.narration');
  // While playing: the change pauses at the new scenario's start.
  await page.getByRole('button', { name: 'Play' }).click();
  await expect(pane).toHaveAttribute('data-clock', 'playing');
  await loadPreset(page, 'Propagation');
  await expect(pane).toHaveAttribute('data-clock', 'paused');
  await expect(pane).toHaveAttribute('data-playing', '0');
  await expect(narration).toHaveText('Paused at the start of Propagation');
  // While a wave is held: the change discards it, and the live region
  // changes once, to the change's own sentence.
  await primary(page).click();
  await untilNext(page, 'wave 1 held');
  await expect(pane).toHaveAttribute('data-clock', 'before-wave');
  await record(page, 'said', '.narration');
  await loadPreset(page, 'Sandbox');
  await expect(narration).toHaveText('Paused at the start of Sandbox');
  await expect(pane).toHaveAttribute('data-clock', 'paused');
  const said = await recorded(page, 'said');
  expect(said.slice(1), 'one mutation over the held wave').toEqual(['Paused at the start of Sandbox']);
});

/** Every `<button>` or `<summary>` outside the action line whose name offers
 *  a fault or a heal: none may exist before a story's end (A58). */
const faultsOutsideLine = (page: Page): Promise<string[]> =>
  page.evaluate(() =>
    [...document.querySelectorAll<HTMLElement>('button, summary')]
      .filter((el) => el.closest('#next-action') === null)
      .map((el) => (el.textContent ?? '').replace(/\s+/g, ' ').trim())
      .filter((name) => /Silence|Cut node|Cut zone|Break one of|^Heal\b/.test(name)),
  );

// Each gateway story: healthy at load with Send the primary and no fault
// anywhere; after the first Send's run the caption's fault is the one filled
// control; its press breaks the gateway, draws its mark, brings node 0 its
// next burst and runs with nothing filled through the failover; the ending's
// primary is Go, the heading's Reconnect is secondary, and the pane offers
// node 0's faults again (A45, A58, A43).
test('each gateway story runs from its Send to its fault to the failover (A45, A58)', async ({ page }, info) => {
  test.skip(!['audit-1440-light', 'audit-400-dark'].includes(info.project.name), 'the gateway stories, at the widest and narrowest');
  test.setTimeout(240_000);
  await open(page);
  await setSpeed(page, 4);
  for (const [name, fault, mark, next] of [
    ['Gateway failure', /^Silence node (\d+)$/, 'silenced', 'Go to Gateway cut'],
    ['Gateway cut', /^Cut node (\d+) off from zone b$/, 'unheard', 'Go to Sandbox'],
  ] as const) {
    const where = `${info.project.name}, ${name}`;
    await loadPreset(page, name);
    expect(await checkState(page, `${where}: load`)).toMatch(/^Send/);
    expect(await faultsOutsideLine(page), `${where}: load`).toEqual([]);
    await expect(page.locator('#next-action button'), `${where}: no fault offered at load`).toHaveCount(0);
    await send(page, 0);
    const label = await checkState(page, `${where}: the offer`);
    const g = Number(fault.exec(label ?? '')?.[1]);
    expect(label, `${where}: the fault is the primary`).toMatch(fault);
    expect(await faultsOutsideLine(page), `${where}: the offer`).toEqual([]);
    const before = (await snapshot(page)).admitted_hits;
    await primary(page).click();
    // One command: the cut, drawn at once, and node 0's next burst.
    await expect
      .poll(() => page.locator('.stage canvas').evaluate((c: HTMLCanvasElement, kind) => JSON.parse(c.dataset.faults ?? '{}')[kind] ?? [], mark))
      .toEqual([g]);
    await expect.poll(async () => (await snapshot(page)).admitted_hits, `${where}: the press sent node 0's burst`).toBeGreaterThan(before);
    // Nothing filled and no other fault offered from the press to the
    // ending, where the pane offers node 0's faults again; then Go, once the
    // run has paused itself.
    const deadline = Date.now() + 120_000;
    for (;;) {
      if (Date.now() > deadline) throw new Error(`${where}: no ending: "${squash(await line(page).textContent())}"`);
      // Read in one task: the ending may come between two reads.
      const seen = await page.evaluate(() => ({
        ended: document.querySelector('.inspector details.faults') !== null,
        filled: document.querySelectorAll('[data-primary]').length,
        faults: [...document.querySelectorAll<HTMLElement>('button, summary')]
          .filter((el) => el.closest('#next-action') === null)
          .map((el) => (el.textContent ?? '').replace(/\s+/g, ' ').trim())
          .filter((name) => /Silence|Cut node|Cut zone|Break one of|^Heal\b/.test(name)),
      }));
      if (seen.ended) break;
      // A run that reached its cap before the failover offers Play.
      if (seen.filled > 0 && (await page.locator('.stage-pane').getAttribute('data-clock')) === 'capped') {
        expect(await checkState(page, `${where}: capped before the ending`)).toBe('Play');
        await primary(page).click();
        continue;
      }
      expect(seen.filled, `${where}: a control filled before the ending`).toBe(0);
      expect(seen.faults, `${where}: after the press`).toEqual([]);
      await page.waitForTimeout(100);
    }
    await still(page, 60_000);
    expect(await checkState(page, `${where}: the ending`)).toBe(next);
    const failovers = (await snapshot(page)).nodes.find((n) => n.id === 0)?.gateway_failovers ?? 0;
    expect(failovers, `${where}: node 0 failed over`).toBeGreaterThan(0);
    const repair = page.locator('.stage-heading .repair');
    await expect(repair, `${where}: the heading's repair`).toHaveText(`Reconnect node ${g}`);
    await expect(repair).toBeEnabled();
    await expect(repair).not.toHaveAttribute('data-primary');
  }
});

/** The fault marks the stage drew, by kind. */
const marks = (page: Page): Promise<Record<string, number[]>> =>
  page.locator('.stage canvas').evaluate((c: HTMLCanvasElement) => JSON.parse(c.dataset.faults ?? '{}'));

// Each cut is drawn where it happens and listed with its own Reconnect, which
// undoes it alone; Heal undoes the rest (A43). Add keeps its zone across
// presses, says what the next press does, and a join moves no existing node
// of its ring more than 360° ÷ (n + 1) (A44).
test('cuts are drawn, listed and reconnected one by one, and Add keeps its zone (A43, A44)', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the network and membership controls, checked once');
  test.setTimeout(120_000);
  await open(page);
  await loadPreset(page, 'Sandbox');
  const heal = page.getByRole('button', { name: /^Heal \d+ cuts?$/ });
  await expect(heal, 'nothing to heal').toBeDisabled();
  await page.locator('#cut-zone').selectOption({ label: 'zone c' });
  await page.getByRole('button', { name: 'Cut zone c off' }).click();
  await expect.poll(async () => (await marks(page)).zone).toEqual([2]);
  const faults = page.locator('.inspector details.faults');
  await faults.locator('summary').click();
  const silence = faults.getByRole('button', { name: /^Silence node \d+$/ }).first();
  const g = Number(/\d+/.exec((await silence.textContent()) ?? '')?.[0]);
  await silence.click();
  await expect.poll(async () => (await marks(page)).silenced).toEqual([g]);
  // The key names each fault mark drawn (A5), and a fault's disabled button
  // says why beside it (A50).
  const key = page.locator('.key:not(.sizer) li');
  await expect(key.filter({ hasText: 'zone cut off' })).toHaveCount(1);
  await expect(key.filter({ hasText: 'a silenced node' })).toHaveCount(1);
  await expect(silence).toBeDisabled();
  await expect(faults.locator('.reason').first()).toHaveText(`node ${g} is silenced`);
  const cuts = page.locator('.rail .cuts li');
  await expect(cuts).toHaveText([/^zone c cut off/, new RegExp(`^node ${g} silenced`)]);
  await expect(heal).toHaveText('Heal 2 cuts');
  // Reconnect zone c alone: the silenced node stays silenced.
  await page.locator('.rail .cuts').getByRole('button', { name: 'Reconnect zone c' }).click();
  await expect.poll(async () => (await marks(page)).zone).toEqual([]);
  expect((await marks(page)).silenced, 'the other cut stays drawn').toEqual([g]);
  await expect(key.filter({ hasText: 'zone cut off' }), 'a key item leaves with its mark').toHaveCount(0);
  await expect(cuts).toHaveText([new RegExp(`^node ${g} silenced`)]);
  await expect(page.locator('.lede')).toContainText(`node ${g} silenced`);
  await still(page, 60_000);
  // In the engine too: a Send reaches zone c again, and never node g.
  await send(page, 0);
  const sent = await snapshot(page);
  for (const n of sent.nodes) {
    if (n.id === g) expect(n.window_total, `node ${g} is still silenced`).toBe(0);
    else if (n.zone === 2) expect(n.window_total, `node ${n.id} in zone c is reconnected`).toBe(25);
  }
  await page.getByRole('button', { name: 'Heal 1 cut' }).click();
  await expect.poll(async () => (await marks(page)).silenced).toEqual([]);
  await expect(cuts).toHaveCount(0);
  await still(page, 60_000);

  // Add: every zone by default; a zone picked stays picked.
  const add = page.locator('.rail').getByRole('button', { name: /^Add a node to/ });
  await expect(add).toHaveText('Add a node to every zone');
  const sizes = async (): Promise<number[]> => (await snapshot(page)).zones.map((z) => z.live);
  /** Each node's angle round its zone ring, in degrees, as the stage placed it. */
  const angles = async (): Promise<Map<number, number>> =>
    new Map(await page.locator('.stage canvas').evaluate((c: HTMLCanvasElement & { glyphs: GlyphTable }) => {
      const rings = JSON.parse(c.dataset.rings ?? '[]') as { x: number; y: number }[];
      const g = c.glyphs;
      const out: [number, number][] = [];
      for (let s = 0; s < g.len; s++) {
        const cell = document.querySelector<HTMLElement>(`[role="gridcell"][data-node="${g.id[s]}"]`);
        const zone = Number(cell?.closest<HTMLElement>('[role="row"]')?.dataset.zone ?? -1);
        if (zone < 0) continue;
        out.push([g.id[s], (Math.atan2(g.y[s] - rings[zone].y, g.x[s] - rings[zone].x) * 180) / Math.PI]);
      }
      return out;
    }));
  await page.getByLabel('Add to').selectOption({ label: 'zone b' });
  for (let i = 0; i < 3; i++) {
    await expect(add).toHaveText('Add a node to zone b');
    const before = await angles();
    const n = (await sizes())[1];
    await add.click();
    await expect.poll(sizes).toEqual([4, 5 + i, 4]);
    await still(page, 60_000);
    const after = await angles();
    for (const [id, a] of before) {
      const zone = (await snapshot(page)).nodes.find((x) => x.id === id)?.zone;
      if (zone !== 1) continue;
      const moved = Math.abs((((after.get(id)! - a) % 360) + 540) % 360 - 180);
      expect(moved, `node ${id} moved ${moved}° on a join to ${n + 1}`).toBeLessThanOrEqual(360 / (n + 1) + 1e-6);
    }
  }
  await expect(page.getByLabel('Add to'), 'the pick stays').toHaveValue('1');
  await page.getByLabel('Add to').selectOption({ label: 'every zone' });
  await add.click();
  await expect.poll(sizes).toEqual([5, 8, 5]);
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
      await expect(page.locator('.stage-pane')).toHaveAttribute('data-clock', 'paused', { timeout: 30_000 });
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

test('Requests per Send is checked when committed, not while typed (A59, A50)', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the field, checked once');
  await open(page);
  const field = page.getByLabel('Requests per Send', { exact: true });
  await expect(field, 'the field is labelled Requests per Send').toHaveAttribute('id', 'burst-requests');
  const send = sendButton(page);
  await field.click();
  await field.press('ControlOrMeta+a');
  for (const key of ['5', 'Backspace', '5', '0']) {
    await field.press(key);
    await expect(primary(page), `after ${key}`).toHaveCount(1);
    await expect(primary(page)).toHaveAttribute('id', 'burst-requests');
    await expect(page.getByRole('button', { name: /^Use \d+ requests/ })).toHaveCount(0);
    await expect(send).toBeEnabled();
  }
  await field.press('Enter');
  await expect(send).toHaveText(/Send\s*50\s*requests/);
  await field.fill('');
  await field.blur();
  await expect(send).toHaveText('Send');
  await expect(send).toBeDisabled();
  const reason = page.locator('#send-reason');
  await expect(reason).toBeVisible();
  const [a, b] = [await send.boundingBox(), await reason.boundingBox()];
  expect(Math.max(b!.x - (a!.x + a!.width), a!.x - (b!.x + b!.width), b!.y - (a!.y + a!.height), a!.y - (b!.y + b!.height)), 'the reason stands beside Send').toBeLessThanOrEqual(48);
  const use = page.getByRole('button', { name: /^Use 25 requests/ });
  await expect(use).toHaveAttribute('data-primary', '');
  await use.click();
  await expect(send).toBeEnabled();
  await expect(send).toHaveText(/25\s*requests/);
  // Restart keeps a committed Requests per Send (A6).
  await field.fill('30');
  await field.press('Enter');
  await page.getByRole('button', { name: 'Restart Rate limiting' }).click();
  await expect(page.locator('.narration')).toContainText('Paused at the start of Rate limiting');
  await expect(send).toHaveText(/Send\s*30\s*requests/);
  await expect(field).toHaveValue('30');
});

test('a Send of 250 at limit 100 admits 100 and rejects 150 (A60)', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the burst, checked once');
  await open(page);
  const field = page.locator('#burst-requests');
  await field.fill('250');
  await field.press('Enter');
  const send = sendButton(page);
  await expect(send).toHaveText(/Send\s*250\s*requests to node\s*0/);
  // Recorded from the press: the decision text goes when the Send's run ends.
  await record(page, 'narrations', '.narration');
  await record(page, 'decisions', '.decision[data-node="0"]');
  await send.click();
  await expect.poll(() => page.evaluate(() => (window as unknown as { decisions: string[] }).decisions)).toContain('+100 admitted · 150 rejected');
  // Rate limiting has no feed, and the run has ended: nothing changes the
  // counts read below.
  await still(page);
  const said = await recorded(page, 'narrations');
  expect(
    said.some((t) => t.startsWith('node 0 admitted 100 of 250 requests and rejected 150: its window holds 100 of the 100 limit')),
    `the verdict states the split: ${said.join(' | ')}`,
  ).toBe(true);
  const snap = await snapshot(page);
  expect([snap.admitted_hits, snap.rejected_hits, snap.oracle_total]).toEqual([100, 150, 100]);
  expect(snap.nodes.map((n) => n.window_total)).toEqual(snap.nodes.map(() => 100));
  // After the first rejection the next action is Play (owner Q15).
  expect(await checkState(page, 'after the split')).toBe('Play');
});

/** What Play shows 300 ms of simulated time after it is pressed. */
interface Played {
  /** Whether a stage mark changed: a node's count, a new line, a glyph (arc,
   *  ring, fill, strata, cap, the heartbeat sweep, a refusal), the heading or
   *  a decision; or a wait's meter rose. */
  readonly changed: boolean;
  /** The narration, without its countdown. */
  readonly said: string;
  /** The enabled filled control's label, or null when none is filled. */
  readonly label: string | null;
  readonly wallMs: number;
  readonly ms: number;
}

/** Press Play and read the first frame at least 300 ms of simulated time
 *  later: a stage mark changed by then, or the narration says nothing is
 *  coming and quotes the filled control's label verbatim (A61). The marks are
 *  first read from a stage at rest, so a ring's sweep or a refusal that a
 *  Send left playing cannot pass for Play's. Simulated time, not wall time,
 *  so the outcome is the same however loaded the machine; the wall time is
 *  printed. The NEXT line, the narration and the time readout are not marks.
 *  Pauses again after. */
async function playChecks(page: Page, where: string): Promise<void> {
  await page.mouse.move(0, 0);
  const seen = await page.evaluate(
    () =>
      new Promise<Played>((resolve, reject) => {
        const canvas = document.querySelector('.stage canvas') as HTMLCanvasElement & {
          beams: BeamPool & { counted?: boolean };
          glyphs: GlyphTable;
        };
        // Lines pushed, so a fading line can never count as a change.
        const w = window as unknown as { arrivals: number };
        const pool = canvas.beams;
        if (pool.counted !== true) {
          pool.counted = true;
          w.arrivals = 0;
          const push = pool.push.bind(pool);
          pool.push = (path, sent, src, dst, flags) => {
            w.arrivals += 1;
            push(path, sent, src, dst, flags);
          };
        }
        const text = (el: Element | null): string => (el?.textContent ?? '').replace(/\s+/g, ' ').trim();
        // Each node's count, in grid order, '' while its disc hides it. A
        // count moved when it shows at both reads and differs: one shown or
        // hidden as the layout fits it is no hit arriving.
        const counts = (): string[] => [...document.querySelectorAll('[role="gridcell"]')].map((c) => text(c.querySelector('[data-field="node-view"]')));
        const moved = (a: string[], b: string[]): boolean => a.length === b.length && a.some((c, i) => c !== '' && b[i] !== '' && c !== b[i]);
        const sig = (): string => {
          const g = canvas.glyphs;
          const marks: number[] = [w.arrivals, g.sweep, g.bounce];
          for (let s = 0; s < g.len; s++) marks.push(g.arc[s], g.ring[s], g.fillTop[s], g.strata[s], g.cap[s]);
          const decisions = [...document.querySelectorAll('.decision')].map(text);
          return JSON.stringify([marks, decisions, text(document.querySelector('[data-template]'))]);
        };
        const ms = (): number => Number(document.querySelector<HTMLElement>('[data-field="virtual-time"]')?.dataset.ms);
        const meter = (): number => Number(document.querySelector('#next-action [role="progressbar"]')?.getAttribute('aria-valuenow') ?? -1);
        const play = [...document.querySelectorAll<HTMLButtonElement>('.transport button')].find((b) => b.textContent?.trim() === 'Play');
        if (play === undefined) {
          reject(new Error('no Play button'));
          return;
        }
        const timer = setTimeout(() => reject(new Error('the stage did not rest, or 300 ms of simulated time did not pass, in 15 s')), 15_000);
        // At rest: no refusal plays, and no mark or count moved since the
        // last frame.
        let before = sig();
        let shown = counts();
        const rest = (): void => {
          const [now, nowShown] = [sig(), counts()];
          if (now !== before || nowShown.join() !== shown.join() || canvas.glyphs.bounce >= 0) {
            [before, shown] = [now, nowShown];
            requestAnimationFrame(rest);
            return;
          }
          const from = ms();
          const t0 = performance.now();
          play.click();
          let firstMeter = -1;
          const frame = (): void => {
            const m = meter();
            if (firstMeter < 0) firstMeter = m;
            if (ms() < from + 300) {
              requestAnimationFrame(frame);
              return;
            }
            clearTimeout(timer);
            const primary = document.querySelector<HTMLElement>('[data-primary]');
            const enabled = primary !== null && !(primary as HTMLButtonElement).disabled;
            resolve({
              changed: sig() !== before || moved(counts(), shown) || (firstMeter >= 0 && m > firstMeter),
              said: text(document.querySelector('.narration [aria-live]')),
              label: enabled ? primary.innerText.replace(/\s+/g, ' ').trim() : null,
              wallMs: performance.now() - t0,
              ms: ms() - from,
            });
          };
          requestAnimationFrame(frame);
        };
        requestAnimationFrame(rest);
      }),
  );
  console.log(`${where}: read ${seen.ms} ms simulated, ${Math.round(seen.wallMs)} ms wall after Play`);
  const quoted = seen.label !== null && seen.said.includes('nothing is coming') && seen.said.includes(`“${seen.label}”`);
  expect(
    seen.changed || quoted,
    `${where}: Play changed no mark, and the narration "${seen.said}" does not say nothing is coming, quoting "${seen.label}"`,
  ).toBe(true);
  await pauseIfPlaying(page);
  await still(page);
}

/** Step while the run stands before a wave Step draws, at most `max` times. */
async function stepWaves(page: Page, max = 8): Promise<void> {
  for (let i = 0; i < max && (await page.locator('.stage-pane').getAttribute('data-clock')) === 'before-wave'; i++) await step(page);
}

test('Play is never silent (A61)', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light-motion', 'Play, checked once with motion on');
  test.setTimeout(180_000);
  await open(page);
  // Every scenario at load.
  for (const preset of PRESETS) {
    await loadPreset(page, preset.label);
    await playChecks(page, `${preset.label} at load`);
  }
  // Each story's settled states.
  const field = page.locator('#burst-requests');
  const fill = async (requests: number): Promise<void> => {
    await field.fill(String(requests));
    await field.press('Enter');
  };
  await loadPreset(page, 'Rate limiting');
  await send(page, 0);
  await expect(primary(page)).toHaveText('Select node 1');
  await playChecks(page, 'Rate limiting after the first Send');
  await fill(250);
  await page.getByRole('button', { name: 'Restart Rate limiting' }).click();
  await expect(page.locator('.narration')).toContainText('Paused at the start of Rate limiting');
  await send(page, 0);
  await expect(primary(page)).toHaveText('Play');
  await playChecks(page, 'Rate limiting after a Send of 250');

  await loadPreset(page, 'Propagation');
  await send(page, 0);
  await stepWaves(page);
  await expect(primary(page)).toHaveText('Step');
  expect((await snapshot(page)).nodes.every((n) => n.lag.kind === 'CaughtUp'), 'every node has the hits').toBe(true);
  await playChecks(page, "Propagation after its Send's waves");
  for (let i = 0; i < 6 && (await snapshot(page)).oracle_total > 0; i++) await step(page);
  await expect(primary(page)).toHaveText('Go to Rate limiting');
  await playChecks(page, 'Propagation at its end');

  await loadPreset(page, 'Zone partition');
  await fill(250);
  await send(page, 0);
  await expect(primary(page)).toHaveText(/^Select node \d+$/);
  await playChecks(page, 'Zone partition after a Send of 250');

  await loadPreset(page, 'Sandbox');
  await send(page, 0);
  await playChecks(page, 'Sandbox after a Send');
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

// A rebuild that fails while Propagation's run stands before a wave keeps the
// wave for Step: its engine still holds it, so the page never claims a plain
// pause over it.
test('a rebuild that fails over a held wave keeps it for Step (A6, A55)', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the rebuild, checked once');
  await open(page);
  await loadPreset(page, 'Propagation');
  await primary(page).click();
  await untilNext(page, 'wave 1 held');
  const pane = page.locator('.stage-pane');
  await expect(pane).toHaveAttribute('data-clock', 'before-wave');
  await page.evaluate(() => {
    (window.sim!.constructor as unknown as { create: () => Promise<never> }).create = () => Promise.reject(new Error('the test refused the build.'));
  });
  await page.getByRole('button', { name: 'Restart Propagation' }).click();
  await expect(page.getByText(/could not be rebuilt, so the running one stays/)).toBeVisible();
  await expect(pane).toHaveAttribute('data-clock', 'before-wave');
  expect(await checkState(page, 'wave 1 held, after the failed rebuild')).toBe('Step');
});

// A Step moves time, so the narration stops saying the run is paused at its
// start, and a scenario with a feed never hears that nothing is waiting, even
// at a Step whose window is still empty: the feed always has hits to send.
test('a Step in a fed scenario retires the start sentence and never says nothing waits', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the narration, checked once');
  await open(page);
  await loadPreset(page, 'Sustained overload');
  const narration = page.locator('.narration');
  await expect(narration).toHaveText('Paused at the start of Sustained overload');
  for (let i = 0; i < 3; i++) {
    await step(page);
    await expect(narration, `after Step ${i + 1}`).not.toContainText('Paused at the start');
    await expect(narration, `after Step ${i + 1}`).not.toContainText('nothing is waiting');
  }
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
  await page.locator('#add-zone').focus();
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
  // Every control that names a node previews that node: the pane's gateway
  // faults, under a summary naming whose gateways they are, and the pane's
  // Remove, also after the cluster grows to 24 nodes. Sandbox offers them.
  await loadPreset(page, 'Sandbox');
  const check = async (): Promise<void> => {
    const faults = page.locator('.inspector details.faults');
    await expect(faults.locator('summary')).toHaveText(/^Break one of node \d+'s gateways$/);
    if ((await faults.getAttribute('open')) === null) await faults.locator('summary').click();
    const named = faults.getByRole('button', { name: /^(Silence node|Cut node) \d+/ });
    for (let i = 0; i < (await named.count()); i++) {
      const button = named.nth(i);
      const id = Number(await button.locator('[data-field="fault"]').first().textContent());
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
  await page.getByLabel('Add to').selectOption({ label: 'zone a' });
  for (let i = 0; i < 12; i++) {
    await page.getByRole('button', { name: 'Add a node to zone a' }).click();
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
  await breakGateway(page, true);
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
      [...body.querySelectorAll('.pane-head > .pane-title, .charts h3, .inspector .section-head, .pane-foot')].map((el) => (el.textContent ?? '').replace(/\s+/g, ' ').trim()),
    );
  const before = await order();
  expect(before[0]).toMatch(/^node 0 selected · zone a$/);
  expect(before.slice(1, 4)).toEqual(['node 0’s window against the limit', 'Requests', 'Gossip targets']);
  expect(before.at(-1)).toMatch(/^Remove node 0$/);
  await page.locator('[role="gridcell"][data-node="4"]').press('Enter');
  await expect(page.locator('.pane-title [data-field="node"]')).toHaveText('4');
  const after = await order();
  // R1 follows the selection; nothing else changes, and nothing swaps.
  const scoped = (titles: string[]): string[] => titles.slice(1, -1).map((t) => t.replace(/^node \d+’s window/, 'the window'));
  expect(scoped(after), 'selecting another node swaps nothing').toEqual(scoped(before));
});
