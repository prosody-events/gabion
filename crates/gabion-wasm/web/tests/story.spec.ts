import { expect, test, type Locator, type Page } from '@playwright/test';
import type { ClusterState } from '../src/lib/sim/types';
import type { GlyphTable } from '../src/lib/stage/renderer';
import { CONTROL, CROSS, RELAY, STRATA_MIN_INNER } from '../src/lib/stage/layout';
import { readTheme } from '../src/lib/theme';
import { WATCH, still } from './drive';
import { COMMANDS } from './next-actions';

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

async function loadPreset(page: Page, name: string): Promise<void> {
  const preset = page.getByRole('group', { name: 'Scenario preset' }).getByRole('button', { name, exact: true });
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
    ({ commands }) => {
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
      // A56: the goal explains and never commands.
      const goal = (document.querySelector('.scenario')?.textContent ?? '').replace(/\s+/g, ' ').trim();
      for (const clause of goal.split(/[.;:,·]| and | then /).map((c) => c.trim()).filter((c) => c !== '')) {
        if ((commands as string[]).includes(clause.split(/\s+/)[0].toLowerCase())) out.push(`the goal commands: "${clause}"`);
      }
      // A19: the narration takes at most two lines where the stage has its
      // measure (from 1100 px; the narrower layouts come with C22 and C23).
      const narration = innerWidth >= 1100 ? document.querySelector('.narration') : null;
      if (narration !== null) {
        const range = document.createRange();
        range.selectNodeContents(narration);
        const lines = new Set([...range.getClientRects()].map((r) => Math.round(r.bottom)));
        if (lines.size > 2) out.push(`the narration takes ${lines.size} lines`);
      }
      return out;
    },
    { commands: [...COMMANDS] },
  );
  expect(found, where).toEqual([]);
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
  expect(lineText.includes(label), `${where}: the line "${lineText}" quotes "${label}"`).toBe(true);
  const name = await p.evaluate((el) => (el.getAttribute('aria-label') ?? el.textContent ?? '').replace(/\s+/g, ' ').trim());
  expect(name.startsWith(label.split(' ')[0]), `${where}: its accessible name "${name}" begins with its label`).toBe(true);
  expect(await line(page).getByRole('button').count(), `${where}: at most one button in the line`).toBeLessThanOrEqual(1);
  // A49.6: in view once the line is scrolled to the top, where the page does
  // not scroll (from 1100 px; the scrolling layouts' sticky bar comes with C22
  // and C23).
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
      expect(now, `${where}: the meter falls in "${clause}"`).toBeGreaterThanOrEqual(last);
      last = now;
    }
    await page.waitForTimeout(100);
  }
}

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
  await loadPreset(page, 'Rate limiting');
  // At 1×: the sliding-window act wants its Sends a bucket apart, and the
  // checks between presses then cost the story next to nothing in simulated
  // time.
  await recordTemplates(page);
  const pressed: string[] = [];
  let refusals = 0;
  for (let step = 0; step < 40; step++) {
    const where = `${info.project.name}, state ${step}`;
    await untilNext(page, where);
    const label = await checkState(page, where);
    if (label === null) continue;
    pressed.push(label.startsWith('Send') ? 'Send' : label);
    if (process.env.STORY_DEBUG) console.log(step, label, '|', squash(await page.locator('[data-template]').textContent()), '|', squash(await page.locator('.narration').textContent()), '|', squash(await line(page).textContent()));
    if (label.startsWith('Go to')) break;
    const before = await snapshot(page);
    await primary(page).click();
    if (label.startsWith('Send')) {
      await expect.poll(async () => {
        const s = await snapshot(page);
        return s.admitted_hits + s.rejected_hits;
      }).toBeGreaterThan(before.admitted_hits + before.rejected_hits);
      const after = await snapshot(page);
      if (after.rejected_hits > before.rejected_hits) {
        refusals += 1;
        // The one rejection template, naming the nodes that filled the window.
        const narration = squash(await page.locator('.narration').textContent());
        expect(narration, `${where}: the rejection`).toMatch(/^node \d+ rejected all 25: it sees 100/);
        if (refusals === 1) expect(narration, `${where}: who filled the window`).toContain('admitted at nodes 0, 1 and 2');
      }
    } else if (label.startsWith('Select node')) {
      await expect(page.locator('.pane-title [data-field="node"]')).toHaveText(label.replace('Select node ', ''));
    }
  }
  expect(pressed, `${info.project.name}: the presses`).toEqual([
    'Send',
    'Select node 1',
    'Send',
    'Select node 2',
    'Send',
    'Send',
    'Send',
    'Play',
    'Send',
    'Send',
    'Send',
    'Send',
    'Send',
    'Send',
    'Go to Zone partition',
  ]);
  expect(refusals, 'two refusals: at the limit and in the sliding window').toBe(2);
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
});

test('Propagation stops before each wave and Step flies it (A55, Q16)', async ({ page }, info) => {
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
  await primary(page).click();
  // (1) Until the run pauses before wave 1, the line waits and fills nothing.
  await expect(line(page).locator('.tag')).toHaveText(/Wait/);
  await expect(primary(page)).toHaveCount(0);
  await untilNext(page, `${where}: before wave 1`);
  // (2) Step, to send wave 1.
  expect(await checkState(page, `${where}: wave 1 held`)).toBe('Step');
  await expect(line(page)).toContainText('to send wave 1');
  expect(await data(), 'nothing flew before Step').toEqual([]);
  // (3) Wave 1 alone: node 0's own packets, Z − 1 of them across zones.
  await primary(page).click();
  await untilNext(page, `${where}: before wave 2`);
  const wave1 = await data();
  expect(wave1.length, 'wave 1: node 0 to its zone and a gateway per other zone').toBe(5);
  expect(wave1.every((b) => b.src === 0 && (b.flags & RELAY) === 0)).toBe(true);
  expect(wave1.filter((b) => (b.flags & CROSS) !== 0).length).toBe(2);
  expect(await checkState(page, `${where}: wave 2 held`)).toBe('Step');
  await expect(line(page)).toContainText('for wave 2, the relays');
  // (4) Wave 2: the relays, from node 0's gateways; then every count is the truth.
  const gateways = ((await snapshot(page, 0)).inspected?.peers ?? []).filter((p) => p.role === 'gateway').map((p) => p.id);
  await primary(page).click();
  await untilNext(page, `${where}: after the relays`);
  const wave2 = await data();
  expect(wave2.length, 'wave 2: the six relays').toBe(6);
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
  expect(over.every(Boolean), 'every vessel draws over its brim').toBe(true);
});

test('Hits per Send is checked when committed, not while typed (A59, A50)', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the field, checked once');
  await open(page);
  const field = page.locator('#burst-hits');
  const send = page.locator('.transport').getByRole('button', { name: /^Send/ });
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
});

test('the next step is reachable from the keyboard (A52, A21)', async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the keyboard, checked once');
  await open(page);
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
        if (rgb(c).length > 0 && !c.endsWith(', 0)') && c !== 'transparent') return c;
      }
      return getComputedStyle(document.documentElement).backgroundColor;
    };
    for (const el of document.querySelectorAll<HTMLElement>('[role="button"], [onclick]')) {
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
  const glyphs = (): Promise<{ preview: number; id: number; previewDash: number; selectionDash: number }> =>
    page.locator('.stage canvas').evaluate((c: HTMLCanvasElement & { glyphs: GlyphTable }) => ({
      preview: c.glyphs.preview,
      id: c.glyphs.preview < 0 ? -1 : c.glyphs.id[c.glyphs.preview],
      previewDash: c.glyphs.previewDash.length,
      selectionDash: c.glyphs.selectionDash.length,
    }));
  expect((await glyphs()).previewDash, 'the preview is dashed').toBeGreaterThan(0);
  expect((await glyphs()).selectionDash, 'the selection is solid').toBe(0);
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
  };
  await check();
  for (let i = 0; i < 12; i++) {
    await page.getByRole('button', { name: 'Add node' }).click();
    await expect(page.locator('[data-field="status-nodes"]')).toHaveText(String(13 + i));
  }
  await check();
});

test('the page says what it is (A20, Q13, A22)', async ({ page }, info) => {
  await open(page);
  if ((page.viewportSize()?.width ?? 0) >= 1100) {
    const header = (await page.locator('header').boundingBox())!;
    expect(header.height, `${info.project.name}: the header is one row`).toBeLessThanOrEqual(56);
  }
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

test("a story's end hands over, and Undo brings it back (C55)", async ({ page }, info) => {
  test.skip(info.project.name !== 'audit-1440-light', 'the hand-over, checked once');
  test.setTimeout(120_000);
  await open(page);
  await loadPreset(page, 'Propagation');
  // Propagation's story told quickly: a Send, then Step to the window's edge.
  await primary(page).click();
  for (let i = 0; i < 12 && !(await primary(page).textContent())?.startsWith('Go to'); i++) {
    await untilNext(page, 'Propagation');
    if ((await primary(page).textContent())?.startsWith('Go to')) break;
    await primary(page).click();
    await still(page);
  }
  await expect(primary(page)).toHaveText(/^Go to Rate limiting/);
  const left = await snapshot(page);
  await primary(page).click();
  await expect(page.getByRole('group', { name: 'Scenario preset' }).getByRole('button', { name: 'Rate limiting', exact: true })).toHaveAttribute('aria-current', 'true');
  await expect(page.locator('.narration')).toContainText('Paused at the start of Rate limiting');
  await expect(page.locator('.pane-title [data-field="node"]')).toHaveText('0');
  await page.getByRole('button', { name: 'Undo' }).click();
  await expect(page.getByRole('group', { name: 'Scenario preset' }).getByRole('button', { name: 'Propagation', exact: true })).toHaveAttribute('aria-current', 'true');
  const back = await snapshot(page);
  expect(back.virtual_ms).toBe(left.virtual_ms);
  expect(back.nodes.map((n) => n.window_total)).toEqual(left.nodes.map((n) => n.window_total));
});
