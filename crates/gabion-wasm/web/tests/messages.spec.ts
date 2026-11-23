import { expect, test as base, type Page } from '@playwright/test';
import { HEADER_FIELDS, HEADER_LEN, bodyFields, readLe } from '../src/lib/frame';
import type { MessageDetail } from '../src/lib/messages';
import type { EventBatch } from '../src/lib/sim/types';
import { send } from './drive';

// The Messages view of the detail pane (the network inspector): every message
// the cluster sent, one row each, read from the engine's capture.

/** Every test fails on an error the page throws. */
const test = base.extend<{ page: Page }>({
  page: async ({ page }, use) => {
    const errors: string[] = [];
    page.on('pageerror', (e) => errors.push(e.message));
    await use(page);
    expect(errors, 'page errors').toEqual([]);
  },
});

const status = (page: Page) => page.locator('[data-field="messages-status"]');
const tab = (page: Page, name: 'Details' | 'Messages') => page.getByRole('tab', { name, exact: true });

async function boot(page: Page): Promise<void> {
  await page.goto('/');
  await page.waitForSelector('.stage canvas', { timeout: 30_000 });
}

/** Log every message of the page's engine from now on: seq, route, role and
 *  whether it was lost. */
async function logMessages(page: Page): Promise<void> {
  await page.evaluate(() => {
    type Logged = { seq: number; src: number; dst: number; role: string; lost: boolean };
    const w = window as unknown as { logged: Logged[] };
    w.logged = [];
    const sim = window.sim!;
    const record = (batch: EventBatch): EventBatch => {
      for (const { kind } of batch.events) {
        if (kind.type === 'PacketSent' || kind.type === 'PacketDropped') {
          w.logged.push({ seq: kind.seq, src: kind.src, dst: kind.dst, role: kind.role, lost: kind.type === 'PacketDropped' });
        }
      }
      return batch;
    };
    const submit = sim.submitRequest.bind(sim);
    sim.submitRequest = async (...args) => record(await submit(...args));
    const step = sim.step.bind(sim);
    sim.step = async (...args) => record(await step(...args));
  });
}

const logged = (page: Page) =>
  page.evaluate(() => (window as unknown as { logged: { seq: number; src: number; dst: number; role: string; lost: boolean }[] }).logged);

/** A60 and A65 in part: before a Send the empty state shows and By type
 *  shows only all messages, ack and gap report; after it, every message the
 *  engine reported is listed and counted, numbered from 1, and opening
 *  Messages leaves the next action and its one filled control as they were. */
test('the first Send is listed message by message', async ({ page }) => {
  await boot(page);
  const nextBefore = await page.locator('#next-action').textContent();
  const primaries = await page.locator('[data-primary]').count();
  await tab(page, 'Messages').click();
  await expect(tab(page, 'Messages')).toHaveAttribute('aria-selected', 'true');
  await expect(status(page)).toHaveText(/^\s*0\s+messages/);
  await expect(page.locator('.messages .empty')).toContainText('No messages yet');
  await expect(page.locator('.by-type [data-type]')).toHaveText([/all messages/, /ack/, /gap report/]);
  await expect(page.locator('#next-action')).toHaveText(nextBefore ?? '');
  await expect(page.locator('[data-primary]')).toHaveCount(primaries);
  await expect(page.locator('[role="tabpanel"] [data-primary]')).toHaveCount(0);

  await logMessages(page);
  await send(page, 0);
  // Every node's messages: the scope chip's × widens the node scope.
  await page.getByRole('button', { name: "Show every node's messages" }).click();
  const events = await logged(page);
  expect(events.length, 'a Send makes messages').toBeGreaterThan(5);
  expect(events.map((e) => e.seq)).toEqual(events.map((_, i) => i + 1));
  await expect(status(page)).toContainText(`${events.length} messages`);
  await expect(page.locator('[data-field="count-all"]')).toHaveText(String(events.length));
  const own = events.filter((e) => e.role === 'OwnData').length;
  await expect(page.locator('[data-field="count-OwnData"]')).toHaveText(String(own));
  expect(events.filter((e) => e.role === 'OwnData').every((e) => e.src === 0)).toBe(true);
  await expect(page.locator('[data-field="count-GapReport"]')).toHaveText('0');
  expect(events.some((e) => e.lost), 'a lossless Send loses nothing').toBe(false);

  // The newest rows are listed, in send order, each naming its route.
  const rows = page.locator('.messages .row.message');
  await expect(rows.first()).toBeVisible();
  const seqs = (await rows.evaluateAll((els) => els.map((el) => Number((el as HTMLElement).dataset.seq)))).sort((a, b) => a - b);
  expect(seqs.at(-1)).toBe(events.length);
  const last = events.at(-1)!;
  await expect(page.locator(`.messages .row[data-seq="${last.seq}"] .message`)).toHaveAttribute(
    'aria-label',
    new RegExp(`^message ${last.seq}, [0-9.]+ seconds, node ${last.src} to node ${last.dst}, `),
  );
  await expect(page.locator('#next-action')).not.toHaveText('');
  await expect(page.locator('[role="tabpanel"] [data-primary]')).toHaveCount(0);
});

/** A71 (TS half) and the detail: selecting a message decodes its frame; the
 *  header and body fields cover every byte exactly once, and each header
 *  field read at its offset equals the engine's decoded value. The pair
 *  button narrows to the ladder of the two. */
test('a selected message reads as its frame', async ({ page }) => {
  await boot(page);
  await tab(page, 'Messages').click();
  await logMessages(page);
  await send(page, 0);
  const events = await logged(page);
  for (const role of new Set(events.map((e) => e.role))) {
    const seq = events.find((e) => e.role === role)!.seq;
    const d = (await page.evaluate((s) => window.sim!.selectMessage(s), seq)) as MessageDetail;
    expect(d.bytes.length, `${role}: its bytes`).toBe(d.wire_len);
    const fields = [...HEADER_FIELDS, ...bodyFields(d.bytes, d.header.cells)];
    const owners = new Array<number>(d.wire_len).fill(0);
    for (const f of fields) for (let i = f.at; i < f.at + f.len; i++) owners[i] += 1;
    expect(owners.every((n) => n === 1), `${role}: every byte has one field`).toBe(true);
    const read = (id: string): number => {
      const f = HEADER_FIELDS.find((x) => x.id === id)!;
      return Number(readLe(d.bytes, f.at, f.len));
    };
    expect(read('cells')).toBe(d.header.cells);
    expect(read('own-ack')).toBe(d.header.own_ack);
    expect(read('relay-ack')).toBe(d.header.relay_ack);
    expect(read('ack-start')).toBe(d.header.ack_incarnation);
    expect(d.header.flags).toBe(d.bytes[7]);
    if (d.header.range !== undefined) expect([read('lower'), read('upper')]).toEqual(d.header.range);
    expect(d.bytes.length >= HEADER_LEN).toBe(true);
  }

  // Select an own-data row through the list, shown alone by its By type
  // toggle.
  const ownData = events.find((e) => e.role === 'OwnData' && e.dst !== 0)!;
  await page.locator('.by-type [data-type="OwnData"] button').click();
  await expect(page.locator('.messages .row.message')).toHaveCount(events.filter((e) => e.role === 'OwnData').length);
  await page.locator(`.messages .row[data-seq="${ownData.seq}"]`).click();
  await expect(page.locator('#message-title')).toContainText(`own data #${ownData.seq}`);
  await expect(page.locator('[data-field="message-sentence"]')).toContainText(`Node 0 sent node ${ownData.dst}`);
  await expect(page.locator('.frame .grid [role="listitem"]')).toHaveCount(HEADER_FIELDS.length);
  await expect(page.locator('.frame .caption')).toContainText('header 86');
  await page.getByRole('button', { name: `See only node 0 ↔ node ${ownData.dst}` }).click();
  await expect(page.locator('[data-field="scope-chip"]')).toContainText(`node 0 ↔ node ${ownData.dst}`);
  await expect(page.locator('.messages svg.ladder').first()).toBeVisible();
  await expect(page.locator('[role="tabpanel"] [data-primary]')).toHaveCount(0);
});

/** The pair button of a message that does not touch the selected node scopes
 *  to that message's two nodes: the chip and the ladder name them, the
 *  message is listed and selected, and its rungs are at full ink. */
test('a pair is the selected message’s two nodes', async ({ page }) => {
  await boot(page);
  await tab(page, 'Messages').click();
  await logMessages(page);
  await send(page, 0);
  await page.getByRole('button', { name: "Show every node's messages" }).click();
  const events = await logged(page);
  await expect(page.locator('.messages .row.message').first()).toBeVisible();
  const listed = await page.locator('.messages .row.message').evaluateAll((els) => els.map((el) => Number((el as HTMLElement).dataset.seq)));
  const far = events.find((e) => e.src !== 0 && e.dst !== 0 && listed.includes(e.seq));
  expect(far, 'the newest rows hold a message between two other nodes').toBeDefined();
  const { seq, src, dst } = far!;
  await page.locator(`.messages .row[data-seq="${seq}"]`).click();
  await page.getByRole('button', { name: `See only node ${src} ↔ node ${dst}` }).click();
  await expect(page.locator('[data-field="scope-chip"]')).toContainText(`node ${src} ↔ node ${dst}`);
  await expect(page.locator('.messages .row.selected')).toHaveAttribute('data-seq', String(seq));
  await expect(page.getByRole('button', { name: /^See only/ })).toHaveCount(0);
  await expect(page.locator('.messages svg.ladder:not(.faded)').first()).toBeVisible();
  await expect(page.locator('.messages .list-head .lanes')).toHaveText(`node ${src}node ${dst}`);
});

/** At 768 px the list's columns fit the pane: neither the Message head nor
 *  any row's words are cut at its edge. */
test('the list fits a 768 px pane', async ({ page }) => {
  await page.setViewportSize({ width: 768, height: 1024 });
  await boot(page);
  await tab(page, 'Messages').click();
  await send(page, 0);
  await page.getByRole('button', { name: "Show every node's messages" }).click();
  await expect(page.locator('.messages .row.message').first()).toBeVisible();
  const cut = await page.locator('.messages').evaluate((m) => {
    const edge = m.getBoundingClientRect().right;
    return [...m.querySelectorAll<HTMLElement>('.list-head .message, .row.message .message')]
      .filter((el) => el.getBoundingClientRect().right > edge + 0.5 || el.scrollWidth > el.clientWidth + 1)
      .map((el) => el.textContent);
  });
  expect(cut, 'the Message column cuts nothing').toEqual([]);
});
