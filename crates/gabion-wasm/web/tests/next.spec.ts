import { expect, test } from '@playwright/test';
import { actionLabel, sendLabel } from '../src/lib/controls';
import { heading } from '../src/lib/heading';
import { admittedWaiting, holding, nothingComing, rejected, split, strandedStop, waveOneCounted } from '../src/lib/narration';
import {
  goal,
  nextAction,
  observe,
  pressClause,
  progress,
  storyDone,
  verdictOf,
  waitClause,
  type NextAction,
  type Observation,
  type RateLimiting,
  type RunView,
  type Story,
} from '../src/lib/next';
import { PRESETS } from '../src/lib/presets';
import type { NodeState } from '../src/lib/sim/types';
import { BASE, COMMANDS, TABLE, clauses } from './next-actions';

// The next action, the goal and the heading as pure functions of the run,
// with no browser: every row of the table (`next-actions.ts`) through
// `nextAction`, every story's goal through the clause rule (A56), and the
// heading's forms.

const text = (parts: readonly unknown[]): string =>
  parts.map((p) => (typeof p === 'object' && p !== null ? String((p as { value: unknown }).value) : String(p))).join('');

for (const row of TABLE) {
  test(`next action: ${row.name}`, () => {
    expect(nextAction({ ...BASE, ...row.view })).toEqual(row.action);
  });
}

test('no goal commands, and none quotes a control (A56)', () => {
  // Every row's state, every scenario's opening, and Zone partition's
  // endings either way.
  const partition = (act: 'counted' | 'done', both: boolean): Story => ({ kind: 'partition', next: 'gateway-failure', act, both });
  const views: [string, RunView][] = [
    ...TABLE.map((row): [string, RunView] => [row.name, { ...BASE, ...row.view }]),
    ...PRESETS.map((p): [string, RunView] => [p.label, { ...BASE, story: p.story }]),
    ...[partition('counted', false), partition('done', false)].map((story): [string, RunView] => ['healed early', { ...BASE, story }]),
  ];
  for (const [name, view] of views) {
    const said = text(goal(view));
    for (const clause of clauses(said)) {
      const first = clause.split(/\s+/)[0].toLowerCase();
      expect(COMMANDS as readonly string[], `"${said}" (${name}): the clause "${clause}"`).not.toContain(first);
    }
    for (const label of ['Send', 'Play', 'Step', 'Select node', 'Use 25 requests', 'Go to']) {
      expect(new RegExp(`\\b${label}\\b`, 'i').test(said), `"${said}" quotes "${label}"`).toBe(false);
    }
  }
});

test('each watched scenario watches itself', () => {
  for (const p of PRESETS) if (p.story.kind === 'watch') expect(p.story.watched, p.label).toBe(p.id);
});

// One more request's decisions, and bursts admitted whole, split and refused
// whole.
const admitted = { kind: 'Admitted' as const, total_before: 0 };
const refused = { kind: 'Rejected' as const, total: 100, reopens_ms: 1 };
const all = { kind: 'Admitted' as const, requests: 25, total_before: 0 };
const some = { kind: 'Rejected' as const, admitted: 10, rejected: 15, total: 100, reopens_ms: 1 };
const none = { kind: 'Rejected' as const, admitted: 0, rejected: 25, total: 100, reopens_ms: 1 };
const quiet: Observation = {
  decided: null,
  brim: refused,
  oracle: 100,
  bucket: 3,
  cuts: 0,
  cutZone: null,
  faulted: null,
  failovers: 0,
  agreed: true,
};
const actOf = (s: Story): string => ('act' in s ? s.act : s.kind);

test("Rate limiting's acts move on decisions and reopenings only", () => {
  const o = quiet;
  let s = observe(PRESETS[0].story, { ...o, decided: verdictOf(all, 0) });
  expect(s).toEqual({ kind: 'rate-limiting', next: 'partition', act: 'filling', zones: 1, sendBucket: 3 });
  s = observe(s, { ...o, decided: verdictOf(none, 2) });
  expect(s, 'refused whole: neither zone nor bucket counts').toEqual({ kind: 'rate-limiting', next: 'partition', act: 'refused', zones: 1, sendBucket: 3 });
  s = observe(s, o);
  expect(actOf(s), 'still refused while the window holds').toBe('refused');
  s = observe(s, { ...o, brim: admitted });
  expect(actOf(s)).toBe('reopened');
  s = observe(s, { ...o, decided: verdictOf(none, 2) });
  expect(actOf(s)).toBe('sliding-refused');
  s = observe(s, { ...o, brim: admitted });
  expect(actOf(s)).toBe('drained');
  s = observe(s, { ...o, decided: verdictOf(all, 2) });
  expect(actOf(s)).toBe('done');
});

test("each gateway story moves from its Send to its fault to the failover's agreement (A45)", () => {
  for (const id of ['gateway-failure', 'gateway-cut'] as const) {
    const opening = PRESETS.find((p) => p.id === id)!.story;
    expect(storyDone(opening), `${id} opens before its end`).toBe(false);
    let s = observe(opening, { ...quiet, decided: verdictOf(none, 0) });
    expect(actOf(s), `${id}: a refused Send offers no fault`).toBe('open');
    s = observe(s, { ...quiet, decided: verdictOf(all, 0) });
    expect(actOf(s), `${id}: the first Send's run`).toBe('sent');
    s = observe(s, quiet);
    expect(actOf(s), `${id}: no fault until it is pressed`).toBe('sent');
    s = observe(s, { ...quiet, faulted: 4, failovers: 2 });
    expect(s, `${id}: the fault names its gateway and the failovers before it`).toMatchObject({ act: 'faulted', gateway: 4, failovers: 2 });
    s = observe(s, { ...quiet, failovers: 2 });
    expect(actOf(s), `${id}: no failover yet`).toBe('faulted');
    s = observe(s, { ...quiet, failovers: 3, agreed: false });
    expect(actOf(s), `${id}: failed over, zone b still behind`).toBe('faulted');
    s = observe(s, { ...quiet, failovers: 3 });
    expect(actOf(s), `${id}: failed over and agreed`).toBe('done');
    expect(storyDone(s)).toBe(true);
  }
});

test('the failover wait counts down on the silent gateway, then measures the run (A45)', () => {
  const faulted: Story = { kind: 'gateway', fault: 'silenced', next: 'gateway-cut', origin: 0, zone: 1, act: 'faulted', gateway: 4, failovers: 0 };
  const v: RunView = { ...BASE, story: faulted, gateway: 4, unanswered: 1200, suspectAfterMs: 2000, clock: 'settled' };
  const from = { reason: 'failover' as const, virtualMs: 0, reopensMs: 0 };
  expect(progress(v, 'failover', from)).toBeCloseTo(0.6);
  expect(text(waitClause('failover', v))).toBe(
    'node 0 has waited 1.2 s for node 4 to answer · it gives up at 2.0 s, then picks another gateway into zone b',
  );
  // Failed over: the run's share of the reachable nodes that have the hits.
  const after: RunView = { ...v, failovers: 1, nodes: BASE.nodes.map((n) => ({ ...n, lag: n.zone === 1 ? { kind: 'Behind', missing_for_ms: 5 } : n.lag })) };
  expect(progress(after, 'failover', from)).toBeCloseTo(8 / 12);
  expect(text(waitClause('failover', after))).toBe('node 0 now reaches zone b through another gateway · the hits are spreading');
});

test('a split Send is a refusal that also counts what it admitted (T3)', () => {
  expect(verdictOf(all, 1)).toEqual({ kind: 'admitted', zone: 1 });
  expect(verdictOf(some, 1)).toEqual({ kind: 'split', zone: 1 });
  expect(verdictOf(none, 1)).toEqual({ kind: 'refused', zone: 1 });
  // Rate limiting: a split refuses, and counts its zone and bucket.
  const filling = observe(PRESETS[0].story, { ...quiet, decided: verdictOf(all, 0) });
  expect(observe(filling, { ...quiet, bucket: 4, decided: verdictOf(some, 2) })).toEqual({
    kind: 'rate-limiting',
    next: 'partition',
    act: 'refused',
    zones: 0b101,
    sendBucket: 4,
  });
  const at = (act: RateLimiting['act']): RateLimiting => ({ kind: 'rate-limiting', next: 'partition', act, zones: 7, sendBucket: 3 });
  expect(actOf(observe(at('reopened'), { ...quiet, decided: verdictOf(some, 2) }))).toBe('sliding-refused');
  expect(actOf(observe(at('drained'), { ...quiet, decided: verdictOf(some, 2) }))).toBe('done');
  // Propagation: a split sends what it admitted.
  const prop: Story = { kind: 'propagation', next: 'rate-limiting', act: 'open' };
  expect(actOf(observe(prop, { ...quiet, decided: verdictOf(some, 0) }))).toBe('sent');
  expect(actOf(observe(prop, { ...quiet, decided: verdictOf(none, 0) }))).toBe('open');
  // Zone partition: a split is that side refusing.
  const partition = PRESETS.find((p) => p.id === 'partition')!.story;
  expect(observe(partition, { ...quiet, cuts: 1, cutZone: 2, decided: verdictOf(some, 0) })).toEqual({
    kind: 'partition',
    next: 'gateway-failure',
    act: 'far-side',
    refused: 'near',
  });
});

test("Zone partition's ending says whether both sides refused before the heal", () => {
  const opening = PRESETS.find((p) => p.id === 'partition')!.story;
  const cut: Observation = { ...quiet, cuts: 1, cutZone: 2 };
  // Healed with nothing sent: reconnected, and not the over-admission ending.
  let s = observe(opening, quiet);
  expect(s).toEqual({ kind: 'partition', next: 'gateway-failure', act: 'counted', both: false });
  s = observe(s, { ...quiet, oracle: 0 });
  expect(s).toEqual({ kind: 'partition', next: 'gateway-failure', act: 'done', both: false });
  expect(text(goal({ ...BASE, story: s }))).not.toContain('each side admitted');
  // Both sides refused, then healed: the over-admission ending.
  s = observe(opening, { ...cut, decided: verdictOf(none, 0) });
  expect(s).toEqual({ kind: 'partition', next: 'gateway-failure', act: 'far-side', refused: 'near' });
  s = observe(s, { ...cut, decided: verdictOf(none, 1) });
  expect(actOf(s), 'the same side again').toBe('far-side');
  s = observe(s, { ...cut, decided: verdictOf(none, 2) });
  expect(actOf(s)).toBe('repair');
  s = observe(observe(s, quiet), { ...quiet, oracle: 0 });
  expect(s).toEqual({ kind: 'partition', next: 'gateway-failure', act: 'done', both: true });
  expect(text(goal({ ...BASE, story: s }))).toContain('each side admitted on what it could hear');
});

test("the action line's clauses say what the press does", () => {
  const prop: RunView = { ...BASE, story: { kind: 'propagation', next: 'rate-limiting', act: 'sent' } };
  const behind = BASE.nodes.map((n) => (n.id === 0 ? n : { ...n, lag: { kind: 'Behind' as const, missing_for_ms: 5 } }));
  // Paused before any wave: Step draws the next one, not the window's edge.
  expect(text(pressClause({ ...prop, nodes: behind }, 'step', 1))).toBe(' to send wave 1');
  expect(text(pressClause({ ...prop, nodes: behind }, 'step', 2))).toBe(' for wave 2, the relays');
  expect(text(pressClause({ ...prop, clock: 'before-wave' }, 'step', 1))).toBe(' to send wave 1');
  expect(text(pressClause(prop, 'step', 3))).toBe(" to reach the window's edge");
  // The drained Send names the selected node's own count, and only while it
  // still counts some.
  const drained: RunView = { ...BASE, story: { kind: 'rate-limiting', next: 'partition', act: 'drained', zones: 7, sendBucket: 3 }, selected: 2, oracle: 100, subjectCount: 75 };
  expect(text(pressClause(drained, 'send', 1))).toBe(' · node 2 admits while it still counts 75');
  expect(text(pressClause({ ...drained, subjectCount: 0 }, 'send', 1))).toBe('');
});

test("a run's meter counts the reachable nodes that have the hits", () => {
  const view: RunView = {
    ...BASE,
    nodes: BASE.nodes.map((n) => ({ ...n, lag: n.zone === 2 ? { kind: 'Stranded', missing_for_ms: 1 } : n.id < 3 ? { kind: 'CaughtUp' } : { kind: 'Behind', missing_for_ms: 1 } })),
  };
  // Zone c (4 nodes) is stranded; of the 8 reachable, nodes 0 and 1 are up.
  expect(progress(view, 'run', { reason: 'run', virtualMs: 0, reopensMs: 0 })).toBe(2 / 8);
  const waiting: RunView = { ...BASE, brim: { kind: 'Rejected', total: 100, reopens_ms: 2500 } };
  expect(progress(waiting, 'window', { reason: 'window', virtualMs: 0, reopensMs: 10_000 })).toBe(0.75);
  expect(progress({ ...BASE, virtualMs: 3500 }, 'bucket', { reason: 'bucket', virtualMs: 3000, reopensMs: 0 })).toBe(0.5);
  expect(progress(BASE, 'starting', { reason: 'starting', virtualMs: 0, reopensMs: 0 })).toBeNull();
});

/** A node of the heading's tests. */
const node = (id: number, view: number, lag: NodeState['lag'], brim: NodeState['brim'] = { kind: 'Admitted', total_before: 0 }): NodeState =>
  ({ id, zone: id % 3, window_total: view, lag, brim }) as NodeState;

test("the heading's forms (A18)", () => {
  const all = (view: number) => Array.from({ length: 12 }, (_, id) => node(id, view, { kind: 'CaughtUp' }));
  const base = { oracle: 0, limit: 100, onScale: true, newer: null, cuts: [], zones: [{ name: 'zone-a' }, { name: 'zone-b' }, { name: 'zone-c' }], selected: 2, brim: undefined, emptied: false, badge: null, tickMs: 500 };
  expect(heading({ ...base, nodes: all(0) }).id).toBe('rl-0');
  const spreading = all(25).map((n) => (n.id === 0 ? n : node(n.id, 0, { kind: 'Behind', missing_for_ms: 1 })));
  const one = heading({ ...base, nodes: spreading, oracle: 25 });
  expect([one.id, text(one.parts)]).toEqual(['rl-1', '1 of 12 nodes sees 25 of the 100 limit · the rest are up to 25 behind']);
  expect(one.repair).toBeNull();
  expect(heading({ ...base, nodes: all(25), oracle: 25, badge: { hops: 2, afterMs: null } }).id).toBe('rl-2');
  const countdown = heading({ ...base, nodes: all(100), oracle: 100, badge: { hops: 2, afterMs: null }, brim: { kind: 'Rejected', total: 100, reopens_ms: 9_950 } });
  expect([countdown.id, text(countdown.parts)], 'the countdown beats the badge').toEqual([
    'rl-3',
    'All 12 nodes see 100 of the 100 limit · node 2 admits again in 9.9 s',
  ]);
  expect(heading({ ...base, nodes: all(0), emptied: true }).id).toBe('rl-4');
  expect(heading({ ...base, nodes: all(75), oracle: 75 }).id).toBe('rl-5');
  const cut = Array.from({ length: 12 }, (_, id) => node(id, id % 3 === 2 ? 0 : 25, id % 3 === 2 ? { kind: 'Stranded', missing_for_ms: 900 } : { kind: 'CaughtUp' }));
  const stranded = heading({ ...base, nodes: cut, oracle: 25, cuts: [{ kind: 'zone', zone: 2 }] });
  expect([stranded.id, text(stranded.parts), text(stranded.repair ?? [])]).toEqual([
    'hd-stranded',
    '✓ All 8 reachable nodes see 25 hits · zone c is cut off',
    'Reconnect zone c',
  ]);
  // With a feed, a stranded node whose count is within the grace is still
  // stranded, not up.
  const fed = cut.map((n) => (n.zone === 2 ? node(n.id, 25, n.lag) : n));
  expect(heading({ ...base, nodes: fed, oracle: 25, newer: 5, cuts: [{ kind: 'zone', zone: 2 }] }).id).toBe('hd-stranded');
  const feed = heading({ ...base, nodes: all(25).map((n) => (n.id === 0 ? node(0, 10, { kind: 'Behind', missing_for_ms: 1 }) : n)), oracle: 25, newer: 5 });
  expect(feed.id).toBe('hd-feed');
  expect(text(feed.parts)).toMatch(/^11 of 12 nodes have every hit older than one heartbeat/);
  // Zone c stranded with nothing, zone b still behind: the gap is zone b's,
  // the reachable nodes' only.
  const mixed = cut.map((n) => (n.zone === 1 ? node(n.id, 15, { kind: 'Behind', missing_for_ms: 100 }) : n));
  const behind = heading({ ...base, nodes: mixed, oracle: 25, cuts: [{ kind: 'zone', zone: 2 }] });
  expect([behind.id, text(behind.parts)]).toEqual(['rl-1', '4 of 12 nodes see 25 of the 100 limit · the rest are up to 10 behind']);
  const over = heading({ ...base, nodes: all(200), oracle: 200 });
  expect(text(over.parts)).toContain('· 200 admitted against the limit of 100');
  // A gateway its zone cannot hear strands no count: its repair is offered
  // once nothing is behind, a feed's newest hits aside (Gateway cut's ending).
  const unheard = [{ kind: 'unheard-in-zone' as const, node: 4, zone: 1 }];
  expect(text(heading({ ...base, nodes: all(25), oracle: 25, cuts: unheard }).repair ?? []), 'Gateway cut, agreed').toBe('Reconnect node 4');
  expect(heading({ ...base, nodes: spreading, oracle: 25, cuts: unheard }).repair, 'Gateway cut, still spreading').toBeNull();
  const young = all(25).map((n) => (n.id === 1 ? node(1, 23, { kind: 'Behind', missing_for_ms: 100 }) : n));
  expect(text(heading({ ...base, nodes: young, oracle: 25, newer: 2, cuts: unheard }).repair ?? []), "a feed's newest hits aside").toBe('Reconnect node 4');
});

test('narration forms', () => {
  expect(text(waveOneCounted([3, 6, 9], [4, 8]))).toBe('nodes 3, 6 and 9 and gateways node 4 and node 8 counted them');
  expect(text(admittedWaiting(0, 1, 1_000_000, 100))).toBe('node 0 admitted 1 request · that is under 1\u00a0% of the limit, so it waits for its next heartbeat');
  // Either side of the budget's edge, 1 % of the limit, and at half a percent:
  // the reason is given only under it.
  expect(text(admittedWaiting(0, 9_999, 1_000_000, 100))).toBe('node 0 admitted 9999 requests · that is under 1\u00a0% of the limit, so it waits for its next heartbeat');
  expect(text(admittedWaiting(0, 10_000, 1_000_000, 100))).toBe('node 0 admitted 10000 requests · it waits for its next heartbeat');
  expect(text(admittedWaiting(0, 4_999, 1_000_000, 50))).toBe('node 0 admitted 4999 requests · that is under 0.5\u00a0% of the limit, so it waits for its next heartbeat');
  expect(text(split(0, 100, 150, 100, 100))).toBe('node 0 admitted 100 of 250 requests and rejected 150: its window holds 100 of the 100 limit');
  expect(text(rejected(2, 25, 100, 100, [0, 1, 2]))).toBe(
    'node 2 rejected all 25 requests: it sees 100, admitted at nodes 0, 1 and 2, and the limit is 100 · nothing was counted or sent',
  );
  expect(text(rejected(2, 1, 100, 100, null))).toBe('node 2 rejected its request: it sees 100 and the limit is 100 · nothing was counted or sent');
  expect(text(holding(0, 50))).toBe('node 0 holds 50 admitted hits and sends them at its next heartbeat');
  expect(text(nothingComing(['Send 25 requests to node 0'], null))).toBe('Time is running, but nothing is coming · press “Send 25 requests to node 0”');
  expect(text(nothingComing(['Step'], 50))).toBe('Time is running · press “Step”: nothing is coming before 50 hits leave the window');
  expect(text(nothingComing(['Step'], 1))).toBe('Time is running · press “Step”: nothing is coming before 1 hit leaves the window');
  expect(text(strandedStop([['zone b is cut off'], ['zone c is cut off']]))).toBe(
    'paused: zone b is cut off and zone c is cut off, so their hits cannot cross until they are reconnected',
  );
});

test("Send's labels and the next action's label (T5)", () => {
  expect(text(sendLabel('wide', 25, 0))).toBe('Send 25 requests to node 0');
  expect(text(sendLabel('mid', 25, 0))).toBe('Send 25 requests to node 0');
  expect(text(sendLabel('narrow', 25, 0))).toBe('Send 25 requests');
  expect(text(sendLabel('wide', 1, 3))).toBe('Send 1 request to node 3');
  for (const w of ['wide', 'mid', 'narrow'] as const) expect(text(sendLabel(w, null, 0))).toBe('Send');
  const send = sendLabel('wide', 25, 0);
  const label = (action: NextAction): string | null => {
    const parts = actionLabel(action, send, (id) => PRESETS.find((p) => p.id === id)!.label, (z) => `zone ${'abc'[z]}`);
    return parts === null ? null : text(parts);
  };
  expect(label({ kind: 'press', control: 'send' })).toBe('Send 25 requests to node 0');
  expect(label({ kind: 'press', control: 'play' })).toBe('Play');
  expect(label({ kind: 'press', control: 'step' })).toBe('Step');
  expect(label({ kind: 'inline', action: { kind: 'select', node: 1, zone: 1 } })).toBe('Select node 1');
  expect(label({ kind: 'inline', action: { kind: 'use-requests', requests: 25 } })).toBe('Use 25 requests');
  expect(label({ kind: 'inline', action: { kind: 'fault', fault: 'silenced', node: 4, zone: 1, origin: 0 } })).toBe('Silence node 4');
  expect(label({ kind: 'inline', action: { kind: 'fault', fault: 'unheard', node: 4, zone: 1, origin: 0 } })).toBe(
    'Cut node 4 off from zone b',
  );
  expect(label({ kind: 'repair', label: ['Reconnect zone c'] })).toBe('Reconnect zone c');
  expect(label({ kind: 'go', to: 'partition' })).toBe('Go to Zone partition');
  expect(label({ kind: 'edit' })).toBeNull();
  expect(label({ kind: 'wait', reason: 'window' })).toBeNull();
  expect(label({ kind: 'watch', what: 'loss', next: 'rate-limiting' })).toBeNull();
});
