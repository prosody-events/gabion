import { expect, test } from '@playwright/test';
import { heading } from '../src/lib/heading';
import { admittedWaiting, strandedStop, waveOneCounted } from '../src/lib/narration';
import { goal, nextAction, observe, pressClause, progress, verdictOf, type Observation, type RunView, type Story } from '../src/lib/next';
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
    for (const label of ['Send', 'Play', 'Step', 'Select node', 'Use 25 hits', 'Go to']) {
      expect(new RegExp(`\\b${label}\\b`, 'i').test(said), `"${said}" quotes "${label}"`).toBe(false);
    }
  }
});

test('each watched scenario watches itself', () => {
  for (const p of PRESETS) if (p.story.kind === 'watch') expect(p.story.watched, p.label).toBe(p.id);
});

const admitted = { kind: 'Admitted' as const, total_before: 0 };
const refused = { kind: 'Rejected' as const, total: 100, reopens: { kind: 'In' as const, ms: 1 } };
const heavier = { kind: 'Rejected' as const, total: 0, reopens: { kind: 'Never' as const } };
const quiet: Observation = { decided: null, nextSend: refused, oracle: 100, bucket: 3, cuts: 0, cutZone: null };
const actOf = (s: Story): string => ('act' in s ? s.act : s.kind);

test("Rate limiting's acts move on decisions and reopenings only", () => {
  const o = quiet;
  let s = observe(PRESETS[0].story, { ...o, decided: verdictOf(admitted, 0) });
  expect(s).toEqual({ kind: 'rate-limiting', next: 'partition', act: 'filling', zones: 1, sendBucket: 3 });
  s = observe(s, { ...o, decided: verdictOf(heavier, 2) });
  expect(actOf(s), 'a request heavier than the limit moves no story').toBe('filling');
  s = observe(s, { ...o, decided: verdictOf(refused, 2) });
  expect(actOf(s)).toBe('refused');
  s = observe(s, o);
  expect(actOf(s), 'still refused while the window holds').toBe('refused');
  s = observe(s, { ...o, nextSend: admitted });
  expect(actOf(s)).toBe('reopened');
  s = observe(s, { ...o, decided: verdictOf(refused, 2) });
  expect(actOf(s)).toBe('sliding-refused');
  s = observe(s, { ...o, nextSend: admitted });
  expect(actOf(s)).toBe('drained');
  s = observe(s, { ...o, decided: verdictOf(admitted, 2) });
  expect(actOf(s)).toBe('done');
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
  s = observe(opening, { ...cut, decided: verdictOf(refused, 0) });
  expect(s).toEqual({ kind: 'partition', next: 'gateway-failure', act: 'far-side', refused: 'near' });
  s = observe(s, { ...cut, decided: verdictOf(refused, 1) });
  expect(actOf(s), 'the same side again').toBe('far-side');
  s = observe(s, { ...cut, decided: verdictOf(refused, 2) });
  expect(actOf(s)).toBe('repair');
  s = observe(observe(s, quiet), { ...quiet, oracle: 0 });
  expect(s).toEqual({ kind: 'partition', next: 'gateway-failure', act: 'done', both: true });
  expect(text(goal({ ...BASE, story: s }))).toContain('each side admitted on what it could hear');
});

test("the action line's clauses say what the press does", () => {
  const prop: RunView = { ...BASE, story: { kind: 'propagation', next: 'rate-limiting', act: 'sent' } };
  const behind = BASE.nodes.map((n) => (n.id === 0 ? n : { ...n, lag: { kind: 'Behind' as const, missing_for_ms: 5 } }));
  // Paused before any wave: Step flies the next one, not the window's edge.
  expect(text(pressClause({ ...prop, nodes: behind }, 'step', 1))).toBe(' to send wave 1');
  expect(text(pressClause({ ...prop, nodes: behind }, 'step', 2))).toBe(' for wave 2, the relays');
  expect(text(pressClause({ ...prop, flight: 'waiting' }, 'step', 1))).toBe(' to send wave 1');
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
  const waiting: RunView = { ...BASE, nextSend: { kind: 'Rejected', total: 100, reopens: { kind: 'In', ms: 2500 } } };
  expect(progress(waiting, 'window', { reason: 'window', virtualMs: 0, reopensMs: 10_000 })).toBe(0.75);
  expect(progress({ ...BASE, virtualMs: 3500 }, 'bucket', { reason: 'bucket', virtualMs: 3000, reopensMs: 0 })).toBe(0.5);
  expect(progress(BASE, 'starting', { reason: 'starting', virtualMs: 0, reopensMs: 0 })).toBeNull();
});

/** A node of the heading's tests. */
const node = (id: number, view: number, lag: NodeState['lag'], brim: NodeState['brim'] = { kind: 'Admitted', total_before: 0 }): NodeState =>
  ({ id, zone: id % 3, window_total: view, lag, brim }) as NodeState;

test("the heading's forms (A18)", () => {
  const all = (view: number) => Array.from({ length: 12 }, (_, id) => node(id, view, { kind: 'CaughtUp' }));
  const base = { oracle: 0, limit: 100, onScale: true, newer: null, cuts: [], zones: [{ name: 'zone-a' }, { name: 'zone-b' }, { name: 'zone-c' }], selected: 2, nextSend: undefined, sendHits: 25, emptied: false, badge: null, tickMs: 500 };
  expect(heading({ ...base, nodes: all(0) }).id).toBe('rl-0');
  const spreading = all(25).map((n) => (n.id === 0 ? n : node(n.id, 0, { kind: 'Behind', missing_for_ms: 1 })));
  const one = heading({ ...base, nodes: spreading, oracle: 25 });
  expect([one.id, text(one.parts)]).toEqual(['rl-1', '1 of 12 nodes sees 25 of the 100 limit · the rest are up to 25 behind']);
  expect(one.repair).toBeNull();
  expect(heading({ ...base, nodes: all(25), oracle: 25, badge: { hops: 2, afterMs: null } }).id).toBe('rl-2');
  const countdown = heading({ ...base, nodes: all(100), oracle: 100, badge: { hops: 2, afterMs: null }, nextSend: { kind: 'Rejected', total: 100, reopens: { kind: 'In', ms: 9_950 } } });
  expect([countdown.id, text(countdown.parts)], 'the countdown beats the badge').toEqual([
    'rl-3',
    'All 12 nodes see 100 of the 100 limit · node 2 admits 25 again in 9.9 s',
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
});

test('narration forms', () => {
  expect(text(waveOneCounted([3, 6, 9], [4, 8]))).toBe('nodes 3, 6 and 9 and gateways node 4 and node 8 counted them');
  expect(text(admittedWaiting(0, 1, 1_000_000, 100))).toBe('node 0 admitted 1 hit · that is under 1\u00a0% of the limit, so it waits for its next heartbeat');
  // Either side of the budget's edge, 1 % of the limit, and at half a percent:
  // the reason is given only under it.
  expect(text(admittedWaiting(0, 9_999, 1_000_000, 100))).toBe('node 0 admitted 9999 hits · that is under 1\u00a0% of the limit, so it waits for its next heartbeat');
  expect(text(admittedWaiting(0, 10_000, 1_000_000, 100))).toBe('node 0 admitted 10000 hits · it waits for its next heartbeat');
  expect(text(admittedWaiting(0, 4_999, 1_000_000, 50))).toBe('node 0 admitted 4999 hits · that is under 0.5\u00a0% of the limit, so it waits for its next heartbeat');
  expect(text(strandedStop([['zone b is cut off'], ['zone c is cut off']]))).toBe(
    'paused: zone b is cut off and zone c is cut off, so their hits cannot cross until they are reconnected',
  );
});
