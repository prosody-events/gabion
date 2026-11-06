import { expect, test } from '@playwright/test';
import { heading } from '../src/lib/heading';
import { admittedWaiting, strandedStop, waveOneCounted } from '../src/lib/narration';
import { goal, nextAction, observe, progress, START, type RunView } from '../src/lib/next';
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
  for (const row of TABLE) {
    const view: RunView = { ...BASE, ...row.view };
    for (const { id: scenario } of PRESETS) {
      const said = text(goal(view, scenario));
      for (const clause of clauses(said)) {
        const first = clause.split(/\s+/)[0].toLowerCase();
        expect(COMMANDS as readonly string[], `"${said}" (${row.name}): the clause "${clause}"`).not.toContain(first);
      }
      for (const label of ['Send', 'Play', 'Step', 'Select node', 'Use 25 hits', 'Go to']) {
        expect(new RegExp(`\\b${label}\\b`, 'i').test(said), `"${said}" quotes "${label}"`).toBe(false);
      }
    }
  }
});

test("Rate limiting's acts move on decisions and reopenings only", () => {
  const admitted = { kind: 'Admitted' as const, total_before: 0 };
  const refused = { kind: 'Rejected' as const, total: 100, reopens: { kind: 'In' as const, ms: 1 } };
  const o = { decided: null, nextSend: refused, oracle: 100, bucket: 3, cuts: 0, cutZone: null };
  let p = observe(START, 'rate-limiting', { ...o, decided: { zone: 0, admission: admitted } });
  expect(p).toEqual({ act: 'filling', zones: 1, sendBucket: 3 });
  p = observe(p, 'rate-limiting', { ...o, decided: { zone: 2, admission: refused } });
  expect(p.act).toBe('refused');
  p = observe(p, 'rate-limiting', o);
  expect(p.act, 'still refused while the window holds').toBe('refused');
  p = observe(p, 'rate-limiting', { ...o, nextSend: admitted });
  expect(p.act).toBe('reopened');
  p = observe(p, 'rate-limiting', { ...o, decided: { zone: 2, admission: refused } });
  expect(p.act).toBe('sliding-refused');
  p = observe(p, 'rate-limiting', { ...o, nextSend: admitted });
  expect(p.act).toBe('drained');
  p = observe(p, 'rate-limiting', { ...o, decided: { zone: 2, admission: admitted } });
  expect(p.act).toBe('done');
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
  expect(text(strandedStop([['zone b is cut off'], ['zone c is cut off']]))).toBe(
    'paused: zone b is cut off and zone c is cut off, so their hits cannot cross until they are reconnected',
  );
});
