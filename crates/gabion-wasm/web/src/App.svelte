<script lang="ts">
  import { onMount } from 'svelte';
  import { blurb, fmtCount, fmtSeconds, joined, noun, plain, zoneAt, type Blurb } from './lib/format';
  import { Sim } from './lib/sim/sim';
  import { readTheme, watchTheme } from './lib/theme';
  import {
    ANSWER,
    CARRIES_CELLS,
    KIND,
    admittedOf,
    waves,
    type Burst,
    type ClusterState,
    type EventBatch,
    type Inspected,
    type PeerView,
    type SimConfig,
    type StepUntil,
  } from './lib/sim/types';
  import type { Fault, Faults, Roles, Scale } from './lib/stage/renderer';
  import {
    DEFAULT_PRESET,
    PRESETS,
    WATCHED_KEY,
    BURST_REQUESTS,
    BURST_REQUESTS_MAX,
    BURST_REQUESTS_MIN,
    ORIGIN,
    knobsFromPreset,
    presetById,
    warm,
    type Knobs,
    type Preset,
    type SustainedTraffic,
  } from './lib/presets';
  import { nominalBuckets } from './lib/buckets';
  import { cutName, cutState, type Cut } from './lib/cuts';
  import { actionLabel, sendLabel, watchWidth, widthNow, type Width } from './lib/controls';
  import { heading as headingOf, type Badge } from './lib/heading';
  import { onScale } from './lib/limit';
  import {
    goal as goalOf,
    nextAction,
    observe,
    pressClause,
    progress,
    storyDone,
    verdictOf,
    waitClause,
    type Clock,
    type GatewayFault,
    type NextAction,
    type PresetId,
    type RunView,
    type UntilSettled,
    type Verdict,
    type WaitStart,
  } from './lib/next';
  import * as say from './lib/narration';
  import { Run, UNDO_MS } from './lib/run.svelte';
  import Stage from './lib/components/Stage.svelte';
  import Charts from './lib/components/Charts.svelte';
  import Messages from './lib/components/Messages.svelte';
  import NodeInspector from './lib/components/NodeInspector.svelte';
  import ControlRail from './lib/components/ControlRail.svelte';
  import TransportBar from './lib/components/TransportBar.svelte';
  import StageHeading from './lib/components/StageHeading.svelte';
  import StageCaption from './lib/components/StageCaption.svelte';
  import RoleGlyph from './lib/components/RoleGlyph.svelte';
  import Sentence from './lib/components/Sentence.svelte';

  // The session runs one scenario at a time (`lib/presets.ts`), as a `Run`: a
  // warmed cluster, its seed, and everything the page keeps about it. Play
  // advances simulated time, Step runs it to the next event, and Send submits
  // a burst of Requests per Send at the selected node. A scenario button, Restart
  // and Rebuild each build a fresh run first, then swap it in; Undo swaps the
  // previous run back.
  //
  // Every engine call runs through one queue (`serial`), one at a time, and
  // each batch the engine returns is shown whole: its packets as lines, then
  // its snapshot, in one task, so every count changes in the frame its line
  // appears, never before, and every readout is the engine's own state. Play
  // never waits for a packet, and a line fades over one simulated second. A
  // command that changes the cluster first runs past gabion's emit floor
  // (`pastFloor`), then shows its own effect.
  //
  // One time model: a Send, Heal or Add pressed while paused plays until
  // every node that waiting can reach has the hits and one heartbeat period
  // has passed since the last wave, so the acknowledgements have been sent,
  // then pauses itself; in Propagation the Send also stops before each wave,
  // which the engine holds until Step draws it. Pressed while playing, a
  // command leaves time alone.
  //
  // The page decides one next action per state (`lib/next.ts`): its control
  // alone is filled, and the caption's action line names it.

  // The Rust-side production defaults (`Sim.defaultConfig()`), the single source
  // the controls open from, and the zone stepper's ceiling.
  let defaults = $state.raw<SimConfig | null>(null);
  let maxZones = $state(0);
  let run = $state.raw<Run | null>(null);
  // The run a scenario change, Restart or Rebuild left: Undo brings it back
  // until the next command or `UNDO_MS`.
  let previous = $state.raw<Run | null>(null);
  let undoTimer = 0;
  // Building the first run, or a rebuild: the controls wait for it.
  let busy = $state(true);
  // A first load that could not build, and a later command that failed.
  let firstError = $state<string | null>(null);
  let commandError = $state<string | null>(null);

  // Where a step by event stops: before a wave of data packets is delivered,
  // or where some node's window drops a bucket that holds hits.
  const EVENT: StepUntil = { Event: { key: WATCHED_KEY } };
  // Cap one advance so a backgrounded tab resuming rAF can't leap seconds of
  // virtual time in a single frame.
  const MAX_STEP_MS = 500;
  // The longest a command's run plays on its own, in heartbeat periods.
  const UNTIL_SETTLED_MAX_PERIODS = 16;
  // The visualizer's upper bound on live nodes; the rail disables Add here so a
  // held key can't spawn runtimes without limit. The one-second step of 183
  // nodes was not measured to fit a frame, so the cap stays at 100 (see the
  // crate README).
  const MAX_LIVE_NODES = 100;

  let speed = $state(1);
  let theme = $state.raw(readTheme());
  let width = $state<Width>(widthNow());
  // Below 640 px the rail folds into a "Controls" disclosure above the stage; at
  // and above it the rail is always open (its summary is hidden).
  let controlsOpen = $state(widthNow() !== 'narrow');
  let helpOpen = $state(false);

  // The run's clocks and bounds, from the knobs it was built with.
  const tickMs = $derived(run?.applied.tick_interval_ms ?? 0);
  const floorMs = $derived(run?.config.min_emit_interval_ms ?? 0);
  const bucketMs = $derived(run?.config.rule_bucket_ms ?? 1000);
  const cluster = $derived(run?.cluster ?? null);
  const virtualMs = $derived(cluster?.virtual_ms ?? 0);
  // A feed's newest hits, younger than one heartbeat period and the floor, do
  // not count a node as behind.
  const grace = $derived(run?.traffic === undefined ? 0 : tickMs + floorMs);

  // Virtual ms one emit floor after the last wave's instant: a command runs
  // the run on to here first (`pastFloor`). An instant the run has reached
  // leaves nothing due.
  let floorUntil = 0;
  // The tail of the queue every engine call runs on.
  let queue: Promise<void> = Promise.resolve();

  /** What time does, the next action's `Clock`: paused; playing freely;
   *  stopped where a command's run reached its cap, until the next Play or
   *  command; a command's run playing on its own, with where it started and
   *  its last wave, in virtual ms; or Propagation's run stopped before the
   *  wave the engine holds (`held`, the batch that sent it), until Step draws
   *  it. A held wave exists only there, so time never runs over one. */
  type Time =
    | { readonly kind: Exclude<Clock, UntilSettled | 'before-wave'> }
    | { readonly kind: UntilSettled; readonly start: number; lastWave: number }
    | { readonly kind: 'before-wave'; readonly start: number; lastWave: number; readonly held: EventBatch };
  const PAUSED: Time = { kind: 'paused' };
  const PLAYING: Time = { kind: 'playing' };
  const CAPPED: Time = { kind: 'capped' };
  let time = $state.raw<Time>(PAUSED);
  // Whether simulated time runs: the play loop's and the transport's one
  // reading of `time`.
  const playing = $derived(loops(time));

  // The last Send: its node's decisions, as the story reads them, and the
  // narration sentence it was told in. Cleared at the next command; a
  // decision that admitted any also when its run ends.
  let decisions = $state.raw<ReadonlyMap<number, Burst>>(new Map());
  let decided: Verdict | null = null;
  // The gateway a gateway story's fault just broke, for the story to read.
  let faulted: number | null = null;
  let told: Blurb = [];
  // The badge's count: data waves delivered since the last admitted Send, the
  // Send's instant, whether it waited for a heartbeat, and the packets sent.
  let hops = 0;
  let sentAt = 0;
  let waited = false;
  let armed = false;
  let dataSent = 0;
  let dataCrossed = 0;
  let badge = $state.raw<Badge | null>(null);
  let emptied = $state(false);

  // The narration: the page's one live region. `IDLE`, compared by identity,
  // stands for the sentence derived from the run as it is now (`idleText`), so
  // an idle narration can never go stale.
  const IDLE: Blurb = [];
  let narration = $state.raw<Blurb>(IDLE);
  // The rebuilt run's sentence, which says it stands at its start: the first
  // snapshot that moves time replaces it while it is still told.
  let atStart: Blurb | null = null;

  // Requests per Send as typed, as last committed, whether that is valid, and
  // whether it is being typed.
  let requestsText = $state('25');
  let requestsCommitted = '25';
  let requestsValid = $state(true);
  let editingRequests = $state(false);

  // The node under the pointer, the node with keyboard focus, and the node a
  // hovered or focused control names.
  let hoveredId = $state<number | null>(null);
  let focusedId = $state<number | null>(null);
  let namedId = $state<number | null>(null);
  // The selected node's cells and peer rows, as the last snapshot inspecting
  // it reported them.
  let inspected = $state.raw<Inspected | null>(null);

  // Wall ms the last whole heartbeat period took to play, and the period the
  // play loop stands in since Play, with the wall instant it crossed into it
  // (null in the period Play started in, which is partial); null before the
  // first advance.
  let heartbeatWallMs = $state<number | null>(null);
  let period: { index: number; wall: number | null } | null = null;

  let rafId = 0;
  let lastWall = 0;
  // The fraction of a simulated ms the last frame left over.
  let carry = 0;
  // Exactly one advance is ever running: the play loop will not issue the
  // next `step` until the previous resolves, so commands can't pile up faster
  // than the engine drains them.
  let stepping = false;
  // Where the current wait started: replaced only when its reason changes.
  let waitStart: WaitStart | null = null;

  // The stage, commanded in the order the queue runs the engine.
  let stage = $state.raw<ReturnType<typeof Stage>>();

  /** The selected node's marks: itself, the gateway the runtime reports for
   *  each remote zone, and a remote peer it suspects whatever its role, so a
   *  gateway it gave up on keeps its hollow port. */
  function rolesOf(selected: number, zone: number, peers: PeerView[] | undefined): Roles | null {
    if (peers === undefined) return null;
    const gateways = new Map<number, { zone: number; suspect: boolean }>();
    for (const p of peers) {
      if (p.id === undefined || p.zone === undefined || p.zone === zone) continue;
      const suspect = ANSWER[p.answer].suspect;
      if (p.role === 'gateway' || suspect) gateways.set(p.id, { zone: p.zone, suspect });
    }
    return { selected, gateways };
  }
  // Replaced only when the marks change, not with every snapshot's fresh peer
  // rows: the stage redraws every mark when it is handed new roles.
  let rolesKey = '';
  let rolesKept: Roles | null = null;
  const roles = $derived.by((): Roles | null => {
    const r = run;
    if (r === null || inspected === null || inspected.id !== r.selected) return rolesKept;
    const zone = r.cluster.nodes.find((n) => n.id === r.selected)?.zone ?? -1;
    const next = rolesOf(r.selected, zone, inspected.peers);
    const key = next === null ? '' : `${next.selected}:${[...next.gateways].map(([g, x]) => `${g}/${x.zone}/${x.suspect}`).join(',')}`;
    if (key !== rolesKey) {
      rolesKey = key;
      rolesKept = next;
    }
    return rolesKept;
  });

  const selectedNode = $derived(cluster?.nodes.find((n) => n.id === run?.selected) ?? null);
  const limit = $derived(run?.applied.rule_limit ?? 1);
  const scaled = $derived(run !== null && onScale(limit, run.requests));
  const buckets = $derived(run === null ? 1 : nominalBuckets(run.applied.rule_window_ms, bucketMs));
  const scale = $derived<Scale>({ limit, onScale: scaled, requests: run?.requests ?? 1, buckets });
  const faults = $derived.by((): Faults => {
    const nodes = new Map<number, Fault>();
    const zones = new Set<number>();
    for (const c of run?.cuts ?? []) {
      if (c.kind === 'zone') zones.add(c.zone);
      else if (c.kind === 'silenced') nodes.set(c.node, { kind: 'silenced' });
      else nodes.set(c.node, { kind: 'unheard', zone: c.zone });
    }
    return { nodes, zones };
  });
  const cutZone = $derived(run?.cuts.find((c) => c.kind === 'zone')?.zone ?? null);

  // The selected node's held hits, if it holds any for its heartbeat.
  const held = $derived(run !== null && run.held?.node === run.selected ? run.held : null);
  // The sweep toward the selected node's heartbeat, while it holds hits unsent.
  const awaiting = $derived(
    held === null || selectedNode === null || tickMs === 0 ? null : 1 - (selectedNode.next_heartbeat_ms - virtualMs) / tickMs,
  );

  /** The origin if live, else the lowest live node in its zone, else the
   *  lowest live node: where Escape and a removal take the selection. */
  function subjectOf(r: Run): number {
    const nodes = r.cluster.nodes;
    const home = ORIGIN;
    if (nodes.some((n) => n.id === home)) return home;
    const zone = home % Math.max(r.config.zones, 1);
    const inZone = nodes.filter((n) => n.zone === zone);
    return (inZone[0] ?? nodes[0])?.id ?? home;
  }

  /** The preset's config with the knob values layered on top. The bucket width
   *  is fixed so every window the slider picks stays a whole number of
   *  buckets. */
  function effectiveConfig(preset: Preset, k: Knobs, d: SimConfig): SimConfig {
    return {
      ...d,
      ...preset.config,
      nodes: k.nodes,
      zones: k.zones,
      zone_mode: k.zone_mode,
      target_err_bps: k.target_err_bps,
      uniform_loss: k.uniform_loss,
      rule_limit: k.rule_limit,
      rule_window_ms: k.rule_window_ms,
      rule_bucket_ms: d.rule_bucket_ms,
      tick_interval_ms: k.tick_interval_ms,
    };
  }

  /** Build, warm and seed a fresh run of `preset` with knobs `k`, without
   *  touching the running one: a build that fails leaves it running. */
  async function build(preset: Preset, k: Knobs, requests: number): Promise<Run> {
    const d = defaults ?? (defaults = await Sim.defaultConfig());
    if (maxZones === 0) maxZones = await Sim.maxZones();
    const config = effectiveConfig(preset, k, d);
    const fresh = await Sim.create(config);
    try {
      const gateways = await warm(fresh, config);
      const held = await preset.seed(fresh, config);
      // The run's messages are listed from its first paint: a seed that
      // settled before it is warm-up, one that waits plays after it.
      fresh.markRunStart();
      const snap = await fresh.snapshot(ORIGIN, WATCHED_KEY);
      return new Run(fresh, preset, config, { ...k }, snap, requests, held, gateways);
    } catch (e) {
      await fresh.shutdown();
      throw e;
    }
  }

  /** Start `preset` fresh with knobs `k` and Requests per Send `requests`:
   *  build it first, then pause, keep the run it replaces for Undo, and swap. */
  function start(preset: Preset, k: Knobs, requests: number, staged?: Knobs): Promise<void> {
    return serial(async () => {
      pause();
      busy = true;
      commandError = null;
      try {
        const next = await build(preset, k, requests);
        const old = run;
        if (staged !== undefined) next.knobs = { ...staged };
        if (old !== null) {
          await keep(old);
          narration = atStart = say.rebuilt(preset.label, discarded(old));
        }
        adopt(next);
        if (old === null) narration = IDLE;
      } catch (e) {
        const message = e instanceof Error ? e.message : String(e);
        if (run === null) firstError = message;
        else
          commandError = `The cluster could not be rebuilt, so the running one stays: ${message} Check the Tune settings, or press Restart.`;
      } finally {
        busy = false;
      }
    });
  }

  /** What leaving `r` discards, beyond its scenario's own state. */
  function discarded(r: Run): Blurb {
    const added = r.cluster.nodes.length - r.config.nodes;
    const parts: Blurb[] = [];
    if (added > 0) parts.push(blurb`${added} added ${noun(added, 'node')}`);
    if (added < 0) parts.push(blurb`${-added} removed ${noun(-added, 'node')}`);
    const cuts = r.cuts.length - r.seededCuts;
    if (cuts > 0) parts.push(blurb`${cuts} ${noun(cuts, 'cut')}`);
    return joined(parts);
  }

  /** Keep `old` for Undo, delivering the wave Propagation's run stood before
   *  (a no-op otherwise): its engine then holds nothing, and its saved
   *  snapshot is the engine's own. */
  async function keep(old: Run): Promise<void> {
    await old.sim.step(0);
    old.cluster = await old.sim.snapshot(old.selected, WATCHED_KEY);
    dropPrevious();
    previous = old;
    undoTimer = window.setTimeout(dropPrevious, UNDO_MS);
  }

  /** Shut the run kept for Undo down. */
  function dropPrevious(): void {
    window.clearTimeout(undoTimer);
    const old = previous;
    previous = null;
    if (old !== null) void old.sim.shutdown();
  }

  /** Make `r` the run: a fresh picture, paused, nothing held or decided. */
  function adopt(r: Run): void {
    floorUntil = 0;
    setTime(PAUSED);
    decisions = new Map();
    decided = null;
    badge = null;
    armed = false;
    emptied = false;
    waitStart = null;
    stage?.reset();
    hoveredId = null;
    namedId = null;
    heartbeatWallMs = null;
    run = r;
    window.sim = r.sim;
    inspected = r.cluster.inspected ?? null;
    requestsText = String(r.requests);
    requestsCommitted = requestsText;
    requestsValid = true;
    r.history.push(r.cluster);
    r.chartVersion += 1;
  }

  /** Undo: the previous run back, paused, exactly as it was left. */
  function undo(): Promise<void> {
    return serial(async () => {
      const back = previous;
      const now = run;
      if (back === null || now === null) return;
      pause();
      window.clearTimeout(undoTimer);
      previous = null;
      adopt(back);
      void now.sim.shutdown();
      narration = [`Back to the ${back.preset.label} run you left`];
    });
  }

  /** The idle sentence: what the selection means, and its gateways. */
  function idleOf(r: Run): Blurb {
    const peers = r.cluster.inspected?.peers ?? [];
    const gateways = peers.flatMap((p) =>
      p.role === 'gateway' && p.id !== undefined && p.zone !== undefined
        ? [{ node: p.id, zone: zoneAt(r.cluster.zones, p.zone) }]
        : [],
    );
    return say.idle(r.selected, gateways, onScale(r.applied.rule_limit, r.requests));
  }

  /** Adopt a fresh snapshot of `r` and append it to the chart history: the one
   *  place per-frame state, the story's act and the rolling series stay in
   *  step. */
  function applySnapshot(r: Run, snap: ClusterState): void {
    const before = r.cluster;
    r.cluster = snap;
    if (snap.inspected !== undefined && snap.inspected.id === r.selected) inspected = snap.inspected;
    r.history.push(snap);
    r.chartVersion += 1;
    const brim = snap.nodes.find((n) => n.id === r.selected)?.brim;
    r.story = observe(r.story, {
      decided,
      brim,
      oracle: snap.oracle_total,
      bucket: snap.bucket_epoch_now,
      cuts: r.cuts.length,
      cutZone,
      faulted,
      failovers: failoversOf(r, snap),
      agreed: !snap.nodes.some((n) => n.lag.kind === 'Behind' && n.lag.missing_for_ms >= grace),
    });
    decided = null;
    faulted = null;
    if (narration === atStart && snap.virtual_ms !== before.virtual_ms) narration = IDLE;
    const agreed = snap.nodes.every((n) => n.lag.kind === 'CaughtUp');
    if (!agreed) badge = null;
    else if (armed && snap.oracle_total > 0) {
      armed = false;
      badge = { hops, afterMs: waited ? snap.virtual_ms - sentAt : null };
    }
    // Hits leaving the window, told where no feed hides it.
    if (snap.oracle_total < before.oracle_total && r.traffic === undefined) {
      const left = before.oracle_total - snap.oracle_total;
      emptied = snap.oracle_total === 0;
      badge = null;
      const reopened = before.nodes.find((n) => n.id === r.selected)?.brim.kind === 'Rejected' && brim?.kind === 'Admitted';
      narration = say.expiry(left, emptied, r.applied.rule_window_ms / 1000, r.selected, reopened);
    }
  }

  /** The gateway story's origin's gateway failovers in `snap`; 0 elsewhere. */
  function failoversOf(r: Run, snap: ClusterState): number {
    const s = r.story;
    return s.kind === 'gateway' ? (snap.nodes.find((n) => n.id === s.origin)?.gateway_failovers ?? 0) : 0;
  }

  function frame(now: number): void {
    rafId = 0;
    if (!loops(time) || run === null) return;
    rafId = requestAnimationFrame(frame);
    if (stepping) return;
    const wallDelta = lastWall === 0 ? tickMs / 6 : now - lastWall;
    lastWall = now;
    // The fraction a frame leaves carries to the next, so every speed paces
    // exactly.
    const want = carry + wallDelta * speed;
    const deltaMs = Math.min(Math.floor(want), MAX_STEP_MS);
    carry = deltaMs === MAX_STEP_MS ? 0 : want - deltaMs;
    if (deltaMs <= 0) return;
    stepping = true;
    void exec((r) => advance(r, deltaMs)).finally(() => {
      stepping = false;
    });
  }

  /** Run `task` after every engine call queued before it, so the page shows
   *  each batch in the order the engine made them. */
  function serial(task: () => Promise<void>): Promise<void> {
    const next = queue.then(task);
    queue = next.catch(() => {});
    return next;
  }

  /** Run `task` on the live run, on the queue. A failure stops play (the
   *  engine's next command closes a wave it holds) and says so in place. */
  function exec(task: (r: Run) => Promise<void>): Promise<void> {
    return serial(async () => {
      const r = run;
      if (r === null) return;
      try {
        await task(r);
      } catch (e) {
        const message = e instanceof Error ? e.message : String(e);
        commandError = `The simulator stopped a command: ${message} The cluster is paused as it was; press Restart to start the scenario again.`;
        pause();
      }
    });
  }

  /** Apply the engine's current state. */
  async function refresh(r: Run): Promise<void> {
    applySnapshot(r, await r.sim.snapshot(r.selected, WATCHED_KEY));
  }

  /** What one batch tells the page beyond its snapshot: a held Send's hits
   *  leaving their node, a gateway change of the selected node, and, for a
   *  batch with a wave, where gabion's emit floor next lets a flush go and the
   *  run's last wave. */
  function note(r: Run, batch: EventBatch): void {
    let wave = Number.NaN;
    for (const { kind, virtual_ms } of batch.events) {
      if ((kind.type === 'PacketSent' || kind.type === 'PacketDropped') && CARRIES_CELLS[kind.role]) {
        if (kind.src === r.held?.node) r.held = null;
        dataSent += 1;
        if (kind.tier === 'CrossZone') dataCrossed += 1;
        wave = virtual_ms;
      } else if (kind.type === 'GatewayChanged' && kind.origin === r.selected) {
        narration = say.gatewayChanged(kind.origin, zoneAt(r.cluster.zones, kind.zone), kind.to, kind.from);
      }
    }
    if (Number.isNaN(wave)) return;
    floorUntil = wave + floorMs;
    if (time.kind === 'settled' || time.kind === 'per-wave' || time.kind === 'before-wave') time.lastWave = wave;
  }

  /** Show a batch the engine returned, whole: its packets as lines, then its
   *  snapshot, in one task, so every count changes in the frame its line
   *  appears. */
  async function meet(r: Run, batch: EventBatch): Promise<void> {
    const snap = await r.sim.snapshot(r.selected, WATCHED_KEY);
    note(r, batch);
    hops += waves(batch);
    stage?.show(batch);
    applySnapshot(r, snap);
  }

  /** Deliver the wave the engine stopped before, whose batch is `held`:
   *  `step(0)` closes its sub-step at the same instant, and its lines and the
   *  receivers' counts show in one frame. */
  async function deliverWave(r: Run, held: EventBatch): Promise<void> {
    const delivered = await r.sim.step(0);
    const snap = await r.sim.snapshot(r.selected, WATCHED_KEY);
    note(r, delivered);
    hops += 1;
    stage?.show(held);
    stage?.show(delivered);
    applySnapshot(r, snap);
  }

  /** Stop Propagation's run before the wave `batch` holds: the engine keeps it
   *  undelivered, the page shows the senders' state and none of its lines,
   *  and Step draws it. */
  async function stopBefore(r: Run, t: { readonly start: number; readonly lastWave: number }, batch: EventBatch): Promise<void> {
    note(r, batch);
    await refresh(r);
    setTime({ kind: 'before-wave', start: t.start, lastWave: t.lastWave, held: batch });
  }

  /** Deliver the wave Propagation's run stands before, if it does, and play
   *  the run on to its next wave. Whether there was one. */
  async function sendHeld(r: Run): Promise<boolean> {
    const t = time;
    if (t.kind !== 'before-wave') return false;
    await deliverWave(r, t.held);
    setTime({ kind: 'per-wave', start: t.start, lastWave: t.lastWave });
    return true;
  }

  /** Run on to one emit floor past the last wave, drawing each wave due on
   *  the way (the relays); each pushes the floor past itself. The run then
   *  stands at least one floor past its last threshold round, the instant
   *  gabion's floor (`runtime.rs` `want_immediate_flush`) lets the next eager
   *  flush go, whatever the floor knob says. */
  async function pastFloor(r: Run): Promise<void> {
    while (r.cluster.virtual_ms < floorUntil) await meet(r, await r.sim.step(floorUntil - r.cluster.virtual_ms));
  }

  /** The node feed hit number `index` goes to, or null when none can take it. */
  function feedNode(traffic: SustainedTraffic, nodes: readonly { id: number }[], index: number): number | null {
    if (traffic.at === 'every-node') return nodes.length === 0 ? null : nodes[index % nodes.length].id;
    for (let i = 0; i < nodes.length; i++) if (nodes[i].id === traffic.at.node) return traffic.at.node;
    return null;
  }

  /** Inject the feed's owed one-hit requests at the current virtual time,
   *  each drawn as its eager flush's lines; their counts show with the next
   *  snapshot. */
  async function feed(r: Run): Promise<void> {
    const traffic = r.traffic;
    if (traffic === undefined) return;
    const nodes = r.cluster.nodes;
    const owed = Math.floor((traffic.rate_per_sec * (r.cluster.virtual_ms - r.feedFrom)) / 1000) - r.trafficInjected;
    for (let i = 0; i < owed; i++) {
      const node = feedNode(traffic, nodes, r.trafficInjected);
      r.trafficInjected += 1;
      if (node === null) continue;
      const batch = await r.sim.submitRequest(node, WATCHED_KEY, 1);
      note(r, batch);
      stage?.show(batch);
    }
  }

  /** One play-loop advance of `deltaMs` of virtual time; Propagation's run
   *  stops before a wave. */
  async function advance(r: Run, deltaMs: number): Promise<void> {
    await feed(r);
    const t = time;
    const batch = await r.sim.step(deltaMs, t.kind === 'per-wave' ? EVENT : 'Time');
    if (t.kind === 'per-wave' && waves(batch) > 0) await stopBefore(r, t, batch);
    else await meet(r, batch);
    notePeriod(performance.now());
    settle(r);
  }

  /** The one stop rule: a command's run pauses itself once no node that
   *  waiting can reach is behind and one heartbeat period has passed since
   *  the last wave, or at its cap. Its Send's admitted text goes with it. */
  function settle(r: Run): void {
    const b = time;
    if (b.kind !== 'settled' && b.kind !== 'per-wave') return;
    const now = r.cluster.virtual_ms;
    const late = r.cluster.nodes.find((n) => n.lag.kind === 'Behind' && n.lag.missing_for_ms >= grace);
    const ends = late === undefined && now >= b.lastWave + tickMs;
    if (!ends && now < b.start + UNTIL_SETTLED_MAX_PERIODS * tickMs) return;
    setTime(ends ? PAUSED : CAPPED);
    for (const d of decisions.values()) if (admittedOf(d) > 0) decisions = new Map();
    if (ends) {
      const stranded = r.cluster.nodes.some((n) => n.lag.kind === 'Stranded');
      narration = stranded ? say.strandedStop(r.cuts.map((c) => cutState(c, r.cluster.zones))) : [...told, ...say.EVERY_NODE];
    } else {
      narration = say.cappedStop(UNTIL_SETTLED_MAX_PERIODS, late?.id ?? r.selected);
    }
  }

  /** Measure how long a heartbeat period takes to watch: the wall time between
   *  two period boundaries the play loop crossed. */
  function notePeriod(now: number): void {
    const p = Math.floor(virtualMs / tickMs);
    if (period === null) {
      period = { index: p, wall: null };
      return;
    }
    if (p === period.index) return;
    if (period.wall !== null) heartbeatWallMs = (now - period.wall) / (p - period.index);
    period.index = p;
    period.wall = now;
  }

  /** Run `task` against the cluster as a command: the run kept for Undo goes,
   *  the last Send's decision text clears, a wave Propagation's run stood
   *  before is sent first, and the run goes past gabion's emit floor; then
   *  `task` shows its own effect, told whether a wave was sent first. */
  function command(task: (r: Run, delivered: boolean) => Promise<void>): Promise<void> {
    commandError = null;
    dropPrevious();
    return exec(async (r) => {
      const delivered = await sendHeld(r);
      await pastFloor(r);
      decisions = new Map();
      if (time.kind === 'capped') setTime(PAUSED);
      await task(r, delivered);
    });
  }

  /** Start a bounded run: time plays until the command has propagated, then
   *  pauses itself. A run already bounded starts again from here; one the
   *  reader plays is left alone. */
  function runOn(r: Run, mode: UntilSettled): void {
    if (time.kind === 'playing') return;
    const start = r.cluster.virtual_ms;
    setTime({ kind: mode, start, lastWave: start });
  }

  /** Send a burst of Requests per Send at the selected node, at the current
   *  virtual time. The node decides it in the frame of the press: the
   *  requests it rejects play in its vessel; a burst rejected whole runs no
   *  time; the admitted requests' eager flush is drawn with its receivers'
   *  counts in the frame of the press, except that Propagation's paused run
   *  stops before it, as before every wave. */
  const sendBurst = (): Promise<void> => command((r) => burst(r, r.selected));

  /** Send a burst of Requests per Send at `node`, as `sendBurst` says. */
  async function burst(r: Run, node: number): Promise<void> {
    const before = r.cluster.nodes.find((n) => n.id === node);
    const mode: UntilSettled = r.story.kind === 'propagation' ? 'per-wave' : 'settled';
    const perWave = mode === 'per-wave' && time.kind !== 'playing';
    const batch = await r.sim.submitRequest(node, WATCHED_KEY, r.requests, perWave ? EVENT : 'Time');
    const event = batch.events.find((e) => e.kind.type === 'Decided');
    if (event === undefined || event.kind.type !== 'Decided') return;
    const burst = event.kind.burst;
    decisions = new Map([[node, burst]]);
    decided = verdictOf(burst, before?.zone ?? 0);
    const admitted = admittedOf(burst);
    r.sends.push(node, r.cluster.bucket_epoch_now, admitted, burst.kind === 'Rejected' ? burst.rejected : 0);
    if (burst.kind === 'Rejected') {
      stage?.reject(node, burst.rejected);
      if (burst.admitted === 0) {
        narration = say.rejected(node, burst.rejected, burst.total, limit, senders(r, burst.total));
        await refresh(r);
        return;
      }
    }
    const eager = waves(batch) > 0;
    hops = 0;
    dataSent = 0;
    dataCrossed = 0;
    sentAt = r.cluster.virtual_ms;
    waited = !eager;
    armed = true;
    emptied = false;
    // Hits that wait for the node's heartbeat join any it holds; an eager
    // flush's packets clear them (`note`).
    if (!eager) r.held = { node, hits: (r.held?.node === node ? r.held.hits : 0) + admitted };
    told =
      burst.kind === 'Rejected'
        ? say.split(node, burst.admitted, burst.rejected, burst.total, limit)
        : eager
          ? say.admittedEager(node, burst.requests, burst.total_before, limit)
          : say.admittedWaiting(node, burst.requests, limit, r.config.target_err_bps);
    narration = told;
    runOn(r, mode);
    // Only a wave the engine was asked to hold: a Pause pressed while the
    // Send was decided must not claim one it already delivered.
    const t = time;
    if (perWave && eager && t.kind === 'per-wave') await stopBefore(r, t, batch);
    else await meet(r, batch);
  }

  /** A gateway story's fault: break `gateway` (silence it, or cut it off
   *  from `zone`), and in the same command bring the story's origin its next
   *  burst, whose run plays through the consequence, the detection and the
   *  failover. */
  const breakGateway = (fault: GatewayFault, gateway: number, zone: number): Promise<void> =>
    command(async (r) => {
      const s = r.story;
      if (s.kind !== 'gateway') return;
      const c: Cut = fault === 'silenced' ? { kind: 'silenced', node: gateway } : { kind: 'unheard-in-zone', node: gateway, zone };
      await r.sim.cut(c, r.cluster.nodes);
      r.cuts = r.sim.cuts();
      r.startFeed();
      faulted = gateway;
      await burst(r, s.origin);
      told = say.faultPressed(fault === 'silenced', gateway, zoneAt(r.cluster.zones, zone), s.origin, r.requests);
      narration = told;
    });

  /** The nodes whose admitted hits fill the window, for the rejection, when
   *  the refusing node sees them all and more than one sent them. */
  function senders(r: Run, view: number): readonly number[] | null {
    if (view !== r.cluster.oracle_total) return null;
    const nodes = r.sends.nodesInWindow(r.cluster.bucket_epoch_now, buckets);
    return nodes.length > 1 ? nodes : null;
  }

  /** Select node `id`, once a snapshot inspecting it has its rows. Changes no
   *  count, so it delivers nothing. */
  const select = (id: number): Promise<void> =>
    exec(async (r) => {
      const snap = await r.sim.snapshot(id, WATCHED_KEY);
      r.selected = id;
      inspected = snap.inspected ?? null;
      r.cluster = snap;
      if (time.kind === 'paused' || time.kind === 'playing' || time.kind === 'capped') narration = IDLE;
    });

  /** Make fault `c` over the live nodes; made while Propagation's run stood
   *  before a wave, it says the wave was sent first. */
  const cut = (c: Parameters<Sim['cut']>[0]): Promise<void> =>
    command(async (r, delivered) => {
      await r.sim.cut(c, r.cluster.nodes);
      r.cuts = r.sim.cuts();
      await refresh(r);
      if (delivered) narration = [...cutState(c, r.cluster.zones), ...say.ARRIVED_FIRST];
    });

  /** Undo `cut` alone, or with none every listed cut, restoring the
   *  scenario's own network, its loss included; then run time until the
   *  nodes agree. */
  const heal = (cut?: Cut): Promise<void> =>
    command(async (r) => {
      if (cut === undefined) await r.sim.heal();
      else await r.sim.reconnect(cut, r.cluster.nodes);
      r.cuts = r.sim.cuts();
      await refresh(r);
      told = cut === undefined ? ['the network is whole again'] : repairOf(cut, r);
      narration = told;
      runOn(r, 'settled');
    });

  /** What reconnecting `cut` did, as the narration says it. */
  function repairOf(cut: Cut, r: Run): Blurb {
    return cut.kind === 'zone' ? blurb`${zoneAt(r.cluster.zones, cut.zone)} is reconnected` : blurb`node ${cut.node} is reconnected`;
  }

  /** Spawn a fresh cold-start node into zone `to` of the live cluster, or one
   *  into every zone with members, each cut as the listed faults say, and run
   *  time until they have caught up. */
  const addNode = (to: number | 'every'): Promise<void> =>
    command(async (r) => {
      const zones = r.cluster.zones.flatMap((z, i) => (z.live > 0 && (to === 'every' || to === i) ? [i] : []));
      for (const zone of zones) await r.sim.addNode(zone);
      await refresh(r);
      await r.sim.reapply(r.cluster.nodes);
      await refresh(r);
      told = to === 'every' ? ['a node joined every zone'] : blurb`a node joined ${zoneAt(r.cluster.zones, to)}`;
      narration = told;
      runOn(r, 'settled');
    });

  /** Remove the selected node. Survivors keep their ids and re-converge; the
   *  selection moves to the scenario's fallback, and hits the node held for
   *  its heartbeat go with it. */
  const removeNode = (id: number): Promise<void> =>
    command(async (r) => {
      await r.sim.removeNode(id);
      if (r.held?.node === id) r.held = null;
      r.cuts = r.sim.cuts();
      const snap = await r.sim.snapshot(null, WATCHED_KEY);
      r.cluster = snap;
      r.selected = subjectOf(r);
      await refresh(r);
    });

  /** Whether the play loop runs under `t`: playing, or a command's run that
   *  is not stopped before a wave. */
  function loops(t: Time): boolean {
    return t.kind === 'playing' || t.kind === 'settled' || t.kind === 'per-wave';
  }

  /** Set what time does, starting or stopping the play loop to match. */
  function setTime(t: Time): void {
    time = t;
    if (!loops(t)) {
      cancelAnimationFrame(rafId);
      rafId = 0;
    } else if (rafId === 0) {
      lastWall = 0;
      carry = 0;
      period = null;
      rafId = requestAnimationFrame(frame);
    }
  }

  /** The reader's Play: time runs freely, ending a bounded run or a stop at
   *  its cap; a wave Propagation's run stood before is sent first. */
  function play(): void {
    if (run === null || busy) return;
    if (time.kind !== 'before-wave') {
      runFreely();
      return;
    }
    void exec(async (r) => {
      await sendHeld(r);
      runFreely();
    });
  }

  /** Time runs freely; when nothing is coming, the narration says so at once,
   *  replacing what a settled run left. */
  function runFreely(): void {
    setTime(PLAYING);
    if (quiet && label !== null) narration = IDLE;
  }

  /** The reader's Pause: time stops, and a bounded run ends. A run stopped
   *  before a wave stays there, since its engine still holds the wave: only
   *  Step, Play or a command sends it. */
  function pause(): void {
    if (time.kind !== 'before-wave') setTime(PAUSED);
  }

  function toggle(): void {
    if (playing) pause();
    else play();
  }

  /** Step to the next event and draw it: the wave Propagation's run stands
   *  before, which it sends, the run playing on to its next; else, paused, the
   *  next wave, drawn at once with its counts, or the next instant hits leave
   *  the window, or one heartbeat period when neither is coming. */
  async function stepOnce(): Promise<void> {
    if (busy) return;
    pause();
    await exec(async (r) => {
      const t = time;
      if (t.kind === 'before-wave') {
        tellWave(r, t.held);
        await sendHeld(r);
        return;
      }
      await feed(r);
      const expiry = r.cluster.next_expiry_ms;
      const delta = expiry === undefined ? tickMs : expiry - r.cluster.virtual_ms;
      const batch = await r.sim.step(delta, EVENT);
      if (waves(batch) === 0) {
        await meet(r, batch);
        // A feed always has hits to send, whatever this instant's window holds.
        if (r.traffic === undefined && expiry === undefined && r.cluster.oracle_total === 0) narration = say.NOTHING_WAITING;
        return;
      }
      // Stopped before the wave: draw it at once, with its counts.
      note(r, batch);
      tellWave(r, batch);
      await deliverWave(r, batch);
    });
  }

  /** On Step, say what the wave does, where it is the selected node's own and
   *  no feed muddies it. */
  function tellWave(r: Run, batch: EventBatch): void {
    if (r.traffic !== undefined) return;
    const data = batch.events.flatMap(({ kind }) =>
      (kind.type === 'PacketSent' || kind.type === 'PacketDropped') && CARRIES_CELLS[kind.role] ? [kind] : [],
    );
    const own = data.filter((p) => KIND[p.role] === 'Own');
    const relays = data.filter((p) => KIND[p.role] === 'Relay');
    const zones = r.cluster.zones;
    const zoneOf = (id: number): number => r.cluster.nodes.find((n) => n.id === id)?.zone ?? -1;
    if (own.length > 0 && own.every((p) => p.src === r.selected)) {
      if (waited) {
        const peers = own.filter((p) => p.tier === 'InZone').length;
        const gateways = own.filter((p) => p.tier === 'CrossZone').map((p) => ({ node: p.dst, zone: zoneAt(zones, zoneOf(p.dst)) }));
        narration = say.waveOne(r.selected, zoneAt(zones, zoneOf(r.selected)), peers, gateways);
      } else {
        const counted = own.filter((p) => p.type === 'PacketSent');
        narration = say.waveOneCounted(
          counted.filter((p) => p.tier === 'InZone').map((p) => p.dst),
          counted.filter((p) => p.tier === 'CrossZone').map((p) => p.dst),
        );
      }
    } else if (relays.length > 0 && own.length === 0) {
      const gateways = [...new Set(relays.map((p) => p.src))].sort((a, b) => a - b);
      const lost = relays.filter((p) => p.type === 'PacketDropped').length;
      narration = say.waveTwo(gateways, relays.length - lost, dataCrossed, dataSent, lost);
    }
  }

  /** Commit what Requests per Send holds: a whole number in range becomes
   *  the Send's size; anything else disables Send until it is fixed. */
  function commitRequests(): void {
    const r = run;
    const n = /^\d+$/.test(requestsText.trim()) ? Number(requestsText.trim()) : Number.NaN;
    requestsCommitted = requestsText;
    requestsValid = Number.isSafeInteger(n) && n >= BURST_REQUESTS_MIN && n <= BURST_REQUESTS_MAX;
    if (requestsValid && r !== null) r.requests = n;
  }

  /** The caption's fix: back to the scenario's own Requests per Send. */
  function useRequests(requests: number): void {
    requestsText = String(requests);
    commitRequests();
  }

  function selectPreset(preset: Preset): void {
    if (defaults === null || preset.id === run?.preset.id) return;
    void start(preset, knobsFromPreset(preset, defaults), BURST_REQUESTS);
  }

  const go = (to: PresetId): void => selectPreset(presetById(to));
  const presetLabel = (id: PresetId): string => presetById(id).label;

  function restart(): void {
    if (run !== null) void start(run.preset, run.applied, run.requests, run.knobs);
  }

  function rebuild(): void {
    if (run !== null) void start(run.preset, run.knobs, run.requests);
  }

  function backToScenario(): void {
    if (run !== null && defaults !== null) run.knobs = knobsFromPreset(run.preset, defaults);
  }

  /** The page's own keys, only from the page itself: Space plays or pauses, →
   *  steps, Escape returns the selection to the scenario's node. */
  function onKeydown(event: KeyboardEvent): void {
    const target = event.target;
    if (target !== document.body && !(target instanceof HTMLElement && target.classList.contains('stage'))) return;
    if (event.key === ' ') {
      event.preventDefault();
      toggle();
    } else if (event.key === 'ArrowRight') {
      event.preventDefault();
      void stepOnce();
    } else if (event.key === 'Escape' && run !== null) {
      void select(subjectOf(run));
    }
  }

  /** The pane's tabs: ← and → move between them; Enter or Space activates. */
  function paneTabKey(event: KeyboardEvent): void {
    if (event.key !== 'ArrowLeft' && event.key !== 'ArrowRight') return;
    event.preventDefault();
    const other = (event.currentTarget as HTMLElement).id === 'pane-tab-details' ? 'messages' : 'details';
    document.getElementById(`pane-tab-${other}`)?.focus();
  }

  /** The words for the cut that blocks the link from `src` to `dst`, if one
   *  does. */
  function cutWordsOf(r: Run, src: number, dst: number): string | undefined {
    const zoneOf = (id: number): number | undefined => r.cluster.nodes.find((n) => n.id === id)?.zone;
    for (const c of r.cuts) {
      if (c.kind === 'silenced' && (c.node === src || c.node === dst)) return `node ${c.node} is silenced`;
      if (c.kind === 'zone' && (zoneOf(src) === c.zone) !== (zoneOf(dst) === c.zone)) return `${zoneAt(r.cluster.zones, c.zone)} is cut off`;
      if (c.kind === 'unheard-in-zone' && c.node === src && zoneOf(dst) === c.zone)
        return `node ${c.node} is cut off from ${zoneAt(r.cluster.zones, c.zone)}`;
    }
    return undefined;
  }

  /** "Skip to the next step": focus the filled control, or the action line in
   *  a wait. */
  function skipToNext(): void {
    const target = document.querySelector<HTMLElement>('[data-primary]') ?? document.getElementById('next-action');
    target?.focus();
  }

  // -- What the page shows, derived from the run --------------------------

  // A gateway story's gateway: the one it broke, or before its fault the
  // origin's gateway into the story's zone as the cluster warmed.
  const storyGateway = $derived.by((): number | null => {
    const s = run?.story;
    if (s === undefined || s.kind !== 'gateway') return null;
    return s.act === 'faulted' || s.act === 'done' ? s.gateway : (run?.gateways[s.origin]?.[s.zone] ?? null);
  });
  // Every fault and repair is offered only once the story has ended, or
  // where it has no scripted end.
  const offered = $derived(run !== null && storyDone(run.story));

  const view = $derived.by((): RunView | null => {
    const r = run;
    if (r === null || selectedNode === null) return null;
    const from = r.cluster.nodes.indexOf(selectedNode) * buckets;
    let strata = 0;
    for (let b = from; b < from + buckets; b++) if ((r.cluster.window_buckets[b] ?? 0) > 0) strata += 1;
    return {
      story: r.story,
      selected: r.selected,
      selectedZone: selectedNode.zone,
      nodes: r.cluster.nodes,
      brim: selectedNode.brim,
      subjectCount: selectedNode.window_total,
      subjectStrata: strata,
      cutZone,
      repair: heading?.repair ?? null,
      clock: time.kind,
      busy,
      requests: editingRequests ? 'editing' : requestsValid ? 'valid' : 'invalid',
      defaultRequests: BURST_REQUESTS,
      oracle: r.cluster.oracle_total,
      virtualMs: r.cluster.virtual_ms,
      bucketNow: r.cluster.bucket_epoch_now,
      bucketMs,
      limit,
      windowMs: r.applied.rule_window_ms,
      zoneCount: r.cluster.zones.filter((z) => z.live > 0).length,
      loss: r.applied.uniform_loss,
      zones: r.cluster.zones,
      gateway: storyGateway,
      unanswered: inspected?.peers.find((p) => p.id === storyGateway && storyGateway !== null)?.unanswered_ms ?? null,
      suspectAfterMs: r.cluster.suspect_after_ms,
      failovers: failoversOf(r, r.cluster),
    };
  });
  const action = $derived<NextAction>(view === null ? { kind: 'wait', reason: 'starting' } : nextAction(view));
  const primary = $derived(action.kind === 'press' ? action.control : null);
  const goal = $derived(view === null ? [] : goalOf(view));
  const send = $derived(sendLabel(width, requestsValid && run !== null ? run.requests : null, run?.selected ?? 0));
  // The next action's control label, as the control renders it: the action
  // line and the narration quote it.
  const zoneName = (z: number): string => zoneAt(cluster?.zones ?? [], z);
  const label = $derived(actionLabel(action, send, presetLabel, zoneName));
  const waitText = $derived(view === null || action.kind !== 'wait' ? [] : waitClause(action.reason, view));
  const lineClause = $derived(
    view === null || action.kind !== 'press' ? [] : pressClause(view, action.control, hops + 1),
  );
  const requestsReason = $derived<Blurb>(
    requestsCommitted.trim() === ''
      ? ['Requests per Send is empty']
      : blurb`Requests per Send must be a whole number from ${BURST_REQUESTS_MIN} to\u00a0${BURST_REQUESTS_MAX}`,
  );

  // Nothing is coming: no feed, no hits held for a heartbeat, and no node a
  // wave can still reach. The hits that leave the window first, if any.
  const quiet = $derived(
    run !== null && run.traffic === undefined && run.held === null && !run.cluster.nodes.some((n) => n.lag.kind === 'Behind'),
  );
  const leaving = $derived(cluster?.oracle_buckets.find((n) => n > 0) ?? null);
  // Time runs, nothing is coming, and the next action names a control.
  const nothingComing = $derived(time.kind === 'playing' && quiet && label !== null);
  // The idle narration, derived from the run as it is now: while time runs
  // with nothing coming, that, with the control to press; while the selected
  // node holds hits, when they leave; else what the selection means.
  const idleText = $derived.by((): Blurb => {
    const r = run;
    if (r === null) return [];
    if (nothingComing && label !== null) return say.nothingComing(label, leaving);
    if (held !== null) return say.holding(held.node, held.hits);
    return idleOf(r);
  });
  const said = $derived(narration === IDLE ? idleText : narration);
  // The countdown beside the narration, in 100 ms slots: to the selected
  // node's heartbeat while it holds hits (not beside the start sentence), or
  // to the window's edge while nothing else is coming.
  const countdown = $derived(
    held !== null && selectedNode !== null && narration !== atStart
      ? selectedNode.next_heartbeat_ms - virtualMs
      : narration === IDLE && nothingComing && leaving !== null
        ? (cluster?.next_expiry_ms ?? virtualMs) - virtualMs
        : null,
  );
  const heading = $derived(
    run === null
      ? null
      : headingOf({
          nodes: run.cluster.nodes,
          oracle: run.cluster.oracle_total,
          limit,
          onScale: scaled,
          newer: newerHits(run),
          cuts: run.cuts,
          zones: run.cluster.zones,
          selected: run.selected,
          brim: selectedNode?.brim,
          emptied,
          badge,
          tickMs,
        }),
  );

  /** The feed's hits younger than the grace, or null with no feed. */
  function newerHits(r: Run): number | null {
    const traffic = r.traffic;
    if (traffic === undefined) return null;
    const fed = (t: number): number => Math.floor((traffic.rate_per_sec * Math.max(t - r.feedFrom, 0)) / 1000);
    return fed(r.cluster.virtual_ms) - fed(r.cluster.virtual_ms - grace);
  }

  // A wait's meter: its progress since it began, in [0, 1] and in words.
  const meter = $derived.by((): { value: number; text: string } | null => {
    if (action.kind !== 'wait' || view === null) return null;
    const reason = action.reason;
    if (waitStart?.reason !== reason) {
      waitStart = {
        reason,
        virtualMs: view.virtualMs,
        reopensMs: view.brim.kind === 'Rejected' ? view.brim.reopens_ms : 0,
      };
    }
    const value = progress(view, reason, waitStart);
    if (value === null) return null;
    const text =
      reason === 'run'
        ? `${Math.round(value * 100)} percent of the reachable nodes have the hits`
        : reason === 'failover'
          ? plain(waitText)
          : `${Math.round(value * 100)} percent of the wait has passed`;
    return { value, text };
  });

  // The lede: what the cluster is, with the faults listed.
  const zoneCount = $derived(cluster?.zones.filter((z) => z.live > 0).length ?? 0);


  onMount(() => {
    void (async () => {
      try {
        defaults = await Sim.defaultConfig();
      } catch (e) {
        firstError = e instanceof Error ? e.message : String(e);
        busy = false;
        return;
      }
      await start(DEFAULT_PRESET, knobsFromPreset(DEFAULT_PRESET, defaults), BURST_REQUESTS);
    })();
    const unwatch = watchTheme(() => (theme = readTheme()));
    const unwidth = watchWidth((w) => {
      if ((w === 'narrow') !== (width === 'narrow')) controlsOpen = w !== 'narrow';
      width = w;
    });
    return () => {
      cancelAnimationFrame(rafId);
      unwatch();
      unwidth();
      dropPrevious();
    };
  });
</script>

<svelte:window onkeydown={onKeydown} />

<button class="btn-secondary skip-link" onclick={skipToNext}>Skip to the next step</button>

<div class="layout">
  <header>
    <h1>Gabion · <span class="subtitle">a distributed rate limiter</span></h1>
    {#if cluster !== null && run !== null}
      <p class="lede">
        <span data-field="status-nodes">{cluster.nodes.length}</span>
        {noun(cluster.nodes.length, 'node')}{#if run.applied.zone_mode === 'Configured'}{' '}in
          <span data-field="status-zones">{zoneCount}</span> {noun(zoneCount, 'zone')}{:else}, no zone configured,{/if}
        {#if scaled}
          enforce one rate limit: <span data-field="status-limit">{fmtCount(limit)}</span> hits per
          <span data-field="status-window">{run.applied.rule_window_ms / 1000} s</span>
        {:else}
          share one rate limit
        {/if}{#each run.cuts as c, i (i)}{' · '}<Sentence parts={cutName(c, cluster.zones)} field="status-cut" />{/each}
      </p>
    {/if}
    <p class="once">
      <span class="one-run">One simulated run ·</span>
      <a href={`${import.meta.env.BASE_URL}study/dashboard/`}>measured results in the study</a>
    </p>
    <button class="btn-tertiary help" aria-expanded={helpOpen} aria-controls="glossary" onclick={() => (helpOpen = !helpOpen)}
      >How to read this</button
    >
    {#if helpOpen}
      <dl class="glossary serif" id="glossary">
        <dt>node</dt>
        <dd>
          A vessel holding the hits it counts in its window, the count in its middle, and its name, "node 4", beside it. Each
          node counts its own clients' hits; gossip spreads every node's count to the others.
        </dd>
        <dt>selected node</dt>
        <dd>
          <RoleGlyph kind="selected" /> The node Send goes to. Each node picks its own gateway into each other zone, so the ports
          change when you select another node.
        </dd>
        <dt>zone</dt>
        <dd>An availability zone. A node's zone is whatever the node says it is.</dd>
        <dt>inside a zone</dt>
        <dd>Delivery between nodes of one zone: a thin arrow that stays inside its ring.</dd>
        <dt>across zones</dt>
        <dd>Delivery between zones: a thick arrow that crosses the empty centre.</dd>
        <dt>packet</dt>
        <dd>
          A line from sender to receiver, drawn the instant it is sent and fading over one simulated second: Pause holds it, Speed
          stretches it. Data ends in an arrow; an answer in a dot, a message that asks for one in a ring, and a gap report in a
          square.
        </dd>
        <dt>message</dt>
        <dd>One packet one node sent another. Messages, beside Details at the top of the pane, lists each one.</dd>
        <dt>own data</dt>
        <dd>A node's own changes to its count.</dd>
        <dt>relay data</dt>
        <dd>Other nodes' changes a gateway passes on into its zone.</dd>
        <dt>skip</dt>
        <dd>Changes that expired before they were sent, so the peer can count them as received.</dd>
        <dt>gateway</dt>
        <dd>
          <RoleGlyph kind="port" /> The member of a remote zone that the selected node sends its hits to. It relays them into its
          zone, from the port on its rim.
        </dd>
        <dt>relay</dt>
        <dd>A gateway forwarding hits it received to the rest of its zone.</dd>
        <dt>ack</dt>
        <dd>"I have your changes through position N." One number acknowledges everything below it.</dd>
        <dt>repeat</dt>
        <dd>Data sent again, after a gap report or after waiting one heartbeat for an ack.</dd>
        <dt>probe</dt>
        <dd>"Answer me", which is also how a node greets a peer it has not heard.</dd>
        <dt>probe answer</dt>
        <dd>The answer to a probe, carrying the answering node's acks.</dd>
        <dt>suspect</dt>
        <dd>
          <RoleGlyph kind="suspect" /> A peer that left something unanswered for the suspect timeout, or a remote peer that said
          it cannot relay. A suspect is not used as a gateway.
        </dd>
        <dt>gap report</dt>
        <dd>Gabion's NAK: "I am missing your changes from position N on; repeat them".</dd>
        <dt>cannot relay</dt>
        <dd>A flag a node sets on every message while it cannot pass hits on into its zone; only a node in another zone acts on it.</dd>
        <dt>join sweep</dt>
        <dd>
          A one-off copy of a node's whole live store, sent to a zone member that just joined: a sweep request asks for it, and
          sweep pages carry it.
        </dd>
        <dt>position</dt>
        <dd>A node's changes are numbered in order; one change can carry many hits.</dd>
        <dt>start</dt>
        <dd>Which start of a node sent a message: a node that restarts counts again from a new start.</dd>
        <dt>in flight</dt>
        <dd>
          In flight is the simulation's own state, while Step has stopped before a wave; a picture of a packet can still be
          moving after it has arrived.
        </dd>
        <dt>lost</dt>
        <dd>A message the network dropped: on a cut link, on a link that drops packets, or at a full queue.</dd>
        <dt>ladder</dt>
        <dd>Two nodes' conversation; time runs down, and a slanted rung shows order, not delay.</dd>
        <dt>the frame</dt>
        <dd>A message's bytes as gabion encodes them: an 86-byte header, then, with data, the cells.</dd>
        <dt>How the controls work</dt>
        <dd>
          A Send is a burst of one-hit requests, as many as Requests per Send says: the node admits them in order while its
          window has room and rejects the rest.
          The one dark button is what to press next, and the line under the goal names it; when nothing is dark, watch the
          meter. A Send pressed while paused plays until every node has the hits, then pauses again; Space plays or pauses, →
          steps to the next event, and Escape returns the selection to the scenario's node. Restart keeps your settings, a
          scenario button takes its own, and Undo brings back the run you left. In Messages: ↑ ↓ choose a message · Enter
          reads it · Esc goes back.
        </dd>
        <dt>backstop</dt>
        <dd>A gateway that can reach the sender but not its own zone says it cannot relay, and the sender moves to another gateway.</dd>
      </dl>
    {/if}
  </header>

  <main>
    {#if firstError !== null}
      <div class="overlay error" role="alert">
        <strong>The simulation could not start.</strong>
        <span>{firstError}</span>
        <span>Reloading starts it again; if this persists, the browser may not run WebAssembly.</span>
        <button data-primary="" onclick={() => location.reload()}>Reload the page</button>
      </div>
    {:else if run === null || cluster === null || heading === null || defaults === null}
      <div class="overlay">Starting the simulator…</div>
    {:else}
      {@const r = run}
      <div class="workspace" aria-busy={busy}>
        <details class="controls" bind:open={controlsOpen}>
          <summary>Controls</summary>
          <ControlRail
            presets={PRESETS}
            activeId={r.preset.id}
            bind:requestsText
            {requestsValid}
            editing={editingRequests}
            zones={cluster.zones}
            {maxZones}
            cuts={r.cuts}
            {offered}
            headingRepair={heading.repair !== null}
            maxNodes={MAX_LIVE_NODES}
            knobs={r.knobs}
            appliedKnobs={r.applied}
            bucketMs={defaults.rule_bucket_ms}
            onSelectPreset={selectPreset}
            onRestart={restart}
            onCommitRequests={commitRequests}
            onEditRequests={(on) => (editingRequests = on)}
            onRevertRequests={() => (requestsText = requestsCommitted)}
            onApplyKnobs={rebuild}
            onBackToScenario={backToScenario}
            onCutZone={(zone) => void cut({ kind: 'zone', zone })}
            onReconnect={(c) => void heal(c)}
            onHeal={() => void heal()}
            onAddNode={(to) => void addNode(to)}
            onPreview={(node) => (namedId = node)}
          />
        </details>
        <section
          class="stage-pane"
          aria-label="Stage"
          aria-describedby="next-action"
          data-clock={time.kind}
          data-playing={playing ? '1' : '0'}
        >
          <StageCaption
            {goal}
            {action}
            {label}
            pressClause={lineClause}
            phone={width === 'narrow'}
            {waitText}
            requests={r.requests}
            {requestsReason}
            {zoneName}
            {meter}
            {presetLabel}
            onSelect={(node) => void select(node)}
            onFault={(fault, node, zone) => void breakGateway(fault, node, zone)}
            onUseRequests={useRequests}
            onGo={go}
            onPreview={(node) => (namedId = node)}
          />
          <StageHeading {heading} repairPrimary={action.kind === 'repair'} onRepair={() => void heal()} />
          {#if commandError !== null}
            <p class="command-error" role="alert">
              {commandError}
              <button class="btn-secondary" onclick={() => (commandError = null)}>Dismiss</button>
            </p>
          {/if}
          <div class="narration-row">
            <p class="narration">
              <span aria-live="polite" aria-atomic="true"><Sentence parts={said} field="narration" /></span
              >{#if countdown !== null && countdown >= 0}<span class="countdown">, in <span data-field="countdown"
                    >{fmtSeconds(countdown)}</span
                  ></span
                >{/if}
            </p>
            {#if previous !== null}
              <button class="btn-secondary undo" onclick={() => void undo()}>Undo</button>
            {/if}
          </div>
          <Stage
            bind:this={stage}
            {cluster}
            {roles}
            {scale}
            {grace}
            {faults}
            {awaiting}
            previewId={namedId}
            {decisions}
            phone={width === 'narrow'}
            {speed}
            {hoveredId}
            {focusedId}
            {theme}
            onSelect={(node) => void select(node)}
            onEscape={() => void select(subjectOf(r))}
            onHover={(node) => (hoveredId = node)}
            onFocus={(node) => (focusedId = node)}
          />
        </section>
        <div class="detail-rail">
          <div class="rail-body">
            {#if selectedNode !== null}
              <div class="pane-head">
                <h2 class="pane-title">
                  node <span data-field="node">{selectedNode.id}</span>
                  <span class="pane-sub">selected · {zoneAt(cluster.zones, selectedNode.zone)}</span>
                  {#if selectedNode.id !== subjectOf(r)}
                    <span class="pane-esc">Esc returns to node <span data-field="esc-node">{subjectOf(r)}</span></span>
                  {/if}
                </h2>
                <div class="pane-tabs" role="tablist" aria-label="Pane view">
                  {#each [['details', 'Details'], ['messages', 'Messages']] as const as [id, label] (id)}
                    <button
                      class="pane-tab"
                      id={`pane-tab-${id}`}
                      role="tab"
                      aria-selected={r.pane === id}
                      aria-controls={`pane-${id}`}
                      tabindex={r.pane === id ? 0 : -1}
                      onclick={() => (r.pane = id)}
                      onkeydown={paneTabKey}>{label}</button
                    >
                  {/each}
                </div>
              </div>
              {#if r.pane === 'messages'}
                <div id="pane-messages" class="messages-panel" role="tabpanel" aria-labelledby="pane-tab-messages">
                  <Messages
                    sim={r.sim}
                    selected={r.selected}
                    bind:view={r.messages}
                    version={r.chartVersion}
                    live={r.traffic !== undefined}
                    nodes={cluster.nodes}
                    zones={cluster.zones}
                    cutWords={(src, dst) => cutWordsOf(r, src, dst)}
                    onPreview={(node) => (namedId = node)}
                  />
                </div>
              {:else}
              <div id="pane-details" role="tabpanel" aria-labelledby="pane-tab-details">
              <Charts
                {cluster}
                history={r.history}
                version={r.chartVersion}
                sends={r.sends}
                {limit}
                charts={r.preset.charts}
                onScale={onScale(limit, r.requests)}
                timeBins={r.preset.traffic !== undefined}
                {bucketMs}
                selected={r.selected}
                {theme}
              />
              {#if inspected !== null && inspected.id === selectedNode.id}
                <NodeInspector
                  node={selectedNode}
                  peers={inspected.peers}
                  zones={cluster.zones}
                  zoneMode={r.applied.zone_mode}
                  {offered}
                  cuts={r.cuts}
                  onSilence={(node) => void cut({ kind: 'silenced', node })}
                  onCutFromZone={(node, zone) => void cut({ kind: 'unheard-in-zone', node, zone })}
                  onPreview={(node) => (namedId = node)}
                  onMessages={() => (r.pane = 'messages')}
                />
              {/if}
              <div class="pane-foot">
                <button
                  class="btn-secondary"
                  disabled={cluster.nodes.length <= 1}
                  aria-describedby={cluster.nodes.length <= 1 ? 'remove-reason' : undefined}
                  onclick={() => void removeNode(selectedNode.id)}
                  onpointerenter={() => (namedId = selectedNode.id)}
                  onpointerleave={() => (namedId = null)}
                  onfocus={() => (namedId = selectedNode.id)}
                  onblur={() => (namedId = null)}>Remove node <span data-field="node">{selectedNode.id}</span></button
                >
                {#if cluster.nodes.length <= 1}<span class="reason" id="remove-reason">a cluster needs one node</span>{/if}
              </div>
              </div>
              {/if}
            {/if}
          </div>
        </div>
        {#if busy}
          <div class="overlay rebuilding">Rebuilding…</div>
        {/if}
      </div>
    {/if}
  </main>

  <footer>
    <TransportBar
      {playing}
      {busy}
      bind:speed
      {virtualMs}
      {heartbeatWallMs}
      sendLabel={send}
      sendValid={requestsValid}
      {primary}
      bounded={playing && (time.kind === 'settled' || time.kind === 'per-wave')}
      onToggle={toggle}
      onStep={() => void stepOnce()}
      onSend={() => void sendBurst()}
      onSendHover={(on) => (namedId = on ? (run?.selected ?? null) : null)}
    />
  </footer>
</div>

<style>
  .layout {
    display: grid;
    grid-template-rows: auto 1fr auto;
    height: 100%;
  }

  /* One row from 1100 px: the title, the lede, then the study and the help
     toggle at the right. */
  header {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: var(--gap-xs) var(--gap-md);
    padding: var(--gap-sm) var(--gap-lg);
    border-bottom: 1px solid var(--color-border-line);
  }

  h1 {
    margin: 0;
    font-size: var(--font-size-body);
    font-weight: 600;
  }

  .subtitle {
    color: var(--color-fg-muted);
    font-weight: 400;
  }

  .lede {
    margin: 0;
    font-size: var(--font-size-body);
    color: var(--color-fg);
  }

  .lede :global([data-field]) {
    font-weight: 600;
  }

  .once {
    margin: 0 0 0 auto;
    font-size: var(--font-size-caption);
    color: var(--color-fg-muted);
  }

  .once a {
    color: var(--color-fg);
  }

  /* Below 1280 px the lede and its listed cuts need the row: the link alone
     keeps the header one row. */
  @media (width < 1280px) {
    .one-run {
      display: none;
    }
  }

  .help {
    min-height: 24px;
    font-size: var(--font-size-caption);
  }

  .glossary {
    flex-basis: 100%;
    display: grid;
    grid-template-columns: max-content minmax(0, 80ch);
    gap: var(--gap-xs) var(--gap-md);
    max-height: 40vh;
    margin: var(--gap-sm) 0 0;
    overflow-y: auto;
    font-size: var(--font-size-caption);
  }

  .glossary dt {
    font-weight: 600;
  }

  .glossary dd {
    margin: 0;
  }

  main {
    position: relative;
    min-height: 0;
  }

  /* The three-pane chassis: the controls rail, the dominant stage, and the
     detail pane. Three columns from 1100 px; two from 640 px (stage over the
     controls on the left, the detail pane the full height on the right); one
     scrolling column below. */
  .workspace {
    display: grid;
    grid-template-columns: clamp(200px, 16%, 260px) minmax(0, 1fr) clamp(300px, 28%, 380px);
    grid-template-areas: 'rail stage detail';
    height: 100%;
    min-height: 0;
  }

  /* A flex column so the open rail fills the row: a percentage height does not
     resolve through the disclosure's content box, a flex item's size does. */
  .controls {
    grid-area: rail;
    display: flex;
    flex-direction: column;
    min-height: 0;
  }

  .controls::details-content {
    flex: 1;
    min-height: 0;
  }

  /* Phone: the disclosure's bar above the stage. */
  .controls > summary {
    padding: var(--gap-sm) var(--gap-md);
    background: var(--color-panel);
    border-bottom: 1px solid var(--color-border-line);
    font-weight: 600;
  }

  /* The stage stack: the caption's goal and action line, the heading, the
     narration, then the canvas, which takes the rest, and its key. */
  .stage-pane {
    grid-area: stage;
    position: relative;
    display: flex;
    flex-direction: column;
    gap: var(--gap-sm);
    min-width: 0;
    min-height: 0;
    padding: var(--gap-md) var(--gap-lg) var(--gap-sm);
  }

  .stage-pane > :global(.stage-box) {
    flex: 1;
  }

  .narration-row {
    display: flex;
    align-items: flex-start;
    gap: var(--gap-sm);
  }

  .narration {
    max-width: 34em;
    min-height: 2.5rem;
    margin: 0;
    font-size: var(--font-size-caption);
    line-height: 1.43;
    color: var(--color-fg);
  }

  .narration :global([data-field]) {
    font-weight: 600;
  }

  .undo {
    min-height: 28px;
    padding: 0 var(--gap-sm);
    font-size: var(--font-size-caption);
  }

  .command-error {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: var(--gap-sm);
    margin: 0;
    font-size: var(--font-size-caption);
    color: var(--color-bad);
  }

  .command-error button {
    min-height: 24px;
    padding: 0 var(--gap-sm);
    font-size: var(--font-size-caption);
  }

  /* The detail pane: the selected node's title, the scenario's charts, its
     own sections and Remove at the foot, one column that scrolls. */
  .detail-rail {
    grid-area: detail;
    display: flex;
    flex-direction: column;
    min-width: 0;
    min-height: 0;
    height: 100%;
    padding: var(--gap-md);
    background: var(--color-panel);
    border-left: 1px solid var(--color-border-line);
  }

  .rail-body {
    display: flex;
    flex-direction: column;
    gap: var(--gap-md);
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    container-type: size;
  }

  .messages-panel {
    flex: 1 1 0;
    min-height: 0;
    display: flex;
    flex-direction: column;
  }

  .pane-head {
    display: flex;
    align-items: flex-start;
    gap: var(--gap-sm);
  }

  .pane-title {
    flex: 1;
    min-width: 0;
    margin: 0;
    font-size: var(--font-size-body);
    font-weight: 600;
  }

  /* A narrow pane gives the title its own line and sets the tabs below it. */
  @container (width < 320px) {
    .pane-head {
      flex-wrap: wrap;
    }

    .pane-title {
      flex-basis: 100%;
    }
  }

  /* Details · Messages: a segmented control on the thing it switches. The
     selected segment is tinted and ruled, never filled: ink fill is the
     primary's alone. */
  .pane-tabs {
    display: inline-flex;
    flex: none;
  }

  .pane-tab {
    min-height: 28px;
    padding: 0 8px;
    border: 1px solid var(--chart-axis);
    border-radius: 6px 0 0 6px;
    background: var(--color-panel);
    color: var(--color-fg);
    font-size: var(--font-size-caption);
    font-weight: 600;
    cursor: pointer;
  }

  .pane-tab + .pane-tab {
    margin-left: -1px;
    border-radius: 0 6px 6px 0;
  }

  .pane-tab[aria-selected='true'] {
    background: var(--color-bg);
    box-shadow: inset 0 -2px 0 var(--color-fg);
  }

  .pane-sub,
  .pane-esc {
    font-size: var(--font-size-caption);
    font-weight: 400;
    color: var(--color-fg-muted);
  }

  .pane-esc {
    float: right;
    line-height: 24px;
  }

  .pane-foot {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: var(--gap-sm);
  }

  .pane-foot button {
    min-height: 32px;
    padding: 0 var(--gap-md);
    font-size: var(--font-size-caption);
  }

  .reason {
    font-size: var(--font-size-caption);
    color: var(--color-fg-muted);
  }

  @media (width >= 640px) {
    .controls > summary {
      display: none;
    }

    .detail-rail {
      overflow: hidden;
    }
  }

  /* Two columns that scroll as a page: the stage over the controls on the
     left, the detail pane on the right. */
  @media (640px <= width < 1100px) {
    .workspace {
      grid-template-columns: minmax(0, 1fr) clamp(220px, 32%, 280px);
      grid-template-areas:
        'stage detail'
        'rail detail';
      grid-template-rows: auto auto;
      height: auto;
    }

    .rail-body {
      flex: none;
      height: 48rem;
    }

    /* The detail pane runs the height of both rows. */
    .detail-rail .rail-body {
      flex: 1;
      height: auto;
    }
  }

  /* One scrolling column. `main` keeps a
     height so the absolutely positioned overlays stay visible; the square
     stage keeps the ring the same shape at every width. The rail body gets a
     definite height, so its scroller keeps a definite box under an
     auto-height parent. */
  @media (width < 640px) {
    main {
      min-height: 50vh;
    }

    .rail-body {
      flex: none;
      height: 32rem;
    }

    .workspace {
      grid-template-columns: minmax(0, 1fr);
      grid-template-areas:
        'rail'
        'stage'
        'detail';
      height: auto;
    }

    .stage-pane {
      padding: var(--gap-md);
    }

    .glossary {
      grid-template-columns: minmax(0, 1fr);
    }
  }

  .overlay {
    position: absolute;
    inset: 0;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: var(--gap-sm);
    color: var(--color-fg-muted);
    font-size: var(--font-size-caption);
  }

  /* A rebuild covers the workspace without unmounting it, so the rail's open
     disclosure and the keyboard focus survive it. */
  .overlay.rebuilding {
    background: color-mix(in srgb, var(--color-bg) 70%, transparent);
    color: var(--color-fg);
  }

  .overlay.error {
    color: var(--color-bad);
    padding: var(--gap-lg);
    text-align: center;
  }

  .overlay.error strong {
    color: var(--color-fg);
  }

  .overlay.error button {
    min-height: 36px;
    padding: 0 var(--gap-md);
  }
</style>
