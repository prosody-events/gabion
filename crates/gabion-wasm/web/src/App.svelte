<script lang="ts">
  import { onMount } from 'svelte';
  import { blurb, fmtCount, fmtSeconds, joined, noun, zoneLabel, type Blurb } from './lib/format';
  import { Sim } from './lib/sim/sim';
  import { readTheme, watchTheme } from './lib/theme';
  import {
    ANSWER,
    CARRIES_CELLS,
    isWave,
    type Admission,
    type ClusterState,
    type EventBatch,
    type Inspected,
    type PeerView,
    type SimConfig,
    type StepUntil,
    type Watch,
  } from './lib/sim/types';
  import type { Fault, Roles, Scale, Trail } from './lib/stage/renderer';
  import {
    DEFAULT_PRESET,
    PRESETS,
    WATCHED_KEY,
    BURST_HITS_MAX,
    BURST_HITS_MIN,
    knobsFromPreset,
    presetById,
    warm,
    type Knobs,
    type Preset,
    type SustainedTraffic,
  } from './lib/presets';
  import { nominalBuckets } from './lib/buckets';
  import { cutName, cutState } from './lib/cuts';
  import { sendLabel, watchWidth, widthNow, type Width } from './lib/controls';
  import { heading as headingOf, type Badge } from './lib/heading';
  import { onScale } from './lib/limit';
  import {
    goal as goalOf,
    nextAction,
    observe,
    pressClause,
    progress,
    type Flight,
    type NextAction,
    type PresetId,
    type RunView,
    type UntilSettled,
    type WaitStart,
  } from './lib/next';
  import * as say from './lib/narration';
  import { Run, UNDO_MS } from './lib/run.svelte';
  import Stage from './lib/components/Stage.svelte';
  import type { Decision } from './lib/components/NodeGrid.svelte';
  import Dashboard from './lib/components/Dashboard.svelte';
  import NodeInspector from './lib/components/NodeInspector.svelte';
  import BucketStrata from './lib/components/BucketStrata.svelte';
  import ControlRail from './lib/components/ControlRail.svelte';
  import TransportBar from './lib/components/TransportBar.svelte';
  import StageHeading from './lib/components/StageHeading.svelte';
  import StageCaption from './lib/components/StageCaption.svelte';
  import RoleGlyph from './lib/components/RoleGlyph.svelte';
  import Sentence from './lib/components/Sentence.svelte';

  // The session runs one scenario at a time (`lib/presets.ts`), as a `Run`: a
  // warmed cluster, its seed, and everything the page keeps about it. Play
  // advances simulated time, Step runs it to the next event, and Send submits
  // a request of Hits per Send at the selected node. A scenario button, Restart
  // and Rebuild each build a fresh run first, then swap it in; Undo swaps the
  // previous run back.
  //
  // Every engine call runs through one queue (`serial`), one at a time. In a
  // `'held'` preset a batch that holds a wave of data packets is held: the
  // engine keeps the wave in flight, the page shows the senders' snapshot and
  // draws the wave, and it calls the engine again only once the stage reports
  // the wave landed (`onLanded`); then `step(0)` delivers it and the receivers'
  // counts change. So no count changes before its packet lands, and every
  // readout is the engine's own state. A command that changes the cluster
  // first fast-forwards the wave in flight and its relays (`land`), then shows
  // its own effect; a rebuild discards the wave instead. The `'live'` preset
  // shows every snapshot at once.
  //
  // One time model (C39): a Send, Heal or Add pressed while paused plays until
  // every node that waiting can reach has the hits and one heartbeat period
  // has passed since the last landing, so the acknowledgements have flown,
  // then pauses itself; in Propagation it also stops before each wave, which
  // Step flies. Pressed while playing, a command leaves time alone.
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

  // Where a held preset's steps stop: before a wave of data packets lands, or
  // where some node's window drops a bucket that holds hits.
  const EVENT: StepUntil = { Event: { key: WATCHED_KEY } };
  // Presentation-clock ms a Send's decision stands alone before its eager
  // flush flies.
  const ARRIVAL_MS = 250;
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

  let playing = $state(false);
  let speed = $state(1);
  let theme = $state.raw(readTheme());
  let width = $state<Width>(widthNow());
  // Below 640 px the rail folds into a "Controls" disclosure above the stage; at
  // and above it the rail is always open (its summary is hidden).
  const wide = window.matchMedia('(width >= 640px)');
  let controlsOpen = $state(wide.matches);
  let helpOpen = $state(false);

  // What every snapshot reads: the watched key, and the weight of the next Send
  // its selected node decides.
  const watchOf = (r: Run): Watch => ({ key: WATCHED_KEY, probe_hits: r.hits });

  // The run's clocks and bounds, from the knobs it was built with.
  const tickMs = $derived(run?.applied.tick_interval_ms ?? 0);
  const floorMs = $derived(run?.config.min_emit_interval_ms ?? 0);
  const bucketMs = $derived(run?.config.rule_bucket_ms ?? 1000);
  const cluster = $derived(run?.cluster ?? null);
  const virtualMs = $derived(cluster?.virtual_ms ?? 0);
  // A feed's newest hits, younger than one heartbeat period and the floor, do
  // not count a node as behind.
  const grace = $derived(run?.preset.traffic === undefined ? 0 : tickMs + floorMs);

  /** A wave the engine holds in flight: the page shows it and calls the engine
   *  again only once the stage reports it landed, which resolves `landed`.
   *  `seq` tells one hold from the next. Under Propagation's per-wave run a
   *  wave is held unflown until Step (`flight` `'waiting'`). */
  interface Held {
    readonly seq: number;
    readonly batch: EventBatch;
    readonly delay: number;
    readonly landed: Promise<void>;
    readonly land: () => void;
  }
  let held = $state.raw<Held | null>(null);
  let flight = $state<Flight>('none');
  let holds = 0;
  // Virtual ms one emit floor after the last held wave's instant: `land` runs
  // the run on to here. An instant the run has reached leaves nothing due.
  let floorUntil = 0;
  // The tail of the queue every engine call runs on.
  let queue: Promise<void> = Promise.resolve();
  // Whether landed routes stay drawn until the next command: where no feed runs.
  const trail = $derived<Trail>(run?.preset.traffic === undefined ? 'keep' : 'drop');

  // The bounded run a paused command started, where it started and its last
  // data landing, in virtual ms; and whether it stopped at its cap.
  let untilSettled = $state<UntilSettled>('off');
  let runStart = 0;
  let lastLanding = 0;
  let capped = $state(false);

  // The last Send: each node's decision text, the decision itself, and the
  // narration sentence it was told in. Cleared at the next command.
  let decisions = $state.raw<ReadonlyMap<number, Decision>>(new Map());
  let lastDecision = $state.raw<Admission | null>(null);
  let decided: { zone: number; admission: Admission } | null = null;
  let told: Blurb = [];
  // The node holding admitted hits it has not yet sent, whose heartbeat the
  // selection outline sweeps toward.
  let awaitingNode = $state<number | null>(null);
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

  // The narration: the page's one live region.
  let narration = $state.raw<Blurb>([]);

  // Hits per Send as typed, as last committed, whether that is valid, and
  // whether it is being typed.
  let hitsText = $state('25');
  let hitsCommitted = '25';
  let hitsValid = $state(true);
  let editingHits = $state(false);

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
  // Exactly one advance is ever in flight: the play loop will not issue the
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
  const scaled = $derived(run !== null && onScale(limit, run.hits));
  const buckets = $derived(run === null ? 1 : nominalBuckets(run.applied.rule_window_ms, bucketMs));
  const scale = $derived<Scale>({ limit, onScale: scaled, hits: run?.hits ?? 1, buckets });
  const faults = $derived.by((): ReadonlyMap<number, Fault> => {
    const out = new Map<number, Fault>();
    for (const c of run?.cuts ?? []) {
      if (c.kind === 'silenced') out.set(c.node, { kind: 'silenced' });
      else if (c.kind === 'unheard-in-zone') out.set(c.node, { kind: 'unheard', zone: c.zone });
    }
    return out;
  });
  const cutZone = $derived(run?.cuts.find((c) => c.kind === 'zone')?.zone ?? null);

  // The sweep toward the selected node's heartbeat, while it holds hits unsent.
  const awaiting = $derived.by((): number | null => {
    if (awaitingNode === null || selectedNode === null || awaitingNode !== selectedNode.id || tickMs === 0) return null;
    return 1 - (selectedNode.next_heartbeat_ms - virtualMs) / tickMs;
  });
  // Its countdown, beside the narration, in 100 ms slots.
  const heartbeatIn = $derived(
    awaitingNode === null ? null : (cluster?.nodes.find((n) => n.id === awaitingNode)?.next_heartbeat_ms ?? virtualMs) - virtualMs,
  );

  /** The scenario's node if live, else the lowest live node in its zone, else
   *  the lowest live node: where Escape and a removal take the selection. */
  function subjectOf(r: Run): number {
    const nodes = r.cluster.nodes;
    const home = r.preset.select;
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
  async function build(preset: Preset, k: Knobs, hits: number): Promise<Run> {
    const d = defaults ?? (defaults = await Sim.defaultConfig());
    if (maxZones === 0) maxZones = await Sim.maxZones();
    const config = effectiveConfig(preset, k, d);
    const fresh = await Sim.create(config);
    try {
      await preset.seed(fresh, config, await warm(fresh, config));
      // A seed's last admitted request leaves its sub-step open, its eager
      // flush (if any) held: deliver it now, so the first paint shows what
      // the engine holds and nothing lands undrawn later.
      await fresh.step(0);
      const snap = await fresh.snapshot(preset.select, { key: WATCHED_KEY, probe_hits: hits });
      return new Run(fresh, preset, config, { ...k }, snap, hits);
    } catch (e) {
      await fresh.shutdown();
      throw e;
    }
  }

  /** Start `preset` fresh with knobs `k` and Hits per Send `hits`: build it
   *  first, then pause, keep the run it replaces for Undo, and swap. */
  function start(preset: Preset, k: Knobs, hits: number, staged?: Knobs): Promise<void> {
    return serial(async () => {
      pause();
      busy = true;
      commandError = null;
      try {
        const next = await build(preset, k, hits);
        const old = run;
        if (staged !== undefined) next.knobs = { ...staged };
        if (old !== null) {
          await keep(old);
          narration = say.rebuilt(preset.label, discarded(old));
        }
        adopt(next);
        if (old === null) narration = idleOf(next);
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

  /** Keep `old` for Undo, closing the wave it holds first: its engine then
   *  holds nothing in flight, and its saved snapshot is the engine's own. */
  async function keep(old: Run): Promise<void> {
    if (held !== null) {
      await old.sim.step(0);
      old.cluster = await old.sim.snapshot(old.selected, watchOf(old));
    }
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
    held = null;
    flight = 'none';
    floorUntil = 0;
    untilSettled = 'off';
    capped = false;
    decisions = new Map();
    lastDecision = null;
    decided = null;
    awaitingNode = null;
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
    hitsText = String(r.hits);
    hitsCommitted = hitsText;
    hitsValid = true;
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
        ? [{ node: p.id, zone: zoneLabel(r.cluster.zones[p.zone]?.name ?? '') }]
        : [],
    );
    return say.idle(r.selected, gateways, onScale(r.applied.rule_limit, r.hits));
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
    const nextSend = snap.inspected?.next_send;
    r.progress = observe(r.progress, r.preset.story, {
      decided,
      nextSend,
      oracle: snap.oracle_total,
      bucket: snap.bucket_epoch_now,
      cuts: r.cuts.length,
      cutZone,
    });
    decided = null;
    const agreed = snap.nodes.every((n) => n.lag.kind === 'CaughtUp');
    if (!agreed) badge = null;
    else if (armed && snap.oracle_total > 0) {
      armed = false;
      badge = { hops, afterMs: waited ? snap.virtual_ms - sentAt : null };
    }
    // Hits leaving the window, told where no feed hides it.
    if (snap.oracle_total < before.oracle_total && r.preset.traffic === undefined) {
      const left = before.oracle_total - snap.oracle_total;
      emptied = snap.oracle_total === 0;
      badge = null;
      const reopened = before.inspected?.next_send.kind === 'Rejected' && nextSend?.kind === 'Admitted';
      narration = say.expiry(left, emptied, r.applied.rule_window_ms / 1000, r.selected, reopened ? r.hits : null);
    }
  }

  function frame(now: number): void {
    if (!playing || run === null) return;
    rafId = requestAnimationFrame(frame);
    if (stepping) return;
    // Virtual time waits while a wave flies, and the next advance does not
    // make up the wait.
    if (held !== null) {
      lastWall = now;
      return;
    }
    const wallDelta = lastWall === 0 ? tickMs / 6 : now - lastWall;
    lastWall = now;
    const deltaMs = Math.min(Math.round(wallDelta * speed), MAX_STEP_MS);
    if (deltaMs <= 0) return;
    stepping = true;
    void exec((r) => advance(r, deltaMs)).finally(() => {
      stepping = false;
    });
  }

  /** Run `task` after every engine call queued before it: no command can then
   *  deliver a wave the page is still drawing. */
  function serial(task: () => Promise<void>): Promise<void> {
    const next = queue.then(task);
    queue = next.catch(() => {});
    return next;
  }

  /** Run `task` on the live run, on the queue. A failure stops play, drops any
   *  hold (the engine's next command closes it) and says so in place. */
  function exec(task: (r: Run) => Promise<void>): Promise<void> {
    return serial(async () => {
      const r = run;
      if (r === null) return;
      try {
        await task(r);
      } catch (e) {
        const message = e instanceof Error ? e.message : String(e);
        commandError = `The simulator stopped a command: ${message} The cluster is paused as it was; press Restart to start the scenario again.`;
        held = null;
        flight = 'none';
        pause();
      }
    });
  }

  /** Apply the engine's current state. */
  async function refresh(r: Run): Promise<void> {
    applySnapshot(r, await r.sim.snapshot(r.selected, watchOf(r)));
  }

  /** What one batch tells the page beyond its snapshot: a held Send's hits
   *  leaving their node, a gateway change of the selected node. */
  function note(r: Run, batch: EventBatch): void {
    for (const { kind } of batch.events) {
      if (kind.type === 'PacketSent' && CARRIES_CELLS[kind.kind]) {
        if (kind.src === awaitingNode) awaitingNode = null;
        dataSent += 1;
        if (kind.tier === 'CrossZone') dataCrossed += 1;
      } else if (kind.type === 'GatewayChanged' && kind.origin === r.selected) {
        narration = say.gatewayChanged(kind.origin, zoneLabel(r.cluster.zones[kind.zone]?.name ?? ''), kind.to, kind.from);
      }
    }
  }

  /** Show a batch the engine just returned: in a held preset, a wave is held;
   *  anything else is drawn, its beams leaving `delay` ms from now (flown at
   *  once while paused), and its snapshot applied. */
  async function meet(r: Run, batch: EventBatch, delay: number): Promise<void> {
    note(r, batch);
    if (r.preset.pacing === 'held' && isWave(batch)) return hold(r, batch, delay);
    stage?.show(batch, delay, 'free');
    if (!playing) stage?.finish('at-speed');
    await refresh(r);
  }

  /** Hold `batch`'s wave: apply the snapshot, in which only the senders have
   *  changed, then draw the wave and wait for the stage to report it landed;
   *  while paused the wave flies all the same. Under Propagation's per-wave
   *  run the wave waits, unflown, for Step. */
  async function hold(r: Run, batch: EventBatch, delay: number): Promise<void> {
    await refresh(r);
    const { promise, resolve } = Promise.withResolvers<void>();
    const h: Held = { seq: ++holds, batch, delay, landed: promise, land: resolve };
    held = h;
    floorUntil = batch.virtual_ms + floorMs;
    if (untilSettled === 'per-wave') {
      flight = 'waiting';
      halt();
      return;
    }
    await fly(h);
  }

  /** Draw held wave `h`'s beams; with no stage to draw on, deliver it at once. */
  async function fly(h: Held): Promise<void> {
    flight = 'flying';
    const drawn = stage?.show(h.batch, h.delay, 'held') ?? false;
    if (!playing) stage?.finish('at-speed');
    if (!drawn) await deliver(h, false);
  }

  /** Deliver held wave `h`, unless something else already has: `step(0)`
   *  closes the engine's sub-step at the same virtual instant, so the
   *  receivers merge it, and their counts show. With `resume`, a per-wave run
   *  plays on to its next wave. */
  async function deliver(h: Held, resume: boolean): Promise<void> {
    const r = run;
    if (held !== h || r === null) return;
    const batch = await r.sim.step(0);
    note(r, batch);
    stage?.show(batch, 0, 'free');
    hops += 1;
    await refresh(r);
    held = null;
    flight = 'none';
    lastLanding = r.cluster.virtual_ms;
    if (resume && untilSettled === 'per-wave') play();
    settle(r);
  }

  /** The stage reports the held wave landed. */
  function onLanded(): void {
    const h = held;
    if (h === null) return;
    h.land();
    void exec(() => deliver(h, true));
  }

  /** Fly the held wave, if any, to its landing within a short wall time and
   *  deliver it. */
  async function flyFast(): Promise<void> {
    const h = held;
    if (h === null) return;
    if (stage) {
      if (flight === 'waiting') await fly(h);
      flight = 'flying';
      stage.finish('fast');
      await h.landed;
    }
    await deliver(h, false);
  }

  /** Fast-forward the run in flight before a command: land the held wave,
   *  then run on to one emit floor past it, flying each wave due on the way
   *  (the relays) the same way; each pushes the floor past itself. The run
   *  then stands at least one floor past its last threshold round, the
   *  instant gabion's floor (`runtime.rs` `want_immediate_flush`) lets the next
   *  eager flush go, whatever the floor knob says. */
  async function land(r: Run): Promise<void> {
    await flyFast();
    while (r.cluster.virtual_ms < floorUntil) {
      await meet(r, await r.sim.step(floorUntil - r.cluster.virtual_ms, EVENT), 0);
      await flyFast();
    }
  }

  /** The node feed hit number `index` goes to, or null when none can take it. */
  function feedNode(traffic: SustainedTraffic, nodes: readonly { id: number }[], index: number): number | null {
    if (traffic.at === 'every-node') return nodes.length === 0 ? null : nodes[index % nodes.length].id;
    for (let i = 0; i < nodes.length; i++) if (nodes[i].id === traffic.at.node) return traffic.at.node;
    return null;
  }

  /** Inject the feed's owed one-hit requests at the current virtual time: in
   *  a held preset, until one flushes a wave, which is held (true; the rest
   *  stay owed); in the live one, each submit's eager flush is drawn. */
  async function feed(r: Run): Promise<boolean> {
    const traffic = r.preset.traffic;
    if (traffic === undefined) return false;
    const nodes = r.cluster.nodes;
    const owed = Math.floor((traffic.rate_per_sec * (r.cluster.virtual_ms - r.feedFrom)) / 1000) - r.trafficInjected;
    for (let i = 0; i < owed; i++) {
      const node = feedNode(traffic, nodes, r.trafficInjected);
      r.trafficInjected += 1;
      if (node === null) continue;
      const batch = await r.sim.submitRequest(node, WATCHED_KEY, 1);
      if (r.preset.pacing === 'live') {
        stage?.show(batch, 0, 'free');
      } else if (isWave(batch)) {
        await hold(r, batch, 0);
        return true;
      }
    }
    return false;
  }

  /** One play-loop advance of `deltaMs` of virtual time. */
  async function advance(r: Run, deltaMs: number): Promise<void> {
    if (await feed(r)) return;
    await meet(r, await r.sim.step(deltaMs, r.preset.pacing === 'live' ? 'Time' : EVENT), 0);
    notePeriod(performance.now());
    settle(r);
  }

  /** The one stop rule: a command's run pauses itself once no node that
   *  waiting can reach is behind and one heartbeat period has passed since
   *  the last landing, or at its cap. */
  function settle(r: Run): void {
    if (untilSettled === 'off' || held !== null) return;
    const now = r.cluster.virtual_ms;
    const late = r.cluster.nodes.find((n) => n.lag.kind === 'Behind' && n.lag.missing_for_ms >= grace);
    if (late === undefined && now >= lastLanding + tickMs) {
      halt();
      // The acknowledgements still in the air land rather than freeze.
      stage?.finish('at-speed');
      untilSettled = 'off';
      const stranded = r.cluster.nodes.some((n) => n.lag.kind === 'Stranded');
      narration = stranded ? say.strandedStop(r.cuts.map((c) => cutState(c, r.cluster.zones))) : [...told, ...say.EVERY_NODE];
    } else if (now >= runStart + UNTIL_SETTLED_MAX_PERIODS * tickMs) {
      halt();
      stage?.finish('at-speed');
      untilSettled = 'off';
      capped = true;
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
   *  the last Send's decision text clears, the run in flight fast-forwards
   *  past its relays and the landed routes drop; then `task` shows its own
   *  effect. */
  function command(task: (r: Run) => Promise<void>): Promise<void> {
    commandError = null;
    dropPrevious();
    return exec(async (r) => {
      await land(r);
      stage?.clearTrail();
      decisions = new Map();
      lastDecision = null;
      capped = false;
      await task(r);
    });
  }

  /** A command with no batch to show: apply the snapshot after it. */
  const act = (cmd: (r: Run) => Promise<unknown>): Promise<void> =>
    command(async (r) => {
      await cmd(r);
      await refresh(r);
    });

  /** Start a bounded run from a paused page: time plays until the command has
   *  propagated, then pauses itself. While playing, time is left alone. */
  function runOn(r: Run, mode: UntilSettled): void {
    if (playing) return;
    untilSettled = mode;
    runStart = r.cluster.virtual_ms;
    lastLanding = runStart;
    play();
  }

  /** Send a request of Hits per Send at the selected node, at the current
   *  virtual time. The node decides it in the frame of the press: a refusal
   *  plays in its vessel and runs no time; an admitted request's eager flush
   *  stands `ARRIVAL_MS` before it flies, and is held. */
  const sendBurst = (): Promise<void> =>
    command(async (r) => {
      const node = r.selected;
      const hits = r.hits;
      const before = r.cluster.nodes.find((n) => n.id === node);
      const batch = await r.sim.submitRequest(node, WATCHED_KEY, hits);
      const event = batch.events.find((e) => e.kind.type === 'Decided');
      if (event === undefined || event.kind.type !== 'Decided') return;
      const admission = event.kind.admission;
      decisions = new Map([[node, { hits, admitted: admission.kind === 'Admitted' }]]);
      lastDecision = admission;
      decided = { zone: before?.zone ?? 0, admission };
      if (admission.kind === 'Rejected') {
        stage?.reject(node, hits);
        narration =
          admission.reopens.kind === 'Never'
            ? say.never(node, hits, limit)
            : say.rejected(node, hits, admission.total, limit, senders(r, admission.total));
        await refresh(r);
        return;
      }
      r.requests.push(node, r.cluster.bucket_epoch_now, hits);
      const eager = isWave(batch);
      hops = 0;
      dataSent = 0;
      dataCrossed = 0;
      sentAt = r.cluster.virtual_ms;
      waited = !eager;
      armed = true;
      emptied = false;
      awaitingNode = eager ? null : node;
      told = eager ? say.admittedEager(node, hits, admission.total_before, limit) : say.admittedWaiting(node, hits, limit);
      narration = told;
      runOn(r, r.preset.story === 'propagation' ? 'per-wave' : 'settled');
      await meet(r, batch, ARRIVAL_MS);
    });

  /** The nodes whose admitted hits fill the window, for the rejection, when
   *  the refusing node sees them all and more than one sent them. */
  function senders(r: Run, view: number): readonly number[] | null {
    if (view !== r.cluster.oracle_total) return null;
    const nodes = r.requests.nodesInWindow(r.cluster.bucket_epoch_now, buckets);
    return nodes.length > 1 ? nodes : null;
  }

  /** Select node `id`, once a snapshot inspecting it has its rows. Changes no
   *  count, so it lands nothing. */
  const select = (id: number): Promise<void> =>
    exec(async (r) => {
      const snap = await r.sim.snapshot(id, watchOf(r));
      r.selected = id;
      inspected = snap.inspected ?? null;
      r.cluster = snap;
      if (untilSettled === 'off' && held === null) narration = idleOf(r);
    });

  /** Make fault `cut` over the live nodes. */
  const cut = (c: Parameters<Sim['cut']>[0]): Promise<void> =>
    act(async (r) => {
      await r.sim.cut(c, r.cluster.nodes);
      r.cuts = [...r.sim.cuts()];
    });

  /** Restore the scenario's own network, its loss included, then run time
   *  until the nodes agree. */
  const heal = (): Promise<void> =>
    command(async (r) => {
      await r.sim.heal();
      r.cuts = [];
      await refresh(r);
      told = ['the network is whole again'];
      narration = told;
      runOn(r, 'settled');
    });

  /** Spawn a fresh cold-start node into zone `zone` of the live cluster, cut
   *  as the listed faults say, and run time until it has caught up. */
  const addNode = (zone: number): Promise<void> =>
    command(async (r) => {
      await r.sim.addNode(zone);
      await refresh(r);
      await r.sim.reapply(r.cluster.nodes);
      await refresh(r);
      told = blurb`a node joined ${zoneLabel(r.cluster.zones[zone]?.name ?? '')}`;
      narration = told;
      runOn(r, 'settled');
    });

  /** Remove the selected node. Survivors keep their ids and re-converge; the
   *  selection moves to the scenario's fallback. */
  const removeNode = (id: number): Promise<void> =>
    command(async (r) => {
      await r.sim.removeNode(id);
      r.cuts = [...r.sim.cuts()];
      const snap = await r.sim.snapshot(null, watchOf(r));
      r.cluster = snap;
      r.selected = subjectOf(r);
      await refresh(r);
    });

  function play(): void {
    if (playing || run === null || busy) return;
    playing = true;
    if (flight === 'frozen') flight = 'flying';
    lastWall = 0;
    period = null;
    rafId = requestAnimationFrame(frame);
  }

  /** Stop the play loop, leaving the run's bound as it is. */
  function halt(): void {
    playing = false;
    if (rafId !== 0) {
      cancelAnimationFrame(rafId);
      rafId = 0;
    }
  }

  /** The reader's Pause: time stops, a wave in flight freezes, and a bounded
   *  run ends. */
  function pause(): void {
    halt();
    untilSettled = 'off';
    if (flight === 'flying') flight = 'frozen';
  }

  function toggle(): void {
    if (playing) {
      pause();
      return;
    }
    untilSettled = 'off';
    // A wave held for Step flies once Play runs the clock.
    const h = held;
    if (h !== null && flight === 'waiting') void exec(() => fly(h));
    play();
  }

  /** Step to the next event, pausing first: fly the held wave, if any; else,
   *  in a held preset, run to the next wave, which is held and flown, or to the
   *  next instant hits leave the window, or one heartbeat period when neither
   *  is coming; in the live one, one period. A per-wave run stays one: the
   *  wave lands and the run plays on to the next. */
  async function stepOnce(): Promise<void> {
    if (busy) return;
    if (untilSettled === 'per-wave' && held !== null) halt();
    else pause();
    await exec(async (r) => {
      const h = held;
      if (h !== null) {
        tellWave(r, h.batch);
        if (flight === 'waiting') await fly(h);
        flight = 'flying';
        stage?.finish('at-speed');
        return;
      }
      if (await feed(r)) return;
      const live = r.preset.pacing === 'live';
      const expiry = r.cluster.next_expiry_ms;
      const delta = live || expiry === undefined ? tickMs : expiry - r.cluster.virtual_ms;
      const batch = await r.sim.step(delta, live ? 'Time' : EVENT);
      if (!isWave(batch) && expiry === undefined && r.cluster.oracle_total === 0) narration = say.NOTHING_WAITING;
      if (isWave(batch)) tellWave(r, batch);
      await meet(r, batch, 0);
    });
  }

  /** On Step, say what the wave does, where it is the selected node's own and
   *  no feed muddies it. */
  function tellWave(r: Run, batch: EventBatch): void {
    if (r.preset.traffic !== undefined) return;
    const data = batch.events.flatMap(({ kind }) =>
      (kind.type === 'PacketSent' || kind.type === 'PacketDropped') && CARRIES_CELLS[kind.kind] ? [kind] : [],
    );
    const own = data.filter((p) => p.kind === 'Own');
    const relays = data.filter((p) => p.kind === 'Relay');
    const zones = r.cluster.zones;
    const zoneOf = (id: number): number => r.cluster.nodes.find((n) => n.id === id)?.zone ?? -1;
    if (own.length > 0 && own.every((p) => p.src === r.selected)) {
      const peers = own.filter((p) => p.tier === 'InZone').length;
      const gateways = own
        .filter((p) => p.tier === 'CrossZone')
        .map((p) => ({ node: p.dst, zone: zoneLabel(zones[zoneOf(p.dst)]?.name ?? '') }));
      narration = say.waveOne(r.selected, zoneLabel(zones[zoneOf(r.selected)]?.name ?? ''), peers, gateways);
    } else if (relays.length > 0 && own.length === 0) {
      const gateways = [...new Set(relays.map((p) => p.src))].sort((a, b) => a - b);
      const lost = relays.filter((p) => p.type === 'PacketDropped').length;
      narration = say.waveTwo(gateways, relays.length - lost, dataCrossed, dataSent + relays.length, lost);
    }
  }

  /** Commit what Hits per Send holds: a whole number in range becomes the
   *  Send's weight; anything else disables Send until it is fixed. */
  function commitHits(): void {
    const r = run;
    const n = /^\d+$/.test(hitsText.trim()) ? Number(hitsText.trim()) : Number.NaN;
    hitsCommitted = hitsText;
    hitsValid = Number.isSafeInteger(n) && n >= BURST_HITS_MIN && n <= BURST_HITS_MAX;
    if (hitsValid && r !== null) r.hits = n;
  }

  /** The caption's fix: back to the scenario's own Hits per Send. */
  function useHits(hits: number): void {
    hitsText = String(hits);
    commitHits();
    lastDecision = null;
  }

  function selectPreset(preset: Preset): void {
    if (defaults === null || preset.id === run?.preset.id) return;
    void start(preset, knobsFromPreset(preset, defaults), preset.burst_hits);
  }

  const go = (to: PresetId): void => selectPreset(presetById(to));

  function restart(): void {
    if (run !== null) void start(run.preset, run.applied, run.hits, run.knobs);
  }

  function rebuild(): void {
    if (run !== null) void start(run.preset, run.knobs, run.hits);
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

  /** "Skip to the next step": focus the filled control, or the action line in
   *  a wait. */
  function skipToNext(): void {
    const target = document.querySelector<HTMLElement>('[data-primary]') ?? document.getElementById('next-action');
    target?.focus();
  }

  // -- What the page shows, derived from the run --------------------------

  const view = $derived.by((): RunView | null => {
    const r = run;
    if (r === null || selectedNode === null) return null;
    const from = r.cluster.nodes.indexOf(selectedNode) * buckets;
    let strata = 0;
    for (let b = from; b < from + buckets; b++) if ((r.cluster.window_buckets[b] ?? 0) > 0) strata += 1;
    return {
      story: r.preset.story,
      next: r.preset.next,
      progress: r.progress,
      selected: r.selected,
      selectedZone: selectedNode.zone,
      nodes: r.cluster.nodes,
      nextSend: r.cluster.inspected?.next_send,
      subjectStrata: strata,
      subjectAtLimit: selectedNode.brim.kind === 'Rejected',
      cuts: r.cuts.length,
      cutZone,
      untilSettled,
      flight,
      playing,
      capped,
      busy,
      hits: editingHits ? 'editing' : hitsValid ? 'valid' : 'invalid',
      lastDecision,
      burstHits: r.preset.burst_hits,
      sendHits: r.hits,
      oracle: r.cluster.oracle_total,
      virtualMs: r.cluster.virtual_ms,
      bucketNow: r.cluster.bucket_epoch_now,
      bucketMs,
      limit,
      windowMs: r.applied.rule_window_ms,
      zoneCount: r.cluster.zones.filter((z) => z.live > 0).length,
      loss: r.applied.uniform_loss,
    };
  });
  const action = $derived<NextAction>(view === null ? { kind: 'wait', reason: 'starting' } : nextAction(view));
  const primary = $derived(action.kind === 'press' ? action.control : null);
  const goal = $derived(view === null || run === null ? [] : goalOf(view, run.preset.id));
  const send = $derived(sendLabel(width, hitsValid && run !== null ? run.hits : null, run?.selected ?? 0));
  const pressLabel = $derived(
    action.kind !== 'press' ? [] : action.control === 'send' ? send : action.control === 'play' ? ['Play'] : ['Step'],
  );
  const lineClause = $derived(
    view === null || action.kind !== 'press' ? [] : pressClause(view, action.control, hops + 1),
  );
  const useHitsReason = $derived<Blurb>(
    lastDecision?.kind === 'Rejected' && lastDecision.reopens.kind === 'Never'
      ? blurb`a request of ${run?.hits ?? 0} can never fit the limit of ${limit}`
      : hitsCommitted.trim() === ''
        ? ['Hits per Send is empty']
        : blurb`Hits per Send must be a whole number from ${BURST_HITS_MIN} to ${BURST_HITS_MAX}`,
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
          nextSend: run.cluster.inspected?.next_send,
          sendHits: run.hits,
          emptied,
          badge,
          tickMs,
        }),
  );

  /** The feed's hits younger than the grace, or null with no feed. */
  function newerHits(r: Run): number | null {
    const traffic = r.preset.traffic;
    if (traffic === undefined) return null;
    const fed = (t: number): number => Math.floor((traffic.rate_per_sec * Math.max(t - r.feedFrom, 0)) / 1000);
    return fed(r.cluster.virtual_ms) - fed(r.cluster.virtual_ms - grace);
  }

  // A wait's meter: its progress since it began, in [0, 1] and in words.
  const meter = $derived.by((): { value: number; text: string } | null => {
    if (action.kind !== 'wait' || view === null) return null;
    const reason = action.reason;
    if (waitStart?.reason !== reason) {
      const next = view.nextSend;
      waitStart = {
        reason,
        virtualMs: view.virtualMs,
        reopensMs: next?.kind === 'Rejected' && next.reopens.kind === 'In' ? next.reopens.ms : 0,
      };
    }
    const value = progress(view, reason, waitStart);
    if (value === null) return null;
    const text =
      reason === 'run'
        ? `${Math.round(value * 100)} percent of the reachable nodes have the hits`
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
      await start(DEFAULT_PRESET, knobsFromPreset(DEFAULT_PRESET, defaults), DEFAULT_PRESET.burst_hits);
    })();
    const unwatch = watchTheme(() => (theme = readTheme()));
    const unwidth = watchWidth((w) => (width = w));
    const onWide = (event: MediaQueryListEvent): void => {
      controlsOpen = event.matches;
    };
    wide.addEventListener('change', onWide);
    return () => {
      halt();
      unwatch();
      unwidth();
      wide.removeEventListener('change', onWide);
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
        {/if}{#each run.cuts as c, i (i)}
          · <Sentence parts={cutName(c, cluster.zones)} field="status-cut" />{/each}
      </p>
    {/if}
    <p class="once">
      One simulated run · <a href={`${import.meta.env.BASE_URL}study/dashboard/`}>measured results in the study</a>
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
        <dd>Delivery between nodes of one zone: a thin line that stays inside its ring.</dd>
        <dt>across zones</dt>
        <dd>Delivery between zones: a thick arrow that crosses the empty centre.</dd>
        <dt>gateway</dt>
        <dd>
          <RoleGlyph kind="port" /> The member of a remote zone that the selected node sends its hits to. It relays them into its
          zone, from the port on its rim.
        </dd>
        <dt>relay</dt>
        <dd>A gateway forwarding hits it received to the rest of its zone.</dd>
        <dt>ack</dt>
        <dd>I hold every item of your stream so far. One number acknowledges everything below it.</dd>
        <dt>retransmission timeout (RTO)</dt>
        <dd>How long a sender waits for an ack before resending: one heartbeat.</dd>
        <dt>suspect</dt>
        <dd>
          <RoleGlyph kind="suspect" /> A peer that left something unanswered for the suspect timeout, or a remote peer that said
          it cannot relay. A suspect is not used as a gateway.
        </dd>
        <dt>gap report</dt>
        <dd>A receiver's message "I am missing part of your stream; resend from my ack".</dd>
        <dt>join sweep</dt>
        <dd>A one-off copy of a node's whole live store, sent to a zone member that just joined.</dd>
        <dt>How the controls work</dt>
        <dd>
          The one dark button is what to press next, and the line under the goal names it; when nothing is dark, watch the
          meter. A Send pressed while paused plays until every node has the hits, then pauses again; Space plays or pauses, →
          steps to the next event, and Escape returns the selection to the scenario's node. Restart keeps your settings, a
          scenario button takes its own, and Undo brings back the run you left.
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
            bind:hitsText
            {hitsValid}
            editing={editingHits}
            zones={cluster.zones}
            selected={r.selected}
            {roles}
            {maxZones}
            canAdd={cluster.nodes.length < MAX_LIVE_NODES}
            canHeal={r.cuts.length > 0}
            maxNodes={MAX_LIVE_NODES}
            knobs={r.knobs}
            appliedKnobs={r.applied}
            bucketMs={defaults.rule_bucket_ms}
            onSelectPreset={selectPreset}
            onRestart={restart}
            onCommitHits={commitHits}
            onEditHits={(on) => (editingHits = on)}
            onRevertHits={() => (hitsText = hitsCommitted)}
            onApplyKnobs={rebuild}
            onBackToScenario={backToScenario}
            onCutZone={(zone) => void cut({ kind: 'zone', zone })}
            onSilence={(node) => void cut({ kind: 'silenced', node })}
            onCutFromZone={(node, zone) => void cut({ kind: 'unheard-in-zone', node, zone })}
            onHeal={() => void heal()}
            onAddNode={(zone) => void addNode(zone)}
            onPreview={(node) => (namedId = node)}
          />
        </details>
        <section
          class="stage-pane"
          aria-label="Stage"
          aria-describedby="next-action"
          data-held={held?.seq ?? 0}
          data-flight={flight}
          data-run={untilSettled}
          data-playing={playing ? '1' : '0'}
        >
          <StageCaption
            {goal}
            {action}
            {pressLabel}
            pressClause={lineClause}
            phone={width === 'narrow'}
            subject={r.selected}
            {useHitsReason}
            repairLabel={heading.repair}
            zoneName={(z) => zoneLabel(cluster.zones[z]?.name ?? '')}
            {meter}
            presetLabel={(id) => presetById(id).label}
            onSelect={(node) => void select(node)}
            onUseHits={useHits}
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
              <span aria-live="polite" aria-atomic="true"><Sentence parts={narration} field="narration" /></span
              >{#if heartbeatIn !== null && heartbeatIn >= 0}<span class="countdown">, in <span data-field="countdown"
                    >{fmtSeconds(heartbeatIn)}</span
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
            running={playing}
            {speed}
            {trail}
            {onLanded}
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
              <h2 class="pane-title">
                node <span data-field="node">{selectedNode.id}</span>
                <span class="pane-sub">selected · {zoneLabel(cluster.zones[selectedNode.zone]?.name ?? '')}</span>
              </h2>
              <Dashboard
                {cluster}
                history={r.history}
                version={r.chartVersion}
                {limit}
                charts={r.preset.charts}
                {theme}
                hotId={hoveredId ?? focusedId}
              />
              {#if inspected !== null && inspected.id === selectedNode.id}
                <NodeInspector
                  node={selectedNode}
                  peers={inspected.peers}
                  zones={cluster.zones}
                  zoneMode={r.applied.zone_mode}
                  onPreview={(node) => (namedId = node)}
                >
                  <BucketStrata
                    cells={inspected.cells}
                    currentEpoch={cluster.bucket_epoch_now}
                    epochFraction={(cluster.virtual_ms % bucketMs) / bucketMs}
                    liveBuckets={buckets}
                    windowMs={r.applied.rule_window_ms}
                    {bucketMs}
                    {limit}
                    atLimit={selectedNode.brim.kind === 'Rejected'}
                  />
                </NodeInspector>
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
      sendValid={hitsValid}
      {primary}
      {untilSettled}
      {flight}
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

  .pane-title {
    margin: 0;
    font-size: var(--font-size-body);
    font-weight: 600;
  }

  .pane-sub {
    font-size: var(--font-size-caption);
    font-weight: 400;
    color: var(--color-fg-muted);
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
  }

  /* One scrolling column. `main` keeps a
     height so the absolutely positioned overlays stay visible; the square
     stage keeps the ring the same shape at every width. The rail body gets a
     definite height, because the dashboard sizes its charts from
     `height: 100%` rows, which collapse under an auto-height parent. */
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
