<script lang="ts">
  import { onMount } from 'svelte';
  import { fmtCount, noun } from './lib/format';
  import { Sim } from './lib/sim/sim';
  import { readTheme, watchTheme } from './lib/theme';
  import {
    ANSWER,
    isWave,
    type ClusterState,
    type EventBatch,
    type Inspected,
    type PeerView,
    type SimConfig,
    type StepUntil,
    type Watch,
  } from './lib/sim/types';
  import type { Roles, Trail } from './lib/stage/renderer';
  import { ChartHistory } from './lib/charts/history';
  import {
    DEFAULT_PRESET,
    PRESETS,
    WATCHED_KEY,
    knobsFromPreset,
    warm,
    type Knobs,
    type Preset,
    type SustainedTraffic,
  } from './lib/presets';
  import { nominalBuckets } from './lib/buckets';
  import Stage from './lib/components/Stage.svelte';
  import Dashboard from './lib/components/Dashboard.svelte';
  import HeadlineMetric from './lib/components/HeadlineMetric.svelte';
  import NodeInspector from './lib/components/NodeInspector.svelte';
  import BucketStrata from './lib/components/BucketStrata.svelte';
  import ControlRail from './lib/components/ControlRail.svelte';
  import TransportBar from './lib/components/TransportBar.svelte';

  // The session runs one scenario preset at a time (see `lib/presets.ts`): a
  // config plus an opening seed against a fresh, warmed cluster. Play advances
  // virtual time, Step runs it to the next event, and a Send submits a request
  // for the watched key; Reset re-runs the active preset; selecting another
  // rebuilds.
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
  // The workspace stays mounted across rebuilds, with a "Rebuilding" overlay on
  // top: the rail's open disclosure, the focused control and the grid cell
  // keyboard focus is on all survive a rebuild without being restored.
  let activePreset = $state<Preset>(DEFAULT_PRESET);
  // The Rust-side production defaults (`Sim.defaultConfig()`), the single source
  // the controls open from — no hand-typed TS mirror. Fetched once during the
  // first load (the first-load overlay covers the workspace until then), then
  // every knob default and the bucket width read from it. Changing
  // `gabion::defaults` is the only edit; the controls follow.
  let defaults = $state<SimConfig | null>(null);
  // The rebuild knobs (cluster size, zones, error budget, packet loss, …). Seated
  // from the active preset and merged over its config on each (re)build, so a
  // control tweak followed by a rebuild explores the parameter space without
  // leaving the scenario. They take effect only on rebuild — a live engine can't
  // change its node count — so editing a knob just *stages* it; the rail's
  // explicit "Rebuild" applies the staged set in one build. `null` until the
  // first load seeds them from `defaults` (see `bootstrap`).
  let knobs = $state<Knobs | null>(null);
  // The knob values the *current* engine was built from. `bootstrap` snapshots
  // `knobs` into this on every (re)build, so the rail can show which controls
  // have been moved since — staged but not yet applied — and enable Rebuild only
  // when there is something to apply.
  let appliedKnobs = $state<Knobs | null>(null);
  // The most zones the stepper offers, from the engine; `0` until the first load.
  let maxZones = $state(0);
  // The full config the live cluster was built with, which the active preset's
  // blurb is written from.
  let builtConfig = $state.raw<SimConfig | null>(null);
  // Hits per Send, shared by the inspector's and the control rail's Send so the
  // two always agree. Each Send targets the same watched key, growing the same
  // counter the preset seeded. The default sits well under the threshold-AE
  // budget (ε ≈ limit·bps/(10⁴·N), ~833 at the narrative presets' pinned
  // 1 000 000 limit), so a Send spreads at its node's heartbeat — raising it (or
  // dropping the rule limit) far enough trips an eager flush, itself worth seeing.
  let burstHits = $state(25);
  // What every snapshot reads: the watched key, and the weight of the next Send
  // its inspected node decides. Derived, so a frame reuses one object. Hits per
  // Send is unvalidated, so anything but a whole number of 1 or more probes one
  // hit rather than throwing at `BigInt`.
  const watch = $derived<Watch>({
    key: WATCHED_KEY,
    probe_hits: Number.isSafeInteger(burstHits) && burstHits > 0 ? burstHits : 1,
  });
  // The active cluster's heartbeat period: how far a Step runs when no event is
  // coming, and the unit of the heartbeat-wall readout. `0` only before the
  // first build seeds `appliedKnobs`, when nothing steps.
  const tickMs = $derived(appliedKnobs?.tick_interval_ms ?? 0);
  // One emit floor: how far past a wave `land` runs, so the next eager flush
  // may go.
  const floorMs = $derived(builtConfig?.min_emit_interval_ms ?? 0);
  // Where a held preset's steps stop: before a wave of data packets lands, or
  // where some node's window drops a bucket that holds hits.
  const EVENT: StepUntil = { Event: { key: WATCHED_KEY } };
  // Presentation-clock ms a Send's decision stands alone before its eager
  // flush flies.
  const ARRIVAL_MS = 250;
  // Cap one advance so a backgrounded tab resuming rAF can't leap seconds of
  // virtual time in a single frame.
  const MAX_STEP_MS = 500;

  let sim: Sim | null = null;
  // Not named `state`: `$state(...)` would then parse as store-subscription
  // syntax (`$` + a variable called `state`) and confuse the compiler.
  let cluster: ClusterState | null = $state(null);
  // The stage, commanded in the order the queue runs the engine.
  let stage = $state.raw<ReturnType<typeof Stage>>();
  let playing = $state(false);
  let speed = $state(1);
  let virtualMs = $state(0);
  // Node pairs with a link cut, from the sim's own record of its link policies.
  let cutLinks = $state(0);
  let error: string | null = $state(null);
  let loading = $state(true);
  // The canvas colours, re-read on an OS scheme or data-theme change; the stage
  // and charts re-tint from it. `raw`: replaced whole, never deep-proxied.
  let theme = $state.raw(readTheme());
  // Below 640 px the rail folds into a "Controls" disclosure above the stage; at
  // and above it the rail is always open (its summary is hidden).
  const wide = window.matchMedia('(width >= 640px)');
  let controlsOpen = $state(wide.matches);

  // The live stable ids, in rank order — what the rail's remove/send controls
  // pick from and the count the Cluster controls gate on. Ids have gaps under
  // churn, so this is the authoritative live set, not a `0..count` range.
  // `$derived.by` (a closure) rather than a bare `$derived(cluster?…)`: `cluster`
  // is `$state` reassigned only inside async callbacks, so at this top-level
  // position TS control-flow narrows it to its `null` initializer. Reading it
  // through a closure keeps its declared `ClusterState | null` type (the closure
  // may run after a reassignment) — and is what Svelte's reactivity wants too.
  const nodeIds = $derived.by(() => (cluster?.nodes ?? []).map((n) => n.id));
  // The visualizer's upper bound on live nodes; the rail disables Add here so a
  // held key can't spawn runtimes without limit. The one-second step of 183
  // nodes was not measured to fit a frame, so the cap stays at 100 (see the
  // crate README).
  const MAX_LIVE_NODES = 100;

  // The selected node (clicking a disc selects it and opens the inspector in the
  // right rail). `selectedNode` resolves the id against the live snapshot; an
  // effect clears the selection the moment its node is no longer live — a remove
  // or a rebuild — so the inspector can never show a stale or vanished node.
  let selectedId = $state<number | null>(null);
  const selectedNode = $derived.by(() => {
    if (selectedId === null || cluster === null) return null;
    return cluster.nodes.find((n) => n.id === selectedId) ?? null;
  });
  $effect(() => {
    if (selectedId !== null && !nodeIds.includes(selectedId)) selectedId = null;
  });
  // The node under the pointer and the node with keyboard focus: each shows the
  // tooltip, and the dashboard draws the hovered node's own line.
  let hoveredId = $state<number | null>(null);
  let focusedId = $state<number | null>(null);
  // The selected node's cells and peer rows. A snapshot carries them for the
  // node it was taken inspecting alone, so `inspected` keeps the last rows for
  // the selection, and `select` fetches them before the selection moves.
  let inspected = $state<Inspected | null>(null);
  const selectedRows = $derived(
    inspected !== null && inspected.id === selectedId ? inspected : null,
  );

  /** The selected origin's marks: itself, and the gateway the runtime reports
   *  for each remote zone, `suspect` once it has left something unanswered. */
  function rolesOf(origin: number | null, peers: PeerView[] | undefined): Roles | null {
    if (origin === null || peers === undefined) return null;
    const gateways = new Map<number, { zone: number; suspect: boolean }>();
    for (const p of peers) {
      if (p.role === 'gateway' && p.id !== undefined && p.zone !== undefined) {
        gateways.set(p.id, { zone: p.zone, suspect: ANSWER[p.answer].suspect });
      }
    }
    return { origin, gateways };
  }
  // Replaced only when the marks change, not with every snapshot's fresh peer
  // rows: the stage redraws every node's mark when it is handed new roles.
  // Derived, not set by an effect, so the marks never lag the cluster they
  // name: a rebuild with fewer zones never shows a gateway into a zone gone.
  let rolesKey = '';
  let rolesKept: Roles | null = null;
  const roles = $derived.by((): Roles | null => {
    const next = rolesOf(selectedId, selectedRows?.peers);
    const key =
      next === null
        ? ''
        : `${next.origin}:${[...next.gateways].map(([g, r]) => `${g}/${r.zone}/${r.suspect}`).join(',')}`;
    if (key !== rolesKey) {
      rolesKey = key;
      rolesKept = next;
    }
    return rolesKept;
  });

  // The rolling chart history is a plain (non-reactive) structure — 600 samples
  // under a deep `$state` proxy would cost on every frame. `chartVersion` is the
  // single reactive signal the dashboard redraws on; it ticks on each new sample.
  const history = new ChartHistory();
  let chartVersion = $state(0);

  // The active preset's feed: the hits injected since the build was handed to
  // the reader at virtual `feedFrom`, which also indexes the round-robin spread,
  // so ⌊rate·T⌋ hits are in by virtual time T however play or Step chunked it.
  // Plain `let` (not `$state`): nothing renders them.
  let feedFrom = 0;
  let trafficInjected = 0;

  /** A wave the engine holds in flight: the page shows it and calls the engine
   *  again only once the stage reports it landed, which resolves `landed`.
   *  `seq` tells one hold from the next. */
  interface Held {
    readonly seq: number;
    readonly landed: Promise<void>;
    readonly land: () => void;
  }
  let held = $state.raw<Held | null>(null);
  let holds = 0;
  // Virtual ms one emit floor after the last held wave's instant: `land` runs
  // the run on to here.
  let floorUntil = 0;
  // The tail of the queue every engine call runs on.
  let queue: Promise<void> = Promise.resolve();
  // Whether landed routes stay drawn until the next command: where no feed runs.
  const trail = $derived<Trail>(activePreset.traffic === undefined ? 'keep' : 'drop');

  // Wall ms the last whole heartbeat period took to play, and the period and
  // wall instant the measurement runs from (`periodWall` 0 until a period
  // boundary has been crossed since Play).
  let heartbeatWallMs = $state<number | null>(null);
  let periodIndex = -1;
  let periodWall = 0;

  let rafId = 0;
  let lastWall = 0;
  // Exactly one advance is ever in flight: the play loop will not issue the
  // next `step` until the previous resolves, so commands can't pile up faster
  // than the engine drains them.
  let stepping = false;

  /** The preset's config with the live knob values layered on top. The preset's
   *  other pinned fields survive; the rule + gossip knobs seated from the preset
   *  (its limit, the shared window/tick defaults) win on drag, exactly like
   *  loss. The bucket width is fixed so every window the slider picks stays a
   *  whole number of buckets. */
  function effectiveConfig(preset: Preset, k: Knobs, d: SimConfig): Partial<SimConfig> {
    return {
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

  /** Build the cluster for `preset`, warm it, run its opening seed, and adopt
   *  it as the active scenario. Reset and a knob change re-run the current
   *  preset with the current knobs; the rail's scenario buttons pass a new one
   *  (via `selectPreset`, which re-seats the knobs first). A wave in flight is
   *  discarded, not landed. */
  function bootstrap(preset: Preset = activePreset): Promise<void> {
    return serial(async () => {
      pause();
      loading = true;
      error = null;
      held = null;
      floorUntil = 0;
      stage?.reset();
      hoveredId = null;
      try {
        // Source the Rust defaults once, then seed the knobs from the active
        // preset before the control rail first shows. After this first pass
        // both are non-null for the life of the page; Reset and knob edits keep
        // the current `knobs`, a preset switch re-seats them in `selectPreset`
        // before calling here.
        const d = defaults ?? (defaults = await Sim.defaultConfig());
        if (maxZones === 0) maxZones = await Sim.maxZones();
        const k = knobs ?? (knobs = knobsFromPreset(preset, d));
        // Clear the rolling history so a rebuild starts the charts from a blank
        // slate, measuring the share over the new window. Only a rebuild clears
        // it — adding or removing a node reshapes in place, so the shared time
        // axis keeps running across a join or leave.
        history.reset(k.rule_window_ms);
        chartVersion += 1;
        // Tear the previous engine down first; otherwise its spawned task,
        // runtimes, and tick channels leak for the life of the page (each
        // rebuild would pile up an engine).
        if (sim !== null) {
          await sim.shutdown();
          sim = null;
          window.sim = undefined;
        }
        const config = { ...d, ...effectiveConfig(preset, k, d) };
        const fresh = await Sim.create(config);
        // A seed that cannot start (a gateway scenario with no gateway to find)
        // must not strand the engine it was seeding.
        try {
          await preset.seed(fresh, config, await warm(fresh, config));
          // A seed's last admitted request leaves its sub-step open, its eager
          // flush (if any) held: deliver it now, so the first paint shows what
          // the engine holds and nothing lands undrawn later.
          await fresh.step(0);
        } catch (e) {
          await fresh.shutdown();
          throw e;
        }
        sim = fresh;
        window.sim = fresh;
        // The engine now reflects the current knobs — mark them applied so the
        // rail's "staged" cue and Rebuild button reset to clean.
        appliedKnobs = { ...k };
        // The selection and the cluster it names arrive together, so the effect
        // that drops a selection no live node has never sees one without the
        // other.
        const snap = await fresh.snapshot(preset.select ?? null, watch);
        selectedId = preset.select ?? null;
        applySnapshot(snap);
        // The feed counts from here, the instant the reader is handed the build.
        feedFrom = snap.virtual_ms;
        trafficInjected = 0;
        heartbeatWallMs = null;
        // Adopted only once built, so the rail's `aria-current` marks a finished
        // build.
        builtConfig = config;
        activePreset = preset;
      } catch (e) {
        error = e instanceof Error ? e.message : String(e);
      } finally {
        loading = false;
      }
    });
  }

  /** Adopt a scenario: re-seat the knobs from its config (so the controls mirror
   *  it), then build it. The rail's scenario buttons route here; Reset and knob
   *  changes call `bootstrap()` directly, keeping the current knobs. */
  function selectPreset(preset: Preset): void {
    // The rail (which routes here) only shows once the first load seeds
    // `defaults`, so it is non-null; re-seat the knobs from the chosen scenario.
    if (defaults !== null) knobs = knobsFromPreset(preset, defaults);
    void bootstrap(preset);
  }

  /** Adopt a fresh snapshot as the current state and append it to the chart
   *  history — the one place per-frame state and the rolling series stay in
   *  step. */
  function applySnapshot(snap: ClusterState): void {
    cluster = snap;
    if (snap.inspected !== undefined && snap.inspected.id === selectedId) inspected = snap.inspected;
    virtualMs = snap.virtual_ms;
    cutLinks = sim?.cutLinks() ?? 0;
    history.push(snap);
    chartVersion += 1;
  }

  function frame(now: number): void {
    if (!playing || sim === null) return;
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
    void run((s) => advance(s, deltaMs)).finally(() => {
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

  /** Run `task` on the live engine, on the queue. A failure stops play, drops
   *  any hold (the engine's next command closes it) and surfaces. */
  function run(task: (s: Sim) => Promise<void>): Promise<void> {
    return serial(async () => {
      if (sim === null) return;
      try {
        await task(sim);
      } catch (e) {
        error = e instanceof Error ? e.message : String(e);
        held = null;
        pause();
      }
    });
  }

  /** Apply the engine's current state. */
  async function refresh(s: Sim): Promise<void> {
    applySnapshot(await s.snapshot(selectedId, watch));
  }

  /** Show a batch the engine just returned: in a held preset, a wave is held;
   *  anything else is drawn, its beams leaving `delay` ms from now (flown at
   *  once while paused), and its snapshot applied. */
  async function meet(s: Sim, batch: EventBatch, delay: number): Promise<void> {
    if (activePreset.pacing === 'held' && isWave(batch)) return hold(s, batch, delay);
    stage?.show(batch, delay, false);
    if (!playing) stage?.finish(false);
    await refresh(s);
  }

  /** Hold `batch`'s wave: apply the snapshot, in which only the senders have
   *  changed, then draw the wave and wait for the stage to report it landed;
   *  while paused the wave flies all the same. With no stage to draw on, it is
   *  delivered at once. */
  async function hold(s: Sim, batch: EventBatch, delay: number): Promise<void> {
    await refresh(s);
    const { promise, resolve } = Promise.withResolvers<void>();
    const h: Held = { seq: ++holds, landed: promise, land: resolve };
    held = h;
    floorUntil = batch.virtual_ms + floorMs;
    const drawn = stage?.show(batch, delay, true) ?? false;
    if (!playing) stage?.finish(false);
    if (!drawn) await deliver(h);
  }

  /** Deliver held wave `h`, unless something else already has: `step(0)`
   *  closes the engine's sub-step at the same virtual instant, so the
   *  receivers merge it, and their counts show. */
  async function deliver(h: Held): Promise<void> {
    const s = sim;
    if (held !== h || s === null) return;
    const batch = await s.step(0);
    stage?.show(batch, 0, false);
    await refresh(s);
    held = null;
  }

  /** The stage reports the held wave landed. */
  function onLanded(): void {
    const h = held;
    if (h === null) return;
    h.land();
    void run(() => deliver(h));
  }

  /** Fly the held wave, if any, to its landing within a short wall time and
   *  deliver it. */
  async function flyFast(): Promise<void> {
    const h = held;
    if (h === null) return;
    if (stage) {
      stage.finish(true);
      await h.landed;
    }
    await deliver(h);
  }

  /** Fast-forward the run in flight before a command: land the held wave,
   *  then run on to one emit floor past it, flying the relays due there the
   *  same way. The run then stands at least one floor past its last threshold
   *  round, the instant gabion's floor (`runtime.rs` `want_immediate_flush`)
   *  lets the next eager flush go, whatever the floor knob says. */
  async function land(s: Sim): Promise<void> {
    await flyFast();
    if (virtualMs < floorUntil) {
      await meet(s, await s.step(floorUntil - virtualMs, EVENT), 0);
      await flyFast();
    }
    floorUntil = 0;
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
  async function feed(s: Sim): Promise<boolean> {
    const traffic = activePreset.traffic;
    if (traffic === undefined || cluster === null) return false;
    const nodes = cluster.nodes;
    const owed = Math.floor((traffic.rate_per_sec * (virtualMs - feedFrom)) / 1000) - trafficInjected;
    for (let i = 0; i < owed; i++) {
      const node = feedNode(traffic, nodes, trafficInjected);
      trafficInjected += 1;
      if (node === null) continue;
      const batch = await s.submitRequest(node, WATCHED_KEY, 1);
      if (activePreset.pacing === 'live') {
        stage?.show(batch, 0, false);
      } else if (isWave(batch)) {
        await hold(s, batch, 0);
        return true;
      }
    }
    return false;
  }

  /** One play-loop advance of `deltaMs` of virtual time. */
  async function advance(s: Sim, deltaMs: number): Promise<void> {
    if (await feed(s)) return;
    await meet(s, await s.step(deltaMs, activePreset.pacing === 'live' ? 'Time' : EVENT), 0);
    notePeriod(performance.now());
  }

  /** Measure how long a heartbeat period takes to watch: the wall time between
   *  two period boundaries the play loop crossed. */
  function notePeriod(now: number): void {
    const p = Math.floor(virtualMs / tickMs);
    if (p === periodIndex) return;
    if (periodWall !== 0) heartbeatWallMs = (now - periodWall) / (p - periodIndex);
    periodWall = periodIndex < 0 ? 0 : now;
    periodIndex = p;
  }

  /** Run `task` against the cluster as a command: first fast-forward the run
   *  in flight past its relays (Conflict 4) and drop the landed routes, then
   *  let it show its own effect. */
  function command(task: (s: Sim) => Promise<void>): Promise<void> {
    return run(async (s) => {
      await land(s);
      stage?.clearTrail();
      await task(s);
    });
  }

  /** A command with no batch to show: apply the snapshot after it. */
  const act = (cmd: (s: Sim) => Promise<unknown>): Promise<void> =>
    command(async (s) => {
      await cmd(s);
      await refresh(s);
    });

  /** Send a request of `burstHits` at `node` for the watched key, at the
   *  current virtual time. The node decides it in the frame of the press; an
   *  eager flush stands `ARRIVAL_MS` before it flies, and is held. */
  const sendBurst = (node: number): Promise<void> =>
    command(async (s) => meet(s, await s.submitRequest(node, WATCHED_KEY, burstHits), ARRIVAL_MS));

  /** Select node `id`, once a snapshot inspecting it has its rows, so the
   *  inspector opens on them rather than on nothing. Changes no count, so it
   *  lands nothing. */
  const select = (id: number): Promise<void> =>
    run(async (s) => {
      const snap = await s.snapshot(id, watch);
      selectedId = id;
      inspected = snap.inspected ?? null;
      cluster = snap;
    });

  /** The live nodes in zone `zone`, and every other live node. */
  function split(zone: number): [number[], number[]] {
    const inside: number[] = [];
    const rest: number[] = [];
    for (const n of cluster?.nodes ?? []) (n.zone === zone ? inside : rest).push(n.id);
    return [inside, rest];
  }

  /** Cut every link between zone `zone` and the rest of the cluster. */
  const cutZone = (zone: number): Promise<void> =>
    act((s) => s.cutZone(...split(zone)));

  /** Silence gateway `g` — the silent crash that triggers a failover — by
   *  cutting it from every other live node. */
  const silenceGateway = (g: number): Promise<void> =>
    act((s) => s.silence(g, nodeIds.filter((id) => id !== g)));

  /** Cut gateway `g` from its zone peers in one direction: it hears them, they
   *  never hear it. */
  const cutGatewayFromZone = (g: number, zone: number): Promise<void> =>
    act((s) => s.cutFromZone(g, split(zone)[0].filter((id) => id !== g)));

  /** Restore the scenario's own network, its loss included — undo a cut. It
   *  opens the links but moves no counts until the next heartbeat gossips, so
   *  the user plays or steps to watch the cluster reconcile. */
  const heal = (): Promise<void> => act((s) => s.heal());

  /** Spawn a fresh cold-start node into zone `zone` of the live cluster — no
   *  rebuild. It joins by gossip and catches up by anti-entropy; the
   *  re-snapshot drives the stage to fade it in at its ring slot and re-space
   *  the survivors. */
  const addNode = (zone: number): Promise<void> => act((s) => s.addNode(zone));

  /** Remove the live node with stable `id`. Survivors keep their ids and
   *  re-converge; the re-snapshot drives the stage to scale it out and glide
   *  the survivors into the gap it leaves. */
  const removeNode = (id: number): Promise<void> => act((s) => s.removeNode(id));

  function play(): void {
    if (playing || sim === null || loading) return;
    playing = true;
    lastWall = 0;
    periodIndex = -1;
    periodWall = 0;
    rafId = requestAnimationFrame(frame);
  }

  function pause(): void {
    playing = false;
    if (rafId !== 0) {
      cancelAnimationFrame(rafId);
      rafId = 0;
    }
  }

  function toggle(): void {
    if (playing) {
      pause();
    } else {
      play();
    }
  }

  /** Step to the next event, pausing first: land the held wave, if any;
   *  else, in a held preset, run to the next wave, which is held and flown, or
   *  to the next instant hits leave the window, or one heartbeat period when
   *  neither is coming; in the live one, one period. */
  async function stepOnce(): Promise<void> {
    if (loading) return;
    pause();
    await run(async (s) => {
      if (held !== null) {
        stage?.finish(false);
        return;
      }
      if (await feed(s)) return;
      const live = activePreset.pacing === 'live';
      const expiry = cluster?.next_expiry_ms;
      const delta = live || expiry === undefined ? tickMs : expiry - virtualMs;
      await meet(s, await s.step(delta, live ? 'Time' : EVENT), 0);
    });
  }

  function onKeydown(event: KeyboardEvent): void {
    if (event.target instanceof HTMLInputElement) return;
    if (event.key === ' ') {
      event.preventDefault();
      toggle();
    } else if (event.key === 'ArrowRight') {
      event.preventDefault();
      void stepOnce();
    }
  }

  onMount(() => {
    void bootstrap();
    const unwatch = watchTheme(() => (theme = readTheme()));
    const onWide = (event: MediaQueryListEvent): void => {
      controlsOpen = event.matches;
    };
    wide.addEventListener('change', onWide);
    return () => {
      pause();
      unwatch();
      wide.removeEventListener('change', onWide);
    };
  });
</script>

<svelte:window onkeydown={onKeydown} />

<div class="layout">
  <header>
    <h1>Gabion · <span class="subtitle">Gossip Visualizer</span></h1>
    {#if cluster !== null && appliedKnobs !== null}
      <p class="status">
        <span data-field="status-nodes">{cluster.nodes.length}</span>
        {noun(cluster.nodes.length, 'node')}
        {#if appliedKnobs.zone_mode === 'Configured'}
          in <span data-field="status-zones">{cluster.zones.filter((z) => z.live > 0).length}</span>
          {noun(cluster.zones.filter((z) => z.live > 0).length, 'zone')}
        {:else}
          · no node knows its zone, so every change goes to every peer directly
        {/if}
        · <span data-field="status-cut">{fmtCount(cutLinks)}</span>
        {noun(cutLinks, 'link')} cut · true total
        <span data-field="status-total">{fmtCount(cluster.oracle_total)}</span>
      </p>
    {/if}
    <p class="once">
      Everything here is one simulated run on one machine. Measured results, with credible
      intervals, are in the <a href={`${import.meta.env.BASE_URL}study/dashboard/`}>gossip study</a>.
    </p>
    <details class="method">
      <summary>How to read this</summary>
      <dl>
        <dt>zone</dt>
        <dd>An availability zone. A node's zone is whatever the node says it is.</dd>
        <dt>inside a zone (level 0)</dt>
        <dd>Delivery between nodes of one zone: a thin line that stays inside its ring.</dd>
        <dt>across zones (level 1)</dt>
        <dd>Delivery between zones: a thick arrow that crosses the empty centre.</dd>
        <dt>origin</dt>
        <dd>The node whose clients produced a change.</dd>
        <dt>gateway</dt>
        <dd>The member of a remote zone that an origin sends its changes to (◆). It relays them into its zone.</dd>
        <dt>relay</dt>
        <dd>A gateway forwarding changes it received from an origin to the rest of its zone.</dd>
        <dt>ack</dt>
        <dd>I hold every item of your stream so far. One number acknowledges everything below it.</dd>
        <dt>retransmission timeout (RTO)</dt>
        <dd>How long a sender waits for an ack before resending: one heartbeat.</dd>
        <dt>suspect</dt>
        <dd>A peer that left something unanswered for the suspect timeout, or a remote peer that said it cannot relay (◇). A suspect is not used as a gateway.</dd>
        <dt>gap report</dt>
        <dd>A receiver's message "I am missing part of your stream; resend from my ack".</dd>
        <dt>join sweep</dt>
        <dd>A one-off copy of a node's whole live store, sent to a zone member that just joined.</dd>
        <dt>backstop</dt>
        <dd>A gateway that can reach its origin but not its own zone says it cannot relay, and the origin moves to another gateway.</dd>
      </dl>
    </details>
  </header>

  <main>
    {#if error !== null}
      <div class="overlay error" role="alert">
        <strong>The simulation could not start.</strong>
        <span>{error}</span>
      </div>
    {:else if cluster === null || knobs === null || appliedKnobs === null || defaults === null || builtConfig === null}
      <div class="overlay" aria-live="polite">Loading the gossip engine…</div>
    {:else}
      <div class="workspace" aria-busy={loading}>
        <details class="controls" bind:open={controlsOpen}>
          <summary>Controls</summary>
          <ControlRail
            presets={PRESETS}
            activeId={activePreset.id}
            blurb={activePreset.blurb(builtConfig)}
            {nodeIds}
            zones={cluster.zones}
            {roles}
            {maxZones}
            canAdd={nodeIds.length < MAX_LIVE_NODES}
            bind:burstHits
            {knobs}
            {appliedKnobs}
            bucketMs={defaults.rule_bucket_ms}
            onSelectPreset={selectPreset}
            onApplyKnobs={() => void bootstrap()}
            onSend={(node) => void sendBurst(node)}
            onCutZone={(zone) => void cutZone(zone)}
            onSilence={(g) => void silenceGateway(g)}
            onCutFromZone={(g, zone) => void cutGatewayFromZone(g, zone)}
            onHeal={() => void heal()}
            onAddNode={(zone) => void addNode(zone)}
            onRemoveNode={(id) => void removeNode(id)}
          />
        </details>
        <div class="stage-pane" data-held={held?.seq ?? 0}>
          <Stage
            bind:this={stage}
            {cluster}
            {roles}
            running={playing}
            {speed}
            {trail}
            {onLanded}
            {hoveredId}
            {focusedId}
            {theme}
            onSelect={(node) => void select(node)}
            onDeselect={() => (selectedId = null)}
            onHover={(node) => (hoveredId = node)}
            onFocus={(node) => (focusedId = node)}
          />
        </div>
        <div class="detail-rail">
          <HeadlineMetric {cluster} />
          <div class="rail-body">
            {#if selectedNode !== null && selectedRows !== null}
              <NodeInspector
                node={selectedNode}
                peers={selectedRows.peers}
                zones={cluster.zones}
                zoneMode={appliedKnobs.zone_mode}
                oracleTotal={cluster.oracle_total}
                ruleLimit={appliedKnobs.rule_limit}
                {burstHits}
                onSend={(id) => void sendBurst(id)}
                onRemove={(id) => void removeNode(id)}
                onClose={() => (selectedId = null)}
              >
                <BucketStrata
                  cells={selectedRows.cells}
                  currentEpoch={cluster.bucket_epoch_now}
                  epochFraction={(cluster.virtual_ms % defaults.rule_bucket_ms) /
                    defaults.rule_bucket_ms}
                  liveBuckets={nominalBuckets(appliedKnobs.rule_window_ms, defaults.rule_bucket_ms)}
                  windowMs={appliedKnobs.rule_window_ms}
                  bucketMs={defaults.rule_bucket_ms}
                  limit={appliedKnobs.rule_limit}
                  atLimit={selectedNode.brim.kind === 'Rejected'}
                />
              </NodeInspector>
            {:else}
              <Dashboard
                {cluster}
                {history}
                version={chartVersion}
                limit={appliedKnobs.rule_limit}
                showLimit={activePreset.showsLimitChart === true}
                {theme}
                hotId={hoveredId ?? focusedId}
              />
            {/if}
          </div>
        </div>
        {#if loading}
          <div class="overlay rebuilding" aria-live="polite">Rebuilding…</div>
        {/if}
      </div>
    {/if}
  </main>

  <footer>
    <TransportBar
      {playing}
      busy={loading}
      bind:speed
      {virtualMs}
      {heartbeatWallMs}
      onToggle={toggle}
      onStep={() => void stepOnce()}
      onReset={() => void bootstrap()}
    />
  </footer>
</div>

<style>
  .layout {
    display: grid;
    grid-template-rows: auto 1fr auto;
    height: 100%;
  }

  header {
    padding: var(--gap-md) var(--gap-lg);
    border-bottom: 1px solid var(--color-border-line);
  }

  h1 {
    margin: 0;
    font-size: var(--font-size-title);
    font-weight: 650;
    letter-spacing: -0.01em;
  }

  .subtitle {
    color: var(--color-fg-muted);
    font-weight: 400;
  }

  .status {
    margin: var(--gap-xs) 0 0;
    font-size: var(--font-size-body);
    color: var(--color-fg);
  }

  .once {
    margin: var(--gap-xs) 0 0;
    max-width: 80ch;
    font-size: var(--font-size-caption);
    color: var(--color-fg);
  }

  .method {
    margin-top: var(--gap-xs);
    font-size: var(--font-size-caption);
  }

  .method summary {
    cursor: pointer;
    font-weight: 600;
  }

  .method dl {
    display: grid;
    grid-template-columns: max-content minmax(0, 80ch);
    gap: var(--gap-xs) var(--gap-md);
    max-height: 40vh;
    margin: var(--gap-sm) 0 0;
    overflow-y: auto;
  }

  .method dt {
    font-weight: 600;
  }

  .method dd {
    margin: 0;
  }

  main {
    position: relative;
    min-height: 0;
  }

  /* The three-pane chassis: quiet controls rail, the dominant stage (the single
     focal point — kept ≥ 55% so the rails never out-weigh it), and the charts
     dashboard rail. The rail widths sum to ~44% at typical desktop widths and
     cap on wide screens, so the stage only grows. Three layouts: three columns
     from 1100 px; two from 640 px (stage over the controls on the left, the
     detail rail the full height on the right); one scrolling column below. */
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
    font-weight: 650;
  }

  .stage-pane {
    grid-area: stage;
    position: relative;
    min-width: 0;
    min-height: 0;
  }

  /* The right rail: the pinned headline metric above a swappable body — the
     charts dashboard by default, the node inspector when a node is selected.
     Owns the panel chrome (the Dashboard/Inspector inside are borderless). */
  .detail-rail {
    grid-area: detail;
    display: flex;
    flex-direction: column;
    gap: var(--gap-md);
    min-width: 0;
    min-height: 0;
    height: 100%;
    padding: var(--gap-md);
    background: var(--color-panel);
    border-left: 1px solid var(--color-border-line);
  }

  /* A size container, so the dashboard can tell when its charts no longer
     fit together; then the body scrolls under the pinned headline. */
  .rail-body {
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    container-type: size;
  }

  @media (width >= 640px) {
    .controls > summary {
      display: none;
    }

    .detail-rail {
      overflow: hidden;
    }
  }

  /* The detail rail takes the right column at full height, so the headline and
     the whole of chart 1 are in the first view and the rest scroll; the
     controls sit under the stage and scroll in their own row. The stage keeps
     the larger row, since its ring scales with its height. */
  @media (640px <= width < 1100px) {
    .workspace {
      grid-template-columns: minmax(0, 1fr) clamp(220px, 32%, 280px);
      grid-template-areas:
        'stage detail'
        'rail detail';
      grid-template-rows: minmax(0, 7fr) minmax(0, 4fr);
    }
  }

  /* One scrolling column. `main` keeps a height so the absolutely positioned
     loading and error overlay stays visible; the square stage keeps the ring
     the same shape at every width. The rail body gets a definite height,
     because the dashboard sizes its charts from `height: 100%` rows, which
     collapse under an auto-height parent. */
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

    .method dl {
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
</style>
