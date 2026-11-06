<script lang="ts">
  import { onMount } from 'svelte';
  import { fmtCount, noun } from './lib/format';
  import { Sim } from './lib/sim/sim';
  import { readTheme, watchTheme } from './lib/theme';
  import {
    ANSWER,
    type ClusterState,
    type EventBatch,
    type Inspected,
    type PeerView,
    type SimConfig,
    type SimEvent,
    type Watch,
  } from './lib/sim/types';
  import type { Roles } from './lib/stage/renderer';
  import { ChartHistory } from './lib/charts/history';
  import {
    DEFAULT_PRESET,
    PRESETS,
    WATCHED_KEY,
    knobsFromPreset,
    type Knobs,
    type Preset,
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
  // config plus an opening seed against a fresh cluster. Pressing play advances
  // virtual time so the seeded traffic gossips outward, hop by hop, until every
  // node's aggregate reaches the true total — the end-to-end proof the real
  // gabion core runs in a browser. Reset re-runs the active preset; selecting
  // another rebuilds. Clicking a node (or the rail's Send) injects more traffic
  // for the watched key on top.
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
  // Hits per burst, shared by a stage click and the control rail's Send so the
  // two always agree. Each burst targets the same watched key, growing the same
  // counter the preset seeded. The default sits well under the threshold-AE
  // budget (ε ≈ limit·bps/(10⁴·N), ~833 at the narrative presets' pinned
  // 1 000 000 limit), so a burst spreads only by lazy heartbeat — raising it (or
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
  // One Step = the active cluster's heartbeat period, so a Sandbox Step runs
  // every node's heartbeat exactly once, each at its own phase, at any
  // interval. `0` only before the first build seeds `appliedKnobs`, when nothing
  // steps. See `appliedKnobs`.
  const tickMs = $derived(appliedKnobs?.tick_interval_ms ?? 0);
  // Cap one advance so a backgrounded tab resuming rAF can't leap seconds of
  // virtual time in a single frame.
  const MAX_STEP_MS = 500;

  let sim: Sim | null = null;
  // Not named `state`: `$state(...)` would then parse as store-subscription
  // syntax (`$` + a variable called `state`) and confuse the compiler.
  let cluster: ClusterState | null = $state(null);
  // The latest step's gossip events, handed to the stage to animate as beams.
  // Only ever reassigned to a non-empty batch, so a sub-tick step that produced
  // nothing doesn't re-trigger the stage.
  let events: SimEvent[] = $state([]);
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
  let roles = $state.raw<Roles | null>(null);
  let rolesKey = '';
  $effect(() => {
    const next = rolesOf(selectedId, selectedRows?.peers);
    const key =
      next === null
        ? ''
        : `${next.origin}:${[...next.gateways].map(([g, r]) => `${g}/${r.zone}/${r.suspect}`).join(',')}`;
    if (key !== rolesKey) {
      rolesKey = key;
      roles = next;
    }
  });

  // The rolling chart history is a plain (non-reactive) structure — 600 samples
  // under a deep `$state` proxy would cost on every frame. `chartVersion` is the
  // single reactive signal the dashboard redraws on; it ticks on each new sample.
  const history = new ChartHistory();
  let chartVersion = $state(0);

  // The active preset's background feed (every narrative preset and overload;
  // not sandbox), folded into the play loop. Carry-save accumulation makes the
  // total injected by virtual time T exactly ⌊rate·T⌋ regardless of how play
  // chunked the steps — deterministic and replayable from zero. The feed is
  // uncapped (the windowed oracle decays on its own); `trafficInjected` is the
  // running global hit count, kept only to index the round-robin node spread so
  // it stays chunk-independent. Plain `let` (not `$state`) — nothing renders them.
  let trafficCarry = 0;
  let trafficInjected = 0;

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

  /** Build the cluster for `preset`, run its opening seed, and adopt it as the
   *  active scenario. Reset and a knob change re-run the current preset with the
   *  current knobs; the rail's scenario buttons pass a new one (via
   *  `selectPreset`, which re-seats the knobs first). */
  async function bootstrap(preset: Preset = activePreset): Promise<void> {
    pause();
    loading = true;
    error = null;
    events = [];
    hoveredId = null;
    // Restart the sustained feed from a clean slate so a Reset mid-ramp doesn't
    // inherit stale carry or a stale node cursor.
    trafficCarry = 0;
    trafficInjected = 0;
    try {
      // Source the Rust defaults once, then seed the knobs from the active
      // preset before the control rail first shows. After this first pass both
      // are non-null for the life of the page; Reset and knob edits keep the
      // current `knobs`, a preset switch re-seats them in `selectPreset` before
      // calling here.
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
      await preset.seed(fresh, config).catch(async (e: unknown) => {
        await fresh.shutdown();
        throw e;
      });
      sim = fresh;
      window.sim = fresh;
      // The engine now reflects the current knobs — mark them applied so the
      // rail's "staged" cue and Rebuild button reset to clean.
      appliedKnobs = { ...k };
      // The selection and the cluster it names arrive together, so the effect
      // that drops a selection no live node has never sees one without the other.
      const snap = await fresh.snapshot(preset.select ?? null, watch);
      selectedId = preset.select ?? null;
      applySnapshot(snap);
      // Adopted only once built, so the rail's `aria-current` marks a finished
      // build.
      builtConfig = config;
      activePreset = preset;
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    } finally {
      loading = false;
    }
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
    const wallDelta = lastWall === 0 ? tickMs / 6 : now - lastWall;
    lastWall = now;
    const deltaMs = Math.min(Math.round(wallDelta * speed), MAX_STEP_MS);
    if (deltaMs <= 0) return;
    stepping = true;
    void advance(deltaMs).finally(() => {
      stepping = false;
    });
  }

  /** Show a batch's gossip events as stage beams — but only when it produced
   *  some, so a no-op sub-tick step doesn't clear beams still in flight. The one
   *  place this invariant lives; both the play loop and a click route through it. */
  function showEvents(batch: EventBatch): void {
    if (batch.events.length > 0) events = batch.events;
  }

  /** Drive the active preset's sustained feed for a `deltaMs` advance (a no-op
   *  for presets without one): many clients each sending a one-hit request,
   *  which its node admits or rejects. The requests land at the current virtual
   *  time, *before* the step that gossips them, so a hit and its propagation
   *  animate in the same frame. The submit batches, eager-flush packets
   *  included, are not drawn; the following step delivers those packets. */
  async function injectTraffic(deltaMs: number): Promise<void> {
    const traffic = activePreset.traffic;
    if (traffic === undefined || sim === null || cluster === null) return;
    const count = cluster.nodes.length;
    if (count === 0) return;
    trafficCarry += (traffic.rate_per_sec * deltaMs) / 1000;
    const hits = Math.floor(trafficCarry);
    trafficCarry -= hits;
    if (hits <= 0) return;
    // Spread by global hit index so the per-node split is chunk-independent;
    // the round-robin runs over *ranks* (positions in the live node list) and
    // resolves each to that node's stable id, since the engine addresses nodes
    // by id.
    const liveNodes = cluster.nodes;
    for (let i = 0; i < hits; i++) {
      await sim.submitRequest(liveNodes[(trafficInjected + i) % count].id, WATCHED_KEY, 1);
    }
    trafficInjected += hits;
  }

  async function advance(deltaMs: number): Promise<void> {
    if (sim === null) return;
    try {
      await injectTraffic(deltaMs);
      showEvents(await sim.step(deltaMs));
      applySnapshot(await sim.snapshot(selectedId, watch));
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
      pause();
    }
  }

  /** Run one command against the live sim, then show its effect: a snapshot at
   *  the current virtual time (a command never advances time), which also
   *  refreshes the cut-link count. Any failure stops play and surfaces. */
  async function act(command: (sim: Sim) => Promise<unknown>): Promise<void> {
    if (sim === null) return;
    try {
      await command(sim);
      applySnapshot(await sim.snapshot(selectedId, watch));
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
      pause();
    }
  }

  /** Inject a burst at `node` for the watched key, at the current virtual time —
   *  a pure inject: it never advances time, so the stage/charts only move on the
   *  next step or play. While playing the loop snapshots every frame, so a click
   *  just shows its beams; while paused nothing else will, so it snapshots once. */
  async function sendBurst(node: number): Promise<void> {
    if (sim === null) return;
    try {
      showEvents(await sim.submitRequest(node, WATCHED_KEY, burstHits));
      if (!playing) applySnapshot(await sim.snapshot(selectedId, watch));
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
      pause();
    }
  }

  /** Select node `id`, once a snapshot inspecting it has its rows, so the
   *  inspector opens on them rather than on nothing. */
  async function select(id: number): Promise<void> {
    if (sim === null) return;
    try {
      const snap = await sim.snapshot(id, watch);
      selectedId = id;
      inspected = snap.inspected ?? null;
      cluster = snap;
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
      pause();
    }
  }

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

  /** Restore the scenario's own network, its loss included — undo a cut.
   *  Pure, like a burst: it opens the links but moves no counts until the next
   *  heartbeat gossips, so the user plays/steps to watch the cluster
   *  reconcile. */
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

  async function stepOnce(): Promise<void> {
    if (playing || loading) return;
    await advance(tickMs);
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
        <div class="stage-pane">
          <Stage
            {cluster}
            {events}
            {roles}
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
