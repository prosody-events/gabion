<script lang="ts">
  import { onMount } from 'svelte';
  import { Sim } from './lib/sim/sim';
  import type { ClusterState, EventBatch, SimConfig, SimEvent } from './lib/sim/types';
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
  let activePreset = $state<Preset>(DEFAULT_PRESET);
  // The Rust-side production defaults (`Sim.defaultConfig()`), the single source
  // the sliders open from — no hand-typed TS mirror. Fetched once during the
  // first load (the workspace is unmounted while `loading`, so the `null` window
  // is never rendered), then every knob default and the bucket width read from
  // it. Changing `gabion::defaults` is the only edit; the sliders follow.
  let defaults = $state<SimConfig | null>(null);
  // The rebuild knobs (cluster size, fanout, error budget, packet loss). Seated
  // from the active preset and merged over its config on each (re)build, so a
  // slider tweak followed by a rebuild explores the parameter space without
  // leaving the scenario. They take effect only on rebuild — a live engine can't
  // change its node count — so editing a knob just *stages* it; the rail's
  // explicit "Rebuild" applies the staged set in one build. `null` until the
  // first load seeds them from `defaults` (see `bootstrap`).
  let knobs = $state<Knobs | null>(null);
  // The knob values the *current* engine was built from. `bootstrap` snapshots
  // `knobs` into this on every (re)build, so the rail can show which sliders have
  // been moved since — staged but not yet applied — and enable Rebuild only when
  // there is something to apply.
  let appliedKnobs = $state<Knobs | null>(null);
  // The "Tune the cluster" disclosure's open state, lifted here so it survives
  // the workspace unmount during a rebuild's loading flash — a knob edit (which
  // no longer rebuilds) keeps it open, and an explicit Rebuild reopens it.
  let tuneOpen = $state(false);
  // Hits per burst, shared by a stage click and the control rail's Send so the
  // two always agree. Each burst targets the same watched key, growing the same
  // counter the preset seeded. The default sits well under the threshold-AE
  // budget (ε ≈ limit·bps/(10⁴·N), ~833 at the narrative presets' pinned
  // 1 000 000 limit), so a burst spreads only by lazy heartbeat — raising it (or
  // dropping the rule limit) far enough trips an eager flush, itself worth seeing.
  let burstHits = $state(25);
  // One gossip tick = the active cluster's gossip interval, so a Sandbox Step
  // advances exactly one heartbeat round and the invariant holds at any tick
  // setting (not just the old hard-coded 100 ms). `0` only before the first
  // build seeds `appliedKnobs`, when nothing steps. See `appliedKnobs`.
  const tickMs = $derived(appliedKnobs?.tick_interval_ms ?? 0);
  // Cap one advance so a backgrounded tab resuming rAF can't leap seconds of
  // virtual time in a single frame.
  const MAX_STEP_MS = 500;

  let sim: Sim | null = null;
  // Not named `state`: `$state(...)` would then parse as store-subscription
  // syntax (`$` + a variable called `state`) and confuse the compiler.
  let cluster: ClusterState | null = $state(null);
  // The latest step's gossip events, handed to the stage to animate as
  // light-beam packets. Only ever reassigned to a non-empty batch, so a
  // sub-tick step that produced nothing doesn't re-trigger the stage.
  let events: SimEvent[] = $state([]);
  let playing = $state(false);
  let speed = $state(1);
  let tick = $state(0);
  let virtualMs = $state(0);
  let error: string | null = $state(null);
  let loading = $state(true);

  // The live stable ids, in rank order — what the rail's remove/send controls
  // pick from and the count the Cluster controls gate on. Ids have gaps under
  // churn, so this is the authoritative live set, not a `0..count` range.
  // `$derived.by` (a closure) rather than a bare `$derived(cluster?…)`: `cluster`
  // is `$state` reassigned only inside async callbacks, so at this top-level
  // position TS control-flow narrows it to its `null` initializer. Reading it
  // through a closure keeps its declared `ClusterState | null` type (the closure
  // may run after a reassignment) — and is what Svelte's reactivity wants too.
  const nodeIds = $derived.by(() => (cluster?.nodes ?? []).map((n) => n.id));
  // The visualizer's upper bound on live nodes (the design's 6–100 range); the
  // rail disables Add here so a held key can't spawn runtimes without limit.
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
   *  other pinned fields (e.g. the rng seed) survive; the rule + gossip knobs
   *  seated from the preset (its limit, the shared window/tick defaults) win on
   *  drag, exactly like fanout/loss. Fanout is clamped to the peer count so a
   *  small cluster can't ask for more peers than it has; the bucket width is
   *  fixed so every window the slider picks stays a whole number of buckets. */
  function effectiveConfig(preset: Preset, k: Knobs, d: SimConfig): Partial<SimConfig> {
    const nodes = k.nodes;
    const fanout = Math.min(Math.max(k.fanout, 1), Math.max(nodes - 1, 1));
    return {
      ...preset.config,
      nodes,
      fanout,
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
    activePreset = preset;
    pause();
    loading = true;
    error = null;
    events = [];
    // Restart the sustained feed from a clean slate so a Reset mid-ramp doesn't
    // inherit stale carry or a stale node cursor.
    trafficCarry = 0;
    trafficInjected = 0;
    // Clear the rolling history so a rebuild starts the charts from a blank
    // slate (the first sample reshapes the fan for the new cluster). Only a
    // rebuild clears it — adding or removing a node reshapes in place, so the
    // shared time axis keeps running across a join or leave.
    history.reset();
    chartVersion += 1;
    try {
      // Source the Rust defaults once, then seed the knobs from the active
      // preset before the (still-unmounted) control rail mounts. After this
      // first pass both are non-null for the life of the page; Reset and knob
      // edits keep the current `knobs`, a preset switch re-seats them in
      // `selectPreset` before calling here.
      const d = defaults ?? (defaults = await Sim.defaultConfig());
      const k = knobs ?? (knobs = knobsFromPreset(preset, d));
      // Tear the previous engine down first; otherwise its spawned task,
      // runtimes, and tick channels leak for the life of the page (each
      // rebuild would pile up an engine).
      if (sim !== null) {
        await sim.shutdown();
        sim = null;
      }
      const fresh = await Sim.create(effectiveConfig(preset, k, d));
      await preset.seed(fresh);
      sim = fresh;
      // The engine now reflects the current knobs — mark them applied so the
      // rail's "staged" cue and Rebuild button reset to clean.
      appliedKnobs = { ...k };
      await refresh();
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    } finally {
      loading = false;
    }
  }

  /** Adopt a scenario: re-seat the knobs from its config (so the sliders mirror
   *  it), then build it. The rail's scenario buttons route here; Reset and knob
   *  changes call `bootstrap()` directly, keeping the current knobs. */
  function selectPreset(preset: Preset): void {
    // The rail (which routes here) only mounts after the first load seeds
    // `defaults`, so it is non-null; re-seat the knobs from the chosen scenario.
    if (defaults !== null) knobs = knobsFromPreset(preset, defaults);
    void bootstrap(preset);
  }

  async function refresh(): Promise<void> {
    if (sim === null) return;
    applySnapshot(await sim.snapshot());
  }

  /** Adopt a fresh snapshot as the current state and append it to the chart
   *  history — the one place per-frame state and the rolling series stay in
   *  step. */
  function applySnapshot(snap: ClusterState): void {
    cluster = snap;
    tick = snap.tick;
    virtualMs = snap.virtual_ms;
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
   *  for presets without one). The hits land at the current virtual time,
   *  *before* the step that gossips them, so a hit and its propagation animate
   *  in the same frame. Submit batches are dropped — they carry no packets; the
   *  following step's batch carries the beams the feed's eager flush triggers. */
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
    // by id. Group into one submit per node touched this advance.
    const liveNodes = cluster.nodes;
    const perNode = new Map<number, number>();
    for (let i = 0; i < hits; i++) {
      const id = liveNodes[(trafficInjected + i) % count].id;
      perNode.set(id, (perNode.get(id) ?? 0) + 1);
    }
    trafficInjected += hits;
    for (const [id, n] of perNode) {
      await sim.submitRequest(id, WATCHED_KEY, n);
    }
  }

  async function advance(deltaMs: number): Promise<void> {
    if (sim === null) return;
    try {
      await injectTraffic(deltaMs);
      showEvents(await sim.step(deltaMs));
      applySnapshot(await sim.snapshot());
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
      if (!playing) applySnapshot(await sim.snapshot());
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
      pause();
    }
  }

  /** Restore every link to lossless `Pass` — undo the active preset's partition
   *  or isolation. Pure, like a burst: it opens the links but moves no counts
   *  until the next tick gossips, so the user plays/steps to watch reconcile. */
  async function heal(): Promise<void> {
    if (sim === null) return;
    try {
      await sim.heal();
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
      pause();
    }
  }

  /** Spawn a fresh cold-start node into the live cluster — no rebuild. It joins
   *  by gossip and catches up by anti-entropy; the re-snapshot drives the stage
   *  to fade it in at its ring slot and re-space the survivors. Play (or it is
   *  already playing) to watch it converge. */
  async function addNode(): Promise<void> {
    if (sim === null) return;
    try {
      await sim.addNode();
      applySnapshot(await sim.snapshot());
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
      pause();
    }
  }

  /** Remove the live node with stable `id`. Survivors keep their ids and
   *  re-converge; the re-snapshot drives the stage to scale it out and glide
   *  the survivors into the gap it leaves. */
  async function removeNode(id: number): Promise<void> {
    if (sim === null) return;
    try {
      await sim.removeNode(id);
      applySnapshot(await sim.snapshot());
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
      pause();
    }
  }

  function play(): void {
    if (playing || sim === null) return;
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
    if (playing) return;
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
    return () => pause();
  });
</script>

<svelte:window onkeydown={onKeydown} />

<div class="layout">
  <header>
    <h1>Gabion · <span class="subtitle">Gossip Visualizer</span></h1>
    <p class="lede">
      A cluster of nodes spreading per-origin rate-limit counters by anti-entropy
      gossip. Pick a scenario and press play to watch every node converge on the
      cluster-wide total — or click any node (or use the controls) to inject more.
    </p>
  </header>

  <main>
    {#if error !== null}
      <div class="overlay error" role="alert">
        <strong>The simulation could not start.</strong>
        <span>{error}</span>
      </div>
    {:else if loading || knobs === null || appliedKnobs === null || defaults === null}
      <div class="overlay" aria-live="polite">Loading the gossip engine…</div>
    {:else}
      <div class="workspace">
        <ControlRail
          presets={PRESETS}
          activeId={activePreset.id}
          {nodeIds}
          canAdd={nodeIds.length < MAX_LIVE_NODES}
          bind:burstHits
          {knobs}
          {appliedKnobs}
          bucketMs={defaults.rule_bucket_ms}
          {tuneOpen}
          onToggleTune={(open) => (tuneOpen = open)}
          onSelectPreset={selectPreset}
          onApplyKnobs={() => void bootstrap()}
          onSend={(node) => void sendBurst(node)}
          onHeal={() => void heal()}
          onAddNode={() => void addNode()}
          onRemoveNode={(id) => void removeNode(id)}
        />
        <div class="stage-pane">
          <Stage
            {cluster}
            {events}
            {selectedId}
            onSelect={(node) => (selectedId = node)}
            onDeselect={() => (selectedId = null)}
            onDeleteNode={(id) => void removeNode(id)}
          />
        </div>
        <div class="detail-rail">
          <HeadlineMetric {cluster} {history} version={chartVersion} />
          <div class="rail-body">
            {#if selectedNode !== null}
              <NodeInspector
                node={selectedNode}
                oracleTotal={cluster?.oracle_total ?? 0}
                ruleLimit={appliedKnobs.rule_limit}
                baseFanout={appliedKnobs.fanout}
                {burstHits}
                version={chartVersion}
                onSend={(id) => void sendBurst(id)}
                onClose={() => (selectedId = null)}
              >
                <BucketStrata
                  cells={selectedNode.cells}
                  currentEpoch={cluster?.bucket_epoch_now ?? 0}
                  epochFraction={((cluster?.virtual_ms ?? 0) % defaults.rule_bucket_ms) /
                    defaults.rule_bucket_ms}
                  liveBuckets={nominalBuckets(appliedKnobs.rule_window_ms, defaults.rule_bucket_ms)}
                  windowMs={appliedKnobs.rule_window_ms}
                  bucketMs={defaults.rule_bucket_ms}
                  limit={appliedKnobs.rule_limit}
                />
              </NodeInspector>
            {:else}
              <Dashboard
                {history}
                version={chartVersion}
                limit={appliedKnobs.rule_limit}
                showLimit={activePreset.showsLimitChart === true}
              />
            {/if}
          </div>
        </div>
      </div>
    {/if}
  </main>

  <footer>
    <TransportBar
      {playing}
      bind:speed
      {tick}
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
    padding: var(--space-3) var(--space-4);
    background: var(--chrome-bg);
    border-bottom: 1px solid var(--chrome-border);
  }

  h1 {
    margin: 0;
    font-size: var(--text-xl);
    font-weight: 650;
    letter-spacing: -0.01em;
  }

  .subtitle {
    color: var(--ink-soft);
    font-weight: 400;
  }

  .lede {
    margin: var(--space-1) 0 0;
    max-width: 66ch;
    font-size: var(--text-sm);
    color: var(--ink-soft);
  }

  main {
    position: relative;
    min-height: 0;
    background: var(--stage-bg);
  }

  /* The three-pane chassis: quiet controls rail, the dominant stage (the single
     focal point — kept ≥ 55% so the rails never out-weigh it), and the charts
     dashboard rail. The rail widths sum to ~44% at typical desktop widths and
     cap on wide screens, so the stage only grows. Stacks vertically when narrow. */
  .workspace {
    display: grid;
    grid-template-columns: clamp(200px, 16%, 260px) minmax(0, 1fr) clamp(300px, 28%, 380px);
    height: 100%;
    min-height: 0;
  }

  .stage-pane {
    position: relative;
    min-width: 0;
    min-height: 0;
  }

  /* The right rail: the pinned headline metric above a swappable body — the
     charts dashboard by default, the node inspector when a node is selected.
     Owns the panel chrome (the Dashboard/Inspector inside are borderless). */
  .detail-rail {
    display: flex;
    flex-direction: column;
    gap: var(--space-3);
    min-width: 0;
    min-height: 0;
    height: 100%;
    padding: var(--space-3);
    background: var(--chrome-panel);
    border-left: 1px solid var(--chrome-border);
    overflow: hidden;
  }

  .rail-body {
    flex: 1;
    min-height: 0;
  }

  @media (max-width: 880px) {
    .workspace {
      grid-template-columns: 1fr;
      grid-template-rows: auto minmax(0, 1.2fr) minmax(0, 1fr);
    }
  }

  .overlay {
    position: absolute;
    inset: 0;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: var(--space-2);
    color: var(--ink-soft);
    font-size: var(--text-sm);
  }

  .overlay.error {
    color: var(--signal-reject);
    padding: var(--space-4);
    text-align: center;
  }

  .overlay.error strong {
    color: var(--ink);
  }
</style>
