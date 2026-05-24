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
  import Stage from './lib/components/Stage.svelte';
  import Dashboard from './lib/components/Dashboard.svelte';
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
  // The rebuild knobs (cluster size, fanout, error budget, packet loss). Seated
  // from the active preset and merged over its config on each (re)build, so a
  // slider tweak followed by a rebuild explores the parameter space without
  // leaving the scenario. They take effect only on rebuild — a live engine can't
  // change its node count — so the rail's sliders rebuild on release.
  let knobs = $state<Knobs>(knobsFromPreset(DEFAULT_PRESET));
  // Hits per burst, shared by a stage click and the control rail's Send so the
  // two always agree. Each burst targets the same watched key, growing the same
  // counter the preset seeded. The default sits well under the threshold-AE
  // budget (ε ≈ limit·bps/(10⁴·N), ~900 at the default limit), so a burst
  // spreads only by lazy heartbeat — raising it far enough trips an eager
  // flush, which is itself worth seeing.
  let burstHits = $state(25);
  // One gossip tick at the production default (`GOSSIP_TICK_INTERVAL_MILLIS`),
  // which the presets leave unset.
  const TICK_MS = 100;
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

  // The rolling chart history is a plain (non-reactive) structure — 600 samples
  // under a deep `$state` proxy would cost on every frame. `chartVersion` is the
  // single reactive signal the dashboard redraws on; it ticks on each new sample.
  const history = new ChartHistory();
  let chartVersion = $state(0);

  // The active preset's sustained feed (the overload scenario), folded into the
  // play loop. Carry-save accumulation makes the total injected by virtual time
  // T exactly ⌊rate·T⌋ regardless of how play chunked the steps — deterministic
  // and replayable from zero. `trafficInjected` is the running global hit count:
  // it caps the feed and indexes the round-robin node spread, so both stay
  // chunk-independent too. Plain `let` (not `$state`) — nothing renders them.
  let trafficCarry = 0;
  let trafficInjected = 0;

  let rafId = 0;
  let lastWall = 0;
  // Exactly one advance is ever in flight: the play loop will not issue the
  // next `step` until the previous resolves, so commands can't pile up faster
  // than the engine drains them.
  let stepping = false;

  /** The preset's config with the live knob values layered on top. The preset's
   *  other pinned fields (e.g. the overload limit, the rng seed) survive; fanout
   *  is clamped to the peer count so a small cluster can't ask for more peers
   *  than it has. */
  function effectiveConfig(preset: Preset): Partial<SimConfig> {
    const nodes = knobs.nodes;
    const fanout = Math.min(Math.max(knobs.fanout, 1), Math.max(nodes - 1, 1));
    return {
      ...preset.config,
      nodes,
      fanout,
      target_err_bps: knobs.target_err_bps,
      uniform_loss: knobs.uniform_loss,
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
    // slate (the first sample re-shapes it for the cluster's node count).
    history.reset(0);
    chartVersion += 1;
    try {
      // Tear the previous engine down first; otherwise its spawned task,
      // runtimes, and tick channels leak for the life of the page (each
      // rebuild would pile up an engine).
      if (sim !== null) {
        await sim.shutdown();
        sim = null;
      }
      const fresh = await Sim.create(effectiveConfig(preset));
      await preset.seed(fresh);
      sim = fresh;
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
    knobs = knobsFromPreset(preset);
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
    const wallDelta = lastWall === 0 ? TICK_MS / 6 : now - lastWall;
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
    let hits = Math.floor(trafficCarry);
    trafficCarry -= hits;
    hits = Math.min(hits, traffic.cap - trafficInjected);
    if (hits <= 0) return;
    // Spread by global hit index so the per-node split is chunk-independent;
    // group into one submit per node touched this advance.
    const perNode = new Map<number, number>();
    for (let i = 0; i < hits; i++) {
      const node = (trafficInjected + i) % count;
      perNode.set(node, (perNode.get(node) ?? 0) + 1);
    }
    trafficInjected += hits;
    for (const [node, n] of perNode) {
      await sim.submitRequest(node, WATCHED_KEY, n);
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
    await advance(TICK_MS);
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
    {:else if loading}
      <div class="overlay" aria-live="polite">Loading the gossip engine…</div>
    {:else}
      <div class="workspace">
        <ControlRail
          presets={PRESETS}
          activeId={activePreset.id}
          nodeCount={cluster?.nodes.length ?? 0}
          bind:burstHits
          {knobs}
          onSelectPreset={selectPreset}
          onApplyKnobs={() => void bootstrap()}
          onSend={(node) => void sendBurst(node)}
          onHeal={() => void heal()}
        />
        <div class="stage-pane">
          <Stage {cluster} {events} onSendBurst={(node) => void sendBurst(node)} />
        </div>
        <Dashboard
          {cluster}
          {history}
          version={chartVersion}
          limit={activePreset.config.rule_limit ?? 0}
          showLimit={activePreset.traffic !== undefined}
        />
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
    color: var(--on-stage-soft);
    font-size: var(--text-sm);
  }

  .overlay.error {
    color: #f4b4a4;
    padding: var(--space-4);
    text-align: center;
  }

  .overlay.error strong {
    color: var(--on-stage);
  }
</style>
