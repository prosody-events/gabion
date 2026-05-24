<script lang="ts">
  import { onMount } from 'svelte';
  import { Sim } from './lib/sim/sim';
  import type { ClusterState, EventBatch, SimConfig, SimEvent } from './lib/sim/types';
  import { ChartHistory } from './lib/charts/history';
  import Stage from './lib/components/Stage.svelte';
  import Dashboard from './lib/components/Dashboard.svelte';
  import TransportBar from './lib/components/TransportBar.svelte';

  // A 12-node cluster with one burst of hits seeded on node 0 at t=0. Pressing
  // play advances virtual time so the burst gossips outward, hop by hop, until
  // every node's aggregate reaches the true total — the end-to-end proof the
  // real gabion core runs in a browser. The default rule limit is left high
  // (far above the seeded burst) so the burst stays well under gabion's
  // threshold anti-entropy and spreads by lazy heartbeat over several rounds —
  // the multi-hop propagation this view exists to show. A burst large enough to
  // approach the limit would trip the eager threshold flush and converge in one
  // round; that overload regime, and the Aggregate-vs-Limit chart it powers,
  // land as a Phase 6 preset. Clicking a node already injects a burst (see
  // `sendBurst`); the control rail, presets, and scrubber are still to come.
  const CONFIG: Partial<SimConfig> = { nodes: 12, rng_seed: 1 };
  const SEED_NODE = 0;
  const SEED_KEY = 1;
  const SEED_HITS = 50;
  // Clicking a node injects this many hits for the same watched key, so the
  // click grows the *same* counter the seed did and the burst spreads on the
  // next ticks. Sized well under the threshold-AE budget (ε ≈ limit·bps/(10⁴·N),
  // ~900 at the default limit) so a click never trips an eager flush by itself.
  const CLICK_HITS = 25;
  // One gossip tick at the production default (`GOSSIP_TICK_INTERVAL_MILLIS`),
  // which `CONFIG` leaves unset.
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

  let rafId = 0;
  let lastWall = 0;
  // Exactly one advance is ever in flight: the play loop will not issue the
  // next `step` until the previous resolves, so commands can't pile up faster
  // than the engine drains them.
  let stepping = false;

  async function bootstrap(): Promise<void> {
    pause();
    loading = true;
    error = null;
    events = [];
    // Clear the rolling history so a Reset starts the charts from a blank slate
    // (the first sample re-shapes it for the cluster's node count).
    history.reset(0);
    chartVersion += 1;
    try {
      // Tear the previous engine down first; otherwise its spawned task,
      // runtimes, and tick channels leak for the life of the page (Reset would
      // pile up an engine per click).
      if (sim !== null) {
        await sim.shutdown();
        sim = null;
      }
      const fresh = await Sim.create(CONFIG);
      await fresh.submitRequest(SEED_NODE, SEED_KEY, SEED_HITS);
      sim = fresh;
      await refresh();
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    } finally {
      loading = false;
    }
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

  async function advance(deltaMs: number): Promise<void> {
    if (sim === null) return;
    try {
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
      showEvents(await sim.submitRequest(node, SEED_KEY, CLICK_HITS));
      if (!playing) applySnapshot(await sim.snapshot());
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
      gossip. Press play to watch one node's burst of {SEED_HITS} hits propagate until
      every node agrees — or click any node to send it a fresh burst of {CLICK_HITS}.
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
        <div class="stage-pane">
          <Stage {cluster} {events} onSendBurst={(node) => void sendBurst(node)} />
        </div>
        <Dashboard {cluster} {history} version={chartVersion} />
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

  /* Stage dominant on the left (the single focal point), the quieter charts
     dashboard on the right rail. Stacks vertically on a narrow viewport. */
  .workspace {
    display: grid;
    grid-template-columns: minmax(0, 1fr) clamp(320px, 32%, 440px);
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
      grid-template-rows: minmax(0, 1.2fr) minmax(0, 1fr);
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
