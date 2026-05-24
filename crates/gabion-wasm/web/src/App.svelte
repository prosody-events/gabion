<script lang="ts">
  import { onMount } from 'svelte';
  import { Sim } from './lib/sim/sim';
  import type { ClusterState, SimConfig, SimEvent } from './lib/sim/types';
  import Stage from './lib/components/Stage.svelte';
  import TransportBar from './lib/components/TransportBar.svelte';

  // Phase-3 skeleton scenario: a 12-node cluster with one burst of hits seeded
  // on node 0 at t=0. Pressing play advances virtual time so the burst gossips
  // outward and every node's aggregate climbs toward the true total — the first
  // end-to-end proof the real gabion core runs in a browser. Interactions
  // (click-to-send, presets, partitions) arrive in Phase 6.
  const CONFIG: Partial<SimConfig> = { nodes: 12, rng_seed: 1 };
  const SEED_NODE = 0;
  const SEED_KEY = 1;
  const SEED_HITS = 50;
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
    const snap = await sim.snapshot();
    cluster = snap;
    tick = snap.tick;
    virtualMs = snap.virtual_ms;
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

  async function advance(deltaMs: number): Promise<void> {
    if (sim === null) return;
    try {
      const batch = await sim.step(deltaMs);
      tick = batch.tick;
      virtualMs = batch.virtual_ms;
      if (batch.events.length > 0) events = batch.events;
      cluster = await sim.snapshot();
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
      every node agrees.
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
      <Stage {cluster} {events} />
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
