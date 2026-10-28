<script lang="ts">
  import type { NodeState } from '../sim/types';
  import InfoTip from './InfoTip.svelte';

  // §4 of the node inspector: how this node gossips — how *alive* it is and
  // how *often* it flushes. The flush *rate* adapts to load. This section makes
  // it visible:
  //
  //   (a) a round sparkline + status word — the heartbeat keeps firing even
  //       when the cluster is quiet (the "still ticking, not dead" story);
  //   (b) heartbeat vs threshold — what share of *recent* rounds were eager
  //       flushes (a burst crossing the error budget ε) rather than the timer.
  //
  // Why "recent": (b) is a ratio of cumulative counters. As the round counts
  // climb into the thousands, a lifetime ratio freezes — a whole burst barely
  // moves it. So it is computed over a rolling window fed by a component-local
  // ring buffer (no per-node history exists in `ChartHistory`): each snapshot
  // (driven by the `version` prop) accumulates the round / threshold deltas
  // since the last sample, committing a paired sample every `STRIDE`
  // snapshots — at 60 fps a raw per-frame delta is mostly 0/1 and reads as noise.
  let { node, version }: { node: NodeState; version: number } = $props();

  const RING_CAP = 48;
  const STRIDE = 4;

  // Rolling window of recent per-stride samples; each entry sums the round and
  // threshold-round deltas over `STRIDE` snapshots. One ring (not two) so the
  // counts can never desync across a stride boundary. `$state` so the derived
  // sparkline and gauge re-render on push.
  let samples = $state<{ ticks: number; threshold: number }[]>([]);
  // Plain `let` cursors — they persist across runs without joining reactivity, so
  // writing the ring never re-triggers the effect (no loop).
  let prevTicks = 0;
  let prevThreshold = 0;
  let prevNodeId = -1;
  let accTicks = 0;
  let accThreshold = 0;
  let sinceCommit = 0;

  const rounds = $derived(node.rounds_heartbeat + node.rounds_threshold);

  $effect(() => {
    void version; // the per-snapshot trigger
    const id = node.id;
    const ticks = rounds;
    const threshold = node.rounds_threshold;
    if (id !== prevNodeId) {
      // Selection switched — start the trace fresh for the new node.
      samples = [];
      prevTicks = ticks;
      prevThreshold = threshold;
      prevNodeId = id;
      accTicks = 0;
      accThreshold = 0;
      sinceCommit = 0;
      return;
    }
    accTicks += Math.max(0, ticks - prevTicks);
    accThreshold += Math.max(0, threshold - prevThreshold);
    prevTicks = ticks;
    prevThreshold = threshold;
    sinceCommit += 1;
    if (sinceCommit >= STRIDE) {
      samples.push({ ticks: accTicks, threshold: accThreshold });
      if (samples.length > RING_CAP) samples.shift();
      accTicks = 0;
      accThreshold = 0;
      sinceCommit = 0;
    }
  });

  // Inline-SVG polyline over recent round deltas, auto-scaled to its own peak.
  // Drawn on a 100×24 viewBox with 2px headroom; a flat baseline when empty.
  const sparkPoints = $derived.by(() => {
    if (samples.length < 2) return '';
    const peak = Math.max(...samples.map((s) => s.ticks), 1);
    const span = samples.length - 1;
    return samples
      .map((s, i) => {
        const x = (i / span) * 100;
        const y = 24 - (s.ticks / peak) * 22;
        return `${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .join(' ');
  });

  // Windowed share. `recentTicks === 0` means nothing ticked recently (paused,
  // or just switched node) — an honest empty state, not a phantom 0/100 split.
  const recentTicks = $derived(samples.reduce((sum, s) => sum + s.ticks, 0));
  const recentThreshold = $derived(samples.reduce((sum, s) => sum + s.threshold, 0));
  const hasRecent = $derived(recentTicks > 0);
  const thresholdPct = $derived(hasRecent ? (recentThreshold / recentTicks) * 100 : 0);

  // The direct status word: recent rounds mean the heartbeat is alive; none
  // means quiet (paused, or settled with nothing ticking through).
  const status = $derived(samples.some((s) => s.ticks > 0) ? 'Ticking' : 'Quiet');
</script>

<div class="cadence">
  <div class="status" role="status">
    <span class="dot" class:live={status === 'Ticking'} aria-hidden="true"></span>
    <span class="status-word">{status}</span>
    <span class="ticks numeric" title="cumulative gossip rounds">{rounds.toLocaleString()} ticks</span>
  </div>

  <svg class="spark" viewBox="0 0 100 24" preserveAspectRatio="none" aria-hidden="true">
    {#if sparkPoints !== ''}
      <polyline points={sparkPoints} />
    {:else}
      <line x1="0" y1="23" x2="100" y2="23" />
    {/if}
  </svg>

  <div class="bar-row">
    <span class="bar-label">
      <InfoTip text="Of recent gossip ticks (a rolling window of the last several seconds), the share triggered eagerly by a burst whose accumulated hits crossed the per-rule error budget ε, rather than by the proactive heartbeat timer. This is the burst-driven knob — a busy node shows a higher threshold share.">
        heartbeat vs threshold
      </InfoTip>
    </span>
    {#if hasRecent}
      <div class="bar" role="img" aria-label="{Math.round(thresholdPct)}% of recent ticks were threshold-triggered (error budget {node.error_budget})">
        <div class="seg heartbeat" style="width: {100 - thresholdPct}%"></div>
        <div class="seg threshold" style="width: {thresholdPct}%"></div>
      </div>
    {:else}
      <span class="bar-empty">no recent ticks</span>
    {/if}
  </div>

  <div class="budget numeric">
    {#if node.error_budget > 0}
      error budget ε <span class="budget-val">{node.error_budget.toLocaleString()}</span> hits/rule before an eager flush
    {:else}
      error budget ε <span class="budget-val">—</span> set on this node's first request
    {/if}
  </div>
</div>

<style>
  .cadence {
    display: flex;
    flex-direction: column;
    gap: var(--gap-sm);
  }

  .status {
    display: flex;
    align-items: baseline;
    gap: var(--gap-sm);
    font-size: var(--font-size-caption);
  }

  .dot {
    align-self: center;
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--color-fg-muted);
    flex: none;
  }

  /* Alive-and-gossiping pulse; gated so reduced motion shows a steady dot. */
  .dot.live {
    background: var(--color-warn);
  }

  @media (prefers-reduced-motion: no-preference) {
    .dot.live {
      animation: pulse 1.4s ease-in-out infinite;
    }
  }

  @keyframes pulse {
    0%,
    100% {
      opacity: 1;
    }
    50% {
      opacity: 0.35;
    }
  }

  .status-word {
    font-weight: 600;
    color: var(--color-fg);
  }

  .ticks {
    margin-left: auto;
    color: var(--color-fg-muted);
  }

  /* The liveness trace: a quiet amber line proving the heartbeat fires. */
  .spark {
    width: 100%;
    height: 24px;
    display: block;
  }

  .spark polyline {
    fill: none;
    stroke: var(--color-warn);
    stroke-width: 1.5;
    stroke-linejoin: round;
    stroke-linecap: round;
    vector-effect: non-scaling-stroke;
  }

  .spark line {
    stroke: var(--color-border-line);
    stroke-width: 1.5;
    vector-effect: non-scaling-stroke;
  }

  .bar-row {
    display: grid;
    grid-template-columns: 9rem 1fr;
    align-items: center;
    gap: var(--gap-sm);
    font-size: var(--font-size-caption);
  }

  .bar-label {
    color: var(--color-fg-muted);
  }

  .bar {
    display: flex;
    height: 8px;
    border-radius: 4px;
    overflow: hidden;
    background: var(--color-bg);
  }

  .seg {
    height: 100%;
    transition: width 220ms ease;
  }

  @media (prefers-reduced-motion: reduce) {
    .seg {
      transition: none;
    }
  }

  .seg.heartbeat {
    background: var(--chart-muted);
  }

  .seg.threshold {
    background: var(--color-warn);
  }

  /* Honest empty state — reserves the row height so the layout never jumps. */
  .bar-empty {
    display: flex;
    align-items: center;
    height: 8px;
    color: var(--color-fg-muted);
    font-style: italic;
    font-size: var(--font-size-caption);
  }

  .budget {
    font-size: var(--font-size-caption-sm);
    color: var(--color-fg-muted);
  }

  .budget-val {
    color: var(--color-fg-muted);
  }
</style>
