<script lang="ts">
  import type { NodeState } from '../sim/types';
  import InfoTip from './InfoTip.svelte';

  // §4 of the node inspector: is this node still alive, and how busy is its
  // gossip loop? Three honest layers —
  //   (a) a pulse sparkline of recent ticks, proving the heartbeat keeps firing
  //       even when the cluster is quiet (the "still ticking, not dead" story);
  //   (b) heartbeat vs threshold-triggered ticks — how often an eager flush beat
  //       the timer;
  //   (c) working vs idle ticks — how often a tick actually had dirty cells to
  //       gossip (idle is calm, not an error).
  //
  // No per-node tick history exists in `ChartHistory`, so the sparkline is fed by
  // a component-local ring buffer: each snapshot (driven by the `version` prop)
  // pushes the tick delta since the last sample. Samples are accumulated over a
  // small stride so each point sums a few frames — at 60 fps a raw per-frame
  // delta is mostly 0/1 and reads as noise.
  let { node, version }: { node: NodeState; version: number } = $props();

  const RING_CAP = 48;
  const STRIDE = 4;

  // The sparkline samples. `$state` so the derived polyline re-renders on push.
  let pulses = $state<number[]>([]);
  // Plain `let` cursors — they persist across runs without joining reactivity, so
  // writing the ring never re-triggers the effect (no loop).
  let prevTicks = 0;
  let prevNodeId = -1;
  let accum = 0;
  let sinceCommit = 0;

  $effect(() => {
    void version; // the per-snapshot trigger
    const id = node.id;
    const ticks = node.ticks_total;
    if (id !== prevNodeId) {
      // Selection switched — start the trace fresh for the new node.
      pulses = [];
      prevTicks = ticks;
      prevNodeId = id;
      accum = 0;
      sinceCommit = 0;
      return;
    }
    accum += Math.max(0, ticks - prevTicks);
    prevTicks = ticks;
    sinceCommit += 1;
    if (sinceCommit >= STRIDE) {
      pulses.push(accum);
      if (pulses.length > RING_CAP) pulses.shift();
      accum = 0;
      sinceCommit = 0;
    }
  });

  // Inline-SVG polyline over the ring, auto-scaled to its own peak. Drawn on a
  // 100×24 viewBox with 2px headroom; a flat baseline when there is nothing yet.
  const sparkPoints = $derived.by(() => {
    if (pulses.length < 2) return '';
    const peak = Math.max(...pulses, 1);
    const span = pulses.length - 1;
    return pulses
      .map((v, i) => {
        const x = (i / span) * 100;
        const y = 24 - (v / peak) * 22;
        return `${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .join(' ');
  });

  // Layer (b): of all ticks, the heartbeat (timer) share vs the threshold
  // (eager-flush) share.
  const thresholdPct = $derived(
    node.ticks_total > 0 ? (node.threshold_fires / node.ticks_total) * 100 : 0,
  );

  // Layer (c): of all ticks, the share that actually carried dirty cells.
  const workedPct = $derived(
    node.ticks_total > 0 ? (node.dirty_ticks / node.ticks_total) * 100 : 0,
  );

  // The direct status word. Dirty rows queued now → there is news to push;
  // otherwise recent ticks mean the heartbeat is alive but idle; no recent ticks
  // means quiet (paused or settled with nothing ticking through).
  const hasDirty = $derived(node.local_dirty_len + node.forwarded_dirty_len > 0);
  const recentlyTicking = $derived(pulses.some((v) => v > 0));
  const status = $derived(
    hasDirty ? 'Gossiping' : recentlyTicking ? 'Idle — heartbeat only' : 'Quiet',
  );
</script>

<div class="cadence">
  <div class="status" role="status">
    <span class="dot" class:live={status === 'Gossiping'} aria-hidden="true"></span>
    <span class="status-word">{status}</span>
    <span class="ticks numeric" title="cumulative gossip ticks">{node.ticks_total.toLocaleString()} ticks</span>
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
      <InfoTip text="Of all gossip ticks, the share triggered eagerly by a burst crossing the per-rule error budget (vs the proactive heartbeat timer).">
        heartbeat vs threshold
      </InfoTip>
    </span>
    <div class="bar" role="img" aria-label="{Math.round(thresholdPct)}% of ticks were threshold-triggered">
      <div class="seg heartbeat" style="width: {100 - thresholdPct}%"></div>
      <div class="seg threshold" style="width: {thresholdPct}%"></div>
    </div>
  </div>

  <div class="bar-row">
    <span class="bar-label">
      <InfoTip text="Of all gossip ticks, the share during which at least one cell was dirty and actually gossiped out. Idle ticks are heartbeats with nothing to send — expected, not an error.">
        working vs idle
      </InfoTip>
    </span>
    <div class="bar" role="img" aria-label="{Math.round(workedPct)}% of ticks carried gossip work">
      <div class="seg worked" style="width: {workedPct}%"></div>
      <div class="seg idle" style="width: {100 - workedPct}%"></div>
    </div>
  </div>
</div>

<style>
  .cadence {
    display: flex;
    flex-direction: column;
    gap: var(--space-2);
  }

  .status {
    display: flex;
    align-items: baseline;
    gap: var(--space-2);
    font-size: var(--text-sm);
  }

  .dot {
    align-self: center;
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--ink-faint);
    flex: none;
  }

  /* Alive-and-gossiping pulse; gated so reduced motion shows a steady dot. */
  .dot.live {
    background: var(--signal-dirty);
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
    color: var(--ink);
  }

  .ticks {
    margin-left: auto;
    color: var(--ink-faint);
  }

  /* The liveness trace: a quiet dirty-hued line proving the heartbeat fires. */
  .spark {
    width: 100%;
    height: 24px;
    display: block;
  }

  .spark polyline {
    fill: none;
    stroke: var(--signal-dirty);
    stroke-width: 1.5;
    stroke-linejoin: round;
    stroke-linecap: round;
    vector-effect: non-scaling-stroke;
  }

  .spark line {
    stroke: var(--chrome-border);
    stroke-width: 1.5;
    vector-effect: non-scaling-stroke;
  }

  .bar-row {
    display: grid;
    grid-template-columns: 9rem 1fr;
    align-items: center;
    gap: var(--space-2);
    font-size: var(--text-sm);
  }

  .bar-label {
    color: var(--ink-soft);
  }

  .bar {
    display: flex;
    height: 8px;
    border-radius: 4px;
    overflow: hidden;
    background: var(--chrome-bg);
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

  .seg.heartbeat,
  .seg.worked {
    background: var(--node-fill);
  }

  .seg.threshold {
    background: var(--signal-dirty);
  }

  .seg.idle {
    background: transparent;
  }
</style>
