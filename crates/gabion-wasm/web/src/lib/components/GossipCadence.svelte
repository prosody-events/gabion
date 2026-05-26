<script lang="ts">
  import type { NodeState } from '../sim/types';
  import InfoTip from './InfoTip.svelte';

  // §4 of the node inspector: how this node gossips — how *alive* it is, how
  // *often* it flushes, and how *wide* it fans out. The runtime adapts the last
  // two under load; this section exists to make those adaptations visible:
  //
  //   (a) a tick sparkline + status word — the heartbeat keeps firing even when
  //       the cluster is quiet (the "still ticking, not dead" story);
  //   (b) adaptive fanout — the effective peer count the most recent emit chose,
  //       which grows above the configured base as the dirty set grows (a burst);
  //   (c) heartbeat vs threshold — what share of *recent* ticks were eager
  //       flushes (a burst crossing the error budget ε) rather than the timer;
  //   (d) working vs idle — what share of *recent* ticks actually carried gossip
  //       work (idle is calm, not an error).
  //
  // Why "recent": (c) and (d) are ratios of cumulative counters. As `ticks_total`
  // climbs into the thousands, a lifetime ratio freezes — a whole burst barely
  // moves it. So both are computed over a rolling window fed by a component-local
  // ring buffer (no per-node history exists in `ChartHistory`): each snapshot
  // (driven by the `version` prop) accumulates the tick / threshold / dirty
  // deltas since the last sample, committing a paired sample every `STRIDE`
  // snapshots — at 60 fps a raw per-frame delta is mostly 0/1 and reads as noise.
  let {
    node,
    version,
    baseFanout,
  }: { node: NodeState; version: number; baseFanout: number } = $props();

  const RING_CAP = 48;
  const STRIDE = 4;

  // Rolling window of recent per-stride samples; each entry sums the tick,
  // threshold-fire, and dirty-tick deltas over `STRIDE` snapshots. One ring (not
  // three) so the counts can never desync across a stride boundary. `$state` so
  // the derived sparkline and gauges re-render on push.
  let samples = $state<{ ticks: number; threshold: number; dirty: number }[]>([]);
  // Plain `let` cursors — they persist across runs without joining reactivity, so
  // writing the ring never re-triggers the effect (no loop).
  let prevTicks = 0;
  let prevThreshold = 0;
  let prevDirty = 0;
  let prevNodeId = -1;
  let accTicks = 0;
  let accThreshold = 0;
  let accDirty = 0;
  let sinceCommit = 0;

  $effect(() => {
    void version; // the per-snapshot trigger
    const id = node.id;
    const ticks = node.ticks_total;
    const threshold = node.threshold_fires;
    const dirty = node.dirty_ticks;
    if (id !== prevNodeId) {
      // Selection switched — start the trace fresh for the new node.
      samples = [];
      prevTicks = ticks;
      prevThreshold = threshold;
      prevDirty = dirty;
      prevNodeId = id;
      accTicks = 0;
      accThreshold = 0;
      accDirty = 0;
      sinceCommit = 0;
      return;
    }
    accTicks += Math.max(0, ticks - prevTicks);
    accThreshold += Math.max(0, threshold - prevThreshold);
    accDirty += Math.max(0, dirty - prevDirty);
    prevTicks = ticks;
    prevThreshold = threshold;
    prevDirty = dirty;
    sinceCommit += 1;
    if (sinceCommit >= STRIDE) {
      samples.push({ ticks: accTicks, threshold: accThreshold, dirty: accDirty });
      if (samples.length > RING_CAP) samples.shift();
      accTicks = 0;
      accThreshold = 0;
      accDirty = 0;
      sinceCommit = 0;
    }
  });

  // Inline-SVG polyline over recent tick deltas, auto-scaled to its own peak.
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

  // Windowed shares. `recentTicks === 0` means nothing ticked recently (paused,
  // or just switched node) — an honest empty state, not a phantom 0/100 split.
  const recentTicks = $derived(samples.reduce((sum, s) => sum + s.ticks, 0));
  const recentThreshold = $derived(samples.reduce((sum, s) => sum + s.threshold, 0));
  const recentDirty = $derived(samples.reduce((sum, s) => sum + s.dirty, 0));
  const hasRecent = $derived(recentTicks > 0);
  const thresholdPct = $derived(hasRecent ? (recentThreshold / recentTicks) * 100 : 0);
  const workedPct = $derived(hasRecent ? (recentDirty / recentTicks) * 100 : 0);

  // Adaptive fanout. The runtime caps the per-tick fanout at the peer count and
  // floors it at the configured base, growing it with the dirty-set size in
  // between (`config.fanout.max(⌊log₂(dirty)⌋+1).min(peers)`). The meter scale is
  // 0…peers; the base is the floor it never drops below, and any fill past the
  // base is the adaptive widening a burst caused.
  const peerCap = $derived(node.peers.length);
  const base = $derived(Math.min(baseFanout, peerCap));
  const effective = $derived(Math.min(node.effective_fanout, peerCap));
  const peak = $derived(Math.min(node.peak_fanout, peerCap));
  const hasPeers = $derived(peerCap > 0);
  const widened = $derived(effective > base);
  const basePct = $derived(hasPeers ? (Math.min(base, effective) / peerCap) * 100 : 0);
  const widenPct = $derived(hasPeers ? (Math.max(effective - base, 0) / peerCap) * 100 : 0);
  const peakPct = $derived(hasPeers ? (peak / peerCap) * 100 : 0);

  // The direct status word. Dirty rows queued now → there is news to push;
  // otherwise recent ticks mean the heartbeat is alive but idle; no recent ticks
  // means quiet (paused or settled with nothing ticking through). This "right
  // now" tri-state pairs with the "recent window" bars — the micro/macro reading.
  const hasDirty = $derived(node.local_dirty_len + node.forwarded_dirty_len > 0);
  const recentlyTicking = $derived(samples.some((s) => s.ticks > 0));
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

  <!-- Adaptive fanout: base → effective → peak, on a 0…peers scale. -->
  <div class="fanout">
    <div class="fanout-head">
      <span class="bar-label">
        <InfoTip text="How many peers this node gossiped to on its most recent emit. The runtime floors fanout at the configured base and grows it with the dirty-set size (⌊log₂(dirty)⌋+1), capped at the peer count — so a burst makes it fan out wider to converge faster, then it relaxes back to the base.">
          adaptive fanout
        </InfoTip>
      </span>
      <span class="fanout-now numeric" class:widened>
        {effective}<span class="sep"> / </span>{peerCap}
        <span class="unit">peers</span>
      </span>
    </div>
    {#if hasPeers}
      <div
        class="meter"
        role="meter"
        aria-valuemin="0"
        aria-valuemax={peerCap}
        aria-valuenow={effective}
        aria-valuetext="{effective} of {peerCap} peers; base {base}, peak {peak}"
      >
        <div class="seg fan-base" style="width: {basePct}%"></div>
        <div class="seg fan-widen" style="width: {widenPct}%"></div>
        <span class="peak-tick" style="left: {peakPct}%" aria-hidden="true"></span>
      </div>
      <div class="fanout-foot">
        <span>base {base}</span>
        <span>peak {peak}</span>
        <span class="widen-note" class:on={widened}>{widened ? 'widened by load' : 'at base'}</span>
      </div>
    {:else}
      <span class="bar-empty">no peers — nothing to fan out to</span>
    {/if}
  </div>

  <div class="bar-row">
    <span class="bar-label">
      <InfoTip text="Of recent gossip ticks (a rolling window of the last several seconds), the share triggered eagerly by a burst whose accumulated hits crossed the per-rule error budget ε, rather than by the proactive heartbeat timer. The same burst that widens the fanout fires this eager flush — so a busy node shows a higher threshold share.">
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

  <div class="bar-row">
    <span class="bar-label">
      <InfoTip text="Of recent gossip ticks (the same rolling window), the share during which at least one cell was dirty and actually gossiped out. Idle ticks are heartbeats with nothing to send — expected, not an error.">
        working vs idle
      </InfoTip>
    </span>
    {#if hasRecent}
      <div class="bar" role="img" aria-label="{Math.round(workedPct)}% of recent ticks carried gossip work">
        <div class="seg worked" style="width: {workedPct}%"></div>
        <div class="seg idle" style="width: {100 - workedPct}%"></div>
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

  /* Adaptive fanout block. */
  .fanout {
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
  }

  .fanout-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    font-size: var(--text-sm);
  }

  .fanout-now {
    color: var(--ink-soft);
  }

  .fanout-now.widened {
    color: var(--signal-dirty);
    font-weight: 600;
  }

  .fanout-now .unit {
    color: var(--ink-faint);
    font-weight: 400;
  }

  .meter {
    position: relative;
    display: flex;
    height: 8px;
    border-radius: 4px;
    overflow: hidden;
    background: var(--chrome-bg);
  }

  .fanout-foot {
    display: flex;
    gap: var(--space-3);
    font-size: var(--text-xs);
    color: var(--ink-faint);
  }

  .widen-note.on {
    color: var(--signal-dirty);
  }

  /* The peak high-water mark: a 2px tick standing above the fill. */
  .peak-tick {
    position: absolute;
    top: -2px;
    bottom: -2px;
    width: 2px;
    background: var(--ink-soft);
    transform: translateX(-1px);
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
  .seg.worked,
  .seg.fan-base {
    background: var(--node-fill);
  }

  .seg.threshold,
  .seg.fan-widen {
    background: var(--signal-dirty);
  }

  .seg.idle {
    background: transparent;
  }

  /* Honest empty state — reserves the row height so the layout never jumps. */
  .bar-empty {
    display: flex;
    align-items: center;
    height: 8px;
    color: var(--ink-faint);
    font-style: italic;
    font-size: var(--text-sm);
  }

  .budget {
    font-size: var(--text-xs);
    color: var(--ink-faint);
  }

  .budget-val {
    color: var(--ink-soft);
  }
</style>
