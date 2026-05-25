<script lang="ts">
  import type { StoreStats } from '../sim/types';
  import InfoTip from './InfoTip.svelte';

  // §6 of the node inspector: how full this node's CRDT store is, and whether it
  // has ever had to drop work for want of room. Occupancy is three small
  // multiples on one shared 0–100% scale (fill ∝ used/capacity) so they compare
  // like-for-like; the differing absolute ceilings are carried by the numerals,
  // not the bar lengths. Saturation is a single calm "holding" badge until a
  // counter actually moves — then it escalates, in words and colour, not colour
  // alone.
  let { stats }: { stats: StoreStats } = $props();

  interface Gauge {
    label: string;
    tip: string;
    used: number;
    capacity: number;
  }

  const gauges = $derived<Gauge[]>([
    {
      label: 'Cells',
      tip: 'Active CRDT cells held in this node’s store — one per (rule, key, bucket) it tracks.',
      used: stats.active_cells,
      capacity: stats.cell_capacity,
    },
    {
      label: 'Rule slots',
      tip: 'Distinct rules interned in the rule dictionary. Bounded; a new rule is rejected once full.',
      used: stats.rule_slots_used,
      capacity: stats.rule_slots_capacity,
    },
    {
      label: 'Node slots',
      tip: 'Distinct origin node identities interned in the node dictionary — every cluster member this node has cells from.',
      used: stats.node_slots_used,
      capacity: stats.node_slots_capacity,
    },
  ]);

  /** Occupancy as a percentage, guarding a zero/unknown capacity. */
  function pct(used: number, capacity: number): number {
    return capacity > 0 ? Math.min((used / capacity) * 100, 100) : 0;
  }

  /** Escalating tone on the shared scale: slate < 75% ≤ amber < 95% ≤ red. */
  function tone(p: number): 'ok' | 'warn' | 'full' {
    return p >= 95 ? 'full' : p >= 75 ? 'warn' : 'ok';
  }

  function valueText(g: Gauge): string {
    const p = Math.round(pct(g.used, g.capacity));
    const t = tone(p);
    const suffix = t === 'full' ? ', nearly full' : t === 'warn' ? ', filling' : '';
    return `${g.used} of ${g.capacity} (${p}%)${suffix}`;
  }

  interface Saturation {
    label: string;
    detail: string;
    count: number;
  }

  const saturation = $derived<Saturation[]>([
    {
      label: 'Cell store',
      detail: 'cell store full',
      count: stats.cell_store_full_rejects,
    },
    {
      label: 'Rule dictionary',
      detail: 'rule dictionary full',
      count: stats.rule_dictionary_full_rejects,
    },
    {
      label: 'Node dictionary',
      detail: 'node dictionary full',
      count: stats.node_dictionary_full_rejects,
    },
  ]);

  const anyDropped = $derived(saturation.some((s) => s.count > 0));
</script>

<div class="storage">
  <div class="gauges">
    {#each gauges as g (g.label)}
      {@const p = pct(g.used, g.capacity)}
      <div class="gauge">
        <span class="gauge-label">
          <InfoTip text={g.tip}>{g.label}</InfoTip>
        </span>
        <div
          class="track"
          role="meter"
          aria-label={g.label}
          aria-valuemin={0}
          aria-valuemax={g.capacity}
          aria-valuenow={g.used}
          aria-valuetext={valueText(g)}
        >
          <div class="fill {tone(p)}" style="width: {p}%"></div>
        </div>
        <span class="gauge-value numeric">{g.used}<span class="sep"> / </span>{g.capacity}</span>
      </div>
    {/each}
  </div>

  {#if anyDropped}
    <ul class="drops" aria-live="polite">
      {#each saturation as s (s.label)}
        {#if s.count > 0}
          <li>
            <span class="warn-glyph" aria-hidden="true">▲</span>
            {s.label} dropped <span class="numeric">{s.count.toLocaleString()}</span>
            <span class="drops-why">({s.detail})</span>
          </li>
        {/if}
      {/each}
    </ul>
  {:else}
    <p class="holding" role="status">
      <span class="ok-glyph" aria-hidden="true">✓</span>
      All capacities holding · <span class="numeric">0</span> rejects
    </p>
  {/if}
</div>

<style>
  .storage {
    display: flex;
    flex-direction: column;
    gap: var(--space-2);
  }

  .gauges {
    display: flex;
    flex-direction: column;
    gap: var(--space-2);
  }

  /* Label · bar · numerals on one baseline; the bar takes the slack so all three
     gauges share one scale and align. */
  .gauge {
    display: grid;
    grid-template-columns: 5.5rem 1fr auto;
    align-items: center;
    gap: var(--space-2);
    font-size: var(--text-sm);
  }

  .gauge-label {
    color: var(--ink-soft);
  }

  .track {
    height: 8px;
    border-radius: 4px;
    background: var(--chrome-bg);
    overflow: hidden;
  }

  .fill {
    height: 100%;
    border-radius: 4px;
    transition: width 220ms ease;
  }

  .fill.ok {
    background: var(--node-fill);
  }

  .fill.warn {
    background: var(--signal-dirty);
  }

  .fill.full {
    background: var(--signal-reject);
  }

  @media (prefers-reduced-motion: reduce) {
    .fill {
      transition: none;
    }
  }

  .gauge-value {
    color: var(--ink);
    font-variant-numeric: tabular-nums lining-nums;
  }

  .gauge-value .sep {
    color: var(--ink-faint);
  }

  /* Calm zero state — capacity is a non-event until it isn't. Mirrors the
     headline's converged badge. */
  .holding {
    margin: 0;
    font-size: var(--text-sm);
    color: var(--signal-converged);
  }

  .ok-glyph {
    font-weight: 700;
  }

  /* Escalated state: per-counter lines, word + glyph + colour (never colour
     alone), only for counters that actually moved. */
  .drops {
    margin: 0;
    padding: 0;
    list-style: none;
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
    font-size: var(--text-sm);
    color: var(--signal-reject);
  }

  .warn-glyph {
    margin-right: var(--space-1);
  }

  .drops-why {
    color: var(--ink-soft);
  }
</style>
