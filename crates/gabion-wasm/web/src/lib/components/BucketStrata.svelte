<script lang="ts">
  import { flip } from 'svelte/animate';
  import { fade, scale } from 'svelte/transition';
  import type { CellView } from '../sim/types';

  // The sliding window made literal: one strip per active key, each a row of
  // per-bucket bars oldest → newest, with a windowed Σ against the rule limit.
  // This is the one device that shows the windowed-rate-limit mechanic — the
  // individual bucket counts, and buckets being created / aging out.
  //
  // Source of truth is the CRDT: `cells` are the selected node's live cells, and
  // the window's edges (`currentEpoch`, `oldestEpoch`) are reported straight off
  // the snapshot by gabion's `RuleDescriptor` helpers — no window math is redone
  // here. The slide is driven by `oldestEpoch` advancing as virtual time crosses
  // bucket boundaries; keying each bar by its absolute epoch makes the
  // new-bucket enter, the oldest-bucket age-out, and the surviving bars' leftward
  // slide fall out of Svelte transitions for free.
  let {
    cells,
    currentEpoch,
    oldestEpoch,
    limit,
  }: {
    cells: CellView[];
    currentEpoch: number;
    oldestEpoch: number;
    limit: number;
  } = $props();

  // Purposeful motion only: under reduced motion every transition snaps (0 ms).
  const reduceMotion =
    typeof window !== 'undefined' && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  const motionMs = reduceMotion ? 0 : 260;

  interface Slot {
    epoch: number;
    count: number;
  }
  interface Strip {
    key: string;
    slots: Slot[];
    sigma: number;
    /** Tallest bar — the per-strip auto-scale denominator. */
    max: number;
  }

  // Group the live cells by key and bin them into the window's bucket slots,
  // summing counts that share a slot (different origins, or — defensively — any
  // epoch the engine kept just outside the nominal range). Σ is the sum of the
  // live slots, so it equals this node's aggregate total for the key.
  const strips = $derived.by((): Strip[] => {
    // Bars span the CRDT-reported live window [oldestEpoch, currentEpoch]; a
    // cell in epoch `e` bins to slot `e - oldestEpoch`. (`Math.max(…, 1)` guards
    // the degenerate equal-epoch case so we always draw at least one slot.)
    const slotCount = Math.max(currentEpoch - oldestEpoch + 1, 1);
    const byKey = new Map<string, number[]>();
    for (const cell of cells) {
      let counts = byKey.get(cell.key);
      if (counts === undefined) {
        counts = new Array<number>(slotCount).fill(0);
        byKey.set(cell.key, counts);
      }
      let slot = cell.bucket - oldestEpoch;
      if (slot < 0) slot = 0;
      else if (slot >= slotCount) slot = slotCount - 1;
      counts[slot] += cell.count;
    }
    const result: Strip[] = [];
    for (const [key, counts] of byKey) {
      let sigma = 0;
      let max = 0;
      const slots = counts.map((count, i) => {
        sigma += count;
        if (count > max) max = count;
        return { epoch: oldestEpoch + i, count };
      });
      result.push({ key, slots, sigma, max });
    }
    // Stable order so strips don't reshuffle as keys come and go.
    result.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
    return result;
  });

  /** Bar height as a percent of the strip's tallest bar (auto-scaled). */
  function heightPct(count: number, max: number): number {
    if (max <= 0 || count <= 0) return 0;
    return (count / max) * 100;
  }

  /** Whether the limit threshold falls within a strip's auto-scaled range —
   *  only then is the dashed line drawn (else it would point off-canvas and the
   *  Σ / limit readout carries the cap story alone). */
  function limitWithinRange(max: number): boolean {
    return limit > 0 && limit <= max;
  }

  /** Shorten a long hex key for the strip label; short keys pass through. */
  function shortKey(key: string): string {
    return key.length > 12 ? `${key.slice(0, 6)}…${key.slice(-4)}` : key;
  }

  /** A concise screen-reader summary of one strip's non-empty buckets. */
  function stripSummary(strip: Strip): string {
    const active = strip.slots.filter((s) => s.count > 0).map((s) => s.count);
    const tally = active.length === 0 ? 'no buckets' : `buckets ${active.join(', ')}`;
    return `Sliding window, key ${shortKey(strip.key)}: total ${strip.sigma} of ${limit}; ${tally}`;
  }
</script>

<div class="strata">
  {#if strips.length === 0}
    <p class="no-traffic">No traffic in this node's window yet.</p>
  {:else}
    {#each strips as strip (strip.key)}
      <section class="strip" role="group" aria-label={stripSummary(strip)}>
        <div class="strip-head">
          <span class="key numeric">key {shortKey(strip.key)}</span>
          <span class="sigma" class:over={strip.sigma >= limit}>
            Σ <span class="sigma-value numeric">{strip.sigma}</span>
            <span class="sigma-sep">/</span>
            <span class="numeric">{limit.toLocaleString()}</span>
          </span>
        </div>
        <div class="bars" aria-hidden="true">
          {#if limitWithinRange(strip.max)}
            <div
              class="limit-line"
              style="bottom: {(limit / strip.max) * 100}%"
              title="limit {limit}"
            ></div>
          {/if}
          {#each strip.slots as slot (slot.epoch)}
            <div class="bar-cell" animate:flip={{ duration: motionMs }}>
              <div
                class="bar"
                class:empty={slot.count === 0}
                style="height: {heightPct(slot.count, strip.max)}%"
                in:scale={{ duration: motionMs, start: 0.6 }}
                out:fade={{ duration: motionMs }}
              >
                {#if slot.count > 0}
                  <span class="bar-count numeric">{slot.count}</span>
                {/if}
              </div>
            </div>
          {/each}
        </div>
      </section>
    {/each}
  {/if}
</div>

<style>
  .strata {
    display: flex;
    flex-direction: column;
    gap: var(--space-3);
  }

  .no-traffic {
    margin: var(--space-2) 0 0;
    font-size: var(--text-sm);
    color: var(--ink-faint);
    font-style: italic;
  }

  .strip {
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
  }

  .strip-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    font-size: var(--text-sm);
  }

  .key {
    color: var(--ink-faint);
  }

  .sigma {
    color: var(--ink-soft);
  }

  .sigma-value {
    font-weight: 650;
    color: var(--ink);
  }

  .sigma-sep {
    color: var(--ink-faint);
  }

  /* Over the cap: the Σ readout turns red — the honest "this window is rejecting"
     signal, shown at any limit (the per-bar line may be off the auto-scaled
     range, but the readout always tells the truth). Paired with the word "Σ" and
     position, never color alone. */
  .sigma.over .sigma-value {
    color: var(--signal-reject);
  }

  /* The bar area: a baseline the bars rise from, with headroom on top for the
     count labels that ride each bar's crown. */
  .bars {
    position: relative;
    display: flex;
    align-items: flex-end;
    gap: 2px;
    height: 96px;
    padding-top: 16px;
    border-bottom: 1.5px solid var(--chrome-border);
  }

  .bar-cell {
    flex: 1;
    height: 100%;
    display: flex;
    align-items: flex-end;
    justify-content: center;
  }

  /* Slate data-ink bars (the disc colour) — clears 3:1 on the white panel as a
     graphical mark. Height encodes count; the auto-scale keeps them legible at
     any limit. */
  .bar {
    position: relative;
    width: 72%;
    min-height: 0;
    background: var(--node-fill);
    border-radius: 3px 3px 0 0;
  }

  /* Smooth growth when a bucket fills; gated so reduced motion snaps. */
  @media (prefers-reduced-motion: no-preference) {
    .bar {
      transition: height 260ms ease;
    }
  }

  /* An empty slot keeps its column (so the window's width reads) but shows no
     ink beyond the shared baseline. */
  .bar.empty {
    background: transparent;
  }

  .bar-count {
    position: absolute;
    bottom: 100%;
    left: 50%;
    transform: translateX(-50%);
    margin-bottom: 2px;
    font-size: var(--text-xs);
    color: var(--ink-soft);
    white-space: nowrap;
  }

  /* The rule limit as a dashed threshold across the strip — drawn only when it
     sits within the auto-scaled range. */
  .limit-line {
    position: absolute;
    left: 0;
    right: 0;
    border-top: 1.5px dashed var(--signal-reject);
    pointer-events: none;
  }
</style>
