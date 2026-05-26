<script lang="ts">
  import type { CellView } from '../sim/types';

  // The sliding window made literal: one strip per active key, a conveyor belt of
  // per-bucket bars that scrolls left as virtual time advances. This is the one
  // device that shows the windowed-rate-limit mechanic — per-bucket counts, new
  // buckets forming under "now", old buckets aging off the trailing edge — and Σ
  // against the rule limit.
  //
  // Two channels, kept strictly separate so neither lies about the other:
  //   • TIME is the horizontal scroll. The whole track drifts left by the
  //     fraction of the current bucket already elapsed (`epochFraction`), so the
  //     window visibly slides; bars keep their identity (keyed by epoch) and
  //     translate, they do not rebind in place.
  //   • DATA is the bar height. A bar only grows/shrinks when its bucket's count
  //     changes — so a growing bar honestly means "this bucket is accumulating",
  //     never "the window moved".
  //
  // The columns are a FIXED pixel width (`viewportWidth / visibleCols`), so they
  // never re-distribute or stretch. The track is one column wider than the
  // viewport and `overflow:hidden` clips the emerging/expiring edges. Because the
  // epoch window and the fractional offset are both re-derived every frame, the
  // translate is always within `[-colW, 0]` — a high-speed multi-bucket leap just
  // re-anchors, it can never scroll off screen. There is no CSS transition on the
  // transform: the per-frame snapshot loop is the animation.
  let {
    cells,
    currentEpoch,
    epochFraction,
    liveBuckets,
    windowMs,
    bucketMs,
    limit,
  }: {
    cells: CellView[];
    /** The bucket epoch "now" sits in (`RuleDescriptor::current_epoch`). */
    currentEpoch: number;
    /** Fraction of the current bucket already elapsed, `[0, 1)` — the sub-bucket
     *  scroll offset. `(virtual_ms mod bucket_ms) / bucket_ms`, computed by the
     *  caller so this component needn't know the bucket size. */
    epochFraction: number;
    /** `RuleDescriptor::live_buckets()` (nominal `ceil(window / bucket)`). The
     *  window holds `liveBuckets + 1` buckets; one more emerging bucket is
     *  rendered so a fresh empty bucket can scroll in under "now". */
    liveBuckets: number;
    /** The rule window in ms — labels the trailing edge of the axis ("Ns ago"). */
    windowMs: number;
    /** The rule's bucket width in ms (`defaults.rule_bucket_ms`). Used with the
     *  unclamped `epochFraction` to count down the oldest live bucket's
     *  time-to-expiry — a number that honestly ticks toward 0 each step. */
    bucketMs: number;
    limit: number;
  } = $props();

  // The trailing edge of the visible window, in whole seconds, for the axis
  // caption. The window knob steps in seconds, so this is exact.
  const windowSeconds = $derived(Math.round(windowMs / 1000));

  // Purposeful motion only: reduced motion snaps to whole buckets (no sub-bucket
  // drift) and the height transition is dropped (gated in CSS below).
  const reduceMotion =
    typeof window !== 'undefined' && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  // Measured once layout settles; 0 on the first frame, where colW falls to 0 and
  // the track sits un-translated until the real width arrives.
  let viewportWidth = $state(0);

  // The window shows `liveBuckets + 1` buckets [oldest, currentEpoch]; the track
  // carries one more (`currentEpoch + 1`, the bucket forming just off the right
  // edge) so it has something to scroll in.
  const visibleCols = $derived(Math.max(liveBuckets + 1, 1));
  const colCount = $derived(visibleCols + 1);
  const colW = $derived(viewportWidth > 0 ? viewportWidth / visibleCols : 0);
  const oldest = $derived(currentEpoch - (visibleCols - 1));
  const fraction = $derived(reduceMotion ? 0 : Math.min(Math.max(epochFraction, 0), 1));
  const translateX = $derived(-fraction * colW);

  interface Slot {
    epoch: number;
    count: number;
  }
  interface Strip {
    key: string;
    slots: Slot[];
    sigma: number;
    /** Tallest in-window bar — the per-strip auto-scale denominator. */
    max: number;
    /** Oldest in-window bucket that still carries hits — the next to age off the
     *  trailing edge. `null` when the window holds no counts. Anchors the
     *  time-to-expiry caption and the `.expiring` bar highlight. */
    oldestActiveEpoch: number | null;
  }

  // Bin the live cells by key into the rendered epoch range [oldest, oldest +
  // colCount). Cells outside the range are dropped (not clipped to the edge), so
  // Σ — summed over the in-window buckets [oldest, currentEpoch] — equals this
  // node's windowed aggregate for the key. The emerging `currentEpoch + 1` column
  // is excluded from Σ (it is always empty in any case — no cell is dated to the
  // future).
  const strips = $derived.by((): Strip[] => {
    const byKey = new Map<string, number[]>();
    for (const cell of cells) {
      const off = cell.bucket - oldest;
      if (off < 0 || off >= colCount) continue;
      let counts = byKey.get(cell.key);
      if (counts === undefined) {
        counts = new Array<number>(colCount).fill(0);
        byKey.set(cell.key, counts);
      }
      counts[off] += cell.count;
    }
    const result: Strip[] = [];
    for (const [key, counts] of byKey) {
      let sigma = 0;
      let max = 0;
      const slots = counts.map((count, i) => {
        const epoch = oldest + i;
        if (epoch <= currentEpoch) sigma += count;
        if (count > max) max = count;
        return { epoch, count };
      });
      // Slots run oldest→newest, so the first in-window non-empty one is the
      // oldest — the bucket nearest the trailing edge, next to age out.
      const oldestActive = slots.find((s) => s.epoch <= currentEpoch && s.count > 0);
      result.push({
        key,
        slots,
        sigma,
        max,
        oldestActiveEpoch: oldestActive?.epoch ?? null,
      });
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

  /** Seconds until `epoch` ages off the trailing edge. The engine keeps a bucket
   *  while `bucket + liveBuckets >= currentEpoch`, so it leaves the window the
   *  instant the continuous current (`currentEpoch + epochFraction`) reaches
   *  `epoch + liveBuckets + 1`. Uses the raw, unclamped `epochFraction` (not the
   *  reduced-motion `fraction`) so the readout is an honest sub-second countdown
   *  rather than a per-bucket snap. */
  function tteSeconds(epoch: number): number {
    const epochsLeft = epoch + liveBuckets + 1 - currentEpoch - epochFraction;
    return Math.max(0, (epochsLeft * bucketMs) / 1000);
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

  /** A concise screen-reader summary of one strip's in-window non-empty buckets. */
  function stripSummary(strip: Strip): string {
    const active = strip.slots
      .filter((s) => s.epoch <= currentEpoch && s.count > 0)
      .map((s) => s.count);
    const tally = active.length === 0 ? 'no buckets' : `buckets ${active.join(', ')}`;
    const aging =
      strip.oldestActiveEpoch === null
        ? ''
        : `; oldest bucket ages out in ${tteSeconds(strip.oldestActiveEpoch).toFixed(1)} seconds`;
    return `Sliding window, key ${shortKey(strip.key)}: total ${strip.sigma} of ${limit}; ${tally}${aging}`;
  }
</script>

<div class="strata" bind:clientWidth={viewportWidth}>
  {#if strips.length === 0}
    <p class="no-traffic">No traffic in this node's window yet.</p>
  {:else}
    {#each strips as strip (strip.key)}
      <section class="strip" role="group" aria-label={stripSummary(strip)}>
        <div class="strip-head">
          <span class="head-left">
            <span class="key numeric">key {shortKey(strip.key)}</span>
            {#if strip.oldestActiveEpoch !== null}
              <span class="tte numeric">ages out in {tteSeconds(strip.oldestActiveEpoch).toFixed(1)}s</span>
            {/if}
          </span>
          <span class="sigma" class:over={strip.sigma >= limit}>
            Σ <span class="sigma-value numeric">{strip.sigma}</span>
            <span class="sigma-sep">/</span>
            <span class="numeric">{limit.toLocaleString()}</span>
          </span>
        </div>
        <div class="viewport" aria-hidden="true">
          {#if limitWithinRange(strip.max)}
            <div class="limit-line" style="bottom: {(limit / strip.max) * 100}%"></div>
          {/if}
          <div
            class="track"
            style="width: {colCount * colW}px; transform: translateX({translateX}px)"
          >
            {#each strip.slots as slot (slot.epoch)}
              <div class="bar-cell" style="width: {colW}px" title="bucket {slot.epoch}: {slot.count}">
                <div
                  class="bar"
                  class:empty={slot.count === 0}
                  class:expiring={strip.oldestActiveEpoch !== null &&
                    slot.epoch === strip.oldestActiveEpoch}
                  style="height: {heightPct(slot.count, strip.max)}%"
                ></div>
              </div>
            {/each}
          </div>
        </div>
      </section>
    {/each}
    <!-- One shared time axis under all strips: the window flows left, the newest
         buckets enter under "now" on the right and age out toward the trailing
         edge on the left. -->
    <div class="axis" aria-hidden="true">
      <span class="numeric">{windowSeconds}s ago</span>
      <span>now</span>
    </div>
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

  .head-left {
    display: flex;
    align-items: baseline;
    gap: var(--space-2);
    min-width: 0;
  }

  .key {
    color: var(--ink-faint);
  }

  /* The time-to-expiry countdown for the oldest live bucket: amber (the
     "transient / on the clock" hue, matching the `.expiring` bar it points at),
     tabular figures so the ticking number doesn't jitter its width. */
  .tte {
    color: var(--signal-dirty);
    font-variant-numeric: tabular-nums;
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

  /* The scroll window: a fixed-height viewport that clips the track's emerging
     (right) and expiring (left) edges. The bottom border is the baseline the bars
     rise from. */
  .viewport {
    position: relative;
    height: 96px;
    overflow: hidden;
    border-bottom: 1.5px solid var(--chrome-border);
  }

  /* The conveyor belt: a row of fixed-width columns, one wider than the viewport,
     translated horizontally by the caller's sub-bucket fraction. No transition —
     the per-frame snapshot loop drives the scroll, and a CSS transition would
     fight it and jitter. */
  .track {
    position: absolute;
    left: 0;
    bottom: 0;
    height: 100%;
    display: flex;
    align-items: flex-end;
    will-change: transform;
  }

  .bar-cell {
    flex: none;
    height: 100%;
    display: flex;
    align-items: flex-end;
    justify-content: center;
  }

  /* Slate data-ink bars (the disc colour) — clears 3:1 on the white panel as a
     graphical mark. Height encodes count; the auto-scale keeps them legible at
     any limit. */
  .bar {
    width: 70%;
    min-height: 0;
    background: var(--node-fill);
    border-radius: 3px 3px 0 0;
  }

  /* The one purposeful height motion: a bar easing as its bucket's count changes
     (the data channel). Gated so reduced motion snaps. */
  @media (prefers-reduced-motion: no-preference) {
    .bar {
      transition: height 220ms ease;
    }
  }

  /* An empty slot keeps its column (so the window's width reads) but shows no ink
     beyond the shared baseline. */
  .bar.empty {
    background: transparent;
  }

  /* The oldest live bucket — the next to scroll off the trailing edge. Amber
     marks the one bar the countdown is timing, so the otherwise-uniform fence
     of equal-height bars has a clear anchor for "this is what ages out in
     N.Ns". */
  .bar.expiring {
    background: var(--signal-dirty);
  }

  /* The shared time axis: quiet end-labels orienting the scroll. "now" on the
     right (where buckets enter), the window's trailing edge on the left (where
     they age out). Letterspaced small-caps, the panel's faintest ink. */
  .axis {
    display: flex;
    justify-content: space-between;
    margin-top: calc(-1 * var(--space-2));
    font-size: var(--text-xs);
    letter-spacing: 0.04em;
    color: var(--ink-faint);
  }

  /* The rule limit as a dashed threshold across the viewport — fixed (it does not
     scroll with the track), drawn only when it sits within the auto-scaled range. */
  .limit-line {
    position: absolute;
    left: 0;
    right: 0;
    border-top: 1.5px dashed var(--signal-reject);
    pointer-events: none;
    z-index: 1;
  }
</style>
