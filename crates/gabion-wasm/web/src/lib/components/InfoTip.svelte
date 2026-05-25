<script lang="ts">
  import type { Snippet } from 'svelte';

  // The one tooltip primitive, reused by every inspector section. Native `title`
  // is mouse-only and gives keyboard users no visible cue; this shows the
  // explanation on **hover and focus** (so a keyboard tab reveals it too) and
  // links it to the trigger with `aria-describedby` so assistive tech announces
  // it. The trigger wears the conventional help affordance: a dotted underline
  // and `cursor: help`.
  // `align` decides which edge the bubble grows from: left by default (grows
  // rightward, for terms in the left of the panel), or right for a term near the
  // panel's right edge so the bubble grows inward and isn't clipped by the
  // inspector's `overflow` box.
  let {
    text,
    align = 'left',
    children,
  }: { text: string; align?: 'left' | 'right'; children: Snippet } = $props();

  // Per-instance id tying the trigger to its description. Generated once at
  // construction — stable for the component's life.
  const id = `tip-${Math.random().toString(36).slice(2, 9)}`;
</script>

<button class="term" type="button" aria-describedby={id}>
  {@render children()}
  <span class="bubble" class:right={align === 'right'} role="tooltip" {id}>{text}</span>
</button>

<style>
  /* A button (natively focusable, so hover *and* keyboard focus reveal the tip)
     stripped of its chrome to read as an inline help term. */
  .term {
    position: relative;
    display: inline;
    margin: 0;
    padding: 0;
    border: none;
    border-bottom: 1px dotted var(--ink-faint);
    background: none;
    font: inherit;
    color: inherit;
    text-align: inherit;
    cursor: help;
  }

  .bubble {
    position: absolute;
    left: 0;
    top: calc(100% + var(--space-1));
    z-index: 10;
    width: max-content;
    max-width: 15rem;
    padding: var(--space-2);
    border-radius: var(--radius);
    background: var(--ink);
    color: var(--on-disc);
    font-size: var(--text-xs);
    font-weight: 400;
    line-height: 1.4;
    letter-spacing: 0;
    text-transform: none;
    box-shadow: 0 2px 8px rgb(27 35 48 / 18%);
    /* Hidden but kept in the DOM so `aria-describedby` can still reach it. */
    opacity: 0;
    visibility: hidden;
    transition: opacity 120ms ease;
  }

  /* Grow inward from the right edge — keeps a right-side term's bubble inside
     the inspector's overflow box. */
  .bubble.right {
    left: auto;
    right: 0;
  }

  .term:hover .bubble,
  .term:focus-visible .bubble {
    opacity: 1;
    visibility: visible;
  }

  /* Reduced motion: no fade, just appear. */
  @media (prefers-reduced-motion: reduce) {
    .bubble {
      transition: none;
    }
  }
</style>
