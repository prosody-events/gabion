<script lang="ts">
  import type { Snippet } from 'svelte';

  // The one tooltip primitive, reused by every inspector section. Native `title`
  // is mouse-only and gives keyboard users no visible cue; this shows the
  // explanation on **hover and focus** (so a keyboard tab reveals it too) and
  // links it to the trigger with `aria-describedby` so assistive tech announces
  // it. The trigger wears the conventional help affordance: a dotted underline
  // and `cursor: help`.
  let { text, children }: { text: string; children: Snippet } = $props();

  // Per-instance id tying the trigger to its description. Generated once at
  // construction — stable for the component's life.
  const id = `tip-${Math.random().toString(36).slice(2, 9)}`;
</script>

<button class="term" type="button" aria-describedby={id}>
  {@render children()}
  <span class="bubble" role="tooltip" {id}>{text}</span>
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
    border-bottom: 1px dotted var(--color-fg-muted);
    background: none;
    font: inherit;
    color: inherit;
    text-align: inherit;
    cursor: help;
  }

  .bubble {
    position: absolute;
    left: 0;
    top: calc(100% + var(--gap-xs));
    z-index: 10;
    width: max-content;
    max-width: 15rem;
    padding: var(--gap-sm);
    border-radius: var(--radius);
    background: var(--surface-popover);
    color: var(--color-fg);
    border: 1px solid var(--color-border-line);
    font-size: var(--font-size-caption-sm);
    font-weight: 400;
    line-height: 1.4;
    letter-spacing: 0;
    text-transform: none;
    box-shadow: var(--shadow-popover);
    /* Hidden but kept in the DOM so `aria-describedby` can still reach it,
       and out of the layout, so a term near an edge never widens the page. */
    display: none;
  }

  .term:hover .bubble,
  .term:focus-visible .bubble {
    display: block;
  }
</style>
