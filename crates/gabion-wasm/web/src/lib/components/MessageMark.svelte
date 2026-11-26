<script lang="ts">
  import { RING_WIDTH, SvgPen, messageRing, messageSquare } from '../glyphs';
  import type { Fate } from '../messages';
  import { CONTROL, RING, SQUARE, markFlags } from '../stage/layout';
  import type { MessageRole, Tier } from '../sim/types';

  // A message's stage mark at list scale, in the mark column: data an
  // arrowhead (across zones in ink, inside one muted), an answer a dot, an
  // ask a ring, a gap report a square. Lost: the mark, then the stop bar. In
  // flight: a dotted leader before it.
  let { role, cells, tier, fate }: { role: MessageRole; cells: number; tier: Tier; fate: Fate } = $props();

  const flags = $derived(markFlags(role, cells));
  const x = $derived(fate === 'lost' ? 6 : 12);
  const d = $derived.by(() => {
    const pen = new SvgPen();
    if ((flags & RING) !== 0) messageRing(pen, x, 0);
    else if ((flags & SQUARE) !== 0) messageSquare(pen, x, 0);
    return pen.d;
  });
</script>

<svg class="mark" width="16" height="16" viewBox="0 -8 16 16" aria-hidden="true">
  {#if fate === 'in flight'}
    <circle class="muted" cx="1.5" cy="0" r="0.9" />
    <circle class="muted" cx="4.5" cy="0" r="0.9" />
  {/if}
  {#if (flags & CONTROL) === 0}
    {#if fate === 'lost'}
      <path class="stroke" class:cross={tier === 'CrossZone'} d="M1 0L11 0" />
    {:else}
      <path class="fill" class:cross={tier === 'CrossZone'} d={`M${x + 2} 0L${x - 4} -3.5L${x - 4} 3.5Z`} />
    {/if}
  {:else if (flags & RING) !== 0}
    <path class="ring" style:stroke-width={RING_WIDTH} {d} />
  {:else if (flags & SQUARE) !== 0}
    <path class="muted" {d} />
  {:else}
    <circle class="muted" cx={x} cy="0" r="2" />
  {/if}
  {#if fate === 'lost'}
    <path class="stroke" d="M11.5 -3L11.5 3" />
  {/if}
</svg>

<style>
  .mark {
    flex: none;
    overflow: visible;
    vertical-align: middle;
  }

  path,
  circle {
    fill: none;
    stroke: none;
  }

  .muted,
  .fill {
    fill: var(--chart-muted);
  }

  .fill.cross {
    fill: var(--color-fg);
  }

  .stroke {
    stroke: var(--chart-muted);
    stroke-width: 1.5;
  }

  .stroke.cross {
    stroke: var(--color-fg);
    stroke-width: 2;
  }

  .ring {
    stroke: var(--chart-muted);
  }
</style>
