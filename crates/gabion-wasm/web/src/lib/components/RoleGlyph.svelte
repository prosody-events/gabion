<script lang="ts">
  import { brim, chord, dashedRing, port, ring, SvgPen } from '../glyphs';
  import { PREVIEW_DASH } from '../stage/layout';

  // A node mark as the stage draws it, for the DOM: the key, the node table,
  // Gossip targets and the glossary. The same geometry (`lib/glyphs.ts`) at
  // 16 × 16 px, in the stage's colour roles.
  export type Glyph =
    | 'vessel'
    | 'far'
    | 'cap'
    | 'ring'
    | 'selected'
    | 'preview'
    | 'port'
    | 'suspect'
    | 'rejected'
    | 'lost'
    | 'ack';

  let { kind }: { kind: Glyph } = $props();

  const R = 5;
  const BRIM = R - 0.8 * 2 * R;
  const path = (draw: (pen: SvgPen) => void): string => {
    const pen = new SvgPen();
    draw(pen);
    return pen.d;
  };
  const disc = path((p) => ring(p, R, 0, 2 * Math.PI));
  const outer = path((p) => ring(p, R + 2.5, 0, 2 * Math.PI));
  const level = path((p) => chord(p, R, 1, R));
  const full = path((p) => chord(p, R, BRIM, R));
  const brimLine = path((p) => brim(p, R, BRIM, 1));
  const band = path((p) => chord(p, R, -R, BRIM));
  const arc = path((p) => ring(p, R + 2.5, -Math.PI / 2, Math.PI));
  const selection = path((p) => ring(p, R + 2.5, 0, 2 * Math.PI));
  const preview = path((p) => dashedRing(p, R + 2.5, [PREVIEW_DASH[0] / 2, PREVIEW_DASH[1] / 2]));
  const diamond = path((p) => port(p, 0, 0, 4.5, Math.PI / 2));
</script>

<svg class="glyph" width="16" height="16" viewBox="-8 -8 16 16" aria-hidden="true" data-glyph={kind}>
  {#if kind === 'vessel' || kind === 'cap' || kind === 'far' || kind === 'rejected'}
    <path class="axis-stroke" d={outer} />
    <path class="panel" d={disc} />
    {#if kind === 'vessel'}
      <path class="level" d={level} />
      <path class="ink-stroke" d={brimLine} />
    {:else if kind === 'cap'}
      <path class="level" d={full} />
      <path class="bad-stroke thick" d={brimLine} />
    {:else if kind === 'rejected'}
      <path class="level" d={full} />
      <path class="bad" d={band} />
      <path class="ink-stroke" d={brimLine} />
    {/if}
  {:else if kind === 'ring'}
    <path class="warn-stroke thin" d={outer} />
    <path class="warn-stroke" d={arc} />
  {:else if kind === 'selected'}
    <path class="panel axis-thin" d={disc} />
    <path class="ink-stroke" d={selection} />
  {:else if kind === 'preview'}
    <path class="panel axis-thin" d={disc} />
    <path class="muted-stroke" d={preview} />
  {:else if kind === 'port'}
    <path class="ink" d={diamond} />
  {:else if kind === 'suspect'}
    <path class="hollow" d={diamond} />
  {:else if kind === 'lost'}
    <path class="muted-stroke" d="M-7 0L2 0M2 -5L2 5" />
  {:else if kind === 'ack'}
    <circle class="muted" r="2" />
  {/if}
</svg>

<style>
  .glyph {
    flex: none;
    overflow: visible;
    vertical-align: -0.2em;
  }

  path,
  circle {
    fill: none;
    stroke: none;
  }

  .panel {
    fill: var(--color-panel);
  }

  .level {
    fill: var(--color-level);
  }

  .ink {
    fill: var(--color-fg);
  }

  .bad {
    fill: var(--color-bad);
  }

  .muted {
    fill: var(--chart-muted);
  }

  .hollow {
    fill: var(--color-bg);
    stroke: var(--color-fg);
    stroke-width: 1.5;
  }

  .axis-stroke {
    stroke: var(--chart-axis);
    stroke-width: 2;
  }

  .axis-thin {
    stroke: var(--chart-axis);
    stroke-width: 1;
  }

  .ink-stroke {
    stroke: var(--color-fg);
    stroke-width: 1.5;
  }

  .muted-stroke {
    stroke: var(--chart-muted);
    stroke-width: 1.5;
  }

  .bad-stroke {
    stroke: var(--color-bad);
  }

  .thick {
    stroke-width: 2.5;
  }

  .warn-stroke {
    stroke: var(--color-warn);
    stroke-width: 2;
  }

  .thin {
    stroke-width: 1;
  }
</style>
