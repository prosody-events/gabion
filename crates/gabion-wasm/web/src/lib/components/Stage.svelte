<script lang="ts">
  import type { ClusterState } from '../sim/types';

  let { state }: { state: ClusterState | null } = $props();

  // A fixed, deterministically-ordered ring: node `i` always sits at the same
  // screen address so the eye learns it (and so a shared URL renders identically
  // — no re-solving force layout). Light-beam packets and per-node cell glyphs
  // layer onto this in Phase 4; for now each node shows the cluster-aggregate
  // total it currently believes.
  const VIEW = 1000;
  const CENTER = VIEW / 2;
  const RING_RADIUS = 370;

  const nodes = $derived(state?.nodes ?? []);
  const count = $derived(nodes.length);
  const nodeRadius = $derived(Math.max(9, Math.min(38, 1100 / Math.max(count, 1))));
  const labelSize = $derived(Math.max(9, nodeRadius * 0.7));

  function position(i: number, n: number): { x: number; y: number } {
    // Start at 12 o'clock, go clockwise.
    const angle = (2 * Math.PI * i) / n - Math.PI / 2;
    return {
      x: CENTER + RING_RADIUS * Math.cos(angle),
      y: CENTER + RING_RADIUS * Math.sin(angle),
    };
  }
</script>

<svg
  class="stage"
  viewBox="0 0 {VIEW} {VIEW}"
  preserveAspectRatio="xMidYMid meet"
  role="img"
  aria-label="Gossip cluster of {count} nodes arranged in a ring"
>
  <!-- Faint guide ring the nodes sit on. -->
  <circle cx={CENTER} cy={CENTER} r={RING_RADIUS} class="guide" />

  {#each nodes as node (node.index)}
    {@const p = position(node.index, count)}
    <g class="node" transform="translate({p.x} {p.y})">
      <circle r={nodeRadius} class="node-disc" />
      <text class="node-index numeric" y={-nodeRadius - 6} font-size={labelSize}>
        {node.index}
      </text>
      <text class="node-count numeric" dy="0.34em" font-size={labelSize}>
        {node.aggregate_total}
      </text>
    </g>
  {/each}
</svg>

<style>
  .stage {
    width: 100%;
    height: 100%;
    display: block;
    background: var(--stage-bg);
  }

  .guide {
    fill: none;
    stroke: var(--stage-grid);
    stroke-width: 1.5;
  }

  .node-disc {
    fill: var(--node-fill);
    stroke: var(--node-stroke);
    stroke-width: 2;
  }

  .node-index {
    fill: var(--on-stage-soft);
    text-anchor: middle;
  }

  .node-count {
    fill: var(--stage-bg);
    text-anchor: middle;
    font-weight: 600;
  }
</style>
