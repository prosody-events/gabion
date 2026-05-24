<script lang="ts">
  import { onMount } from 'svelte';
  import type { ClusterState, SimEvent } from '../sim/types';
  import { StageRenderer } from '../stage/renderer';
  import {
    DOT_THRESHOLD,
    fitTransform,
    nodePosition,
    nodeRadius,
    toScreen,
    type StageTransform,
  } from '../stage/layout';

  // The PixiJS stage. The WebGL canvas (managed imperatively by `StageRenderer`)
  // draws the discs, cell arcs, light-beam packets, and convergence pulse;
  // `cluster` feeds steady per-node state and `events` feeds the transient
  // gossip packets from each step. A DOM overlay sits on top of the canvas with
  // the per-node index and total — the canvas is opaque to assistive tech and
  // to test tooling, so the real numbers live in queryable, tabular-figure text.
  let { cluster, events }: { cluster: ClusterState | null; events: SimEvent[] } = $props();

  let container: HTMLDivElement;
  let renderer: StageRenderer | null = null;
  let ready = $state(false);
  let transform: StageTransform = $state(fitTransform(0, 0));

  const nodes = $derived(cluster?.nodes ?? []);
  const count = $derived(nodes.length);
  // Labels track the canvas: their size scales with the disc, and they vanish
  // once the ring is too dense to label (matching the renderer's dot mode).
  const labelFont = $derived(nodeRadius(count) * transform.scale * 0.62);
  // Hold the overlay until the stage has a real fitted size (scale > 0),
  // otherwise the first frame stacks every label at the origin.
  const showLabels = $derived(count > 0 && count <= DOT_THRESHOLD && transform.scale > 0);
  const summary = $derived(
    cluster === null
      ? 'Gossip cluster: loading.'
      : `Gossip cluster of ${count} nodes. True total ${cluster.oracle_total}. ` +
        `Per-node totals: ${nodes.map((n) => n.aggregate_total).join(', ')}.`,
  );

  $effect(() => {
    if (ready && renderer !== null) renderer.setCluster(cluster);
  });

  $effect(() => {
    if (ready && renderer !== null && events.length > 0) renderer.applyEvents(events);
  });

  onMount(() => {
    let disposed = false;
    let observer: ResizeObserver | null = null;

    void (async () => {
      const r = await StageRenderer.create(container);
      if (disposed) {
        r.destroy();
        return;
      }
      renderer = r;
      const apply = (): void => {
        const rect = container.getBoundingClientRect();
        transform = r.resize(rect.width, rect.height);
      };
      apply();
      observer = new ResizeObserver(apply);
      observer.observe(container);
      if (cluster !== null) r.setCluster(cluster);
      ready = true;
    })();

    return () => {
      disposed = true;
      observer?.disconnect();
      renderer?.destroy();
      renderer = null;
      ready = false;
    };
  });
</script>

<div class="stage" bind:this={container} role="img" aria-label={summary}>
  {#if showLabels}
    <div class="stage-labels" aria-hidden="true">
      {#each nodes as node (node.index)}
        {@const s = toScreen(nodePosition(node.index, count), transform)}
        <span class="node-label" style="left: {s.x}px; top: {s.y}px; font-size: {labelFont}px;">
          <span class="node-index numeric">{node.index}</span>
          <span class="node-count numeric">{node.aggregate_total}</span>
        </span>
      {/each}
    </div>
  {/if}
</div>

<style>
  .stage {
    position: relative;
    width: 100%;
    height: 100%;
    overflow: hidden;
    background: var(--stage-bg);
  }

  /* The overlay must never intercept pointer events — Phase 6 hit-tests the
     canvas underneath for click-a-node. */
  .stage-labels {
    position: absolute;
    inset: 0;
    pointer-events: none;
  }

  .node-label {
    position: absolute;
    transform: translate(-50%, -50%);
    display: flex;
    flex-direction: column;
    align-items: center;
    line-height: 1;
  }

  .node-index {
    color: var(--on-stage-soft);
    font-size: 0.7em;
    margin-bottom: 0.15em;
  }

  /* Dark ink reads against the light disc fill the node is drawn with. */
  .node-count {
    color: var(--stage-bg);
    font-weight: 650;
  }
</style>
