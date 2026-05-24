<script lang="ts">
  import { onMount } from 'svelte';
  import type { ClusterState, SimEvent } from '../sim/types';
  import { StageRenderer } from '../stage/renderer';
  import {
    DOT_THRESHOLD,
    fitTransform,
    nodeAt,
    nodePosition,
    nodeRadius,
    toScreen,
    type StageTransform,
  } from '../stage/layout';

  // The PixiJS stage. The WebGL canvas (managed imperatively by `StageRenderer`)
  // draws the discs, cell arcs, light-beam packets, and convergence pulse;
  // `cluster` feeds steady per-node state and `events` feeds the transient
  // gossip packets from each step. A DOM overlay sits on top of the canvas with
  // each node's stable id and total — the canvas is opaque to assistive tech and
  // to test tooling, so the real numbers live in queryable, tabular-figure text.
  //
  // `onSendBurst` is the click-a-node affordance: clicking a disc injects a
  // burst at that node. `onDeleteNode` is the per-node "×" on its label, which
  // removes that node live. Both are pointer-only power gestures (the canvas is
  // one opaque image to assistive tech); the keyboard/AT-accessible equivalents
  // are the explicit "send to node N" and "remove node N" controls in the rail.
  // The "×" lives inside the aria-hidden label overlay and is `tabindex=-1`, so
  // it is not a focusable element hidden from assistive tech — the rail owns
  // that path.
  let {
    cluster,
    events,
    onSendBurst,
    onDeleteNode,
  }: {
    cluster: ClusterState | null;
    events: SimEvent[];
    onSendBurst?: (node: number) => void;
    onDeleteNode?: (id: number) => void;
  } = $props();

  let container: HTMLDivElement;
  let renderer: StageRenderer | null = null;
  let ready = $state(false);
  let transform: StageTransform = $state(fitTransform(0, 0));
  // The node the pointer is currently over, or null — drives the cursor
  // affordance so the ring reads as clickable only where a click would land.
  let hoverNode: number | null = $state(null);

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

  /** Resolve a pointer event to the node under it (or null), in the container's
   *  own pixel space. The label overlay is `pointer-events: none`, so a pointer
   *  over a disc reaches this handler whether it is over the canvas or a label. */
  function nodeUnder(event: PointerEvent): number | null {
    const rect = container.getBoundingClientRect();
    return nodeAt({ x: event.clientX - rect.left, y: event.clientY - rect.top }, nodes, transform);
  }

  function onPointerMove(event: PointerEvent): void {
    hoverNode = nodeUnder(event);
  }

  function onPointerLeave(): void {
    hoverNode = null;
  }

  function onPointerDown(event: PointerEvent): void {
    const node = nodeUnder(event);
    if (node !== null) onSendBurst?.(node);
  }

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

<div
  class="stage"
  class:actionable={hoverNode !== null}
  bind:this={container}
  role="img"
  aria-label={summary}
  onpointerdown={onPointerDown}
  onpointermove={onPointerMove}
  onpointerleave={onPointerLeave}
>
  {#if showLabels}
    <div class="stage-labels" aria-hidden="true">
      {#each nodes as node, rank (node.id)}
        {@const s = toScreen(nodePosition(rank, count), transform)}
        <span
          class="node-label"
          data-id={node.id}
          style="left: {s.x}px; top: {s.y}px; font-size: {labelFont}px;"
        >
          <span class="node-id numeric">{node.id}</span>
          <span class="node-count numeric">{node.aggregate_total}</span>
          {#if onDeleteNode}
            <button
              class="node-delete"
              type="button"
              tabindex="-1"
              aria-hidden="true"
              title={`Remove node ${node.id}`}
              onpointerdown={(event) => event.stopPropagation()}
              onclick={(event) => {
                event.stopPropagation();
                onDeleteNode?.(node.id);
              }}>×</button
            >
          {/if}
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

  /* The ring reads as interactive only over a disc — the cursor is the
     signifier that a click here will land on a node. */
  .stage.actionable {
    cursor: pointer;
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

  /* When membership changes the ring re-spaces: glide each label's screen
     address (and its size) in step with the canvas disc's GSAP glide, so the
     number rides along with its node rather than snapping ahead of it. Only
     fires when left/top/font-size actually change — steady play leaves them
     untouched — and is dropped entirely under reduced motion. */
  @media (prefers-reduced-motion: no-preference) {
    .node-label {
      transition:
        left 520ms ease-in-out,
        top 520ms ease-in-out,
        font-size 520ms ease-in-out;
    }
  }

  /* The per-node "×": a pointer-only affordance that removes this node live.
     `pointer-events: auto` lifts it out of the pass-through overlay so it can
     be clicked, while the rest of the label stays click-through for the burst
     hit-test. A small white chip with a hairline border so it reads on the
     light stage; subtle until hovered, so twelve of them don't clutter it. */
  .node-delete {
    pointer-events: auto;
    position: absolute;
    top: -0.85em;
    right: -1em;
    display: flex;
    align-items: center;
    justify-content: center;
    width: 1.15em;
    height: 1.15em;
    padding: 0;
    border: 1px solid var(--chrome-border);
    border-radius: 50%;
    background: var(--chrome-panel);
    color: var(--ink-faint);
    font-size: 0.8em;
    line-height: 1;
    cursor: pointer;
    opacity: 0.55;
    transition:
      opacity 120ms ease,
      color 120ms ease,
      border-color 120ms ease;
  }

  .node-delete:hover,
  .node-delete:focus-visible {
    opacity: 1;
    color: var(--signal-reject);
    border-color: var(--signal-reject);
  }

  .node-id {
    color: var(--on-disc-soft);
    font-size: 0.7em;
    margin-bottom: 0.15em;
  }

  /* Light text reads against the dark slate disc the node is drawn with. */
  .node-count {
    color: var(--on-disc);
    font-weight: 650;
  }
</style>
