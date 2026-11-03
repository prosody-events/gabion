<script lang="ts">
  import { onMount, untrack } from 'svelte';
  import { blurb, zoneLabel, type Blurb } from '../format';
  import type { ClusterState, SimEvent } from '../sim/types';
  import { StageRenderer, labelExtent, type Roles } from '../stage/renderer';
  import {
    CROSS_WIDTH,
    IN_WIDTH,
    LABEL_GAP,
    R_MAX,
    nodeAt,
    stageLayout,
    type Point,
    type StageLayout,
  } from '../stage/layout';
  import type { Theme } from '../theme';
  import NodeGrid from './NodeGrid.svelte';

  // The tier view. The WebGL canvas (managed imperatively by `StageRenderer`)
  // draws one ring per zone, the nodes with their cell arcs and role glyphs, and
  // the packets in flight; `cluster` feeds steady per-node state and `events`
  // feeds the transient packets from each step. `NodeGrid` lays the same nodes
  // over the canvas as focusable cells with a tooltip, and a table, because the
  // canvas is opaque to assistive tech and to test tooling.
  //
  // Clicking or tapping a disc selects that node and shows its tooltip; clicking
  // the bare stage deselects. The key and the one-line annotation sit under the
  // canvas, outside it, so nothing overlaps the drawing.
  let {
    cluster,
    events,
    roles,
    hoveredId,
    focusedId,
    theme,
    onSelect,
    onDeselect,
    onHover,
    onFocus,
  }: {
    cluster: ClusterState | null;
    events: SimEvent[];
    /** The selected origin's marks, or `null` with no selection. */
    roles: Roles | null;
    hoveredId: number | null;
    focusedId: number | null;
    theme: Theme;
    onSelect: (node: number) => void;
    onDeselect: () => void;
    onHover: (node: number | null) => void;
    onFocus: (node: number | null) => void;
  } = $props();

  // How long a resize waits for the next one before the layout follows it
  // (design rule 26).
  const RESIZE_DEBOUNCE_MS = 100;

  let container: HTMLDivElement;
  let renderer: StageRenderer | null = null;
  let ready = $state(false);
  let size = $state({ w: 0, h: 0 });

  // The room a node needs outside its centre: the largest disc, the gap to its
  // label and the label's width.
  const margin = $derived(R_MAX + LABEL_GAP + labelExtent(theme));

  // The layout, recomputed only when the stage's size, the zone count or the
  // (id, zone) list changes: an O(N) comparison per snapshot and no allocation
  // while they hold.
  let memo: {
    w: number;
    h: number;
    zones: number;
    margin: number;
    ids: number[];
    zs: number[];
    layout: StageLayout;
  } | null = null;
  const layout = $derived.by((): StageLayout | null => {
    if (cluster === null || size.w <= 0 || size.h <= 0) return null;
    const nodes = cluster.nodes;
    const zones = cluster.zones.length;
    const m = memo;
    if (
      m !== null &&
      m.w === size.w &&
      m.h === size.h &&
      m.zones === zones &&
      m.margin === margin &&
      m.ids.length === nodes.length &&
      nodes.every((n, i) => n.id === m.ids[i] && n.zone === m.zs[i])
    ) {
      return m.layout;
    }
    const next = stageLayout(size.w, size.h, zones, nodes, margin);
    memo = {
      w: size.w,
      h: size.h,
      zones,
      margin,
      ids: nodes.map((n) => n.id),
      zs: nodes.map((n) => n.zone),
      layout: next,
    };
    return next;
  });

  // The latest gateway change or join-sweep request, in a sentence.
  let annotation = $state.raw<Blurb>([]);
  $effect(() => {
    if (events.length === 0) {
      annotation = [];
      return;
    }
    // Read once per batch: the cluster moves every frame, the annotation doesn't.
    const names = untrack(() => cluster?.zones.map((z) => z.name) ?? []);
    const nodes = untrack(() => cluster?.nodes ?? []);
    const zone = (z: number): string => zoneLabel(names[z] ?? '');
    for (let i = events.length - 1; i >= 0; i--) {
      const kind = events[i].kind;
      if (kind.type === 'GatewayChanged') {
        const { origin, to, from } = kind;
        annotation =
          to === undefined
            ? blurb`node ${origin} has no gateway into ${zone(kind.zone)}`
            : from === undefined
              ? blurb`node ${origin} now reaches ${zone(kind.zone)} through node ${to}`
              : blurb`node ${origin} now reaches ${zone(kind.zone)} through node ${to}, after node ${from}`;
        return;
      }
      if (kind.type === 'PacketSent' && kind.kind === 'SweepRequest') {
        // A request to a node that has since left names no zone: skip it.
        const dst = nodes.find((n) => n.id === kind.dst);
        if (dst === undefined) continue;
        annotation = blurb`node ${kind.src} asked ${zone(dst.zone)} for missed changes`;
        return;
      }
    }
  });

  $effect(() => {
    if (ready && renderer !== null) renderer.resize(size.w, size.h);
  });

  // Runs only when the memoised layout changes, never per snapshot: the zone
  // names are a function of the zone count, which the layout already follows,
  // so they are read untracked.
  $effect(() => {
    if (ready && renderer !== null && layout !== null) {
      renderer.setLayout(layout, untrack(() => cluster?.zones.map((z) => z.name) ?? []));
    }
  });

  $effect(() => {
    if (ready && renderer !== null) renderer.setCluster(cluster);
  });

  $effect(() => {
    if (ready && renderer !== null && events.length > 0) renderer.applyEvents(events);
  });

  $effect(() => {
    if (ready && renderer !== null) renderer.setRoles(roles);
  });

  $effect(() => {
    if (ready && renderer !== null) renderer.setFocus(focusedId);
  });

  $effect(() => {
    if (ready && renderer !== null) renderer.setTheme(theme);
  });

  /** The node under a pointer event, or null, in the container's own pixels. The
   *  grid cells are `pointer-events: none`, so a pointer over a disc reaches
   *  this handler whether it is over the canvas or a cell. */
  function nodeUnder(event: PointerEvent): number | null {
    if (layout === null) return null;
    const rect = container.getBoundingClientRect();
    const p: Point = { x: event.clientX - rect.left, y: event.clientY - rect.top };
    return nodeAt(p, layout);
  }

  function onPointerMove(event: PointerEvent): void {
    onHover(nodeUnder(event));
  }

  /** A mouse leaving the stage clears the hover. A touch fires `pointerleave` the
   *  moment the finger lifts, which would hide the tooltip a tap just showed:
   *  it stays until the next tap. */
  function onPointerLeave(event: PointerEvent): void {
    if (event.pointerType === 'mouse') onHover(null);
  }

  function onPointerDown(event: PointerEvent): void {
    // Hovering too, so a tap, which fires no move first, shows the tooltip.
    const node = nodeUnder(event);
    onHover(node);
    if (node !== null) onSelect(node);
    else onDeselect();
  }

  onMount(() => {
    let disposed = false;
    let observer: ResizeObserver | null = null;
    let timer = 0;

    void (async () => {
      const r = await StageRenderer.create(container, theme);
      if (disposed) {
        r.destroy();
        return;
      }
      renderer = r;
      const measure = (): void => {
        const rect = container.getBoundingClientRect();
        size = { w: rect.width, h: rect.height };
      };
      measure();
      let first = true;
      observer = new ResizeObserver(() => {
        // The first observation applies at once; later ones are debounced.
        if (first) {
          first = false;
          measure();
          return;
        }
        window.clearTimeout(timer);
        timer = window.setTimeout(measure, RESIZE_DEBOUNCE_MS);
      });
      observer.observe(container);
      ready = true;
    })();

    return () => {
      disposed = true;
      window.clearTimeout(timer);
      observer?.disconnect();
      renderer?.destroy();
      renderer = null;
      ready = false;
    };
  });
</script>

<div class="stage-box">
  <div
    class="stage"
    class:actionable={hoveredId !== null}
    bind:this={container}
    role="presentation"
    onpointerdown={onPointerDown}
    onpointermove={onPointerMove}
    onpointerleave={onPointerLeave}
  ></div>

  <ul class="key" aria-label="Key">
    <li>
      <svg width="28" height="10" aria-hidden="true">
        <line class="in" x1="1" y1="5" x2="27" y2="5" stroke-width={IN_WIDTH} />
      </svg>
      inside a zone (a relay starts at a ◆)
    </li>
    <li>
      <svg width="28" height="10" aria-hidden="true">
        <line class="cross" x1="1" y1="5" x2="20" y2="5" stroke-width={CROSS_WIDTH} />
        <polygon class="arrow" points="19,1 27,5 19,9" />
      </svg>
      across zones
    </li>
    <li>
      <svg width="10" height="10" aria-hidden="true"><circle class="dot" cx="5" cy="5" r="2" /></svg>
      ack, probe or gap report
    </li>
    <li><span aria-hidden="true">◆</span> gateway</li>
    <li><span aria-hidden="true">◇</span> suspected gateway</li>
  </ul>

  <p class="annotation" aria-live="polite">
    {#each annotation as part}{#if typeof part === 'number'}<span data-field="annotation">{part}</span>{:else}{part}{/if}{/each}
  </p>

  {#if cluster !== null && layout !== null}
    <NodeGrid
      {cluster}
      {layout}
      {roles}
      width={size.w}
      height={size.h}
      {hoveredId}
      {focusedId}
      {onSelect}
      {onDeselect}
      {onFocus}
    />
  {/if}
</div>

<style>
  .stage-box {
    position: relative;
    display: flex;
    flex-direction: column;
    height: 100%;
    min-height: 0;
  }

  .stage {
    position: relative;
    flex: 1;
    min-height: 0;
    overflow: hidden;
  }

  /* The ring reads as interactive only over a disc: the cursor is the
     signifier that a click here will land on a node. */
  .stage.actionable {
    cursor: pointer;
  }

  /* The phone: the stage is a square, so the rings keep their shape. */
  @media (width < 640px) {
    .stage {
      flex: none;
      aspect-ratio: 1;
    }
  }

  .key {
    display: flex;
    flex-wrap: wrap;
    gap: var(--gap-xs) var(--gap-md);
    margin: 0;
    padding: var(--gap-xs) var(--gap-md);
    list-style: none;
    font-size: var(--font-size-caption);
    color: var(--color-fg);
  }

  .key li {
    display: flex;
    align-items: center;
    gap: var(--gap-xs);
  }

  .key .in {
    stroke: var(--chart-muted);
  }

  .key .cross {
    stroke: var(--color-fg);
  }

  .key .arrow {
    fill: var(--color-fg);
  }

  .key .dot {
    fill: var(--chart-muted);
  }

  .annotation {
    min-height: 1.5em;
    margin: 0;
    padding: 0 var(--gap-md) var(--gap-xs);
    font-size: var(--font-size-caption);
    color: var(--color-fg);
  }
</style>
