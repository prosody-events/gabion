<script lang="ts">
  import { onMount, untrack } from 'svelte';
  import { blurb, zoneLabel, type Blurb } from '../format';
  import type { ClusterState, EventBatch } from '../sim/types';
  import { StageRenderer, labelExtent, type Roles, type Trail } from '../stage/renderer';
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
  // the packets in flight. State arrives as props (`cluster`, `roles`, whether
  // the clock runs, its speed, the trail); the packets arrive as commands
  // (`show`, `finish`, `clearTrail`, `reset`), which App calls in the order its
  // one queue runs the engine, and the renderer reports a held wave's landing
  // through `onLanded`. `NodeGrid` lays the same nodes over the canvas as
  // focusable cells with a tooltip, and a table, because the canvas is opaque
  // to assistive tech and to test tooling.
  //
  // Clicking or tapping a disc selects that node and shows its tooltip; clicking
  // the bare stage deselects. The key and the one-line annotation sit under the
  // canvas, outside it, so nothing overlaps the drawing.
  let {
    cluster,
    roles,
    running,
    speed,
    trail,
    onLanded,
    hoveredId,
    focusedId,
    theme,
    onSelect,
    onDeselect,
    onHover,
    onFocus,
  }: {
    cluster: ClusterState | null;
    /** The selected origin's marks, or `null` with no selection. */
    roles: Roles | null;
    /** Whether the presentation clock runs freely: the page is playing. */
    running: boolean;
    /** The presentation clock's speed against wall time. */
    speed: number;
    trail: Trail;
    /** Called once a held wave's beams have all landed. */
    onLanded: () => void;
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

  // The latest gateway change or join-sweep request, in a sentence: it stands
  // until a later one replaces it.
  let annotation = $state.raw<Blurb>([]);

  /** Draw `batch`'s packets as beams that leave `delay` ms of presentation
   *  clock from now, and read its annotation; with `hold`, call `onLanded`
   *  once they have all landed. False when no renderer exists yet: then
   *  nothing will land, and the caller delivers at once. */
  export function show(batch: EventBatch, delay: number, hold: boolean): boolean {
    annotation = annotate(batch) ?? annotation;
    if (!ready || renderer === null) return false;
    renderer.show(batch.events, delay, hold);
    return true;
  }

  /** Let the clock run, though paused, until every beam shown has landed; with
   *  `fast`, within a fixed short wall time. */
  export function finish(fast: boolean): void {
    renderer?.finish(fast);
  }

  /** Drop the landed routes: every command starts a fresh picture. */
  export function clearTrail(): void {
    renderer?.clearTrail();
  }

  /** Drop every beam, any landing still due and the annotation: a rebuild. */
  export function reset(): void {
    renderer?.reset();
    annotation = [];
  }

  /** Zone `z`'s name as the stage writes it. */
  function zoneName(z: number): string {
    return zoneLabel(cluster?.zones[z]?.name ?? '');
  }

  /** The newest gateway change or join-sweep request in `batch`, in a sentence,
   *  or null with neither. */
  function annotate(batch: EventBatch): Blurb | null {
    const events = batch.events;
    for (let i = events.length - 1; i >= 0; i--) {
      const kind = events[i].kind;
      if (kind.type === 'GatewayChanged') {
        const { origin, to, from } = kind;
        return to === undefined
          ? blurb`node ${origin} has no gateway into ${zoneName(kind.zone)}`
          : from === undefined
            ? blurb`node ${origin} now reaches ${zoneName(kind.zone)} through node ${to}`
            : blurb`node ${origin} now reaches ${zoneName(kind.zone)} through node ${to}, after node ${from}`;
      }
      if (kind.type === 'PacketSent' && kind.kind === 'SweepRequest') {
        // A request to a node that has since left names no zone: skip it.
        const dst = cluster?.nodes.find((n) => n.id === kind.dst);
        if (dst !== undefined) return blurb`node ${kind.src} asked ${zoneName(dst.zone)} for missed changes`;
      }
    }
    return null;
  }

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
    if (ready && renderer !== null) renderer.setRunning(running);
  });

  $effect(() => {
    if (ready && renderer !== null) renderer.setSpeed(speed);
  });

  $effect(() => {
    if (ready && renderer !== null) renderer.setTrail(trail);
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
      const r = await StageRenderer.create(container, theme, () => onLanded());
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
    <li>
      <svg width="28" height="12" aria-hidden="true">
        <line class="lost" x1="1" y1="6" x2="20" y2="6" stroke-width={IN_WIDTH} />
        <line class="lost" x1="20" y1="1" x2="20" y2="11" stroke-width={IN_WIDTH} />
      </svg>
      lost on the way
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

  .key .lost {
    stroke: var(--chart-muted);
  }

  .annotation {
    min-height: 1.5em;
    margin: 0;
    padding: 0 var(--gap-md) var(--gap-xs);
    font-size: var(--font-size-caption);
    color: var(--color-fg);
  }
</style>
