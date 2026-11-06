<script lang="ts">
  import { onMount, untrack } from 'svelte';
  import { blurb, fmtCount, type Blurb } from '../format';
  import { capHeight, textWidth } from '../measure';
  import type { ClusterState, EventBatch } from '../sim/types';
  import {
    MARK,
    StageRenderer,
    type Fault,
    type Finish,
    type LabelBox,
    type Landing,
    type Roles,
    type Scale,
    type Trail,
  } from '../stage/renderer';
  import { R_MAX, countFits, nodeAt, stageLayout, stageMargin, type Point, type StageLayout } from '../stage/layout';
  import { fontsReady, type Theme } from '../theme';
  import InfoTip from './InfoTip.svelte';
  import NodeGrid, { type Decision } from './NodeGrid.svelte';
  import RoleGlyph, { type Glyph } from './RoleGlyph.svelte';
  import Sentence from './Sentence.svelte';

  // The tier view. The WebGL canvas (managed imperatively by `StageRenderer`)
  // draws one ring per zone, each node as a vessel inside its ring with its
  // name, the selection and its gateways' ports, and the packets in flight.
  // State arrives as props; the packets and a refusal arrive as commands
  // (`show`, `finish`, `clearTrail`, `reset`, `reject`), which App calls in the
  // order its one queue runs the engine, and the renderer reports a held
  // wave's landing through `onLanded`. `NodeGrid` lays the same nodes over the
  // canvas as focusable cells holding each count, with a tooltip and a table,
  // because the canvas is opaque to assistive tech and to test tooling.
  //
  // Clicking or tapping a disc selects that node and shows its tooltip; the
  // selection never clears. The key under the canvas lists only what is drawn
  // now.
  let {
    cluster,
    roles,
    scale,
    grace,
    faults,
    awaiting,
    previewId,
    decisions,
    phone,
    running,
    speed,
    trail,
    onLanded,
    hoveredId,
    focusedId,
    theme,
    onSelect,
    onEscape,
    onHover,
    onFocus,
  }: {
    cluster: ClusterState | null;
    /** The selected node's marks. */
    roles: Roles | null;
    scale: Scale;
    /** How long a feed's newest hits may lag before they open a ring. */
    grace: number;
    faults: ReadonlyMap<number, Fault>;
    /** How far the selected node is through the wait for its heartbeat, or
     *  null while it holds nothing unsent. */
    awaiting: number | null;
    /** The node a hovered or focused control names, previewed dashed. */
    previewId: number | null;
    decisions: ReadonlyMap<number, Decision>;
    phone: boolean;
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
    onEscape: () => void;
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
  let marks = $state(0);
  let gliding = $state.raw<readonly number[]>([]);
  let labelBoxes = $state.raw<readonly LabelBox[]>([]);

  // The line a node's name takes, px: the room a node needs outside its
  // centre is its disc, its ring, the gap to its name and this line.
  const labelLine = $derived(Math.ceil(theme.captionSmPx * 1.25) + 4);
  const margin = $derived(stageMargin(labelLine));

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
    // Laid out for the largest disc first; a stage too small for it is laid
    // out again with the room its own discs need, so small stages keep their
    // rings as large as they can.
    const first = stageLayout(size.w, size.h, zones, nodes, margin);
    const r = first.spots[0]?.r ?? 0;
    const next = r < R_MAX ? stageLayout(size.w, size.h, zones, nodes, stageMargin(labelLine, r)) : first;
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

  // Whether the counts fit their discs, decided for every node at once from
  // the true total's width: recomputed when it gains a digit, the layout or
  // the theme changes.
  const countFont = $derived(`600 ${theme.countPx}px ${theme.font}`);
  const digits = $derived(fmtCount(cluster?.oracle_total ?? 0).replace(/\d/g, '0'));
  const showCounts = $derived(
    layout !== null && layout.spots.length > 0 && countFits(textWidth(digits, countFont), layout.spots[0].r, capHeight(countFont)),
  );
  const decisionFont = $derived(`600 ${theme.captionPx}px ${theme.font}`);

  /** Draw `batch`'s packets as beams that leave `delay` ms of presentation
   *  clock from now; with `'held'`, call `onLanded` once they have all
   *  landed. False when no renderer exists yet: then nothing will land, and
   *  the caller delivers at once. */
  export function show(batch: EventBatch, delay: number, landing: Landing): boolean {
    if (!ready || renderer === null) return false;
    renderer.show(batch.events, delay, landing);
    return true;
  }

  /** Let the clock run, though paused, until every beam shown has landed; with
   *  `'fast'`, within a fixed short wall time. */
  export function finish(pace: Finish): void {
    renderer?.finish(pace);
  }

  /** Drop the landed routes: every command starts a fresh picture. */
  export function clearTrail(): void {
    renderer?.clearTrail();
  }

  /** Drop every beam and any landing still due: a rebuild. */
  export function reset(): void {
    renderer?.reset();
  }

  /** Play node `node`'s refusal of `hits`. */
  export function reject(node: number, hits: number): void {
    renderer?.reject(node, hits);
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
      labelBoxes = JSON.parse(container.querySelector('canvas')?.dataset.labelBoxes ?? '[]');
    }
  });

  $effect(() => {
    if (ready && renderer !== null) renderer.setScale(scale, grace);
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
    if (ready && renderer !== null) {
      renderer.setRoles(roles);
      labelBoxes = JSON.parse(container.querySelector('canvas')?.dataset.labelBoxes ?? '[]');
    }
  });

  $effect(() => {
    if (ready && renderer !== null) {
      renderer.setFaults(faults);
      labelBoxes = JSON.parse(container.querySelector('canvas')?.dataset.labelBoxes ?? '[]');
    }
  });

  $effect(() => {
    if (ready && renderer !== null) renderer.setAwaiting(awaiting);
  });

  $effect(() => {
    if (ready && renderer !== null) renderer.setPreview(previewId ?? hoveredId ?? focusedId);
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
  }

  /** The key's items, in a fixed order: each mark's glyph, its phrase and the
   *  glossary sentence it opens. An item shows only while its mark is drawn. */
  const KEY: readonly { bit: number; glyph: Glyph; text: Blurb; tip: string }[] = $derived([
    {
      bit: MARK.COUNT,
      glyph: 'vessel',
      text: ['count of hits in the window; the fill rises to the brim, the limit'],
      tip: "The number is the hits this node counts in its window. The fill rises with it: the brim is the limit, so a full vessel is a node that refuses more.",
    },
    {
      bit: MARK.FAR,
      glyph: 'far',
      text: blurb`count of hits; the limit (${scale.limit}) is far away here`,
      tip: 'This scenario sets its limit far above what it sends, so no node comes near refusing: watch the count and the ring instead.',
    },
    {
      bit: MARK.RING,
      glyph: 'ring',
      text: ['the ring closes when the node has heard every hit'],
      tip: "The ring is the share of the cluster's hits this node has heard. It is closed and grey when the node holds them all, amber while it is behind.",
    },
    {
      bit: MARK.SELECTED,
      glyph: 'selected',
      text: ['the selected node'],
      tip: 'Send goes to the selected node, and its gateways are marked. Click any node to select it; Escape returns to the scenario’s node.',
    },
    {
      bit: MARK.PORT,
      glyph: 'port',
      text: ['its gateway into that zone'],
      tip: 'Each node picks its own gateway into each other zone; the port points into the zone that gateway relays to.',
    },
    {
      bit: MARK.SUSPECT,
      glyph: 'suspect',
      text: ['a gateway it suspects'],
      tip: 'A hollow port: the selected node has waited too long for this gateway, or was told it cannot relay, and will pick another.',
    },
    {
      bit: MARK.CAP,
      glyph: 'cap',
      text: ['red brim: at the limit'],
      tip: 'The brim turns red while the node counts the limit: it refuses any request until the oldest hits leave its window.',
    },
    { bit: MARK.LOST, glyph: 'lost', text: ['lost packet'], tip: 'A packet the network dropped: it stops partway, and is resent.' },
    {
      bit: MARK.DOT,
      glyph: 'ack',
      text: ['dot: ack, probe, gap report or sweep request'],
      tip: 'Control packets carry no hits: acknowledgements, probes, gap reports and requests for a join sweep.',
    },
  ]);

  onMount(() => {
    let disposed = false;
    let observer: ResizeObserver | null = null;
    let timer = 0;

    void (async () => {
      // The canvas measures its names in the page's faces: it waits for them,
      // failing open after a short gate.
      await fontsReady();
      if (disposed) return;
      const r = await StageRenderer.create(container, theme, {
        onLanded: () => onLanded(),
        onMarks: (bits) => (marks = bits),
        onGliding: (ids) => (gliding = ids),
      });
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
    aria-keyshortcuts="Escape"
    onpointerdown={onPointerDown}
    onpointermove={onPointerMove}
    onpointerleave={onPointerLeave}
  ></div>

  <!-- The key lists what is drawn now. Every item, laid out unseen in the
       same cell, holds the key's height, so an item that appears never moves
       the canvas. -->
  <div class="key-box">
    <ul class="key" aria-label="Key">
      {#each KEY as item (item.bit)}
        {#if (marks & item.bit) !== 0}
          <li data-mark={item.bit}>
            <RoleGlyph kind={item.glyph} />
            <InfoTip text={item.tip}><Sentence parts={item.text} field="key" /></InfoTip>
          </li>
        {/if}
      {/each}
    </ul>
    <ul class="key sizer" aria-hidden="true">
      {#each KEY as item (item.bit)}
        <li><RoleGlyph kind={item.glyph} /><Sentence parts={item.text} field="key" /></li>
      {/each}
    </ul>
  </div>

  {#if cluster !== null && layout !== null}
    <NodeGrid
      {cluster}
      {layout}
      {roles}
      width={size.w}
      height={size.h}
      {hoveredId}
      {focusedId}
      {showCounts}
      {gliding}
      {decisions}
      {labelBoxes}
      {phone}
      decisionWidth={(text) => textWidth(text, decisionFont)}
      {onSelect}
      {onEscape}
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

  /* Where the page scrolls, the stage keeps a shape of its own: 4 by 3 on
     a tablet, a square on the phone, so the rings keep their size. */
  @media (width < 1100px) {
    .stage {
      flex: none;
      aspect-ratio: 4 / 3;
    }
  }

  @media (width < 640px) {
    .stage {
      aspect-ratio: 1;
    }
  }


  .key-box {
    display: grid;
  }

  .key-box > .key {
    grid-area: 1 / 1;
  }

  .key.sizer {
    visibility: hidden;
  }

  .key {
    display: flex;
    flex-wrap: wrap;
    align-content: flex-start;
    gap: var(--gap-xs) var(--gap-md);
    margin: 0;
    padding: var(--gap-sm) 0 0;
    list-style: none;
    font-size: var(--font-size-caption);
    line-height: 1.25;
    color: var(--color-fg);
  }

  .key li {
    display: flex;
    align-items: center;
    gap: var(--gap-xs);
  }
</style>
