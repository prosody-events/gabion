<script module lang="ts">
  /** A node's decision on a Send: how many hits, and whether it admitted. */
  export interface Decision {
    readonly hits: number;
    readonly admitted: boolean;
  }
</script>

<script lang="ts">
  import { claim, fmtCount, fmtDuration, noun, zoneLabel } from '../format';
  import type { LabelBox, Roles } from '../stage/renderer';
  import { HIT_MIN, RING_REACH, type StageLayout } from '../stage/layout';
  import { missingFor, type ClusterState } from '../sim/types';
  import RoleGlyph from './RoleGlyph.svelte';
  import Sentence from './Sentence.svelte';

  // The stage's text and keyboard alternative. The canvas is opaque to assistive
  // tech and to test tooling, so every node also exists here as a grid cell laid
  // exactly over its disc: focusable with a roving tabindex, named by a sentence,
  // the source of the tooltip, and the home of the node's count, centred in its
  // disc, and of a Send's decision beside it. "Show as table" lists the same
  // rows as a table. One row model feeds all three, in (zone, id) order, never
  // re-sorted. Rows are rebuilt per snapshot: at most 100, by DOM
  // reconciliation, outside the renderer's no-allocation contract.
  //
  // The cells never take the pointer (`pointer-events: none`): the stage's hit
  // test picks the node under it, so a mouse, a touch and a test all reach the
  // same code.
  let {
    cluster,
    layout,
    roles,
    width,
    height,
    hoveredId,
    focusedId,
    showCounts,
    gliding,
    decisions,
    labelBoxes,
    phone,
    decisionWidth,
    onSelect,
    onEscape,
    onFocus,
  }: {
    cluster: ClusterState;
    layout: StageLayout;
    roles: Roles | null;
    /** The stage box, px: the tooltip stays inside it. */
    width: number;
    height: number;
    hoveredId: number | null;
    focusedId: number | null;
    /** Whether every count fits its disc: all shown or all hidden. */
    showCounts: boolean;
    /** Nodes gliding to a new slot, whose counts hide while they move. */
    gliding: readonly number[];
    /** Each node's decision on the last Send, until it is cleared. */
    decisions: ReadonlyMap<number, Decision>;
    /** The stage's name boxes, which a decision keeps clear of. */
    labelBoxes: readonly LabelBox[];
    phone: boolean;
    /** A decision's width in px, by its text. */
    decisionWidth: (text: string) => number;
    onSelect: (id: number) => void;
    /** Escape on a cell: the selection returns to the scenario's node. */
    onEscape: () => void;
    /** The node whose cell has keyboard focus, or `null` once focus leaves. */
    onFocus: (id: number | null) => void;
  } = $props();

  /** What a node is to the selection. */
  type Role = 'selected' | 'gateway' | 'suspect' | null;

  interface Row {
    id: number;
    zone: number;
    zoneName: string;
    view: number;
    behind: number;
    /** How long the node has lacked a change, at least; absent once caught up. */
    missingFor: number | undefined;
    gatewayFor: number;
    role: Role;
    x: number;
    y: number;
    r: number;
    /** The cell's half-size, px: the disc, or a 24 px target for a smaller one. */
    half: number;
  }

  const rows = $derived.by((): Row[] => {
    const byId = new Map(cluster.nodes.map((n) => [n.id, n]));
    return layout.spots.flatMap((spot) => {
      const node = byId.get(spot.id);
      if (node === undefined) return [];
      if (import.meta.env.DEV && node.window_total > cluster.oracle_total) {
        console.error(`node ${node.id} sees ${node.window_total}, more than the true total ${cluster.oracle_total}`);
      }
      const gateway = roles?.gateways.get(spot.id);
      const role: Role =
        roles?.selected === spot.id ? 'selected' : gateway === undefined ? null : gateway.suspect ? 'suspect' : 'gateway';
      return [
        {
          id: spot.id,
          zone: spot.zone,
          zoneName: zoneLabel(cluster.zones[spot.zone].name),
          view: node.window_total,
          behind: cluster.oracle_total - node.window_total,
          missingFor: missingFor(node.lag),
          gatewayFor: node.gateway_for,
          role,
          x: spot.x,
          y: spot.y,
          r: spot.r,
          half: Math.max(spot.r, HIT_MIN),
        },
      ];
    });
  });

  // One group per zone that has nodes, in zone order: a row of the grid.
  const groups = $derived.by(() => {
    const out: Row[][] = [];
    for (const row of rows) {
      if (out.length > 0 && out[out.length - 1][0].zone === row.zone) out[out.length - 1].push(row);
      else out.push([row]);
    }
    return out;
  });

  // The one cell in the tab order: the last one focused, else the first.
  let roving = $state<number | null>(null);
  const tabbable = $derived(rows.some((r) => r.id === roving) ? roving : (rows[0]?.id ?? null));

  // Escape hides the tooltip until the hover or focus moves on.
  let dismissed = $state(false);
  $effect(() => {
    void hoveredId;
    void focusedId;
    dismissed = false;
  });
  const hotId = $derived(hoveredId ?? focusedId);
  const hot = $derived(dismissed ? undefined : rows.find((r) => r.id === hotId));

  let tableOpen = $state(false);
  let grid: HTMLElement;
  // The tooltip's border box, fractional: a rounded size would let it cross the
  // stage's edge by up to a pixel.
  let tipSize = $state<readonly ResizeObserverSize[] | undefined>(undefined);
  const tipWidth = $derived(tipSize?.[0].inlineSize ?? 0);
  const tipHeight = $derived(tipSize?.[0].blockSize ?? 0);

  const TIP_GAP = 8;
  // The tooltip sits right of its node, pulled back inside the stage.
  const tipLeft = $derived(
    hot === undefined ? 0 : Math.max(0, Math.min(hot.x + hot.half + TIP_GAP, width - tipWidth)),
  );
  const tipTop = $derived(
    hot === undefined ? 0 : Math.max(0, Math.min(hot.y - tipHeight / 2, height - tipHeight)),
  );

  // A decision stands this far outside the node's ring.
  const DECISION_GAP = 9;

  /** Where node `row`'s decision text goes: right of its ring, or left when
   *  the right would leave the stage or touch a name. */
  function decisionLeft(row: Row, text: string): { left: number; flipped: boolean } {
    const w = decisionWidth(text);
    const reach = row.r + RING_REACH + DECISION_GAP;
    const right = { x: row.x + reach, y: row.y - 10, w, h: 20 };
    const crosses = labelBoxes.some(
      (b) => right.x < b.x + b.w && b.x < right.x + right.w && right.y < b.y + b.h && b.y < right.y + right.h,
    );
    return crosses || right.x + w > width ? { left: row.x - reach - w, flipped: true } : { left: right.x, flipped: false };
  }

  const decisionText = (d: Decision): string =>
    phone ? (d.admitted ? `+${d.hits}` : `${d.hits}`) : d.admitted ? `+${d.hits} admitted` : `${d.hits} rejected`;

  /** The cell's accessible name: the live sentence for the cell in the tab order
   *  (the one a screen reader lands on), the bare identity for the rest, so a
   *  frame rewrites one label rather than every node's. */
  function label(row: Row): string {
    const who = `Select node ${row.id}, ${row.zoneName}`;
    if (row.id !== tabbable) return who;
    const missing =
      row.missingFor === undefined ? '' : `, not caught up for at least ${fmtDuration(row.missingFor)}`;
    return `${who}, sees ${fmtCount(row.view)} of ${fmtCount(cluster.oracle_total)}, ${fmtCount(row.behind)} behind${missing}`;
  }

  function focusCell(id: number | undefined): void {
    if (id !== undefined) grid.querySelector<HTMLElement>(`[data-node="${id}"]`)?.focus();
  }

  /** Left and Right move within the zone, Up and Down to the same rank in the
   *  adjacent zone with nodes (the rank clamped), Home and End to the zone's
   *  ends; Enter or Space selects and Escape returns the selection to the
   *  scenario's node. */
  function onKeydown(event: KeyboardEvent, row: Row): void {
    const g = groups.findIndex((group) => group[0].zone === row.zone);
    const group = groups[g];
    const rank = group.findIndex((r) => r.id === row.id);
    let next: number | undefined;
    switch (event.key) {
      case 'ArrowRight':
        next = group[Math.min(rank + 1, group.length - 1)].id;
        break;
      case 'ArrowLeft':
        next = group[Math.max(rank - 1, 0)].id;
        break;
      case 'ArrowDown':
      case 'ArrowUp': {
        const to = groups[g + (event.key === 'ArrowDown' ? 1 : -1)];
        next = to?.[Math.min(rank, to.length - 1)].id ?? row.id;
        break;
      }
      case 'Home':
        next = group[0].id;
        break;
      case 'End':
        next = group[group.length - 1].id;
        break;
      case 'Enter':
      case ' ':
        onSelect(row.id);
        break;
      case 'Escape':
        dismissed = true;
        onEscape();
        break;
      default:
        return;
    }
    // The page's own Space and arrow keys (play, step) must not fire from here.
    event.preventDefault();
    event.stopPropagation();
    focusCell(next);
  }
</script>

<div class="layer" style="width: {width}px; height: {height}px">
  <div class="grid" role="grid" aria-label="Nodes by zone" bind:this={grid}>
    {#each groups as group (group[0].zone)}
      <div class="row" role="row" data-zone={group[0].zone} aria-label={group[0].zoneName}>
        {#each group as row (row.id)}
          <div
            class="cell"
            role="gridcell"
            tabindex={row.id === tabbable ? 0 : -1}
            data-node={row.id}
            aria-label={label(row)}
            style="left: {row.x - row.half}px; top: {row.y - row.half}px; width: {2 * row.half}px; height: {2 *
              row.half}px"
            onfocus={() => {
              roving = row.id;
              onFocus(row.id);
            }}
            onblur={() => onFocus(null)}
            onkeydown={(event) => onKeydown(event, row)}
          >
            {#if showCounts && !gliding.includes(row.id)}
              <span class="count" data-field="node-view" aria-hidden="true">{fmtCount(row.view)}</span>
            {/if}
          </div>
          {@const d = decisions.get(row.id)}
          {#if d !== undefined && showCounts && !gliding.includes(row.id)}
            {@const text = decisionText(d)}
            {@const at = decisionLeft(row, text)}
            <span
              class="decision"
              class:rejected={!d.admitted}
              class:flipped={at.flipped}
              data-node={row.id}
              style="left: {at.left}px; top: {row.y}px"
              ><Sentence
                parts={phone
                  ? [d.admitted ? '+' : '', claim(d.hits, row.id, d.admitted ? 'admitted' : 'rejected')]
                  : d.admitted
                    ? ['+', claim(d.hits, row.id, 'admitted'), ' admitted']
                    : [claim(d.hits, row.id, 'rejected'), ' rejected']}
                field="decision"
              />{#if phone && !d.admitted}<RoleGlyph kind="rejected" />{/if}</span
            >
          {/if}
        {/each}
      </div>
    {/each}
  </div>
  {#if hot !== undefined}
    <div
      class="tooltip"
      role="tooltip"
      style="left: {tipLeft}px; top: {tipTop}px"
      bind:borderBoxSize={tipSize}
    >
      select node <span data-field="node">{hot.id}</span> · {hot.zoneName} · sees
      <span data-field="view">{fmtCount(hot.view)}</span> of
      <span data-field="true-total">{fmtCount(cluster.oracle_total)}</span>
      (<span data-field="behind">{fmtCount(hot.behind)}</span> behind){#if hot.missingFor !== undefined}
        · not caught up (≥ <span data-field="missing-for">{fmtDuration(hot.missingFor)}</span>){/if}{#if (hot.role === 'gateway' || hot.role === 'suspect') && roles !== null}
        · node <span data-field="carrier">{hot.id}</span> carries node <span data-field="sender">{roles.selected}</span>'s hits
        into {hot.zoneName}{/if}{#if hot.gatewayFor > 0}
        · gateway into {hot.zoneName} for
        <span data-field="gateway-for">{fmtCount(hot.gatewayFor)}</span>
        {noun(hot.gatewayFor, 'node')}{/if}
    </div>
  {/if}
</div>

<details class="node-table" bind:open={tableOpen}>
  <summary>Show as table</summary>
  {#if tableOpen}
    <table>
      <thead>
        <tr>
          <th scope="col">Zone</th>
          <th scope="col">Node</th>
          <th scope="col">Sees</th>
          <th scope="col">Behind</th>
          <th scope="col">Role</th>
        </tr>
      </thead>
      <tbody>
        {#each rows as row (row.id)}
          <tr data-node={row.id} data-zone={row.zone}>
            <td>{row.zoneName}</td>
            <td>node <span data-field="node">{row.id}</span></td>
            <td data-field="view">{fmtCount(row.view)}</td>
            <td data-field="behind">{fmtCount(row.behind)}</td>
            <td data-role={row.role ?? undefined}>
              {#if row.role === 'selected'}
                <RoleGlyph kind="selected" /> selected
              {:else if row.role === 'gateway'}
                <RoleGlyph kind="port" /> its gateway
              {:else if row.role === 'suspect'}
                <RoleGlyph kind="suspect" /> suspected gateway
              {/if}
              {#if row.gatewayFor > 0}
                gateway into {row.zoneName} for
                <span data-field="gateway-for">{fmtCount(row.gatewayFor)}</span>
                {noun(row.gatewayFor, 'node')}
              {:else if row.role === null}
                —
              {/if}
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  {/if}
</details>

<style>
  .layer {
    position: absolute;
    top: 0;
    left: 0;
    pointer-events: none;
  }

  .grid,
  .row {
    position: absolute;
    inset: 0;
    pointer-events: none;
  }

  .cell {
    position: absolute;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 50%;
    pointer-events: none;
  }

  .cell:focus-visible {
    outline: 2px solid var(--color-warn);
    outline-offset: 0;
  }

  /* The count, centred in its disc, haloed in the panel colour so it reads on
     the fill and on the empty inside alike. */
  .count {
    font-size: var(--font-size-count);
    font-weight: 600;
    line-height: 1;
    color: var(--color-fg);
    paint-order: stroke;
    -webkit-text-stroke: 3px var(--color-panel);
  }

  /* A Send's decision beside the disc, its baseline on the count's. */
  .decision {
    position: absolute;
    display: inline-flex;
    align-items: center;
    gap: 2px;
    transform: translateY(-50%);
    font-size: var(--font-size-caption);
    font-weight: 600;
    line-height: 1;
    white-space: nowrap;
    color: var(--color-fg);
    paint-order: stroke;
    -webkit-text-stroke: 3px var(--color-bg);
  }

  .decision.rejected {
    color: var(--color-bad);
  }

  .tooltip {
    position: absolute;
    width: max-content;
    max-width: 22rem;
    padding: var(--gap-xs) var(--gap-sm);
    border: 1px solid var(--color-border-line);
    border-radius: var(--radius);
    background: var(--surface-popover);
    box-shadow: var(--shadow-popover);
    color: var(--color-fg);
    font-size: var(--font-size-caption);
    line-height: 1.4;
  }

  .node-table {
    font-size: var(--font-size-caption);
    padding: 0 var(--gap-md);
  }

  .node-table summary {
    cursor: pointer;
    color: var(--color-fg-muted);
    font-weight: 600;
  }

  .node-table[open] {
    max-height: 40%;
    overflow-y: auto;
  }

  table {
    width: 100%;
    border-collapse: collapse;
  }

  th,
  td {
    padding: var(--gap-xs) var(--gap-sm);
    border-bottom: 1px solid var(--color-border-line);
    text-align: left;
  }

  th {
    position: sticky;
    top: 0;
    background: var(--color-bg);
    color: var(--color-fg-muted);
    font-weight: 600;
  }
</style>
