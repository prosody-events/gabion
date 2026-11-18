<script lang="ts">
  import type { Snippet } from 'svelte';
  import { fmtCount } from '../format';
  import type { NodeState, PeerView, ZoneMode, ZoneView } from '../sim/types';
  import GossipTargets from './GossipTargets.svelte';
  import InfoTip from './InfoTip.svelte';

  // The selected node's own sections in the detail pane, under the charts: its
  // bucket window (`children`, the Strata), its gossip targets and, quietest
  // and collapsed, its identity.
  let {
    node,
    peers,
    zones,
    zoneMode,
    onPreview,
    children,
  }: {
    node: NodeState;
    /** The node's peer table, as the snapshot inspecting it reports it. */
    peers: PeerView[];
    zones: ZoneView[];
    zoneMode: ZoneMode;
    /** A node the route sentence names, previewed on the stage. */
    onPreview: (node: number | null) => void;
    children?: Snippet;
  } = $props();

  /** Shorten a 32-nibble hex id for display; the full value stays in the
   *  identity footer. Mirrors the Strata's key shortening. */
  function shortHex(hex: string): string {
    return hex.length > 12 ? `${hex.slice(0, 6)}…${hex.slice(-4)}` : hex;
  }
</script>

<section class="inspector" aria-label={`node ${node.id} details`}>
  {#if children}
    <section class="section">
      <h3 class="section-head">Bucket window</h3>
      {@render children()}
    </section>
  {/if}

  <section class="section">
    <h3 class="section-head">Gossip targets</h3>
    <GossipTargets {node} {peers} {zones} {zoneMode} {onPreview} />
  </section>

  <!-- Identity: the quietest element, collapsed by default. -->
  <details class="identity">
    <summary>
      Identity &amp; lease
      <span class="id-preview"><span data-field="gossip-id">{shortHex(node.node_id)}</span> · inc <span data-field="incarnation">{node.incarnation}</span></span>
    </summary>
    <dl class="id-list">
      <div class="id-row">
        <dt>
          <InfoTip text="The on-the-wire gossip identity this node announces in every packet header — distinct from the display id above.">
            Gossip node id
          </InfoTip>
        </dt>
        <dd data-field="gossip-id">{node.node_id}</dd>
      </div>
      <div class="id-row">
        <dt>
          <InfoTip text="Bumped each time a node restarts, so peers can supersede a stale alias for the same address. Never bumped here: the simulator has no restart path.">
            Incarnation
          </InfoTip>
        </dt>
        <dd data-field="incarnation">{node.incarnation}</dd>
      </div>
      <div class="id-row">
        <dt>Cells held</dt>
        <dd>
          <span data-field="cells-held">{fmtCount(node.store_stats.active_cells)}</span> of
          <span data-field="cells-capacity">{fmtCount(node.store_stats.cell_capacity)}</span>
        </dd>
      </div>
    </dl>
  </details>
</section>

<style>
  .inspector {
    display: flex;
    flex-direction: column;
    gap: var(--gap-md);
  }

  /* The shared section idiom: a quiet small-caps heading over a hairline rule.
     One modular step, no per-row boxes. */
  .section {
    display: flex;
    flex-direction: column;
    gap: var(--gap-sm);
  }

  .section-head {
    margin: 0;
    padding-bottom: var(--gap-xs);
    border-bottom: 1px solid var(--color-border-line);
    font-size: var(--font-size-caption);
    font-weight: 650;
    color: var(--color-fg-muted);
  }

  /* Identity footer: the quietest element, collapsed by default. */
  .identity {
    font-size: var(--font-size-caption);
  }

  .identity summary {
    cursor: pointer;
    color: var(--color-fg-muted);
    font-weight: 600;
  }

  .id-preview {
    margin-left: var(--gap-xs);
    font-weight: 400;
    color: var(--color-fg-muted);
  }

  .id-list {
    margin: var(--gap-sm) 0 0;
    display: flex;
    flex-direction: column;
    gap: var(--gap-xs);
  }

  .id-row {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: var(--gap-sm);
  }

  .id-row dt {
    color: var(--color-fg-muted);
  }

  .id-row dd {
    margin: 0;
    color: var(--color-fg);
    word-break: break-all;
    text-align: right;
  }
</style>
