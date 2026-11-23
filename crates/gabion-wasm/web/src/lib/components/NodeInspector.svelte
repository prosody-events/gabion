<script lang="ts">
  import { faultLabel } from '../controls';
  import type { Cut } from '../cuts';
  import { fmtCount, zoneLabel } from '../format';
  import type { NodeState, PeerView, ZoneMode, ZoneView } from '../sim/types';
  import GossipTargets from './GossipTargets.svelte';
  import Icon from './Icon.svelte';
  import InfoTip from './InfoTip.svelte';
  import Sentence from './Sentence.svelte';

  // The selected node's own sections in the detail pane, under the charts: its
  // gossip targets, with the faults of its gateways behind one closed
  // disclosure once the story offers faults, and, quietest and collapsed, its
  // identity.
  let {
    node,
    peers,
    zones,
    zoneMode,
    offered,
    cuts,
    onSilence,
    onCutFromZone,
    onPreview,
  }: {
    node: NodeState;
    /** The node's peer table, as the snapshot inspecting it reports it. */
    peers: PeerView[];
    zones: ZoneView[];
    zoneMode: ZoneMode;
    /** Whether faults are offered: the story has ended, or has no scripted
     *  end. */
    offered: boolean;
    /** The faults made: a gateway already broken one way is not offered
     *  that way again. */
    cuts: readonly Cut[];
    onSilence: (gateway: number) => void;
    onCutFromZone: (gateway: number, zone: number) => void;
    /** A node the route sentence or a fault names, previewed on the stage. */
    onPreview: (node: number | null) => void;
  } = $props();

  // The node's gateways, one per remote zone, in zone order.
  const gateways = $derived(
    peers
      .flatMap((p) => (p.role === 'gateway' && p.id !== undefined && p.zone !== undefined ? [{ id: p.id, zone: p.zone }] : []))
      .sort((a, b) => a.zone - b.zone),
  );
  const silenced = (id: number): boolean => cuts.some((c) => c.kind === 'silenced' && c.node === id);
  const unheard = (id: number): boolean => cuts.some((c) => c.kind === 'unheard-in-zone' && c.node === id);
  /** The preview events of a control that names node `id`. */
  const names = (id: number) => ({
    onpointerenter: () => onPreview(id),
    onpointerleave: () => onPreview(null),
    onfocus: () => onPreview(id),
    onblur: () => onPreview(null),
  });

  /** Shorten a 32-nibble hex id for display; the full value stays in the
   *  identity footer. */
  function shortHex(hex: string): string {
    return hex.length > 12 ? `${hex.slice(0, 6)}…${hex.slice(-4)}` : hex;
  }
</script>

<section class="inspector" aria-label={`node ${node.id} details`}>
  <section class="section">
    <h3 class="section-head">Gossip targets</h3>
    <GossipTargets {node} {peers} {zones} {zoneMode} {onPreview} />
    <!-- With no zone the route sentence already says there are no gateways. -->
    {#if offered && zoneMode === 'Configured'}
      {#if gateways.length === 0}
        <p class="hint">node <span data-field="node">{node.id}</span> has no gateways yet: it finds them at its first heartbeat</p>
      {:else}
        <!-- Closed again for each node selected: its faults are its own. -->
        {#key node.id}
        <details class="faults">
          <summary>Break one of node <span data-field="node">{node.id}</span>'s gateways</summary>
          {#each gateways as g (g.id)}
            <div class="fault-row">
              <button class="btn-secondary" disabled={silenced(g.id)} onclick={() => onSilence(g.id)} {...names(g.id)}
                ><Icon name="silence" /><Sentence parts={faultLabel('silenced', g.id, zoneLabel(zones[g.zone].name))} field="fault" /></button
              >
              <button
                class="btn-secondary"
                disabled={silenced(g.id) || unheard(g.id)}
                onclick={() => onCutFromZone(g.id, g.zone)}
                {...names(g.id)}
                ><Icon name="cut" /><Sentence parts={faultLabel('unheard', g.id, zoneLabel(zones[g.zone].name))} field="fault" /></button
              >
            </div>
          {/each}
        </details>
        {/key}
      {/if}
    {/if}
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

  .hint {
    margin: var(--gap-sm) 0 0;
    font-size: var(--font-size-caption);
    color: var(--color-fg-muted);
  }

  .faults {
    margin-top: var(--gap-sm);
    font-size: var(--font-size-caption);
  }

  .faults summary {
    cursor: pointer;
  }

  .fault-row {
    display: flex;
    flex-wrap: wrap;
    gap: var(--gap-xs) var(--gap-sm);
    margin-top: var(--gap-xs);
  }

  .fault-row button {
    display: inline-flex;
    align-items: center;
    gap: var(--gap-xs);
    min-height: 32px;
    padding: 0 var(--gap-md);
    font-size: var(--font-size-caption);
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
