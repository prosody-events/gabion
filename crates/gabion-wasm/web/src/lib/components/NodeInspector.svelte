<script lang="ts">
  import type { Snippet } from 'svelte';
  import { fmtCount } from '../format';
  import type { NodeState, PeerView, ZoneMode, ZoneView } from '../sim/types';
  import GossipTargets from './GossipTargets.svelte';
  import InfoTip from './InfoTip.svelte';

  // The right rail's drill-in: the selected node's state, replacing the charts
  // dashboard while a node is selected (the pinned headline stays above both).
  // The composition root for the node-detail panel: it owns the section heading
  // idiom and the decision-first ordering (hero, actions, bucket window, gossip
  // targets, identity) and delegates the gossip targets to GossipTargets.
  // `children` is the slot the Strata fills (the per-bucket window view).
  let {
    node,
    peers,
    zones,
    zoneMode,
    oracleTotal,
    ruleLimit,
    burstHits,
    onSend,
    onRemove,
    onClose,
    children,
  }: {
    node: NodeState;
    /** The node's peer table, as the snapshot inspecting it reports it. */
    peers: PeerView[];
    zones: ZoneView[];
    zoneMode: ZoneMode;
    /** The windowed ground-truth cluster total: the reference this node's view
     *  is racing toward. */
    oracleTotal: number;
    ruleLimit: number;
    burstHits: number;
    onSend: (id: number) => void;
    onRemove: (id: number) => void;
    onClose: () => void;
    children?: Snippet;
  } = $props();

  // `aggregate ≤ oracle` always (a node's cells are a subset of cluster
  // origins), so lag ≥ 0. `hasTotal` gates the badge so a fresh node never
  // flashes a false "caught up".
  const hasTotal = $derived(oracleTotal > 0);
  const lag = $derived(Math.max(oracleTotal - node.aggregate_total, 0));
  // Admission state: a *fact about this node's windowed total*, not a claim that
  // the visualizer rejected anything (the sim records unconditionally so the
  // count can climb past the limit; production admission is what would reject).
  const overLimit = $derived(ruleLimit > 0 && node.aggregate_total >= ruleLimit);

  /** Shorten a 32-nibble hex id for display; the full value stays in the
   *  identity footer. Mirrors the Strata's key shortening. */
  function shortHex(hex: string): string {
    return hex.length > 12 ? `${hex.slice(0, 6)}…${hex.slice(-4)}` : hex;
  }
</script>

<section class="inspector" aria-label={`Node ${node.id} inspector`}>
  <header class="head">
    <h2>Node <span data-field="node">{node.id}</span></h2>
    <button class="close" type="button" onclick={onClose}>← Charts</button>
  </header>

  <div class="convergence">
    <div class="headline">
      <span class="hl-label">This node's view</span>
      <span class="hl-value" class:over={overLimit} data-field="view">{fmtCount(node.aggregate_total)}</span>
      <span class="hl-refs">
        of <span data-field="true-total">{fmtCount(oracleTotal)}</span> true
        <span class="dot-sep">·</span>
        limit <span data-field="limit">{fmtCount(ruleLimit)}</span>
      </span>
    </div>
    <div class="badges">
      {#if !hasTotal}
        <span class="badge muted" role="status">no traffic yet</span>
      {:else if lag === 0}
        <span class="badge ok" role="status">✓ caught up</span>
      {:else}
        <span class="badge dirty" role="status">lagging −<span data-field="lag">{fmtCount(lag)}</span></span>
      {/if}
      {#if overLimit}
        <span class="pill">
          <InfoTip
            align="right"
            text="This node's windowed total is at or above the rule limit. In production, admission rejects new hits for this key until the window decays back below the limit — the visualizer keeps counting so you can watch it cross."
          >
            over limit
          </InfoTip>
        </span>
      {/if}
    </div>
  </div>

  <div class="actions">
    <button class="send" type="button" onclick={() => onSend(node.id)}>Send burst</button>
    <button class="remove" type="button" onclick={() => onRemove(node.id)}>Remove this node</button>
    <span class="hint"><span data-field="burst-hits">{fmtCount(burstHits)}</span> hits to this node</span>
  </div>

  {#if children}
    <section class="section">
      <h3 class="section-head">Bucket window</h3>
      {@render children()}
    </section>
  {/if}

  <section class="section">
    <h3 class="section-head">Gossip targets</h3>
    <GossipTargets {node} {peers} {zones} {zoneMode} />
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
    height: 100%;
    min-height: 0;
    overflow-y: auto;
  }

  .head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: var(--gap-sm);
  }

  h2 {
    margin: 0;
    font-size: var(--font-size-heading);
    font-weight: 650;
    letter-spacing: -0.01em;
  }

  .close {
    padding: var(--gap-xs) var(--gap-sm);
    border: 1px solid var(--color-border-line);
    border-radius: var(--radius);
    background: var(--color-bg);
    font-size: var(--font-size-caption);
    color: var(--color-fg-muted);
    transition:
      background 120ms ease,
      border-color 120ms ease;
  }

  .close:hover {
    background: var(--color-panel);
    border-color: var(--color-fg-muted);
  }

  /* §2 — the headline. One step quieter than the cluster hero in HeadlineMetric
     (2.25rem vs 2.75rem): this is one node's view, not the cluster's. */
  .convergence {
    display: grid;
    grid-template-columns: 1fr auto;
    align-items: end;
    gap: var(--gap-sm);
  }

  .headline {
    display: flex;
    flex-direction: column;
    gap: 2px;
  }

  .hl-label {
    font-size: var(--font-size-caption-sm);
    font-weight: 650;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: var(--color-fg-muted);
  }

  .hl-value {
    font-size: 2.25rem;
    font-weight: 350;
    line-height: 1;
    color: var(--color-fg);
    transition: color 0.4s ease;
  }

  .hl-value.over {
    color: var(--color-bad);
  }

  .hl-refs {
    font-size: var(--font-size-caption);
    color: var(--color-fg-muted);
  }

  .dot-sep {
    color: var(--color-fg-muted);
  }

  .badges {
    display: flex;
    flex-direction: column;
    align-items: flex-end;
    gap: var(--gap-xs);
  }

  .badge {
    font-size: var(--font-size-caption);
    font-weight: 600;
    padding: var(--gap-xs) var(--gap-sm);
    border-radius: var(--radius);
    white-space: nowrap;
  }

  .badge.ok {
    color: var(--color-ok);
    background: color-mix(in srgb, var(--color-ok) 12%, transparent);
  }

  .badge.dirty {
    color: var(--color-warn);
    background: color-mix(in srgb, var(--color-warn) 14%, transparent);
  }

  .badge.muted {
    color: var(--color-fg-muted);
    background: var(--color-bg);
    font-weight: 500;
  }

  /* The over-limit pill reuses the reject vocabulary — word + colour, paired. */
  .pill {
    font-size: var(--font-size-caption);
    font-weight: 600;
    color: var(--color-bad);
    background: color-mix(in srgb, var(--color-bad) 12%, transparent);
    padding: var(--gap-xs) var(--gap-sm);
    border-radius: var(--radius);
  }

  .actions {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: var(--gap-sm);
  }

  .send {
    padding: var(--gap-sm) var(--gap-md);
    border: 1px solid var(--color-fg);
    border-radius: var(--radius);
    background: var(--color-fg);
    color: var(--color-panel);
    font-size: var(--font-size-caption);
    transition: background 120ms ease;
  }

  .send:hover {
    background: var(--color-fg-muted);
  }

  .remove {
    padding: var(--gap-sm) var(--gap-md);
    border: 1px solid var(--color-border-line);
    border-radius: var(--radius);
    background: var(--color-bg);
    font-size: var(--font-size-caption);
  }

  .remove:hover {
    border-color: var(--color-fg-muted);
  }

  .hint {
    font-size: var(--font-size-caption);
    color: var(--color-fg-muted);
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

  /* §8 — identity footer: the quietest element, collapsed by default. */
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
