<script lang="ts">
  import type { Snippet } from 'svelte';
  import type { NodeState, PeerView } from '../sim/types';
  import GossipCadence from './GossipCadence.svelte';
  import StorageCapacity from './StorageCapacity.svelte';
  import InfoTip from './InfoTip.svelte';

  // The right rail's drill-in: the selected node's full state, replacing the
  // charts dashboard while a node is selected (the pinned headline stays above
  // both). The composition root for the node-detail panel — it owns the section
  // heading idiom and the decision-first ordering, and delegates the busier
  // sections to GossipCadence (§4) and StorageCapacity (§6). `children` is the
  // slot the Strata fills (§3, the per-bucket window view).
  let {
    node,
    oracleTotal,
    ruleLimit,
    baseFanout,
    burstHits,
    version,
    onSend,
    onClose,
    children,
  }: {
    node: NodeState;
    /** The windowed ground-truth cluster total — the reference this node's view
     *  is racing toward. */
    oracleTotal: number;
    ruleLimit: number;
    /** The configured base fanout knob — the floor the adaptive fanout grows
     *  above under load. */
    baseFanout: number;
    burstHits: number;
    /** The per-snapshot counter (drives GossipCadence's tick ring buffer). */
    version: number;
    onSend: (id: number) => void;
    onClose: () => void;
    children?: Snippet;
  } = $props();

  // §2 — convergence. `aggregate ≤ oracle` always (a node's cells are a subset
  // of cluster origins), so lag ≥ 0. `hasTotal` gates the badge so a fresh node
  // never flashes a false "caught up".
  const hasTotal = $derived(oracleTotal > 0);
  const lag = $derived(Math.max(oracleTotal - node.aggregate_total, 0));
  // Admission state — a *fact about this node's windowed total*, not a claim that
  // the visualizer rejected anything (the sim records unconditionally so the
  // count can climb past the limit; production admission is what would reject).
  const overLimit = $derived(ruleLimit > 0 && node.aggregate_total >= ruleLimit);

  // §7 — peers sorted known-first, then by stable id (unresolved last), so the
  // table settles calmly as peers resolve rather than reshuffling. The Option
  // fields cross the wasm boundary as `undefined` (not `null`) when None, so the
  // checks here are the loose `!= null` that catches both.
  const sortedPeers = $derived(
    [...node.peers].sort((a, b) => {
      const aKnown = a.node_id != null;
      const bKnown = b.node_id != null;
      if (aKnown !== bKnown) return aKnown ? -1 : 1;
      return (a.id ?? Number.MAX_SAFE_INTEGER) - (b.id ?? Number.MAX_SAFE_INTEGER);
    }),
  );
  const knownPeers = $derived(node.peers.filter((p) => p.node_id != null).length);
  const pendingPeers = $derived(node.peers.length - knownPeers);

  // §5 — the send queue has no static capacity; its high-water mark is the only
  // honest denominator. When the mark is 0 the queue never backed up.
  const sendPct = $derived(
    node.max_send_pending_depth > 0
      ? (node.send_pending_depth / node.max_send_pending_depth) * 100
      : 0,
  );

  /** Shorten a 32-nibble hex id for display; full value stays available on hover
   *  and in the identity footer. Mirrors the Strata's key shortening. */
  function shortHex(hex: string): string {
    return hex.length > 12 ? `${hex.slice(0, 6)}…${hex.slice(-4)}` : hex;
  }

  function peerName(p: PeerView): string {
    return p.id != null ? `Node ${p.id}` : 'unresolved';
  }
</script>

<section class="inspector" aria-label={`Node ${node.id} inspector`}>
  <header class="head">
    <h2>Node <span class="numeric">{node.id}</span></h2>
    <button class="close" type="button" onclick={onClose}>← Charts</button>
  </header>

  <!-- §2 Convergence & aggregate — the decision-relevant headline. -->
  <div class="convergence">
    <div class="headline">
      <span class="hl-label">This node's view</span>
      <span class="hl-value numeric" class:over={overLimit}>{node.aggregate_total.toLocaleString()}</span>
      <span class="hl-refs">
        of <span class="numeric">{oracleTotal.toLocaleString()}</span> true
        <span class="dot-sep">·</span>
        limit <span class="numeric">{ruleLimit.toLocaleString()}</span>
      </span>
    </div>
    <div class="badges">
      {#if !hasTotal}
        <span class="badge muted" role="status">no traffic yet</span>
      {:else if lag === 0}
        <span class="badge ok" role="status">✓ caught up</span>
      {:else}
        <span class="badge dirty" role="status">lagging −{lag.toLocaleString()}</span>
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
    <span class="hint"><span class="numeric">{burstHits}</span> hits to this node</span>
  </div>

  <!-- §3 Bucket-window Strata (the per-bucket mechanic). -->
  {#if children}
    <section class="section">
      <h3 class="section-head">Bucket window</h3>
      {@render children()}
    </section>
  {/if}

  <!-- §4 Gossip cadence & adaptive fanout. -->
  <section class="section">
    <h3 class="section-head">Gossip cadence &amp; fanout</h3>
    <GossipCadence {node} {version} {baseFanout} />
  </section>

  <!-- §5 Gossip I/O & queues. -->
  <section class="section">
    <h3 class="section-head">Gossip I/O &amp; queues</h3>
    <div class="io">
      <div class="io-row">
        <span class="io-label">
          <InfoTip text="Outbound packets queued behind the transport, scaled to the high-water mark since startup — the only honest denominator, since there is no fixed send capacity. At the peak it reads full by definition.">
            send queue
          </InfoTip>
        </span>
        {#if node.max_send_pending_depth > 0}
          <div
            class="meter"
            role="meter"
            aria-label="send queue depth"
            aria-valuemin={0}
            aria-valuemax={node.max_send_pending_depth}
            aria-valuenow={node.send_pending_depth}
            aria-valuetext="{node.send_pending_depth} of {node.max_send_pending_depth} peak"
          >
            <div class="meter-fill" style="width: {sendPct}%"></div>
          </div>
          <span class="io-value numeric">{node.send_pending_depth}<span class="sep"> / </span>{node.max_send_pending_depth}</span>
        {:else}
          <span class="io-idle">queue idle</span>
          <span class="io-value numeric">0</span>
        {/if}
      </div>

      <div class="io-row">
        <span class="io-label">
          <InfoTip text="Locally-originated cells waiting in the dirty ring to be gossiped out, and cells received from peers waiting to be re-gossiped (forwarded).">
            dirty rings
          </InfoTip>
        </span>
        <span class="rings">
          <span class="ring">local <span class="numeric">{node.local_dirty_len}</span></span>
          <span class="ring">forwarded <span class="numeric">{node.forwarded_dirty_len}</span></span>
        </span>
      </div>

      <div class="io-row">
        <span class="io-label">
          <InfoTip text="Inbound packets the wire decoder rejected — bad HMAC, truncated, or otherwise undecodable. Zero is the calm, expected state.">
            decode rejects
          </InfoTip>
        </span>
        {#if node.decode_reject_count > 0}
          <span class="rejects bad" aria-live="polite">
            <span class="warn-glyph" aria-hidden="true">▲</span>
            Rejected <span class="numeric">{node.decode_reject_count.toLocaleString()}</span>
          </span>
        {:else}
          <span class="rejects calm"><span class="numeric">0</span> rejected</span>
        {/if}
      </div>
    </div>
  </section>

  <!-- §6 Storage & capacity. -->
  <section class="section">
    <h3 class="section-head">Storage &amp; capacity</h3>
    <StorageCapacity stats={node.store_stats} />
  </section>

  <!-- §7 Peer table. -->
  <section class="section">
    <h3 class="section-head">Peers</h3>
    <p class="peer-summary">
      <span class="numeric">{node.peers.length}</span> tracked
      <span class="dot-sep">·</span>
      <span class="numeric">{knownPeers}</span> known
      <span class="dot-sep">·</span>
      <span class="numeric">{pendingPeers}</span> pending
      {#if node.peers.length > 0 && pendingPeers === 0}
        <span class="badge ok inline" role="status">✓ all known</span>
      {/if}
    </p>
    {#if node.peers.length === 0}
      <p class="peer-empty">No peers yet — this node is alone.</p>
    {:else}
      <div class="peer-scroll">
        <table class="peers">
          <thead>
            <tr>
              <th scope="col">Peer</th>
              <th scope="col">State</th>
              <th scope="col">Gossip id</th>
            </tr>
          </thead>
          <tbody>
            {#each sortedPeers as peer, i (peer.id ?? `pending-${i}`)}
              <tr>
                <td class:unresolved={peer.id == null}>{peerName(peer)}</td>
                <td>
                  {#if peer.node_id != null}
                    <span class="state known"><span aria-hidden="true">●</span> known</span>
                  {:else}
                    <span class="state pending"><span aria-hidden="true">○</span> pending</span>
                  {/if}
                </td>
                <td class="numeric gossip-id">
                  {peer.node_id != null ? shortHex(peer.node_id) : '—'}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>

  <!-- §8 Identity & lease — quietest, collapsed by default. -->
  <details class="identity">
    <summary>
      Identity &amp; lease
      <span class="id-preview numeric">{shortHex(node.node_id)} · inc {node.incarnation}</span>
    </summary>
    <dl class="id-list">
      <div class="id-row">
        <dt>
          <InfoTip text="The on-the-wire gossip identity this node announces in every packet header — distinct from the display id above.">
            Gossip node id
          </InfoTip>
        </dt>
        <dd class="numeric">{node.node_id}</dd>
      </div>
      <div class="id-row">
        <dt>
          <InfoTip text="Bumped each time a node restarts, so peers can supersede a stale alias for the same address. Always 1 here — the simulator has no restart path.">
            Incarnation
          </InfoTip>
        </dt>
        <dd class="numeric">{node.incarnation}</dd>
      </div>
    </dl>
  </details>
</section>

<style>
  .inspector {
    display: flex;
    flex-direction: column;
    gap: var(--space-3);
    height: 100%;
    min-height: 0;
    overflow-y: auto;
  }

  .head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: var(--space-2);
  }

  h2 {
    margin: 0;
    font-size: var(--text-lg);
    font-weight: 650;
    letter-spacing: -0.01em;
  }

  .close {
    padding: var(--space-1) var(--space-2);
    border: 1px solid var(--chrome-border);
    border-radius: var(--radius);
    background: var(--chrome-bg);
    font-size: var(--text-sm);
    color: var(--ink-soft);
    transition:
      background 120ms ease,
      border-color 120ms ease;
  }

  .close:hover {
    background: var(--chrome-panel);
    border-color: var(--ink-faint);
  }

  /* §2 — the headline. One step quieter than the cluster hero in HeadlineMetric
     (2.25rem vs 2.75rem): this is one node's view, not the cluster's. */
  .convergence {
    display: grid;
    grid-template-columns: 1fr auto;
    align-items: end;
    gap: var(--space-2);
  }

  .headline {
    display: flex;
    flex-direction: column;
    gap: 2px;
  }

  .hl-label {
    font-size: var(--text-xs);
    font-weight: 650;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: var(--ink-soft);
  }

  .hl-value {
    font-size: 2.25rem;
    font-weight: 350;
    line-height: 1;
    color: var(--ink);
    transition: color 0.4s ease;
  }

  .hl-value.over {
    color: var(--signal-reject);
  }

  .hl-refs {
    font-size: var(--text-sm);
    color: var(--ink-faint);
  }

  .dot-sep {
    color: var(--chrome-border);
  }

  .badges {
    display: flex;
    flex-direction: column;
    align-items: flex-end;
    gap: var(--space-1);
  }

  .badge {
    font-size: var(--text-sm);
    font-weight: 600;
    padding: var(--space-1) var(--space-2);
    border-radius: var(--radius);
    white-space: nowrap;
  }

  .badge.ok {
    color: var(--signal-converged);
    background: color-mix(in srgb, var(--signal-converged) 12%, transparent);
  }

  .badge.dirty {
    color: var(--signal-dirty);
    background: color-mix(in srgb, var(--signal-dirty) 14%, transparent);
  }

  .badge.muted {
    color: var(--ink-soft);
    background: var(--chrome-bg);
    font-weight: 500;
  }

  .badge.inline {
    margin-left: var(--space-1);
    padding: 0 var(--space-1);
    font-size: var(--text-xs);
  }

  /* The over-limit pill reuses the reject vocabulary — word + colour, paired. */
  .pill {
    font-size: var(--text-sm);
    font-weight: 600;
    color: var(--signal-reject);
    background: color-mix(in srgb, var(--signal-reject) 12%, transparent);
    padding: var(--space-1) var(--space-2);
    border-radius: var(--radius);
  }

  .actions {
    display: flex;
    align-items: center;
    gap: var(--space-2);
  }

  .send {
    padding: var(--space-2) var(--space-3);
    border: 1px solid var(--ink);
    border-radius: var(--radius);
    background: var(--ink);
    color: var(--chrome-panel);
    font-size: var(--text-sm);
    transition: background 120ms ease;
  }

  .send:hover {
    background: var(--ink-hover);
  }

  .hint {
    font-size: var(--text-sm);
    color: var(--ink-faint);
  }

  /* The shared section idiom: a quiet small-caps heading over a hairline rule.
     One modular step, no per-row boxes. */
  .section {
    display: flex;
    flex-direction: column;
    gap: var(--space-2);
  }

  .section-head {
    margin: 0;
    padding-bottom: var(--space-1);
    border-bottom: 1px solid var(--chrome-border);
    font-size: var(--text-sm);
    font-weight: 650;
    color: var(--ink-soft);
  }

  /* §5 — I/O rows: label · meter/value, aligned. */
  .io {
    display: flex;
    flex-direction: column;
    gap: var(--space-2);
  }

  .io-row {
    display: grid;
    grid-template-columns: 7rem 1fr auto;
    align-items: center;
    gap: var(--space-2);
    min-height: 1.5rem;
    font-size: var(--text-sm);
  }

  .io-label {
    color: var(--ink-soft);
  }

  .meter {
    height: 8px;
    border-radius: 4px;
    background: var(--chrome-bg);
    overflow: hidden;
  }

  .meter-fill {
    height: 100%;
    border-radius: 4px;
    background: var(--node-fill);
    transition: width 220ms ease;
  }

  @media (prefers-reduced-motion: reduce) {
    .meter-fill {
      transition: none;
    }
  }

  .io-idle {
    color: var(--ink-faint);
    font-style: italic;
  }

  .io-value {
    color: var(--ink);
    text-align: right;
  }

  .io-value .sep {
    color: var(--ink-faint);
  }

  .rings {
    grid-column: 2 / -1;
    display: flex;
    gap: var(--space-3);
  }

  .ring {
    color: var(--ink-soft);
  }

  .ring .numeric {
    color: var(--ink);
    font-weight: 600;
  }

  .rejects {
    grid-column: 2 / -1;
  }

  .rejects.calm {
    color: var(--ink-soft);
  }

  .rejects.bad {
    color: var(--signal-reject);
    font-weight: 600;
  }

  .warn-glyph {
    margin-right: var(--space-1);
  }

  /* §7 — peer table: low-chrome, capped inner scroll with a sticky header so a
     large membership never pushes §8 off-screen. */
  .peer-summary {
    margin: 0;
    font-size: var(--text-sm);
    color: var(--ink-soft);
  }

  .peer-summary .numeric {
    color: var(--ink);
    font-weight: 600;
  }

  .peer-empty {
    margin: 0;
    font-size: var(--text-sm);
    font-style: italic;
    color: var(--ink-faint);
  }

  .peer-scroll {
    max-height: 12rem;
    overflow-y: auto;
  }

  .peers {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--text-sm);
  }

  .peers th {
    position: sticky;
    top: 0;
    background: var(--chrome-panel);
    text-align: left;
    font-weight: 600;
    color: var(--ink-faint);
    padding: var(--space-1) var(--space-2);
    border-bottom: 1px solid var(--chrome-border);
  }

  .peers td {
    padding: var(--space-1) var(--space-2);
    border-bottom: 1px solid var(--chrome-bg);
    color: var(--ink);
  }

  .peers td.unresolved {
    font-style: italic;
    color: var(--ink-faint);
  }

  .gossip-id {
    color: var(--ink-soft);
  }

  .state.known {
    color: var(--signal-converged);
  }

  .state.pending {
    color: var(--signal-dirty);
  }

  /* §8 — identity footer: the quietest element, collapsed by default. */
  .identity {
    font-size: var(--text-sm);
  }

  .identity summary {
    cursor: pointer;
    color: var(--ink-soft);
    font-weight: 600;
  }

  .id-preview {
    margin-left: var(--space-1);
    font-weight: 400;
    color: var(--ink-faint);
  }

  .id-list {
    margin: var(--space-2) 0 0;
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
  }

  .id-row {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: var(--space-2);
  }

  .id-row dt {
    color: var(--ink-soft);
  }

  .id-row dd {
    margin: 0;
    color: var(--ink);
    word-break: break-all;
    text-align: right;
  }
</style>
