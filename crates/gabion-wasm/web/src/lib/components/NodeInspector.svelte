<script lang="ts">
  import type { Snippet } from 'svelte';
  import type { NodeState } from '../sim/types';

  // The right rail's drill-in view: the selected node's own state, replacing the
  // charts dashboard while a node is selected (the pinned headline stays above
  // both). v1 surfaces the node's aggregate total, threshold fires, and peer
  // count, plus the re-homed "send a burst" gesture (the stage click now
  // *selects*; bursting moved here and to the rail). `children` is the slot the
  // Strata small-multiples fill — the per-bucket view of this node's window.
  let {
    node,
    burstHits,
    onSend,
    onClose,
    children,
  }: {
    node: NodeState;
    burstHits: number;
    onSend: (id: number) => void;
    onClose: () => void;
    children?: Snippet;
  } = $props();

  // The gossip peer-table size — how many other members this node currently
  // tracks. Reads straight off the snapshot so it follows joins and leaves.
  const peerCount = $derived(node.peers.length);
</script>

<section class="inspector" aria-label={`Node ${node.id} inspector`}>
  <header class="head">
    <h2>Node <span class="numeric">{node.id}</span></h2>
    <button class="close" type="button" onclick={onClose}>← Charts</button>
  </header>

  <!-- The node's own numbers, tabular so they don't jitter as they climb. -->
  <dl class="stats">
    <div class="stat">
      <dt>Cluster total</dt>
      <dd class="numeric">{node.aggregate_total}</dd>
    </div>
    <div class="stat">
      <dt>Threshold fires</dt>
      <dd class="numeric">{node.threshold_fires}</dd>
    </div>
    <div class="stat">
      <dt>Peers</dt>
      <dd class="numeric">{peerCount}</dd>
    </div>
  </dl>

  <div class="actions">
    <button class="send" type="button" onclick={() => onSend(node.id)}>Send burst</button>
    <span class="hint"><span class="numeric">{burstHits}</span> hits to this node</span>
  </div>

  {#if children}
    <div class="strata-slot">
      {@render children()}
    </div>
  {/if}
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

  /* A quiet back affordance — recognition over recall: it reads as "return to
     the charts", the thing the inspector replaced. */
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

  /* Three readouts as a definition list: the number dominates its label
     (visual hierarchy), each on its own row. */
  .stats {
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
    margin: 0;
  }

  .stat {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    padding: var(--space-1) var(--space-2);
    border: 1px solid var(--chrome-border);
    border-radius: var(--radius);
    background: var(--chrome-bg);
  }

  dt {
    font-size: var(--text-sm);
    color: var(--ink-soft);
  }

  dd {
    margin: 0;
    font-size: var(--text-lg);
    font-weight: 600;
    line-height: 1;
    color: var(--ink);
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

  /* Where the Strata small-multiples mount (Slice 5). Holds the remaining
     height and scrolls within itself if the window has many keys. */
  .strata-slot {
    flex: 1;
    min-height: 0;
  }
</style>
