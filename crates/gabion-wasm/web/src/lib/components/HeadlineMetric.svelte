<script lang="ts">
  import type { ClusterState } from '../sim/types';
  import type { ChartHistory } from '../charts/history';

  // The one always-pinned metric: the live spread between the most- and
  // least-informed node (max − min), the single most-legible realization of the
  // design's "one always-visible headline". Lives above *either* the charts
  // dashboard or the node inspector, so the convergence story stays in view
  // whichever the right rail is showing. Reads straight off the cluster so it
  // tracks the stage; `history` only feeds the latched "converged in N rounds".
  let {
    cluster,
    history,
    version,
  }: {
    cluster: ClusterState | null;
    history: ChartHistory;
    version: number;
  } = $props();

  const disagreement = $derived.by(() => {
    if (cluster === null || cluster.nodes.length === 0) return 0;
    let min = Number.POSITIVE_INFINITY;
    let max = 0;
    for (const node of cluster.nodes) {
      if (node.aggregate_total < min) min = node.aggregate_total;
      if (node.aggregate_total > max) max = node.aggregate_total;
    }
    return max - min;
  });
  const hasTotal = $derived(cluster !== null && cluster.oracle_total > 0);
  const converged = $derived(hasTotal && disagreement === 0);
  const convergedRound = $derived.by(() => {
    void version;
    return history.convergedRound();
  });
</script>

<div class="headline" class:converged>
  <span class="headline-label">Disagreement → 0</span>
  <span class="headline-value numeric">{disagreement}</span>
  {#if converged && convergedRound !== null}
    <span class="badge" role="status">
      ✓ converged in {convergedRound} {convergedRound === 1 ? 'round' : 'rounds'}
    </span>
  {:else if hasTotal}
    <span class="badge muted" role="status">spreading…</span>
  {/if}
</div>

<style>
  .headline {
    display: grid;
    grid-template-columns: auto 1fr;
    grid-template-areas: 'label badge' 'value badge';
    align-items: baseline;
    column-gap: var(--gap-sm);
  }

  .headline-label {
    grid-area: label;
    font-size: var(--font-size-caption-sm);
    font-weight: 650;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: var(--color-fg-muted);
  }

  .headline-value {
    grid-area: value;
    font-size: 2.75rem;
    font-weight: 350;
    line-height: 1;
    color: var(--color-warn);
    transition: color 0.4s ease;
  }

  .headline.converged .headline-value {
    color: var(--color-ok);
  }

  .badge {
    grid-area: badge;
    align-self: center;
    justify-self: end;
    font-size: var(--font-size-caption);
    font-weight: 600;
    color: var(--color-ok);
    background: color-mix(in srgb, var(--color-ok) 12%, transparent);
    padding: var(--gap-xs) var(--gap-sm);
    border-radius: var(--radius);
  }

  .badge.muted {
    color: var(--color-fg-muted);
    background: var(--color-bg);
    font-weight: 500;
  }
</style>
