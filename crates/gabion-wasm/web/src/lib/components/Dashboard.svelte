<script lang="ts">
  import type uPlot from 'uplot';
  import type { ClusterState } from '../sim/types';
  import type { ChartHistory } from '../charts/history';
  import Chart from '../charts/Chart.svelte';
  import { aggregateLimitOptions, disagreementOptions, fanOptions } from '../charts/options';

  // The abstract rung: the convergence story as charts. Fed the live `cluster`
  // (for the pinned headline) plus the rolling `history` and a `version` counter
  // that ticks on every new sample — the single signal the charts redraw on.
  // `showLimit` adds the Aggregate-vs-Limit panel: it is only legible when the
  // active scenario drives the aggregate toward `limit` (the overload preset).
  let {
    cluster,
    history,
    version,
    limit,
    showLimit,
  }: {
    cluster: ClusterState | null;
    history: ChartHistory;
    version: number;
    limit: number;
    showLimit: boolean;
  } = $props();

  // The fan has one line per retained column — live nodes plus any departed one
  // whose history is still in the window — which is `history.nodeCount`, not the
  // live count. Reading `version` makes it reactive; its *value* changes only on
  // a join or leave, so the fan options (and the chart rebuild they drive via
  // `Chart.svelte`) fire only then, not every sample. The continuous time axis
  // means that rebuild redraws the same window, it doesn't reset it.
  const fanSeriesCount = $derived.by(() => {
    void version;
    return history.nodeCount;
  });

  // Option shapes rebuild only when their structural input changes (the fan's
  // series count, the limit line's height) — not per sample.
  const fanOpts = $derived(fanOptions(fanSeriesCount));
  const disagreementOpts = disagreementOptions();
  const aggregateOpts = $derived(showLimit ? aggregateLimitOptions(limit) : null);

  // Column-major data, re-wrapped each sample. The inner arrays are the same
  // references `ChartHistory` mutates; `version` is what makes the charts read.
  const fanData = $derived.by((): uPlot.AlignedData => {
    void version;
    return [history.times, ...history.nodes, history.oracle];
  });
  const disagreementData = $derived.by((): uPlot.AlignedData => {
    void version;
    return [history.times, history.disagreement];
  });
  // The aggregate the cluster converges on is the ground-truth oracle — the same
  // total the fan chases, reframed here against the rule limit.
  const aggregateData = $derived.by((): uPlot.AlignedData => {
    void version;
    return [history.times, history.oracle];
  });

  // The always-pinned headline: the live spread between the most- and
  // least-informed node. Reads straight off the cluster so it tracks the stage.
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

<aside class="dashboard" class:with-limit={showLimit} aria-label="Convergence dashboard">
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

  <div class="panel hero">
    <Chart label="Convergence — each node's view vs. true total" opts={fanOpts} data={fanData} {version} />
  </div>
  {#if showLimit && aggregateOpts !== null}
    <div class="panel">
      <Chart label="Aggregate vs. limit" opts={aggregateOpts} data={aggregateData} {version} />
    </div>
  {/if}
  <div class="panel">
    <Chart label="Disagreement decay" opts={disagreementOpts} data={disagreementData} {version} />
  </div>
</aside>

<style>
  .dashboard {
    display: grid;
    grid-template-rows: auto 2fr 1fr;
    gap: var(--space-3);
    height: 100%;
    min-height: 0;
    padding: var(--space-3);
    background: var(--chrome-panel);
    border-left: 1px solid var(--chrome-border);
    overflow: hidden;
  }

  /* The overload scenario adds the Aggregate-vs-Limit panel between the hero
     fan and the disagreement strip; trim the hero so three plots fit. (Only
     reshapes on a preset switch, which fully remounts the dashboard.) */
  .dashboard.with-limit {
    grid-template-rows: auto 1.6fr 1fr 1fr;
  }

  .headline {
    display: grid;
    grid-template-columns: auto 1fr;
    grid-template-areas: 'label badge' 'value badge';
    align-items: baseline;
    column-gap: var(--space-2);
  }

  .headline-label {
    grid-area: label;
    font-size: var(--text-xs);
    font-weight: 650;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: var(--ink-soft);
  }

  .headline-value {
    grid-area: value;
    font-size: 2.75rem;
    font-weight: 350;
    line-height: 1;
    color: var(--signal-dirty);
    transition: color 0.4s ease;
  }

  .headline.converged .headline-value {
    color: var(--signal-converged);
  }

  .badge {
    grid-area: badge;
    align-self: center;
    justify-self: end;
    font-size: var(--text-sm);
    font-weight: 600;
    color: var(--signal-converged);
    background: color-mix(in srgb, var(--signal-converged) 12%, transparent);
    padding: var(--space-1) var(--space-2);
    border-radius: var(--radius);
  }

  .badge.muted {
    color: var(--ink-soft);
    background: var(--chrome-bg);
    font-weight: 500;
  }

  .panel {
    min-height: 0;
    display: flex;
  }

  .panel :global(.chart) {
    flex: 1;
    min-width: 0;
  }
</style>
