<script lang="ts">
  import type uPlot from 'uplot';
  import type { ChartHistory } from '../charts/history';
  import Chart from '../charts/Chart.svelte';
  import { aggregateLimitOptions, disagreementOptions, fanOptions } from '../charts/options';

  // The abstract rung: the convergence story as charts. Fed the rolling
  // `history` and a `version` counter that ticks on every new sample — the
  // single signal the charts redraw on. The pinned headline metric lives above
  // this in `HeadlineMetric` (shared with the inspector), not here. `showLimit`
  // adds the Aggregate-vs-Limit panel: it is only legible when the active
  // scenario drives the aggregate toward `limit` (the overload preset).
  let {
    history,
    version,
    limit,
    showLimit,
  }: {
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
</script>

<div class="dashboard" class:with-limit={showLimit} aria-label="Convergence dashboard">
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
</div>

<style>
  .dashboard {
    display: grid;
    grid-template-rows: 2fr 1fr;
    gap: var(--space-3);
    height: 100%;
    min-height: 0;
    overflow: hidden;
  }

  /* The overload scenario adds the Aggregate-vs-Limit panel between the hero
     fan and the disagreement strip; trim the hero so three plots fit. (Only
     reshapes on a preset switch, which fully remounts the dashboard.) */
  .dashboard.with-limit {
    grid-template-rows: 1.6fr 1fr 1fr;
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
