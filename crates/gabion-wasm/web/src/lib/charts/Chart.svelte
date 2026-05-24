<script lang="ts">
  import { onMount, untrack } from 'svelte';
  import uPlot from 'uplot';

  // A thin Svelte shell around one uPlot instance. uPlot is imperative, so the
  // component never re-renders it reactively: `version` (bumped by the owner on
  // each new sample) drives `setData`, a ResizeObserver drives `setSize`, and
  // `opts` changing (e.g. the node count on reset) rebuilds it. `data`'s inner
  // arrays are mutated in place by `ChartHistory`, so it must NOT be tracked —
  // `version` is the single redraw signal.
  let {
    opts,
    data,
    version,
    label,
  }: { opts: uPlot.Options; data: uPlot.AlignedData; version: number; label: string } = $props();

  let host: HTMLDivElement;
  let chart: uPlot | null = null;

  function size(): { width: number; height: number } {
    const r = host.getBoundingClientRect();
    return { width: Math.max(Math.round(r.width), 1), height: Math.max(Math.round(r.height), 1) };
  }

  function build(): void {
    chart?.destroy();
    chart = new uPlot({ ...opts, ...size() }, data, host);
  }

  onMount(() => {
    const observer = new ResizeObserver(() => chart?.setSize(size()));
    observer.observe(host);
    return () => {
      observer.disconnect();
      chart?.destroy();
      chart = null;
    };
  });

  // Rebuild only when the option shape changes (node count on reset); reading
  // `data` inside `build` is untracked so a sample doesn't trigger a teardown.
  $effect(() => {
    void opts;
    untrack(build);
  });

  // Each new sample: hand uPlot the current columns. `data` is read untracked.
  $effect(() => {
    void version;
    untrack(() => chart?.setData(data));
  });
</script>

<figure class="chart">
  <figcaption>{label}</figcaption>
  <div class="plot" bind:this={host}></div>
</figure>

<style>
  .chart {
    margin: 0;
    display: flex;
    flex-direction: column;
    min-height: 0;
  }

  figcaption {
    font-size: var(--text-xs);
    font-weight: 650;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: var(--ink-soft);
    margin-bottom: var(--space-1);
  }

  /* uPlot draws axis ticks on the canvas, but the plot area inherits tabular
     figures for any DOM text uPlot adds (and documents the intent). */
  .plot {
    flex: 1;
    min-height: 0;
    font-variant-numeric: tabular-nums lining-nums;
  }
</style>
