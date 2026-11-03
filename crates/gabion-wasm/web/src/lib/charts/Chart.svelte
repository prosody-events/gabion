<script lang="ts">
  import { onMount, untrack, type Snippet } from 'svelte';
  import uPlot from 'uplot';

  // A thin Svelte shell around one uPlot instance. uPlot is imperative, so the
  // component never re-renders it reactively: `stamp` (changed by the owner on
  // each new sample) drives `setData`, a debounced ResizeObserver drives
  // `setSize`, and `opts` changing rebuilds it. `data`'s inner arrays are
  // mutated in place by `ChartHistory`, so it must NOT be tracked: `stamp` is
  // the single redraw signal. The built chart is left on the plot element as
  // `uplot`, which the design audit reads to check every mark lies inside its
  // plot.
  let {
    opts,
    data,
    stamp,
    height,
    zone,
    attrs,
    caption,
    note,
  }: {
    opts: uPlot.Options;
    data: uPlot.AlignedData;
    /** Anything that changes whenever the data should be redrawn: the owner's
     *  sample counter and whatever else the drawing depends on. */
    stamp: unknown;
    /** The figure's height in px, fixed so charts never reflow as data arrives. */
    height: number;
    /** The zone a small-multiple panel shows, for the audit's order check. */
    zone?: number;
    /** Further attributes on the figure, such as published `data-*` values. */
    attrs?: Record<string, string | undefined>;
    caption: Snippet;
    note?: Snippet;
  } = $props();

  // How long a resize waits for the next one before redrawing (design rule 26).
  const RESIZE_DEBOUNCE_MS = 100;

  let host: HTMLDivElement;
  let chart: uPlot | null = null;

  function size(): { width: number; height: number } {
    const r = host.getBoundingClientRect();
    return { width: Math.max(Math.round(r.width), 1), height: Math.max(Math.round(r.height), 1) };
  }

  function build(): void {
    chart?.destroy();
    chart = new uPlot({ ...opts, ...size() }, data, host);
    Object.assign(host, { uplot: chart });
  }

  onMount(() => {
    let timer = 0;
    let measured = false;
    const observer = new ResizeObserver(() => {
      // The first measurement applies at once; later ones are debounced.
      if (!measured) {
        measured = true;
        chart?.setSize(size());
        return;
      }
      window.clearTimeout(timer);
      timer = window.setTimeout(() => chart?.setSize(size()), RESIZE_DEBOUNCE_MS);
    });
    observer.observe(host);
    return () => {
      window.clearTimeout(timer);
      observer.disconnect();
      chart?.destroy();
      chart = null;
    };
  });

  // Rebuild only when the option shape changes; reading `data` inside `build` is
  // untracked so a sample doesn't trigger a teardown.
  $effect(() => {
    void opts;
    untrack(build);
  });

  // Each new sample: hand uPlot the current columns. `data` is read untracked.
  $effect(() => {
    void stamp;
    untrack(() => chart?.setData(data));
  });
</script>

<figure class="chart" {...attrs} data-zone={zone} style="height: {height}px">
  <figcaption>{@render caption()}</figcaption>
  <div class="plot" bind:this={host}></div>
  {#if note}
    <p class="note">{@render note()}</p>
  {/if}
</figure>

<style>
  .chart {
    margin: 0;
    display: flex;
    flex-direction: column;
    min-height: 0;
  }

  figcaption {
    font-size: var(--font-size-caption-sm);
    font-weight: 650;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: var(--color-fg-muted);
    margin-bottom: var(--gap-xs);
  }

  .plot {
    flex: 1;
    min-height: 0;
  }

  .note {
    margin: var(--gap-xs) 0 0;
    max-width: 80ch;
    font-size: var(--font-size-caption-sm);
    color: var(--color-fg-muted);
  }
</style>
