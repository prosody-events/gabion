<script lang="ts">
  import { onMount, untrack } from 'svelte';
  import type { ChartHistory } from '../charts/history';
  import { SLOTS, slotIndex } from '../charts/history';
  import {
    BAR_PITCH,
    GUTTER,
    Labels,
    R1_HEIGHT,
    begin,
    binSlots,
    drawBehind,
    drawBins,
    drawRequests,
    drawShare,
    drawWindow,
    firstBar,
    lHeight,
    palette,
    plotWidth,
    r2Height,
    sHeight,
    type Lanes,
    type References,
  } from '../charts/draw';
  import { behindLine, binLine, requestLine, shareLine, windowLine } from '../charts/readout';
  import { fmtDuration, fmtPercent, zoneLabel } from '../format';
  import type { ChartId } from '../presets';
  import type { RequestLog } from '../requests';
  import type { ClusterState } from '../sim/types';
  import { fontsReady, type Theme } from '../theme';

  // The detail pane's charts, in one fixed order (R1, R2, L, S), each present
  // when the scenario lists it: the selected node's window by bucket against
  // the limit, the requests admitted and rejected, who is behind and for how
  // long, and the share of data bytes that crossed a zone. The time charts share
  // one window of virtual time and one x axis, under the last of them. A
  // readout line under the stack names the item under the cursor or a focused
  // chart's arrow keys, and "Show the charts as tables" lists the window and the requests.
  let {
    cluster,
    history,
    version,
    sends,
    limit,
    charts,
    onScale,
    timeBins,
    bucketMs,
    selected,
    theme,
  }: {
    cluster: ClusterState;
    history: ChartHistory;
    /** Ticks with each sample: the one signal the charts redraw on. */
    version: number;
    sends: RequestLog;
    limit: number;
    /** The charts the scenario lists. */
    charts: readonly ChartId[];
    /** Whether the limit is drawn on the vessels; R1 shows only then. */
    onScale: boolean;
    /** Whether R2 bins requests in time (a scenario with a feed) rather than
     *  drawing the reader's Sends in order. */
    timeBins: boolean;
    bucketMs: number;
    selected: number;
    theme: Theme;
  } = $props();

  const ORDER: readonly ChartId[] = ['window', 'requests', 'behind', 'share'];
  const TITLES: Record<Exclude<ChartId, 'window'>, string> = {
    requests: 'Requests',
    behind: 'Who is behind, and for how long',
    share: 'Share of data bytes that crossed a zone',
  };

  let width = $state(0);
  const pal = $derived(palette(theme));
  const canvases: Partial<Record<ChartId, HTMLCanvasElement>> = $state({});
  /** The item the readout names: a chart and its index there. */
  let active: { id: ChartId; at: number } | null = $state(null);
  let tableOpen = $state(false);
  /** Ticks once a second: the charts' spoken labels follow it, not every
   *  frame. */
  let second = $state(0);
  /** Whether the page's faces have loaded (or failed): the labels are
   *  measured again in them. */
  let faces = $state(false);
  onMount(() => {
    void fontsReady().then(() => (faces = true));
    const timer = window.setInterval(() => (second += 1), 1000);
    return () => window.clearInterval(timer);
  });

  const node = $derived(cluster.nodes.find((n) => n.id === selected) ?? null);
  const row = $derived(node === null ? -1 : cluster.nodes.indexOf(node));
  const buckets = $derived(cluster.oracle_buckets.length);
  const sent = $derived(cluster.admitted_hits + cluster.rejected_hits);
  const zones = $derived(cluster.zones.filter((z) => z.live > 0).length);
  /** The rejection standing on R1's current bucket: the selected node's last
   *  Send, when it was refused in this bucket. */
  const rejectedNow = $derived.by(() => {
    void version;
    const last = sends.length - 1;
    return last >= 0 && sends.node(last) === selected && sends.epoch(last) === cluster.bucket_epoch_now ? sends.rejected(last) : 0;
  });

  const shareNow = $derived.by(() => {
    void version;
    return history.shareAt(history.head);
  });

  /** The charts shown, in the fixed order, and which are empty: an empty chart
   *  prints its sentence instead of a plot. */
  const shown = $derived(ORDER.filter((id) => charts.includes(id) && (id !== 'window' || (onScale && node !== null))));
  const empty = $derived.by((): Partial<Record<ChartId, string>> => {
    void version;
    return {
      requests: (timeBins ? sent === 0 : sends.length === 0) ? 'No requests yet · each Send adds a bar here' : undefined,
      share:
        cluster.zones.length < 2
          ? 'No bytes cross a zone here: every node is in one zone'
          : cluster.admitted_hits === 0
            ? 'No hits sent yet · the line starts once hits spread'
            : undefined,
    };
  });
  /** The time chart the shared x axis sits under: the last one drawn. */
  const axisUnder = $derived(
    shown.filter((id) => empty[id] === undefined && (id === 'behind' || id === 'share' || (id === 'requests' && timeBins))).at(-1) ?? null,
  );

  // L's rows in (zone, id) order, rebuilt only when the membership changes.
  let lanes: Lanes = { nodes: [], zoneNames: [] };
  function lanesOf(c: ClusterState): Lanes {
    const same =
      lanes.nodes.length === c.nodes.length &&
      c.nodes.every((n) => lanes.nodes.some((m) => m.id === n.id && m.zone === n.zone)) &&
      lanes.zoneNames.length === c.zones.length;
    if (!same) {
      lanes = {
        nodes: c.nodes.map((n) => ({ id: n.id, zone: n.zone })).sort((a, b) => a.zone - b.zone || a.id - b.id),
        zoneNames: c.zones.map((z) => zoneLabel(z.name)),
      };
    }
    return lanes;
  }
  const lanesNow = $derived(lanesOf(cluster));

  /** S's two predictions from the placement: with zones a change crosses
   *  Z − 1 of the N − 1 links; with none, N − n_z of them, averaged over the
   *  nodes. Null below two nodes. */
  const refs = $derived.by((): References | null => {
    const n = cluster.nodes.length;
    if (n < 2) return null;
    let nozone = 0;
    for (const m of cluster.nodes) nozone += (n - cluster.zones[m.zone].live) / (n - 1);
    return { zones: (zones - 1) / (n - 1), nozone: nozone / n };
  });

  function heightOf(id: ChartId): number {
    const axis = axisUnder === id;
    if (id === 'window') return R1_HEIGHT;
    if (id === 'requests') return r2Height(!timeBins, axis);
    if (id === 'behind') return lHeight(lanesNow.nodes, axis);
    return sHeight(axis);
  }

  function draw(id: ChartId): void {
    const canvas = canvases[id];
    if (canvas === undefined || width <= 0) return;
    const height = heightOf(id);
    const ctx = begin(canvas, width, height);
    if (ctx === null) return;
    const labels = new Labels();
    const at = active?.id === id ? active.at : null;
    const now = cluster.virtual_ms;
    const axis = axisUnder === id;
    if (id === 'window') {
      drawWindow(ctx, pal, labels, width, { window: cluster.window_buckets, row, oracle: cluster.oracle_buckets, limit, rejected: rejectedNow, windowMs: history.windowMs }, at);
      canvas.dataset.columns = cluster.window_buckets.slice(row * buckets, (row + 1) * buckets).join(',');
    } else if (id === 'requests') {
      if (timeBins) drawBins(ctx, pal, labels, width, history, now, axis, at);
      else drawRequests(ctx, pal, labels, width, sends, at);
    } else if (id === 'behind') {
      drawBehind(ctx, pal, labels, width, history, lanesNow, now, axis, at);
      canvas.dataset.rows = lanesNow.nodes.map((n) => n.id).join(',');
      canvas.dataset.marked = lanesNow.nodes
        .map((n) => {
          const behind = history.behind(n.id);
          let marked = 0;
          if (behind !== undefined) for (let i = 0; i < SLOTS; i++) if (!Number.isNaN(behind[i])) marked += 1;
          return marked;
        })
        .join(',');
    } else {
      drawShare(ctx, pal, labels, width, history, refs, now, axis, at);
    }
    canvas.dataset.labelBoxes = JSON.stringify(labels.boxes);
  }

  $effect(() => {
    void version;
    void width;
    void pal;
    void active;
    void axisUnder;
    void faces;
    for (const id of shown) if (empty[id] === undefined) draw(id);
  });

  // -- The readout and the keyboard --------------------------------------

  /** The last index the arrow keys reach on chart `id`, and the first. */
  function range(id: ChartId): [number, number] {
    if (id === 'window') return [0, buckets - 1];
    if (id === 'requests' && !timeBins) return [firstBar(sends, width), sends.length - 1];
    return [0, SLOTS - 1];
  }

  /** The index under `x` px across chart `id`'s canvas. */
  function indexAt(id: ChartId, x: number): number {
    const pw = plotWidth(width);
    const [lo, hi] = range(id);
    let i: number;
    if (id === 'window') i = Math.floor(((x - GUTTER) / pw) * buckets);
    else if (id === 'requests' && !timeBins) i = lo + Math.floor((x - GUTTER) / BAR_PITCH);
    else i = Math.floor(((x - GUTTER) / pw) * SLOTS);
    return Math.min(Math.max(i, lo), hi);
  }

  function onPointer(id: ChartId, event: PointerEvent): void {
    active = { id, at: indexAt(id, event.offsetX) };
  }

  function onKey(id: ChartId, event: KeyboardEvent): void {
    const [lo, hi] = range(id);
    const big = id === 'behind' || id === 'share' || (id === 'requests' && timeBins) ? SLOTS / 10 : 1;
    const at = active?.id === id ? active.at : hi;
    const step = event.shiftKey ? big : 1;
    const next =
      event.key === 'ArrowRight' ? at + step : event.key === 'ArrowLeft' ? at - step : event.key === 'Home' ? lo : event.key === 'End' ? hi : null;
    if (next === null) return;
    event.preventDefault();
    active = { id, at: Math.min(Math.max(next, lo), hi) };
  }

  /** The readout's sentence for the active item. */
  const readout = $derived.by((): string => {
    void version;
    if (active === null || node === null) return '';
    const { id, at } = active;
    const ageOf = (c: number): number => (SLOTS - 1 - c) * history.slotMs;
    const j = history.head - SLOTS + 1 + at;
    if (id === 'window') {
      return windowLine(at, buckets, bucketMs, node.id, cluster.window_buckets[row * buckets + at] ?? 0, cluster.oracle_buckets[at] ?? 0, node.window_total, limit);
    }
    if (id === 'requests') {
      if (!timeBins) return at < sends.length ? requestLine(at + 1, sends.node(at), sends.admitted(at), sends.rejected(at)) : '';
      const per = binSlots(history);
      const b = Math.floor(j / per) * per;
      let a = 0;
      let r = 0;
      for (let s = Math.max(b, history.head - SLOTS + 1); s < b + per && s <= history.head; s++) {
        a += history.admitted[slotIndex(s)];
        r += history.rejected[slotIndex(s)];
      }
      return binLine((history.head - b) * history.slotMs, a, r);
    }
    if (id === 'behind') {
      const behind: [number, number][] = [];
      for (const n of lanesNow.nodes) {
        const v = history.behind(n.id)?.[slotIndex(j)];
        if (v !== undefined && !Number.isNaN(v)) behind.push([n.id, v]);
      }
      return behindLine(ageOf(at), behind);
    }
    return shareLine(ageOf(at), history.shareAt(j));
  });

  /** Each chart's spoken sentence, refreshed at most once a second. */
  const spoken = $derived.by((): Record<ChartId, string> => {
    void second;
    void shown;
    return untrack(() => {
      const last = sends.length - 1;
      const lastSend =
        last < 0
          ? ''
          : `; the last request, ${sends.admitted(last) + sends.rejected(last)} at node ${sends.node(last)}, ${sends.rejected(last) === 0 ? 'admitted' : sends.admitted(last) === 0 ? 'rejected' : 'partly rejected'}`;
      const behindNow = cluster.nodes.filter((n) => n.lag.kind !== 'CaughtUp').length;
      const share = shareNow;
      return {
        window: `Window by bucket: ${node?.window_total ?? 0} of the ${limit} limit in node ${selected}'s window${lastSend}`,
        requests: `Requests: ${cluster.admitted_hits} admitted and ${cluster.rejected_hits} rejected in this run`,
        behind: `Who is behind: ${behindNow} of ${cluster.nodes.length} nodes behind now`,
        share: `Share of data bytes that crossed a zone: ${share === null ? 'none sent' : fmtPercent(share)} over the last ${fmtDuration(history.windowMs)}`,
      };
    });
  });

  /** The selected node's window, bucket by bucket, for the table. */
  const tableRows = $derived.by(() => {
    void version;
    if (!tableOpen || row < 0) return [];
    return cluster.oracle_buckets.map((truth, k) => ({
      age: (buckets - 1 - k) * bucketMs,
      own: cluster.window_buckets[row * buckets + k] ?? 0,
      truth,
    }));
  });
  const tableSends = $derived.by(() => {
    void version;
    if (!tableOpen) return [];
    return Array.from({ length: sends.length }, (_, i) => ({ node: sends.node(i), admitted: sends.admitted(i), rejected: sends.rejected(i) }));
  });
</script>

<div class="charts" bind:clientWidth={width}>
  {#each shown as id (id)}
    <figure class="chart" data-chart={id} data-ref-zones={id === 'share' && refs !== null ? refs.zones : undefined} data-ref-nozone={id === 'share' && refs !== null ? refs.nozone : undefined}>
      <h3>
        {#if id === 'window'}node <span data-field="r1-node">{selected}</span>’s window against the limit{:else}{TITLES[id]}{/if}
      </h3>
      <p class="caption">
        {#if id === 'window'}
          <b data-field="r1-total">{node?.window_total ?? 0}</b> of the <b data-field="limit">{limit}</b> limit in node
          <b data-field="node">{selected}</b>’s window
        {:else if id === 'requests'}
          <b data-field="admitted">{cluster.admitted_hits}</b> admitted ·
          <b class="bad" data-field="rejected">{cluster.rejected_hits}</b> rejected, in this run
        {:else if id === 'behind'}
          a mark while a node lacks a hit the cluster has counted
        {:else}
          <b data-field="share">{shareNow === null ? '—' : fmtPercent(shareNow)}</b> over the last
          <b data-field="window">{fmtDuration(history.windowMs)}</b>
        {/if}
      </p>
      {#if empty[id] !== undefined}
        <p class="empty">{empty[id]}</p>
      {:else}
        <canvas
          bind:this={canvases[id]}
          style="height: {heightOf(id)}px"
          tabindex="0"
          role="slider"
          aria-label={spoken[id]}
          aria-valuemin={range(id)[0]}
          aria-valuemax={range(id)[1]}
          aria-valuenow={active?.id === id ? active.at : range(id)[1]}
          aria-valuetext={active?.id === id ? readout : spoken[id]}
          onpointermove={(e) => onPointer(id, e)}
          onpointerleave={() => (active = null)}
          onkeydown={(e) => onKey(id, e)}
          onfocus={() => (active = { id, at: range(id)[1] })}
          onblur={() => (active = null)}
        ></canvas>
      {/if}
      {#if id === 'share' && empty.share === undefined}
        <p class="note">
          With zones, each change crosses <span data-field="z-links">Z − 1</span> of the
          <span data-field="n-links">N − 1</span> links, and the share sits on that line.
        </p>
      {/if}
    </figure>
  {/each}
  <p class="chart-readout" data-field="readout">{readout}</p>
  <details class="table" bind:open={tableOpen}>
    <summary>Show the charts as tables</summary>
    {#if tableOpen}
      {#if row >= 0}
        <table>
          <caption>node <span data-field="node">{selected}</span>’s window, by bucket</caption>
          <thead><tr><th scope="col">bucket</th><th scope="col">node <span data-field="node">{selected}</span></th><th scope="col">cluster</th></tr></thead>
          <tbody>
            {#each tableRows as b, k (k)}
              <tr><td data-field="age">{b.age === 0 ? 'current' : `${b.age / 1000} s ago`}</td><td data-field="own">{b.own}</td><td data-field="truth">{b.truth}</td></tr>
            {/each}
          </tbody>
        </table>
      {/if}
      {#if tableSends.length > 0}
        <table>
          <caption>Your Sends, oldest first</caption>
          <thead><tr><th scope="col">node</th><th scope="col">admitted</th><th scope="col">rejected</th></tr></thead>
          <tbody>
            {#each tableSends as s, i (i)}
              <tr><td data-field="node">{s.node}</td><td data-field="admitted">{s.admitted}</td><td data-field="rejected">{s.rejected}</td></tr>
            {/each}
          </tbody>
        </table>
      {/if}
    {/if}
  </details>
</div>

<style>
  .charts {
    display: flex;
    flex-direction: column;
    gap: var(--gap-md);
  }

  .chart {
    margin: 0;
    display: flex;
    flex-direction: column;
  }

  h3 {
    margin: 0;
    font-size: var(--font-size-caption);
    line-height: 20px;
    font-weight: 600;
    color: var(--color-fg);
  }

  .caption,
  .note,
  .empty {
    margin: 0 0 var(--gap-sm);
    font-size: var(--font-size-caption);
    line-height: 20px;
    color: var(--color-fg);
  }

  .caption b {
    font-weight: 600;
  }

  .caption .bad {
    color: var(--color-bad);
  }

  .note {
    margin: var(--gap-xs) 0 0;
    font-size: var(--font-size-caption-sm);
    line-height: 16px;
    color: var(--color-fg-muted);
  }

  canvas {
    display: block;
    width: 100%;
  }

  canvas:focus-visible {
    outline: 2px solid var(--color-fg);
    outline-offset: 2px;
  }

  .chart-readout {
    margin: 0;
    min-height: 20px;
    font-size: var(--font-size-caption);
    line-height: 20px;
    color: var(--color-fg);
  }

  .table summary {
    cursor: pointer;
    font-size: var(--font-size-caption);
    line-height: 24px;
    color: var(--color-fg);
  }

  table {
    margin-top: var(--gap-sm);
    border-collapse: collapse;
    font-size: var(--font-size-caption);
    line-height: 20px;
  }

  caption {
    text-align: left;
    color: var(--color-fg-muted);
  }

  th,
  td {
    padding: 0 var(--gap-sm) 0 0;
    text-align: right;
    font-weight: 400;
  }

  th {
    color: var(--color-fg-muted);
  }
</style>
