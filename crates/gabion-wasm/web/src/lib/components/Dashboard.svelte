<script lang="ts">
  import type uPlot from 'uplot';
  import type { ChartHistory, Mark } from '../charts/history';
  import Chart from '../charts/Chart.svelte';
  import {
    aggregateLimitOptions,
    agreementPanel,
    shareOptions,
    spreadPanel,
    X_AXIS_ROOM,
    type References,
    type YRange,
  } from '../charts/options';
  import { fmtCount, fmtDuration, fmtPercent, zoneLabel } from '../format';
  import type { ClusterState } from '../sim/types';
  import type { Theme } from '../theme';

  // The abstract rung: the convergence story as charts, shown while no node is
  // selected. Fed the rolling `history` and a `version` counter that ticks on
  // every new sample, the single signal the charts redraw on. Chart 1 is the
  // study's central result made visible in one run: the share of data bytes
  // that crossed zones, over what the design predicts with zones and without.
  // Charts 2 and 3 are small multiples, one panel per zone with members, in zone
  // order; chart 4 (aggregate vs limit) is shown only under the overload preset.
  let {
    cluster,
    history,
    version,
    limit,
    showLimit,
    theme,
    hotId,
  }: {
    cluster: ClusterState;
    history: ChartHistory;
    version: number;
    limit: number;
    showLimit: boolean;
    theme: Theme;
    /** The node hovered or focused, whose own line joins its zone's panel. */
    hotId: number | null;
  } = $props();

  // Fixed plot heights, px: charts never reflow as data arrives, and the rail
  // body scrolls. The last panel of a stack is taller by its x axis, so its
  // plot is as tall as the others'. Chart 4's plot is at least 60 px, where its
  // 0 to 2 × limit axis always labels two values.
  const SHARE_HEIGHT = 232;
  const LIMIT_HEIGHT = 180;
  const PANEL_HEIGHT = 84;

  // Which zones have members, as a string so the panels rebuild only when that
  // changes and not with every sample.
  const liveKey = $derived(cluster.zones.map((z) => (z.live > 0 ? '1' : '0')).join(''));
  const panels = $derived(
    [...liveKey].flatMap((c, zone) => (c === '1' ? [zone] : [])),
  );
  const names = $derived(cluster.zones.map((z) => zoneLabel(z.name)));

  /** The two predictions chart 1 draws, from this snapshot's placement and the
   *  data bytes each origin sent over the window `start` opens. `null` below two
   *  live nodes. With zones, an origin's change reaches N − 1 nodes, Z − 1 of
   *  them across zones. With no zone it reaches all N − 1 directly, N − n_z of
   *  them across zones, weighted by each origin's own data bytes in the window
   *  (equally while none were sent). */
  function predict(c: ClusterState, start: Mark): References | null {
    const n = c.nodes.length;
    if (n < 2) return null;
    const zones = c.zones.filter((z) => z.live > 0).length;
    let weight = 0;
    let weighted = 0;
    let plain = 0;
    for (const node of c.nodes) {
      const across = (n - c.zones[node.zone].live) / (n - 1);
      const bytes = node.own_data_bytes - (start.own.get(node.id) ?? 0);
      weight += bytes;
      weighted += bytes * across;
      plain += across;
    }
    return { zones: (zones - 1) / (n - 1), nozone: weight > 0 ? weighted / weight : plain / n };
  }

  const refs = $derived.by(() => {
    void version;
    return predict(cluster, history.windowStart(cluster.virtual_ms));
  });
  const share = $derived.by(() => {
    void version;
    return history.share.at(-1) ?? null;
  });
  const hasShare = $derived.by(() => {
    void version;
    return history.share.some((v) => v !== null);
  });
  const controlShare = $derived.by(() => {
    const { in_zone: a, cross_zone: b } = cluster.bytes_by_tier;
    const control = a.control + b.control;
    const all = control + a.data + b.data;
    return all > 0 ? control / all : 0;
  });

  // The fitted axes the small multiples share: the largest value any panel
  // shows, with headroom; rewritten with each sample before the charts read it.
  const agreeRange: YRange = { max: 1 };
  const spreadRange: YRange = { max: 1 };
  /** The axis top for `peak`: 10 % headroom, rounded up to 1, 2, 2.5 or 5 times
   *  a power of ten, uPlot's own tick steps, so the top is itself a step and
   *  even a short panel labels at least two values. */
  function top(peak: number): number {
    const raw = Math.max(peak, 1) * 1.1;
    const power = 10 ** Math.floor(Math.log10(raw));
    return power * ([1, 2, 2.5, 5].find((m) => m * power >= raw) ?? 10);
  }
  /** The largest non-null value in any of `columns`. */
  function peak(columns: readonly (readonly (number | null)[])[]): number {
    let max = 0;
    for (const column of columns) for (const v of column) if (v !== null && v > max) max = v;
    return max;
  }

  // The reference lines are drawn from the latest values, which the chart reads
  // at each draw; `data` derives after them so a redraw sees them.
  let latestRefs: References | null = null;
  const shareOpts = $derived(shareOptions(theme, () => latestRefs));
  const refAttrs = $derived({
    'data-ref-zones': refs === null ? undefined : String(refs.zones),
    'data-ref-nozone': refs === null ? undefined : String(refs.nozone),
  });

  const lastZone = $derived(panels.length - 1);
  const agreementOpts = $derived(
    panels.map((_, i) => agreementPanel(theme, i === 0, i === lastZone, agreeRange)),
  );
  const spreadOpts = $derived([
    ...panels.map(() => spreadPanel(theme, false, spreadRange)),
    spreadPanel(theme, true, spreadRange),
  ]);
  const aggregateOpts = $derived(showLimit ? aggregateLimitOptions(limit, theme) : null);

  /** One zone's agreement panel data: the true total, the band's edges and the
   *  hovered node's line when it is in this zone. */
  function agreementData(zone: number): uPlot.AlignedData {
    const own = cluster.nodes.find((n) => n.id === hotId && n.zone === zone);
    return [
      history.times,
      history.oracle,
      history.zoneMax[zone],
      history.zoneMin[zone],
      (own === undefined ? undefined : history.column(own.id)) ?? history.blank,
    ];
  }

  // The columns every chart draws, re-read with each sample: a rebuild gives the
  // history new arrays, which a chart must be handed again.
  const data = $derived.by(() => {
    void version;
    latestRefs = refs;
    return {
      share: [history.times, history.share] as uPlot.AlignedData,
      agreement: panels.map(agreementData),
      spread: [
        ...panels.map((zone): uPlot.AlignedData => [history.times, history.zoneSpread[zone]]),
        [history.times, history.disagreement] as uPlot.AlignedData,
      ],
      aggregate: [history.times, history.oracle] as uPlot.AlignedData,
    };
  });

  // What every chart redraws on. The shared axes are refitted here, before any
  // chart reads them to draw.
  const stamp = $derived.by(() => {
    agreeRange.max = top(peak([history.oracle, ...history.zoneMax]));
    spreadRange.max = top(peak([...history.zoneSpread, history.disagreement]));
    return `${version}/${hotId}`;
  });
  // The latest spread of each panel's zone and of the whole cluster, for the
  // panel titles. Reads `version` because the history is not reactive.
  const spreadNow = $derived.by(() => {
    void version;
    return {
      zones: panels.map((zone) => history.zoneSpread[zone]?.at(-1) ?? 0),
      cluster: history.disagreement.at(-1) ?? 0,
    };
  });
</script>

<div class="dashboard" aria-label="Convergence dashboard">
  <section class="block">
    <h3>Share of data bytes that crossed zones</h3>
    <p class="note">
      Control packets are left out; they were
      <span data-field="control-share">{fmtPercent(controlShare)}</span> of all bytes in this run.
    </p>
    {#if cluster.zones.length === 1}
      <p class="empty">No bytes crossed zones in this run: every node is in one zone</p>
    {:else if !hasShare}
      <p class="empty">No traffic yet: send a burst from the rail</p>
    {:else}
      <Chart
        opts={shareOpts}
        data={data.share}
        {stamp}
        height={SHARE_HEIGHT}
        attrs={refAttrs}
      >
        {#snippet caption()}
          <span data-field="share">{share === null ? '—' : fmtPercent(share)}</span>
          over the last <span data-field="window">{fmtDuration(history.windowMs)}</span>
        {/snippet}
      </Chart>
    {/if}
  </section>

  <section class="block">
    <h3>Agreement by zone</h3>
    <p class="note">
      Each band runs from the least to the most informed node in the zone, and closes on
      the dashed true total when the zone agrees.
    </p>
    {#each panels as zone, i (zone)}
      <Chart
        opts={agreementOpts[i]}
        data={data.agreement[i]}
        {stamp}
        {zone}
        height={PANEL_HEIGHT + (i === lastZone ? X_AXIS_ROOM : 0)}
      >
        {#snippet caption()}{names[zone]}{/snippet}
      </Chart>
    {/each}
  </section>

  <section class="block">
    <h3>Disagreement by zone</h3>
    <p class="note">
      The gap between the most and least informed node, in hits. A zone that lags after a
      partition shows in its own panel.
    </p>
    {#each panels as zone, i (zone)}
      <Chart
        opts={spreadOpts[i]}
        data={data.spread[i]}
        {stamp}
        {zone}
        height={PANEL_HEIGHT}
      >
        {#snippet caption()}{names[zone]} · <span data-field="spread">{fmtCount(spreadNow.zones[i])}</span>{/snippet}
      </Chart>
    {/each}
    <Chart
      opts={spreadOpts[panels.length]}
      data={data.spread[panels.length]}
      {stamp}
      height={PANEL_HEIGHT + X_AXIS_ROOM}
    >
      {#snippet caption()}whole cluster · <span data-field="spread">{fmtCount(spreadNow.cluster)}</span>{/snippet}
    </Chart>
  </section>

  {#if aggregateOpts !== null}
    <section class="block">
      <h3>Aggregate vs. limit</h3>
      <p class="note">
        The true total across the window climbs through the limit into the rejecting band.
      </p>
      <Chart
        opts={aggregateOpts}
        data={data.aggregate}
        {stamp}
        height={LIMIT_HEIGHT}
      >
        {#snippet caption()}true total{/snippet}
      </Chart>
    </section>
  {/if}
</div>

<style>
  .dashboard {
    display: flex;
    flex-direction: column;
    gap: var(--gap-lg);
  }

  .block {
    display: flex;
    flex-direction: column;
    gap: var(--gap-xs);
  }

  h3 {
    margin: 0;
    font-size: var(--font-size-caption);
    font-weight: 650;
    color: var(--color-fg);
  }

  .note,
  .empty {
    margin: 0 0 var(--gap-xs);
    max-width: 80ch;
    font-size: var(--font-size-caption-sm);
    color: var(--color-fg-muted);
  }

  .empty {
    font-size: var(--font-size-caption);
    color: var(--color-fg);
  }
</style>
