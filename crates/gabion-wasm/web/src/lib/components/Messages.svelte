<script lang="ts">
  import { onDestroy, tick } from 'svelte';
  import { fmtCount, zoneAt } from '../format';
  import {
    MAX_ROWS,
    ROLE_WORD,
    TYPES,
    TYPE_WORD,
    asksAnswer,
    filterOf,
    fmtInstant,
    gist,
    headOf,
    markerWords,
    nodeName,
    riderWords,
    rowAt,
    type Head,
    type MessageDetail,
    type MessagesView,
    type Row,
    type Says,
  } from '../messages';
  import type { Sim } from '../sim/sim';
  import type { ZoneView } from '../sim/types';
  import ByType from './ByType.svelte';
  import MessageDetailView from './MessageDetail.svelte';
  import MessageMark from './MessageMark.svelte';

  // The Messages view of the detail pane: every message the cluster sent,
  // one row each, scoped by the selection, with By type below while nothing
  // is selected and the selected message's detail when one is. It never
  // fills a control and never opens itself. The capture in the engine counts
  // and filters; this reads a window of rows, on scroll, on a filter change,
  // and, while following the newest, when a batch lands.
  let {
    sim,
    selected,
    view = $bindable(),
    version,
    live,
    nodes,
    zones,
    cutWords,
    onPreview,
  }: {
    sim: Sim;
    /** The selected node: the node scope. */
    selected: number;
    view: MessagesView;
    /** Bumped each time a batch lands, so a following list reads again. */
    version: number;
    /** Whether the run is under live pacing: nothing is held there. */
    live: boolean;
    nodes: readonly { id: number; zone: number }[];
    zones: ZoneView[];
    /** The words for the cut a lost message's link was under, if any. */
    cutWords: (src: number, dst: number) => string | undefined;
    onPreview: (node: number | null) => void;
  } = $props();

  const ROW_PX = 24;
  const VISIBLE = 8;
  const WINDOW = Math.min(VISIBLE + 4, MAX_ROWS);

  const liveIds = $derived(new Set(nodes.map((n) => n.id)));
  const zoneOf = (id: number): string | undefined => {
    const n = nodes.find((m) => m.id === id);
    return n === undefined ? undefined : zoneAt(zones, n.zone);
  };
  const name = (id: number): string => nodeName(id, liveIds);
  const filter = $derived(filterOf(view, selected));
  const pair = $derived(filter.scope.kind === 'Pair' ? filter.scope.pair : null);

  let head = $state.raw<Head | null>(null);
  let rows = $state.raw<readonly Row[]>([]);
  let first = $state(0);
  let following = $state(true);
  let newer = $state(0);
  let detail = $state.raw<MessageDetail | null>(null);
  let stale = $state(false);
  let scroller = $state<HTMLDivElement>();
  let hovered = $state<number | null>(null);
  let width = $state(348);

  /** Below this content width the list drops its time column (the
   *  instant stays in each row's accessible name and the detail). */
  const NARROW = 300;
  /** The message column's width: the content less the columns before it and
   *  their 8 px gutters, as the grid lays them out. */
  const messageWidth = $derived(
    width - (width < NARROW ? 0 : 56) - (pair !== null ? 80 : filter.scope.kind === 'All' ? 136 : 108),
  );

  /** Read the window from ordinal `from`. */
  function read(from: number): void {
    const { rows: r, written } = sim.messageRows(Math.max(from, 0), WINDOW);
    const h = headOf(r);
    const out: Row[] = [];
    for (let i = 0; i < written; i++) out.push(rowAt(r, Math.max(from, 0), i));
    head = h;
    rows = out;
    first = Math.max(from, 0);
    if (detail !== null && !stale && detail.seq <= h.total - h.kept) stale = true;
  }

  /** Read the newest window and scroll to it. */
  function follow(): void {
    const len = sim.messageRows(0, 0).rows[2];
    const from = Math.max(len - VISIBLE, 0);
    read(from);
    newer = 0;
    // Scroll once the list has grown to hold the new rows.
    void tick().then(() => scrollTo(from));
  }

  /** Scroll to ordinal `from`, which the scroll handler then ignores. */
  let scrolling = false;
  function scrollTo(from: number): void {
    if (scroller === undefined || scroller.scrollTop === from * ROW_PX) return;
    scrolling = true;
    scroller.scrollTop = from * ROW_PX;
  }

  // A filter change rebuilds the view and follows the newest.
  let filterKey = '';
  $effect(() => {
    const key = JSON.stringify(filter);
    if (key === filterKey) return;
    filterKey = key;
    sim.setMessageFilter(filter);
    following = true;
    follow();
  });

  // A batch landed: follow, at most once a frame, and four times a second
  // under live pacing; or count what is newer. A throttled frame keeps the
  // read pending, so the last batch before a pause is still read.
  let frame = 0;
  let lastRead = 0;
  const readFrame = (now: number): void => {
    if (live && now - lastRead < 250) {
      frame = requestAnimationFrame(readFrame);
      return;
    }
    frame = 0;
    lastRead = now;
    if (following) follow();
    else {
      const len = sim.messageRows(0, 0).rows[2];
      newer = Math.max(len - (first + VISIBLE), 0);
      head = headOf(sim.messageRows(0, 0).rows);
    }
  };
  $effect(() => {
    void version;
    if (frame === 0) frame = requestAnimationFrame(readFrame);
  });
  onDestroy(() => cancelAnimationFrame(frame));

  function onScroll(): void {
    if (scroller === undefined || head === null) return;
    if (scrolling) {
      scrolling = false;
      return;
    }
    const from = Math.floor(scroller.scrollTop / ROW_PX);
    const atBottom = scroller.scrollTop + scroller.clientHeight >= scroller.scrollHeight - ROW_PX / 2;
    following = atBottom;
    if (from !== first || !atBottom) read(from);
  }

  function select(seq: number | null): void {
    detail = seq === null ? null : (sim.selectMessage(seq) ?? null);
    stale = false;
    if (seq !== null) following = false;
    read(first);
  }

  /** Select `seq` and scroll its row into view, if it is listed. */
  function follow_link(seq: number): void {
    select(seq);
    const at = rows.find((r) => r.kind === 'message' && r.seq === seq);
    if (at !== undefined) scrollTo(Math.max(at.ordinal - 2, 0));
  }

  const setView = (patch: Partial<MessagesView>): void => {
    view = { ...view, ...patch };
  };

  /** A click on a message row selects it. */
  function onGridClick(event: MouseEvent): void {
    const row = (event.target as HTMLElement).closest<HTMLElement>('[data-seq]');
    if (row !== null) select(Number(row.dataset.seq));
  }

  function onGridKey(event: KeyboardEvent): void {
    const messages = rows.filter((r): r is Extract<Row, { kind: 'message' }> => r.kind === 'message');
    const at = messages.findIndex((r) => r.seq === detail?.seq);
    switch (event.key) {
      case 'ArrowDown':
      case 'ArrowUp': {
        event.preventDefault();
        const next = messages[at < 0 ? (event.key === 'ArrowDown' ? 0 : messages.length - 1) : at + (event.key === 'ArrowDown' ? 1 : -1)];
        if (next !== undefined) {
          select(next.seq);
          if (scroller !== undefined && (next.ordinal < first + 1 || next.ordinal >= first + VISIBLE - 1)) {
            scroller.scrollTop = Math.max(next.ordinal - (event.key === 'ArrowDown' ? VISIBLE - 2 : 1), 0) * ROW_PX;
          }
        }
        return;
      }
      case 'Home':
        event.preventDefault();
        if (scroller !== undefined) scroller.scrollTop = 0;
        return;
      case 'End':
        event.preventDefault();
        following = true;
        follow();
        return;
      case 'Enter':
        event.preventDefault();
        document.querySelector<HTMLElement>('.message-bottom .link, .message-bottom button')?.focus();
        return;
      case 'Escape':
        event.preventDefault();
        if (detail !== null) select(null);
        else if (pair !== null) setView({ scope: { kind: 'node' } });
        else document.getElementById('pane-tab-messages')?.focus();
        return;
      case ' ':
      case 'ArrowRight':
      case 'ArrowLeft':
        // Time moves only from the body or the stage.
        event.stopPropagation();
    }
  }

  /** Whether row `i` of the window prints its time: the first of its instant. */
  const printsTime = (i: number): boolean => {
    const r = rows[i];
    if (r.kind !== 'message') return false;
    const prev = rows.slice(0, i).findLast((p) => p.kind === 'message');
    return prev === undefined || prev.kind !== 'message' || prev.at !== r.at;
  };

  /** A message row's words: role · lost · gist · riders, the riders that do
   *  not fit becoming "+n". */
  function wordsOf(r: Extract<Row, { kind: 'message' }>): { role: string; lost: boolean; gist: string; riders: string[]; more: number } {
    const riders = riderWords(r.riders, r.repeat, zoneOf(r.dst));
    // Source Sans 3 at 12 px averages about 5.2 px a character.
    const budget = Math.floor(messageWidth / 5.2);
    let used = ROLE_WORD[r.role].length + (r.fate === 'lost' ? 7 : 0);
    let g = gist(r);
    if (used + g.length + 3 > budget) g = '';
    used += g.length + 3;
    const kept: string[] = [];
    for (const [k, w] of riders.entries()) {
      // Room for a "+n" after it unless it is the last.
      if (used + w.length + 3 > budget - (k === riders.length - 1 ? 0 : 4)) break;
      kept.push(w);
      used += w.length + 3;
    }
    return { role: ROLE_WORD[r.role], lost: r.fate === 'lost', gist: g, riders: kept, more: riders.length - kept.length };
  }

  /** A row's accessible name: a sentence, its fate said. */
  function labelOf(r: Extract<Row, { kind: 'message' }>): string {
    const w = riderWords(r.riders, r.repeat, zoneOf(r.dst));
    return [`message ${r.seq}`, `${fmtInstant(r.at)} seconds`, `${name(r.src)} to ${name(r.dst)}`, ROLE_WORD[r.role], gist(r), ...w, r.fate].join(', ');
  }

  const says = $derived<Says>({ name, zoneOf, cut: detail === null ? undefined : cutWords(detail.src, detail.dst) });

  const scopeWords = $derived(
    pair !== null ? `the messages between node ${pair[0]} and node ${pair[1]}` : filter.scope.kind === 'All' ? "every node's messages" : `the messages to and from node ${selected}`,
  );

  /** The previous message in the window before row `i`, for interval words. */
  const silentFrom = (i: number): number | null => {
    if (pair === null || view.pairWay !== 'Both' || view.types !== 0 || view.lostOnly) return null;
    const prev = rows.slice(0, i).findLast((p) => p.kind === 'message');
    if (prev === undefined || prev.kind !== 'message' || !asksAnswer(prev)) return null;
    return prev.dst;
  };

  const typeToken = $derived.by((): string | null => {
    const on = TYPES.filter((_, i) => (view.types & (1 << i)) !== 0);
    if (on.length === 0) return null;
    return on.length === 1 ? TYPE_WORD[on[0]] : `${on.length} types`;
  });

  const empty = $derived(head !== null && head.viewLen === 0);
  const shownTotal = $derived(head === null ? 0 : head.scopeCount);
</script>

<div class="messages" data-messages bind:clientWidth={width}>
  <div class="scope-row">
    {#if filter.scope.kind === 'All'}
      <button class="btn-secondary chip-button" onclick={() => setView({ scope: { kind: 'node' } })} {...{ onpointerenter: () => onPreview(selected), onpointerleave: () => onPreview(null) }}
        >Only node {selected}</button
      >
    {:else}
      <span class="chip" data-field="scope-chip">
        {#if pair !== null}node {pair[0]}&thinsp;↔&thinsp;node {pair[1]}{:else}node {selected}{/if}
        <button
          class="chip-x"
          aria-label={pair !== null ? `Back to node ${selected}'s messages` : "Show every node's messages"}
          title={pair !== null ? 'Shift+click a second node to see only the two' : undefined}
          onclick={() => setView({ scope: { kind: pair !== null ? 'node' : 'all' } })}>×</button
        >
      </span>
    {/if}
    {#if typeToken !== null}
      <span class="chip">{typeToken}<button class="chip-x" aria-label="Show every type" onclick={() => setView({ types: 0 })}>×</button></span>
    {/if}
  </div>
  <div class="way-row">
    {#if pair !== null}
      <div class="segments" role="group" aria-label="Direction">
        {#each [['Both', 'Both'], ['AToB', `node ${pair[0]} → node ${pair[1]}`], ['BToA', `node ${pair[1]} → node ${pair[0]}`]] as [w, label] (w)}
          <button class="segment" aria-pressed={view.pairWay === w} onclick={() => setView({ pairWay: w as MessagesView['pairWay'] })}>{label}</button>
        {/each}
      </div>
    {:else if filter.scope.kind === 'Node'}
      <div class="segments" role="group" aria-label="Direction">
        {#each ['Both', 'Sent', 'Received'] as w (w)}
          <button class="segment" aria-pressed={view.way === w} onclick={() => setView({ way: w as MessagesView['way'] })}>{w}</button>
        {/each}
      </div>
      <div class="segments" role="group" aria-label="Tier">
        {#each [['Any', 'All'], ['InZone', `In ${zoneOf(selected) ?? 'its zone'}`], ['CrossZone', 'Across']] as [t, label] (t)}
          <button class="segment" aria-pressed={view.tier === t} onclick={() => setView({ tier: t as MessagesView['tier'] })}>{label}</button>
        {/each}
      </div>
    {:else}
      <div class="segments" role="group" aria-label="Tier">
        {#each [['Any', 'All'], ['InZone', 'Within zones'], ['CrossZone', 'Across zones']] as [t, label] (t)}
          <button class="segment" aria-pressed={view.tier === t} onclick={() => setView({ tier: t as MessagesView['tier'] })}>{label}</button>
        {/each}
      </div>
    {/if}
  </div>

  <div class="list-head" class:pair={pair !== null} class:all={filter.scope.kind === 'All'} aria-hidden="true">
    <span class="time">Time (s)</span>
    {#if pair !== null}
      <span class="ladder lanes"><span>node {pair[0]}</span><span>node {pair[1]}</span></span>
    {:else}
      <span class="mark"></span>
      <span class="peer">{filter.scope.kind === 'All' ? 'Route' : 'Peer'}</span>
    {/if}
    <span class="message">Message</span>
  </div>
  <div
    class="list"
    bind:this={scroller}
    onscroll={onScroll}
    style:height={`${VISIBLE * ROW_PX}px`}
  >
    <div
      class="grid"
      role="grid"
      tabindex="0"
      aria-label="Messages"
      aria-readonly="true"
      aria-rowcount={(head?.viewLen ?? 0) + 1}
      style:height={`${(head?.viewLen ?? 0) * ROW_PX}px`}
      onkeydown={onGridKey}
      onclick={onGridClick}
    >
      {#if empty}
        <p class="empty">
          {#if head !== null && head.total === 0}
            No messages yet · each wave of requests makes some: data to the sending node's zone and its gateways, relays into the other zones, then acknowledgements
          {:else if view.types !== 0 || view.lostOnly}
            No {typeToken ?? 'lost'} messages among {scopeWords.replace('the messages', 'the')} · a gap report asks for a repeat after a packet is lost; the Packet loss scenario has them
          {:else}
            Messages to and from the selected node show here · selecting another node shows its own
          {/if}
        </p>
      {/if}
      {#each rows as r, i (r.ordinal)}
        <div
          class="row {r.kind}"
          class:selected={r.kind === 'message' && r.relation === 2}
          class:related={r.kind === 'message' && r.relation === 1}
          class:hovered={r.kind === 'message' && hovered === r.seq}
          class:muted={r.kind === 'message' && (r.role === 'Ack' || r.role === 'ProbeAnswer') && r.riders === 0 && r.relation !== 2}
          class:pair={pair !== null}
          class:all={filter.scope.kind === 'All'}
          role="row"
          tabindex="-1"
          aria-rowindex={r.ordinal + 2}
          aria-selected={r.kind === 'message' ? r.relation === 2 : undefined}
          style:top={`${r.ordinal * ROW_PX}px`}
          data-seq={r.kind === 'message' ? r.seq : undefined}
          onpointerenter={() => (hovered = r.kind === 'message' ? r.seq : null)}
          onpointerleave={() => (hovered = null)}
        >
          {#if r.kind === 'message'}
            {@const w = wordsOf(r)}
            <span class="time" role="gridcell">{printsTime(i) ? fmtInstant(r.at) : ''}</span>
            {#if pair !== null}
              {@const ltr = r.src === pair[0]}
              {@const x1 = ltr ? 8 : 64}
              {@const x2 = r.fate === 'lost' ? 36 : ltr ? 64 : 8}
              <svg class="ladder" width="72" height="24" viewBox="0 0 72 24" aria-hidden="true" class:faded={detail !== null && r.relation === 0}>
                <line class="lane" x1="8" y1="0" x2="8" y2="24" />
                <line class="lane" x1="64" y1="0" x2="64" y2="24" />
                <line
                  class="rung"
                  class:data={r.role === 'OwnData' || r.role === 'RelayData'}
                  class:cross={r.tier === 'CrossZone'}
                  class:flight={r.fate === 'in flight'}
                  class:heavy={r.relation === 2}
                  {x1}
                  y1="7"
                  {x2}
                  y2={r.fate === 'lost' ? 12 : 17}
                />
                {#if r.fate === 'lost'}
                  <line class="stop" x1="36" y1="9" x2="36" y2="15" />
                {:else}
                  <circle class="end" cx={x2} cy="17" r="2" />
                {/if}
              </svg>
            {:else}
              <span class="mark" role="gridcell"><MessageMark role={r.role} cells={r.cells} tier={r.tier} fate={r.fate} /></span>
              <span class="peer" role="gridcell">
                {#if filter.scope.kind === 'All'}
                  <span class:me={r.src === selected}>{name(r.src)}</span>&thinsp;→&thinsp;<span class:me={r.dst === selected}>{name(r.dst)}</span>
                {:else}
                  {r.src === selected ? `to ${name(r.dst)}` : `from ${name(r.src)}`}
                {/if}
              </span>
            {/if}
            <span class="message" role="gridcell" aria-label={labelOf(r)}>
              {w.role}{#if w.lost}{' · '}<b class="lost">lost</b>{/if}{w.gist === '' ? '' : ` · ${w.gist}`}{w.riders.map((r) => ` · ${r}`).join('')}{#if w.more > 0}{' '}<span
                  class="more">+{w.more}</span
                >{/if}
            </span>
          {:else if r.kind === 'marker'}
            <span class="marker-text" role="gridcell">{markerWords(r.marker, (z) => zoneAt(zones, z))}</span>
          {:else if r.kind === 'interval'}
            {@const silent = silentFrom(i)}
            <span class="interval-text" role="gridcell"
              >· <b>{(r.ms / 1000).toFixed(r.ms < 1000 ? 2 : 1)}</b>{'\u202F'}s {silent === null ? 'later' : `with no answer from ${name(silent)}`} ·</span
            >
          {:else if i === 0 || rows[i - 1].kind !== 'gone'}
            <span class="marker-text" role="gridcell"
              ><b>{fmtCount(Math.max((head?.firstKept ?? 0) - r.ordinal, 1))}</b> older {(head?.firstKept ?? 0) - r.ordinal === 1 ? 'message is' : 'messages are'} no longer kept</span
            >
          {/if}
        </div>
      {/each}
    </div>
  </div>

  <div class="message-bottom">
    {#if detail !== null}
      <MessageDetailView
        {detail}
        {says}
        zoneName={zoneOf}
        {stale}
        pairShown={pair !== null && ((pair[0] === detail.src && pair[1] === detail.dst) || (pair[0] === detail.dst && pair[1] === detail.src))}
        onFollow={follow_link}
        onPair={(a, b) => setView({ scope: { kind: 'pair', pair: [a, b], under: selected } })}
        {onPreview}
      />
    {:else if head !== null}
      <ByType {head} types={view.types} lostOnly={view.lostOnly} {scopeWords} onTypes={(types) => setView({ types })} onLost={(lostOnly) => setView({ lostOnly })} />
    {/if}
  </div>

  {#if head !== null}
    <p class="status" data-field="messages-status">
      {#if view.types !== 0 || view.lostOnly}
        <b>{fmtCount(head.shown)}</b> of <b>{fmtCount(shownTotal)}</b> shown
      {:else}
        <b>{fmtCount(head.shown)}</b> {head.shown === 1 ? 'message' : 'messages'}
      {/if}
      {#if head.bytesShown > 0} · <b>{(head.bytesShown / 1000).toFixed(1)}</b>{'\u202F'}kB{/if}
      {#if head.lostShown > 0} · <b>{fmtCount(head.lostShown)}</b> lost{/if}
      {#if !live && head.inFlight > 0} · <b>{fmtCount(head.inFlight)}</b> in flight{/if}
      {#if !live && head.inFlightAll > head.inFlight && filter.scope.kind !== 'All'}
        · <b>{fmtCount(head.inFlightAll - head.inFlight)}</b> more in flight outside {pair !== null ? `node ${pair[0]} ↔ node ${pair[1]}` : `node ${selected}`}{/if}
      {#if head.total > head.kept} · the last <b>{fmtCount(head.kept)}</b> of <b>{fmtCount(head.total)}</b> are kept{/if}
      {#if !following && newer > 0}
        <span class="newer"><b>{fmtCount(newer)}</b> newer · <button class="btn-tertiary" onclick={() => ((following = true), follow())}>Jump to the newest</button></span>
      {/if}
    </p>
  {/if}
</div>

<style>
  /* The view fills the pane: the bottom region takes what the list leaves,
     at least 240 px, and scrolls within. */
  .messages {
    flex: 1 1 0;
    min-height: 0;
    display: flex;
    flex-direction: column;
    gap: var(--gap-sm);
    min-width: 0;
    container: messages / inline-size;
  }

  .messages > :not(.message-bottom) {
    flex: none;
  }

  .scope-row,
  .way-row {
    display: flex;
    flex-wrap: wrap;
    gap: var(--gap-sm);
    min-height: 28px;
  }

  .chip {
    display: inline-flex;
    align-items: center;
    gap: var(--gap-xs);
    min-height: 28px;
    padding: 0 0 0 12px;
    border: 1px solid var(--chart-axis);
    border-radius: 14px;
    font-size: var(--font-size-caption);
    font-weight: 600;
  }

  .chip-x {
    width: 24px;
    height: 24px;
    padding: 0;
    border: none;
    border-radius: 12px;
    background: none;
    color: var(--color-fg);
    font: inherit;
    cursor: pointer;
  }

  .chip-button {
    min-height: 28px;
    font-size: var(--font-size-caption);
  }

  .segments {
    display: inline-flex;
  }

  .segment {
    min-height: 28px;
    padding: 0 8px;
    border: 1px solid var(--chart-axis);
    border-radius: 0;
    background: var(--color-panel);
    color: var(--color-fg);
    font-size: var(--font-size-caption);
    font-weight: 600;
    cursor: pointer;
  }

  .segment + .segment {
    margin-left: -1px;
  }

  .segment:first-child {
    border-radius: 6px 0 0 6px;
  }

  .segment:last-child {
    border-radius: 0 6px 6px 0;
  }

  .segment[aria-pressed='true'] {
    background: var(--color-bg);
    box-shadow: inset 0 -2px 0 var(--color-fg);
  }

  .list-head,
  .row {
    display: grid;
    grid-template-columns: 48px 16px 76px 1fr;
    column-gap: 8px;
    align-items: center;
    font-size: var(--font-size-caption-sm);
    font-variant-numeric: tabular-nums;
  }

  .list-head.all,
  .row.all {
    grid-template-columns: 48px 16px 104px 1fr;
  }

  .list-head.pair,
  .row.pair {
    grid-template-columns: 48px 72px 1fr;
  }

  .list-head {
    height: 24px;
    color: var(--color-fg-muted);
    border-bottom: 1px solid var(--color-border-line);
  }

  .lanes {
    display: flex;
    justify-content: space-between;
  }

  .time {
    text-align: right;
  }

  .list {
    position: relative;
    overflow-y: auto;
    overscroll-behavior: contain;
  }

  .grid {
    position: relative;
  }

  .grid:focus-visible {
    outline: 2px solid var(--color-fg);
    outline-offset: 2px;
  }

  .row {
    position: absolute;
    left: 0;
    right: 0;
    height: 24px;
    white-space: nowrap;
    cursor: default;
  }

  .row.message {
    cursor: pointer;
  }

  .row.hovered {
    background: var(--color-bg);
  }

  .row.selected {
    background: var(--color-bg);
    box-shadow: inset 3px 0 0 var(--color-fg);
  }

  .row.related {
    box-shadow: inset 1px 0 0 var(--color-fg);
  }

  .row.muted .peer,
  .row.muted .message {
    color: var(--color-fg-muted);
  }

  .row.marker,
  .row.interval,
  .row.gone {
    color: var(--color-fg-muted);
  }

  .marker-text {
    grid-column: 2 / -1;
  }

  .interval-text {
    grid-column: -2 / -1;
  }

  .interval-text b,
  .marker-text b,
  .status b {
    font-weight: 600;
    color: var(--color-fg);
  }

  .message,
  .peer {
    overflow: hidden;
  }

  .me {
    font-weight: 600;
  }

  .lost {
    font-weight: 600;
    color: var(--color-fg);
  }

  .more {
    color: var(--color-fg-muted);
  }

  .ladder .lane {
    stroke: var(--chart-grid);
    stroke-width: 1;
  }

  .ladder .rung {
    stroke: var(--chart-muted);
    stroke-width: 1;
  }

  .ladder .rung.data {
    stroke-width: 1.5;
  }

  .ladder .rung.data.cross {
    stroke: var(--color-fg);
    stroke-width: 2.5;
  }

  .ladder .rung.flight {
    stroke-dasharray: 1 2.5;
  }

  .ladder .rung.heavy {
    stroke-width: 3;
  }

  .ladder .stop {
    stroke: var(--chart-muted);
    stroke-width: 1.5;
  }

  .ladder .end {
    fill: var(--chart-muted);
  }

  .ladder.faded {
    opacity: 0.25;
  }

  .empty {
    margin: 0;
    padding: var(--pad-sm) 0;
    font-size: var(--font-size-caption-sm);
    color: var(--color-fg-muted);
  }

  .message-bottom {
    padding-top: var(--gap-sm);
    border-top: 1px solid var(--color-border-line);
    flex: 1 1 0;
    min-height: 240px;
    overflow-y: auto;
  }

  .status {
    margin: 0;
    font-size: var(--font-size-caption-sm);
    color: var(--color-fg-muted);
    font-variant-numeric: tabular-nums;
  }

  .newer {
    float: right;
  }

  /* Keep in step with NARROW: the time column gives way to the message. */
  @container messages (width < 300px) {
    .list-head,
    .row {
      grid-template-columns: 16px 76px 1fr;
    }

    .list-head.all,
    .row.all {
      grid-template-columns: 16px 104px 1fr;
    }

    .list-head.pair,
    .row.pair {
      grid-template-columns: 72px 1fr;
    }

    .time {
      display: none;
    }

    .marker-text {
      grid-column: 1 / -1;
    }
  }
</style>
