<script lang="ts">
  import { fmtCount } from '../format';
  import { RELATION_WORD, ROLE_WORD, fmtInstant, fmtOffset, lostWords, sentenceOf, type MessageDetail, type Says } from '../messages';
  import FrameDiagram from './FrameDiagram.svelte';
  import InfoTip from './InfoTip.svelte';

  // One message, read: its title, a plain sentence, its links to what it
  // answers or caused, the pair button, its frame, and its cells.
  let {
    detail,
    says,
    zoneName,
    stale,
    pairShown,
    onFollow,
    onPair,
    onPreview,
  }: {
    detail: MessageDetail;
    says: Says;
    /** A node's zone by stable id, by name. */
    zoneName: (id: number) => string | undefined;
    /** Whether the message has left the log since it was chosen. */
    stale: boolean;
    /** Whether the scope is already this message's pair. */
    pairShown: boolean;
    onFollow: (seq: number) => void;
    onPair: (a: number, b: number) => void;
    onPreview: (node: number | null) => void;
  } = $props();

  const fate = $derived(detail.fate.kind === 'Lost' ? ` · lost: ${lostWords(detail.fate.by, detail.dst, detail.src, says.cut)}` : detail.fate.kind === 'InFlight' ? ' · in flight' : '');
  const shortId = (hex: string): string => {
    const id = hex.replace(/^0+/, '') || '0';
    return id.length > 4 ? `${id.slice(0, 4)}…` : id;
  };
  const names = (id: number) => ({
    onpointerenter: () => onPreview(id),
    onpointerleave: () => onPreview(null),
    onfocus: () => onPreview(id),
    onblur: () => onPreview(null),
  });
</script>

<section class="detail" aria-labelledby="message-title">
  <h3 class="title" id="message-title">{ROLE_WORD[detail.role]} <span class="seq">#{detail.seq}</span></h3>
  <p class="sub">
    {says.name(detail.src)} → {says.name(detail.dst)} · {zoneName(detail.src) ?? 'no zone'} to {zoneName(detail.dst) ?? 'no zone'} ·
    {fmtInstant(detail.at_ms)}{'\u202F'}s · {fmtCount(detail.wire_len)} bytes{fate}
  </p>
  <p class="sentence" data-field="message-sentence">
    {sentenceOf(detail, says)}{#if stale}
      No longer in the log; shown from the copy made when you chose it.{/if}
  </p>
  {#if detail.links.length > 0}
    <dl class="links">
      {#each detail.links as l, i (i)}
        <dt>{RELATION_WORD[l.relation]}</dt>
        <dd><button class="btn-tertiary link" onclick={() => onFollow(l.seq)}>{ROLE_WORD[l.role]} #{l.seq}</button> · {fmtOffset(l.offset_ms)}</dd>
      {/each}
    </dl>
  {/if}
  {#if !pairShown}
    <button class="btn-secondary pair" onclick={() => onPair(detail.src, detail.dst)} {...names(detail.dst)}
      >See only node {detail.src} ↔ node {detail.dst}</button
    >
  {/if}
  <FrameDiagram {detail} name={says.name} zoneName={zoneName(detail.src)} />
  {#if detail.cells.length > 0}
    <section class="cells" aria-labelledby="cells-title">
      <h3 class="title" id="cells-title">Cells</h3>
      <p class="caption">counts are totals, not additions: the receiver keeps the larger</p>
      <table>
        <thead>
          <tr><th>Node</th><th>Rule</th><th>Key</th><th class="num">Second</th><th class="num">Count</th><th class="num">Updated (s)</th></tr>
        </thead>
        <tbody>
          {#each detail.cells as c, i (i)}
            <tr>
              <td>{c.origin === undefined ? 'unknown' : `node ${c.origin}`}</td>
              <td><InfoTip text={c.rule}><code>{shortId(c.rule)}</code></InfoTip></td>
              <td><InfoTip text={c.key}><code>{shortId(c.key)}</code></InfoTip></td>
              <td class="num">{fmtCount(c.bucket)}</td>
              <td class="num">{fmtCount(c.count)}</td>
              <td class="num">{fmtInstant(c.updated_ms)}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </section>
  {/if}
</section>

<style>
  .detail {
    display: flex;
    flex-direction: column;
    gap: var(--gap-sm);
  }

  .title {
    margin: 0;
    font-size: var(--font-size-body);
    font-weight: 600;
  }

  .cells .title {
    font-size: var(--font-size-caption);
  }

  .seq {
    font-weight: 400;
    color: var(--color-fg-muted);
  }

  .sub,
  .sentence,
  .links,
  .caption {
    margin: 0;
    font-size: var(--font-size-caption);
    font-variant-numeric: tabular-nums;
  }

  .sub,
  .caption {
    color: var(--color-fg-muted);
  }

  .sentence {
    text-wrap: pretty;
  }

  .links {
    display: grid;
    grid-template-columns: 104px 1fr;
    row-gap: 0;
  }

  .links dt {
    color: var(--color-fg-muted);
  }

  .links dd {
    margin: 0;
  }

  .link {
    min-height: 20px;
  }

  .pair {
    align-self: flex-start;
    min-height: 28px;
    font-size: var(--font-size-caption);
  }

  table {
    border-collapse: collapse;
    font-size: var(--font-size-caption-sm);
    font-variant-numeric: tabular-nums;
  }

  th {
    font-weight: 400;
    color: var(--color-fg-muted);
    text-align: left;
    border-bottom: 1px solid var(--color-border-line);
  }

  th,
  td {
    padding: 0 8px 0 0;
    height: 20px;
  }

  .num {
    text-align: right;
  }
</style>
