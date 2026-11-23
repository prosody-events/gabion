<script lang="ts">
  import { fmtCount } from '../format';
  import { ALWAYS_SHOWN, TYPES, TYPE_WORD, type Head, type MessageType } from '../messages';
  import InfoTip from './InfoTip.svelte';
  import RoleGlyph, { type Glyph } from './RoleGlyph.svelte';

  // By type: the overview of the scope's messages and the type filter in one
  // table. Each row but "all messages" is a toggle; the Lost head toggles
  // lost messages only. Counts come from the engine, never a page tally.
  let {
    head,
    types,
    lostOnly,
    scopeWords,
    onTypes,
    onLost,
  }: {
    head: Head;
    types: number;
    lostOnly: boolean;
    /** Whose messages, in words: "the messages to and from node 0". */
    scopeWords: string;
    onTypes: (types: number) => void;
    onLost: (on: boolean) => void;
  } = $props();

  const GLYPH: Partial<Record<MessageType, Glyph>> = {
    OwnData: 'data',
    RelayData: 'data',
    Repeat: 'data',
    SweepPage: 'data',
    Ack: 'ack',
    ProbeAnswer: 'ack',
    Skip: 'ack',
    GapReport: 'gap',
    Probe: 'ask',
    SweepRequest: 'ask',
  };

  const shown = $derived(TYPES.map((t, i) => ({ t, i })).filter(({ t, i }) => ALWAYS_SHOWN.has(t) || head.counts[i] > 0 || (types & (1 << i)) !== 0));
  const waiting = $derived(TYPES.map((t, i) => ({ t, i })).filter(({ t, i }) => !ALWAYS_SHOWN.has(t) && head.counts[i] === 0 && (types & (1 << i)) === 0));
</script>

<section class="by-type" aria-labelledby="by-type-title">
  <h3 class="title" id="by-type-title">
    <InfoTip text={`Counts ${scopeWords} (the current scope). A message with two signals, such as data carrying a gap report, counts in both rows.`}>By type</InfoTip>
  </h3>
  <div class="table" role="table" aria-labelledby="by-type-title">
    <div class="row head" role="row">
      <span class="show" role="columnheader">Show only</span>
      <span class="count" role="columnheader">Count</span>
      <span class="lost" role="columnheader">
        <button class="toggle" aria-pressed={lostOnly} onclick={() => onLost(!lostOnly)}><span class="box" aria-hidden="true"></span>Lost</button>
      </span>
    </div>
    <div class="row" role="row" data-type="all">
      <span class="show" role="cell"><span class="gutter"></span>all messages</span>
      <span class="count" role="cell" data-field="count-all">{fmtCount(head.scopeCount)}</span>
      <span class="lost" role="cell">{fmtCount(head.scopeLost)}</span>
    </div>
    {#each shown as { t, i } (t)}
      {@const pressed = (types & (1 << i)) !== 0}
      <div class="row" class:pressed role="row" data-type={t}>
        <span class="show" role="cell">
          <button class="toggle" aria-pressed={pressed} onclick={() => onTypes(types ^ (1 << i))}>
            <span class="box" aria-hidden="true"></span>
            <span class="glyph">{#if GLYPH[t] !== undefined}<RoleGlyph kind={GLYPH[t]} />{/if}</span>
            {TYPE_WORD[t]}
          </button>
        </span>
        <span class="count" role="cell" data-field={`count-${t}`}>{fmtCount(head.counts[i])}</span>
        <span class="lost" role="cell" data-field={`lost-${t}`}>{fmtCount(head.lost[i])}</span>
      </div>
    {/each}
  </div>
  {#if waiting.length > 0}
    <p class="none">None yet: {waiting.map(({ t }) => TYPE_WORD[t]).join(', ')}.</p>
  {/if}
</section>

<style>
  .by-type {
    display: flex;
    flex-direction: column;
    gap: var(--gap-xs);
  }

  .title {
    margin: 0;
    font-size: var(--font-size-caption);
    font-weight: 600;
  }

  .row {
    display: grid;
    grid-template-columns: 1fr 52px 64px;
    column-gap: 12px;
    align-items: center;
    min-height: 24px;
    font-size: var(--font-size-caption-sm);
    font-variant-numeric: tabular-nums;
  }

  .row.pressed {
    background: var(--color-bg);
  }

  .head {
    color: var(--color-fg-muted);
    border-bottom: 1px solid var(--color-border-line);
  }

  .count,
  .lost {
    text-align: right;
  }

  .show {
    display: flex;
    align-items: center;
    gap: var(--gap-xs);
  }

  .gutter {
    display: inline-block;
    width: 16px;
  }

  .toggle {
    display: inline-flex;
    align-items: center;
    gap: var(--gap-xs);
    min-height: 24px;
    padding: 0;
    border: none;
    background: none;
    color: inherit;
    font: inherit;
    cursor: pointer;
  }

  .box {
    position: relative;
    display: inline-block;
    width: 12px;
    height: 12px;
    margin: 0 2px;
    border: 1px solid var(--chart-axis);
    border-radius: 2px;
  }

  .toggle[aria-pressed='true'] .box::after {
    content: '';
    position: absolute;
    left: 3px;
    top: 0;
    width: 3px;
    height: 7px;
    border: solid var(--color-fg);
    border-width: 0 1.5px 1.5px 0;
    transform: rotate(45deg);
  }

  .glyph {
    display: inline-flex;
    width: 16px;
  }

  .none {
    margin: 0;
    font-size: var(--font-size-caption-sm);
    color: var(--color-fg-muted);
  }
</style>
