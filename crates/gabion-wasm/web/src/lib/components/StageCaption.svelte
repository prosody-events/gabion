<script lang="ts">
  import { fmtCount, type Blurb } from '../format';
  import { BURST_HITS_MAX } from '../presets';
  import { GOES_TO, WATCHES, waitClause, type NextAction } from '../next';
  import type { Preset } from '../presets';
  import Icon from './Icon.svelte';
  import Sentence from './Sentence.svelte';

  // The caption above the stage: the act's goal, which explains and never
  // commands, and the action line, which names the one next action by the
  // exact label of its control (or carries the control itself), or says what
  // to wait for and shows its progress, or what to watch.
  let {
    goal,
    action,
    pressLabel,
    pressClause,
    phone,
    subject,
    useHitsReason,
    zoneName,
    meter,
    presetLabel,
    onSelect,
    onUseHits,
    onGo,
    onPreview,
  }: {
    goal: Blurb;
    action: NextAction;
    /** The label of the transport control a press names, as it renders. */
    pressLabel: Blurb;
    pressClause: Blurb;
    /** Below 640 px the transport is the sticky bar below. */
    phone: boolean;
    subject: number;
    /** Why Hits per Send needs fixing. */
    useHitsReason: Blurb;
    zoneName: (zone: number) => string;
    /** A wait's progress in [0, 1] and in words, or null while it has no end. */
    meter: { value: number; text: string } | null;
    presetLabel: (id: Preset['id']) => string;
    onSelect: (node: number) => void;
    onUseHits: (hits: number) => void;
    onGo: (to: Preset['id']) => void;
    onPreview: (node: number | null) => void;
  } = $props();

  const tag = $derived(action.kind === 'wait' ? 'Wait' : action.kind === 'watch' ? 'Watch' : 'Next');
</script>

<div class="caption">
  <p class="scenario serif"><Sentence parts={goal} field="goal" /></p>
  <p class="next" id="next-action" tabindex="-1">
    <span class="tag"><Icon name={tag === 'Wait' ? 'wait' : tag === 'Watch' ? 'watch' : 'next'} />{tag}</span>
    {#if action.kind === 'press'}
      <span
        >press <strong
          ><Icon name={action.control === 'send' ? 'next' : action.control} /><Sentence parts={pressLabel} field="next" /></strong
        >
        {phone ? 'in the bar below' : 'below'}<Sentence parts={pressClause} field="next" /></span
      >
    {:else if action.kind === 'inline' && action.action.kind === 'select'}
      {@const node = action.action.node}
      <button
        data-primary=""
        aria-describedby="next-action"
        onclick={() => onSelect(node)}
        onpointerenter={() => onPreview(node)}
        onpointerleave={() => onPreview(null)}
        onfocus={() => onPreview(node)}
        onblur={() => onPreview(null)}>Select node <span data-field="next">{node}</span></button
      >
      <span>in {zoneName(action.action.zone)}, or click its disc</span>
    {:else if action.kind === 'inline'}
      {@const hits = action.action.kind === 'use-hits' ? action.action.hits : 0}
      <button data-primary="" aria-describedby="next-action" onclick={() => onUseHits(hits)}
        >Use <span data-field="next">{hits}</span> hits</button
      >
      <span>· <Sentence parts={useHitsReason} field="next" /></span>
    {:else if action.kind === 'repair'}
      <span
        >press <strong><Icon name="reconnect" /><Sentence parts={action.label} field="next" /></strong> in the heading</span
      >
    {:else if action.kind === 'go'}
      {@const to = action.to}
      <button data-primary="" aria-describedby="next-action" onclick={() => onGo(to)}
        >Go to {presetLabel(to)}<Icon name="go" /></button
      >
      <span>· {GOES_TO[to]}</span>
    {:else if action.kind === 'edit'}
      <span
        >type a whole number from <span data-field="next">{1}</span> to&nbsp;<span data-field="next">{fmtCount(BURST_HITS_MAX)}</span> in
        <strong>Hits per Send</strong>, then press Enter</span
      >
    {:else if action.kind === 'wait'}
      <span><Sentence parts={waitClause(action.reason, subject)} field="next" /></span>
      {#if meter === null}
        <span class="meter indeterminate" role="progressbar" aria-label="Progress"></span>
      {:else}
        <span
          class="meter"
          role="progressbar"
          aria-label="Progress"
          aria-valuemin="0"
          aria-valuemax="100"
          aria-valuenow={Math.round(meter.value * 100)}
          aria-valuetext={meter.text}><span class="fill" style="width: {meter.value * 100}%"></span></span
        >
      {/if}
    {:else if action.kind === 'watch'}
      {@const to = action.next}
      <span>{WATCHES[action.what]}</span>
      <button class="btn-secondary go" onclick={() => onGo(to)}>Go to {presetLabel(to)}<Icon name="go" /></button>
    {/if}
  </p>
</div>

<style>
  .caption {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }

  .scenario {
    max-width: 34em;
    margin: 0;
    font-size: var(--font-size-body);
    line-height: 1.5;
    text-wrap: pretty;
  }

  .next {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: var(--gap-xs) var(--gap-sm);
    min-height: 32px;
    margin: 0;
    font-size: var(--font-size-caption);
    color: var(--color-fg);
  }

  .next:focus-visible {
    outline-offset: 4px;
  }

  .tag {
    display: inline-flex;
    align-items: center;
    gap: var(--gap-xs);
    font-variant-caps: all-small-caps;
    font-weight: 600;
    letter-spacing: 0.06em;
    color: var(--color-fg);
  }

  /* On the baseline of the words around it, its icon sitting on that
     baseline too. */
  strong {
    display: inline-flex;
    align-items: baseline;
    gap: var(--gap-xs);
    font-weight: 600;
  }

  button {
    display: inline-flex;
    align-items: center;
    gap: var(--gap-xs);
    min-height: 28px;
    padding: 0 var(--gap-sm);
    border-radius: var(--radius);
    font-size: var(--font-size-caption);
  }

  .meter {
    display: inline-block;
    width: 112px;
    height: 6px;
    border-radius: 3px;
    background: var(--color-border-line);
    overflow: hidden;
  }

  .meter .fill {
    display: block;
    height: 100%;
    background: var(--color-fg);
  }

  .meter.indeterminate {
    background: repeating-linear-gradient(90deg, var(--color-fg) 0 8px, var(--color-border-line) 8px 16px);
  }
</style>
