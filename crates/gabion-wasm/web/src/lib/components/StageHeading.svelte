<script lang="ts">
  import type { Heading } from '../heading';
  import Icon from './Icon.svelte';
  import Sentence from './Sentence.svelte';

  // The stage heading: the cluster's state in one line, the page's largest
  // type, and the one repair it may offer: quiet (a dashed secondary) until the
  // next action is that repair, then the filled control.
  let {
    heading,
    repairPrimary,
    onRepair,
  }: {
    heading: Heading;
    repairPrimary: boolean;
    onRepair: () => void;
  } = $props();
</script>

<div class="stage-heading">
  <h2 data-template={heading.id}><Sentence parts={heading.parts} field="heading" /></h2>
  {#if heading.repair !== null}
    <button
      class="repair {repairPrimary ? '' : 'btn-secondary'}"
      data-primary={repairPrimary ? '' : undefined}
      aria-describedby={repairPrimary ? 'next-action' : undefined}
      onclick={onRepair}
    >
      <Icon name="reconnect" /><Sentence parts={heading.repair} field="repair" />
    </button>
  {/if}
</div>

<style>
  .stage-heading {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: var(--gap-xs) var(--gap-md);
  }

  h2 {
    margin: 0;
    font-size: var(--font-size-heading);
    font-weight: 400;
    line-height: 1.4;
  }

  h2 :global([data-field]) {
    font-weight: 600;
  }

  .repair {
    display: inline-flex;
    align-items: center;
    gap: var(--gap-xs);
    min-height: 28px;
    padding: 0 var(--gap-sm);
    font-size: var(--font-size-caption);
  }

  .repair.btn-secondary {
    border-style: dashed;
  }
</style>
