<script lang="ts">
  import { fmtCount, type Blurb } from '../format';

  // A sentence the page prints: text as text, and every number in its own
  // `data-field` element, named `field`; a claim also says whose number it is
  // and what it counts (`data-subject`, `data-noun`).
  let { parts, field }: { parts: Blurb; field: string } = $props();
</script>

{#each parts as part, i (i)}{#if typeof part === 'string'}{part}{:else if typeof part === 'number'}<span
      data-field={field}>{fmtCount(part)}</span
    >{:else}<span data-field={field} data-subject={part.subject} data-noun={part.noun}
      >{typeof part.value === 'number' ? fmtCount(part.value) : part.value}</span
    >{/if}{/each}
