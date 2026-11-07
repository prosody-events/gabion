<script lang="ts">
  // The page's control and tag icons, drawn, never set from a font: play,
  // pause and step for the transport; next, wait and watch for the action
  // line's tag, each a different shape; go for the next scenario; and the
  // fault and repair marks before their labels.
  export type IconName = 'play' | 'pause' | 'step' | 'next' | 'wait' | 'watch' | 'go' | 'silence' | 'cut' | 'reconnect';

  let { name }: { name: IconName } = $props();
</script>

<svg class="icon" width="12" height="12" viewBox="0 0 12 12" aria-hidden="true">
  {#if name === 'play' || name === 'next'}
    <path class="fill" d="M2 1L11 6L2 11Z" />
  {:else if name === 'pause'}
    <path class="fill" d="M2 1H5V11H2ZM7 1H10V11H7Z" />
  {:else if name === 'step'}
    <path class="fill" d="M1 1L8 6L1 11ZM9 1H11V11H9Z" />
  {:else if name === 'wait'}
    <circle class="line" cx="6" cy="6" r="5" />
    <path class="line" d="M6 3V6L8 8" />
  {:else if name === 'watch'}
    <path class="line" d="M1 6C3 2.5 9 2.5 11 6C9 9.5 3 9.5 1 6Z" />
    <circle class="fill" cx="6" cy="6" r="1.6" />
  {:else if name === 'go'}
    <path class="line" d="M1 6H10M7 3L10 6L7 9" />
  {:else if name === 'silence'}
    <circle class="line dashed" cx="6" cy="6" r="4.5" />
  {:else if name === 'cut'}
    <path class="line" d="M6 1V11M2 3H10M2 9H10" />
  {:else if name === 'reconnect'}
    <circle class="line" cx="6" cy="6" r="4.5" />
  {/if}
</svg>

<style>
  .icon {
    flex: none;
    overflow: visible;
  }

  .fill {
    fill: currentColor;
  }

  .line {
    fill: none;
    stroke: currentColor;
    stroke-width: 1.5;
    stroke-linecap: round;
    stroke-linejoin: round;
  }

  .dashed {
    stroke-dasharray: 2 2;
  }
</style>
