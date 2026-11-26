<script lang="ts">
  // The playback speed: in the transport from 640 px, in the Controls
  // disclosure below, so the phone's bar keeps one row.
  let { speed = $bindable() }: { speed: number } = $props();
</script>

<label class="speed">
  <span>Speed</span>
  <input type="range" min="0.1" max="4" step="0.1" bind:value={speed} aria-label="Playback speed" />
  <span class="speed-value" data-field="speed">{speed.toFixed(1)}×</span>
</label>

<style>
  .speed {
    display: inline-flex;
    align-items: center;
    gap: var(--gap-sm);
    font-size: var(--font-size-caption);
    color: var(--color-fg-muted);
  }

  /* Narrow enough that the bar stays one row at 1100 px. */
  .speed input {
    width: 5.5rem;
  }

  .speed-value {
    min-width: 2.5rem;
  }
</style>
