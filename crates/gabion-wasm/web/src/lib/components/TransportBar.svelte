<script lang="ts">
  // The time dock: play/pause, Step to the next event, reset, a speed control,
  // the current virtual-time readout and, once a heartbeat period has played,
  // how long it took to watch at the chosen speed.
  import { fmtDuration } from '../format';

  let {
    playing,
    busy,
    speed = $bindable(),
    virtualMs,
    heartbeatWallMs,
    onToggle,
    onStep,
    onReset,
  }: {
    playing: boolean;
    /** A rebuild is running: the controls wait for it. */
    busy: boolean;
    speed: number;
    virtualMs: number;
    /** Wall ms the last whole heartbeat period took to play, or null before
     *  one has. */
    heartbeatWallMs: number | null;
    onToggle: () => void;
    onStep: () => void;
    onReset: () => void;
  } = $props();
</script>

<div class="transport" role="group" aria-label="Playback controls">
  <button
    class="primary"
    onclick={onToggle}
    disabled={busy}
    aria-pressed={playing}
    aria-label={playing ? 'Pause' : 'Play'}
  >
    {playing ? '❚❚ Pause' : '▶ Play'}
  </button>
  <button onclick={onStep} disabled={busy} aria-label="Step to the next event" title="→ steps to the next event">
    ▸❙ Step
  </button>
  <button onclick={onReset} disabled={busy} aria-label="Reset to start"> ↺ Reset </button>

  <label class="speed">
    <span>Speed</span>
    <input type="range" min="0.1" max="4" step="0.1" bind:value={speed} aria-label="Playback speed" />
    <span class="speed-value" data-field="speed">{speed.toFixed(1)}×</span>
  </label>

  <div class="readout">
    <span class="readout-item">
      <span class="readout-label">virtual time</span>
      <span class="readout-value" data-field="virtual-time" data-ms={virtualMs}>{fmtDuration(virtualMs)}</span>
    </span>
    {#if heartbeatWallMs !== null}
      <span class="readout-item">
        <span class="readout-note">
          a heartbeat took <span data-field="heartbeat-wall" data-ms={heartbeatWallMs}>{fmtDuration(heartbeatWallMs)}</span>
          to watch
        </span>
      </span>
    {/if}
  </div>
</div>

<style>
  .transport {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: var(--gap-md);
    padding: var(--gap-sm) var(--gap-md);
    background: var(--color-panel);
    border-top: 1px solid var(--color-border-line);
  }

  button {
    display: inline-flex;
    align-items: center;
    gap: var(--gap-xs);
    padding: var(--gap-sm) var(--gap-md);
    border: 1px solid var(--color-border-line);
    border-radius: var(--radius);
    background: var(--color-panel);
    font-size: var(--font-size-caption);
    transition: background 120ms ease;
  }

  button:hover:not(:disabled) {
    background: var(--color-bg);
  }

  button:disabled {
    opacity: 0.45;
    cursor: not-allowed;
  }

  .primary {
    background: var(--color-fg);
    color: var(--color-panel);
    border-color: var(--color-fg);
    min-width: 6.5rem;
    justify-content: center;
  }

  .primary:hover:not(:disabled) {
    background: var(--color-fg-muted);
  }

  .speed {
    display: inline-flex;
    align-items: center;
    gap: var(--gap-sm);
    font-size: var(--font-size-caption);
    color: var(--color-fg-muted);
  }

  .speed-value {
    min-width: 2.5rem;
  }

  .readout {
    margin-left: auto;
    display: flex;
    gap: var(--gap-lg);
  }

  .readout-item {
    display: flex;
    flex-direction: column;
    align-items: flex-end;
    line-height: 1.1;
  }

  .readout-label {
    font-size: var(--font-size-caption-sm);
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--color-fg-muted);
  }

  .readout-value {
    font-size: var(--font-size-heading);
    color: var(--color-fg);
  }

  .readout-note {
    font-size: var(--font-size-caption);
    color: var(--color-fg);
  }

  .readout-note [data-field] {
    font-weight: 600;
  }
</style>
