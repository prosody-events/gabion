<script lang="ts">
  // The time dock: play/pause, single-step, reset, a speed control, and the
  // current virtual-time readout. The full linear scrubber and the Tide
  // causality strip layer in here in Phase 6.
  let {
    playing,
    speed = $bindable(),
    tick,
    virtualMs,
    onToggle,
    onStep,
    onReset,
  }: {
    playing: boolean;
    speed: number;
    tick: number;
    virtualMs: number;
    onToggle: () => void;
    onStep: () => void;
    onReset: () => void;
  } = $props();

  const seconds = $derived((virtualMs / 1000).toFixed(1));
</script>

<div class="transport" role="group" aria-label="Playback controls">
  <button
    class="primary"
    onclick={onToggle}
    aria-pressed={playing}
    aria-label={playing ? 'Pause' : 'Play'}
  >
    {playing ? '❚❚ Pause' : '▶ Play'}
  </button>
  <button onclick={onStep} disabled={playing} aria-label="Step forward one tick"> ▸❙ Step </button>
  <button onclick={onReset} aria-label="Reset to start"> ↺ Reset </button>

  <label class="speed">
    <span>Speed</span>
    <input type="range" min="0.1" max="4" step="0.1" bind:value={speed} aria-label="Playback speed" />
    <span class="numeric speed-value">{speed.toFixed(1)}×</span>
  </label>

  <div class="readout">
    <span class="readout-item">
      <span class="readout-label">tick</span>
      <span class="numeric readout-value">{tick}</span>
    </span>
    <span class="readout-item">
      <span class="readout-label">virtual time</span>
      <span class="numeric readout-value">{seconds}s</span>
    </span>
  </div>
</div>

<style>
  .transport {
    display: flex;
    align-items: center;
    gap: var(--space-3);
    padding: var(--space-2) var(--space-3);
    background: var(--chrome-panel);
    border-top: 1px solid var(--chrome-border);
  }

  button {
    display: inline-flex;
    align-items: center;
    gap: var(--space-1);
    padding: var(--space-2) var(--space-3);
    border: 1px solid var(--chrome-border);
    border-radius: var(--radius);
    background: var(--chrome-panel);
    font-size: var(--text-sm);
    transition: background 120ms ease;
  }

  button:hover:not(:disabled) {
    background: var(--chrome-bg);
  }

  button:disabled {
    opacity: 0.45;
    cursor: not-allowed;
  }

  .primary {
    background: var(--ink);
    color: var(--chrome-panel);
    border-color: var(--ink);
    min-width: 6.5rem;
    justify-content: center;
  }

  .primary:hover:not(:disabled) {
    background: #2c2f36;
  }

  .speed {
    display: inline-flex;
    align-items: center;
    gap: var(--space-2);
    font-size: var(--text-sm);
    color: var(--ink-soft);
  }

  .speed-value {
    min-width: 2.5rem;
  }

  .readout {
    margin-left: auto;
    display: flex;
    gap: var(--space-4);
  }

  .readout-item {
    display: flex;
    flex-direction: column;
    align-items: flex-end;
    line-height: 1.1;
  }

  .readout-label {
    font-size: var(--text-xs);
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--ink-faint);
  }

  .readout-value {
    font-size: var(--text-lg);
    color: var(--ink);
  }
</style>
