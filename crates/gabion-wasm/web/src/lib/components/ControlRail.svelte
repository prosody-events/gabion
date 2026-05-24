<script lang="ts">
  import type { Knobs, Preset } from '../presets';

  // The left control rail (the chassis's controls region): scenario presets, the
  // rebuild knobs (a collapsed disclosure, so the rail stays compact), the
  // keyboard/AT-accessible "send a burst" control (the equivalent of clicking a
  // disc — the same `burstHits` drives both), and the network Heal action that
  // completes the partition / isolation stories.
  let {
    presets,
    activeId,
    nodeCount,
    burstHits = $bindable(),
    knobs,
    onSelectPreset,
    onApplyKnobs,
    onSend,
    onHeal,
  }: {
    presets: readonly Preset[];
    activeId: string;
    nodeCount: number;
    burstHits: number;
    // A live `$state` proxy owned by App: the sliders mutate its fields in place
    // (no reassignment, so it need not be `$bindable`), and `onApplyKnobs`
    // rebuilds the cluster with the new values once a slider is released.
    knobs: Knobs;
    onSelectPreset: (preset: Preset) => void;
    onApplyKnobs: () => void;
    onSend: (node: number) => void;
    onHeal: () => void;
  } = $props();

  const activePreset = $derived(presets.find((p) => p.id === activeId));
  const activeBlurb = $derived(activePreset?.blurb ?? '');
  // Packet loss reads as a percentage; the engine wants the fraction. Fanout
  // can't exceed the peer count of the cluster the knobs will build.
  const lossPct = $derived(Math.round(knobs.uniform_loss * 100));
  const maxFanout = $derived(Math.max(knobs.nodes - 1, 1));
  // Heal only does something after a partition or isolation, so the rail shows
  // it only for those scenarios (progressive disclosure — fewer idle controls).
  const showNetwork = $derived(activePreset?.usesNetwork ?? false);

  let targetNode = $state(0);
  const maxNode = $derived(Math.max(nodeCount - 1, 0));

  /** Keep the node index inside the cluster — a stale value (e.g. after the
   *  cluster shrinks) would otherwise reject at the engine boundary. */
  function clampNode(): void {
    targetNode = Math.min(Math.max(Math.trunc(targetNode), 0), maxNode);
  }

  function send(): void {
    clampNode();
    onSend(targetNode);
  }
</script>

<aside class="rail" aria-label="Controls">
  <section class="group">
    <h2>Scenario</h2>
    <div class="presets" role="group" aria-label="Scenario preset">
      {#each presets as preset (preset.id)}
        <button
          class="preset"
          class:active={preset.id === activeId}
          aria-current={preset.id === activeId ? 'true' : undefined}
          onclick={() => onSelectPreset(preset)}
        >
          {preset.label}
        </button>
      {/each}
    </div>
    <p class="blurb">{activeBlurb}</p>
  </section>

  <details class="tune">
    <summary>Tune the cluster</summary>
    <p class="hint">Sliders rebuild the cluster on release.</p>
    <div class="knob">
      <label for="knob-fanout">Fanout<span class="val numeric">{knobs.fanout}</span></label>
      <input
        id="knob-fanout"
        type="range"
        min="1"
        max={maxFanout}
        step="1"
        bind:value={knobs.fanout}
        onchange={onApplyKnobs}
      />
    </div>
    <div class="knob">
      <label for="knob-bps">Error budget<span class="val numeric">{knobs.target_err_bps} bps</span></label>
      <input
        id="knob-bps"
        type="range"
        min="0"
        max="2000"
        step="50"
        bind:value={knobs.target_err_bps}
        onchange={onApplyKnobs}
      />
    </div>
    <div class="knob">
      <label for="knob-loss">Packet loss<span class="val numeric">{lossPct}%</span></label>
      <input
        id="knob-loss"
        type="range"
        min="0"
        max="0.9"
        step="0.05"
        bind:value={knobs.uniform_loss}
        onchange={onApplyKnobs}
      />
    </div>
  </details>

  {#if showNetwork}
    <section class="group">
      <h2>Network</h2>
      <p class="hint">Restore every link, then play to reconcile.</p>
      <button class="secondary" onclick={onHeal} disabled={nodeCount === 0}>Heal network</button>
    </section>
  {/if}

  <section class="group">
    <h2>Send a burst</h2>
    <p class="hint">Pick a node, or click any disc on the stage.</p>
    <div class="field">
      <label for="burst-node">Node</label>
      <input
        id="burst-node"
        class="numeric"
        type="number"
        min="0"
        max={maxNode}
        bind:value={targetNode}
        onchange={clampNode}
      />
    </div>
    <div class="field">
      <label for="burst-hits">Hits</label>
      <input id="burst-hits" class="numeric" type="number" min="1" bind:value={burstHits} />
    </div>
    <button class="send" onclick={send} disabled={nodeCount === 0}>Send burst</button>
  </section>
</aside>

<style>
  .rail {
    display: flex;
    flex-direction: column;
    gap: var(--space-3);
    min-height: 0;
    overflow-y: auto;
    padding: var(--space-3);
    background: var(--chrome-panel);
    border-right: 1px solid var(--chrome-border);
  }

  .group {
    display: flex;
    flex-direction: column;
    gap: var(--space-2);
  }

  h2 {
    margin: 0;
    font-size: var(--text-xs);
    font-weight: 650;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: var(--ink-soft);
  }

  .hint {
    margin: 0 0 var(--space-1);
    font-size: var(--text-sm);
    color: var(--ink-faint);
  }

  .presets {
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
  }

  .preset {
    padding: var(--space-1) var(--space-2);
    border: 1px solid var(--chrome-border);
    border-radius: var(--radius);
    background: var(--chrome-bg);
    font-size: var(--text-sm);
    text-align: left;
    transition:
      background 120ms ease,
      border-color 120ms ease;
  }

  .preset:hover {
    background: var(--chrome-panel);
    border-color: var(--ink-faint);
  }

  /* Active scenario marked by fill + weight, not color alone. */
  .preset.active {
    background: var(--ink);
    border-color: var(--ink);
    color: var(--chrome-panel);
    font-weight: 600;
  }

  .blurb {
    margin: var(--space-1) 0 0;
    font-size: var(--text-sm);
    line-height: 1.45;
    color: var(--ink-soft);
  }

  .field {
    display: grid;
    grid-template-columns: 3.5rem 1fr;
    align-items: center;
    gap: var(--space-2);
    font-size: var(--text-sm);
    color: var(--ink-soft);
  }

  input.numeric {
    width: 100%;
    padding: var(--space-1) var(--space-2);
    border: 1px solid var(--chrome-border);
    border-radius: var(--radius);
    background: var(--chrome-bg);
    font-family: inherit;
    font-size: var(--text-sm);
    color: var(--ink);
  }

  /* Rebuild knobs — collapsed by default so the rail stays compact (Hick's law:
     fewer simultaneous choices), and so the network/burst controls stay near the
     top. */
  .tune {
    border-top: 1px solid var(--chrome-border);
    padding-top: var(--space-2);
  }

  .tune summary {
    cursor: pointer;
    font-size: var(--text-xs);
    font-weight: 650;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: var(--ink-soft);
  }

  .tune .hint {
    margin: var(--space-2) 0 0;
  }

  .knob {
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
    margin-top: var(--space-2);
  }

  .knob label {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    font-size: var(--text-sm);
    color: var(--ink-soft);
  }

  .knob .val {
    color: var(--ink);
  }

  input[type='range'] {
    width: 100%;
    accent-color: var(--ink);
    cursor: pointer;
  }

  .send {
    margin-top: var(--space-1);
    padding: var(--space-2) var(--space-3);
    border: 1px solid var(--ink);
    border-radius: var(--radius);
    background: var(--ink);
    color: var(--chrome-panel);
    font-size: var(--text-sm);
    transition: background 120ms ease;
  }

  .send:hover:not(:disabled) {
    background: #2c2f36;
  }

  .send:disabled {
    opacity: 0.45;
    cursor: not-allowed;
  }

  .secondary {
    align-self: flex-start;
    padding: var(--space-2) var(--space-3);
    border: 1px solid var(--chrome-border);
    border-radius: var(--radius);
    background: var(--chrome-bg);
    font-size: var(--text-sm);
    transition: background 120ms ease;
  }

  .secondary:hover:not(:disabled) {
    background: var(--chrome-panel);
    border-color: var(--ink-faint);
  }

  .secondary:disabled {
    opacity: 0.45;
    cursor: not-allowed;
  }
</style>
