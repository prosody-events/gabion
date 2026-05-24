<script lang="ts">
  import type { Preset } from '../presets';

  // The left control rail (the chassis's controls region): scenario presets, the
  // keyboard/AT-accessible "send a burst" control (the equivalent of clicking a
  // disc — the same `burstHits` drives both), and the network Heal action that
  // completes the partition / isolation stories. Rebuild-knob sliders land here
  // in a later slice.
  let {
    presets,
    activeId,
    nodeCount,
    burstHits = $bindable(),
    onSelectPreset,
    onSend,
    onHeal,
  }: {
    presets: readonly Preset[];
    activeId: string;
    nodeCount: number;
    burstHits: number;
    onSelectPreset: (preset: Preset) => void;
    onSend: (node: number) => void;
    onHeal: () => void;
  } = $props();

  const activePreset = $derived(presets.find((p) => p.id === activeId));
  const activeBlurb = $derived(activePreset?.blurb ?? '');
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

  input {
    width: 100%;
    padding: var(--space-1) var(--space-2);
    border: 1px solid var(--chrome-border);
    border-radius: var(--radius);
    background: var(--chrome-bg);
    font-family: inherit;
    font-size: var(--text-sm);
    color: var(--ink);
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
