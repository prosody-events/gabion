<script lang="ts">
  // The left control rail (the chassis's controls region). In this slice it
  // carries the keyboard/AT-accessible equivalent of click-a-node: pick a node
  // and a burst size, press Send. The same `burstHits` drives a stage click, so
  // the pointer gesture and this control always agree on the burst size.
  // Scenario presets and the rebuild-knob sliders land here in later slices.
  let {
    nodeCount,
    burstHits = $bindable(),
    onSend,
  }: {
    nodeCount: number;
    burstHits: number;
    onSend: (node: number) => void;
  } = $props();

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
    gap: var(--space-4);
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
</style>
