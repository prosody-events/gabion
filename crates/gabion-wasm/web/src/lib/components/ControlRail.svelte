<script lang="ts">
  import { RULE_BUCKET_MS, type Knobs, type Preset } from '../presets';
  import { visibleBuckets } from '../buckets';

  // The left control rail (the chassis's controls region): scenario presets, the
  // rebuild knobs (a collapsed disclosure, so the rail stays compact), the
  // keyboard/AT-accessible "send a burst" control (the equivalent of clicking a
  // disc — the same `burstHits` drives both), and the network Heal action that
  // completes the partition / isolation stories.
  let {
    presets,
    activeId,
    nodeIds,
    canAdd,
    burstHits = $bindable(),
    knobs,
    onSelectPreset,
    onApplyKnobs,
    onSend,
    onHeal,
    onAddNode,
    onRemoveNode,
  }: {
    presets: readonly Preset[];
    activeId: string;
    // The live stable ids in rank order (with gaps after churn) — what the
    // send and remove pickers choose from. Authoritative live set, not 0..N.
    nodeIds: number[];
    // Whether the cluster is below the live-node cap (gates the Add button).
    canAdd: boolean;
    burstHits: number;
    // A live `$state` proxy owned by App: the sliders mutate its fields in place
    // (no reassignment, so it need not be `$bindable`), and `onApplyKnobs`
    // rebuilds the cluster with the new values once a slider is released.
    knobs: Knobs;
    onSelectPreset: (preset: Preset) => void;
    onApplyKnobs: () => void;
    onSend: (node: number) => void;
    onHeal: () => void;
    onAddNode: () => void;
    onRemoveNode: (id: number) => void;
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
  const nodeCount = $derived(nodeIds.length);
  // The window reads in whole seconds; its bar count is the bucket math the
  // Strata uses (one source of truth — `buckets.ts`), so the readout can't
  // disagree with the strip the inspector draws.
  const windowSec = $derived(Math.round(knobs.rule_window_ms / 1000));
  const windowBuckets = $derived(visibleBuckets(knobs.rule_window_ms, RULE_BUCKET_MS));

  // Send and remove each pick a live id. A bare `$state` can't follow the live
  // set: before the cluster loads it is null, and a picked node can leave under
  // churn. A reconciling effect snaps each pick to the first live id whenever it
  // names no live node — which also *seeds* it once the cluster arrives, so the
  // `<select>` shows a real target (node 0) instead of a blank until first touch.
  // It converges in one pass (the snapped value is itself live), so it does not
  // loop.
  let sendPick = $state<number | null>(null);
  let removePick = $state<number | null>(null);
  $effect(() => {
    if (sendPick === null || !nodeIds.includes(sendPick)) sendPick = nodeIds[0] ?? null;
  });
  $effect(() => {
    if (removePick === null || !nodeIds.includes(removePick)) removePick = nodeIds[0] ?? null;
  });

  function send(): void {
    if (sendPick !== null) onSend(sendPick);
  }

  function remove(): void {
    if (removePick !== null) onRemoveNode(removePick);
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
    <!-- The rule + gossip knobs. Longer gossip interval = slower propagation;
         the window readout shows its derived bucket count (the bars the Strata
         draws); the limit reaches 1 000 000 so a narrative preset's pinned limit
         is editable, not clamped away. -->
    <div class="knob">
      <label for="knob-gossip">Gossip interval<span class="val numeric">{knobs.tick_interval_ms} ms</span></label>
      <input
        id="knob-gossip"
        type="range"
        min="50"
        max="1000"
        step="50"
        bind:value={knobs.tick_interval_ms}
        onchange={onApplyKnobs}
      />
    </div>
    <div class="knob">
      <label for="knob-window">Window<span class="val numeric">{windowSec} s · {windowBuckets} buckets</span></label>
      <input
        id="knob-window"
        type="range"
        min="3000"
        max="30000"
        step="1000"
        bind:value={knobs.rule_window_ms}
        onchange={onApplyKnobs}
      />
    </div>
    <div class="knob">
      <label for="knob-limit">Rule limit</label>
      <input
        id="knob-limit"
        class="numeric"
        type="number"
        min="1"
        max="1000000"
        step="100"
        bind:value={knobs.rule_limit}
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
    <h2>Cluster</h2>
    <p class="hint">Add or remove members live — ids stay stable.</p>
    <!-- The current size, so the +/− pair reads against a live value (system
         status) without counting discs on the stage. -->
    <div class="members">
      <span class="members-label">Members</span>
      <span class="members-value numeric">{nodeCount}</span>
    </div>
    <!-- Which node "Remove" takes — the AT/keyboard equivalent of the stage "×".
         Disabled (not hidden) at one node, so the section never changes height. -->
    <div class="field">
      <label for="remove-node">Remove</label>
      <select id="remove-node" class="numeric" bind:value={removePick} disabled={nodeCount <= 1}>
        {#each nodeIds as id (id)}
          <option value={id}>node {id}</option>
        {/each}
      </select>
    </div>
    <!-- Add and Remove as a balanced pair, so they read as the inverse of one
         another. The glyphs are the signifier; the aria-labels carry the action
         for assistive tech (and keep the stage "×" buttons unambiguous). -->
    <div class="cluster-actions">
      <button class="cluster-btn" aria-label="Add node" onclick={onAddNode} disabled={!canAdd}>
        <span class="sign" aria-hidden="true">+</span>Add
      </button>
      <button
        class="cluster-btn"
        aria-label="Remove node"
        onclick={remove}
        disabled={nodeCount <= 1}
      >
        <span class="sign" aria-hidden="true">−</span>Remove
      </button>
    </div>
  </section>

  <section class="group">
    <h2>Send a burst</h2>
    <p class="hint">Pick a node, or click any disc on the stage.</p>
    <div class="field">
      <label for="burst-node">Node</label>
      <select id="burst-node" class="numeric" bind:value={sendPick} disabled={nodeCount === 0}>
        {#each nodeIds as id (id)}
          <option value={id}>node {id}</option>
        {/each}
      </select>
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

  input.numeric,
  select.numeric {
    width: 100%;
    padding: var(--space-1) var(--space-2);
    border: 1px solid var(--chrome-border);
    border-radius: var(--radius);
    background: var(--chrome-bg);
    font-family: inherit;
    font-size: var(--text-sm);
    color: var(--ink);
  }

  select.numeric:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  /* The live member count: a quiet readout the +/− pair acts on. The number
     dominates its label (visual hierarchy) and is tabular, so it never jitters
     as nodes join and leave. */
  .members {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    padding: var(--space-1) var(--space-2);
    border: 1px solid var(--chrome-border);
    border-radius: var(--radius);
    background: var(--chrome-bg);
  }

  .members-label {
    font-size: var(--text-sm);
    color: var(--ink-soft);
  }

  .members-value {
    font-size: var(--text-lg);
    font-weight: 600;
    line-height: 1;
    color: var(--ink);
  }

  /* Add and Remove on one 8-pt row, equal halves — the inverse-pair reading. */
  .cluster-actions {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: var(--space-2);
  }

  .cluster-btn {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 0.4em;
    padding: var(--space-2) var(--space-3);
    border: 1px solid var(--chrome-border);
    border-radius: var(--radius);
    background: var(--chrome-bg);
    font-size: var(--text-sm);
    transition:
      background 120ms ease,
      border-color 120ms ease;
  }

  .cluster-btn:hover:not(:disabled) {
    background: var(--chrome-panel);
    border-color: var(--ink-faint);
  }

  .cluster-btn:disabled {
    opacity: 0.45;
    cursor: not-allowed;
  }

  /* The +/− signifier rides a touch larger than the verb and in the soft ink,
     so it reads as an icon paired with the label, not part of the word. */
  .cluster-btn .sign {
    font-size: 1.15em;
    line-height: 1;
    color: var(--ink-soft);
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
    background: var(--ink-hover);
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
