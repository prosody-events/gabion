<script lang="ts">
  import { type Knobs, type Preset } from '../presets';
  import { visibleBuckets } from '../buckets';
  import { fmtCount, zoneLabel, type Blurb } from '../format';
  import type { Roles } from '../stage/renderer';
  import type { SimConfig, ZoneView } from '../sim/types';

  // The left control rail (the chassis's controls region): scenario presets, the
  // rebuild knobs (a collapsed disclosure, so the rail stays compact), the
  // network actions that cut a zone or a gateway and heal them, live membership,
  // and the keyboard/AT-accessible "send a burst" control (the equivalent of
  // clicking a disc: the same `burstHits` drives both).
  let {
    presets,
    activeId,
    blurb,
    nodeIds,
    zones,
    roles,
    maxZones,
    canAdd,
    burstHits = $bindable(),
    knobs,
    appliedKnobs,
    bucketMs,
    onSelectPreset,
    onApplyKnobs,
    onSend,
    onCutZone,
    onSilence,
    onCutFromZone,
    onHeal,
    onAddNode,
    onRemoveNode,
  }: {
    presets: readonly Preset[];
    activeId: string;
    /** The active scenario's blurb, built from the config the cluster runs. */
    blurb: Blurb;
    // The live stable ids in rank order (with gaps after churn): what the send
    // and remove pickers choose from. Authoritative live set, not 0..N.
    nodeIds: number[];
    /** Every zone with its live members, in zone order. */
    zones: ZoneView[];
    /** The selected origin and its gateways, which name the network actions. */
    roles: Roles | null;
    /** The zone stepper's ceiling, from the engine. */
    maxZones: number;
    // Whether the cluster is below the live-node cap (gates the Add button).
    canAdd: boolean;
    burstHits: number;
    // A live `$state` proxy owned by App: the controls mutate its fields in place
    // (no reassignment, so it need not be `$bindable`). Editing a knob only
    // *stages* it: nothing rebuilds until `onApplyKnobs`.
    knobs: Knobs;
    // The knobs the current engine was built from. A field that differs from
    // `knobs` is staged-but-not-applied; `onApplyKnobs` (Rebuild) applies them.
    appliedKnobs: Knobs;
    // The rule's bucket width (`defaults.rule_bucket_ms`, sourced from Rust), so
    // the window readout's bucket count matches the Strata the inspector draws.
    bucketMs: number;
    onSelectPreset: (preset: Preset) => void;
    onApplyKnobs: () => void;
    onSend: (node: number) => void;
    onCutZone: (zone: number) => void;
    onSilence: (gateway: number) => void;
    onCutFromZone: (gateway: number, zone: number) => void;
    onHeal: () => void;
    onAddNode: (zone: number) => void;
    onRemoveNode: (id: number) => void;
  } = $props();

  // Which rebuild knobs have been moved since the last build: staged, awaiting a
  // Rebuild. The set drives the per-row "changed" cue and gates the button.
  const KNOB_FIELDS = [
    'zones',
    'zone_mode',
    'target_err_bps',
    'uniform_loss',
    'rule_limit',
    'rule_window_ms',
    'tick_interval_ms',
  ] as const satisfies readonly (keyof SimConfig & keyof Knobs)[];
  const changed = $derived(new Set(KNOB_FIELDS.filter((k) => knobs[k] !== appliedKnobs[k])));
  const dirty = $derived(changed.size > 0);

  // Packet loss reads as a percentage; the engine wants the fraction.
  const lossPct = $derived(Math.round(knobs.uniform_loss * 100));
  const nodeCount = $derived(nodeIds.length);
  // The window reads in whole seconds; its bar count uses the same bucket math
  // the Strata uses (one source of truth: `buckets.ts`). This readout tracks the
  // *staged* slider value, while the inspector strip reflects the *applied*
  // engine; the knob's "changed" cue flags exactly that gap.
  const windowSec = $derived(Math.round(knobs.rule_window_ms / 1000));
  const windowBuckets = $derived(visibleBuckets(knobs.rule_window_ms, bucketMs));
  const zoneCap = $derived(Math.min(maxZones, knobs.nodes));

  // The zones a network action can name: those with members.
  const liveZones = $derived(zones.flatMap((z, i) => (z.live > 0 ? [{ i, name: zoneLabel(z.name) }] : [])));
  // The zone Add defaults to: the fewest live members, the lowest index on a tie.
  const fewest = $derived(zones.reduce((best, z, i) => (z.live < zones[best].live ? i : best), 0));
  // The origin's gateways, one per remote zone, in zone order, named by the
  // selection.
  const gateways = $derived(
    roles === null
      ? []
      : [...roles.gateways].map(([id, g]) => ({ id, ...g })).sort((a, b) => a.zone - b.zone),
  );

  // Send, remove, cut and add each pick from a live set. A bare `$state` can't
  // follow it: before the cluster loads it is null, and a pick can leave under
  // churn. A reconciling effect snaps each pick to the first live choice whenever
  // it names none, which also seeds it once the cluster arrives. It converges in
  // one pass (the snapped value is itself live), so it does not loop.
  let sendPick = $state<number | null>(null);
  let removePick = $state<number | null>(null);
  let cutPick = $state<number | null>(null);
  let addPick = $state<number | null>(null);
  $effect(() => {
    if (sendPick === null || !nodeIds.includes(sendPick)) sendPick = nodeIds[0] ?? null;
  });
  $effect(() => {
    if (removePick === null || !nodeIds.includes(removePick)) removePick = nodeIds[0] ?? null;
  });
  $effect(() => {
    if (cutPick === null || !liveZones.some((z) => z.i === cutPick)) cutPick = liveZones[0]?.i ?? null;
  });
  const addZone = $derived(addPick ?? fewest);

  function send(): void {
    if (sendPick !== null) onSend(sendPick);
  }

  function remove(): void {
    if (removePick !== null) onRemoveNode(removePick);
  }

  function add(): void {
    onAddNode(addZone);
    addPick = null;
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
    <p class="blurb">{#each blurb as part}{#if typeof part === 'number'}<span data-field="blurb">{part}</span>{:else}{part}{/if}{/each}</p>
  </section>

  <details class="tune">
    <summary>Tune the cluster</summary>
    <p class="hint">Edit freely — changes stage until you rebuild.</p>
    <div class="knob" class:changed={changed.has('zones')}>
      <label for="knob-zones">Zones</label>
      <input id="knob-zones" type="number" min="1" max={zoneCap} step="1" bind:value={knobs.zones} />
    </div>
    <div class="knob" class:changed={changed.has('zone_mode')}>
      <label for="knob-zone-mode">Zone mode</label>
      <select id="knob-zone-mode" bind:value={knobs.zone_mode}>
        <option value="Configured">Each node knows its zone</option>
        <option value="Unset">No node knows its zone</option>
      </select>
    </div>
    <div class="knob" class:changed={changed.has('target_err_bps')}>
      <label for="knob-bps">Error budget<span class="val" data-field="error-budget-bps">{knobs.target_err_bps} bps</span></label>
      <input id="knob-bps" type="range" min="50" max="2000" step="50" bind:value={knobs.target_err_bps} />
    </div>
    <div class="knob" class:changed={changed.has('uniform_loss')}>
      <label for="knob-loss">Packet loss<span class="val" data-field="loss">{lossPct}%</span></label>
      <input id="knob-loss" type="range" min="0" max="0.9" step="0.05" bind:value={knobs.uniform_loss} />
    </div>
    <!-- The rule + gossip knobs. Longer gossip interval = slower propagation;
         the window readout shows its derived bucket count (the bars the Strata
         draws); the limit reaches 1 000 000 so a narrative preset's pinned limit
         is editable, not clamped away. -->
    <div class="knob" class:changed={changed.has('tick_interval_ms')}>
      <label for="knob-gossip">Gossip interval<span class="val" data-field="gossip-interval">{knobs.tick_interval_ms} ms</span></label>
      <input id="knob-gossip" type="range" min="50" max="1000" step="50" bind:value={knobs.tick_interval_ms} />
    </div>
    <div class="knob" class:changed={changed.has('rule_window_ms')}>
      <label for="knob-window">Window<span class="val" data-field="window">{windowSec} s · {windowBuckets} buckets</span></label>
      <input id="knob-window" type="range" min="3000" max="30000" step="1000" bind:value={knobs.rule_window_ms} />
    </div>
    <div class="knob" class:changed={changed.has('rule_limit')}>
      <label for="knob-limit">Rule limit</label>
      <input id="knob-limit" type="number" min="1" max="1000000" step="100" bind:value={knobs.rule_limit} />
    </div>
    <!-- Build-time settings apply only on an explicit rebuild, so editing never
         nukes the running sim or collapses this section. The button is live only
         while there is a staged change. -->
    <div class="tune-apply">
      <p class="tune-status" aria-live="polite">
        {#if dirty}
          <span class="staged-dot" aria-hidden="true"></span>
          <span data-field="staged">{changed.size}</span> staged — rebuild to apply
        {:else}
          Settings match the running cluster.
        {/if}
      </p>
      <button class="rebuild" onclick={onApplyKnobs} disabled={!dirty}>Rebuild cluster</button>
    </div>
  </details>

  <section class="group">
    <h2>Network</h2>
    <p class="hint">Cut links to watch the cluster fall behind, then heal them and play to reconcile.</p>
    <div class="field wide">
      <label for="cut-zone">Zone</label>
      <select id="cut-zone" bind:value={cutPick} disabled={liveZones.length === 0}>
        {#each liveZones as zone (zone.i)}
          <option value={zone.i}>{zone.name}</option>
        {/each}
      </select>
    </div>
    <button
      class="secondary"
      onclick={() => cutPick !== null && onCutZone(cutPick)}
      disabled={cutPick === null || liveZones.length < 2}
    >
      Cut {liveZones.find((z) => z.i === cutPick)?.name ?? 'zone'} off
    </button>
    {#if gateways.length === 0}
      <p class="hint">Select a node to see its gateways</p>
    {:else}
      {#each gateways as gateway (gateway.zone)}
        {@const zone = zoneLabel(zones[gateway.zone].name)}
        <button class="secondary" onclick={() => onSilence(gateway.id)}>
          Silence node <span data-field="node">{gateway.id}</span>, the gateway into {zone}
        </button>
        <button class="secondary" onclick={() => onCutFromZone(gateway.id, gateway.zone)}>
          Cut node <span data-field="node">{gateway.id}</span> from {zone}
        </button>
      {/each}
    {/if}
    <button class="secondary" onclick={onHeal} disabled={nodeCount === 0}>Heal network</button>
  </section>

  <section class="group">
    <h2>Cluster</h2>
    <p class="hint">Add or remove members live — ids stay stable.</p>
    <!-- The current size, so the +/− pair reads against a live value (system
         status) without counting discs on the stage. -->
    <div class="members">
      <span class="members-label">Members</span>
      <span class="members-value" data-field="members">{fmtCount(nodeCount)}</span>
    </div>
    <div class="field wide">
      <label for="add-zone">Add to</label>
      <select id="add-zone" value={addZone} onchange={(e) => (addPick = Number(e.currentTarget.value))}>
        {#each liveZones as zone (zone.i)}
          <option value={zone.i}>{zone.name}</option>
        {/each}
      </select>
    </div>
    <!-- Which node "Remove" takes: the AT/keyboard path to what the inspector's
         "Remove this node" does for the selection. Disabled (not hidden) at one
         node, so the section never changes height. -->
    <div class="field">
      <label for="remove-node">Remove</label>
      <select id="remove-node" bind:value={removePick} disabled={nodeCount <= 1}>
        {#each nodeIds as id (id)}
          <option value={id} data-field="node">node {id}</option>
        {/each}
      </select>
    </div>
    <!-- Add and Remove as a balanced pair, so they read as the inverse of one
         another. The glyphs are the signifier; the aria-labels carry the action
         for assistive tech. -->
    <div class="cluster-actions">
      <button class="cluster-btn" aria-label="Add node" onclick={add} disabled={!canAdd}>
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
      <select id="burst-node" bind:value={sendPick} disabled={nodeCount === 0}>
        {#each nodeIds as id (id)}
          <option value={id} data-field="node">node {id}</option>
        {/each}
      </select>
    </div>
    <div class="field">
      <label for="burst-hits">Hits</label>
      <input id="burst-hits" type="number" min="1" bind:value={burstHits} />
    </div>
    <button class="send" onclick={send} disabled={nodeCount === 0}>Send burst</button>
  </section>
</aside>

<style>
  .rail {
    display: flex;
    flex-direction: column;
    gap: var(--gap-md);
    height: 100%;
    min-height: 0;
    overflow-y: auto;
    padding: var(--gap-md);
    background: var(--color-panel);
    border-right: 1px solid var(--color-border-line);
  }

  .group {
    display: flex;
    flex-direction: column;
    gap: var(--gap-sm);
  }

  h2 {
    margin: 0;
    font-size: var(--font-size-caption-sm);
    font-weight: 650;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: var(--color-fg-muted);
  }

  .hint {
    margin: 0 0 var(--gap-xs);
    font-size: var(--font-size-caption);
    color: var(--color-fg-muted);
  }

  .presets {
    display: flex;
    flex-direction: column;
    gap: var(--gap-xs);
  }

  .preset {
    padding: var(--gap-xs) var(--gap-sm);
    border: 1px solid var(--color-border-line);
    border-radius: var(--radius);
    background: var(--color-bg);
    font-size: var(--font-size-caption);
    text-align: left;
    transition:
      background 120ms ease,
      border-color 120ms ease;
  }

  .preset:hover {
    background: var(--color-panel);
    border-color: var(--color-fg-muted);
  }

  /* Active scenario marked by fill + weight, not color alone. */
  .preset.active {
    background: var(--color-fg);
    border-color: var(--color-fg);
    color: var(--color-panel);
    font-weight: 600;
  }

  .blurb {
    margin: var(--gap-xs) 0 0;
    font-size: var(--font-size-caption);
    line-height: 1.45;
    color: var(--color-fg-muted);
  }

  .field {
    display: grid;
    grid-template-columns: 3.5rem 1fr;
    align-items: center;
    gap: var(--gap-sm);
    font-size: var(--font-size-caption);
    color: var(--color-fg-muted);
  }

  .field.wide {
    grid-template-columns: 4.5rem 1fr;
  }

  input[type='number'],
  select {
    width: 100%;
    padding: var(--gap-xs) var(--gap-sm);
    border: 1px solid var(--color-border-line);
    border-radius: var(--radius);
    background: var(--color-bg);
    font-family: inherit;
    font-size: var(--font-size-caption);
    color: var(--color-fg);
  }

  select:disabled {
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
    padding: var(--gap-xs) var(--gap-sm);
    border: 1px solid var(--color-border-line);
    border-radius: var(--radius);
    background: var(--color-bg);
  }

  .members-label {
    font-size: var(--font-size-caption);
    color: var(--color-fg-muted);
  }

  .members-value {
    font-size: var(--font-size-heading);
    font-weight: 600;
    line-height: 1;
    color: var(--color-fg);
  }

  /* Add and Remove on one 8-pt row, equal halves — the inverse-pair reading.
     `minmax(0, 1fr)` keeps the halves inside the rail at its narrowest, where
     the 8-px padding still fits each label. */
  .cluster-actions {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: var(--gap-sm);
  }

  .cluster-btn {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 0.4em;
    padding: var(--gap-sm);
    border: 1px solid var(--color-border-line);
    border-radius: var(--radius);
    background: var(--color-bg);
    font-size: var(--font-size-caption);
    transition:
      background 120ms ease,
      border-color 120ms ease;
  }

  .cluster-btn:hover:not(:disabled) {
    background: var(--color-panel);
    border-color: var(--color-fg-muted);
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
    color: var(--color-fg-muted);
  }

  /* Rebuild knobs — collapsed by default so the rail stays compact (Hick's law:
     fewer simultaneous choices), and so the network/burst controls stay near the
     top. */
  .tune {
    border-top: 1px solid var(--color-border-line);
    padding-top: var(--gap-sm);
  }

  .tune summary {
    cursor: pointer;
    font-size: var(--font-size-caption-sm);
    font-weight: 650;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: var(--color-fg-muted);
  }

  .tune .hint {
    margin: var(--gap-sm) 0 0;
  }

  .knob {
    display: flex;
    flex-direction: column;
    gap: var(--gap-xs);
    margin-top: var(--gap-sm);
  }

  .knob label {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    font-size: var(--font-size-caption);
    color: var(--color-fg-muted);
  }

  .knob .val {
    color: var(--color-fg);
  }

  /* A staged (changed-but-not-applied) knob: its readout turns dirty-hued and a
     marker rides the label, so the pending state reads in text + colour + a
     glyph, never colour alone. */
  .knob.changed .val {
    color: var(--color-warn);
    font-weight: 600;
  }

  .knob.changed label::before {
    content: '•';
    margin-right: 0.3em;
    color: var(--color-warn);
  }

  .knob.changed input[type='number'] {
    border-color: var(--color-warn);
  }

  input[type='range'] {
    width: 100%;
    accent-color: var(--color-fg);
    cursor: pointer;
  }

  /* The explicit apply control: a status line (staged count or "matches") and
     the Rebuild button, live only while a change is staged. */
  .tune-apply {
    display: flex;
    flex-direction: column;
    gap: var(--gap-sm);
    margin-top: var(--gap-md);
  }

  .tune-status {
    display: flex;
    align-items: center;
    gap: 0.4em;
    margin: 0;
    font-size: var(--font-size-caption);
    color: var(--color-fg-muted);
  }

  .staged-dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--color-warn);
    flex: none;
  }

  .rebuild {
    align-self: flex-start;
    padding: var(--gap-sm) var(--gap-md);
    border: 1px solid var(--color-fg);
    border-radius: var(--radius);
    background: var(--color-fg);
    color: var(--color-panel);
    font-size: var(--font-size-caption);
    transition: background 120ms ease;
  }

  .rebuild:hover:not(:disabled) {
    background: var(--color-fg-muted);
  }

  .rebuild:disabled {
    opacity: 0.45;
    cursor: not-allowed;
  }

  .send {
    margin-top: var(--gap-xs);
    padding: var(--gap-sm) var(--gap-md);
    border: 1px solid var(--color-fg);
    border-radius: var(--radius);
    background: var(--color-fg);
    color: var(--color-panel);
    font-size: var(--font-size-caption);
    transition: background 120ms ease;
  }

  .send:hover:not(:disabled) {
    background: var(--color-fg-muted);
  }

  .send:disabled {
    opacity: 0.45;
    cursor: not-allowed;
  }

  .secondary {
    align-self: flex-start;
    text-align: left;
    padding: var(--gap-sm) var(--gap-md);
    border: 1px solid var(--color-border-line);
    border-radius: var(--radius);
    background: var(--color-bg);
    font-size: var(--font-size-caption);
    transition: background 120ms ease;
  }

  .secondary:hover:not(:disabled) {
    background: var(--color-panel);
    border-color: var(--color-fg-muted);
  }

  .secondary:disabled {
    opacity: 0.45;
    cursor: not-allowed;
  }
</style>
