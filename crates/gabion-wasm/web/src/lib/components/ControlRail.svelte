<script lang="ts">
  import { BURST_HITS_MAX, RULE_LIMIT_MAX, type Knobs, type Preset } from '../presets';
  import { nominalBuckets } from '../buckets';
  import { fmtCount, zoneLabel } from '../format';
  import type { Roles } from '../stage/renderer';
  import type { SimConfig, ZoneView } from '../sim/types';
  import Icon from './Icon.svelte';

  // The left control rail: the scenarios in four groups with Hits per Send and
  // Restart, the rebuild knobs (a collapsed disclosure), the network actions
  // that cut a zone or one of the selected node's gateways and heal them, and
  // live membership. Every field is checked when committed (Enter or leaving
  // it), never while typed, and a disabled control says why beside it.
  let {
    presets,
    activeId,
    hitsText = $bindable(),
    hitsValid,
    editing,
    zones,
    selected,
    roles,
    maxZones,
    canAdd,
    canHeal,
    maxNodes,
    knobs,
    appliedKnobs,
    bucketMs,
    onSelectPreset,
    onRestart,
    onCommitHits,
    onEditHits,
    onRevertHits,
    onApplyKnobs,
    onBackToScenario,
    onCutZone,
    onSilence,
    onCutFromZone,
    onHeal,
    onAddNode,
    onPreview,
  }: {
    presets: readonly Preset[];
    activeId: string;
    /** What the Hits per Send field holds, typed or committed. */
    hitsText: string;
    /** Whether its last committed value is a valid Hits per Send. */
    hitsValid: boolean;
    /** Whether it is being typed: then it is the next action's target. */
    editing: boolean;
    /** Every zone with its live members, in zone order. */
    zones: ZoneView[];
    /** The selected node, whose gateways the network actions name. */
    selected: number;
    roles: Roles | null;
    /** The zone stepper's ceiling, from the engine. */
    maxZones: number;
    canAdd: boolean;
    /** Whether any cut is listed. */
    canHeal: boolean;
    maxNodes: number;
    // A live `$state` proxy owned by the run: the controls mutate its fields
    // in place. Editing a knob only *stages* it: nothing rebuilds until
    // `onApplyKnobs`.
    knobs: Knobs;
    appliedKnobs: Knobs;
    // The rule's bucket width (`defaults.rule_bucket_ms`, sourced from Rust).
    bucketMs: number;
    onSelectPreset: (preset: Preset) => void;
    onRestart: () => void;
    onCommitHits: () => void;
    onEditHits: (editing: boolean) => void;
    /** Escape: back to the last committed value. */
    onRevertHits: () => void;
    onApplyKnobs: () => void;
    onBackToScenario: () => void;
    onCutZone: (zone: number) => void;
    onSilence: (gateway: number) => void;
    onCutFromZone: (gateway: number, zone: number) => void;
    onHeal: () => void;
    onAddNode: (zone: number) => void;
    /** A control that names a node previews it on the stage while hovered or
     *  focused. */
    onPreview: (node: number | null) => void;
  } = $props();

  const active = $derived(presets.find((p) => p.id === activeId));
  const groups = $derived([0, 1, 2, 3].map((g) => presets.filter((p) => p.group === g)));

  // Which rebuild knobs have been moved since the last build: staged, awaiting a
  // Rebuild.
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
  const zoneCap = $derived(Math.min(maxZones, knobs.nodes));
  const whole = (v: unknown, max: number): boolean => Number.isInteger(v) && (v as number) >= 1 && (v as number) <= max;
  const zonesValid = $derived(whole(knobs.zones, zoneCap));
  const limitValid = $derived(whole(knobs.rule_limit, RULE_LIMIT_MAX));
  const canRebuild = $derived(changed.size > 0 && zonesValid && limitValid);
  const rebuildReason = $derived(
    changed.size === 0 ? 'nothing staged' : !zonesValid || !limitValid ? 'a setting is out of range' : null,
  );

  // Packet loss reads as a percentage; the engine wants the fraction.
  const lossPct = $derived(Math.round(knobs.uniform_loss * 100));
  const windowSec = $derived(Math.round(knobs.rule_window_ms / 1000));
  const windowBuckets = $derived(nominalBuckets(knobs.rule_window_ms, bucketMs));
  const nodeCount = $derived(zones.reduce((n, z) => n + z.live, 0));

  // The zones a network action can name: those with members.
  const liveZones = $derived(zones.flatMap((z, i) => (z.live > 0 ? [{ i, name: zoneLabel(z.name) }] : [])));
  // The zone Add defaults to: the fewest live members, the lowest index on a tie.
  const fewest = $derived(zones.reduce((best, z, i) => (z.live < zones[best].live ? i : best), 0));
  // The selected node's gateways, one per remote zone, in zone order.
  const gateways = $derived(
    roles === null ? [] : [...roles.gateways].map(([id, g]) => ({ id, ...g })).sort((a, b) => a.zone - b.zone),
  );

  let cutPick = $state<number | null>(null);
  let addPick = $state<number | null>(null);
  $effect(() => {
    if (cutPick === null || !liveZones.some((z) => z.i === cutPick)) cutPick = liveZones[0]?.i ?? null;
  });
  const addZone = $derived(addPick ?? fewest);

  function add(): void {
    onAddNode(addZone);
    addPick = null;
  }

  function onHitsKey(event: KeyboardEvent): void {
    if (event.key === 'Escape') {
      // Back to the last committed value before leaving the field, so leaving
      // it commits nothing.
      onRevertHits();
      (event.currentTarget as HTMLInputElement).value = hitsText;
      (event.currentTarget as HTMLInputElement).blur();
      event.stopPropagation();
    }
  }

  /** The preview events of a control that names node `id`. */
  const names = (id: number) => ({
    onpointerenter: () => onPreview(id),
    onpointerleave: () => onPreview(null),
    onfocus: () => onPreview(id),
    onblur: () => onPreview(null),
  });
</script>

<aside class="rail" aria-label="Controls">
  <section class="group">
    <h2>Scenario</h2>
    <div class="presets" role="group" aria-label="Scenario preset">
      {#each groups as group, g (g)}
        <div class="preset-group">
          {#each group as preset (preset.id)}
            <button
              class="preset"
              class:active={preset.id === activeId}
              aria-current={preset.id === activeId ? 'true' : undefined}
              onclick={() => preset.id !== activeId && onSelectPreset(preset)}
            >
              {preset.label}
            </button>
          {/each}
        </div>
      {/each}
    </div>
    <div class="hits">
      <label for="burst-hits">Hits per Send</label>
      <input
        id="burst-hits"
        type="text"
        inputmode="numeric"
        autocomplete="off"
        bind:value={hitsText}
        data-primary={editing ? '' : undefined}
        aria-invalid={!hitsValid}
        aria-describedby={editing ? 'next-action burst-hits-hint' : 'burst-hits-hint'}
        onfocus={() => onEditHits(true)}
        onblur={() => onEditHits(false)}
        onchange={onCommitHits}
        onkeydown={onHitsKey}
      />
    </div>
    <p class="hint" id="burst-hits-hint" class:bad={!hitsValid}>
      a whole number from <span data-field="hits-min">{1}</span> to
      <span data-field="hits-max">{fmtCount(BURST_HITS_MAX)}</span>
    </p>
    <button class="btn-secondary restart" title="keeps your settings" onclick={onRestart}>Restart {active?.label ?? ''}</button>
  </section>

  <details class="tune">
    <summary
      >Tune the cluster{#if changed.size > 0}
        · <span data-field="staged">{changed.size}</span> staged{/if}</summary
    >
    <p class="hint">Edit freely: changes stage until you rebuild.</p>
    <div class="knob" class:changed={changed.has('zones')}>
      <label for="knob-zones">Zones</label>
      <input
        id="knob-zones"
        type="number"
        min="1"
        max={zoneCap}
        step="1"
        bind:value={knobs.zones}
        aria-invalid={!zonesValid}
        aria-describedby="knob-zones-hint"
      />
      <span class="hint" id="knob-zones-hint" class:bad={!zonesValid}
        >a whole number from <span data-field="zones-min">{1}</span> to <span data-field="zones-max">{zoneCap}</span></span
      >
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
      <input
        id="knob-limit"
        type="number"
        min="1"
        max={RULE_LIMIT_MAX}
        step="100"
        bind:value={knobs.rule_limit}
        aria-invalid={!limitValid}
        aria-describedby="knob-limit-hint"
      />
      <span class="hint" id="knob-limit-hint" class:bad={!limitValid}
        >a whole number from <span data-field="limit-min">{1}</span> to
        <span data-field="limit-max">{fmtCount(RULE_LIMIT_MAX)}</span></span
      >
    </div>
    <div class="tune-apply">
      <div class="with-reason">
        <button
          class="btn-secondary"
          onclick={onApplyKnobs}
          disabled={!canRebuild}
          aria-describedby={rebuildReason === null ? undefined : 'rebuild-reason'}>Rebuild cluster</button
        >
        {#if rebuildReason !== null}<span class="reason" id="rebuild-reason">{rebuildReason}</span>{/if}
      </div>
      <button class="btn-secondary" onclick={onBackToScenario} disabled={changed.size === 0}
        >Back to the scenario's settings</button
      >
    </div>
  </details>

  <section class="group">
    <h2>Network</h2>
    <div class="field wide">
      <label for="cut-zone">Zone</label>
      <select id="cut-zone" bind:value={cutPick} disabled={liveZones.length === 0}>
        {#each liveZones as zone (zone.i)}
          <option value={zone.i}>{zone.name}</option>
        {/each}
      </select>
    </div>
    <button
      class="btn-secondary action"
      onclick={() => cutPick !== null && onCutZone(cutPick)}
      disabled={cutPick === null || liveZones.length < 2}
    >
      <Icon name="cut" />Cut {liveZones.find((z) => z.i === cutPick)?.name ?? 'zone'} off
    </button>
    {#if gateways.length > 0}
      <h3>node <span data-field="node">{selected}</span>'s gateways</h3>
      {#each gateways as gateway (gateway.id)}
        {@const zone = zoneLabel(zones[gateway.zone].name)}
        <button class="btn-secondary action" onclick={() => onSilence(gateway.id)} {...names(gateway.id)}>
          <Icon name="silence" />Silence node <span data-field="node">{gateway.id}</span>
        </button>
        <button class="btn-secondary action" onclick={() => onCutFromZone(gateway.id, gateway.zone)} {...names(gateway.id)}>
          <Icon name="cut" />Cut node <span data-field="node">{gateway.id}</span> off from {zone}
        </button>
      {/each}
    {/if}
    <div class="with-reason">
      <button
        class="btn-secondary action"
        onclick={onHeal}
        disabled={!canHeal}
        aria-describedby={canHeal ? undefined : 'heal-reason'}><Icon name="reconnect" />Heal network</button
      >
      {#if !canHeal}<span class="reason" id="heal-reason">nothing to heal</span>{/if}
    </div>
  </section>

  <section class="group">
    <h2>Cluster</h2>
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
    <div class="with-reason">
      <button
        class="btn-secondary action"
        aria-label="Add node"
        onclick={add}
        disabled={!canAdd}
        aria-describedby={canAdd ? undefined : 'add-reason'}>Add node</button
      >
      {#if !canAdd}<span class="reason" id="add-reason"
          >the visualizer holds at most <span data-field="max-nodes">{maxNodes}</span> nodes</span
        >{/if}
    </div>
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
    font-weight: 600;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--color-fg-muted);
  }

  h3 {
    margin: var(--gap-xs) 0 0;
    font-size: var(--font-size-caption);
    font-weight: 600;
    color: var(--color-fg);
  }

  .hint {
    margin: 0;
    font-size: var(--font-size-caption);
    color: var(--color-fg-muted);
  }

  .hint.bad {
    color: var(--color-bad);
  }

  .presets {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }

  .preset-group {
    display: flex;
    flex-direction: column;
  }

  /* A list of scenarios: plain rows, the current one marked by a rule at its
     left, weight and a tint, never by the fill the next action owns. */
  .preset {
    min-height: 32px;
    padding: 0 var(--gap-sm);
    border: none;
    border-left: 3px solid transparent;
    border-radius: 0;
    background: none;
    font-size: var(--font-size-caption);
    text-align: left;
  }

  .preset:hover {
    background: var(--color-bg);
  }

  .preset.active {
    border-left-color: var(--color-fg);
    background: var(--color-bg);
    font-weight: 600;
    cursor: default;
  }

  .hits {
    display: grid;
    grid-template-columns: 1fr 4.5rem;
    align-items: center;
    gap: var(--gap-sm);
    margin-top: var(--gap-sm);
    font-size: var(--font-size-caption);
  }

  .hits input {
    text-align: right;
  }

  .restart,
  .action {
    align-self: flex-start;
    display: inline-flex;
    align-items: center;
    gap: var(--gap-xs);
    min-height: 32px;
    padding: 0 var(--gap-md);
    font-size: var(--font-size-caption);
    text-align: left;
  }

  .with-reason {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: var(--gap-xs) var(--gap-sm);
  }

  .reason {
    font-size: var(--font-size-caption);
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
  input[type='text'],
  select {
    width: 100%;
    padding: var(--gap-xs) var(--gap-sm);
    border: 1px solid var(--chart-axis);
    border-radius: var(--radius);
    background: var(--color-panel);
    font-size: var(--font-size-caption);
    color: var(--color-fg);
  }

  input[aria-invalid='true'] {
    border-color: var(--color-bad);
  }

  input:focus-visible {
    border: 2px solid var(--color-fg);
    outline: none;
  }

  select:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .members {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    font-size: var(--font-size-caption);
    color: var(--color-fg-muted);
  }

  .members-value {
    font-weight: 600;
    color: var(--color-fg);
  }

  .tune {
    border-top: 1px solid var(--color-border-line);
    padding-top: var(--gap-sm);
  }

  .tune summary {
    cursor: pointer;
    font-size: var(--font-size-caption-sm);
    font-weight: 600;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: var(--color-fg-muted);
  }

  .tune > .hint {
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

  input[type='range'] {
    width: 100%;
    accent-color: var(--color-fg);
    cursor: pointer;
  }

  .tune-apply {
    display: flex;
    flex-direction: column;
    gap: var(--gap-sm);
    margin-top: var(--gap-md);
  }

  .tune-apply button {
    min-height: 32px;
    padding: 0 var(--gap-md);
    font-size: var(--font-size-caption);
  }
</style>
