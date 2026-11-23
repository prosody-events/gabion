<script lang="ts">
  // The time dock: Play/Pause, Step to the next event, then Send, then Speed,
  // and the simulated-time readout, which says while a run plays itself; once
  // a heartbeat period has played, how long it took to watch at the chosen
  // speed. Whichever control the next action names is filled (`primary`); the
  // others are secondary. A disabled control says why beside it.
  import { fmtCount, fmtDuration, type Blurb } from '../format';
  import { BURST_REQUESTS_MAX } from '../presets';
  import type { Control } from '../next';
  import Icon from './Icon.svelte';
  import Sentence from './Sentence.svelte';
  import Speed from './Speed.svelte';

  let {
    playing,
    busy,
    speed = $bindable(),
    virtualMs,
    heartbeatWallMs,
    sendLabel,
    sendValid,
    primary,
    bounded,
    phone,
    onToggle,
    onStep,
    onSend,
    onSendHover,
  }: {
    playing: boolean;
    /** The engine is starting: the controls wait for it. */
    busy: boolean;
    speed: number;
    virtualMs: number;
    /** Wall ms the last whole heartbeat period took to play, or null before
     *  one has. */
    heartbeatWallMs: number | null;
    /** Send's label at this width, from `sendLabel`. */
    sendLabel: Blurb;
    /** Whether Requests per Send holds a valid committed value. */
    sendValid: boolean;
    /** The control the next action names, or null. */
    primary: Control | null;
    /** A command's bounded run is playing. */
    bounded: boolean;
    /** Below 640 px: Play, Step and Send only, one sticky row; Speed is in
     *  the Controls disclosure. */
    phone: boolean;
    onToggle: () => void;
    onStep: () => void;
    onSend: () => void;
    /** Send names its target: hovering or focusing it previews that node. */
    onSendHover: (on: boolean) => void;
  } = $props();

  const describe = (control: Control, reason: string | null): string | undefined =>
    [primary === control ? 'next-action' : null, reason].filter((id) => id !== null).join(' ') || undefined;
  const starting = $derived(busy ? 'transport-reason' : null);
</script>

<div class="transport" role="group" aria-label="Playback controls">
  <button
    class={primary === 'play' ? undefined : 'btn-secondary'}
    data-primary={primary === 'play' ? '' : undefined}
    onclick={onToggle}
    disabled={busy}
    aria-keyshortcuts="Space"
    aria-describedby={describe('play', starting)}
    title="Space plays or pauses"
  >
    <Icon name={playing ? 'pause' : 'play'} />{playing ? 'Pause' : 'Play'}
  </button>
  <button
    class={primary === 'step' ? undefined : 'btn-secondary'}
    data-primary={primary === 'step' ? '' : undefined}
    onclick={onStep}
    disabled={busy}
    aria-label="Step to the next event"
    aria-keyshortcuts="ArrowRight"
    aria-describedby={describe('step', starting)}
    title="→ steps to the next event"
  >
    <Icon name="step" />Step
  </button>
  <button
    class="send {primary === 'send' ? '' : 'btn-secondary'}"
    data-primary={primary === 'send' ? '' : undefined}
    onclick={onSend}
    disabled={busy || !sendValid}
    aria-describedby={describe('send', starting ?? (sendValid ? null : 'send-reason'))}
    onpointerenter={() => onSendHover(true)}
    onpointerleave={() => onSendHover(false)}
    onfocus={() => onSendHover(true)}
    onblur={() => onSendHover(false)}
  >
    <Sentence parts={sendLabel} field="send" />
  </button>
  {#if busy}
    <span class="reason" id="transport-reason">starting the simulator…</span>
  {:else if !sendValid}
    <span class="reason" id="send-reason"
      >Requests per Send must be a whole number from <span data-field="requests-min">{1}</span> to&nbsp;<span data-field="requests-max"
        >{fmtCount(BURST_REQUESTS_MAX)}</span
      ></span
    >
  {/if}

  {#if !phone}
    <span class="speed"><Speed bind:speed /></span>
  {/if}

  <div class="readout">
    <!-- Its space is kept before the first period has played, so the bar
         never reflows when it appears. -->
    <span class="readout-item heartbeat" class:pending={heartbeatWallMs === null}>
      a heartbeat took
      <span data-field="heartbeat-wall" data-ms={heartbeatWallMs ?? undefined}>{fmtDuration(heartbeatWallMs ?? 0)}</span>
      to watch
    </span>
    <span class="readout-item time">
      <span class="time-value" data-field="virtual-time" data-ms={virtualMs}>{fmtDuration(virtualMs)}</span>
      simulated{#if bounded}{' · playing until every node has it'}{/if}
    </span>
  </div>
</div>

<style>
  .transport {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: var(--gap-sm);
    padding: var(--gap-sm) var(--gap-md);
    background: var(--color-panel);
    border-top: 1px solid var(--color-border-line);
  }

  button {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: var(--gap-xs);
    min-height: 36px;
    padding: 0 var(--gap-md);
    border-radius: var(--radius);
    font-size: var(--font-size-caption);
    font-weight: 600;
  }

  button:first-child {
    min-width: 6.5em;
  }

  /* Send keeps its distance from the time controls. */
  .send {
    margin-left: var(--gap-sm);
  }

  .reason {
    font-size: var(--font-size-caption);
    color: var(--color-fg-muted);
  }

  .speed {
    margin-left: var(--gap-sm);
  }

  .readout {
    margin-left: auto;
    display: flex;
    align-items: baseline;
    gap: var(--gap-md);
    font-size: var(--font-size-caption);
    color: var(--color-fg-muted);
  }

  .readout [data-field] {
    color: var(--color-fg);
    font-weight: 600;
  }

  .time {
    text-align: right;
  }

  .pending {
    visibility: hidden;
  }

  /* The phone's bar: one row of Play, Step and Send, 8 px apart, in the thumb
     zone. The readout keeps its value for the page's own use but is not
     shown. */
  @media (width < 640px) {
    .send {
      margin-left: 0;
    }

    button:first-child {
      min-width: 0;
    }

    .readout {
      display: none;
    }
  }
</style>
