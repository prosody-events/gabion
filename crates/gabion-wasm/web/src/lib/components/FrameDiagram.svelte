<script lang="ts">
  import { fmtCount } from '../format';
  import { FLAG_BITS, FRAME_ROWS, HEADER_FIELDS, HEADER_LEN, KIND_WORDS, bodyFields, fieldAt, readLe, type Field } from '../frame';
  import { ROLE_WORD, type MessageDetail } from '../messages';

  // The frame as gabion encodes it: a byte bar to scale when there is a body,
  // the field line, the header in rows that break at field boundaries (each
  // box as wide as its bytes, on one scale), the flag bits by name, the body's
  // sections, and the bytes as hex, linked both ways with the boxes.
  let {
    detail,
    name,
    zoneName,
  }: {
    detail: MessageDetail;
    /** A node's name, by stable id. */
    name: (id: number) => string;
    /** The sender's zone, by name. */
    zoneName: string | undefined;
  } = $props();

  const bytes = $derived(detail.bytes);
  const body = $derived(bodyFields(bytes, detail.header.cells));
  const fields = $derived([...HEADER_FIELDS, ...body]);
  const byId = (id: string): Field => HEADER_FIELDS.find((f) => f.id === id)!;
  const bodyLen = $derived(detail.wire_len - HEADER_LEN);

  /** The field under the pointer or the focus; null at rest. */
  let active = $state<Field | null>(null);

  const hex = (at: number, len: number): string =>
    bytes
      .slice(at, at + len)
      .map((b) => b.toString(16).padStart(2, '0'))
      .join(' ');

  /** A field's value as the box prints it. */
  function valueOf(f: Field): string {
    const h = detail.header;
    switch (f.id) {
      case 'magic':
        return `0x${readLe(bytes, 0, 4).toString(16)}`;
      case 'version':
        return String(readLe(bytes, 4, 2));
      case 'kind':
        return KIND_WORDS[bytes[6]] ?? String(bytes[6]);
      case 'flags':
        return `0x${bytes[7].toString(16).padStart(2, '0')}`;
      case 'cluster':
        return `0x…${readLe(bytes, 8, 16).toString(16).slice(-4)}`;
      case 'sender':
        return h.sender === undefined ? 'unknown' : name(h.sender);
      case 'start':
        return String(h.sender_incarnation);
      case 'zone':
        return h.sender_zone === 0 ? 'none' : (zoneName ?? `0x${h.sender_zone.toString(16)}`);
      case 'cells':
        return fmtCount(h.cells);
      case 'ack-start':
        return h.ack_incarnation === 0 ? 'not heard yet' : String(h.ack_incarnation);
      case 'own-ack':
        return `${h.own_ack}${h.own_ack_as_gateway ? ' · as its gateway' : ''}`;
      case 'relay-ack':
        return String(h.relay_ack);
      case 'lower':
        return h.range === undefined ? 'none' : String(h.range[0]);
      case 'upper':
        return h.range === undefined ? 'none' : String(h.range[1]);
      default:
        return `${fmtCount(f.len)} ${f.len === 1 ? 'byte' : 'bytes'}`;
    }
  }

  const range = (f: Field): string => (f.len === 1 ? `byte ${f.at}` : `bytes ${f.at}–${f.at + f.len - 1}`);

  /** What makes the message its role, at rest. */
  const summary = $derived.by((): string => {
    const set = FLAG_BITS.filter((b) => (detail.header.flags & (1 << b.bit)) !== 0).map((b) => `bit ${b.bit}, ${b.name}`);
    const kind = `byte 6, kind ${KIND_WORDS[bytes[6]] ?? bytes[6]}`;
    return `What makes it ${ROLE_WORD[detail.role] === 'ack' ? 'an' : 'a'} ${ROLE_WORD[detail.role]}: ${[kind, ...set].join('; ')}. Point at any field, or move to it with ← →, for its meaning.`;
  });

  function onKeydown(event: KeyboardEvent, i: number): void {
    if (event.key !== 'ArrowRight' && event.key !== 'ArrowLeft') return;
    event.preventDefault();
    const next = Math.min(Math.max(i + (event.key === 'ArrowRight' ? 1 : -1), 0), fields.length - 1);
    document.getElementById(`field-${fields[next].id}`)?.focus();
  }

  const segments = $derived(
    bodyLen > 0
      ? [
          { label: 'header', len: HEADER_LEN, cls: 'seg-header' },
          { label: 'rules and nodes', len: body.slice(0, 4).reduce((s, f) => s + f.len, 0), cls: 'seg-dict' },
          { label: 'cells', len: body.slice(4).reduce((s, f) => s + f.len, 0), cls: 'seg-cells' },
        ]
      : [],
  );
</script>

<section class="frame" aria-labelledby="frame-title">
  <h3 class="title" id="frame-title">The frame</h3>
  <p class="caption">
    {#if bodyLen > 0}
      <b>{fmtCount(detail.wire_len)}</b> bytes · header <b>{HEADER_LEN}</b> · rules and nodes <b>{segments[1].len}</b> · cells
      <b>{segments[2].len}</b>
    {:else}
      <b>{HEADER_LEN}</b> bytes · header <b>{HEADER_LEN}</b> · no body
    {/if}
  </p>
  {#if bodyLen > 0}
    <div class="bar" aria-hidden="true">
      {#each segments as s (s.label)}
        <span class={s.cls} class:gone={s.label !== 'header' && !detail.body_kept} style:flex-grow={s.len}></span>
      {/each}
    </div>
  {/if}
  <div class="field-line">
    {#if active === null}
      <p>{summary}</p>
    {:else}
      <p><b>{active.plain}</b> · {range(active)} · <code data-wire>{active.wire}</code></p>
      <p class="muted"><code>{hex(active.at, Math.min(active.len, 16))}{active.len > 16 ? ' …' : ''}</code> = {valueOf(active)}</p>
      <p>{active.says}</p>
    {/if}
  </div>
  <div class="grid" role="list" aria-label="The header's fields">
    {#each FRAME_ROWS as row, r (r)}
      {@const first = byId(row[0])}
      <div class="grid-row">
        <span class="offset">{first.at}</span>
        <div class="boxes">
          {#each row as id (id)}
            {@const f = byId(id)}
            {@const i = HEADER_FIELDS.indexOf(f)}
            <div class="cell" role="listitem" style:flex-basis={`${(f.len / 16) * 100}%`}>
            <button
              type="button"
              class="box"
              class:odd={i % 2 === 1}
              class:active={active === f}
              id={`field-${f.id}`}
              tabindex={i === 0 ? 0 : -1}
              aria-label={`${f.plain}, ${valueOf(f)}, ${range(f)}, ${f.wire}`}
              onpointerenter={() => (active = f)}
              onpointerleave={() => (active = null)}
              onfocus={() => (active = f)}
              onblur={() => (active = null)}
              onkeydown={(e) => onKeydown(e, i)}
            >
              {#if f.len >= 4}
                <span class="name">{f.plain}</span> · <span class="value">{valueOf(f)}</span>
              {:else}
                <!-- Too narrow for a word: the byte's number, the rest on the field line. -->
                <span class="value">{f.id === 'flags' ? '' : f.id === 'kind' ? bytes[6] : valueOf(f)}</span>
              {/if}
            </button>
            </div>
          {/each}
        </div>
      </div>
    {/each}
  </div>
  <ul class="bits" aria-label="The flags">
    {#each FLAG_BITS as b (b.bit)}
      {@const set = (detail.header.flags & (1 << b.bit)) !== 0}
      <li class:set aria-label={`bit ${b.bit}, ${b.name}, ${set ? 'set' : 'clear'}`}><span class="figure">{set ? 1 : 0}</span> {b.name}</li>
    {/each}
  </ul>
  {#if body.length > 0}
    <ul class="body" aria-label="The body">
      <li>rules · {bytes[HEADER_LEN] ?? 0} ({fmtCount(body[0].len + body[1].len)} bytes)</li>
      <li>nodes · {body[2] === undefined ? 0 : (bytes[body[2].at] ?? 0)} ({fmtCount(body[2].len + body[3].len)} bytes)</li>
      <li>cells · {fmtCount(detail.header.cells)} × 38 bytes</li>
    </ul>
  {:else if bodyLen > 0}
    <p class="muted">Its cells are no longer kept: newer messages reused that memory.</p>
  {/if}
  <details class="hex">
    <summary>Show the {fmtCount(detail.wire_len)} bytes</summary>
    <div class="dump">
      {#each { length: Math.ceil(bytes.length / 16) } as _, line (line)}
        <div class="line">
          <span class="offset">{line * 16}</span>
          {#each bytes.slice(line * 16, line * 16 + 16) as b, k (k)}
            {@const at = line * 16 + k}
            {@const f = fieldAt(fields, at)}
            <span
              class="byte mono"
              class:odd={f !== undefined && fields.indexOf(f) % 2 === 1}
              class:active={f !== undefined && active === f}
              class:group={k % 4 === 0 && k > 0}
              role="presentation"
              onpointerenter={() => (active = f ?? null)}
              onpointerleave={() => (active = null)}>{b.toString(16).padStart(2, '0')}</span
            >
          {/each}
        </div>
      {/each}
      {#if !detail.body_kept && bodyLen > 0}
        <p class="muted">the remaining {fmtCount(bodyLen)} bytes are no longer kept</p>
      {/if}
    </div>
  </details>
</section>

<style>
  .frame {
    display: flex;
    flex-direction: column;
    gap: var(--gap-sm);
  }

  .title {
    margin: 0;
    font-size: var(--font-size-caption);
    font-weight: 600;
  }

  .caption,
  .field-line p,
  .body,
  .bits {
    margin: 0;
    font-size: var(--font-size-caption-sm);
    font-variant-numeric: tabular-nums;
  }

  .caption b,
  .field-line b {
    font-weight: 600;
  }

  .bar {
    display: flex;
    gap: 1px;
    height: 8px;
    margin-left: 28px;
  }

  .bar span {
    min-width: 2px;
  }

  .seg-header {
    background: var(--chart-muted);
  }

  .seg-dict {
    background: var(--chart-axis);
  }

  .seg-cells {
    background: var(--chart-grid);
    outline: 1px solid var(--chart-axis);
  }

  .bar .gone {
    background: none;
    outline: 1px solid var(--chart-axis);
  }

  .field-line {
    position: sticky;
    top: 0;
    z-index: 1;
    min-height: 60px;
    padding: var(--pad-xs) 0;
    background: var(--color-panel);
    border-bottom: 1px solid var(--color-border-line);
  }

  .muted {
    color: var(--color-fg-muted);
  }

  code {
    font-size: var(--font-size-caption-sm);
  }

  .grid {
    display: flex;
    flex-direction: column;
  }

  .grid-row {
    display: flex;
    align-items: stretch;
  }

  .offset {
    flex: none;
    width: 24px;
    margin-right: 4px;
    text-align: right;
    font-size: var(--font-size-caption-sm);
    color: var(--color-fg-muted);
    font-variant-numeric: tabular-nums;
  }

  .boxes {
    display: flex;
    flex: 1;
    min-width: 0;
  }

  .cell {
    display: flex;
    flex-grow: 0;
    flex-shrink: 0;
  }

  .box {
    width: 100%;
    color: var(--color-fg);
    cursor: default;
    box-sizing: border-box;
    min-height: 24px;
    padding: 2px 4px;
    overflow: hidden;
    border: 1px solid var(--chart-axis);
    margin: 0 -1px -1px 0;
    background: var(--color-panel);
    font-size: var(--font-size-caption-sm);
    line-height: 1.6;
    text-align: center;
    white-space: nowrap;
    font-variant-numeric: tabular-nums;
  }

  .box.odd,
  .byte.odd {
    background: var(--color-bg);
  }

  .box.active,
  .byte.active {
    outline: 1.5px solid var(--color-fg);
    outline-offset: -1.5px;
  }

  .box:focus-visible {
    outline: 2px solid var(--color-fg);
    outline-offset: 2px;
  }

  .name {
    color: var(--color-fg-muted);
  }

  .bits {
    display: grid;
    grid-template-columns: 1fr 1fr;
    grid-auto-flow: column;
    grid-template-rows: repeat(4, 20px);
    padding: 0;
    list-style: none;
    color: var(--color-fg-muted);
  }

  .bits li.set {
    color: var(--color-fg);
    font-weight: 600;
  }

  .figure {
    display: inline-block;
    width: 1ch;
  }

  .body {
    padding: 0;
    list-style: none;
    color: var(--color-fg-muted);
  }

  .hex summary {
    font-size: var(--font-size-caption);
    cursor: pointer;
  }

  .dump {
    font-size: var(--font-size-caption-sm);
  }

  .line {
    display: flex;
    gap: 2px;
  }

  .byte.group {
    margin-left: 6px;
  }
</style>
