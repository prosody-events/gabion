// The PixiJS stage renderer: a framework-agnostic class that owns the WebGL
// canvas and turns simulation state into the tier view. Kept out of Svelte's
// reactivity on purpose: Svelte feeds it `setLayout` (where every node sits),
// `setScale` (where the limit sits on the vessels), `setCluster` (each node's
// window and ring), `setRoles`, `setFaults`, `setPreview` and `setAwaiting`
// (marks) and `show` and `reject` (the packets of an engine batch, a refused
// request); the class owns all imperative Pixi/GSAP bookkeeping and cleanup.
//
// Layers, back to front (design rule 19): the zone ring guides, the landed
// routes, the names with a halo, the beams in flight, then the nodes. A name's
// halo knocks out the ground (guides, history), while a packet in flight, the
// figure, passes over a zone title rather than vanishing under it; every name
// is placed clear of the discs and their marks. Nodes are keyed by stable id:
// `setLayout` reconciles the live id set against what is on stage, so a join
// fades in at its ring slot, a leave scales out (its beams cancelled) and
// every survivor glides to the slot the new layout gives it.
//
// Each node is a vessel inside a ring (design language, "The node glyph"):
// the disc fills from the bottom to the node's own windowed count over 1.25
// limits, in one stratum per window bucket, with the brim at the limit; the
// ring is the share of the cluster's windowed hits the node has heard, closed
// when it holds them all; the selected node wears an outline, and its gateways
// a port on their rims facing the zone each relays into. Every value a
// drawing routine draws is written into `GlyphTable` first and drawn from
// there, so the audit reads the drawn geometry, not a recomputation.
//
// Beams fly on a presentation clock of their own (`BeamPool.clock`), which
// advances with the ticker's wall time times the speed while playing, and only
// to a bound (`finish`) while paused, so Speed stretches a flight and Pause
// freezes it. A held wave's landing is the renderer's to report: the tick that
// sets the last of its beams' `LANDED` bit calls `onLanded`, and only then does
// the page let the engine deliver it. A landed data route stays in the pool,
// drawn 1 px and quiet, until the next command where no feed runs. A ring's
// sweep and a refusal play on the same clock, which runs for them while paused.
//
// Every beam is one record in a pre-sized pool, drawn along the path
// `beamPath` gives it into one Graphics per mark style, and the beam layer is
// redrawn only when something changed or a beam is in flight; every node's
// Graphics are built once and redrawn only when what they show changes, so a
// frame allocates nothing of its own however many packets fly.

import { gsap } from 'gsap';
import { Application, Container, Graphics, Text, TextStyle, type Ticker } from 'pixi.js';
import { zoneLabel } from '../format';
import { brim as brimLine, chord, dashedRing, hatch, port as portGlyph, stopBar } from '../glyphs';
import { VESSEL_SCALE } from '../limit';
import { CARRIES_CELLS, type Admission, type ClusterState, type Lag, type PacketKind, type SimEvent } from '../sim/types';
import { BEAM_ALPHA, FADED_ALPHA, LANDED_ALPHA, type Theme } from '../theme';
import {
  ARC_GAP,
  ARC_WIDTH,
  BEAM_FLIGHT_MS,
  BRIM_OVERHANG,
  BRIM_WIDTH,
  CAP_WIDTH,
  CONTROL,
  CROSS,
  CROSS_WIDTH,
  DROPPED,
  IN_WIDTH,
  LABEL_GAP,
  LABEL_SPACING,
  LANDED,
  PORT_GAP,
  PREVIEW_DASH,
  RELAY,
  RING_REACH,
  SELECTION_GAP,
  SELECTION_REACH,
  SELECTION_WIDTH,
  STRATA_MIN_INNER,
  STRATUM_GAP,
  STRATUM_MIN,
  beamPath,
  pointOn,
  portAt,
  portHalf,
  type Point,
  type QuadPath,
  type Spot,
  type StageLayout,
} from './layout';

// The least share of a ring an arc leaves open while its node is behind, so
// one hit short of the total still shows a notch (design rule 17).
const ARC_NOTCH = 1 / 12;
// How long a ring sweeps from its old share to its new one when a packet
// lands, and how long the refusal plays, on the presentation clock.
const SWEEP_MS = 250;
export const BOUNCE_MS = 600;
// The refusal's rebound, as a share of its drop, and the hatch's pitch under
// reduced motion, px.
const REBOUND = 0.3;
const HATCH_STEP = 4;
// The segments the heartbeat sweep reveals round the selection outline.
const SWEEP_SEGMENTS = 32;
// The most nodes the stage holds at once (the page caps live nodes at 100),
// and the most window buckets a vessel draws.
const MAX_STAGE_NODES = 128;
const MAX_BUCKETS = 32;

// The wall time a command's fast-forward of the waves in flight takes, at any
// speed.
const LAND_FAST_MS = 150;
// A flying stroke's tail, as a share of its route.
const TAIL = 0.26;
// A dropped packet dies partway across instead of landing: a half-line ending
// in a stop bar, told by shape and by the muted colour, never by a fainter
// alpha: every beam peaks at `BEAM_ALPHA`.
const DROP_FRACTION = 0.5;
const STOP_BAR = 12;
// Segments of a route drawn whole: landed, lost, or under reduced motion.
const ROUTE_SEGMENTS = 8;
// A cross-zone arrowhead's length and half width, and a control dot's radius, px.
const ARROW_LEN = 9;
const ARROW_HALF = 4.5;
const DOT_R = 2;
// The most beams the pool holds, landed routes included. A beam in flight is
// bounded by the sends of one flight window and a landed route by the waves
// since the last command; a 100-node sustained overload peaks in the low tens
// of thousands, so this leaves an order of magnitude of headroom. When full,
// the oldest landed route makes room; with none, the new beam is dropped
// rather than the pool grown.
const BEAM_CAPACITY = 32768;

// A name's halo, px each side.
const HALO = 2;

// Membership-change motion (join / leave / re-space). Purposeful only: a join
// grows in, a leave shrinks out, survivors glide. Reduced motion skips all of
// it and snaps to the final layout.
const ENTER_MS = 420;
const EXIT_MS = 340;
const GLIDE_MS = 520;
const ENTER_SCALE = 0.25;

/** Each packet kind's flags: a kind that carries no cells is a dot, and a
 *  relay is the selected node's own when its gateway sends it. Read from
 *  `CARRIES_CELLS`, the mirror of `PacketKind::carries_cells`, and exhaustive
 *  through `CARRIES_CELLS`, so a new kind is a type error until it is
 *  classed. */
const kindFlags = (kind: PacketKind): number =>
  (CARRIES_CELLS[kind] ? 0 : CONTROL) | (kind === 'Relay' ? RELAY : 0);

// Beam mark styles; each has a full and a faded Graphics (`style * 2 + faded`).
const IN_ZONE = 0;
const CROSS_ZONE = 1;
const DROPPED_CROSS = 2;
const DOTS = 3;
const ARROWS = 4;
const LANDED_IN = 5;
const LANDED_CROSS = 6;
const STYLES = 7;

/** The kinds of mark the stage draws now, as bits: the key lists exactly
 *  these, in this order. */
export const MARK = {
  COUNT: 1,
  FAR: 2,
  RING: 4,
  SELECTED: 8,
  PORT: 16,
  SUSPECT: 32,
  CAP: 64,
  LOST: 128,
  DOT: 256,
} as const;

/** A ring's colour role: caught up (neutral, closed) or behind (amber arc). */
export const RING_CAUGHT_UP = 1;
export const RING_BEHIND = 2;

/** What becomes of a data route once it lands: kept, quiet, until the next
 *  command (`'keep'`, presets with no feed, so a Send's waves read as a
 *  diagram), or dropped at once (`'drop'`, where a feed sends all the time). */
export type Trail = 'keep' | 'drop';
/** Whether the page waits on a batch's landing: `'held'` calls `onLanded` once
 *  its beams have all landed (after one flight, even if none is drawn). */
export type Landing = 'held' | 'free';
/** How a paused clock runs to its bound: at the speed, or within
 *  `LAND_FAST_MS` of wall time. */
export type Finish = 'at-speed' | 'fast';

/** The selected node's marks: the node itself and each gateway it sends a
 *  remote zone's changes through, `suspect` once its peer has left something
 *  unanswered or said it cannot relay. */
export interface Roles {
  selected: number;
  gateways: ReadonlyMap<number, { zone: number; suspect: boolean }>;
}

/** Where the limit sits on the vessels: the rule's limit, whether it is drawn
 *  at all, and the window's bucket count. */
export interface Scale {
  limit: number;
  onScale: boolean;
  /** Hits per Send: whether a request's stratum is tall enough to draw. */
  hits: number;
  buckets: number;
}

/** What a named node's label says about it beyond its name. */
export type Fault = { kind: 'silenced' } | { kind: 'unheard'; zone: number };

/** One label's box in CSS px of the canvas, halo included, as published for the
 *  design audit. */
export interface LabelBox {
  text: string;
  x: number;
  y: number;
  w: number;
  h: number;
  size: number;
}

/** How one beam Graphics is painted: stroked or filled, in what style. */
interface Brush {
  stroke: boolean;
  style: { color: number; alpha: number; width?: number; cap?: 'round' | 'butt' };
}

/** A stroke the node marks are drawn with, built once per theme. */
type Stroke = { width: number; color: number; alpha: number; cap?: 'butt' | 'round' };

/** One node's persistent display objects, keyed by stable id. Every Graphics
 *  and the name are built in `#createNode` and redrawn in place. */
interface NodeGfx {
  id: number;
  /** Its row in `GlyphTable`. */
  slot: number;
  root: Container;
  vessel: Graphics;
  ring: Graphics;
  port: Graphics;
  selection: Graphics;
  preview: Graphics;
  refusal: Graphics;
  mask: Graphics;
  label: Text;
  zone: number;
  /** The slot this node is gliding to (stage px). */
  center: Point;
  /** The disc radius the geometry was last drawn at. */
  radius: number;
  /** The window row the vessel was last drawn from, its length, the total and
   *  whether the cap was drawn: a snapshot that changes none redraws nothing. */
  row: Float64Array;
  rowLen: number;
  total: number;
  capped: boolean;
  /** The ring: its share drawn now, the sweep's ends and start on the
   *  presentation clock, whether it sweeps, and its role once settled. */
  arc: number;
  arcFrom: number;
  arcTo: number;
  arcStart: number;
  sweeping: boolean;
  behind: boolean;
  /** The port's centre from the node's and the angle it points along, while
   *  it wears one. */
  portX: number;
  portY: number;
  portAngle: number;
  ported: boolean;
  /** Live tweens (enter / exit / glide), killed on destroy so a stale tween
   *  can never touch a freed display object. */
  tweens: Set<gsap.core.Tween>;
}

/** Every beam on the stage, in flight or landed, as columns of plain data: a
 *  beam's route (`from`, `control`, `to` as six floats), its start on the
 *  presentation clock, its ends and flags. Removal swaps the last beam in, so
 *  the pool stays dense and nothing is allocated. Published on the canvas as
 *  `beams`, where the audit wraps `push` and `land` to see each beam enter and
 *  land, and reads `clock`, `redraws`, `inFlight` and `due`. */
export class BeamPool {
  len = 0;
  /** The presentation clock, ms: every `start` is on it. */
  clock = 0;
  /** Redraws of the beam layer. */
  redraws = 0;
  /** Beams not yet landed. */
  inFlight = 0;
  /** When the held wave lands on the clock, or null while nothing is held. */
  due: number | null = null;
  readonly route = new Float32Array(BEAM_CAPACITY * 6);
  readonly start = new Float64Array(BEAM_CAPACITY);
  readonly src = new Int32Array(BEAM_CAPACITY);
  readonly dst = new Int32Array(BEAM_CAPACITY);
  readonly flags = new Uint8Array(BEAM_CAPACITY);

  push(path: QuadPath, start: number, src: number, dst: number, flags: number): void {
    let i = this.len;
    if (i === BEAM_CAPACITY) {
      // Full: the oldest landed route makes room; with none, the beam is dropped.
      if (this.inFlight === this.len) return;
      i = this.#oldestLanded();
    } else {
      this.len += 1;
    }
    const o = i * 6;
    this.route[o] = path.from.x;
    this.route[o + 1] = path.from.y;
    this.route[o + 2] = path.control.x;
    this.route[o + 3] = path.control.y;
    this.route[o + 4] = path.to.x;
    this.route[o + 5] = path.to.y;
    this.start[i] = start;
    this.src[i] = src;
    this.dst[i] = dst;
    this.flags[i] = flags;
    this.inFlight += 1;
  }

  /** Mark beam `i` landed: the frame it reaches its target. */
  land(i: number): void {
    this.flags[i] |= LANDED;
    this.inFlight -= 1;
  }

  /** Drop beam `i` by moving the last beam into its slot. */
  removeAt(i: number): void {
    if ((this.flags[i] & LANDED) === 0) this.inFlight -= 1;
    const last = this.len - 1;
    if (i !== last) {
      this.route.copyWithin(i * 6, last * 6, last * 6 + 6);
      this.start[i] = this.start[last];
      this.src[i] = this.src[last];
      this.dst[i] = this.dst[last];
      this.flags[i] = this.flags[last];
    }
    this.len = last;
  }

  /** Empty the pool. */
  clear(): void {
    this.len = 0;
    this.inFlight = 0;
  }

  /** The landed route that started first: the one a full pool overwrites.
   *  Called only with at least one landed route; a scan, paid only when the
   *  pool is full. */
  #oldestLanded(): number {
    let oldest = -1;
    for (let i = 0; i < this.len; i++) {
      if ((this.flags[i] & LANDED) !== 0 && (oldest < 0 || this.start[i] < this.start[oldest])) oldest = i;
    }
    return oldest;
  }
}

/** Every node mark as drawn, one row per stage slot, as columns of plain data:
 *  set once and published on the canvas as `glyphs`, so the audit reads what
 *  was drawn and no frame or snapshot allocates to publish it. Vessel heights
 *  are px from the disc's centre, y down; port centres are stage px; `NaN`
 *  marks a part not drawn. */
export class GlyphTable {
  /** Rows in use: the highest slot ever held, plus one. A free row's id is −1. */
  len = 0;
  readonly id = new Int32Array(MAX_STAGE_NODES).fill(-1);
  readonly x = new Float32Array(MAX_STAGE_NODES);
  readonly y = new Float32Array(MAX_STAGE_NODES);
  readonly r = new Float32Array(MAX_STAGE_NODES);
  /** The fill's top and the brim's height. */
  readonly fillTop = new Float32Array(MAX_STAGE_NODES).fill(Number.NaN);
  readonly brimY = new Float32Array(MAX_STAGE_NODES).fill(Number.NaN);
  /** Strata drawn (0 when merged into one level) and whether the brim is the cap. */
  readonly strata = new Uint8Array(MAX_STAGE_NODES);
  readonly cap = new Uint8Array(MAX_STAGE_NODES);
  /** The ring's colour role (`RING_CAUGHT_UP` or `RING_BEHIND`) and the share
   *  its arc shows this frame, sweeps included. */
  readonly ring = new Uint8Array(MAX_STAGE_NODES);
  readonly arc = new Float32Array(MAX_STAGE_NODES);
  /** A port's centre in stage px, and whether it is hollow (suspected). */
  readonly portX = new Float32Array(MAX_STAGE_NODES).fill(Number.NaN);
  readonly portY = new Float32Array(MAX_STAGE_NODES).fill(Number.NaN);
  readonly portSuspect = new Uint8Array(MAX_STAGE_NODES);
  /** The refusal in flight: its slot (−1 while none) and its band's top and
   *  bottom this frame. */
  bounce = -1;
  bandTop = Number.NaN;
  bandBottom = Number.NaN;
  /** The selected and the previewed node's slots, −1 for none, and the dash
   *  pattern the preview is drawn with. */
  selected = -1;
  preview = -1;
  readonly previewDash: readonly [number, number] = PREVIEW_DASH;
  /** `MARK` bits of what is drawn now. */
  marks = 0;
  /** The limit the vessels are drawn against, and whether it is drawn. */
  limit = 1;
  onScale = 0;
}

export class StageRenderer {
  readonly #app: Application;
  readonly #nodeLayer: Container;
  readonly #labelLayer: Container;
  readonly #reduceMotion: boolean;
  readonly #guides: Graphics;
  // One Graphics per beam mark style and fade, each redrawn by `#drawBeams`.
  readonly #beamGfx: Graphics[] = [];
  readonly #pool = new BeamPool();
  readonly #glyphs = new GlyphTable();
  // The heartbeat sweep: segments of the selection outline, revealed as the
  // selected node's next heartbeat nears, in one container that moves with
  // the selection.
  readonly #sweep: Container;
  readonly #segments: Graphics[] = [];
  // How each beam Graphics is painted once its marks are added, and every node
  // stroke and text style, rebuilt with the theme so neither a frame nor a
  // snapshot builds a style of its own.
  #brushes: Brush[] = [];
  #ringCaughtUp: Stroke = { width: ARC_WIDTH, color: 0, alpha: 1 };
  #ringTrack: Stroke = { width: 1, color: 0, alpha: 1 };
  #ringArc: Stroke = { width: ARC_WIDTH, color: 0, alpha: 1, cap: 'butt' };
  #brimStroke: Stroke = { width: BRIM_WIDTH, color: 0, alpha: 1 };
  #capStroke: Stroke = { width: CAP_WIDTH, color: 0, alpha: 1 };
  #strataStroke: Stroke = { width: STRATUM_GAP, color: 0, alpha: 1 };
  #selectionStroke: Stroke = { width: SELECTION_WIDTH, color: 0, alpha: 1 };
  #previewStroke: Stroke = { width: SELECTION_WIDTH, color: 0, alpha: 1, cap: 'butt' };
  #hatchStroke: Stroke = { width: 1.5, color: 0, alpha: 1 };
  #nameStyle = new TextStyle();
  #namedStyle = new TextStyle();
  #titleStyle = new TextStyle();
  // Scratch the beam paths, the frame draw and the port placement reuse.
  readonly #path: QuadPath = { from: { x: 0, y: 0 }, control: { x: 0, y: 0 }, to: { x: 0, y: 0 } };
  readonly #at: Point = { x: 0, y: 0 };
  readonly #dir: Point = { x: 0, y: 0 };
  readonly #from: Spot = { id: 0, zone: 0, x: 0, y: 0, r: 0 };
  readonly #to: Spot = { id: 0, zone: 0, x: 0, y: 0, r: 0 };
  readonly #port = { x: 0, y: 0, angle: 0 };
  #theme: Theme;

  #nodes = new Map<number, NodeGfx>();
  // The nodes by slot, so a frame visits them by index, with no iterator.
  readonly #bySlot: (NodeGfx | undefined)[] = new Array(MAX_STAGE_NODES).fill(undefined);
  readonly #freeSlots: number[] = [];
  #titles: Text[] = [];
  #layout: StageLayout | null = null;
  #zoneNames: readonly string[] = [];
  #cluster: ClusterState | null = null;
  #roles: Roles | null = null;
  #faults: ReadonlyMap<number, Fault> = new Map();
  #scale: Scale = { limit: 1, onScale: false, hits: 1, buckets: 1 };
  // A lag younger than this does not open a ring: the feed's newest hits.
  #grace = 0;
  // Rings sweeping now, so a frame with none visits no node.
  #sweeps = 0;
  #gliding = new Set<number>();
  // The disc radius the heartbeat sweep was drawn for.
  #sweepRadius = 0;

  // The presentation clock's drive: whether it runs freely (playing), how
  // fast, and the bound it may still run to while paused (`finish`), at the
  // rate a fast-forward to it needs (0 when none; read only while the clock is
  // short of the bound). A sweep or a refusal also runs the clock, to
  // `#animEnd`, at the speed.
  #running = false;
  #speed = 1;
  #until = 0;
  #fastRate = 0;
  #animEnd = 0;
  // The latest instant any beam shown, or any hold, ends.
  #flightEnd = 0;
  #trail: Trail = 'drop';
  // The beam layer must be redrawn though no beam moved.
  #dirty = false;
  // The refusal: its node, its start on the clock and its drop, px.
  #bounceGfx: NodeGfx | null = null;
  #bounceStart = 0;
  #bounceDrop = 0;
  #bounceRest = 0;
  #bounceHeight = 0;
  #bounceHits = 0;
  // Marks a lost packet or a dot drew since the last command.
  #beamMarks = 0;
  readonly #onLanded: () => void;
  readonly #onMarks: (marks: number) => void;
  readonly #onGliding: (ids: readonly number[]) => void;

  private constructor(app: Application, theme: Theme, hooks: Hooks) {
    this.#app = app;
    this.#theme = theme;
    this.#onLanded = hooks.onLanded;
    this.#onMarks = hooks.onMarks;
    this.#onGliding = hooks.onGliding;
    this.#reduceMotion =
      typeof window !== 'undefined' &&
      window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    this.#guides = new Graphics();
    this.#nodeLayer = new Container();
    this.#labelLayer = new Container();
    const history = new Container();
    const flight = new Container();
    const root = new Container();
    root.addChild(this.#guides, history, this.#labelLayer, flight, this.#nodeLayer);
    this.#app.stage.addChild(root);

    // Parent the beam Graphics before registering the ticker, so the first
    // tick never fires against an orphaned object. The landed styles are the
    // last two.
    for (let i = 0; i < STYLES * 2; i++) {
      const g = new Graphics();
      this.#beamGfx.push(g);
      (i < LANDED_IN * 2 ? flight : history).addChild(g);
    }
    this.#sweep = new Container();
    this.#sweep.visible = false;
    for (let i = 0; i < SWEEP_SEGMENTS; i++) {
      const g = new Graphics();
      this.#segments.push(g);
      this.#sweep.addChild(g);
    }
    for (let slot = MAX_STAGE_NODES - 1; slot >= 0; slot--) this.#freeSlots.push(slot);
    this.#restyle();
    this.#app.ticker.add(this.#tick, this);
    // The beam pool, the Graphics it is drawn into and the glyph table, read
    // by the design audit as Chart.svelte's `uplot` is: set once, so a frame
    // pays nothing.
    Object.assign(this.#app.canvas, { beams: this.#pool, beamGfx: this.#beamGfx, glyphs: this.#glyphs });
  }

  /** Build the renderer and attach its canvas to `container`. Async because
   *  Pixi's WebGL/WebGPU context init is async. */
  static async create(container: HTMLElement, theme: Theme, hooks: Hooks): Promise<StageRenderer> {
    const app = new Application();
    await app.init({
      background: theme.bg,
      antialias: true,
      // Force WebGL: in headless chromium the WebGPU autodetect can land in a
      // half-fallback that renders nothing, which would blind the screenshot
      // gate. WebGL renders reliably under SwiftShader.
      preference: 'webgl',
      // Render at device resolution so discs, arcs, and beams stay crisp on
      // retina displays.
      autoDensity: true,
      resolution: typeof window !== 'undefined' ? window.devicePixelRatio : 1,
    });
    app.canvas.style.display = 'block';
    app.canvas.style.width = '100%';
    app.canvas.style.height = '100%';
    container.appendChild(app.canvas);
    return new StageRenderer(app, theme, hooks);
  }

  /** Resize the canvas to `width × height` CSS pixels. */
  resize(width: number, height: number): void {
    if (width > 0 && height > 0) this.#app.renderer.resize(width, height);
  }

  /** Place every node: enter newcomers, exit departed ones, glide survivors to
   *  their slots, redraw the ring guides and place the names. `zoneNames` are
   *  the engine's zone names, one per ring. Publishes the stage centre, the
   *  ring geometry and the label boxes on the canvas for the design audit. */
  setLayout(layout: StageLayout, zoneNames: readonly string[]): void {
    this.#layout = layout;
    this.#zoneNames = zoneNames;
    const live = new Set<number>();
    let resized = false;
    for (const spot of layout.spots) {
      live.add(spot.id);
      const target = { x: spot.x, y: spot.y };
      const gfx = this.#nodes.get(spot.id);
      if (gfx === undefined) {
        this.#createNode(spot, target);
      } else {
        this.#glide(gfx, target);
        gfx.zone = spot.zone;
        if (gfx.radius !== spot.r) {
          gfx.radius = spot.r;
          this.#drawNode(gfx);
          if (this.#bounceGfx === gfx) this.#shapeRefusal(gfx);
          resized = true;
        }
      }
    }
    // Exit: a node no longer in the layout scales out and is destroyed, its
    // in-flight beams cancelled first.
    for (const [id, gfx] of this.#nodes) {
      if (!live.has(id)) {
        this.#cancelBeamsFor(id);
        this.#removeNode(id, gfx);
      }
    }
    if (this.#titles.length !== layout.rings.length) {
      for (const title of this.#titles) title.destroy();
      this.#titles = layout.rings.map(() => {
        const title = new Text({ text: '', style: this.#titleStyle });
        title.anchor.set(0.5);
        this.#labelLayer.addChild(title);
        return title;
      });
    }
    // A landed route keeps the geometry it flew; history is not worth a
    // re-route, so a new layout drops it.
    this.clearTrail();
    this.#drawGuides();
    if (resized || this.#sweepRadius !== (layout.spots[0]?.r ?? 0)) this.#drawSweep();
    this.#placeRoles();
    this.#placeLabels();
    const dataset = this.#app.canvas.dataset;
    dataset.centre = JSON.stringify(layout.centre);
    dataset.rings = JSON.stringify(layout.rings);
    dataset.crossWidth = String(CROSS_WIDTH);
    if (this.#cluster !== null) this.#syncAll(this.#cluster);
  }

  /** Where the limit sits on the vessels, and how long a feed's newest hits
   *  may lag before they open a ring (0 with no feed). Redraws every vessel. */
  setScale(scale: Scale, grace: number): void {
    this.#scale = scale;
    this.#grace = grace;
    this.#glyphs.limit = scale.limit;
    this.#glyphs.onScale = scale.onScale ? 1 : 0;
    for (const gfx of this.#nodes.values()) this.#drawVessel(gfx, Math.max(gfx.total, 0), gfx.capped);
    if (this.#cluster !== null) this.#syncAll(this.#cluster);
  }

  /** Apply a snapshot: each node's vessel and ring, redrawn only where they
   *  changed. */
  setCluster(state: ClusterState | null): void {
    if (state === null) return;
    this.#cluster = state;
    this.#syncAll(state);
  }

  /** Mark the selected node and its gateways' ports, or clear the marks. */
  setRoles(roles: Roles | null): void {
    const before = this.#roles;
    this.#roles = roles;
    this.#placeRoles();
    // A gateway that takes over a zone's port from another glides it out.
    if (before !== null && roles !== null && before.selected === roles.selected) {
      for (const [id, g] of roles.gateways) {
        if (before.gateways.has(id)) continue;
        for (const [old, was] of before.gateways) {
          if (was.zone === g.zone && !roles.gateways.has(old)) this.#glidePort(id);
        }
      }
    }
    this.#placeLabels();
    this.#dirty = true;
  }

  /** Name the nodes a fault picks out: "node 4 · silenced", "node 4 · can't
   *  reach zone b". */
  setFaults(faults: ReadonlyMap<number, Fault>): void {
    this.#faults = faults;
    this.#placeLabels();
  }

  /** Draw the dashed preview of a selection on node `id`, or on none. */
  setPreview(id: number | null): void {
    let slot = -1;
    for (const gfx of this.#nodes.values()) {
      gfx.preview.visible = gfx.id === id && gfx.id !== this.#roles?.selected;
      if (gfx.preview.visible) slot = gfx.slot;
    }
    this.#glyphs.preview = slot;
  }

  /** Reveal the selection outline as a sweep that closes at the selected
   *  node's next heartbeat: `fraction` of the period gone, or null while it
   *  holds nothing it has not sent. Under reduced motion the outline stays
   *  whole. */
  setAwaiting(fraction: number | null): void {
    const sweep = fraction !== null && !this.#reduceMotion;
    this.#sweep.visible = sweep;
    const selected = this.#roles === null ? undefined : this.#nodes.get(this.#roles.selected);
    if (selected !== undefined) selected.selection.visible = !sweep;
    if (!sweep) return;
    const shown = Math.ceil(Math.min(Math.max(fraction, 0), 1) * SWEEP_SEGMENTS);
    for (let i = 0; i < SWEEP_SEGMENTS; i++) this.#segments[i].visible = i < shown;
  }

  /** Play node `id`'s refusal: the `hits` it would not admit, as a band in its
   *  vessel's headroom that drops onto the brim, rebounds and fades, clipped
   *  to the disc; under reduced motion, the band hatched and at rest. Nothing
   *  where the limit is off the scale: the text carries it. */
  reject(id: number, hits: number): void {
    const gfx = this.#nodes.get(id);
    if (gfx === undefined || !this.#scale.onScale) return;
    this.#endBounce();
    const band = gfx.refusal;
    band.visible = true;
    band.alpha = 1;
    band.position.y = 0;
    this.#bounceGfx = gfx;
    this.#bounceStart = this.#pool.clock;
    this.#bounceHits = hits;
    this.#glyphs.bounce = gfx.slot;
    this.#shapeRefusal(gfx);
    this.#animEnd = Math.max(this.#animEnd, this.#pool.clock + BOUNCE_MS);
    this.#app.canvas.dataset.bounce = String(id);
  }

  /** Draw the refusal's band for `gfx`'s radius, from its brim up by the
   *  refused hits, and the bounce's rest and drop with it: on the press, and
   *  again when a relayout resizes the disc mid-bounce. */
  #shapeRefusal(gfx: NodeGfx): void {
    const full = VESSEL_SCALE * this.#scale.limit;
    const brimY = levelY(gfx.radius, this.#scale.limit, full);
    const top = levelY(gfx.radius, this.#scale.limit + this.#bounceHits, full);
    const band = gfx.refusal;
    band.clear();
    if (this.#reduceMotion) {
      hatch(band, gfx.radius, top, brimY, HATCH_STEP);
      band.stroke(this.#hatchStroke);
    } else {
      chord(band, gfx.radius - 1, top, brimY);
      band.fill(this.#theme.bad);
    }
    this.#bounceRest = brimY;
    this.#bounceHeight = brimY - top;
    this.#bounceDrop = brimY + gfx.radius - 1;
    this.#tickRefusal();
  }

  /** Run the presentation clock freely (playing) or stop it. Stopping leaves
   *  a bound `finish` set, so a command's landing completes even if a Pause
   *  arrives in the same moment. */
  setRunning(running: boolean): void {
    this.#running = running;
  }

  /** How fast the presentation clock runs against wall time. */
  setSpeed(speed: number): void {
    this.#speed = speed;
  }

  /** Keep landed data routes until the next command, or drop them at once. */
  setTrail(trail: Trail): void {
    this.#trail = trail;
    if (trail === 'drop') this.clearTrail();
  }

  /** Draw the packets of `events` as beams that leave `delay` ms of
   *  presentation clock from now. With `'held'`, call `onLanded` once they
   *  have all landed (after one flight, even if none is drawn). */
  show(events: readonly SimEvent[], delay: number, landing: Landing): void {
    const start = this.#pool.clock + delay;
    let drawn = false;
    for (let i = 0; i < events.length; i++) {
      const kind = events[i].kind;
      // `PacketDelivered` is the receiver-side echo of a `PacketSent` already
      // drawn at the sender, so it would double the beams: skip it.
      if (kind.type !== 'PacketSent' && kind.type !== 'PacketDropped') continue;
      let flags = kindFlags(kind.kind) | (kind.tier === 'CrossZone' ? CROSS : 0);
      if (kind.type === 'PacketDropped') flags |= DROPPED;
      drawn = this.#beam(kind.src, kind.dst, flags, start) || drawn;
    }
    if (!drawn && landing === 'free') return;
    const end = start + BEAM_FLIGHT_MS;
    if (end > this.#flightEnd) this.#flightEnd = end;
    if (landing === 'held') this.#pool.due = Math.max(this.#pool.due ?? 0, end);
    this.#dirty = true;
  }

  /** Let the clock run, though paused, until every beam shown has landed:
   *  at the speed (`'at-speed'`) or within `LAND_FAST_MS` of wall time
   *  (`'fast'`). */
  finish(pace: Finish): void {
    this.#until = this.#flightEnd;
    this.#fastRate = pace === 'fast' ? (this.#until - this.#pool.clock) / LAND_FAST_MS : 0;
  }

  /** Drop every landed route: each command starts a fresh picture. */
  clearTrail(): void {
    const pool = this.#pool;
    for (let i = pool.len - 1; i >= 0; i--) {
      if ((pool.flags[i] & LANDED) !== 0) pool.removeAt(i);
    }
    this.#beamMarks = 0;
    this.#dirty = true;
  }

  /** Drop every beam, any landing still due and its bound, and the refusal: a
   *  rebuild. */
  reset(): void {
    this.#pool.clear();
    this.#pool.due = null;
    this.#flightEnd = this.#pool.clock;
    this.#until = this.#pool.clock;
    this.#fastRate = 0;
    this.#beamMarks = 0;
    this.#endBounce();
    this.#dirty = true;
  }

  /** Re-tint every mark from `theme` (an OS scheme or `data-theme` change),
   *  immediately, including while paused. */
  setTheme(theme: Theme): void {
    this.#theme = theme;
    this.#restyle();
    this.#dirty = true;
    this.#app.renderer.background.color = theme.bg;
    this.#drawGuides();
    this.#drawSweep();
    for (const gfx of this.#nodes.values()) this.#drawNode(gfx);
    this.#placeRoles();
    this.#placeLabels();
    if (this.#cluster !== null) this.#syncAll(this.#cluster);
  }

  /** Tear the renderer down: kill every tween (node and beam), free every GPU
   *  resource, drop the canvas. Safe to call once; the owner must not reuse the
   *  instance after. */
  destroy(): void {
    this.#pool.clear();
    for (const gfx of this.#nodes.values()) {
      for (const tween of gfx.tweens) tween.kill();
      gfx.tweens.clear();
    }
    this.#nodes.clear();
    this.#app.ticker.remove(this.#tick, this);
    this.#app.canvas.remove();
    this.#app.destroy(true, { children: true, texture: true, textureSource: true });
  }

  /** Rebuild the brush of every beam style and fade, every node stroke and
   *  the text styles from the theme. A landed route is 1 px at
   *  `LANDED_ALPHA`. */
  #restyle(): void {
    const { muted, fg, fgMuted, axis, warn, bad, panel, bg, font, captionSmPx } = this.#theme;
    const marks: [boolean, number, number, number][] = [
      [true, muted, IN_WIDTH, BEAM_ALPHA], // IN_ZONE
      [true, fg, CROSS_WIDTH, BEAM_ALPHA], // CROSS_ZONE
      [true, muted, CROSS_WIDTH, BEAM_ALPHA], // DROPPED_CROSS
      [false, muted, 0, BEAM_ALPHA], // DOTS
      [false, fg, 0, BEAM_ALPHA], // ARROWS
      [true, muted, 1, LANDED_ALPHA], // LANDED_IN
      [true, fg, 1, LANDED_ALPHA], // LANDED_CROSS
    ];
    this.#brushes = marks.flatMap(([stroke, color, width, peak]) =>
      [peak, peak * FADED_ALPHA].map((alpha) => ({
        stroke,
        style: stroke ? { width, color, alpha, cap: 'round' as const } : { color, alpha },
      })),
    );
    this.#ringCaughtUp = { width: ARC_WIDTH, color: axis, alpha: 1 };
    this.#ringTrack = { width: 1, color: warn, alpha: 1 };
    this.#ringArc = { width: ARC_WIDTH, color: warn, alpha: 1, cap: 'butt' };
    this.#brimStroke = { width: BRIM_WIDTH, color: fg, alpha: 1 };
    this.#capStroke = { width: CAP_WIDTH, color: bad, alpha: 1 };
    this.#strataStroke = { width: STRATUM_GAP, color: panel, alpha: 1 };
    this.#selectionStroke = { width: SELECTION_WIDTH, color: fg, alpha: 1 };
    this.#previewStroke = { width: SELECTION_WIDTH, color: fgMuted, alpha: 1, cap: 'butt' };
    this.#hatchStroke = { width: 1.5, color: bad, alpha: 1 };
    const stroke = { color: bg, width: 2 * HALO, join: 'round' as const };
    this.#nameStyle = new TextStyle({ fontFamily: font, fontSize: captionSmPx, fontWeight: '400', fill: fgMuted, stroke });
    this.#namedStyle = new TextStyle({ fontFamily: font, fontSize: captionSmPx, fontWeight: '600', fill: fg, stroke });
    this.#titleStyle = new TextStyle({ fontFamily: font, fontSize: captionSmPx, fontWeight: '600', fill: fgMuted, stroke });
    for (const title of this.#titles) title.style = this.#titleStyle;
  }

  /** Create one node's display objects at `target` and animate its join. */
  #createNode(spot: Spot, target: Point): void {
    const root = new Container();
    root.position.set(target.x, target.y);
    const vessel = new Graphics();
    const ring = new Graphics();
    const port = new Graphics();
    const selection = new Graphics();
    const preview = new Graphics();
    const refusal = new Graphics();
    const mask = new Graphics();
    selection.visible = false;
    preview.visible = false;
    refusal.visible = false;
    refusal.mask = mask;
    root.addChild(vessel, mask, refusal, ring, selection, preview, port);
    this.#nodeLayer.addChild(root);
    const label = new Text({ text: `node ${spot.id}`, style: this.#nameStyle });
    this.#labelLayer.addChild(label);
    const slot = this.#freeSlots.pop() ?? MAX_STAGE_NODES - 1;
    const gfx: NodeGfx = {
      id: spot.id,
      slot,
      root,
      vessel,
      ring,
      port,
      selection,
      preview,
      refusal,
      mask,
      label,
      zone: spot.zone,
      center: target,
      radius: spot.r,
      row: new Float64Array(MAX_BUCKETS),
      rowLen: -1,
      total: -1,
      capped: false,
      arc: 1,
      arcFrom: 1,
      arcTo: 1,
      arcStart: 0,
      sweeping: false,
      behind: false,
      portX: 0,
      portY: 0,
      portAngle: 0,
      ported: false,
      tweens: new Set(),
    };
    this.#nodes.set(spot.id, gfx);
    this.#bySlot[slot] = gfx;
    const glyphs = this.#glyphs;
    glyphs.id[slot] = spot.id;
    glyphs.x[slot] = target.x;
    glyphs.y[slot] = target.y;
    glyphs.len = Math.max(glyphs.len, slot + 1);
    glyphs.ring[slot] = RING_CAUGHT_UP;
    this.#drawNode(gfx);

    if (this.#reduceMotion) return;
    // Join: fade in and scale up from a small disc: growth reads as "a member
    // arrived here", distinct from a packet landing.
    root.alpha = 0;
    root.scale.set(ENTER_SCALE);
    const s = { v: ENTER_SCALE };
    this.#track(gfx, gsap.to(root, { alpha: 1, duration: ENTER_MS / 1000, ease: 'power2.out' }));
    this.#track(
      gfx,
      gsap.to(s, {
        v: 1,
        duration: ENTER_MS / 1000,
        ease: 'back.out(1.6)',
        onUpdate: () => root.scale.set(s.v),
      }),
    );
  }

  /** Glide a survivor's disc to its new slot; its count hides while it moves. */
  #glide(gfx: NodeGfx, target: Point): void {
    if (gfx.center.x === target.x && gfx.center.y === target.y) return;
    gfx.center = target;
    this.#glyphs.x[gfx.slot] = target.x;
    this.#glyphs.y[gfx.slot] = target.y;
    if (this.#reduceMotion) {
      gfx.root.position.set(target.x, target.y);
      return;
    }
    this.#setGliding(gfx.id, true);
    this.#track(
      gfx,
      gsap.to(gfx.root.position, {
        x: target.x,
        y: target.y,
        duration: GLIDE_MS / 1000,
        ease: 'power2.inOut',
        overwrite: 'auto',
        onComplete: () => this.#setGliding(gfx.id, false),
      }),
    );
  }

  /** Slide a port that just took over a zone out from its gateway's disc,
   *  along the angle it points in. */
  #glidePort(id: number): void {
    const gfx = this.#nodes.get(id);
    if (gfx === undefined || !gfx.ported || this.#reduceMotion) return;
    const back = -(RING_REACH + portHalf(gfx.radius) + PORT_GAP);
    gfx.port.position.set(Math.cos(gfx.portAngle) * back, Math.sin(gfx.portAngle) * back);
    this.#track(gfx, gsap.to(gfx.port.position, { x: 0, y: 0, duration: GLIDE_MS / 1000, ease: 'power2.out' }));
  }

  /** Mark node `id` gliding or not, and tell the page. */
  #setGliding(id: number, on: boolean): void {
    if (on === this.#gliding.has(id)) return;
    if (on) this.#gliding.add(id);
    else this.#gliding.delete(id);
    const ids = [...this.#gliding];
    this.#app.canvas.dataset.gliding = JSON.stringify(ids);
    this.#onGliding(ids);
  }

  /** Animate a departed node's leave, then destroy it. */
  #removeNode(id: number, gfx: NodeGfx): void {
    this.#nodes.delete(id);
    this.#bySlot[gfx.slot] = undefined;
    this.#freeSlots.push(gfx.slot);
    const glyphs = this.#glyphs;
    glyphs.id[gfx.slot] = -1;
    glyphs.fillTop[gfx.slot] = Number.NaN;
    glyphs.brimY[gfx.slot] = Number.NaN;
    glyphs.portX[gfx.slot] = Number.NaN;
    glyphs.portY[gfx.slot] = Number.NaN;
    if (gfx.sweeping) this.#sweeps -= 1;
    if (this.#bounceGfx === gfx) this.#endBounce();
    if (this.#sweep.parent === gfx.root) gfx.root.removeChild(this.#sweep);
    this.#setGliding(id, false);
    gfx.label.destroy();
    const finish = (): void => {
      for (const tween of gfx.tweens) tween.kill();
      gfx.tweens.clear();
      gfx.root.destroy({ children: true });
    };
    if (this.#reduceMotion) {
      finish();
      return;
    }
    const s = { v: gfx.root.scale.x };
    this.#track(gfx, gsap.to(gfx.root, { alpha: 0, duration: EXIT_MS / 1000, ease: 'power2.in' }));
    this.#track(
      gfx,
      gsap.to(s, {
        v: 0,
        duration: EXIT_MS / 1000,
        ease: 'power2.in',
        onUpdate: () => gfx.root.scale.set(s.v),
        onComplete: finish,
      }),
    );
  }

  /** Track a node tween so it is killed on destroy, and drop it from the set
   *  once it finishes so a long session doesn't accumulate inert tweens. */
  #track(gfx: NodeGfx, tween: gsap.core.Tween): gsap.core.Tween {
    gfx.tweens.add(tween);
    void tween.then(() => gfx.tweens.delete(tween)).catch(() => {});
    return tween;
  }

  /** Draw each zone's ring guide: where its nodes sit. Structure, not data. */
  #drawGuides(): void {
    this.#guides.clear();
    if (this.#layout === null) return;
    for (const ring of this.#layout.rings) this.#guides.circle(ring.x, ring.y, ring.rz);
    this.#guides.stroke({ width: IN_WIDTH, color: this.#theme.grid });
  }

  /** Draw the heartbeat sweep's segments at the selection's radius. */
  #drawSweep(): void {
    this.#sweepRadius = this.#layout?.spots[0]?.r ?? 0;
    const r = this.#sweepRadius + SELECTION_GAP;
    const step = (2 * Math.PI) / SWEEP_SEGMENTS;
    for (let i = 0; i < SWEEP_SEGMENTS; i++) {
      const a = -Math.PI / 2 + i * step;
      this.#segments[i]
        .clear()
        .moveTo(r * Math.cos(a), r * Math.sin(a))
        .arc(0, 0, r, a, a + step)
        .stroke(this.#selectionStroke);
    }
  }

  /** Redraw everything a node's radius or the theme changes: its disc mask,
   *  outlines and preview, then its vessel and ring from the last snapshot. */
  #drawNode(gfx: NodeGfx): void {
    const r = gfx.radius;
    gfx.mask.clear().circle(0, 0, r - 1).fill(this.#theme.panel);
    gfx.selection.clear().circle(0, 0, r + SELECTION_GAP).stroke(this.#selectionStroke);
    gfx.preview.clear();
    dashedRing(gfx.preview, r + SELECTION_GAP, PREVIEW_DASH);
    gfx.preview.stroke(this.#previewStroke);
    this.#drawVessel(gfx, Math.max(gfx.total, 0), gfx.capped);
    this.#drawRing(gfx);
    this.#glyphs.r[gfx.slot] = r;
  }

  /** Every node's vessel and ring from `state`, and the marks drawn. */
  #syncAll(state: ClusterState): void {
    const nodes = state.nodes;
    for (let i = 0; i < nodes.length; i++) this.#syncNode(state, i);
    this.#publishMarks();
  }

  /** Node `i` of `state`: redraw its vessel when its window row or its cap
   *  changed, and start its ring's sweep when the share it has heard or its
   *  role changed. Reads the snapshot's arrays in place. */
  #syncNode(state: ClusterState, i: number): void {
    const node = state.nodes[i];
    const gfx = this.#nodes.get(node.id);
    if (gfx === undefined) return;
    const buckets = Math.min(this.#scale.buckets, MAX_BUCKETS);
    const from = i * this.#scale.buckets;
    const capped = isRejected(node.brim);
    let same = gfx.rowLen === buckets && gfx.total === node.window_total && gfx.capped === capped;
    for (let b = 0; same && b < buckets; b++) same = gfx.row[b] === (state.window_buckets[from + b] ?? 0);
    if (!same) {
      for (let b = 0; b < buckets; b++) gfx.row[b] = state.window_buckets[from + b] ?? 0;
      gfx.rowLen = buckets;
      gfx.total = node.window_total;
      gfx.capped = capped;
      this.#drawVessel(gfx, node.window_total, capped);
    }
    const share = state.oracle_total > 0 ? Math.min(node.window_total / state.oracle_total, 1) : 1;
    const behind = this.#isBehind(node.lag);
    if (share !== gfx.arcTo || behind !== gfx.behind) {
      gfx.behind = behind;
      // A share heard sweeps up as its packet lands; the truth growing past a
      // node opens its ring at once.
      if (this.#reduceMotion || share <= gfx.arc) {
        if (gfx.sweeping) this.#sweeps -= 1;
        gfx.sweeping = false;
        gfx.arc = share;
        gfx.arcTo = share;
        this.#drawRing(gfx);
      } else {
        if (!gfx.sweeping) this.#sweeps += 1;
        gfx.sweeping = true;
        gfx.arcFrom = gfx.arc;
        gfx.arcTo = share;
        gfx.arcStart = this.#pool.clock;
        this.#animEnd = Math.max(this.#animEnd, this.#pool.clock + SWEEP_MS);
        this.#drawRing(gfx);
      }
    }
  }

  /** Whether a lag opens the ring: behind or stranded, past the feed's grace.
   *  A read of the engine's tagged enum, whose variants have shapes of their
   *  own: V8 may grow its lookup feedback here, which is not the stage's
   *  allocation, so the allocation audit leaves this and `isRejected` out. */
  #isBehind(lag: Lag): boolean {
    return lag.kind !== 'CaughtUp' && lag.missing_for_ms >= this.#grace;
  }

  /** Draw a node's vessel holding `total`: the disc, the fill to the level in
   *  one stratum per non-empty window bucket (oldest at the bottom), the part
   *  over the brim in red, and the brim, thickened into the cap while the node
   *  is at its limit. Off the scale, the empty disc alone. */
  #drawVessel(gfx: NodeGfx, total: number, capped: boolean): void {
    const g = gfx.vessel;
    const glyphs = this.#glyphs;
    const slot = gfx.slot;
    const r = gfx.radius;
    g.clear().circle(0, 0, r).fill(this.#theme.panel);
    const scale = this.#scale;
    if (!scale.onScale) {
      glyphs.fillTop[slot] = Number.NaN;
      glyphs.brimY[slot] = Number.NaN;
      glyphs.strata[slot] = 0;
      glyphs.cap[slot] = 0;
      return;
    }
    const inner = 2 * (r - 1);
    const full = VESSEL_SCALE * scale.limit;
    const bottom = r - 1;
    const fillTop = levelY(r, total, full);
    const brimY = levelY(r, scale.limit, full);
    glyphs.fillTop[slot] = fillTop;
    glyphs.brimY[slot] = brimY;
    if (fillTop < bottom) {
      chord(g, r - 1, Math.max(fillTop, brimY), bottom);
      g.fill(this.#theme.level);
      if (fillTop < brimY) {
        chord(g, r - 1, fillTop, brimY);
        g.fill(this.#theme.bad);
      }
    }
    // Strata: a gap between each pair of non-empty buckets, oldest lowest,
    // where the inside is tall enough and a request's stratum would show.
    let strata = 0;
    if (inner >= STRATA_MIN_INNER && (inner * scale.hits) / full >= STRATUM_MIN) {
      let below = 0;
      for (let b = 0; b < gfx.rowLen; b++) {
        const hits = gfx.row[b];
        if (hits <= 0) continue;
        strata += 1;
        if (below > 0 && below < full) {
          brimLine(g, r - 1, levelY(r, below, full), 0);
        }
        below += hits;
      }
      if (strata > 1) g.stroke(this.#strataStroke);
    }
    glyphs.strata[slot] = strata;
    glyphs.cap[slot] = capped ? 1 : 0;
    brimLine(g, r, brimY, BRIM_OVERHANG);
    g.stroke(capped ? this.#capStroke : this.#brimStroke);
  }

  /** Draw a node's ring from its state: caught up, a closed neutral ring;
   *  behind, a thin amber track under an amber arc of the share it has heard,
   *  from 12 o'clock, held at least `ARC_NOTCH` short of closing. Butt caps,
   *  so no cap closes the notch. While a sweep runs the ring is amber. */
  #drawRing(gfx: NodeGfx): void {
    const g = gfx.ring;
    const reach = gfx.radius + ARC_GAP;
    const behind = gfx.behind || gfx.sweeping;
    g.clear();
    const slot = gfx.slot;
    this.#glyphs.ring[slot] = behind ? RING_BEHIND : RING_CAUGHT_UP;
    this.#glyphs.arc[slot] = gfx.arc;
    if (!behind) {
      g.circle(0, 0, reach).stroke(this.#ringCaughtUp);
      return;
    }
    g.circle(0, 0, reach).stroke(this.#ringTrack);
    const f = Math.min(Math.max(gfx.arc, 0), 1 - ARC_NOTCH);
    if (f <= 0) return;
    const start = -Math.PI / 2;
    g.moveTo(0, -reach).arc(0, 0, reach, start, start + f * 2 * Math.PI).stroke(this.#ringArc);
  }

  /** Draw the selection outline on the selected node and each gateway's port,
   *  and publish where each is. */
  #placeRoles(): void {
    const roles = this.#roles;
    const layout = this.#layout;
    const glyphs = this.#glyphs;
    glyphs.selected = -1;
    for (const gfx of this.#nodes.values()) {
      const selected = roles !== null && gfx.id === roles.selected;
      gfx.selection.visible = selected && !this.#sweep.visible;
      if (selected) {
        glyphs.selected = gfx.slot;
        if (this.#sweep.parent !== gfx.root) gfx.root.addChild(this.#sweep);
        gfx.preview.visible = false;
      }
      const gateway = roles?.gateways.get(gfx.id);
      gfx.port.clear();
      gfx.ported = false;
      glyphs.portX[gfx.slot] = Number.NaN;
      glyphs.portY[gfx.slot] = Number.NaN;
      glyphs.portSuspect[gfx.slot] = 0;
      if (gateway === undefined || layout === null) continue;
      const ring = layout.rings[gfx.zone];
      if (ring === undefined) continue;
      this.#from.x = gfx.center.x;
      this.#from.y = gfx.center.y;
      this.#from.r = gfx.radius;
      portAt(this.#from, ring, this.#port);
      gfx.portX = this.#port.x;
      gfx.portY = this.#port.y;
      gfx.portAngle = this.#port.angle;
      gfx.ported = true;
      const half = portHalf(gfx.radius);
      portGlyph(gfx.port, this.#port.x, this.#port.y, half, this.#port.angle);
      if (gateway.suspect) {
        gfx.port.fill(this.#theme.bg).stroke(this.#selectionStroke);
      } else {
        gfx.port.fill(this.#theme.fg);
      }
      gfx.port.position.set(0, 0);
      glyphs.portX[gfx.slot] = gfx.center.x + this.#port.x;
      glyphs.portY[gfx.slot] = gfx.center.y + this.#port.y;
      glyphs.portSuspect[gfx.slot] = gateway.suspect ? 1 : 0;
    }
    this.#publishMarks();
  }

  /** The kinds of mark drawn now, and tell the page when they change. */
  #publishMarks(): void {
    const glyphs = this.#glyphs;
    let marks = this.#beamMarks;
    if (this.#nodes.size > 0) marks |= MARK.RING | (this.#scale.onScale ? MARK.COUNT : MARK.FAR);
    if (glyphs.selected >= 0) marks |= MARK.SELECTED;
    for (let slot = 0; slot < glyphs.len; slot++) {
      if (glyphs.id[slot] < 0) continue;
      if (glyphs.cap[slot] === 1) marks |= MARK.CAP;
      if (!Number.isNaN(glyphs.portX[slot])) marks |= glyphs.portSuspect[slot] === 1 ? MARK.SUSPECT : MARK.PORT;
    }
    if (marks === glyphs.marks) return;
    glyphs.marks = marks;
    this.#app.canvas.dataset.marks = String(marks);
    this.#onMarks(marks);
  }

  /** Lay the names out and publish their boxes. Zone titles go first, at
   *  their ring centres; node names follow in (zone, id) order, above the
   *  disc for a node in the upper half of its zone ring and below it
   *  otherwise, their near edge `LABEL_GAP` outside the ring. A name flips to
   *  the other side when it would come within `LABEL_SPACING` px of a placed
   *  name beside it, touch another node's marks or leave the canvas. */
  #placeLabels(): void {
    const layout = this.#layout;
    const boxes: LabelBox[] = [];
    if (layout === null) {
      this.#app.canvas.dataset.labelBoxes = '[]';
      return;
    }
    const width = this.#app.screen.width;
    const height = this.#app.screen.height;
    const size = this.#theme.captionSmPx;
    const clear = (box: LabelBox, self: number, spacing: number, marks = true): boolean => {
      if (box.x < 0 || box.y < 0 || box.x + box.w > width || box.y + box.h > height) return false;
      for (const o of boxes) {
        const vertical = box.y < o.y + o.h && o.y < box.y + box.h;
        if (vertical && box.x < o.x + o.w + spacing && o.x < box.x + box.w + spacing) return false;
      }
      if (!marks) return true;
      for (const spot of layout.spots) {
        const reach = spot.r + (spot.id === this.#roles?.selected ? SELECTION_REACH : RING_REACH);
        const nx = Math.max(box.x, Math.min(spot.x, box.x + box.w));
        const ny = Math.max(box.y, Math.min(spot.y, box.y + box.h));
        if (Math.hypot(spot.x - nx, spot.y - ny) <= reach) return false;
        const gfx = this.#nodes.get(spot.id);
        if (gfx?.ported === true && spot.id !== self) {
          const half = portHalf(spot.r);
          const px = spot.x + gfx.portX;
          const py = spot.y + gfx.portY;
          if (px + half > box.x && px - half < box.x + box.w && py + half > box.y && py - half < box.y + box.h) return false;
        }
      }
      return true;
    };
    // A name stands against its own disc when it overlaps the disc's span
    // across or down the stage, so it reads as that node's.
    const against = (box: LabelBox, own: Spot): boolean =>
      (box.x < own.x + own.r && own.x - own.r < box.x + box.w) || (box.y < own.y + own.r && own.y - own.r < box.y + box.h);
    layout.rings.forEach((ring, z) => {
      const title = this.#titles[z];
      if (title === undefined) return;
      title.text = zoneLabel(this.#zoneNames[z] ?? '');
      title.position.set(ring.x, ring.y);
      boxes.push({
        text: title.text,
        x: ring.x - title.width / 2,
        y: ring.y - title.height / 2,
        w: title.width,
        h: title.height,
        size,
      });
    });
    for (const spot of layout.spots) {
      const gfx = this.#nodes.get(spot.id);
      if (gfx === undefined) continue;
      const label = gfx.label;
      const fault = this.#faults.get(spot.id);
      label.text =
        fault === undefined
          ? `node ${spot.id}`
          : fault.kind === 'silenced'
            ? `node ${spot.id} · silenced`
            : `node ${spot.id} · can't reach ${zoneLabel(this.#zoneNames[fault.zone] ?? '')}`;
      label.style = fault !== undefined || spot.id === this.#roles?.selected ? this.#namedStyle : this.#nameStyle;
      const w = label.width;
      const h = label.height;
      const ring = layout.rings[spot.zone];
      const above = spot.y < ring.y - 1e-6;
      const near = spot.r + RING_REACH + LABEL_GAP;
      // Above or below the disc, its own side first, centred, then nudged
      // along; then beside it. Of the spots that stand against the disc, the
      // first clear by `LABEL_SPACING` wins,
      // else the first merely clear; then any spot merely clear, else the
      // first clear of every other name (a stage too small for its names),
      // else the first.
      const at = (x: number, y: number): LabelBox => ({ text: label.text, x, y, w, h, size });
      const candidates: LabelBox[] = [];
      for (const up of [above, !above]) {
        const y = up ? spot.y - near - h : spot.y + near;
        for (const dx of [0, -w / 2, w / 2, -w, w]) candidates.push(at(spot.x - w / 2 + dx, y));
      }
      const side = spot.x < ring.x ? -1 : 1;
      for (const s of [side, -side]) candidates.push(at(s < 0 ? spot.x - near - w : spot.x + near, spot.y - h / 2));
      const box =
        candidates.find((c) => against(c, spot) && clear(c, spot.id, LABEL_SPACING)) ??
        candidates.find((c) => against(c, spot) && clear(c, spot.id, 1)) ??
        candidates.find((c) => clear(c, spot.id, 1)) ??
        candidates.find((c) => clear(c, spot.id, 1, false)) ??
        at(Math.max(0, Math.min(spot.x - w / 2, width - w)), above ? spot.y - near - h : spot.y + near);
      boxes.push(box);
      label.position.set(box.x, box.y);
    }
    this.#app.canvas.dataset.labelBoxes = JSON.stringify(boxes);
  }

  /** Record one gossip packet as a beam leaving at `start` along the route
   *  between its nodes' live (mid-glide) positions, captured now so the beam
   *  keeps flying its original path even as the nodes glide. A relay sent by
   *  a gateway that wears a port leaves from the port's inward vertex. False
   *  when either node is not on the stage. */
  #beam(src: number, dst: number, flags: number, start: number): boolean {
    const a = this.#nodes.get(src);
    const b = this.#nodes.get(dst);
    if (a === undefined || b === undefined || this.#layout === null) return false;
    this.#from.x = a.root.position.x;
    this.#from.y = a.root.position.y;
    this.#from.zone = a.zone;
    if ((flags & RELAY) !== 0 && a.ported) {
      const half = portHalf(a.radius);
      this.#from.x += a.portX + Math.cos(a.portAngle) * half;
      this.#from.y += a.portY + Math.sin(a.portAngle) * half;
    }
    this.#to.x = b.root.position.x;
    this.#to.y = b.root.position.y;
    this.#to.zone = b.zone;
    beamPath(this.#from, this.#to, this.#layout.centre, this.#path);
    this.#pool.push(this.#path, start, src, dst, flags);
    return true;
  }

  /** Whether a beam is the selected node's: sent by it, or a relay sent by
   *  one of its gateways. Every other beam is drawn as faded context. */
  #isContext(src: number, flags: number): boolean {
    const roles = this.#roles;
    if (roles === null || src === roles.selected) return false;
    return !((flags & RELAY) !== 0 && roles.gateways.has(src));
  }

  /** The ticker: advance the presentation clock, redraw the beams if
   *  anything changed or a beam is in flight, move the rings' sweeps and the
   *  refusal, then report a held wave whose beams this redraw has all
   *  landed. */
  #tick(ticker: Ticker): void {
    if (this.#guides.destroyed) return;
    const pool = this.#pool;
    const before = pool.clock;
    const bound = Math.max(this.#until, this.#animEnd);
    const rate = before < this.#until ? Math.max(this.#speed, this.#fastRate) : this.#speed;
    if (this.#running) pool.clock += ticker.deltaMS * rate;
    else if (before < bound) pool.clock = Math.min(bound, before + ticker.deltaMS * rate);
    if (this.#dirty || (pool.clock !== before && pool.inFlight > 0)) this.#drawBeams();
    if (this.#sweeps > 0) this.#tickRings();
    if (this.#bounceGfx !== null) this.#tickRefusal();
    if (pool.due !== null && pool.clock >= pool.due) {
      pool.due = null;
      this.#onLanded();
    }
  }

  /** Move each sweeping ring from its old share toward its new, eased out,
   *  over `SWEEP_MS` of the presentation clock. */
  #tickRings(): void {
    const now = this.#pool.clock;
    const glyphs = this.#glyphs;
    for (let slot = 0; slot < glyphs.len; slot++) {
      const gfx = this.#bySlot[slot];
      if (gfx === undefined || !gfx.sweeping) continue;
      // Ended on the sum the clock stops at (`#animEnd`), never on a quotient
      // rounding can leave short of 1, which would hold a paused clock's sweep
      // open for ever.
      const ended = now >= gfx.arcStart + SWEEP_MS;
      const t = (now - gfx.arcStart) / SWEEP_MS;
      gfx.arc = ended ? gfx.arcTo : gfx.arcFrom + (gfx.arcTo - gfx.arcFrom) * (1 - (1 - t) * (1 - t));
      if (ended) {
        gfx.sweeping = false;
        this.#sweeps -= 1;
      }
      this.#drawRing(gfx);
    }
  }

  /** Move the refusal: it drops from the disc's top onto the brim over the
   *  first half, rebounds `REBOUND` of the drop and falls back, fading over
   *  the last third; under reduced motion it rests. It never passes the brim. */
  #tickRefusal(): void {
    const gfx = this.#bounceGfx;
    if (gfx === null) return;
    // Ended on the sum the clock stops at, as a sweep is.
    if (this.#pool.clock >= this.#bounceStart + BOUNCE_MS) {
      this.#endBounce();
      return;
    }
    const u = (this.#pool.clock - this.#bounceStart) / BOUNCE_MS;
    const band = gfx.refusal;
    if (!this.#reduceMotion) {
      const drop = this.#bounceDrop;
      let offset: number;
      if (u < 0.5) {
        const t = u / 0.5;
        offset = -drop * (1 - t * t);
      } else if (u < 0.75) {
        const t = (u - 0.5) / 0.25;
        offset = -REBOUND * drop * (1 - (1 - t) * (1 - t));
      } else {
        const t = (u - 0.75) / 0.25;
        offset = -REBOUND * drop * (1 - t * t);
      }
      band.position.y = offset;
      band.alpha = u < 2 / 3 ? 1 : 1 - (u - 2 / 3) * 3;
    }
    const glyphs = this.#glyphs;
    glyphs.bandBottom = this.#bounceRest + band.position.y;
    glyphs.bandTop = glyphs.bandBottom - this.#bounceHeight;
  }

  /** Hide the refusal and clear its record. */
  #endBounce(): void {
    const gfx = this.#bounceGfx;
    if (gfx !== null) gfx.refusal.visible = false;
    this.#bounceGfx = null;
    const glyphs = this.#glyphs;
    glyphs.bounce = -1;
    glyphs.bandTop = Number.NaN;
    glyphs.bandBottom = Number.NaN;
    delete this.#app.canvas.dataset.bounce;
  }

  /** The beam layer: one pass over the pool that adds every started beam to
   *  its style's Graphics by its age on the presentation clock, then one stroke
   *  or fill per style.
   *
   *  A data beam in flight is a short tail from the eased head back through
   *  the route's midpoint, with a head mark: an arrowhead across zones, a dot
   *  inside one. A control packet is a dot at the head. A lost packet is the
   *  line to `DROP_FRACTION` ending in a stop bar. Under reduced motion each is
   *  drawn whole for its flight. A beam lands the frame its age reaches the
   *  flight: a landed data route stays, whole, 1 px and quiet, while the trail
   *  is kept; every other landed beam leaves. */
  #drawBeams(): void {
    const pool = this.#pool;
    const gfx = this.#beamGfx;
    for (let k = 0; k < gfx.length; k++) gfx[k].clear();
    const now = pool.clock;
    const reduce = this.#reduceMotion;
    const keep = this.#trail === 'keep';
    const path = this.#path;
    const at = this.#at;
    let used = 0;
    let marks = this.#beamMarks;

    // Backwards, so the beam a removal swaps into slot `i` is one already drawn.
    for (let i = pool.len - 1; i >= 0; i--) {
      const age = now - pool.start[i];
      if (age < 0) continue;
      let flags = pool.flags[i];
      if ((flags & LANDED) === 0 && age >= BEAM_FLIGHT_MS) {
        pool.land(i);
        if (!keep || (flags & (CONTROL | DROPPED)) !== 0) {
          pool.removeAt(i);
          continue;
        }
        flags |= LANDED;
      }
      const o = i * 6;
      path.from.x = pool.route[o];
      path.from.y = pool.route[o + 1];
      path.control.x = pool.route[o + 2];
      path.control.y = pool.route[o + 3];
      path.to.x = pool.route[o + 4];
      path.to.y = pool.route[o + 5];
      const cross = (flags & CROSS) !== 0;
      const fade = this.#isContext(pool.src[i], flags) ? 1 : 0;

      if ((flags & LANDED) !== 0) {
        const style = (cross ? LANDED_CROSS : LANDED_IN) * 2 + fade;
        this.#route(gfx[style], 1);
        used |= 1 << style;
        continue;
      }
      const dropped = (flags & DROPPED) !== 0;
      const end = dropped ? DROP_FRACTION : 1;
      const head = reduce ? end : easeInOutQuad(age / BEAM_FLIGHT_MS) * end;
      if ((flags & CONTROL) !== 0) {
        pointOn(path, head, at);
        gfx[DOTS * 2 + fade].circle(at.x, at.y, DOT_R);
        used |= 1 << (DOTS * 2 + fade);
        marks |= MARK.DOT;
        continue;
      }
      const style = (cross ? (dropped ? DROPPED_CROSS : CROSS_ZONE) : IN_ZONE) * 2 + fade;
      const g = gfx[style];
      used |= 1 << style;
      if (dropped) {
        this.#route(g, head);
        this.#stopBar(g, head);
        marks |= MARK.LOST;
        continue;
      }
      if (reduce) {
        this.#route(g, 1);
      } else {
        const tail = Math.max(0, head - TAIL);
        pointOn(path, tail, at);
        g.moveTo(at.x, at.y);
        pointOn(path, (tail + head) / 2, at);
        g.lineTo(at.x, at.y);
        pointOn(path, head, at);
        g.lineTo(at.x, at.y);
      }
      if (cross) {
        this.#arrowhead(gfx[ARROWS * 2 + fade], head);
        used |= 1 << (ARROWS * 2 + fade);
      } else {
        pointOn(path, head, at);
        gfx[DOTS * 2 + fade].circle(at.x, at.y, IN_WIDTH + 1);
        used |= 1 << (DOTS * 2 + fade);
      }
    }

    for (let k = 0; k < STYLES * 2; k++) {
      if ((used & (1 << k)) === 0) continue;
      const brush = this.#brushes[k];
      if (brush.stroke) gfx[k].stroke(brush.style);
      else gfx[k].fill(brush.style);
    }
    pool.redraws += 1;
    this.#dirty = false;
    if (marks !== this.#beamMarks) {
      this.#beamMarks = marks;
      this.#publishMarks();
    }
  }

  /** Add the current route from its start to position `to`, as straight
   *  segments. */
  #route(g: Graphics, to: number): void {
    const path = this.#path;
    const at = this.#at;
    pointOn(path, 0, at);
    g.moveTo(at.x, at.y);
    for (let s = 1; s <= ROUTE_SEGMENTS; s++) {
      pointOn(path, (to * s) / ROUTE_SEGMENTS, at);
      g.lineTo(at.x, at.y);
    }
  }

  /** Set `#at` to route position `head` and `#dir` to the route's unit tangent
   *  there; false where the tangent vanishes. */
  #tangentAt(head: number): boolean {
    const path = this.#path;
    pointOn(path, head, this.#at);
    // B'(t) = 2(1 - t)(C - A) + 2t(B - C).
    const tx = 2 * (1 - head) * (path.control.x - path.from.x) + 2 * head * (path.to.x - path.control.x);
    const ty = 2 * (1 - head) * (path.control.y - path.from.y) + 2 * head * (path.to.y - path.control.y);
    const len = Math.hypot(tx, ty);
    if (len === 0) return false;
    this.#dir.x = tx / len;
    this.#dir.y = ty / len;
    return true;
  }

  /** Add a triangle at route position `head`, pointing along the route. */
  #arrowhead(g: Graphics, head: number): void {
    if (!this.#tangentAt(head)) return;
    const { x, y } = this.#at;
    const { x: ux, y: uy } = this.#dir;
    const bx = x - ux * ARROW_LEN;
    const by = y - uy * ARROW_LEN;
    g.moveTo(x, y)
      .lineTo(bx - uy * ARROW_HALF, by + ux * ARROW_HALF)
      .lineTo(bx + uy * ARROW_HALF, by - ux * ARROW_HALF)
      .closePath();
  }

  /** Add a bar across the route at position `head`: where a lost packet died. */
  #stopBar(g: Graphics, head: number): void {
    if (!this.#tangentAt(head)) return;
    stopBar(g, this.#at.x, this.#at.y, this.#dir.x, this.#dir.y, STOP_BAR / 2);
  }

  /** Drop every beam touching node `id`, called when that node leaves, so no
   *  beam draws to or from a disc that is being destroyed. */
  #cancelBeamsFor(id: number): void {
    const pool = this.#pool;
    for (let i = pool.len - 1; i >= 0; i--) {
      if (pool.src[i] === id || pool.dst[i] === id) pool.removeAt(i);
    }
    this.#dirty = true;
  }
}

/** What the renderer tells the page: a held wave landed, the kinds of mark
 *  drawn changed, or the nodes gliding changed. */
export interface Hooks {
  onLanded: () => void;
  onMarks: (marks: number) => void;
  onGliding: (ids: readonly number[]) => void;
}

/** Whether a node's decision is a refusal: at its limit, for `brim`. */
function isRejected(admission: Admission): boolean {
  return admission.kind === 'Rejected';
}

/** GSAP `power2.inOut` reproduced locally: the beam ticker (not a tween) drives
 *  flight progress, so it eases the raw age ratio itself. */
function easeInOutQuad(t: number): number {
  return t < 0.5 ? 2 * t * t : 1 - 2 * (1 - t) * (1 - t);
}

/** The height in a vessel of radius `r` that holds `count` of `full`, px from
 *  the centre with y down: the fill's top, the brim, a stratum, the refusal's
 *  band. */
function levelY(r: number, count: number, full: number): number {
  return r - 1 - (2 * (r - 1) * Math.min(count, full)) / full;
}
