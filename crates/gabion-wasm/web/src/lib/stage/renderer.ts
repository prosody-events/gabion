// The PixiJS stage renderer: a framework-agnostic class that owns the WebGL
// canvas and turns simulation state into the tier view. Kept out of Svelte's
// reactivity on purpose: Svelte feeds it `setLayout` (where every node sits),
// `setCluster` (steady per-node state), `setRoles` and `setFocus` (marks) and
// `show` (the packets of an engine batch); the class owns all imperative
// Pixi/GSAP bookkeeping and cleanup.
//
// Layers, back to front (design rule 19): the zone ring guides, the beams, the
// node discs with their arcs, role glyphs and focus rings, then the labels with
// a halo. Nodes are keyed by stable id: `setLayout` reconciles the live id set
// against what is on stage, so a join fades in at its ring slot, a leave scales
// out (its beams cancelled) and every survivor glides to the slot the new
// layout gives it.
//
// Beams fly on a presentation clock of their own (`BeamPool.clock`), which
// advances with the ticker's wall time times the speed while playing, and only
// to a bound (`finish`) while paused, so Speed stretches a flight and Pause
// freezes it. A held wave's landing is the renderer's to report: the tick that
// sets the last of its beams' `LANDED` bit calls `onLanded`, and only then does
// the page let the engine deliver it. A landed data route stays in the pool,
// drawn 1 px and quiet, until the next command where no feed runs.
//
// Every beam is one record in a pre-sized pool, drawn along the path
// `beamPath` gives it into one Graphics per mark style, and the beam layer is
// redrawn only when something changed or a beam is in flight, so a frame
// allocates nothing of its own however many packets fly.

import { gsap } from 'gsap';
import { Application, CanvasTextMetrics, Container, Graphics, Text, TextStyle, type Ticker } from 'pixi.js';
import { zoneLabel } from '../format';
import { CARRIES_CELLS, type ClusterState, type PacketKind, type SimEvent } from '../sim/types';
import { BEAM_ALPHA, FADED_ALPHA, LANDED_ALPHA, type Theme } from '../theme';
import {
  ARC_GAP,
  ARC_WIDTH,
  CROSS_WIDTH,
  DIAMOND_SCALE,
  IN_WIDTH,
  LABEL_GAP,
  beamPath,
  pointOn,
  type Point,
  type QuadPath,
  type Spot,
  type StageLayout,
} from './layout';

// A node counts as caught up when its view is within this fraction of the
// cluster's true total: the threshold that closes its arc and flips it from
// warn to ok.
const CONVERGED_EPSILON = 0.001;
// The least share of a ring an arc leaves open while its node is behind, so
// one hit short of the total still shows a notch (design rule 17).
const ARC_NOTCH = 1 / 12;

// Presentation-clock flight time of a gossip packet beam: wall time at 1×.
// Eased so the beam accelerates off the source and decelerates into the
// target: motion that reads as cause then effect.
const BEAM_FLIGHT_MS = 520;
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

// Role glyphs and rings, in px: the origin's thick ring and the focus ring's gap.
const ORIGIN_GAP = 8;
const ORIGIN_WIDTH = 4;
// How far a disc's marks reach beyond its radius: the origin ring's outer edge,
// which a label keeps clear of.
const MARK_REACH = ORIGIN_GAP + ORIGIN_WIDTH / 2;
const FOCUS_GAP = 7;
// A label's halo, px each side.
const HALO = 2;

// Membership-change motion (join / leave / re-space). Purposeful only: a join
// grows in, a leave shrinks out, survivors glide. Reduced motion skips all of
// it and snaps to the final layout.
const ENTER_MS = 420;
const EXIT_MS = 340;
const GLIDE_MS = 520;
const ENTER_SCALE = 0.25;

// Beam flag bits.
const CROSS = 1;
const CONTROL = 2;
const RELAY = 4;
const DROPPED = 8;
const LANDED = 16;

/** Each packet kind's flags: a kind that carries no cells is a dot, and a
 *  relay is the origin's own when its gateway sends it. Read from
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

/** What becomes of a data route once it lands: kept, quiet, until the next
 *  command (`'keep'`, presets with no feed, so a Send's waves read as a
 *  diagram), or dropped at once (`'drop'`, where a feed sends all the time). */
export type Trail = 'keep' | 'drop';

/** The selected origin's marks: the origin itself and each gateway into a
 *  remote zone, `suspect` once its peer has left something unanswered. */
export interface Roles {
  origin: number;
  gateways: ReadonlyMap<number, { zone: number; suspect: boolean }>;
}

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

/** One node's persistent display objects, keyed by stable id. */
interface NodeGfx {
  root: Container;
  disc: Graphics;
  arc: Graphics;
  /** The origin's thick ring or a gateway's diamond, cleared otherwise. */
  mark: Graphics;
  /** The keyboard focus ring, cleared when the node has no focus. */
  focus: Graphics;
  zone: number;
  /** The slot this node is gliding to (stage px). */
  center: Point;
  /** The disc radius the geometry was last drawn at. */
  radius: number;
  /** The view / true-total fraction the arc was last drawn at, so a theme
   *  change can redraw it. */
  fraction: number;
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
class BeamPool {
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
      i = this.#oldestLanded();
      if (i < 0) return;
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

  /** The landed route that started first, or -1 with none: the one a full pool
   *  overwrites. A scan, paid only when the pool is full. */
  #oldestLanded(): number {
    let oldest = -1;
    for (let i = 0; i < this.len; i++) {
      if ((this.flags[i] & LANDED) !== 0 && (oldest < 0 || this.start[i] < this.start[oldest])) oldest = i;
    }
    return oldest;
  }
}

/** The width of the widest node label ("000"), halo included: how far outside
 *  its disc the layout keeps the stage edge. */
export function labelExtent(theme: Theme): number {
  return CanvasTextMetrics.measureText('000', labelStyle(theme)).width;
}

function labelStyle(theme: Theme): TextStyle {
  return new TextStyle({
    fontFamily: theme.font,
    fontSize: theme.captionPx,
    fill: theme.fg,
    stroke: { color: theme.bg, width: 2 * HALO, join: 'round' },
  });
}

export class StageRenderer {
  readonly #app: Application;
  readonly #root: Container;
  readonly #guideLayer: Container;
  readonly #beamLayer: Container;
  readonly #nodeLayer: Container;
  readonly #labelLayer: Container;
  readonly #reduceMotion: boolean;
  readonly #guides: Graphics;
  // One Graphics per beam mark style and fade, each redrawn by `#drawBeams`.
  readonly #beamGfx: Graphics[] = [];
  readonly #pool = new BeamPool();
  // How each beam Graphics is painted once its marks are added, and the two
  // arc strokes, rebuilt with the theme so neither a frame nor a snapshot
  // builds a style of its own.
  #brushes: Brush[] = [];
  #arcCaughtUp: Brush['style'] = { color: 0, alpha: 1 };
  #arcBehind: Brush['style'] = { color: 0, alpha: 1 };
  // Scratch the beam paths and the frame draw reuse.
  readonly #path: QuadPath = { from: { x: 0, y: 0 }, control: { x: 0, y: 0 }, to: { x: 0, y: 0 } };
  readonly #at: Point = { x: 0, y: 0 };
  readonly #dir: Point = { x: 0, y: 0 };
  readonly #from: Spot = { id: 0, zone: 0, x: 0, y: 0, r: 0 };
  readonly #to: Spot = { id: 0, zone: 0, x: 0, y: 0, r: 0 };
  #theme: Theme;

  #nodes = new Map<number, NodeGfx>();
  #layout: StageLayout | null = null;
  #titles: readonly string[] = [];
  #cluster: ClusterState | null = null;
  #roles: Roles | null = null;
  #focusId: number | null = null;

  // The presentation clock's drive: whether it runs freely (playing), how
  // fast, and the bound it may still run to while paused (`finish`), at the
  // rate a fast-forward needs (0 when none).
  #running = false;
  #speed = 1;
  #until = 0;
  #fastRate = 0;
  // The latest instant any beam shown, or any hold, ends.
  #flightEnd = 0;
  #trail: Trail = 'drop';
  // The beam layer must be redrawn though no beam moved.
  #dirty = false;
  readonly #onLanded: () => void;

  private constructor(app: Application, theme: Theme, onLanded: () => void) {
    this.#app = app;
    this.#theme = theme;
    this.#onLanded = onLanded;
    this.#reduceMotion =
      typeof window !== 'undefined' &&
      window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    this.#root = new Container();
    this.#guideLayer = new Container();
    this.#beamLayer = new Container();
    this.#nodeLayer = new Container();
    this.#labelLayer = new Container();
    this.#root.addChild(this.#guideLayer, this.#beamLayer, this.#nodeLayer, this.#labelLayer);
    this.#app.stage.addChild(this.#root);

    this.#guides = new Graphics();
    this.#guideLayer.addChild(this.#guides);

    // Parent the beam Graphics before registering the ticker, so the first
    // tick never fires against an orphaned object.
    for (let i = 0; i < STYLES * 2; i++) {
      const g = new Graphics();
      this.#beamGfx.push(g);
      this.#beamLayer.addChild(g);
    }
    this.#restyle();
    this.#app.ticker.add(this.#tick, this);
    // The beam pool, read by the design audit as Chart.svelte's `uplot` is: set
    // once, so a frame pays nothing.
    Object.assign(this.#app.canvas, { beams: this.#pool });
  }

  /** Build the renderer and attach its canvas to `container`. Async because
   *  Pixi's WebGL/WebGPU context init is async. `onLanded` is called in the
   *  tick a held wave's beams have all landed. */
  static async create(container: HTMLElement, theme: Theme, onLanded: () => void): Promise<StageRenderer> {
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
    return new StageRenderer(app, theme, onLanded);
  }

  /** Resize the canvas to `width × height` CSS pixels. */
  resize(width: number, height: number): void {
    if (width > 0 && height > 0) this.#app.renderer.resize(width, height);
  }

  /** Place every node: enter newcomers, exit departed ones, glide survivors to
   *  their slots, redraw the ring guides and rebuild the labels. `titles` are
   *  the zone names, one per ring. Publishes the stage centre, the ring geometry
   *  and the label boxes on the canvas for the design audit. */
  setLayout(layout: StageLayout, titles: readonly string[]): void {
    this.#layout = layout;
    this.#titles = titles;
    const live = new Set<number>();
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
          this.#drawNode(spot.id, gfx);
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
    // A landed route keeps the geometry it flew; history is not worth a
    // re-route, so a new layout drops it.
    this.clearTrail();
    this.#drawGuides();
    this.#rebuildLabels();
    const dataset = this.#app.canvas.dataset;
    dataset.centre = JSON.stringify(layout.centre);
    dataset.rings = JSON.stringify(layout.rings);
    dataset.crossWidth = String(CROSS_WIDTH);
    if (this.#cluster !== null) this.#drawArcs(this.#cluster);
  }

  /** Apply steady per-node state: each node's cell arc. */
  setCluster(state: ClusterState | null): void {
    if (state === null) return;
    this.#cluster = state;
    this.#drawArcs(state);
  }

  /** Mark the selected origin and its gateways, or clear the marks with `null`. */
  setRoles(roles: Roles | null): void {
    this.#roles = roles;
    for (const [id, gfx] of this.#nodes) this.#drawMark(id, gfx);
    this.#dirty = true;
  }

  /** Ring the node with keyboard focus, or clear the ring with `null`. */
  setFocus(id: number | null): void {
    this.#focusId = id;
    for (const [nodeId, gfx] of this.#nodes) this.#drawFocus(nodeId, gfx);
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
   *  presentation clock from now. With `hold`, call `onLanded` once they have
   *  all landed (after one flight, even if none is drawn). */
  show(events: readonly SimEvent[], delay: number, hold: boolean): void {
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
    if (!drawn && !hold) return;
    const end = start + BEAM_FLIGHT_MS;
    if (end > this.#flightEnd) this.#flightEnd = end;
    if (hold) this.#pool.due = Math.max(this.#pool.due ?? 0, end);
    this.#dirty = true;
  }

  /** Let the clock run, though paused, until every beam shown has landed:
   *  at the speed with `fast` false, or within `LAND_FAST_MS` of wall time. */
  finish(fast: boolean): void {
    this.#until = this.#flightEnd;
    this.#fastRate = fast ? (this.#until - this.#pool.clock) / LAND_FAST_MS : 0;
  }

  /** Drop every landed route: each command starts a fresh picture. */
  clearTrail(): void {
    const pool = this.#pool;
    for (let i = pool.len - 1; i >= 0; i--) {
      if ((pool.flags[i] & LANDED) !== 0) pool.removeAt(i);
    }
    this.#dirty = true;
  }

  /** Drop every beam, any landing still due and its bound: a rebuild. */
  reset(): void {
    this.#pool.clear();
    this.#pool.due = null;
    this.#flightEnd = this.#pool.clock;
    this.#until = this.#pool.clock;
    this.#fastRate = 0;
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
    for (const [id, gfx] of this.#nodes) this.#drawNode(id, gfx);
    this.#rebuildLabels();
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

  /** Rebuild the brush of every beam style and fade, and the arc strokes,
   *  from the theme. A landed route is 1 px at `LANDED_ALPHA`. */
  #restyle(): void {
    const { muted, fg, ok, warn } = this.#theme;
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
    this.#arcCaughtUp = { width: ARC_WIDTH, color: ok, alpha: 1 };
    this.#arcBehind = { width: ARC_WIDTH, color: warn, alpha: 1, cap: 'butt' };
  }

  /** Create one node's display objects at `target` and animate its join. */
  #createNode(spot: Spot, target: Point): void {
    const root = new Container();
    root.position.set(target.x, target.y);
    const disc = new Graphics();
    const arc = new Graphics();
    const mark = new Graphics();
    const focus = new Graphics();
    root.addChild(disc, arc, mark, focus);
    this.#nodeLayer.addChild(root);
    const gfx: NodeGfx = {
      root,
      disc,
      arc,
      mark,
      focus,
      zone: spot.zone,
      center: target,
      radius: spot.r,
      fraction: 0,
      tweens: new Set(),
    };
    this.#nodes.set(spot.id, gfx);
    this.#drawNode(spot.id, gfx);

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

  /** Glide a survivor's disc to its new slot. */
  #glide(gfx: NodeGfx, target: Point): void {
    if (gfx.center.x === target.x && gfx.center.y === target.y) return;
    gfx.center = target;
    if (this.#reduceMotion) {
      gfx.root.position.set(target.x, target.y);
      return;
    }
    this.#track(
      gfx,
      gsap.to(gfx.root.position, {
        x: target.x,
        y: target.y,
        duration: GLIDE_MS / 1000,
        ease: 'power2.inOut',
        overwrite: 'auto',
      }),
    );
  }

  /** Animate a departed node's leave, then destroy it. */
  #removeNode(id: number, gfx: NodeGfx): void {
    this.#nodes.delete(id);
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

  /** Redraw everything a node's radius or the theme changes. */
  #drawNode(id: number, gfx: NodeGfx): void {
    gfx.disc.clear().circle(0, 0, gfx.radius).fill(this.#theme.fg);
    this.#drawArc(gfx, gfx.fraction);
    this.#drawMark(id, gfx);
    this.#drawFocus(id, gfx);
  }

  /** Draw a node's cell arc: an annulus around the disc whose sweep is the
   *  node's view as a fraction of the true total, held at least `ARC_NOTCH`
   *  short of a ring until the node is caught up, so every arc closes exactly
   *  when the cluster converges. Butt caps, so no cap closes the notch. The
   *  sweep carries the state and the colour only repeats it (design rule 17). */
  #drawArc(node: NodeGfx, fraction: number): void {
    const f = Math.max(0, Math.min(1, fraction));
    node.fraction = fraction;
    node.arc.clear();
    if (f <= 0) return;
    const reach = node.radius + ARC_GAP;
    if (f >= 1 - CONVERGED_EPSILON) {
      node.arc.circle(0, 0, reach).stroke(this.#arcCaughtUp);
      return;
    }
    const start = -Math.PI / 2;
    node.arc
      .arc(0, 0, reach, start, start + Math.min(f, 1 - ARC_NOTCH) * 2 * Math.PI)
      .stroke(this.#arcBehind);
  }

  /** Redraw the arc of every node whose fraction changed: most snapshots move
   *  few. */
  #drawArcs(state: ClusterState): void {
    const oracle = Math.max(state.oracle_total, 1);
    const nodes = state.nodes;
    for (let i = 0; i < nodes.length; i++) {
      const gfx = this.#nodes.get(nodes[i].id);
      const f = nodes[i].window_total / oracle;
      if (gfx !== undefined && gfx.fraction !== f) this.#drawArc(gfx, f);
    }
  }

  /** Draw or clear a node's role mark: the origin's thick ring, or a
   *  gateway's diamond: filled for a gateway (◆), hollow for a suspect (◇). */
  #drawMark(id: number, gfx: NodeGfx): void {
    const roles = this.#roles;
    gfx.mark.clear();
    if (roles === null) return;
    if (id === roles.origin) {
      gfx.mark
        .circle(0, 0, gfx.radius + ORIGIN_GAP)
        .stroke({ width: ORIGIN_WIDTH, color: this.#theme.fg });
      return;
    }
    const gateway = roles.gateways.get(id);
    if (gateway === undefined) return;
    const d = DIAMOND_SCALE * gfx.radius;
    gfx.mark.moveTo(0, -d).lineTo(d, 0).lineTo(0, d).lineTo(-d, 0).closePath();
    if (gateway.suspect) {
      gfx.mark.fill(this.#theme.bg).stroke({ width: 2, color: this.#theme.fg });
    } else {
      gfx.mark.fill(this.#theme.fg);
    }
  }

  #drawFocus(id: number, gfx: NodeGfx): void {
    gfx.focus.clear();
    if (id !== this.#focusId) return;
    gfx.focus.circle(0, 0, gfx.radius + FOCUS_GAP).stroke({ width: 2, color: this.#theme.warn });
  }

  /** Lay the labels out and publish their boxes. Zone titles go first, at their
   *  ring centres; node ids follow in (zone, id) order, radially outside the
   *  disc, else inside it, else along the ring. A zone title is
   *  drawn only if its measured box lies inside the stage and overlaps no label
   *  already placed; a node id must also clear every disc and its marks. So the
   *  audit's no-overlap rule holds at every width by construction. */
  #rebuildLabels(): void {
    for (const label of this.#labelLayer.removeChildren()) label.destroy();
    const layout = this.#layout;
    const boxes: LabelBox[] = [];
    if (layout === null) {
      this.#app.canvas.dataset.labelBoxes = '[]';
      return;
    }
    const style = labelStyle(this.#theme);
    const width = this.#app.screen.width;
    const height = this.#app.screen.height;
    // Whether a label drawn at (`cx`, `cy`) would sit clear of everything placed.
    // `dx`, `dy` is the direction it grows away from the point, so its near edge,
    // not its centre, is `cx, cy`; a title (no direction) is centred. Returns
    // whether it was drawn.
    const place = (text: string, cx: number, cy: number, dx: number, dy: number, avoidDiscs: boolean): boolean => {
      const m = CanvasTextMetrics.measureText(text, style);
      const half = (Math.abs(dx) * m.width) / 2 + (Math.abs(dy) * m.height) / 2;
      const x = cx + dx * half;
      const y = cy + dy * half;
      const box = {
        text,
        x: x - m.width / 2,
        y: y - m.height / 2,
        w: m.width,
        h: m.height,
        size: this.#theme.captionPx,
      };
      const inside = box.x >= 0 && box.y >= 0 && box.x + box.w <= width && box.y + box.h <= height;
      // One px of slack either side, so boxes that merely touch never count.
      const clear = boxes.every(
        (o) => box.x + box.w + 1 <= o.x || o.x + o.w + 1 <= box.x || box.y + box.h + 1 <= o.y || o.y + o.h + 1 <= box.y,
      );
      const free =
        !avoidDiscs ||
        layout.spots.every((spot) => {
          // The box's nearest point to the disc, against the disc and its marks.
          const nx = Math.max(box.x, Math.min(spot.x, box.x + box.w));
          const ny = Math.max(box.y, Math.min(spot.y, box.y + box.h));
          return Math.hypot(spot.x - nx, spot.y - ny) > spot.r + MARK_REACH;
        });
      if (!inside || !clear || !free) return false;
      boxes.push(box);
      const label = new Text({ text, style });
      label.anchor.set(0.5);
      label.position.set(x, y);
      this.#labelLayer.addChild(label);
      return true;
    };
    layout.rings.forEach((ring, z) => {
      place(zoneLabel(this.#titles[z] ?? ''), ring.x, ring.y, 0, 0, false);
    });
    for (const spot of layout.spots) {
      const ring = layout.rings[spot.zone];
      const dist = Math.hypot(spot.x - ring.x, spot.y - ring.y);
      const dx = dist === 0 ? 0 : (spot.x - ring.x) / dist;
      const dy = dist === 0 ? -1 : (spot.y - ring.y) / dist;
      const reach = spot.r + LABEL_GAP;
      const text = String(spot.id);
      // Outside the ring first, then inside it, then along it either way: a
      // node facing a neighbouring ring has no room outside, and the zone title
      // may fill the inside.
      for (const [ux, uy] of [
        [dx, dy],
        [-dx, -dy],
        [-dy, dx],
        [dy, -dx],
      ]) {
        if (place(text, spot.x + ux * reach, spot.y + uy * reach, ux, uy, true)) break;
      }
    }
    this.#app.canvas.dataset.labelBoxes = JSON.stringify(boxes);
  }

  /** Record one gossip packet as a beam leaving at `start` along the route
   *  between its nodes' live (mid-glide) positions, captured now so the beam
   *  keeps flying its original path even as the nodes glide. False when either
   *  node is not on the stage. */
  #beam(src: number, dst: number, flags: number, start: number): boolean {
    const a = this.#nodes.get(src);
    const b = this.#nodes.get(dst);
    if (a === undefined || b === undefined || this.#layout === null) return false;
    this.#from.x = a.root.position.x;
    this.#from.y = a.root.position.y;
    this.#from.zone = a.zone;
    this.#to.x = b.root.position.x;
    this.#to.y = b.root.position.y;
    this.#to.zone = b.zone;
    beamPath(this.#from, this.#to, this.#layout.centre, this.#path);
    this.#pool.push(this.#path, start, src, dst, flags);
    return true;
  }

  /** Whether a beam is the selected origin's: sent by it, or a relay sent by
   *  one of its gateways. Every other beam is drawn as faded context. */
  #isContext(src: number, flags: number): boolean {
    const roles = this.#roles;
    if (roles === null || src === roles.origin) return false;
    return !((flags & RELAY) !== 0 && roles.gateways.has(src));
  }

  /** The ticker: advance the presentation clock, redraw the beams if
   *  anything changed or a beam is in flight, then report a held wave whose
   *  beams this redraw has all landed. */
  #tick(ticker: Ticker): void {
    if (this.#guides.destroyed) return;
    const pool = this.#pool;
    const before = pool.clock;
    const rate = before < this.#until ? Math.max(this.#speed, this.#fastRate) : this.#speed;
    if (this.#running) pool.clock += ticker.deltaMS * rate;
    else if (before < this.#until) pool.clock = Math.min(this.#until, before + ticker.deltaMS * rate);
    if (pool.clock >= this.#until) this.#fastRate = 0;
    if (this.#dirty || (pool.clock !== before && pool.inFlight > 0)) this.#drawBeams();
    if (pool.due !== null && pool.clock >= pool.due) {
      pool.due = null;
      this.#onLanded();
    }
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
        continue;
      }
      const style = (cross ? (dropped ? DROPPED_CROSS : CROSS_ZONE) : IN_ZONE) * 2 + fade;
      const g = gfx[style];
      used |= 1 << style;
      if (dropped) {
        this.#route(g, head);
        this.#stopBar(g, head);
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
    const { x, y } = this.#at;
    const half = STOP_BAR / 2;
    const nx = -this.#dir.y * half;
    const ny = this.#dir.x * half;
    g.moveTo(x - nx, y - ny).lineTo(x + nx, y + ny);
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

/** GSAP `power2.inOut` reproduced locally: the beam ticker (not a tween) drives
 *  flight progress, so it eases the raw age ratio itself. */
function easeInOutQuad(t: number): number {
  return t < 0.5 ? 2 * t * t : 1 - 2 * (1 - t) * (1 - t);
}
