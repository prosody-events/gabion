// The PixiJS stage renderer: a framework-agnostic class that owns the WebGL
// canvas and turns simulation state into the tier view. Kept out of Svelte's
// reactivity on purpose: Svelte feeds it `setLayout` (where every node sits),
// `setCluster` (steady per-node state), `setRoles` and `setFocus` (marks) and
// `applyEvents` (transient gossip packets); the class owns all imperative
// Pixi/GSAP bookkeeping and cleanup.
//
// Layers, back to front (design rule 19): the zone ring guides, the beams, the
// node discs with their arcs, role glyphs and focus rings, then the labels with
// a halo. Nodes are keyed by stable id: `setLayout` reconciles the live id set
// against what is on stage, so a join fades in at its ring slot, a leave scales
// out (its beams cancelled) and every survivor glides to the slot the new
// layout gives it.
//
// Every beam is one record in a pre-sized pool, drawn each frame along the path
// `beamPath` gives it into one Graphics per mark style, so a frame allocates
// nothing of its own however many packets fly.

import { gsap } from 'gsap';
import { Application, CanvasTextMetrics, Container, Graphics, Text, TextStyle } from 'pixi.js';
import { zoneLabel } from '../format';
import type { ClusterState, PacketKind, SimEvent } from '../sim/types';
import { BEAM_ALPHA, FADED_ALPHA, type Theme } from '../theme';
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

// Wall-clock flight time of a gossip packet beam. Eased so the beam accelerates
// off the source and decelerates into the target: motion that reads as cause
// then effect.
const BEAM_FLIGHT_MS = 520;
// Reduced motion: a beam is its whole route, shown at constant alpha for this
// span instead of flying.
const REDUCE_FADE_MS = 400;
// A flying stroke's tail, as a share of its route.
const TAIL = 0.26;
// A dropped packet dies partway across instead of landing, told by shape and by
// the muted colour, never by a fainter alpha: every beam peaks at `BEAM_ALPHA`.
const DROP_FRACTION = 0.5;
// Segments of a reduced-motion route.
const ROUTE_SEGMENTS = 8;
// A cross-zone arrowhead's length and half width, and a control dot's radius, px.
const ARROW_LEN = 9;
const ARROW_HALF = 4.5;
const DOT_R = 2;
// The most beams in flight. Beams self-expire, so the live set is bounded by
// sends in one flight window; a 100-node sustained overload peaks in the low
// tens of thousands, so this leaves an order of magnitude of headroom and is
// not reached in normal operation. Excess is dropped rather than grown.
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

/** Each packet kind's flags: a control packet is a dot, a relay is the
 *  origin's own when its gateway sends it. Exhaustive, so a new kind is a type
 *  error until it is classed here, as `PacketKind::carries_cells` is in Rust. */
const KIND_FLAGS: Record<PacketKind, number> = {
  Ack: CONTROL,
  SweepRequest: CONTROL,
  Own: 0,
  Relay: RELAY,
  Sweep: 0,
};

// Beam mark styles; each has a full and a faded Graphics (`style * 2 + faded`).
const IN_ZONE = 0;
const CROSS_ZONE = 1;
const DROPPED_CROSS = 2;
const DOTS = 3;
const ARROWS = 4;
const STYLES = 5;

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
  style: { color: number; alpha: number; width?: number; cap?: 'round' };
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

/** Every packet in flight, as columns of plain data: a beam's route (`from`,
 *  `control`, `to` as six floats), start time, ends and flags. Removal swaps
 *  the last beam in, so the pool stays dense and nothing is allocated. */
class BeamPool {
  len = 0;
  readonly route = new Float32Array(BEAM_CAPACITY * 6);
  readonly start = new Float64Array(BEAM_CAPACITY);
  readonly src = new Int32Array(BEAM_CAPACITY);
  readonly dst = new Int32Array(BEAM_CAPACITY);
  readonly flags = new Uint8Array(BEAM_CAPACITY);

  push(path: QuadPath, start: number, src: number, dst: number, flags: number): void {
    if (this.len === BEAM_CAPACITY) return;
    const o = this.len * 6;
    this.route[o] = path.from.x;
    this.route[o + 1] = path.from.y;
    this.route[o + 2] = path.control.x;
    this.route[o + 3] = path.control.y;
    this.route[o + 4] = path.to.x;
    this.route[o + 5] = path.to.y;
    this.start[this.len] = start;
    this.src[this.len] = src;
    this.dst[this.len] = dst;
    this.flags[this.len] = flags;
    this.len += 1;
  }

  /** Drop beam `i` by moving the last beam into its slot. */
  removeAt(i: number): void {
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
  // One Graphics per beam mark style and fade, each redrawn by `#stepBeams`.
  readonly #beamGfx: Graphics[] = [];
  readonly #pool = new BeamPool();
  // How each beam Graphics is painted once its marks are added, rebuilt with the
  // theme so a frame builds no style of its own.
  #brushes: Brush[] = [];
  // Scratch the beam paths and the frame draw reuse.
  readonly #path: QuadPath = { from: { x: 0, y: 0 }, control: { x: 0, y: 0 }, to: { x: 0, y: 0 } };
  readonly #at: Point = { x: 0, y: 0 };
  readonly #from: Spot = { id: 0, zone: 0, x: 0, y: 0, r: 0 };
  readonly #to: Spot = { id: 0, zone: 0, x: 0, y: 0, r: 0 };
  #theme: Theme;

  #nodes = new Map<number, NodeGfx>();
  #layout: StageLayout | null = null;
  #titles: readonly string[] = [];
  #cluster: ClusterState | null = null;
  #roles: Roles | null = null;
  #focusId: number | null = null;
  // Whether the beam Graphics currently hold marks, so the always-on ticker
  // issues exactly one final clear when the pool drains and does nothing after.
  #beamsActive = false;
  #lastTick = -1;
  #lastDisagreement = 0;
  // Delivered beams that landed since the last frame, coalesced so each disc
  // pulses at most once per frame: without this, several arrivals in one frame
  // each fire their own tween on the same `disc.alpha` and strobe it. Drained
  // by a single rAF (`#flashRaf`, 0 when idle).
  #pendingFlash = new Set<number>();
  #flashRaf = 0;

  private constructor(app: Application, theme: Theme) {
    this.#app = app;
    this.#theme = theme;
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
    this.#app.ticker.add(this.#stepBeams, this);
    // The in-flight beams, read by the design audit as Chart.svelte's `uplot` is: set once, so a frame pays nothing.
    Object.assign(this.#app.canvas, { beams: this.#pool });
  }

  /** Build the renderer and attach its canvas to `container`. Async because
   *  Pixi's WebGL/WebGPU context init is async. */
  static async create(container: HTMLElement, theme: Theme): Promise<StageRenderer> {
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
    return new StageRenderer(app, theme);
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
    this.#drawGuides();
    this.#rebuildLabels();
    const dataset = this.#app.canvas.dataset;
    dataset.centre = JSON.stringify(layout.centre);
    dataset.rings = JSON.stringify(layout.rings);
    dataset.crossWidth = String(CROSS_WIDTH);
    if (this.#cluster !== null) this.#drawArcs(this.#cluster);
  }

  /** Apply steady per-node state: each node's cell arc and cluster-wide
   *  convergence. */
  setCluster(state: ClusterState | null): void {
    if (state === null) return;
    // A tick that runs backward means the session was reset (a fresh sim starts
    // at tick 0). Tear down transient state, killing in-flight beams before
    // touching node graphics, so a stale tween can't animate a disc being
    // reconciled.
    if (state.tick < this.#lastTick) {
      this.#clearBeams();
      this.#lastDisagreement = 0;
    }
    this.#lastTick = state.tick;
    this.#cluster = state;
    this.#drawArcs(state);

    // The cluster agrees the instant every node's total matches. Pulse once on
    // the transition from disagreement to agreement (and only once there is a
    // real total to agree on).
    let max = 0;
    let min = state.nodes.length === 0 ? 0 : Number.POSITIVE_INFINITY;
    for (const node of state.nodes) {
      if (node.aggregate_total > max) max = node.aggregate_total;
      if (node.aggregate_total < min) min = node.aggregate_total;
    }
    const disagreement = max - min;
    if (this.#lastDisagreement > 0 && disagreement === 0 && state.oracle_total > 0) {
      this.#pulse();
    }
    this.#lastDisagreement = disagreement;
  }

  /** Mark the selected origin and its gateways, or clear the marks with `null`. */
  setRoles(roles: Roles | null): void {
    this.#roles = roles;
    for (const [id, gfx] of this.#nodes) this.#drawMark(id, gfx);
  }

  /** Ring the node with keyboard focus, or clear the ring with `null`. */
  setFocus(id: number | null): void {
    this.#focusId = id;
    for (const [nodeId, gfx] of this.#nodes) this.#drawFocus(nodeId, gfx);
  }

  /** Animate the gossip packets in a step's event batch as beams. */
  applyEvents(events: SimEvent[]): void {
    for (const event of events) {
      const kind = event.kind;
      if (kind.type === 'PacketSent' || kind.type === 'PacketDropped') {
        let flags = KIND_FLAGS[kind.kind] | (kind.tier === 'CrossZone' ? CROSS : 0);
        if (kind.type === 'PacketDropped') flags |= DROPPED;
        this.#beam(kind.src, kind.dst, flags);
      }
      // `PacketDelivered` is the receiver-side echo of a `PacketSent` already
      // drawn at the sender, so it would double the beams: skip it.
    }
  }

  /** Re-tint every mark from `theme` (an OS scheme or `data-theme` change),
   *  immediately, including while paused. */
  setTheme(theme: Theme): void {
    this.#theme = theme;
    this.#restyle();
    this.#app.renderer.background.color = theme.bg;
    this.#drawGuides();
    for (const [id, gfx] of this.#nodes) this.#drawNode(id, gfx);
    this.#rebuildLabels();
  }

  /** Tear the renderer down: kill every tween (node and beam), free every GPU
   *  resource, drop the canvas. Safe to call once; the owner must not reuse the
   *  instance after. */
  destroy(): void {
    this.#clearBeams();
    for (const gfx of this.#nodes.values()) {
      for (const tween of gfx.tweens) tween.kill();
      gfx.tweens.clear();
    }
    this.#nodes.clear();
    this.#app.ticker.remove(this.#stepBeams, this);
    this.#app.canvas.remove();
    this.#app.destroy(true, { children: true, texture: true, textureSource: true });
  }

  /** Rebuild the brush of every beam style and fade from the theme. */
  #restyle(): void {
    const { muted, fg } = this.#theme;
    const marks: [boolean, number, number][] = [
      [true, muted, IN_WIDTH], // IN_ZONE
      [true, fg, CROSS_WIDTH], // CROSS_ZONE
      [true, muted, CROSS_WIDTH], // DROPPED_CROSS
      [false, muted, 0], // DOTS
      [false, fg, 0], // ARROWS
    ];
    this.#brushes = marks.flatMap(([stroke, color, width]) =>
      [BEAM_ALPHA, BEAM_ALPHA * FADED_ALPHA].map((alpha) => ({
        stroke,
        style: stroke ? { width, color, alpha, cap: 'round' as const } : { color, alpha },
      })),
    );
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
    // Drop any queued delivery pulse for this node: a leaving node shouldn't
    // linger in the queue.
    this.#pendingFlash.delete(id);
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
      node.arc.circle(0, 0, reach).stroke({ width: ARC_WIDTH, color: this.#theme.ok });
      return;
    }
    const start = -Math.PI / 2;
    node.arc
      .arc(0, 0, reach, start, start + Math.min(f, 1 - ARC_NOTCH) * 2 * Math.PI)
      .stroke({ width: ARC_WIDTH, color: this.#theme.warn, cap: 'butt' });
  }

  #drawArcs(state: ClusterState): void {
    const oracle = Math.max(state.oracle_total, 1);
    for (const node of state.nodes) {
      const gfx = this.#nodes.get(node.id);
      if (gfx !== undefined) this.#drawArc(gfx, node.aggregate_total / oracle);
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

  /** Record one gossip packet as a beam along the route between its nodes'
   *  live (mid-glide) positions, captured now so the beam keeps flying its
   *  original path even as the nodes glide. */
  #beam(src: number, dst: number, flags: number): void {
    const a = this.#nodes.get(src);
    const b = this.#nodes.get(dst);
    if (a === undefined || b === undefined || this.#layout === null) return;
    this.#from.x = a.root.position.x;
    this.#from.y = a.root.position.y;
    this.#from.zone = a.zone;
    this.#to.x = b.root.position.x;
    this.#to.y = b.root.position.y;
    this.#to.zone = b.zone;
    beamPath(this.#from, this.#to, this.#layout.centre, this.#path);
    this.#pool.push(this.#path, performance.now(), src, dst, flags);
    this.#beamsActive = true;
  }

  /** Whether a beam is the selected origin's: sent by it, or a relay sent by
   *  one of its gateways. Every other beam is drawn as faded context. */
  #isContext(src: number, flags: number): boolean {
    const roles = this.#roles;
    if (roles === null || src === roles.origin) return false;
    return !((flags & RELAY) !== 0 && roles.gateways.has(src));
  }

  /** The per-frame beam render: one pass over the pool that adds every live
   *  beam to its style's Graphics by wall-clock age, then one stroke or fill
   *  per style. Idle pays nothing: when the pool drains it issues one final
   *  clear and then returns immediately each tick.
   *
   *  A stroke beam flies along its route: a short tail from the eased head back
   *  through the route's midpoint. A control packet is a dot at the head, with
   *  no tail. A cross-zone beam also wears an arrowhead along the route's
   *  tangent. A dropped beam stops at `DROP_FRACTION` and turns muted. */
  #stepBeams(): void {
    if (this.#guides.destroyed) return;
    const pool = this.#pool;
    if (pool.len === 0) {
      if (this.#beamsActive) {
        for (const g of this.#beamGfx) g.clear();
        this.#beamsActive = false;
      }
      return;
    }
    for (const g of this.#beamGfx) g.clear();
    const now = performance.now();
    const reduce = this.#reduceMotion;
    const duration = reduce ? REDUCE_FADE_MS : BEAM_FLIGHT_MS;
    const path = this.#path;
    const at = this.#at;
    let used = 0;

    // Backwards, so the beam a removal swaps into slot `i` is one already drawn.
    for (let i = pool.len - 1; i >= 0; i--) {
      const age = now - pool.start[i];
      const flags = pool.flags[i];
      if (age >= duration) {
        // Flight done. A delivered (non-reduced) beam hands off to the target's
        // pulse; removing it this same frame means that pulse fires exactly once.
        if ((flags & DROPPED) === 0 && !reduce) this.#queueFlash(pool.dst[i]);
        pool.removeAt(i);
        continue;
      }
      const o = i * 6;
      path.from.x = pool.route[o];
      path.from.y = pool.route[o + 1];
      path.control.x = pool.route[o + 2];
      path.control.y = pool.route[o + 3];
      path.to.x = pool.route[o + 4];
      path.to.y = pool.route[o + 5];

      const cross = (flags & CROSS) !== 0;
      const dropped = (flags & DROPPED) !== 0;
      const fade = this.#isContext(pool.src[i], flags) ? 1 : 0;
      const end = dropped ? DROP_FRACTION : 1;
      const head = reduce ? end : easeInOutQuad(age / duration) * end;

      if ((flags & CONTROL) !== 0) {
        pointOn(path, head, at);
        this.#beamGfx[DOTS * 2 + fade].circle(at.x, at.y, DOT_R);
        used |= 1 << (DOTS * 2 + fade);
        continue;
      }
      const style = cross ? (dropped ? DROPPED_CROSS : CROSS_ZONE) : IN_ZONE;
      const g = this.#beamGfx[style * 2 + fade];
      used |= 1 << (style * 2 + fade);
      if (reduce) {
        // No flight: the whole route at constant alpha: a diff, not travel.
        pointOn(path, 0, at);
        g.moveTo(at.x, at.y);
        for (let s = 1; s <= ROUTE_SEGMENTS; s++) {
          pointOn(path, (head * s) / ROUTE_SEGMENTS, at);
          g.lineTo(at.x, at.y);
        }
      } else {
        const tail = Math.max(0, head - TAIL);
        pointOn(path, tail, at);
        g.moveTo(at.x, at.y);
        pointOn(path, (tail + head) / 2, at);
        g.lineTo(at.x, at.y);
        pointOn(path, head, at);
        g.lineTo(at.x, at.y);
      }
      if (cross && !dropped) {
        this.#arrowhead(this.#beamGfx[ARROWS * 2 + fade], head);
        used |= 1 << (ARROWS * 2 + fade);
      }
    }

    for (let k = 0; k < STYLES * 2; k++) {
      if ((used & (1 << k)) === 0) continue;
      const brush = this.#brushes[k];
      if (brush.stroke) this.#beamGfx[k].stroke(brush.style);
      else this.#beamGfx[k].fill(brush.style);
    }
  }

  /** Add a triangle at route position `head`, pointing along the route. */
  #arrowhead(g: Graphics, head: number): void {
    const path = this.#path;
    const at = this.#at;
    pointOn(path, head, at);
    // The route's tangent at the head: B'(t) = 2(1 - t)(C - A) + 2t(B - C).
    const tx = 2 * (1 - head) * (path.control.x - path.from.x) + 2 * head * (path.to.x - path.control.x);
    const ty = 2 * (1 - head) * (path.control.y - path.from.y) + 2 * head * (path.to.y - path.control.y);
    const len = Math.hypot(tx, ty);
    if (len === 0) return;
    const ux = tx / len;
    const uy = ty / len;
    const bx = at.x - ux * ARROW_LEN;
    const by = at.y - uy * ARROW_LEN;
    g.moveTo(at.x, at.y)
      .lineTo(bx - uy * ARROW_HALF, by + ux * ARROW_HALF)
      .lineTo(bx + uy * ARROW_HALF, by - ux * ARROW_HALF)
      .closePath();
  }

  /** Queue node `dst` for a coalesced delivery pulse on the next frame, instead
   *  of pulsing it now, so several beams landing in one frame brighten the disc
   *  once, not N times. A no-op under reduced motion: the pulse is motion the
   *  user opted out of, and the arc/count already updated. */
  #queueFlash(dst: number): void {
    if (this.#reduceMotion) return;
    this.#pendingFlash.add(dst);
    if (this.#flashRaf === 0) {
      this.#flashRaf = requestAnimationFrame(() => this.#flushFlashes());
    }
  }

  /** Fire one delivery pulse per disc that received a beam this frame. Resolves
   *  the disc fresh (a node may have left between queue and flush) and brightens
   *  to a gentle dim floor (0.7) over a soft duration, `overwrite: 'auto'` so a
   *  delivery on the next frame can't strand `disc.alpha` below 1. */
  #flushFlashes(): void {
    this.#flashRaf = 0;
    for (const dst of this.#pendingFlash) {
      const disc = this.#nodes.get(dst)?.disc;
      if (disc === undefined) continue;
      gsap.fromTo(
        disc,
        { alpha: 1 },
        { alpha: 0.7, duration: 0.16, yoyo: true, repeat: 1, overwrite: 'auto' },
      );
    }
    this.#pendingFlash.clear();
  }

  /** Cancel a queued flush and drop every pending target: a teardown or beam
   *  sweep must not pulse a disc that is being freed. */
  #cancelFlashes(): void {
    if (this.#flashRaf !== 0) {
      cancelAnimationFrame(this.#flashRaf);
      this.#flashRaf = 0;
    }
    this.#pendingFlash.clear();
  }

  /** The synchronized convergence pulse: one expanding ring from each node,
   *  fired the instant the cluster agrees. Reduced motion skips it: the arcs
   *  already snapped to ok. */
  #pulse(): void {
    if (this.#reduceMotion) return;
    for (const node of this.#nodes.values()) {
      const ring = new Graphics();
      node.root.addChild(ring);
      // A bold ring (3px, near-opaque at the outset) reads as a deliberate
      // "agrees now" beat rather than an ambient halo as it expands and fades.
      const state = { r: node.radius, alpha: 0.85 };
      gsap.to(state, {
        r: node.radius * 3,
        alpha: 0,
        duration: 0.6,
        ease: 'power2.out',
        onUpdate: () => {
          // The ring can be destroyed (its node left, or a reset tore the stage
          // down) while this tween still ticks.
          if (ring.destroyed) return;
          ring
            .clear()
            .circle(0, 0, state.r)
            .stroke({ width: 3, color: this.#theme.ok, alpha: state.alpha });
        },
        onComplete: () => {
          if (!ring.destroyed) ring.destroy();
        },
      });
    }
  }

  /** Drop every in-flight beam touching node `id`, called when that node
   *  leaves, so no beam draws to or from a disc that is being destroyed. */
  #cancelBeamsFor(id: number): void {
    const pool = this.#pool;
    for (let i = pool.len - 1; i >= 0; i--) {
      if (pool.src[i] === id || pool.dst[i] === id) pool.removeAt(i);
    }
  }

  #clearBeams(): void {
    this.#cancelFlashes();
    this.#pool.len = 0;
    for (const g of this.#beamGfx) g.clear();
    this.#beamsActive = false;
  }
}

/** GSAP `power2.inOut` reproduced locally: the beam ticker (not a tween) drives
 *  flight progress, so it eases the raw age ratio itself. */
function easeInOutQuad(t: number): number {
  return t < 0.5 ? 2 * t * t : 1 - 2 * (1 - t) * (1 - t);
}
