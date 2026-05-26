// The PixiJS stage renderer: a framework-agnostic class that owns the WebGL
// canvas and turns simulation state into the deep-canvas node stage. Kept out
// of Svelte's reactivity on purpose — Svelte feeds it `setCluster` (steady
// per-node state) and `applyEvents` (transient gossip packets); the class owns
// all imperative Pixi/GSAP bookkeeping and cleanup.
//
// Three layers, back to front: the guide ring, light-beam packets, then the
// node discs with their cell arcs on top (so beams emanate from beneath the
// nodes they connect).
//
// Nodes are keyed by **stable id**, never by array position. `setCluster`
// reconciles the live id set against what is on stage: a newcomer fades and
// scales in at its ring slot, a departed node scales out and is destroyed
// (its beams cancelled), and every survivor glides to the slot its new rank
// gives it. So a join or leave re-spaces the ring without any node jumping or
// renumbering — the membership change *is* the animation.

import { gsap } from 'gsap';
import { Application, Container, Graphics } from 'pixi.js';
import type { ClusterState, SimEvent } from '../sim/types';
import {
  DOT_THRESHOLD,
  fitTransform,
  guideRadius,
  nodePosition,
  nodeRadius,
  stageCenter,
  type Point,
  type StageTransform,
} from './layout';

// Palette mirrors the CSS design tokens in `app.css` (Pixi wants numbers, not
// hex strings). A paper-bright stage with dark slate discs as the solid
// figures. Two distinct color families, never mixed:
//   - Three **state** signal hues — amber for "in flight / not yet agreed",
//     green for "converged", red for a rejected request — each always paired
//     with a shape cue (arc length, the pulse), never color alone.
//   - One **ambient traffic** accent — a muted steel-blue for the gossip beams.
//     Gossip is structural, not a state signal, so it gets its own cool hue
//     (analogous to the cool paper bg) and never borrows amber/green/red. A
//     *dropped* beam desaturates toward a neutral cool grey as it dies mid-flight
//     (loss = chroma drained, paired with its visible death + fade), so it reads
//     as "lost" without the alarm of red.
// Every mark clears 3:1 on the stage.
const COLOR_STAGE_BG = 0xe8edf2;
const COLOR_GRID = 0xd2d9e1;
const COLOR_NODE_FILL = 0x39424f;
const COLOR_DIRTY = 0xb3720d; // in flight / still climbing
const COLOR_CONVERGED = 0x137a52; // settled / agreed
const COLOR_BEAM = 0x3f6090; // gossip traffic — ambient steel-blue accent
const COLOR_BEAM_LOST = 0x8b94a1; // dropped beam's terminal color — cool grey
// The selection ring (the inspected node). Drawn in the dark ink — chrome, not
// a signal — so it never reads as amber/green convergence state, and it is
// paired with the inspector opening, so selection is signified by more than the
// ring alone.
const COLOR_SELECT = 0x1b2330;

// A node counts as caught up when its view is within this fraction of the
// cluster's true total — the threshold that flips its arc from amber to green.
const CONVERGED_EPSILON = 0.001;

// Wall-clock flight time of a gossip packet beam. Eased so the beam accelerates
// off the source and decelerates into the target — motion that reads as cause
// then effect.
const BEAM_FLIGHT_MS = 520;
// Reduced motion: a beam is a static edge that fades over this span instead of
// flying — the same lifetime bound, no travel.
const REDUCE_FADE_MS = 400;
// The comet: a bright crisp head leads, a soft wake fades behind it, so a beam
// reads unmistakably as a particle traveling its edge. `BEAM_WAKE` is the wake's
// length as a fraction of the edge; the head is a filled dot of radius
// `width · BEAM_HEAD_R`, set in a faint glow of radius `width · BEAM_GLOW_R` at
// `BEAM_GLOW_ALPHA` of the beam's alpha (plain layered alpha, not additive —
// additive washes toward white on the paper-bright stage).
const BEAM_WAKE = 0.26;
const BEAM_HEAD_R = 1.4;
const BEAM_GLOW_R = 3.0;
const BEAM_GLOW_ALPHA = 0.2;
// Soft beam edges: a beam ramps in over the first slice of its flight and (when
// delivered) back out over the last slice as it reaches the target, so it
// neither snaps on at the source nor vanishes abruptly on arrival — the
// target's pulse takes over the "news landed" beat. Fractions of the flight.
const BEAM_FADE_IN = 0.1;
const BEAM_FADE_OUT = 0.12;
// A dropped packet dies partway across instead of landing.
const DROP_FRACTION = 0.5;
// A last-resort sanity ceiling against a pathological event-batch dump — *not* a
// render-cost cap. Every beam is a few primitives redrawn by a single ticker
// pass into one persistent Graphics, and beams self-expire after one flight, so
// the live set is bounded by sends-in-the-last-flight-window. Measured: a
// 100-node sustained overload (the UI's worst case) peaks in the low tens of
// thousands, so this leaves an order of magnitude of headroom and is not
// reached in normal operation.
const MAX_BEAMS = 32768;
// Above this many concurrent beams each comet drops its head glow and filled
// head dot for an all-stroke "lean comet" (wake + a short bright head streak),
// so dense gossip stays fast; below it every beam is the full comet (glow under
// a soft wake under a crisp filled head).
const BEAM_DETAIL_CAP = 400;

// Membership-change motion (join / leave / re-space). Purposeful only: a join
// grows in, a leave shrinks out, survivors glide. Reduced motion skips all of
// it and snaps to the final layout.
const ENTER_MS = 420;
const EXIT_MS = 340;
const GLIDE_MS = 520;
const ENTER_SCALE = 0.25;

/** One node's persistent display objects, keyed by stable id. */
interface NodeGfx {
  root: Container;
  disc: Graphics;
  arc: Graphics;
  /** A persistent ring drawn around this node while it is the selected one,
   *  cleared otherwise. A child of `root`, so it glides with the node on a
   *  re-space and is destroyed with it on leave. */
  selectRing: Graphics;
  /** The ring slot this node is gliding to (logical coords). */
  center: Point;
  /** Radius the disc geometry was last drawn at; redrawn only when a join or
   *  leave re-spaces the ring and changes it. */
  radius: number;
  /** Live tweens (enter / exit / glide), killed on destroy so a stale tween
   *  can never touch a freed display object. */
  tweens: Set<gsap.core.Tween>;
}

/** One packet in flight, as plain wall-time data. A single ticker pass
 *  (`#stepBeams`) redraws every live beam into one shared Graphics each frame
 *  and deletes it when its flight ends — no per-beam display object or tween.
 *  `src` / `dst` are stable ids; `a` / `b` are the endpoint centers captured at
 *  creation (a node leaving mid-flight is swept by `#cancelBeamsFor`). */
interface Beam {
  src: number;
  dst: number;
  a: Point;
  b: Point;
  startMs: number;
  width: number;
  dropped: boolean;
}

export class StageRenderer {
  readonly #app: Application;
  readonly #root: Container;
  readonly #guideLayer: Container;
  readonly #packetLayer: Container;
  readonly #nodeLayer: Container;
  readonly #reduceMotion: boolean;
  // One persistent Graphics holding every live beam's strokes, redrawn each
  // frame by `#stepBeams` on the app ticker (N beams, one draw call).
  readonly #packetGfx: Graphics;

  #nodes = new Map<number, NodeGfx>();
  #beams = new Set<Beam>();
  // Whether `#packetGfx` currently holds strokes — so the always-on ticker
  // issues exactly one final `.clear()` when the beam set drains, then does zero
  // work each idle tick after.
  #beamsActive = false;
  #lastTick = -1;
  #lastDisagreement = 0;
  // The stable id of the selected node, or null. A static ring marks it; updated
  // by `setSelected` and re-applied each `setCluster` so a re-space keeps it.
  #selectedId: number | null = null;
  // Delivered beams that landed since the last frame, coalesced so each disc
  // pulses at most once per frame: without this, several arrivals in one frame
  // each fire their own tween on the same `disc.alpha` and strobe it. Drained
  // by a single rAF (`#flashRaf`, 0 when idle).
  #pendingFlash = new Set<number>();
  #flashRaf = 0;

  private constructor(app: Application) {
    this.#app = app;
    this.#reduceMotion =
      typeof window !== 'undefined' &&
      window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    this.#root = new Container();
    this.#guideLayer = new Container();
    this.#packetLayer = new Container();
    this.#nodeLayer = new Container();
    this.#root.addChild(this.#guideLayer, this.#packetLayer, this.#nodeLayer);
    this.#app.stage.addChild(this.#root);

    const guide = new Graphics()
      .circle(stageCenter.x, stageCenter.y, guideRadius)
      .stroke({ width: 1.5, color: COLOR_GRID });
    this.#guideLayer.addChild(guide);

    // Parent the shared beam Graphics before registering the ticker, so the
    // first tick never fires against an orphaned object.
    this.#packetGfx = new Graphics();
    this.#packetLayer.addChild(this.#packetGfx);
    this.#app.ticker.add(this.#stepBeams, this);
  }

  /** Build the renderer and attach its canvas to `container`. Async because
   *  Pixi's WebGL/WebGPU context init is async. */
  static async create(container: HTMLElement): Promise<StageRenderer> {
    const app = new Application();
    await app.init({
      background: COLOR_STAGE_BG,
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
    return new StageRenderer(app);
  }

  /** Resize the canvas to `width × height` (CSS pixels) and rescale the logical
   *  stage to fit, centered. Returns the transform so the DOM label overlay can
   *  position itself identically. */
  resize(width: number, height: number): StageTransform {
    if (width <= 0 || height <= 0) return fitTransform(width, height);
    this.#app.renderer.resize(width, height);
    const t = fitTransform(width, height);
    this.#root.scale.set(t.scale);
    this.#root.position.set(t.offsetX, t.offsetY);
    return t;
  }

  /** Apply steady per-node state: reconcile the live id set against the stage
   *  (enter newcomers, exit departed, glide survivors to their new ranks),
   *  then update each node's cell arc and detect cluster-wide convergence. */
  setCluster(state: ClusterState | null): void {
    if (state === null) return;

    // A tick that runs backward means the session was reset (a fresh sim starts
    // at tick 0). Tear down transient state — killing in-flight beams *before*
    // touching node graphics — so a stale tween can't animate a disc we're
    // about to reconcile.
    if (state.tick < this.#lastTick) {
      this.#clearBeams();
      this.#lastDisagreement = 0;
    }
    this.#lastTick = state.tick;

    const n = state.nodes.length;
    const radius = nodeRadius(n);
    const liveIds = new Set<number>();

    // Enter + glide: ensure a gfx for every live id, sitting at the ring slot
    // its rank (position in the live list) gives it.
    state.nodes.forEach((node, rank) => {
      liveIds.add(node.id);
      const target = nodePosition(rank, n);
      let gfx = this.#nodes.get(node.id);
      if (gfx === undefined) {
        gfx = this.#createNode(node.id, target, radius);
      } else {
        this.#glide(gfx, target);
        if (gfx.radius !== radius) {
          this.#drawDisc(gfx.disc, radius);
          gfx.radius = radius;
        }
      }
    });

    // Exit: any node whose id is no longer live scales out and is destroyed,
    // its in-flight beams cancelled first (explicit cleanup, mirroring the
    // tick-regress beam sweep).
    for (const [id, gfx] of this.#nodes) {
      if (!liveIds.has(id)) {
        this.#cancelBeamsFor(id);
        this.#removeNode(id, gfx);
      }
    }

    // Arcs + convergence, over the live nodes only.
    const oracle = Math.max(state.oracle_total, 1);
    let max = 0;
    let min = Number.POSITIVE_INFINITY;
    for (const node of state.nodes) {
      const total = node.aggregate_total;
      if (total > max) max = total;
      if (total < min) min = total;
      const gfx = this.#nodes.get(node.id);
      if (gfx !== undefined) {
        this.#drawArc(gfx, total / oracle, n);
        // Re-apply selection after the arc/radius update so a re-space keeps the
        // ring on the right disc at the freshly-drawn radius.
        this.#drawSelection(gfx, node.id === this.#selectedId);
      }
    }
    if (n === 0) min = 0;

    // The cluster agrees the instant every node's total matches: max − min
    // hits zero. Pulse once on the transition from disagreement to agreement
    // (and only once there is a real total to agree on).
    const disagreement = max - min;
    if (this.#lastDisagreement > 0 && disagreement === 0 && state.oracle_total > 0) {
      this.#pulse();
    }
    this.#lastDisagreement = disagreement;
  }

  /** Animate the gossip packets in a step's event batch as light-beams. */
  applyEvents(events: SimEvent[]): void {
    for (const event of events) {
      const kind = event.kind;
      if (kind.type === 'PacketSent') {
        this.#beam(kind.src, kind.dst, kind.bytes, false);
      } else if (kind.type === 'PacketDropped') {
        this.#beam(kind.src, kind.dst, kind.bytes, true);
      }
      // `PacketDelivered` is the receiver-side echo of a `PacketSent` already
      // drawn at the sender, so it would double the beams — skip it.
    }
  }

  /** Mark node `id` as selected (or clear with `null`), drawing a static ring
   *  around it. Takes effect immediately — including while paused — by redrawing
   *  every node's selection state, and is re-applied by `setCluster` so a
   *  re-space keeps the ring on the right disc at the right radius. */
  setSelected(id: number | null): void {
    this.#selectedId = id;
    for (const [nodeId, gfx] of this.#nodes) {
      this.#drawSelection(gfx, nodeId === id);
    }
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

  /** Create one node's display objects at `center` and animate its join. */
  #createNode(id: number, center: Point, radius: number): NodeGfx {
    const root = new Container();
    root.position.set(center.x, center.y);
    const disc = new Graphics();
    const arc = new Graphics();
    // Selection ring on top: it sits outside the disc and arc, so it is never
    // occluded by them and reads as a frame around the whole node glyph.
    const selectRing = new Graphics();
    root.addChild(disc, arc, selectRing);
    this.#drawDisc(disc, radius);
    this.#nodeLayer.addChild(root);
    const gfx: NodeGfx = { root, disc, arc, selectRing, center, radius, tweens: new Set() };
    this.#nodes.set(id, gfx);
    if (id === this.#selectedId) this.#drawSelection(gfx, true);

    if (this.#reduceMotion) {
      root.alpha = 1;
      root.scale.set(1);
      return gfx;
    }
    // Join: fade in and scale up from a small disc — growth reads as "a member
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
    return gfx;
  }

  /** Glide a survivor's disc to the ring slot its new rank gives it. */
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
    // Drop any queued delivery pulse for this node — the flush re-resolves and
    // would skip it anyway, but a leaving node shouldn't linger in the queue.
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

  /** Draw a node's disc at `radius` (clearing any prior geometry). A flat slate
   *  fill — on the light stage the disc edge is defined by fill-vs-canvas
   *  contrast, so a stroke would only add invisible ink. */
  #drawDisc(disc: Graphics, radius: number): void {
    disc.clear().circle(0, 0, radius).fill(COLOR_NODE_FILL);
  }

  /** Draw a node's cell arc: an annulus around the disc whose sweep is the
   *  node's view as a fraction of the true total, so every arc fills to a full
   *  ring exactly when the cluster converges. Above the dot threshold the arc
   *  is dropped and the disc tints by the same fraction instead. */
  #drawArc(node: NodeGfx, fraction: number, n: number): void {
    const f = Math.max(0, Math.min(1, fraction));
    const converged = f >= 1 - CONVERGED_EPSILON;
    const color = converged ? COLOR_CONVERGED : COLOR_DIRTY;
    node.arc.clear();

    if (n > DOT_THRESHOLD) {
      // Intensity dot: there is no room for an arc, so the disc *fill* itself
      // carries how caught up the node is — lerped from the cold slate base
      // toward the signal hue. (Pixi `tint` multiplies, so tinting the dark
      // base would only ever darken it; we redraw the fill instead.) Position
      // on the ring still encodes identity.
      node.disc.tint = 0xffffff;
      node.disc.alpha = 1;
      const fill = f <= 0 ? COLOR_NODE_FILL : lerpColor(COLOR_NODE_FILL, color, 0.35 + 0.65 * f);
      node.disc.clear().circle(0, 0, node.radius).fill(fill);
      return;
    }

    // Arc mode: the disc keeps its slate base fill (crossing the dot threshold
    // changes the node count, hence the radius, so the glide loop's radius-change
    // redraw already restored it from any dot-mode recolour). The arc carries
    // the fraction.
    node.disc.tint = 0xffffff;
    node.disc.alpha = 1;
    if (f <= 0) return;
    const radius = node.radius + 4;
    const start = -Math.PI / 2;
    node.arc
      .arc(0, 0, radius, start, start + f * 2 * Math.PI)
      .stroke({ width: 3, color, cap: 'round' });
  }

  /** Draw or clear the selection ring around a node — a static dark ring just
   *  outside the cell arc, distinct from the expanding convergence pulse and the
   *  amber/green arc. */
  #drawSelection(node: NodeGfx, selected: boolean): void {
    node.selectRing.clear();
    if (!selected) return;
    node.selectRing
      .circle(0, 0, node.radius + 9)
      .stroke({ width: 2.5, color: COLOR_SELECT, alpha: 0.9 });
  }

  /** Record one gossip packet as a beam. No Graphics, no tween — just a data
   *  record the ticker draws and ages out; endpoints are captured here so the
   *  beam keeps flying along its original path even as the nodes glide. */
  #beam(src: number, dst: number, bytes: number, dropped: boolean): void {
    // Sanity ceiling only — beams self-expire, so the live set is bounded by the
    // last flight window (see MAX_BEAMS); drop excess rather than unbound the set
    // if an event batch ever dumps pathologically many at once.
    if (this.#beams.size >= MAX_BEAMS) return;
    const a = this.#liveCenter(src);
    const b = this.#liveCenter(dst);
    if (a === null || b === null) return;
    this.#beams.add({
      src,
      dst,
      a,
      b,
      startMs: performance.now(),
      width: 1.5 + Math.min(bytes, 400) / 100,
      dropped,
    });
    this.#beamsActive = true;
  }

  /** The per-frame beam render: one ticker pass that redraws every live beam
   *  into the shared `#packetGfx` by wall-clock age and deletes it when its
   *  flight ends. Idle pays nothing — when the set drains it issues one final
   *  `.clear()` (via `#beamsActive`) and then returns immediately each tick.
   *
   *  A beam is a comet: a bright crisp head leads, a soft wake fades behind it,
   *  along the eased flight, so it reads as a particle traveling its edge. Below
   *  `BEAM_DETAIL_CAP` live beams the head sits in a faint glow and is a filled
   *  dot (the full comet); above it the comet goes all-stroke (wake + a short
   *  head streak) so dense gossip stays fast. A delivered beam is the steel-blue
   *  gossip accent, reaches the target, and hands the "news landed" beat to its
   *  pulse; a dropped beam dies at `DROP_FRACTION`, draining to grey as it goes —
   *  the partition / packet-loss cue. */
  #stepBeams(): void {
    if (this.#packetGfx.destroyed) return;
    if (this.#beams.size === 0) {
      if (this.#beamsActive) {
        this.#packetGfx.clear();
        this.#beamsActive = false;
      }
      return;
    }

    const g = this.#packetGfx.clear();
    const now = performance.now();
    const duration = this.#reduceMotion ? REDUCE_FADE_MS : BEAM_FLIGHT_MS;
    const detail = this.#beams.size <= BEAM_DETAIL_CAP;

    // Deleting from a Set during its own for…of is well-defined: the deleted
    // entry simply won't be revisited.
    for (const beam of this.#beams) {
      const age = now - beam.startMs;
      if (age >= duration) {
        // Flight done. A delivered (non-reduced) beam hands off to the target's
        // pulse; deleting this same frame means that pulse fires exactly once.
        if (!beam.dropped && !this.#reduceMotion) this.#queueFlash(beam.dst);
        this.#beams.delete(beam);
        continue;
      }

      if (this.#reduceMotion) {
        // No flight: a static edge fading over its lifetime — a diff, not travel.
        // Delivered stays steel-blue; dropped reads as the terminal grey.
        const color = beam.dropped ? COLOR_BEAM_LOST : COLOR_BEAM;
        const alpha = (beam.dropped ? 0.3 : 0.9) * (1 - age / duration);
        g.moveTo(beam.a.x, beam.a.y)
          .lineTo(beam.b.x, beam.b.y)
          .stroke({ width: beam.width, color, alpha, cap: 'round' });
        continue;
      }

      // Eased flight. A dropped beam only crosses to `DROP_FRACTION`; a delivered
      // one reaches the target. The head leads at `tip`; the wake trails it.
      const end = beam.dropped ? DROP_FRACTION : 1;
      const tip = easeInOutQuad(age / duration) * end;
      const wakeStart = Math.max(0, tip - BEAM_WAKE);
      const headPt = lerp(beam.a, beam.b, tip);
      const wakePt = lerp(beam.a, beam.b, wakeStart);
      // Soft edges multiply the base alpha: ramp in over the first slice; a
      // delivered beam ramps back out over the last slice into the target, while
      // a dropped one carries its own mid-flight dim so it reads as fading *out*
      // partway across, distinct from a delivery.
      const fadeIn = clamp01(tip / (end * BEAM_FADE_IN));
      const base = beam.dropped
        ? 0.7 * (1 - tip / end)
        : 0.95 * clamp01((end - tip) / (end * BEAM_FADE_OUT));
      const alpha = base * fadeIn;
      // Delivered: the steel-blue accent. Dropped: drain chroma toward grey as it
      // dies, so loss reads as color leaving rather than an alarm hue.
      const color = beam.dropped
        ? lerpColor(COLOR_BEAM, COLOR_BEAM_LOST, tip / end)
        : COLOR_BEAM;

      if (detail) {
        // Full comet, back to front: glow, then soft wake, then the crisp head.
        g.circle(headPt.x, headPt.y, beam.width * BEAM_GLOW_R).fill({
          color,
          alpha: alpha * BEAM_GLOW_ALPHA,
        });
        g.moveTo(wakePt.x, wakePt.y)
          .lineTo(headPt.x, headPt.y)
          .stroke({ width: beam.width, color, alpha: alpha * 0.4, cap: 'round' });
        g.circle(headPt.x, headPt.y, beam.width * BEAM_HEAD_R).fill({ color, alpha });
      } else {
        // Lean comet (all-stroke): the soft wake, then a short bright head streak
        // for the leading edge — no fills, so overload stays fast.
        const streakPt = lerp(beam.a, beam.b, Math.max(0, tip - BEAM_WAKE / 4));
        g.moveTo(wakePt.x, wakePt.y)
          .lineTo(headPt.x, headPt.y)
          .stroke({ width: beam.width, color, alpha: alpha * 0.45, cap: 'round' });
        g.moveTo(streakPt.x, streakPt.y)
          .lineTo(headPt.x, headPt.y)
          .stroke({ width: beam.width, color, alpha, cap: 'round' });
      }
    }
  }

  /** The live position of node `id`'s disc (mid-glide if it is moving), or
   *  `null` if no such node is on stage. */
  #liveCenter(id: number): Point | null {
    const gfx = this.#nodes.get(id);
    if (gfx === undefined) return null;
    return { x: gfx.root.position.x, y: gfx.root.position.y };
  }

  /** Queue node `dst` for a coalesced delivery pulse on the next frame, instead
   *  of pulsing it now — so several beams landing in one frame brighten the disc
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
   *  to a gentle dim floor (0.7) over a soft duration — `overwrite: 'auto'` so a
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

  /** Cancel a queued flush and drop every pending target — a teardown or beam
   *  sweep must not pulse a disc that is being freed. */
  #cancelFlashes(): void {
    if (this.#flashRaf !== 0) {
      cancelAnimationFrame(this.#flashRaf);
      this.#flashRaf = 0;
    }
    this.#pendingFlash.clear();
  }

  /** The synchronized convergence pulse: one expanding ring from each node,
   *  fired the instant the cluster agrees. Reduced motion skips it — the arcs
   *  already snapped green. */
  #pulse(): void {
    if (this.#reduceMotion) return;
    const baseRadius = nodeRadius(this.#nodes.size);
    for (const node of this.#nodes.values()) {
      const ring = new Graphics();
      node.root.addChild(ring);
      // The deep converged green carries strong contrast on the light stage, so
      // a bold ring (3px, near-opaque at the outset) reads as a deliberate
      // "agrees now" beat rather than an ambient halo as it expands and fades.
      const state = { r: baseRadius, alpha: 0.85 };
      gsap.to(state, {
        r: baseRadius * 3,
        alpha: 0,
        duration: 0.6,
        ease: 'power2.out',
        onUpdate: () => {
          // Same guard as the beam draw: the ring can be destroyed (its node
          // left, or a reset tore the stage down) while this tween still ticks.
          if (ring.destroyed) return;
          ring
            .clear()
            .circle(0, 0, state.r)
            .stroke({ width: 3, color: COLOR_CONVERGED, alpha: state.alpha });
        },
        onComplete: () => {
          if (!ring.destroyed) ring.destroy();
        },
      });
    }
  }

  /** Drop every in-flight beam touching node `id` — called when that node
   *  leaves, so no beam draws to or from a disc that is being destroyed. The
   *  next ticker pass redraws the survivors (or issues the drain clear). */
  #cancelBeamsFor(id: number): void {
    for (const beam of [...this.#beams]) {
      if (beam.src === id || beam.dst === id) this.#beams.delete(beam);
    }
  }

  #clearBeams(): void {
    this.#cancelFlashes();
    this.#beams.clear();
    this.#packetGfx.clear();
    this.#beamsActive = false;
  }
}

function lerp(a: Point, b: Point, t: number): Point {
  return { x: a.x + (b.x - a.x) * t, y: a.y + (b.y - a.y) * t };
}

/** GSAP `power2.inOut` reproduced locally: the beam ticker (not a tween) now
 *  drives flight progress, so it eases the raw age ratio itself. */
function easeInOutQuad(t: number): number {
  return t < 0.5 ? 2 * t * t : 1 - 2 * (1 - t) * (1 - t);
}

/** Clamp `t` to `[0, 1]` — the beam fade ramps run a raw ratio through this. */
function clamp01(t: number): number {
  return t < 0 ? 0 : t > 1 ? 1 : t;
}

/** Blend two packed `0xRRGGBB` colours channel-wise — the dot-mode disc fill
 *  ramps from the slate base toward a signal hue as a node catches up. */
function lerpColor(a: number, b: number, t: number): number {
  const k = Math.max(0, Math.min(1, t));
  const ar = (a >> 16) & 0xff;
  const ag = (a >> 8) & 0xff;
  const ab = a & 0xff;
  const r = Math.round(ar + ((b >> 16 & 0xff) - ar) * k);
  const g = Math.round(ag + ((b >> 8 & 0xff) - ag) * k);
  const bl = Math.round(ab + ((b & 0xff) - ab) * k);
  return (r << 16) | (g << 8) | bl;
}
