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
// figures; two signal hues only — amber for "in flight / not yet agreed",
// green for "converged" — each always paired with a shape cue (arc length,
// motion, the pulse), never color alone. Every mark clears 3:1 on the stage.
const COLOR_STAGE_BG = 0xe8edf2;
const COLOR_GRID = 0xd2d9e1;
const COLOR_NODE_FILL = 0x39424f;
const COLOR_NODE_STROKE = 0x2b333d;
const COLOR_DIRTY = 0xb3720d; // in flight / still climbing
const COLOR_CONVERGED = 0x137a52; // settled / agreed

// A node counts as caught up when its view is within this fraction of the
// cluster's true total — the threshold that flips its arc from amber to green.
const CONVERGED_EPSILON = 0.001;

// Wall-clock flight time of a gossip packet beam, and how long its bright head
// trails behind it (as a fraction of the edge). Eased so the beam accelerates
// off the source and decelerates into the target — motion that reads as cause
// then effect.
const BEAM_FLIGHT_MS = 520;
const BEAM_TRAIL = 0.28;
// A dropped packet dies partway across instead of landing.
const DROP_FRACTION = 0.5;
// Cap concurrent beams so a burst at high node counts can't unbound the draw
// list; excess sends are simply not drawn (the cell arcs still tell the story).
const MAX_BEAMS = 320;

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
  /** The ring slot this node is gliding to (logical coords). */
  center: Point;
  /** Radius the disc geometry was last drawn at; redrawn only when a join or
   *  leave re-spaces the ring and changes it. */
  radius: number;
  /** Live tweens (enter / exit / glide), killed on destroy so a stale tween
   *  can never touch a freed display object. */
  tweens: Set<gsap.core.Tween>;
}

/** One packet in flight: its graphics plus the tween animating it, tracked so
 *  a reset — or the departure of an endpoint node — can kill both
 *  deterministically. `src` / `dst` are stable ids. */
interface Beam {
  gfx: Graphics;
  tween: gsap.core.Tween;
  src: number;
  dst: number;
}

export class StageRenderer {
  readonly #app: Application;
  readonly #root: Container;
  readonly #guideLayer: Container;
  readonly #packetLayer: Container;
  readonly #nodeLayer: Container;
  readonly #reduceMotion: boolean;

  #nodes = new Map<number, NodeGfx>();
  #beams = new Set<Beam>();
  #lastTick = -1;
  #lastDisagreement = 0;

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
      if (gfx !== undefined) this.#drawArc(gfx, total / oracle, n);
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
    this.#app.canvas.remove();
    this.#app.destroy(true, { children: true, texture: true, textureSource: true });
  }

  /** Create one node's display objects at `center` and animate its join. */
  #createNode(id: number, center: Point, radius: number): NodeGfx {
    const root = new Container();
    root.position.set(center.x, center.y);
    const disc = new Graphics();
    const arc = new Graphics();
    root.addChild(disc, arc);
    this.#drawDisc(disc, radius);
    this.#nodeLayer.addChild(root);
    const gfx: NodeGfx = { root, disc, arc, center, radius, tweens: new Set() };
    this.#nodes.set(id, gfx);

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

  /** Draw a node's disc at `radius` (clearing any prior geometry). */
  #drawDisc(disc: Graphics, radius: number): void {
    disc
      .clear()
      .circle(0, 0, radius)
      .fill(COLOR_NODE_FILL)
      .stroke({ width: 2, color: COLOR_NODE_STROKE });
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
      node.disc
        .clear()
        .circle(0, 0, node.radius)
        .fill(fill)
        .stroke({ width: 1.5, color: COLOR_NODE_STROKE });
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

  #beam(src: number, dst: number, bytes: number, dropped: boolean): void {
    if (this.#beams.size >= MAX_BEAMS) return;
    const a = this.#liveCenter(src);
    const b = this.#liveCenter(dst);
    if (a === null || b === null) return;

    const width = 1.5 + Math.min(bytes, 400) / 100;
    const gfx = new Graphics();
    this.#packetLayer.addChild(gfx);

    if (this.#reduceMotion) {
      // Reduced motion: no flight. Mark the edge briefly, then fade — a static
      // diff rather than animated travel.
      gfx
        .moveTo(a.x, a.y)
        .lineTo(b.x, b.y)
        .stroke({ width, color: COLOR_DIRTY, alpha: dropped ? 0.3 : 0.9, cap: 'round' });
      const beam: Beam = {
        gfx,
        src,
        dst,
        tween: gsap.to(gfx, {
          alpha: 0,
          duration: 0.4,
          onComplete: () => this.#removeBeam(beam),
        }),
      };
      this.#beams.add(beam);
      return;
    }

    const head = { t: 0 };
    const end = dropped ? DROP_FRACTION : 1;
    const draw = (): void => {
      const tip = head.t;
      const tail = Math.max(0, tip - BEAM_TRAIL);
      const from = lerp(a, b, tail);
      const to = lerp(a, b, tip);
      // A dropped beam dims as it dies; a delivered one stays bright to the
      // target (near-opaque so the amber clears 3:1 on the light stage).
      const alpha = dropped ? 0.7 * (1 - tip / end) : 0.95;
      gfx
        .clear()
        .moveTo(from.x, from.y)
        .lineTo(to.x, to.y)
        .stroke({ width, color: COLOR_DIRTY, alpha, cap: 'round' });
    };
    const beam: Beam = {
      gfx,
      src,
      dst,
      tween: gsap.to(head, {
        t: end,
        duration: BEAM_FLIGHT_MS / 1000,
        ease: 'power2.inOut',
        onUpdate: draw,
        onComplete: () => {
          if (!dropped) this.#flash(dst);
          this.#removeBeam(beam);
        },
      }),
    };
    this.#beams.add(beam);
  }

  /** The live position of node `id`'s disc (mid-glide if it is moving), or
   *  `null` if no such node is on stage. */
  #liveCenter(id: number): Point | null {
    const gfx = this.#nodes.get(id);
    if (gfx === undefined) return null;
    return { x: gfx.root.position.x, y: gfx.root.position.y };
  }

  /** A delivered beam briefly brightens its target — the visible "news landed
   *  here" beat that distinguishes delivery from a drop. */
  #flash(dst: number): void {
    const disc = this.#nodes.get(dst)?.disc;
    if (disc === undefined) return;
    gsap.fromTo(disc, { alpha: 1 }, { alpha: 0.4, duration: 0.12, yoyo: true, repeat: 1 });
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
      const state = { r: baseRadius, alpha: 0.7 };
      gsap.to(state, {
        r: baseRadius * 3,
        alpha: 0,
        duration: 0.6,
        ease: 'power2.out',
        onUpdate: () => {
          ring
            .clear()
            .circle(0, 0, state.r)
            .stroke({ width: 2, color: COLOR_CONVERGED, alpha: state.alpha });
        },
        onComplete: () => ring.destroy(),
      });
    }
  }

  #removeBeam(beam: Beam): void {
    if (!this.#beams.delete(beam)) return;
    beam.gfx.destroy();
  }

  /** Cancel every in-flight beam touching node `id` — called when that node
   *  leaves, so no beam animates to or from a disc that is being destroyed. */
  #cancelBeamsFor(id: number): void {
    for (const beam of [...this.#beams]) {
      if (beam.src === id || beam.dst === id) {
        beam.tween.kill();
        this.#removeBeam(beam);
      }
    }
  }

  #clearBeams(): void {
    for (const beam of this.#beams) {
      beam.tween.kill();
      beam.gfx.destroy();
    }
    this.#beams.clear();
    this.#packetLayer.removeChildren().forEach((c) => c.destroy());
  }
}

function lerp(a: Point, b: Point, t: number): Point {
  return { x: a.x + (b.x - a.x) * t, y: a.y + (b.y - a.y) * t };
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
