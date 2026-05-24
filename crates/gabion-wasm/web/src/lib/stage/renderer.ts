// The PixiJS stage renderer: a framework-agnostic class that owns the WebGL
// canvas and turns simulation state into the deep-canvas node stage. Kept out
// of Svelte's reactivity on purpose — Svelte feeds it `setCluster` (steady
// per-node state) and `applyEvents` (transient gossip packets); the class owns
// all imperative Pixi/GSAP bookkeeping and cleanup.
//
// Three layers, back to front: the guide ring, light-beam packets, then the
// node discs with their cell arcs on top (so beams emanate from beneath the
// nodes they connect).

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
// hex strings). Two signal hues only — amber for "in flight / not yet agreed",
// green for "converged" — each always paired with a shape cue (arc length,
// motion, the pulse), never color alone.
const COLOR_STAGE_BG = 0x0e1218;
const COLOR_GRID = 0x1b222c;
const COLOR_NODE_FILL = 0xaeb8c4;
const COLOR_NODE_STROKE = 0x38414d;
const COLOR_DIRTY = 0xd98a2b; // in flight / still climbing
const COLOR_CONVERGED = 0x2a9d6f; // settled / agreed

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

/** One node's persistent display objects. */
interface NodeGfx {
  root: Container;
  disc: Graphics;
  arc: Graphics;
  center: Point;
}

/** One packet in flight: its graphics plus the tween animating it, tracked so
 *  a reset can kill both deterministically. */
interface Beam {
  gfx: Graphics;
  tween: gsap.core.Tween;
}

export class StageRenderer {
  readonly #app: Application;
  readonly #root: Container;
  readonly #guideLayer: Container;
  readonly #packetLayer: Container;
  readonly #nodeLayer: Container;
  readonly #reduceMotion: boolean;

  #nodes: NodeGfx[] = [];
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

  /** Apply steady per-node state: rebuild the ring if the node count changed,
   *  then update each node's cell arc and detect cluster-wide convergence. */
  setCluster(state: ClusterState | null): void {
    if (state === null) return;

    // A tick that runs backward means the session was reset (a fresh sim starts
    // at tick 0). Tear down transient state — killing in-flight beams *before*
    // touching node graphics — so a stale tween can't animate a disc we're
    // about to rebuild.
    if (state.tick < this.#lastTick) {
      this.#clearBeams();
      this.#lastDisagreement = 0;
    }
    this.#lastTick = state.tick;

    const n = state.nodes.length;
    if (n !== this.#nodes.length) this.#rebuildNodes(n);

    const oracle = Math.max(state.oracle_total, 1);
    let max = 0;
    let min = Number.POSITIVE_INFINITY;
    for (const node of state.nodes) {
      const total = node.aggregate_total;
      if (total > max) max = total;
      if (total < min) min = total;
      this.#drawArc(this.#nodes[node.index], total / oracle, n);
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

  /** Tear the renderer down: kill tweens, free every GPU resource, drop the
   *  canvas. Safe to call once; the owner must not reuse the instance after. */
  destroy(): void {
    this.#clearBeams();
    this.#app.canvas.remove();
    this.#app.destroy(true, { children: true, texture: true, textureSource: true });
  }

  #rebuildNodes(n: number): void {
    this.#nodeLayer.removeChildren().forEach((c) => c.destroy({ children: true }));
    this.#nodes = [];
    const radius = nodeRadius(n);
    for (let i = 0; i < n; i++) {
      const center = nodePosition(i, n);
      const root = new Container();
      root.position.set(center.x, center.y);
      const disc = new Graphics()
        .circle(0, 0, radius)
        .fill(COLOR_NODE_FILL)
        .stroke({ width: 2, color: COLOR_NODE_STROKE });
      const arc = new Graphics();
      root.addChild(disc, arc);
      this.#nodeLayer.addChild(root);
      this.#nodes[i] = { root, disc, arc, center };
    }
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
      // Intensity dot: tint the disc from neutral toward the signal hue by how
      // caught up the node is. Position on the ring still encodes identity.
      node.disc.tint = f === 0 ? 0xffffff : color;
      node.disc.alpha = 0.45 + 0.55 * f;
      return;
    }

    node.disc.tint = 0xffffff;
    node.disc.alpha = 1;
    if (f <= 0) return;
    const radius = nodeRadius(n) + 4;
    const start = -Math.PI / 2;
    node.arc
      .arc(0, 0, radius, start, start + f * 2 * Math.PI)
      .stroke({ width: 3, color, cap: 'round' });
  }

  #beam(src: number, dst: number, bytes: number, dropped: boolean): void {
    if (this.#beams.size >= MAX_BEAMS) return;
    const a = this.#nodes[src]?.center;
    const b = this.#nodes[dst]?.center;
    if (a === undefined || b === undefined) return;

    const width = 1.5 + Math.min(bytes, 400) / 100;
    const gfx = new Graphics();
    this.#packetLayer.addChild(gfx);

    if (this.#reduceMotion) {
      // Reduced motion: no flight. Mark the edge briefly, then fade — a static
      // diff rather than animated travel.
      gfx
        .moveTo(a.x, a.y)
        .lineTo(b.x, b.y)
        .stroke({ width, color: COLOR_DIRTY, alpha: dropped ? 0.25 : 0.6, cap: 'round' });
      const beam: Beam = {
        gfx,
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
    const draw = () => {
      const tip = head.t;
      const tail = Math.max(0, tip - BEAM_TRAIL);
      const from = lerp(a, b, tail);
      const to = lerp(a, b, tip);
      // A dropped beam dims as it dies; a delivered one stays bright to the
      // target.
      const alpha = dropped ? 0.7 * (1 - tip / end) : 0.85;
      gfx
        .clear()
        .moveTo(from.x, from.y)
        .lineTo(to.x, to.y)
        .stroke({ width, color: COLOR_DIRTY, alpha, cap: 'round' });
    };
    const beam: Beam = {
      gfx,
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

  /** A delivered beam briefly brightens its target — the visible "news landed
   *  here" beat that distinguishes delivery from a drop. */
  #flash(dst: number): void {
    const disc = this.#nodes[dst]?.disc;
    if (disc === undefined) return;
    gsap.fromTo(disc, { alpha: 1 }, { alpha: 0.4, duration: 0.12, yoyo: true, repeat: 1 });
  }

  /** The synchronized convergence pulse: one expanding ring from each node,
   *  fired the instant the cluster agrees. Reduced motion skips it — the arcs
   *  already snapped green. */
  #pulse(): void {
    if (this.#reduceMotion) return;
    for (const node of this.#nodes) {
      const ring = new Graphics();
      node.root.addChild(ring);
      const state = { r: nodeRadius(this.#nodes.length), alpha: 0.7 };
      gsap.to(state, {
        r: nodeRadius(this.#nodes.length) * 3,
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
