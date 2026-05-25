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
const COLOR_DIRTY = 0xb3720d; // in flight / still climbing
const COLOR_CONVERGED = 0x137a52; // settled / agreed
// The selection ring (the inspected node). Drawn in the dark ink — chrome, not
// a signal — so it never reads as amber/green convergence state, and it is
// paired with the inspector opening, so selection is signified by more than the
// ring alone.
const COLOR_SELECT = 0x1b2330;

// A node counts as caught up when its view is within this fraction of the
// cluster's true total — the threshold that flips its arc from amber to green.
const CONVERGED_EPSILON = 0.001;

// Wall-clock flight time of a gossip packet beam, and how long its bright head
// trails behind it (as a fraction of the edge). Eased so the beam accelerates
// off the source and decelerates into the target — motion that reads as cause
// then effect.
const BEAM_FLIGHT_MS = 520;
const BEAM_TRAIL = 0.28;
// Soft beam edges: a beam ramps in over the first slice of its flight and (when
// delivered) back out over the last slice as it reaches the target, so it
// neither snaps on at the source nor vanishes abruptly on arrival — the
// target's pulse takes over the "news landed" beat. Fractions of the flight.
const BEAM_FADE_IN = 0.1;
const BEAM_FADE_OUT = 0.12;
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
      // A tween tick can land after the beam's graphics were destroyed (a sweep
      // on reset / node-exit, or the completion tick racing the next frame under
      // fast play). Drawing into a destroyed `Graphics` reads its null context
      // and throws — bail before touching it.
      if (gfx.destroyed) return;
      const tip = head.t;
      const tail = Math.max(0, tip - BEAM_TRAIL);
      const from = lerp(a, b, tail);
      const to = lerp(a, b, tip);
      // Soft edges multiply the base alpha: ramp in over the first slice of the
      // flight, and — delivered only — back out over the last slice into the
      // target. A dropped beam keeps its own mid-flight dim (and still fades in)
      // so it reads as fading *out* mid-flight, distinct from a delivery.
      const fadeIn = clamp01(tip / (end * BEAM_FADE_IN));
      const base = dropped
        ? 0.7 * (1 - tip / end)
        : 0.95 * clamp01((end - tip) / (end * BEAM_FADE_OUT));
      const alpha = base * fadeIn;
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
          if (!dropped) this.#queueFlash(dst);
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
    this.#cancelFlashes();
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
