// The stage's coordinate system, shared by the PixiJS renderer (which draws in
// logical space) and the Svelte DOM label overlay (which positions text in
// screen pixels). Both consume these helpers so a node's disc and its label
// always sit at the same address — the "permanent screen address the eye
// learns" the design calls for.

/** The logical stage is a fixed square; the renderer scales it to fit the
 *  container, letterboxed, so node addresses never shift with the viewport. */
export const STAGE_SIZE = 1000;
const CENTER = STAGE_SIZE / 2;
const RING_RADIUS = 370;

/** Above this node count, per-node cell arcs and labels would crowd the ring,
 *  so the stage collapses each node to an intensity dot (the design's
 *  focus+context degradation path). */
export const DOT_THRESHOLD = 40;

export interface Point {
  x: number;
  y: number;
}

/** How the logical stage maps into the container: a uniform scale plus a
 *  centering offset (the letterbox). */
export interface StageTransform {
  scale: number;
  offsetX: number;
  offsetY: number;
}

/** Deterministic ring address for node `i` of `n`: starts at 12 o'clock and
 *  goes clockwise, so a shared URL renders an identical layout. */
export function nodePosition(i: number, n: number): Point {
  const angle = (2 * Math.PI * i) / Math.max(n, 1) - Math.PI / 2;
  return {
    x: CENTER + RING_RADIUS * Math.cos(angle),
    y: CENTER + RING_RADIUS * Math.sin(angle),
  };
}

/** Disc radius (in logical units) for a cluster of `n` nodes — shrinks as the
 *  ring fills so nodes never overlap. */
export function nodeRadius(n: number): number {
  return Math.max(9, Math.min(38, 1100 / Math.max(n, 1)));
}

/** The guide ring the nodes sit on. */
export const guideRadius = RING_RADIUS;
export const stageCenter: Point = { x: CENTER, y: CENTER };

/** Fit the logical stage into a `width × height` container, centered. */
export function fitTransform(width: number, height: number): StageTransform {
  const scale = Math.min(width, height) / STAGE_SIZE;
  return {
    scale,
    offsetX: (width - STAGE_SIZE * scale) / 2,
    offsetY: (height - STAGE_SIZE * scale) / 2,
  };
}

/** Map a logical point into container/screen pixels under a transform. */
export function toScreen(p: Point, t: StageTransform): Point {
  return { x: t.offsetX + p.x * t.scale, y: t.offsetY + p.y * t.scale };
}
