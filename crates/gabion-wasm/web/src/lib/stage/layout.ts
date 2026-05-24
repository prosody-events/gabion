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

/** Deterministic ring address for the node at `rank` (its position in the live,
 *  insertion-ordered node list) out of `count` live nodes: starts at 12 o'clock
 *  and goes clockwise. Position is a function of *rank*, not identity — so when
 *  a node leaves, the survivors' ranks shift and the ring re-spaces, while each
 *  node keeps its stable id. A shared URL still renders an identical layout
 *  because the command order fixes the ranks. */
export function nodePosition(rank: number, count: number): Point {
  const angle = (2 * Math.PI * rank) / Math.max(count, 1) - Math.PI / 2;
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

/** Inverse of {@link toScreen}: container/screen pixels back to logical stage
 *  coordinates. Used to hit-test a pointer against the ring. */
export function toLogical(p: Point, t: StageTransform): Point {
  return { x: (p.x - t.offsetX) / t.scale, y: (p.y - t.offsetY) / t.scale };
}

/** The **stable id** of the node a screen-space pointer is over, or `null` if
 *  it is over none. Takes the live, insertion-ordered node list so each
 *  candidate's ring position comes from its rank (its index in that list);
 *  the match is the nearest disc by Euclidean distance, accepted only *inside*
 *  the node's radius — discs never overlap, so a hit is unambiguous and a click
 *  on the bare stage misses cleanly (the design wants precise, intentional
 *  clicks, not a generous catch-all). */
export function nodeAt(
  screen: Point,
  nodes: readonly { id: number }[],
  t: StageTransform,
): number | null {
  const count = nodes.length;
  if (count === 0 || t.scale <= 0) return null;
  const p = toLogical(screen, t);
  const radius = nodeRadius(count);
  let best: number | null = null;
  let bestDist = radius;
  for (let rank = 0; rank < count; rank++) {
    const c = nodePosition(rank, count);
    const dist = Math.hypot(p.x - c.x, p.y - c.y);
    if (dist <= bestDist) {
      bestDist = dist;
      best = nodes[rank].id;
    }
  }
  return best;
}
