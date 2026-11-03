// The stage's geometry, in CSS pixels of the stage box: one ring per zone, the
// rings set evenly on a larger circle around an empty centre. The renderer, the
// node grid, the pointer hit test and the audit all read it from here, so a
// node's disc, its label, its keyboard cell and its beams share one address.
// Everything is screen-size, so labels never scale with the stage.

/** A ring's radius as a share of the gap between neighbouring ring centres: the
 *  largest value that keeps every cross-zone beam clear of a third ring. */
export const K_Z = 0.75;
/** The largest disc, in px. */
export const R_MAX = 20;
/** A ring of `n` discs of radius `r` holds them `S_D · r` apart along its circle. */
export const S_D = 4;
/** The sampled least distance from a cross-zone beam to a third ring's node
 *  circle, in ring radii, with a margin: 0.386 at the most zones. */
export const BEAM_CLEARANCE = 0.35;
/** The two tier strokes, in px: inside a zone and across zones. */
export const IN_WIDTH = 1.5;
export const CROSS_WIDTH = 3;
/** The gap between a disc and its label, in px: clear of the origin's thick ring. */
export const LABEL_GAP = 12;
/** The smallest pointer target radius: a 24 px target. */
export const HIT_MIN = 12;
/** The smallest disc radius, so a crowded ring still draws. */
const R_FLOOR = 2;

export interface Point {
  x: number;
  y: number;
}

/** One zone's ring: its centre, the radius its nodes sit at and their disc radius. */
export interface Ring {
  x: number;
  y: number;
  rz: number;
  r: number;
}

/** One node's place: its disc's centre and radius, in its zone's ring. */
export interface Spot {
  id: number;
  zone: number;
  x: number;
  y: number;
  r: number;
}

/** Every ring, one per configured zone (an emptied zone keeps its slot), and
 *  every node's spot in (zone, id) order. */
export interface StageLayout {
  centre: Point;
  rings: Ring[];
  spots: Spot[];
}

/** Lay out `nodes` (ascending id) in `zones` rings on a `width × height` stage,
 *  keeping `margin` px between every node centre and the stage edge. A node's
 *  angle in its ring depends only on its rank by id within the zone, so order
 *  never depends on a value. */
export function stageLayout(
  width: number,
  height: number,
  zones: number,
  nodes: readonly { id: number; zone: number }[],
  margin: number,
): StageLayout {
  const centre = { x: width / 2, y: height / 2 };
  const half = Math.max(Math.min(width, height) / 2 - margin, 0);
  const k = K_Z * Math.sin(Math.PI / zones);
  const ringGap = zones === 1 ? 0 : half / (1 + k);
  const rz = zones === 1 ? half : ringGap * k;

  const size = new Array<number>(zones).fill(0);
  for (const node of nodes) size[node.zone] += 1;
  const rings = size.map((n, z) => {
    const angle = -Math.PI / 2 + (2 * Math.PI * z) / zones;
    // The disc is capped by the room on the ring and by the clearance a beam
    // leaves round a third ring (half a cross-zone stroke of it is the stroke's).
    const r = Math.max(
      Math.min(R_MAX, (2 * Math.PI * rz) / (S_D * Math.max(n, 1)), BEAM_CLEARANCE * rz - CROSS_WIDTH / 2),
      R_FLOOR,
    );
    return { x: centre.x + ringGap * Math.cos(angle), y: centre.y + ringGap * Math.sin(angle), rz, r };
  });

  const first = new Array<number>(zones).fill(0);
  for (let z = 1; z < zones; z++) first[z] = first[z - 1] + size[z - 1];
  const rank = new Array<number>(zones).fill(0);
  const spots = new Array<Spot>(nodes.length);
  for (const { id, zone } of nodes) {
    const ring = rings[zone];
    const angle = -Math.PI / 2 + (2 * Math.PI * rank[zone]) / size[zone];
    spots[first[zone] + rank[zone]] = {
      id,
      zone,
      x: ring.x + ring.rz * Math.cos(angle),
      y: ring.y + ring.rz * Math.sin(angle),
      r: ring.r,
    };
    rank[zone] += 1;
  }
  return { centre, rings, spots };
}

/** A beam's route as a quadratic Bézier. */
export interface QuadPath {
  from: Point;
  control: Point;
  to: Point;
}

/** The route of a packet from `from` to `to`, which every beam is drawn along:
 *  inside a zone a straight segment (the control point is the midpoint), across
 *  zones a curve bent through the stage's empty `centre`, so it never runs past
 *  a third ring. Written into `out` when given, so the renderer allocates none. */
export function beamPath(
  from: Spot,
  to: Spot,
  centre: Point,
  out: QuadPath = { from: { x: 0, y: 0 }, control: { x: 0, y: 0 }, to: { x: 0, y: 0 } },
): QuadPath {
  out.from.x = from.x;
  out.from.y = from.y;
  out.to.x = to.x;
  out.to.y = to.y;
  if (from.zone === to.zone) {
    out.control.x = (from.x + to.x) / 2;
    out.control.y = (from.y + to.y) / 2;
  } else {
    out.control.x = centre.x;
    out.control.y = centre.y;
  }
  return out;
}

/** The point at `t` in [0, 1] along `path`, written into `out`. */
export function pointOn(path: QuadPath, t: number, out: Point): Point {
  const u = 1 - t;
  out.x = u * u * path.from.x + 2 * u * t * path.control.x + t * t * path.to.x;
  out.y = u * u * path.from.y + 2 * u * t * path.control.y + t * t * path.to.y;
  return out;
}

/** The id of the node a pointer at `p` picks: the nearest spot within its disc
 *  radius, or within `HIT_MIN` for a disc smaller than a 24 px target. A point
 *  on the bare stage picks none. */
export function nodeAt(p: Point, layout: StageLayout): number | null {
  let best: number | null = null;
  let bestDist = Number.POSITIVE_INFINITY;
  for (const spot of layout.spots) {
    const dist = Math.hypot(p.x - spot.x, p.y - spot.y);
    if (dist <= Math.max(spot.r, HIT_MIN) && dist < bestDist) {
      bestDist = dist;
      best = spot.id;
    }
  }
  return best;
}
