// The stage's geometry, in CSS pixels of the stage box: one ring per zone, the
// rings set evenly on a larger circle around an empty centre. The renderer, the
// node grid, the pointer hit test and the audit all read it from here, so a
// node's disc, its name, its port, its keyboard cell and its beams share one
// address.
// Everything is screen-size, so labels never scale with the stage. It also
// holds the beam encoding the pool packs and the audit reads.

/** A ring's radius as a share of half the gap between neighbouring ring
 *  centres: the largest value that keeps every cross-zone beam clear of a third
 *  ring. */
export const K_Z = 0.75;
/** The largest disc, in px. */
export const R_MAX = 24;
/** A ring of `n` discs of radius `r` holds them `S_D · r` apart along its circle. */
export const S_D = 4;
/** The sampled least distance from a cross-zone beam to a third ring's node
 *  circle, in ring radii, with a margin: 0.386 at the most zones. */
export const BEAM_CLEARANCE = 0.35;
/** The two tier strokes, in px: inside a zone and across zones. */
export const IN_WIDTH = 1.5;
export const CROSS_WIDTH = 3;
/** A beam's flight on the stage's presentation clock, ms: wall time at 1×,
 *  eased so it reads as cause then effect. */
export const BEAM_FLIGHT_MS = 520;
/** The beam pool's flag bits: across zones, a control packet (no cells), a
 *  relay, lost, landed. */
export const CROSS = 1;
export const CONTROL = 2;
export const RELAY = 4;
export const DROPPED = 8;
export const LANDED = 16;
/** A node's ring (the share of the cluster's hits it has heard): its gap
 *  outside the disc and its stroke width, px, and how far it reaches. */
export const ARC_GAP = 4;
export const ARC_WIDTH = 3;
export const RING_REACH = ARC_GAP + ARC_WIDTH / 2;
/** The selection outline: its gap outside the disc, its stroke and its reach,
 *  px. The preview of a selection is the same circle, dashed. */
export const SELECTION_GAP = 10;
export const SELECTION_WIDTH = 1.5;
export const SELECTION_REACH = SELECTION_GAP + SELECTION_WIDTH / 2;
export const PREVIEW_DASH: readonly [number, number] = [4, 3];
/** A gateway's port: its least half-diagonal and its gap outside the ring, px. */
const PORT_MIN = 4;
export const PORT_GAP = 1;
/** A name's near edge outside its node's ring, and the least room between two
 *  names side by side, px. */
export const LABEL_GAP = 7;
export const LABEL_SPACING = 12;
/** The share of the disc's radius a centred count may reach. */
export const COUNT_FIT = 0.95;
/** The vessel: strata show from this inside height up, while each is at least
 *  `STRATUM_MIN` px; the brim's overhang past the disc and its strokes, px. */
export const STRATA_MIN_INNER = 32;
export const STRATUM_MIN = 3;
export const STRATUM_GAP = 1.5;
export const BRIM_OVERHANG = 2;
export const BRIM_WIDTH = 1.5;
export const CAP_WIDTH = 3;
/** The least clear space between the outer marks of two nodes in
 *  neighbouring rings, px. */
const NEIGHBOUR_GAP = 4;
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
 *  never depends on a value. Every disc is sized after the spots are placed, so
 *  two nodes of neighbouring rings that face each other never touch. */
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
  const rings = size.map((_, z) => {
    const angle = -Math.PI / 2 + (2 * Math.PI * z) / zones;
    return { x: centre.x + ringGap * Math.cos(angle), y: centre.y + ringGap * Math.sin(angle), rz, r: 0 };
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
      r: 0,
    };
    rank[zone] += 1;
  }

  // The nearest two nodes of different rings, centre to centre: one may wear
  // the selection outline and the other its ring, with the gap between.
  let nearest = Number.POSITIVE_INFINITY;
  for (let i = 0; i < spots.length; i++) {
    for (let j = i + 1; j < spots.length; j++) {
      if (spots[i].zone !== spots[j].zone) {
        nearest = Math.min(nearest, Math.hypot(spots[i].x - spots[j].x, spots[i].y - spots[j].y));
      }
    }
  }
  const facing = (nearest - NEIGHBOUR_GAP - RING_REACH - SELECTION_REACH) / 2;
  // Every ring takes the least disc of any ring, so levels compare across
  // zones and a lopsided cluster draws one size. The disc is capped by the
  // room on the ring, by the clearance a beam leaves round a third ring less
  // half a cross-zone stroke, and by the facing node of a neighbouring ring.
  let r = Math.min(R_MAX, BEAM_CLEARANCE * rz - CROSS_WIDTH / 2, facing);
  for (let z = 0; z < zones; z++) r = Math.min(r, (2 * Math.PI * rz) / (S_D * Math.max(size[z], 1)));
  r = Math.max(r, R_FLOOR);
  for (const ring of rings) ring.r = r;
  for (const spot of spots) spot.r = r;
  return { centre, rings, spots };
}

/** A gateway port's half-diagonal on a disc of radius `r`. */
export const portHalf = (r: number): number => Math.max(PORT_MIN, r / 3);

/** Where a gateway's port sits, from the gateway's centre: outside its ring,
 *  toward its own zone ring's centre (the zone it relays into), with the
 *  angle it points along. Written into `out`. */
export function portAt(spot: Spot, ring: Point, out: { x: number; y: number; angle: number }): void {
  const angle = Math.atan2(ring.y - spot.y, ring.x - spot.x);
  const d = spot.r + RING_REACH + portHalf(spot.r) + PORT_GAP;
  out.x = d * Math.cos(angle);
  out.y = d * Math.sin(angle);
  out.angle = angle;
}

/** How far a node's centre must stay from the stage's edge: a disc of radius
 *  `r`, its ring, the gap to its name and the name's line. */
export const stageMargin = (labelHeight: number, r = R_MAX): number => r + RING_REACH + LABEL_GAP + labelHeight;

/** Whether a count `width` px wide and `capHeight` px tall fits centred in a
 *  disc of radius `r`. */
export const countFits = (width: number, r: number, capHeight: number): boolean =>
  width <= 2 * Math.sqrt(Math.max((COUNT_FIT * r) ** 2 - (capHeight / 2) ** 2, 0));

/** A beam's route as a quadratic Bézier. */
export interface QuadPath {
  from: Point;
  control: Point;
  to: Point;
}

/** The route of a packet from `from` to `to`, which every beam is drawn along:
 *  inside a zone a straight segment (the control point is the midpoint), across
 *  zones a curve bent through the stage's empty `centre`, so it never runs past
 *  a third ring. Written into `out`, so the renderer allocates none. */
export function beamPath(from: Spot, to: Spot, centre: Point, out: QuadPath): QuadPath {
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
