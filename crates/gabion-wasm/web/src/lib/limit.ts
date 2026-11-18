// Where the limit sits on a node's vessel. The vessel's inside holds
// `VESSEL_SCALE` limits, so the brim, the limit, sits at 0.8 of its height and
// a view that overshoots still shows above it. A limit more than
// `LIMIT_ON_SCALE` requests away is off the scale: no brim, cap or fill is
// drawn, and the count and the ring carry the story. The renderer, the key,
// the heading and the lede all read the one predicate here.

/** How many requests of Hits per Send the limit may hold and still be drawn. */
export const LIMIT_ON_SCALE = 20;

/** The vessel's height, in limits. */
export const VESSEL_SCALE = 1.25;

/** Whether a limit of `limit` is drawn on the vessels, given the Hits per
 *  Send each request carries. */
export const onScale = (limit: number, hits: number): boolean => limit <= LIMIT_ON_SCALE * hits;
