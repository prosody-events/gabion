// One geometry for every mark a node wears, drawn by two pens: the stage's
// Pixi `Graphics` (which satisfies `Pen` structurally) and `SvgPen`, which
// writes the same path into an SVG `d` for the DOM (the key, the table, the
// glossary). Each function writes through its pen. Through the Pixi pen
// nothing allocates, so the renderer may call them while it redraws; SvgPen
// builds its strings once, when a component is built. Coordinates are px with
// y down, centred on the disc; angles are radians, clockwise from 3 o'clock.

/** What a mark is drawn through. */
export interface Pen {
  moveTo(x: number, y: number): Pen;
  lineTo(x: number, y: number): Pen;
  arc(cx: number, cy: number, r: number, a0: number, a1: number): Pen;
  closePath(): Pen;
}

/** A pen that writes an SVG path. */
export class SvgPen implements Pen {
  d = '';
  moveTo(x: number, y: number): this {
    this.d += `M${round(x)} ${round(y)}`;
    return this;
  }
  lineTo(x: number, y: number): this {
    this.d += `L${round(x)} ${round(y)}`;
    return this;
  }
  arc(cx: number, cy: number, r: number, a0: number, a1: number): this {
    // Split at a half turn: one SVG arc command cannot draw a whole circle.
    const sweep = a1 - a0;
    const steps = Math.ceil(Math.abs(sweep) / Math.PI) || 1;
    const start = `${round(cx + r * Math.cos(a0))} ${round(cy + r * Math.sin(a0))}`;
    this.d += this.d === '' || this.d.endsWith('Z') ? `M${start}` : `L${start}`;
    for (let i = 1; i <= steps; i++) {
      const a = a0 + (sweep * i) / steps;
      this.d += `A${round(r)} ${round(r)} 0 0 ${sweep > 0 ? 1 : 0} ${round(cx + r * Math.cos(a))} ${round(cy + r * Math.sin(a))}`;
    }
    return this;
  }
  closePath(): this {
    this.d += 'Z';
    return this;
  }
}

const round = (v: number): number => Math.round(v * 100) / 100;

/** Half the chord of a circle of radius `r` at height `y`. */
const chordHalf = (r: number, y: number): number => Math.sqrt(Math.max(r * r - y * y, 0));

/** A diamond of half-diagonal `half` centred at (`cx`, `cy`), one vertex
 *  pointing along `angle`: a gateway's port, pointing into the zone it
 *  relays to. */
export function port(pen: Pen, cx: number, cy: number, half: number, angle: number): void {
  const ux = Math.cos(angle) * half;
  const uy = Math.sin(angle) * half;
  pen
    .moveTo(cx + ux, cy + uy)
    .lineTo(cx - uy, cy + ux)
    .lineTo(cx - ux, cy - uy)
    .lineTo(cx + uy, cy - ux)
    .closePath();
}

/** The part of a disc of radius `r` between heights `yTop` and `yBottom`
 *  (`yTop` above): a vessel's fill, a stratum, the part over the brim, the
 *  refusal's band. Nothing when the band is empty. */
export function chord(pen: Pen, r: number, yTop: number, yBottom: number): void {
  const top = Math.max(yTop, -r);
  const bottom = Math.min(yBottom, r);
  if (bottom - top <= 0) return;
  const xt = chordHalf(r, top);
  const xb = chordHalf(r, bottom);
  const rightTop = Math.atan2(top, xt);
  const rightBottom = Math.atan2(bottom, xb);
  const leftBottom = Math.atan2(bottom, -xb);
  let leftTop = Math.atan2(top, -xt);
  if (leftTop < leftBottom) leftTop += 2 * Math.PI;
  pen.moveTo(-xt, top).lineTo(xt, top);
  if (rightBottom > rightTop) pen.arc(0, 0, r, rightTop, rightBottom);
  pen.lineTo(-xb, bottom);
  if (leftTop > leftBottom) pen.arc(0, 0, r, leftBottom, leftTop);
  pen.closePath();
}

/** The brim: a line across the disc at height `y`, `overhang` px past it each
 *  side. */
export function brim(pen: Pen, r: number, y: number, overhang: number): void {
  const half = chordHalf(r, y) + overhang;
  pen.moveTo(-half, y).lineTo(half, y);
}

/** Dashes round a circle of radius `r`, `dash[0]` px drawn and `dash[1]` px
 *  left, as separate subpaths: the selection preview. */
export function dashedRing(pen: Pen, r: number, dash: readonly [on: number, off: number]): void {
  const turn = 2 * Math.PI * r;
  const period = dash[0] + dash[1];
  const count = Math.max(1, Math.floor(turn / period));
  const step = (2 * Math.PI) / count;
  const on = (step * dash[0]) / period;
  for (let i = 0; i < count; i++) {
    const a = -Math.PI / 2 + i * step;
    pen.moveTo(r * Math.cos(a), r * Math.sin(a)).arc(0, 0, r, a, a + on);
  }
}

/** A bar `2·half` px long across a route at (`x`, `y`), whose unit tangent is
 *  (`ux`, `uy`): where a lost packet died. */
export function stopBar(pen: Pen, x: number, y: number, ux: number, uy: number, half: number): void {
  pen.moveTo(x + uy * half, y - ux * half).lineTo(x - uy * half, y + ux * half);
}

/** 45° lines `step` px apart across the band between `yTop` and `yBottom`,
 *  wide enough to cover a disc of radius `r`: the refusal under reduced
 *  motion, clipped to the disc by its mask. */
export function hatch(pen: Pen, r: number, yTop: number, yBottom: number, step: number): void {
  const h = yBottom - yTop;
  if (h <= 0) return;
  for (let x = -r - h; x <= r; x += step) pen.moveTo(x, yBottom).lineTo(x + h, yTop);
}
