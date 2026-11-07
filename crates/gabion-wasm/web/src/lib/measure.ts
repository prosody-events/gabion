// Text metrics for DOM text laid over the canvas (the counts, the decision
// text) and for the charts' labels, measured on one shared 2D context, in the
// page's own faces.

let context: OffscreenCanvasRenderingContext2D | null = null;

function ctx(font: string): OffscreenCanvasRenderingContext2D | null {
  context ??= new OffscreenCanvas(1, 1).getContext('2d');
  if (context !== null) context.font = font;
  return context;
}

/** `text`'s advance width in `font`, px. */
export const textWidth = (text: string, font: string): number => ctx(font)?.measureText(text).width ?? 0;

/** The height of a figure in `font`, px: what a centred count must fit. */
export const capHeight = (font: string): number => ctx(font)?.measureText('0').actualBoundingBoxAscent ?? 0;
