// The one place a number or a name becomes text, so every printed number reads
// the same way everywhere (design rule 9) and no locale changes a figure.

/** A share in [0, 1] as a percentage with one decimal: `0.1818` is `18.2%`. */
export function fmtPercent(share: number): string {
  return `${(share * 100).toFixed(1)}%`;
}

/** A span of virtual time: whole milliseconds below one second, else seconds
 *  with one decimal. */
export function fmtDuration(ms: number): string {
  return ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(1)} s`;
}

/** A count with thousands separators, in a fixed locale. */
export function fmtCount(n: number): string {
  return n.toLocaleString('en-US');
}

/** `word` in the singular at one and the plural otherwise: the unit that
 *  follows a count ("1 node", "12 nodes"). */
export function noun(n: number, word: string): string {
  return n === 1 ? word : `${word}s`;
}

/** An engine zone name as it reads in a sentence: `zone-b` is `zone b`. */
export function zoneLabel(name: string): string {
  return name.replace('-', ' ');
}
