// The one place a number or a name becomes text, so every printed number reads
// the same way everywhere (design rule 9) and no locale changes a figure.

/** A share in [0, 1] as a percentage with one decimal: `0.1818` is `18.2%`. */
export function fmtPercent(share: number): string {
  return `${(share * 100).toFixed(1)}%`;
}

/** A span of virtual time: whole milliseconds below one second, else seconds
 *  with one decimal. */
export function fmtDuration(ms: number): string {
  const whole = Math.round(ms);
  return whole < 1000 ? `${whole} ms` : `${(ms / 1000).toFixed(1)} s`;
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

/** What a printed number claims: the noun it counts. The stage's voices (the
 *  heading, the narration, the decision text) tag their numbers with it and
 *  with whose number it is, so the audit can check no claim is made twice. */
export type Noun = 'view' | 'limit' | 'admitted' | 'rejected' | 'behind' | 'reopens';

/** A number that says whose it is and what it counts: `subject` is a node id
 *  or `'cluster'`. `value` is printed as is, so a duration arrives formatted. */
export interface Claim {
  readonly value: number | string;
  readonly subject: number | 'cluster';
  readonly noun: Noun;
}

/** A claim of `noun` about `subject`. */
export const claim = (value: number | string, subject: number | 'cluster', noun: Noun): Claim => ({
  value,
  subject,
  noun,
});

/** One part of a printed sentence: text, a number, or a claim. Every number
 *  and claim is put in its own data-bound element. */
export type Part = string | number | Claim;

/** A sentence as the page prints it. */
export type Blurb = readonly Part[];

/** Build a `Blurb` from a template: a number or claim interpolation becomes a
 *  data-bound part and a string one plain text, so no number can lose its
 *  place. */
export const blurb = (parts: TemplateStringsArray, ...values: Part[]): Blurb =>
  parts.flatMap((text, i) => (i < values.length ? [text, values[i]] : [text]));

/** A list of names joined in prose: "a", "a and b", "a, b and c". */
export function joined(items: readonly Blurb[]): Blurb {
  return items.flatMap((item, i) => [...(i === 0 ? [] : i === items.length - 1 ? [' and '] : [', ']), ...item]);
}

/** A countdown in seconds, floored to a tenth: "9.9 s". */
export function fmtSeconds(ms: number): string {
  return `${(Math.max(Math.floor(ms / 100), 0) / 10).toFixed(1)} s`;
}

/** An engine zone name as it reads in a sentence: `zone-b` is `zone b`. */
export function zoneLabel(name: string): string {
  return name.replace('-', ' ');
}
