// The source file and line a position in the production bundle came from, read
// from the hidden source map `vite build` writes beside it: how the allocation
// audit tells the stage's per-frame code from everything else in a minified
// bundle.

import { readFileSync } from 'node:fs';

/** One generated line's segments: generated column, then source index (-1
 *  where unmapped) and 0-based source line. */
type Line = { column: number; source: number; line: number }[];

const maps = new Map<string, { sources: string[]; lines: Line[] }>();

const BASE64 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';

/** Decode a source map's `mappings`: per generated line, each segment's
 *  generated column, source index and source line (base64 VLQ, each field
 *  relative to the previous segment's, the generated column reset on every
 *  line). */
function decode(mappings: string): Line[] {
  const lines: Line[] = [];
  let source = 0;
  let sourceLine = 0;
  for (const text of mappings.split(';')) {
    const line: Line = [];
    let column = 0;
    for (const segment of text.split(',')) {
      if (segment === '') continue;
      const fields: number[] = [];
      let value = 0;
      let shift = 0;
      for (const char of segment) {
        const digit = BASE64.indexOf(char);
        value += (digit & 31) << shift;
        if ((digit & 32) !== 0) {
          shift += 5;
          continue;
        }
        fields.push(value & 1 ? -(value >>> 1) : value >>> 1);
        value = 0;
        shift = 0;
      }
      column += fields[0];
      // A one-field segment maps nothing: the code from it on has no source.
      if (fields.length >= 4) {
        source += fields[1];
        sourceLine += fields[2];
      }
      line.push({ column, source: fields.length >= 4 ? source : -1, line: sourceLine });
    }
    lines.push(line);
  }
  return lines;
}

/** Where the segment covering 0-based `line` and `column` of the bundle
 *  `mapPath` maps came from (the segment with the greatest generated column at
 *  or before it): its source file and 1-based line, or null where none does. */
export function sourceOf(mapPath: string, line: number, column: number): { file: string; line: number } | null {
  let map = maps.get(mapPath);
  if (map === undefined) {
    const raw = JSON.parse(readFileSync(mapPath, 'utf8')) as { sources: string[]; mappings: string };
    map = { sources: raw.sources, lines: decode(raw.mappings) };
    maps.set(mapPath, map);
  }
  let found: Line[number] | null = null;
  for (const segment of map.lines[line] ?? []) {
    if (segment.column > column) break;
    found = segment;
  }
  if (found === null) return null;
  const file = map.sources[found.source];
  return file === undefined ? null : { file, line: found.line + 1 };
}
