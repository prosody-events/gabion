// The labels of the transport's controls, at each width, in one place: the
// button renders them, and the caption's action line quotes them verbatim.

import { blurb, type Blurb } from './format';

/** The page's width class: three columns from 1100 px, two from 640 px, one
 *  below. */
export type Width = 'wide' | 'mid' | 'narrow';

/** Send's visible label: what a Send is, as long as the width allows. With no
 *  valid Hits per Send it claims no number. */
export function sendLabel(width: Width, hits: number | null, node: number): Blurb {
  if (hits === null) return ['Send'];
  switch (width) {
    case 'wide':
      return blurb`Send a request of ${hits} hits to node ${node}`;
    case 'mid':
      return blurb`Send ${hits} hits to node ${node}`;
    case 'narrow':
      return blurb`Send ${hits} hits`;
  }
}

/** The width class `matchMedia` reports now. */
export function widthNow(): Width {
  if (window.matchMedia('(width >= 1100px)').matches) return 'wide';
  return window.matchMedia('(width >= 640px)').matches ? 'mid' : 'narrow';
}

/** Call `onChange` whenever the width class changes; returns the unsubscribe. */
export function watchWidth(onChange: (width: Width) => void): () => void {
  const queries = ['(width >= 1100px)', '(width >= 640px)'].map((q) => window.matchMedia(q));
  const fire = (): void => onChange(widthNow());
  for (const q of queries) q.addEventListener('change', fire);
  return () => {
    for (const q of queries) q.removeEventListener('change', fire);
  };
}
