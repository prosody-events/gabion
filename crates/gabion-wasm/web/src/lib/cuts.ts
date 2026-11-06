// The faults the page made, by what each means rather than by the links it
// blocked, as the header, the stage labels and the heading's repair name
// them.

import { blurb, zoneLabel, type Blurb } from './format';

/** One fault: a zone cut off from the rest, a node silenced (cut from every
 *  other node), or a node its own zone cannot hear (it still hears them). */
export type Cut =
  | { readonly kind: 'zone'; readonly zone: number }
  | { readonly kind: 'silenced'; readonly node: number }
  | { readonly kind: 'unheard-in-zone'; readonly node: number; readonly zone: number };

/** The cut, as the header lists it. `zones` are the engine's zone names. */
export function cutName(cut: Cut, zones: readonly { name: string }[]): Blurb {
  switch (cut.kind) {
    case 'zone':
      return blurb`${zoneLabel(zones[cut.zone]?.name ?? '')} cut off`;
    case 'silenced':
      return blurb`node ${cut.node} silenced`;
    case 'unheard-in-zone':
      return blurb`node ${cut.node} unheard by ${zoneLabel(zones[cut.zone]?.name ?? '')}`;
  }
}

/** What the cut leaves broken, as the heading and the narration say it. */
export function cutState(cut: Cut, zones: readonly { name: string }[]): Blurb {
  switch (cut.kind) {
    case 'zone':
      return blurb`${zoneLabel(zones[cut.zone]?.name ?? '')} is cut off`;
    case 'silenced':
      return blurb`node ${cut.node} is silenced`;
    case 'unheard-in-zone':
      return blurb`${zoneLabel(zones[cut.zone]?.name ?? '')} cannot hear node ${cut.node}`;
  }
}

/** The label of the one repair that undoes every listed cut: the network the
 *  scenario was built with, its loss included. */
export function repairLabel(cuts: readonly Cut[], zones: readonly { name: string }[]): Blurb {
  if (cuts.length !== 1) return blurb`Heal ${cuts.length} cuts`;
  const cut = cuts[0];
  return cut.kind === 'zone' ? [`Reconnect ${zoneLabel(zones[cut.zone]?.name ?? '')}`] : blurb`Reconnect node ${cut.node}`;
}
