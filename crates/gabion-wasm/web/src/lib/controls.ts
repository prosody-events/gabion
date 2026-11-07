// The labels of the transport's controls, at each width, and of the next
// action's control, in one place: the controls render them, and the caption's
// action line and the narration quote them verbatim.

import { blurb, noun, type Blurb } from './format';
import type { NextAction, PresetId } from './next';

/** The page's width class: three columns from 1100 px, two from 640 px, one
 *  below. */
export type Width = 'wide' | 'mid' | 'narrow';

/** Send's visible label: what a Send is, its target named where the width
 *  allows. With no valid Requests per Send it claims no number. */
export function sendLabel(width: Width, requests: number | null, node: number): Blurb {
  if (requests === null) return ['Send'];
  const what = blurb`Send ${requests} ${noun(requests, 'request')}`;
  return width === 'narrow' ? what : [...what, ...blurb` to node ${node}`];
}

/** The label of the control next action `action` names, exactly as that
 *  control renders it, given Send's label `send`; null when the action names
 *  no control (typing, waiting, watching). The caption's line and the
 *  narration quote this one source. */
export function actionLabel(action: NextAction, send: Blurb, presetLabel: (id: PresetId) => string): Blurb | null {
  switch (action.kind) {
    case 'press':
      return action.control === 'send' ? send : action.control === 'play' ? ['Play'] : ['Step'];
    case 'inline':
      return action.action.kind === 'select'
        ? blurb`Select node ${action.action.node}`
        : blurb`Use ${action.action.requests} ${noun(action.action.requests, 'request')}`;
    case 'repair':
      return action.label;
    case 'go':
      return [`Go to ${presetLabel(action.to)}`];
    case 'edit':
    case 'wait':
    case 'watch':
      return null;
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
