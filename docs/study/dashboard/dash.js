// The page's runtime: exactly the calls index.html makes
// (docs/study/README.md, "How the page runs").
(() => {
  const FILES = { posterior: 'posterior', self_posterior: 'self_posterior', seeds: 'seed_metrics', runs: 'runs', arms: 'arms', regimes: 'regimes', metrics: 'metrics', scale: 'scale' };
  const SOURCE = 'https://github.com/prosody-events/gabion/blob/main/docs/study/dashboard/index.html';
  const MISSING = { status: 'missing', data: [] };
  const sets = {}, calcs = new Map(), memo = new Map(), subs = [];
  const errors = (window.__ERRORS__ = []);
  let state = {};
  const run = () => subs.forEach((fn) => fn());
  const get = (url) => fetch(url).then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status} for ${url}`))));
  const data = (id) => {
    if (id in FILES) return sets[id] ?? MISSING;
    const calc = calcs.get(id);
    if (!calc) return MISSING;
    if (!memo.has(id)) {
      const inputs = calc.inputs.map(data), held = inputs.find((d) => d.status !== 'ok');
      if (held) return held;
      try {
        memo.set(id, { status: 'ok', data: calc.fn(...inputs.map((d) => d.data)) });
      } catch (e) {
        errors.push(`calc ${id}: ${e.message}`);
        console.error(e);
        memo.set(id, { status: 'ok', data: [] });
      }
    }
    return memo.get(id);
  };
  window.dash = {
    data,
    calc: (id, spec) => void calcs.set(id, spec),
    onData: (fn) => { subs.push(fn); fn(); },
    params: () => state,
    setParams: (p) => {
      state = { ...state, ...p };
      history.replaceState(null, '', '#' + new URLSearchParams(Object.entries(state).filter(([, v]) => v !== '')));
      run();
    },
  };
  get('params.json').catch((e) => (console.error(e), {})).then((defaults) => {
    state = { ...defaults, ...Object.fromEntries(new URLSearchParams(location.hash.slice(1))) };
    for (const [id, file] of Object.entries(FILES)) {
      get(`../data/${file}.json`).then((rows) => ({ status: 'ok', data: rows }), (e) => (console.error(e), { status: 'error', data: [] }))
        .then((set) => { sets[id] = set; run(); });
    }
  });
  // Each calculation's title and description, under the method section, with
  // one link to the code.
  document.addEventListener('DOMContentLoaded', () => {
    const make = (tag, props, ...kids) => { const n = Object.assign(document.createElement(tag), props); n.append(...kids); return n; };
    const note = (...kids) => make('p', { className: 'note' }, ...kids);
    document.getElementById('g3-method').append(make('details', { id: 'g3-calcs', className: 'method-body' },
      make('summary', {}, 'Calculations'),
      ...[...calcs.values()].map((c) => note(make('b', {}, `${c.title}.`), ` ${c.description}`)),
      note('Each is a pure function of its inputs; the code is in ', make('a', { href: SOURCE }, 'the page source'), '.')));
  });
})();
