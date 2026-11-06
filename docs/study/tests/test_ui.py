#!/usr/bin/env python3
"""Automated UI audit of docs/study/dashboard/index.html (docs/study/README.md, "Testing").

Usage: python3 docs/study/tests/test_ui.py [--only PREFIX[,PREFIX]] [--shots DIR]
  Serves docs/study/ on an ephemeral port, loads dashboard/index.html with its committed data, drives ONE
  Chromium at a time, one viewport at a time, and prints one PASS/FAIL line per check (a failing check prints
  one FAIL line per failing viewport/theme scope); exits 1 on any FAIL. --shots DIR also writes DIR/<set>/
  w<width>-<theme>.png for each named parameter set in SHOT_SETS at every viewport and theme.
Expected values are recomputed in Python from posterior.json, seed_metrics.json, runs.json and arms.json;
none of the page's JS logic is reused.
"""
import json, math, re, sys, pathlib, functools, threading, http.server, datetime, collections
from html.parser import HTMLParser
from playwright.sync_api import sync_playwright

HERE = pathlib.Path(__file__).resolve().parent
STUDY = HERE.parent
DATA = STUDY / 'data'
PAGE = STUDY / 'dashboard' / 'index.html'
PARAMS = json.load(open(STUDY / 'dashboard' / 'params.json'))
SHOTS = HERE / 'shots-test'
VIEWPORTS = [1440, 1280, 1100, 768, 400]
THEMES = ['light', 'dark']
METRICS6 = ['bytes_total', 'conv_p99_incl_never_ms', 'stale_limited_p99_pct', 'drop_rcvbuf_pkts', 'part_time_to_heal_ms', 'send_pending_max']
ROPES = [0.01, 0.05, 0.10, 0.25]
args = sys.argv[1:]
ONLY = args[args.index('--only') + 1].split(',') if '--only' in args else None
SHOT_DIR = pathlib.Path(args[args.index('--shots') + 1]) if '--shots' in args else None
# --shots: one view per headline question, applied over params.json.
SHOT_SETS = {
    'bytes': {'metric': 'bytes_total'},
    'p99': {'metric': 'conv_p99_incl_never_ms'},
    'stale': {'metric': 'stale_limited_p99_pct', 'regime': 'long-window', 'arm': 'owner-agg'},
    'drops': {'metric': 'drop_rcvbuf_pkts', 'regime': 'az-partition', 'arm': 'bimodal'},
    'heal': {'metric': 'part_time_to_heal_ms', 'regime': 'az-partition', 'arm': 'direct-ack'},
    'cell': {'metric': 'conv_p50_incl_never_ms', 'regime': 'az-partition', 'arm': 'local-only', 'rope': '0.1',
             'y': 'stale_limited_p99_pct', 'plane': 'all', 'fam': 'Speed'},
}
def want(cid): return ONLY is None or any(cid == p or cid.startswith(p + '.') or p.startswith(cid + '.') for p in ONLY)

# ---------------------------------------------------------------- results
RES = []   # (cid, scope, ok, evidence)
def rec(cid, scope, ok, evidence=''):
    RES.append((cid, scope, bool(ok), evidence))
def fails_to_rec(cid, scope, bad, ok_note='', limit=6):
    rec(cid, scope, not bad, (f'{len(bad)} failing: ' + ' || '.join(bad[:limit]) + (f' || ... +{len(bad) - limit} more' if len(bad) > limit else '')) if bad else ok_note)

# ---------------------------------------------------------------- reference data + independent recomputation
load = lambda n: json.load(open(DATA / n))
POST, SEEDS, RUNS, ARMS, REGS, METS = load('posterior.json'), load('seed_metrics.json'), load('runs.json'), load('arms.json'), load('regimes.json'), load('metrics.json')
ARM_ORDER = [a['arm'] for a in sorted(ARMS, key=lambda a: a['order'])]
ARM_NAME = {a['arm']: a['name'] for a in ARMS}
ARM_FAM = {a['arm']: a['family'] for a in ARMS}
CONTENDERS = [a for a in ARM_ORDER if a != 'main']
REF = 'local-only'
MET = {m['metric']: m for m in METS}
MET_ORDER = [m['metric'] for m in sorted(METS, key=lambda m: m['order'])]
grp_rank = {}
for g in sorted(REGS, key=lambda g: g['order']): grp_rank.setdefault(g['group'], len(grp_rank))
REG_ORDER = [g['regime'] for g in sorted(REGS, key=lambda g: (grp_rank[g['group']], g['order']))]
REG_DESC = {g['regime']: g['description'] for g in REGS}
COST = MET['bytes_total']['group']
FAMS = [f for f in dict.fromkeys(MET[m]['group'] for m in MET_ORDER) if f != COST]
PROW = {(r['regime'], r['solution'], r['metric']): r for r in POST}
CENS_Q = {'conv_p50_incl_never_ms': 0.5, 'conv_p99_incl_never_ms': 0.99}
NEVER = {(r['regime'], r['solution']): r['mean'] for r in POST if r['metric'] == 'conv_never_frac'}

def verdict_of(v, rope):
    if v['kind'] == 'na': return 'na'
    if not v['paired']: return 'one'
    if v['kind'] == 'same': return 'tie'
    a, b = 1 / (1 + rope), 1 + rope
    if v['scale'] == 'log':
        if v['lo'] is not None:
            lo, hi = v['lo'], v['hi']
        elif v['ratio'] == 1.0:      # identical on every paired seed: a tie (builder's documented deviation)
            return 'tie'
        else:
            return 'open'
        if hi < a: return 'better'
        if lo > b: return 'worse'
        return 'tie' if (lo >= a and hi <= b) else 'open'
    if v['scale'] == 'diff' and v['dlo'] is not None:
        return 'better' if v['dhi'] < 0 else 'worse' if v['dlo'] > 0 else 'open'
    return 'open'

def classify(r, base):
    """One posterior row against its baseline solution's row (main for posterior.json, `against` for self_posterior.json)."""
    m = PROW.get((r['regime'], base, r['metric']))
    mean, mm = r['mean'], (m['mean'] if m else None)
    n, mn = (r['n_valid'] or 0), ((m['n_valid'] or 0) if m else 0)
    kind = 'na' if mean is None or mm is None else 'same' if mean == 0 and mm == 0 else 'zero' if mean == 0 else 'mainzero' if mm == 0 else 'ratio'
    ratio = (r['ratio'] if r['ratio'] is not None else mean / mm) if kind == 'ratio' else (1 if kind == 'same' else 0 if kind == 'zero' else None)
    paired = n >= 2 and mn >= 2
    q = CENS_Q.get(r['metric'])
    def cens(arm, r=r, q=q): return q is not None and NEVER.get((r['regime'], arm)) is not None and NEVER[(r['regime'], arm)] > 1 - q
    v = dict(arm=r['solution'], regime=r['regime'], metric=r['metric'], kind=kind, scale=r['scale'], paired=paired, ratio=ratio,
             lo=r['ratio_lo'] if r['scale'] == 'log' else None, hi=r['ratio_hi'] if r['scale'] == 'log' else None,
             dlo=r['diff_lo'], dhi=r['diff_hi'], dmean=r['diff_mean'], p=r['p_better'], n=int(n), mn=int(mn),
             ntot=int(r['n_total'] if r['n_total'] is not None else n), mntot=int((m['n_total'] or 0) if m else 0),
             mean=mean, cri=(None if r['cri_lo'] is None else max(0.0, r['cri_lo']), r['cri_hi']),
             mmean=mm, mcri=((None if m['cri_lo'] is None else max(0.0, m['cri_lo']), m['cri_hi']) if m else (None, None)),
             zero_all=r['max'] == 0, mzero_all=bool(m and m['max'] == 0), cens=cens(r['solution']), mcens=cens(base),
             never=NEVER.get((r['regime'], r['solution'])), main_missing=mean is not None and mm is None, against=base)
    for rp in ROPES: v[rp] = verdict_of(v, rp)
    return v

V = {(r['solution'], r['regime'], r['metric']): classify(r, 'main') for r in POST if r['solution'] != 'main'}
SELF_POST = json.load(open(DATA / 'self_posterior.json'))
SELF_V = {(r['comparison'], r['solution'], r['regime'], r['metric']): classify(r, r['against']) for r in SELF_POST}

# ---------------------------------------------------------------- variants (arms.json maps each row to one solution per variant)
VARIANTS = ['baseline', 'improved', 'improved_nopk']
VAR_PHRASE = {'baseline': 'the baseline designs', 'improved': 'the improved designs', 'improved_nopk': 'the improved designs without the per-key fix'}
SOLMAP = {v: {a['arm']: (a['arm'] if v == 'baseline' else a.get(v)) for a in ARMS} for v in VARIANTS}
SOLMAP['baseline']['main'] = None          # baseline main is what every ratio is measured against
ALL_SOLUTIONS = [x for a in sorted(ARMS, key=lambda a: a['order']) for x in (a['arm'], a.get('improved'), a.get('improved_nopk')) if x]
def reason_for(v, arm):
    if v == 'baseline': return 'the baseline: every ratio is measured against it'
    return 'no per-key trigger in this design' if next(a for a in ARMS if a['arm'] == arm).get('improved') else 'reference, not improved'
V_SOL = V                                  # keyed by solution id
BY_SOL = collections.defaultdict(list)
for (sol_, g_, m_), v_ in V_SOL.items(): BY_SOL[sol_].append((g_, m_, v_))
VAR_SUB = {'improved': 'improved', 'improved_nopk': 'improved, no per-key'}
CUR_VAR = 'baseline'; LNAME = {}; SNAME = {}; DNAME = {}
def use_variant(var):
    """Rebind V (keyed by arm id), CONTENDERS (rows with a solution) and the names the page uses, for one variant."""
    global V, CONTENDERS, CUR_VAR, LNAME, SNAME, DNAME
    CUR_VAR = var
    CONTENDERS = [a for a in ARM_ORDER if SOLMAP[var][a]]
    V = {(a, g, m): dict(v, arm=a, sol=SOLMAP[var][a]) for a in CONTENDERS for g, m, v in BY_SOL[SOLMAP[var][a]]}
    DNAME = {a: re.sub(r'\s*\([^)]*\)$', '', ARM_NAME[a]) if var != 'baseline' and a == 'main' else ARM_NAME[a] for a in ARM_ORDER}
    LNAME = {a: DNAME[a] if var == 'baseline' else f'{DNAME[a]}, as {SOLMAP[var][a]}' for a in CONTENDERS}
    SNAME = {a: DNAME[a] if var == 'baseline' else SOLMAP[var][a] for a in CONTENDERS}
use_variant('baseline')

def present_regimes(metric):
    return [g for g in REG_ORDER if any((a, g, metric) in V and V[(a, g, metric)]['kind'] != 'na' for a in CONTENDERS)]

def metric_tally(metric, rope):
    t = collections.Counter()
    for (a, g, m), v in V.items():
        if m == metric and a != REF and v['kind'] != 'na': t[v[rope]] += 1
    return t

def family_tally(arm, fam, rope):
    t = collections.Counter()
    for (a, g, m), v in V.items():
        if a == arm and MET[m]['group'] == fam and v['kind'] != 'na': t[v[rope]] += 1
    return t

def sig_tol(shown, true, s):
    """True when `shown` equals `true` rounded to s significant digits (half-unit tolerance)."""
    if true == 0: return shown == 0
    e1 = math.floor(math.log10(abs(true))); e2 = math.floor(math.log10(abs(shown))) if shown else e1
    half = 0.5 * 10 ** (max(e1, e2) - s + 1) * (1 + 1e-7)
    return abs(shown - true) <= half + 1e-12 * abs(true)

SI = {'k': 1e3, 'M': 1e6, 'G': 1e9, 'T': 1e12, 'P': 1e15, 'm': 1e-3, 'µ': 1e-6, 'μ': 1e-6, 'n': 1e-9, '': 1}
def parse_num(s):
    m = re.fullmatch(r'(−|-)?([\d,]*\.?\d+)([kMGTPmµμn]?)', s.strip())
    if not m: return None
    return (-1 if m.group(1) else 1) * float(m.group(2).replace(',', '')) * SI[m.group(3)]

def val_ok(shown, true, unit):
    """Check a formatted metric value ('1.23 GB', '12.3k s', '4.5 %') against `true`."""
    shown = shown.replace(' ', ' ').strip().lstrip('≥').strip()
    m = re.fullmatch(r'(−?-?[\d,]*\.?\d+)([kMGTP]?) ?(.*)', shown)
    if not m: return False, f'unparseable "{shown}"'
    num = float(m.group(1).replace(',', '').replace('−', '-')); si = SI[m.group(2)]; suf = m.group(3)
    if suf == '':
        fac = 1
    elif unit == 'bytes':
        fac = {'B': 1, 'kB': 1e3, 'MB': 1e6, 'GB': 1e9, 'TB': 1e12}.get(suf)
    elif unit == 'ms':
        fac = {'ms': 1, 's': 1e3}.get(suf)
    elif unit == 'fraction':
        fac = 0.01 if suf == '%' else None
    elif unit == '% of limit':
        fac = 1 if suf == '%' else None
    else:
        fac = 1 if suf == unit else None
    if fac is None: return False, f'unit "{suf}" is wrong for {unit} in "{shown}"'
    S = 2 if unit == 'fraction' else 3
    base = num * si * fac
    e = math.floor(math.log10(abs(num))) if num else 0
    half = 0.5 * 10 ** (e - S + 1) * fac * si * (1 + 1e-7)
    ok = abs(base - true) <= half + 1e-12 * max(abs(true), 1e-300) or (num == 0 and true == 0)
    return ok, f'{shown} vs {true:.6g} (tolerance {half:.3g})'

NUM = r'[−-]?[\d,]*\.?\d+'
def dec_ok(shown, true, factor=1.0):
    """shown printed with its own number of decimals; equals true/factor rounded to those decimals."""
    t = shown.replace(',', '').replace('−', '-'); d = len(t.split('.')[1]) if '.' in t else 0
    return abs(float(t) * factor - true) <= 0.5 * 10 ** (-d) * factor * (1 + 1e-7) + 1e-12 * abs(true)
def sig_ok(shown, true, s):
    return (p := parse_num(shown)) is not None and sig_tol(p, true, s)
SCALES = {'bytes': [(1e12, 'TB'), (1e9, 'GB'), (1e6, 'MB'), (1e3, 'kB'), (1, 'B')], 'ms': [(1000, 's'), (1, 'ms')]}
def unit_scale(unit, ref):
    """(factor, suffix text exactly as printed) for a unit and the value that picks its scale."""
    if unit in SCALES:
        k, sfx = next(((k, s_) for k, s_ in SCALES[unit] if abs(ref) >= k), SCALES[unit][-1]); return k, ' ' + sfx
    if unit == 'fraction': return 0.01, ' %'
    if unit == '% of limit': return 1, ' %'
    return 1, ' ' + unit
def has_diff(v): return v['scale'] == 'diff' and v['dlo'] is not None
def is_exact(v): return v['kind'] == 'ratio' and v['lo'] is None and v['paired'] and v['scale'] == 'log' and v['ratio'] == 1.0

def check_triple(txt, mean, lo, hi, unit, pre='', nolead=False):
    """'m [l, h]UNIT' (all three at one scale). Returns mismatches."""
    ref = mean or hi or lo or 0
    k, sfx = unit_scale(unit, ref) if unit else (1, '')
    m = re.match(rf'^{pre}({NUM}) \[({NUM}), ({NUM})\](.*)$', txt)
    if not m: return [f'shape "{txt}" (expected "m [l, h]{sfx}")']
    bad = []
    if m.group(4) != sfx: bad.append(f'unit "{m.group(4)}" expected "{sfx}" in "{txt}"')
    for g, tv, nm in ((m.group(1), mean, 'mean'), (m.group(2), lo, 'lo'), (m.group(3), hi, 'hi')):
        if tv is None or not dec_ok(g, tv, k): bad.append(f'{nm} {g} vs {None if tv is None else tv / k:.6g}{sfx}')
    ds = {len(x.split('.')[1]) if '.' in x else 0 for x in (m.group(1), m.group(2), m.group(3))}
    if len(ds) != 1: bad.append(f'mixed precision in "{txt}"')
    return bad

def no_interval(n, tot): return f'{n} of {tot} seeds with a value: no interval' if tot > n else f'{n} seed: no interval yet'
def seeds_short(v): return no_interval(v['n'], v['ntot']) if v['n'] < 2 else 'main has ' + no_interval(v['mn'], v['mntot'])

def check_abs(txt, mean, cri, cens, zero, unit, n=None, tot=None):
    """absolute mean with interval: '[≥ ]m [l, h]UNIT', '[≥ ]mUNIT (no interval yet)' / '(N of T seeds with a value: no interval)', or '0 in every seed'."""
    if zero: return [] if txt.strip() == '0 in every seed' else [f'"{txt}" expected "0 in every seed"']
    t = txt
    if bool(t.startswith('≥ ')) != bool(cens): return [f'≥ prefix {t.startswith("≥ ")} expected {bool(cens)} in "{txt}"']
    t = t[2:] if t.startswith('≥ ') else t
    if cri[0] is None or cri[1] is None:
        k, sfx = unit_scale(unit, mean or 0)
        tail = '(no interval yet)' if tot is None or tot <= n else f'({n} of {tot} seeds with a value: no interval)'
        m = re.match(rf'^({NUM})(.*) {re.escape(tail)}$', t)
        if not m: return [f'shape "{txt}" expected "mUNIT {tail}"']
        return ([] if dec_ok(m.group(1), mean, k) else [f'{m.group(1)} vs {mean / k:.6g}']) + ([] if m.group(2) == sfx else [f'unit "{m.group(2)}" expected "{sfx}"'])
    return check_triple(t, mean, cri[0], cri[1], unit)

def check_diff(txt, v, unit):
    return check_triple(txt, v['dmean'], v['dlo'], v['dhi'], unit, pre='difference ')

def check_full(txt, v, unit):
    """The 'full' interval text for a cell (vs-main / Paired ratio / aria-label head, no verdict words)."""
    t = txt.strip(); bad = []
    if v['kind'] == 'same': return [] if t == '0 in every seed, as is main' else [f'"{t}" expected "0 in every seed, as is main"']
    if v['kind'] in ('zero', 'mainzero'):
        head = ('0 in every seed' if v['zero_all'] else '×0') if v['kind'] == 'zero' else 'main 0, arm '
        if not t.startswith(head): return [f'"{t}" does not start with "{head}"']
        if has_diff(v):
            m = re.search(r'; (difference .*)$', t)
            return [f'no "; difference ..." in "{t}"'] if not m else check_diff(m.group(1), v, unit)
        return [] if t.endswith(f'({seeds_short(v)})') else [f'"{t}" lacks the seed note "({seeds_short(v)})"']
    R = v['ratio']
    if is_exact(v): return [] if t == '×1 on every paired seed' else [f'"{t}" expected "×1 on every paired seed"']
    if v['lo'] is None:
        if has_diff(v):
            m = re.match(rf'^×(\S+) \(ratio of means\); (difference .*)$', t)
            if not m: return [f'"{t}" expected "×r (ratio of means); difference ..."']
            return ([] if sig_ok(m.group(1), R, 2 if R >= 1000 else 3 if R >= 1 else 2) else [f'ratio {m.group(1)} vs {R:.6g}']) + check_diff(m.group(2), v, unit)
        m = re.match(rf'^×(\S+) {re.escape("(" + seeds_short(v) + ")")}$', t)
        if not m: return [f'"{t}" expected "×r ({seeds_short(v)})"']
        return [] if sig_ok(m.group(1), R, 2 if R >= 1000 else 3 if R >= 1 else 2) else [f'ratio {m.group(1)} vs {R:.6g}']
    return check_triple(t, R, v['lo'], v['hi'], '', pre='×')

def check_compact(txt, v):
    """Compact form: '×r ±x %' when the half-width is at most 50 %, else '×r [lo, hi]' (ends to 2 significant figures)."""
    m = re.match(r'^×(\S+?)(?: ±(\S+)\u00a0%| \[(\S+), (\S+)\])?(?:,.*)?$', txt.strip())
    if not m: return [f'compact "{txt}" not recognised']
    R = v['ratio']; bad = []
    if not sig_ok(m.group(1), R, 2 if R >= 1000 else 3 if R >= 1 else 2): bad.append(f'ratio {m.group(1)} vs {R:.6g}')
    if v['lo'] is not None and not is_exact(v):
        hw = v['hi'] / R - 1
        if hw <= 0.5:
            if not m.group(2): bad.append(f'no ± in "{txt}" (half-width {100 * hw:.3g} % <= 50 %)')
            elif not sig_ok(m.group(2), 100 * hw, 2): bad.append(f'±{m.group(2)} vs ±{100 * hw:.4g}')
        else:
            if not m.group(3): bad.append(f'no [lo, hi] in "{txt}" (half-width {100 * hw:.3g} % > 50 %)')
            else:
                for g, tv in ((m.group(3), v['lo']), (m.group(4), v['hi'])):
                    if not sig_ok(g, tv, 2): bad.append(f'bound {g} vs {tv:.6g} (2 sig figs)')
    return bad

def fmt_p(p):
    return '—' if p is None else '> 0.999' if p > 0.999 else '< 0.001' if p < 0.001 else f'{p:.3f}'

WORD_END = [('better than main', 'better'), ('worse than main', 'worse'), ('a tie with main', 'tie'), ('one seed: cannot settle', 'one'), ('not settled', 'open')]
KEY_STATE = {'Better.': 'better', 'Tie.': 'tie', 'Worse.': 'worse', 'Not settled.': 'open', 'One seed, no interval yet.': 'one'}

def py_progress():
    seeds = max(3, max(r['seed'] for r in RUNS))
    valid = [r for r in RUNS if r['valid'] in (True, 'true')]
    per = collections.Counter((r['regime'], r['arm']) for r in valid)
    any_ = {r['regime'] for r in RUNS}
    stamps = sorted(r['run_at'] for r in RUNS if r.get('run_at'))
    base = {a['arm'] for a in ARMS}
    return dict(done=len(RUNS), invalid=len(RUNS) - len(valid), planned=len(ALL_SOLUTIONS) * len(REGS) * seeds, regimes=len(REGS),
                base_done=sum(1 for r in RUNS if r['arm'] in base), base_planned=len(base) * len(REGS) * seeds,
                imp_done=sum(1 for r in RUNS if r['arm'] not in base), imp_planned=(len(ALL_SOLUTIONS) - len(base)) * len(REGS) * seeds,
                empty=sum(1 for g in REGS if g['regime'] not in any_),
                settled=sum(1 for g in REGS if all(per[(g['regime'], s_)] >= 2 for s_ in ALL_SOLUTIONS)),
                stamp=stamps[-1], per_planned=len(ALL_SOLUTIONS) * seeds, per_done=collections.Counter(r['regime'] for r in RUNS))

# ---------------------------------------------------------------- browser infrastructure
class Quiet(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        if self.path.startswith('/favicon.ico'):
            self.send_response(204); self.end_headers(); return
        super().do_GET()

class Session:
    """One chromium; one context at a time."""
    def __init__(self, pw, base):
        self.browser = pw.chromium.launch(); self.base = base
    def open(self, width, theme, height=900):
        self.ctx = self.browser.new_context(viewport={'width': width, 'height': height}, color_scheme=theme, device_scale_factor=1)
        self.pg = self.ctx.new_page(); self.logs = []; self.fails = []
        self.pg.on('console', lambda m: self.logs.append(f'{m.type}: {m.text}') if m.type in ('error', 'warning') else None)
        self.pg.on('pageerror', lambda e: self.logs.append(f'pageerror: {e}'))
        self.pg.on('requestfailed', lambda r: self.fails.append(f'requestfailed {r.url}'))
        self.pg.on('response', lambda r: self.fails.append(f'HTTP {r.status} {r.url}') if r.status >= 400 else None)
        for attempt in range(3):
            self.pg.goto(self.base + '/dashboard/')
            try:
                self.pg.wait_for_selector('[data-fk="metric|bytes_total"]', timeout=25000); break
            except Exception as e:
                if attempt == 2: raise RuntimeError(f'the page never drew its metric pills; seen={(self.fails + self.logs)[:3]}') from e
        self.pg.wait_for_timeout(700)
        self.pg.evaluate(f"document.documentElement.setAttribute('data-theme','{theme}')"); self.settle(300)
        self.width, self.theme = width, theme
        return self.pg
    def settle(self, ms=40):
        self.pg.evaluate("ms => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(() => setTimeout(r, ms))))", ms)
        self.pg.wait_for_timeout(30)
    def params(self, **p):
        self.pg.evaluate('p => dash.setParams(p)', p); self.settle(160)
    def reset(self):
        self.params(**PARAMS)
    def close(self):
        self.ctx.close()

# ---------------------------------------------------------------- in-page probes
JS_HELPERS = r'''
window.__t = (() => {
  const root = document.getElementById('g3');
  const cssRgba = (c) => { const k = document.createElement('canvas'); k.width = k.height = 1; const x = k.getContext('2d', { willReadFrequently: true }); x.clearRect(0,0,1,1); x.fillStyle = '#000'; x.fillStyle = c; x.fillRect(0,0,1,1); const d = x.getImageData(0,0,1,1).data; return [d[0], d[1], d[2], d[3] / 255]; };
  const lum = ([r,g,b]) => { const f = (v) => { v /= 255; return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); }; return 0.2126 * f(r) + 0.7152 * f(g) + 0.0722 * f(b); };
  const blend = (fg, bg) => [0,1,2].map((i) => fg[i] * fg[3] + bg[i] * (1 - fg[3])).concat([1]);
  const contrast = (a, b) => { const l1 = lum(a), l2 = lum(b); return (Math.max(l1, l2) + 0.05) / (Math.min(l1, l2) + 0.05); };
  const pageBg = () => cssRgba(getComputedStyle(document.body).backgroundColor);
  const bgOf = (el) => { for (let e = el; e && e !== document.documentElement; e = e.parentElement) { const c = cssRgba(getComputedStyle(e).backgroundColor); if (c[3] > 0.99) return c; } return pageBg(); };
  const opacityChain = (el) => { let o = 1; for (let e = el; e && e.nodeType === 1; e = e.parentElement) o *= parseFloat(getComputedStyle(e).opacity); return o; };
  const sel = (e) => { if (!e || !e.tagName) return '?'; let s = e.tagName.toLowerCase(); if (e.id) return s + '#' + e.id; const c = (e.getAttribute('class') || '').trim().split(/\s+/).filter(Boolean).slice(0, 2).join('.'); if (c) s += '.' + c; const p = e.parentElement; if (p && p !== root && p.tagName) { const pc = ((p.getAttribute && p.getAttribute('class')) || '').trim().split(/\s+/)[0]; s = (p.id ? '#' + p.id : p.tagName.toLowerCase() + (pc ? '.' + pc : '')) + ' > ' + s; } return s; };
  const inMeasure = (e) => !!e.closest('.measure');
  const shown = (e) => { for (let p = e; p && p !== document.body; p = p.parentElement) { if (p.hidden) return false; if (p.tagName === 'DETAILS' && !p.open && !(e.closest('summary') && e.closest('summary').parentElement === p)) return false; const d = getComputedStyle(p); if (d.display === 'none' || d.visibility === 'hidden') return false; } return true; };
  const clipRect = (e, r) => {
    let L = r.left, T = r.top, R = r.right, B = r.bottom;
    for (let p = e.parentElement; p && p !== document.documentElement; p = p.parentElement) {
      if (p.ownerSVGElement) continue; const cs = getComputedStyle(p);
      if (cs.overflowX !== 'visible' || cs.overflowY !== 'visible') { const b = p.getBoundingClientRect(); if (b.width === 0 && b.height === 0) continue; if (cs.overflowX !== 'visible') { L = Math.max(L, b.left); R = Math.min(R, b.right); } if (cs.overflowY !== 'visible') { T = Math.max(T, b.top); B = Math.min(B, b.bottom); } }
    }
    return { left: L, top: T, right: R, bottom: B, width: R - L, height: B - T };
  };
  const items = () => {
    const out = [];
    for (const t of root.querySelectorAll('svg text')) { if (inMeasure(t) || !shown(t) || !t.textContent.trim()) continue; const r = t.getBoundingClientRect(); if (r.width < 0.5) continue; if (/rotate/.test(t.getAttribute('transform') || '')) continue; out.push({ el: t, r, text: t.textContent.trim(), svg: true }); }
    const w = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    for (let n; (n = w.nextNode());) {
      const p = n.parentElement; if (!p || p.closest('svg') || p.closest('script,style') || inMeasure(p) || !n.textContent.trim() || !shown(p)) continue;
      const rg = document.createRange(); rg.selectNodeContents(n);
      for (const r of rg.getClientRects()) if (r.width >= 0.5 && r.height >= 0.5) out.push({ el: p, r, text: n.textContent.trim(), svg: false });
    }
    return out;
  };
  const visibleItems = () => items().map((it) => ({ ...it, c: clipRect(it.el, it.r) })).filter((it) => it.c.width > 0.5 && it.c.height > 0.5);
  return { root, cssRgba, lum, blend, contrast, pageBg, bgOf, opacityChain, sel, inMeasure, shown, clipRect, items, visibleItems };
})();
'''

JS_SCAN = r'''() => {
  const T = window.__t, root = T.root, out = {};
  out.overflow = { scrollWidth: document.documentElement.scrollWidth, innerWidth: window.innerWidth };
  out.errors = window.__ERRORS__.slice();
  const surfaces = [];
  const w = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
  for (let n; (n = w.nextNode());) { const p = n.parentElement; if (p.closest('script,style') || p.closest('.measure')) continue; if (n.textContent.trim()) surfaces.push([T.sel(p), n.textContent]); }
  for (const e of root.querySelectorAll('[aria-label],[title]')) for (const a of ['aria-label', 'title']) if (e.getAttribute(a)) surfaces.push([T.sel(e) + '@' + a, e.getAttribute(a)]);
  out.surfaces = surfaces.length;
  const bad = /\bNaN\b|\bundefined\b|\bnull\b|\bInfinity\b|\[object|\d[eE][+\-]?\d|e\+/;
  out.badText = surfaces.filter(([s, t]) => bad.test(t)).slice(0, 8).map(([s, t]) => s + ' = "' + (t.match(new RegExp('.{0,24}(' + bad.source + ').{0,24}')) || [t])[0] + '"');
  const wordsCase = /(^|\s)CI(\s|$|[.,;:)])/;
  out.badWords = surfaces.filter(([s, t]) => /confidence|significan|p-?value|standard error/i.test(t) || wordsCase.test(t)).slice(0, 8).map(([s, t]) => s + ' = "' + t.slice(0, 80) + '"');
  out.negLower = surfaces.filter(([s, t]) => /[\[(]\s*[-−]\d/.test(t) && !/difference/.test(t)).slice(0, 8).map(([s, t]) => s + ' = "' + (t.match(/.{0,30}[\[(]\s*[-−]\d.{0,20}/) || [t])[0] + '"');
  const nanAttrs = [];
  for (const e of root.querySelectorAll('svg, svg *')) { if (e.closest('.measure')) continue; for (const a of e.attributes) if (/NaN|undefined|Infinity/.test(a.value)) nanAttrs.push(T.sel(e) + ' ' + a.name + '="' + a.value.slice(0, 50) + '"'); }
  out.nanAttrs = nanAttrs.slice(0, 8); out.nanAttrCount = nanAttrs.length;
  const its = T.visibleItems().map((it) => ({ ...it, L: it.c.left, R: it.c.right, Tp: it.c.top, Bt: it.c.bottom }));
  its.sort((a, b) => a.L - b.L);
  const ov = [];
  for (let i = 0; i < its.length; i++) for (let j = i + 1; j < its.length && its[j].L < its[i].R - 1; j++) {
    const a = its[i], b = its[j]; if (a.el === b.el || a.el.contains(b.el) || b.el.contains(a.el)) continue;
    const iw = Math.min(a.R, b.R) - Math.max(a.L, b.L), ih = Math.min(a.Bt, b.Bt) - Math.max(a.Tp, b.Tp);
    if (iw >= 1 && ih >= 1) ov.push(`${T.sel(a.el)} "${a.text.slice(0, 22)}" x ${T.sel(b.el)} "${b.text.slice(0, 22)}" (${iw.toFixed(1)}x${ih.toFixed(1)}px)`);
  }
  out.overlaps = ov.slice(0, 8); out.overlapCount = ov.length; out.textItems = its.length;
  const clipped = [];
  for (const e of root.querySelectorAll('svg text, svg path, svg line, svg circle, svg rect')) {
    if (T.inMeasure(e) || !T.shown(e)) continue; if (e.matches('rect.focus, rect.hl, rect.band')) continue;
    if (e.tagName === 'rect' && e.getAttribute('fill') === 'transparent') continue;
    const r = e.getBoundingClientRect(); if (r.width === 0 && r.height === 0) continue;
    const svg = e.ownerSVGElement; const s = svg.getBoundingClientRect();
    const t = 0.75;
    if (r.left < s.left - t || r.right > s.right + t || r.top < s.top - t || r.bottom > s.bottom + t) clipped.push(`${T.sel(e)}${e.tagName === 'text' ? ' "' + e.textContent.slice(0, 24) + '"' : ''} bbox [${r.left.toFixed(1)},${r.top.toFixed(1)},${r.right.toFixed(1)},${r.bottom.toFixed(1)}] vs svg [${s.left.toFixed(1)},${s.top.toFixed(1)},${s.right.toFixed(1)},${s.bottom.toFixed(1)}]`);
    else for (let p = svg.parentElement; p && p !== document.body; p = p.parentElement) { const cs = getComputedStyle(p); if (cs.overflowX === 'hidden' || cs.overflowX === 'clip') { const b = p.getBoundingClientRect(); if (r.left < b.left - t || r.right > b.right + t) clipped.push(`${T.sel(e)} outside overflow:hidden ${T.sel(p)}`); break; } }
  }
  for (const e of root.querySelectorAll('*')) {
    if (e.closest('svg') || T.inMeasure(e) || !T.shown(e)) continue; const cs = getComputedStyle(e);
    if ((cs.overflowX === 'hidden' || cs.overflowX === 'clip' || cs.overflowY === 'hidden') && e.textContent.trim() && (e.scrollWidth > e.clientWidth + 1 || e.scrollHeight > e.clientHeight + 1) && e.clientWidth > 0) clipped.push(`${T.sel(e)} clips its content (scroll ${e.scrollWidth}x${e.scrollHeight} > client ${e.clientWidth}x${e.clientHeight})`);
  }
  out.clipped = clipped.slice(0, 8); out.clippedCount = clipped.length;
  const strays = [];
  for (const c of root.querySelectorAll('#g3-hub-plot .cell, #g3-super .cell')) {
    const f = c.querySelector('rect.focus'); if (!f) continue; const fr = f.getBoundingClientRect();
    for (const m of c.querySelectorAll('path, line, text')) { const r = m.getBoundingClientRect(); if (r.width === 0 && r.height === 0) continue; const t = 1.5;
      if (r.left < fr.left - t || r.right > fr.right + t || r.top < fr.top - t || r.bottom > fr.bottom + t) strays.push(`${T.sel(c)}[${c.dataset.arm},${c.dataset.regime || c.dataset.metric}] ${m.tagName}${m.tagName === 'text' ? ' "' + m.textContent.slice(0, 20) + '"' : '.' + (m.getAttribute('class') || '')} [${r.left.toFixed(1)},${r.right.toFixed(1)}] vs cell [${fr.left.toFixed(1)},${fr.right.toFixed(1)}]`); }
  }
  out.strays = strays.slice(0, 8); out.strayCount = strays.length;
  return out;
}'''

JS_HUB = r'''() => [...document.querySelectorAll('#g3-hub-plot .cell')].map((c) => ({ arm: c.dataset.arm, regime: c.dataset.regime, rc: c.dataset.rc, label: c.getAttribute('aria-label'), glyph: (c.querySelector('path.gl') || { getAttribute: () => null }).getAttribute('class'), tab: c.getAttribute('tabindex'), sel: c.getAttribute('aria-selected') }))'''

JS_FOREST = r'''() => {
  const plot = document.querySelector('#g3-hub-plot svg'); if (!plot) return null;
  const cells = [...plot.querySelectorAll('.cell')];
  const cols = {};
  for (const c of cells) { const j = +c.dataset.rc.split(',')[1]; const f = c.querySelector('rect.focus').getBoundingClientRect(); if (!cols[j]) cols[j] = { l: f.left - 2, r: f.right + 2, regime: c.dataset.regime }; }
  const lines = [...plot.querySelectorAll('line.ref, line.grid')].filter((l) => !l.closest('.cell')).map((l) => ({ x: l.getBoundingClientRect().left, ref: l.classList.contains('ref') }));
  const bands = [...plot.querySelectorAll('rect.band')].map((b) => { const r = b.getBoundingClientRect(); return { l: r.left, r: r.right }; });
  const labels = [...plot.querySelectorAll('text.sub')].filter((t) => !t.closest('.cell') && !t.closest('.hdr') && /^×[\d.]+k?$/.test(t.textContent)).map((t) => { const r = t.getBoundingClientRect(); return { x: (r.left + r.right) / 2, l: r.left, r: r.right, text: t.textContent }; });
  const colInfo = {};
  for (const j in cols) { const c = cols[j]; const inCol = (x) => x >= c.l && x <= c.r;
    const ls = lines.filter((l) => inCol(l.x)); const ref = ls.find((l) => l.ref); const bd = bands.find((b) => b.l >= c.l - 1 && b.r <= c.r + 1);
    colInfo[j] = { regime: c.regime, left: c.l, right: c.r, ref: ref && ref.x, band: bd || null, grid: ls.filter((l) => !l.ref).map((l) => l.x), labels: labels.filter((t) => inCol(t.x)) }; }
  const cx = (e) => { const r = e.getBoundingClientRect(); return (r.left + r.right) / 2; };
  const data = cells.map((c) => {
    const j = +c.dataset.rc.split(',')[1]; const gl = c.querySelector('path.gl'); const mt = c.querySelector('line.mt'); const ivl = c.querySelector('line.ivl');
    const glr = gl && gl.getBoundingClientRect(); const ivr = ivl && ivl.getBoundingClientRect(); const mtr = mt && mt.getBoundingClientRect();
    return { arm: c.dataset.arm, regime: c.dataset.regime, j, label: c.getAttribute('aria-label'), gl: gl && cx(gl), glB: glr && glr.bottom, glT: glr && glr.top, glL: glr && glr.left, glR: glr && glr.right, mt: mt && cx(mt), state: gl && gl.getAttribute('class').split(' ')[1],
      ivl: ivr && { l: ivr.left, r: ivr.right, t: ivr.top, b: ivr.bottom }, caps: [...c.querySelectorAll('line.cap')].map(cx).sort((a, b) => a - b),
      texts: [...c.querySelectorAll('text')].map((t) => t.textContent), seeds: [...c.querySelectorAll('line.seed')].map(cx) };
  });
  return { colInfo, data };
}'''

JS_PANE = r'''() => {
  const pane = document.getElementById('g3-pane');
  const dts = [...pane.querySelectorAll('dl.read dt')].map((d) => d.textContent), dds = [...pane.querySelectorAll('dl.read dd')].map((d) => d.textContent);
  const zooms = [...pane.querySelectorAll('svg[role=img]')].map((s) => ({ aria: s.getAttribute('aria-label'), rows: [...s.querySelectorAll('line.ivl')].map((l) => { const r = l.getBoundingClientRect(); return { cls: l.getAttribute('class'), w: r.width }; }), texts: [...s.querySelectorAll('text')].map((t) => t.textContent) }));
  const pr = pane.getBoundingClientRect(), hub = document.getElementById('g3-hub-plot').getBoundingClientRect();
  return { h3: [...pane.querySelectorAll('h3')].map((h) => h.textContent), verdict: [...pane.querySelectorAll('p.verdict')].map((p) => p.textContent), dts, dds, zooms, text: pane.innerText, pos: { left: pr.left, top: pr.top, hubRight: hub.right, hubBottom: hub.bottom }, notes: [...pane.querySelectorAll('p.note')].map((p) => p.textContent) };
}'''

def hub_cells(S): return S.pg.evaluate(JS_HUB)

def parse_label(label, arm_name):
    pre = arm_name + ', '
    if not label.startswith(pre): return None, label
    reg, _, body = label[len(pre):].partition(': ')
    return reg, body

def verdict_from_label(body):
    b = body.replace(', a lower bound', '')
    for w, s in WORD_END:
        if b.endswith(w): return s
    return None

# ================================================================= CHECKS
def run_state_checks(S, vp, theme):
    """A1, A2, A3, A8, C4 for each of the 6 metrics at this viewport and theme."""
    S.pg.evaluate(JS_HELPERS)
    base = len(S.logs)
    for metric in METRICS6:
        scope = f'{vp}px/{theme}/{metric}'
        S.params(metric=metric, rope='0.05', regime='', arm='')
        S.pg.evaluate(JS_HELPERS)
        o = S.pg.evaluate(JS_SCAN)
        newlogs = S.logs[base:]; base = len(S.logs)
        if want('A1'):
            rec('A1.no-js-console-errors', scope, not o['errors'] and not [l for l in newlogs if l.startswith(('error', 'pageerror'))] and not S.fails,
                f'selector=window.__ERRORS__/console | seen={(o["errors"] + newlogs + S.fails)[:3]} | expected=[]')
            rec('A1.no-failed-calcs', scope, not [e for e in o['errors'] if e.startswith('calc')], f'selector=window.__ERRORS__ | seen={[e for e in o["errors"] if e.startswith("calc")][:3]} | expected=[]')
            rec('A1.no-page-horizontal-overflow', scope, o['overflow']['scrollWidth'] <= o['overflow']['innerWidth'], f'selector=documentElement.scrollWidth | seen={o["overflow"]["scrollWidth"]} | expected<={o["overflow"]["innerWidth"]}')
        if want('A2'):
            rec('A2.no-NaN-undefined-null-Infinity-exponent-text', scope, not o['badText'], f'selector=text/aria-label/title ({o["surfaces"]} surfaces) | seen={o["badText"][:4]} | expected=none')
        if want('A3'):
            rec('A3.no-NaN-in-svg-attributes', scope, not o['nanAttrs'], f'selector=svg * attributes | seen={o["nanAttrCount"]}: {o["nanAttrs"][:3]} | expected=0')
            rec('A3.no-text-overlap', scope, o['overlapCount'] == 0, f'selector=visible text boxes ({o["textItems"]}) | seen={o["overlapCount"]} overlaps: {o["overlaps"][:4]} | expected=0')
            rec('A3.no-clipping-by-svg-or-container', scope, o['clippedCount'] == 0, f'selector=svg marks/text, overflow:hidden ancestors | seen={o["clippedCount"]}: {o["clipped"][:3]} | expected=0')
            rec('A3.no-mark-outside-its-cell', scope, o['strayCount'] == 0, f'selector=#g3-hub-plot .cell, #g3-super .cell | seen={o["strayCount"]}: {o["strays"][:3]} | expected=0')
        if want('A8'):
            rec('A8.bayesian-wording', scope, not o['badWords'], f'selector=all text | seen={o["badWords"][:3]} | expected=none of the banned wording')
        if want('C4'):
            rec('C4.no-negative-lower-bound-in-text', scope, not o['negLower'], f'selector=all text | seen={o["negLower"][:3]} | expected=no "[-x" bound')
        if any(want(p) for p in ('A2', 'A3', 'A8', 'C4')):
            tips = []
            cells = S.pg.locator('#g3-hub-plot .cell')
            n = cells.count()
            idxs = sorted({0, n - 1, n // 2, max(0, n - 2), min(n - 1, 11)}) if n else []
            for i in idxs:
                try:
                    cells.nth(i).hover(force=True, timeout=4000, position={'x': 9, 'y': 7}); S.pg.wait_for_timeout(60)
                    t = S.pg.evaluate("""() => { const t = document.querySelector('.tip'); const r = t.getBoundingClientRect(); const de = document.documentElement;
                        return { hidden: t.hidden, text: t.innerText, l: r.left, r: r.right, t: r.top, b: r.bottom, vw: de.clientWidth, dh: Math.max(de.scrollHeight, document.body.scrollHeight), sy: window.scrollY }; }""")
                    t['cell'] = cells.nth(i).get_attribute('data-fk'); tips.append(t)
                except Exception as e:
                    tips.append({'err': str(e)[:100], 'cell': i})
            bad_pos = [f'{t["cell"]} tip[{t["l"]:.0f},{t["r"]:.0f}]x[{t["t"] + t["sy"]:.0f},{t["b"] + t["sy"]:.0f}] page 0..{t["vw"]}x{t["dh"]}' for t in tips if 'err' not in t and (t['hidden'] or t['l'] < -0.5 or t['r'] > t['vw'] + 0.5 or t['t'] + t['sy'] < -0.5 or t['b'] + t['sy'] > t['dh'] + 0.5)]
            bad_pos += [f'{t["cell"]} hover failed: {t["err"]}' for t in tips if 'err' in t]
            if want('A3'): fails_to_rec('A3.tooltips-inside-page', scope, bad_pos, f'{len(tips)} tooltips inside page')
            txt = ' '.join(t.get('text', '') for t in tips)
            if want('A2'): rec('A2.tooltips-no-NaN-etc', scope, not re.search(r'\bNaN\b|undefined|\bnull\b|Infinity|\[object|\d[eE][+\-]?\d', txt), f'selector=.tip | seen={txt[:80]!r} | expected=clean')
            if want('A8'): rec('A8.tooltips-wording', scope, not re.search(r'confidence|significan|p-?value|standard error|\bCI\b', txt, re.I), 'selector=.tip')
            if want('C4'): rec('C4.tooltips-no-negative-bound', scope, not re.search(r'[\[(]\s*[-−]\d', ' '.join(l for l in txt.split('\n') if 'difference' not in l)), f'selector=.tip | seen={txt[:80]!r}')
            S.pg.mouse.move(0, 0)

def check_verdict_labels(S, scope, ropes=ROPES, metrics=MET_ORDER, phone=False):
    S.pg.evaluate(JS_HELPERS)
    bad_v, bad_g, bad_r, bad_c, bad_set, bad_tally, bad_main, bad_iw = [], [], [], [], [], [], [], []
    ncell = 0
    for metric in metrics:
        for rope in ropes:
            S.params(metric=metric, rope=str(rope))
            cells = hub_cells(S); ncell += len(cells)
            pres = present_regimes(metric)
            got = {(c['arm'], c['regime']) for c in cells}
            exp = {(a, g) for a in CONTENDERS for g in pres}
            if got != exp: bad_set.append(f'{metric}@{rope}: missing={sorted(exp - got)[:3]} extra={sorted(got - exp)[:3]}')
            for c in cells:
                a, g = c['arm'], c['regime']; v = V.get((a, g, metric))
                reg, body = parse_label(c['label'], LNAME[a])
                if reg != g: bad_set.append(f'{metric}: label "{c["label"][:50]}" regime {reg} != {g}'); continue
                tag = f'{metric}@{rope} {a}/{g}'
                if v is None:
                    if body != 'not run yet': bad_v.append(f'{tag}: seen "{body}" expected "not run yet"')
                    continue
                if v['kind'] == 'na':
                    if body != 'not measured': bad_v.append(f'{tag}: seen "{body}" expected "not measured"')
                    continue
                vs = verdict_from_label(body); ev = v[rope]
                if vs != ev: bad_v.append(f'{tag}: label verdict {vs} vs expected {ev}')
                gcls = (c['glyph'] or '').split(' ')[1] if c['glyph'] else None
                if gcls != ev: bad_g.append(f'{tag}: glyph class {gcls} vs expected {ev}')
                lb = body.endswith(', a lower bound')
                if lb != (v['cens'] and metric in CENS_Q): bad_c.append(f'{tag}: lower-bound suffix {lb} vs expected {v["cens"] and metric in CENS_Q} (never-converged share {v["never"]})')
                if rope == 0.05:
                    head = body.replace(', a lower bound', '')
                    for w_, s_ in WORD_END:
                        if head.endswith(', ' + w_): head = head[: -len(', ' + w_)]; break
                    of_main = v['kind'] == 'ratio' and not has_diff(v)
                    if of_main:
                        if head.endswith(' of main'): head = head[:-8]
                        else: bad_r.append(f'{tag}: missing " of main" in "{body}"')
                    elif head.endswith(' of main'): bad_r.append(f'{tag}: unexpected " of main" in "{body}"')
                    for e in check_full(head, v, MET[metric]['unit']): bad_r.append(f'{tag}: {e}')
            note = S.pg.evaluate("document.getElementById('g3-hub-note').innerText")
            if not note.startswith(f'Showing {VAR_PHRASE[CUR_VAR]}, each against baseline main.'): bad_tally.append(f'{metric}@{rope}: hub note does not name the variant: "{note[:80]}"')
            t = metric_tally(metric, rope)
            if not pres: continue
            mt = re.search(r'At ±(\d+)\s*%: (\d+) arm.regime comparisons better than main, (\d+) worse, (\d+) ties, (\d+) not settled, and (\d+) with one seed', note)
            if not mt: bad_tally.append(f'{metric}@{rope}: sentence not found in "{note[-150:]}"')
            else:
                seen = tuple(int(x) for x in mt.groups()[1:])
                exp_t = (t['better'], t['worse'], t['tie'], t['open'], t['one'])
                if seen != exp_t or int(mt.group(1)) != round(rope * 100): bad_tally.append(f'{metric}@{rope}: seen b/w/t/o/1 {seen} (band {mt.group(1)}) expected {exp_t}')
            if rope == 0.05:
                iv_txt = S.pg.evaluate("document.getElementById('g3-hub-iv').innerText")
                hws = sorted(100 * (v_['hi'] / v_['ratio'] - 1) for (a_, g_, m_), v_ in V.items() if m_ == metric and a_ != REF and v_['lo'] is not None and not is_exact(v_) and v_['ratio'] and v_['ratio'] > 0)
                if hws:
                    med = hws[len(hws) // 2] if len(hws) % 2 else (hws[len(hws) // 2 - 1] + hws[len(hws) // 2]) / 2
                    mm_ = re.search(r'interval is ±(\S+)\u00a0%', iv_txt)
                    if not mm_ or not sig_ok(mm_.group(1), med, 2): bad_iw.append(f'{metric}: "{iv_txt[-70:]}" expected median half-width ±{med:.3g} %')
            if rope == 0.05 and not phone:
                heads = S.pg.evaluate("[...document.querySelectorAll('#g3-hub-plot .hdr')].map(h => [h.getAttribute('aria-label'), [...h.querySelectorAll('text')].map(t => t.textContent)])")
                for lab, txts in heads:
                    g = lab.split(':')[0]; mrow = PROW.get((g, 'main', metric))
                    mt_ = [x for x in txts if x.startswith('main ')]
                    if mrow and mrow['mean'] is not None:
                        if not mt_: bad_main.append(f'{metric}/{g}: no "main ..." header')
                        else:
                            ok, why = val_ok(mt_[0][5:], mrow['mean'], MET[metric]['unit'])
                            if not ok: bad_main.append(f'{metric}/{g}: {why}')
    rec('A4.hub-cell-set', scope, not bad_set, f'selector=#g3-hub-plot .cell | seen/expected: {bad_set[:3]}' if bad_set else f'{ncell} cells, sets match')
    fails_to_rec('A4.verdict-aria-label-vs-spec-rule', scope, bad_v, f'{ncell} cells over {len(metrics)} metrics x {len(ropes)} tie bands')
    fails_to_rec('A4.verdict-glyph-class-vs-spec-rule', scope, bad_g, f'{ncell} glyphs')
    fails_to_rec('A4.censored-lower-bound-flag', scope, bad_c, 'p50/p99 censoring matches never-converged share rule')
    fails_to_rec('A4.hub-ratio-text-vs-posterior', scope, bad_r, 'ratio + interval style/values in every aria-label')
    fails_to_rec('A4.hub-note-tally-vs-posterior', scope, bad_tally, 'better/worse/tie/open/one per metric per tie band, local-only excluded')
    fails_to_rec('A4.interval-width-note-median-half-width', scope, bad_iw, 'median half-width per metric, local-only excluded')
    if not phone: fails_to_rec('A4.hub-column-main-value', scope, bad_main, 'main value in column headers')

def check_hub_table(S, scope):
    S.pg.evaluate(JS_HELPERS)
    bad = []; n = 0
    for metric in MET_ORDER:
        S.params(metric=metric, rope='0.05')
        rows = S.pg.evaluate("""() => { const t = document.querySelector('#g3-hub-table table'); if (!t) return null; const heads = [...t.querySelectorAll('thead th')].map(h => h.textContent);
            return { heads, rows: [...t.querySelectorAll('tbody tr')].map(r => [...r.children].map(c => c.textContent)) }; }""")
        if rows is None: bad.append(f'{metric}: no <table>'); continue
        if [r[0] for r in rows['rows']] != [DNAME[a] for a in ARM_ORDER]: bad.append(f'{metric}: table rows {[r[0] for r in rows["rows"]]} expected every arm in arms.json order')
        for r in rows['rows']:
            arm = next((a for a in ARM_ORDER if DNAME[a] == r[0]), None)
            if arm is None: bad.append(f'{metric}: row "{r[0]}" not an arm'); continue
            if arm not in CONTENDERS:
                if r[1:] != [f'— {reason_for(CUR_VAR, arm)}']: bad.append(f'{metric}: {arm} row {r[1:]} expected the reason "— {reason_for(CUR_VAR, arm)}"')
                continue
            for h, txt in zip(rows['heads'][1:], r[1:]):
                v = V.get((arm, h, metric)); n += 1
                if v is None:
                    if txt != 'not run yet': bad.append(f'{metric} {arm}/{h}: "{txt}" expected "not run yet"')
                elif v['main_missing']:
                    if 'main has no value' not in txt: bad.append(f'{metric} {arm}/{h}: "{txt}" expected main-has-no-value note')
                elif v['kind'] == 'na':
                    if txt != 'not measured': bad.append(f'{metric} {arm}/{h}: "{txt}"')
                elif v['kind'] == 'ratio':
                    t = txt[2:] if txt.startswith('≥ ') else txt
                    m = re.match(r'^(.*?)(?: · (better|tie|worse|not settled))?$', t.strip())
                    for e in check_full(m.group(1), v, MET[metric]['unit']): bad.append(f'{metric} {arm}/{h}: {e}')
                    word = {None: 'one', 'better': 'better', 'tie': 'tie', 'worse': 'worse', 'not settled': 'open'}[m.group(2)]
                    if word != v[0.05]: bad.append(f'{metric} {arm}/{h}: table verdict {word} vs {v[0.05]}')
                    if txt.startswith('≥') != (v['cens'] and metric in CENS_Q): bad.append(f'{metric} {arm}/{h}: ≥ prefix mismatch')
    fails_to_rec('A4.table-fallback-ratios-vs-posterior', scope, bad, f'{n} table cells')

def check_scorecard(S, scope):
    S.pg.evaluate(JS_HELPERS)
    bad_cnt, bad_bytes, bad_worst, bad_ref, bad_title, bad_self = [], [], [], [], [], []
    for rope in ROPES:
        S.params(rope=str(rope))
        rows = S.pg.evaluate("""() => [...document.querySelectorAll('#g3-score tbody tr.arm')].map(tr => ({ name: tr.querySelector('th.name').firstChild.textContent, cells: [...tr.querySelectorAll('td')].map(td => ({ text: td.innerText, title: (td.querySelector('.stack') || td).getAttribute('aria-label'), stack: [...td.querySelectorAll('.stack i')].map(i => i.style.width), counts: (td.querySelector('.counts') || {}).textContent || null, one_title: (td.querySelector('.one-only') || {getAttribute: () => null}).getAttribute('title'), one_glyph: !!td.querySelector('.one-only path.gl.one') })), full: tr.innerText }))""")
        names = {DNAME[a]: a for a in ARM_ORDER}
        if [r['name'] for r in rows] != [DNAME[a] for a in ARM_ORDER]: bad_cnt.append(f'scorecard rows {[r["name"] for r in rows]} expected every arm in arms.json order')
        for r in rows:
            arm = names.get(r['name'])
            if arm is None: bad_cnt.append(f'row "{r["name"]}" unknown'); continue
            if arm not in CONTENDERS:
                if f'— {reason_for(CUR_VAR, arm)}' not in r['full']: bad_ref.append(f'{rope}: {arm} row "{r["full"][:70]}" lacks the reason "— {reason_for(CUR_VAR, arm)}"')
                continue
            if arm == REF:
                if 'sends nothing' not in r['full']: bad_ref.append(f'{rope}: local-only row "{r["full"][:60]}" lacks "sends nothing"')
                continue
            tds = r['cells']
            if CUR_VAR != 'baseline':
                if VAR_SUB[CUR_VAR] not in r['full'].split('\n')[0] + ' ' + r['full']: bad_self.append(f'{arm}@{rope}: no "{VAR_SUB[CUR_VAR]}" sub-label')
                bad_self += self_score_cell(tds[-1]['text'], arm, rope)
            elif len(tds) != len(FAMS) + 2: bad_self.append(f'{arm}@{rope}: baseline view has {len(tds)} cells, expected no vs-own-baseline column')
            cells = [V[(arm, g, 'bytes_total')] for g in REG_ORDER if (arm, g, 'bytes_total') in V]
            cells = [v for v in cells if v['paired'] and v['lo'] is not None]
            if cells:
                rs = sorted(v['ratio'] for v in cells); n = len(rs)
                med = rs[n // 2] if n % 2 else (rs[n // 2 - 1] + rs[n // 2]) / 2
                m = re.match(r'^×(\S+)\s*×(\S+)–×(\S+) · (\d+) regimes?$', tds[0]['text'].replace('\n', ' ').strip())
                if not m: bad_bytes.append(f'{arm}@{rope}: bytes cell shape "{tds[0]["text"]}"')
                else:
                    for g_, tv in ((m.group(1), med), (m.group(2), rs[0]), (m.group(3), rs[-1])):
                        p = parse_num(g_)
                        if p is None or not sig_tol(p, tv, 2 if tv >= 1000 else 3 if tv >= 1 else 2): bad_bytes.append(f'{arm}@{rope}: {g_} vs {tv:.6g}')
                    if int(m.group(4)) != n: bad_bytes.append(f'{arm}@{rope}: {m.group(4)} regimes vs {n}')
            for f, td in zip(FAMS, tds[1:1 + len(FAMS)]):
                t = family_tally(arm, f, rope)
                dec = t['better'] + t['tie'] + t['worse'] + t['open']
                if sum(t.values()) == 0:
                    if td['text'].strip() != '—': bad_cnt.append(f'{arm}/{f}@{rope}: "{td["text"]}" expected —')
                elif dec == 0:
                    if td['text'].strip() != str(t['one']): bad_cnt.append(f'{arm}/{f}@{rope}: "{td["text"]}" expected count {t["one"]}')
                    if td['one_title'] != f'{t["one"]} with one seed so far': bad_cnt.append(f'{arm}/{f}@{rope}: title "{td["one_title"]}" expected "{t["one"]} with one seed so far"')
                    if not td['one_glyph']: bad_cnt.append(f'{arm}/{f}@{rope}: no ◇ one-seed glyph beside the count')
                else:
                    exp_title = f'{t["better"]} better, {t["tie"]} tie, {t["worse"]} worse, {t["open"]} not settled, {t["one"]} with one seed'
                    if td['title'] != exp_title: bad_title.append(f'{arm}/{f}@{rope}: title "{td["title"]}" expected "{exp_title}"')
                    mm = re.match(r'^▼(\d+) =(\d+) ▲(\d+) ○(\d+)$', (td['counts'] or '').strip())
                    if not mm or tuple(map(int, mm.groups())) != (t['better'], t['tie'], t['worse'], t['open']): bad_cnt.append(f'{arm}/{f}@{rope}: counts "{td["counts"]}" expected ▼{t["better"]} ={t["tie"]} ▲{t["worse"]} ○{t["open"]}')
                    tot = dec + t['one']
                    for k, w in zip(['better', 'tie', 'worse', 'open', 'one'], td['stack']):
                        if abs(float(w.rstrip('%')) - 100 * t[k] / tot) > 0.01: bad_cnt.append(f'{arm}/{f}@{rope}: stack {k} width {w} expected {100 * t[k] / tot:.2f}%')
            lost = [V[k] for k in V if k[0] == arm and V[k][rope] == 'worse']
            wt = tds[1 + len(FAMS)]['text'].replace('\n', ' ')
            if not lost:
                if 'none settled' not in wt: bad_worst.append(f'{arm}@{rope}: "{wt}" expected "none settled"')
            else:
                key = lambda v: math.inf if v['kind'] == 'mainzero' else v['ratio']
                best = max(key(v) for v in lost)
                cands = [v for v in lost if key(v) == best]
                ok = any(MET[v['metric']]['short'] in wt and wt.rstrip().endswith('· ' + v['regime']) for v in cands)
                if not ok: bad_worst.append(f'{arm}@{rope}: "{wt}" expected {[(MET[v["metric"]]["short"], v["regime"], v["ratio"]) for v in cands]}')
                elif cands[0]['kind'] == 'ratio' and not has_diff(cands[0]):
                    head = re.sub(r' (?:%s) · .*$' % '|'.join(re.escape(MET[v['metric']]['short']) for v in cands), '', wt.strip())
                    for e in check_compact(head, cands[0]): bad_worst.append(f'{arm}@{rope}: {e}')
    fails_to_rec('A4.scorecard-better-tie-worse-counts', scope, bad_cnt + bad_title, 'per arm, family, tie band; stack widths too')
    fails_to_rec('A4.scorecard-bytes-median-range', scope, bad_bytes, 'median/min/max ratio and regime count')
    fails_to_rec('A4.scorecard-worst-settled-loss', scope, bad_worst, 'worst loss cell, ratio, metric, regime')
    fails_to_rec('A4.scorecard-local-only-excluded', scope, bad_ref, 'local-only row is a description, not counts')
    fails_to_rec('A4.scorecard-vs-own-baseline', scope, bad_self, 'improved views: median bytes ratio and error better/worse counts against the own baseline; sub-label; no column in the baseline view')
    bad_st = []
    for rope in ROPES:
        S.params(rope=str(rope))
        cells = S.pg.evaluate("""() => [...document.querySelectorAll('#g3-super .cell')].map(c => ({ arm: c.dataset.arm, metric: c.dataset.metric, label: c.getAttribute('aria-label'), gl: ['better','worse','tie','open','one'].map(k => c.querySelectorAll('path.gl.' + k).length) }))""")
        seen = set()
        for c in cells:
            seen.add((c['arm'], c['metric']))
            vs = [V[(c['arm'], g, c['metric'])] for g in REG_ORDER if (c['arm'], g, c['metric']) in V and V[(c['arm'], g, c['metric'])]['kind'] != 'na']
            cnt = collections.Counter(v[rope] for v in vs)
            exp = f'{LNAME[c["arm"]]}, {MET[c["metric"]]["short"]}: ' + (f'better in {cnt["better"]}, worse in {cnt["worse"]}, tie in {cnt["tie"]}, not settled in {cnt["open"]}, one seed in {cnt["one"]} of {len(vs)} regimes' if vs else 'no results yet')
            if c['label'] != exp: bad_st.append(f'{c["arm"]}/{c["metric"]}@{rope}: "{c["label"]}" expected "{exp}"')
            if vs and c['gl'] != [cnt['better'], cnt['worse'], cnt['tie'], cnt['open'], cnt['one']]: bad_st.append(f'{c["arm"]}/{c["metric"]}@{rope}: glyph counts b/w/t/o/1 {c["gl"]} expected {[cnt["better"], cnt["worse"], cnt["tie"], cnt["open"], cnt["one"]]}')
        exp_cells = {(a, m) for a in CONTENDERS for m in MET_ORDER if present_regimes(m)}
        if seen != exp_cells: bad_st.append(f'@{rope}: strips missing {sorted(exp_cells - seen)[:3]} extra {sorted(seen - exp_cells)[:3]}')
    fails_to_rec('A4.supertable-strip-counts-and-glyphs', scope, bad_st, 'per arm x metric x tie band')

def check_progress(S, scope):
    S.pg.evaluate(JS_HELPERS)
    P = py_progress()
    txt = S.pg.evaluate("document.getElementById('g3-status').innerText").replace(' ', ' ')
    m = re.match(r'^(\d[\d,]*) of (\d[\d,]*) runs in, newest run (.+?) \((\d+) invalid\): (\d[\d,]*) of (\d[\d,]*) baseline and (\d[\d,]*) of (\d[\d,]*) improved\. (\d+) of (\d+) regimes (?:has|have) every design and variant at (\d+)\+ seeds(?:; (\d+) (?:has|have) no runs yet, so nothing below speaks to (?:it|them))?\.$', txt.strip())
    bad = []
    if not m: bad.append(f'sentence shape: "{txt}"')
    else:
        g = m.groups(); toi = lambda s: int(s.replace(',', ''))
        exp = (P['done'], P['planned'], None, P['invalid'], P['base_done'], P['base_planned'], P['imp_done'], P['imp_planned'], P['settled'], P['regimes'], 2, P['empty'] or None)
        for k, (a, b) in enumerate(zip(g, exp)):
            if k == 2: continue
            if (toi(a) if a is not None else None) != b: bad.append(f'field{k}: seen {a} expected {b}')
        age_txt = g[2]
        now = datetime.datetime.now(datetime.timezone.utc)
        mins = (now - datetime.datetime.fromisoformat(P['stamp'])).total_seconds() / 60
        exp_age = 'less than a minute ago' if mins < 1 else f'{int(mins)} min ago' if mins < 90 else f'{round(mins / 60)} h ago' if mins / 60 < 48 else f'{round(mins / 1440)} days ago'
        m2 = re.match(r'(\d+) (min|h|days) ago', age_txt)
        if m2:
            sec = {'min': 1, 'h': 60, 'days': 1440}[m2.group(2)]; seen_min = int(m2.group(1)) * sec
            if abs(seen_min - mins) > sec + 1.5: bad.append(f'age "{age_txt}" vs newest run_at {P["stamp"]} ({mins:.0f} min ago; expected ~"{exp_age}")')
        elif age_txt != exp_age: bad.append(f'age "{age_txt}" vs expected "{exp_age}" (stamp {P["stamp"]})')
    rec('A4.progress-sentence', scope, not bad, f'selector=#g3-status | seen="{txt[:140]}" | expected done={P["done"]} planned={P["planned"]} invalid={P["invalid"]} settled={P["settled"]}/{P["regimes"]} empty={P["empty"]} | issues: {bad}' if bad else f'"{txt[:110]}"')
    prog = S.pg.evaluate("[...document.querySelectorAll('#g3-prog button')].map(b => [b.dataset.fk, b.querySelector('.n').textContent, b.querySelector('.bar i').style.width])")
    bad2 = []
    seen_r = set()
    for fk, n, w in prog:
        g = fk.split('|')[1]; seen_r.add(g)
        exp = f'{P["per_done"].get(g, 0)}/{P["per_planned"]}'
        if n != exp: bad2.append(f'{g}: seen "{n}" expected "{exp}"')
        ew = 100 * P['per_done'].get(g, 0) / P['per_planned']
        if abs(float(w.rstrip('%')) - ew) > 0.01: bad2.append(f'{g}: bar width {w} expected {ew:.2f}%')
    if seen_r != {g['regime'] for g in REGS}: bad2.append(f'regimes in strip {sorted(seen_r)} vs {len(REGS)}')
    order = [fk.split('|')[1] for fk, _, _ in prog]
    if order != REG_ORDER: bad2.append(f'strip order {order} vs group-then-order {REG_ORDER}')
    fails_to_rec('A4.progress-strip-per-regime-done-planned', scope, bad2, f'{len(prog)} regimes')

def check_panes(S, scope):
    S.pg.evaluate(JS_HELPERS)
    bad, bad_p, bad_c1, bad_c3, bad_neg, bad_own = [], [], [], [], [], []
    seeds = collections.defaultdict(list)
    for s in SEEDS:
        if s['valid'] in (True, 'true'): seeds[(s['arm'], s['regime'], s['metric'])].append((s['seed'], s['value']))
    picked = []
    for metric in MET_ORDER:
        seen_groups = set(); cnt = 0
        for a in CONTENDERS:
            for g in present_regimes(metric):
                v = V.get((a, g, metric))
                if not v or v['kind'] == 'na': continue
                key = (v['kind'], v['scale'], v[0.05], v['cens'], v['lo'] is None, v['mzero_all'], v['zero_all'])
                if key in seen_groups and not (metric in METRICS6 and cnt < 16): continue
                seen_groups.add(key); picked.append((metric, a, g)); cnt += 1
    n = 0
    for metric, a, g in picked:
        v = V[(a, g, metric)]; unit = MET[metric]['unit']
        S.params(metric=metric, arm=a, regime=g, rope='0.05')
        p = S.pg.evaluate(JS_PANE); n += 1
        tag = f'{metric} {a}/{g}'
        exp_h3 = f'{LNAME[a]} · {MET[metric]["short"]} · {g}'
        if not p['h3'] or p['h3'][0] != exp_h3: bad.append(f'{tag}: pane h3 {p["h3"][:1]} expected "{exp_h3}"')
        if REG_DESC[g] not in p['notes']: bad.append(f'{tag}: regime description missing from pane')
        if not p['verdict']: bad.append(f'{tag}: no verdict sentence'); continue
        first = re.match(r'^(Better\.|Tie\.|Worse\.|Not settled\.|One seed, no interval yet\.)', p['verdict'][0])
        if not first or KEY_STATE[first.group(1)] != v[0.05]: bad.append(f'{tag}: verdict sentence "{p["verdict"][0][:50]}" vs expected {v[0.05]}')
        rows = dict(zip(p['dts'], p['dds']))
        for label, mean, cri, cens, zero, nn, tt in ((SNAME[a], v['mean'], v['cri'], v['cens'], v['zero_all'], v['n'], v['ntot']), ('Main', v['mmean'], v['mcri'], v['mcens'], v['mzero_all'], v['mn'], v['mntot'])):
            dd = rows.get(label)
            if dd is None: bad_p.append(f'{tag}: pane row "{label}" missing'); continue
            for e in check_abs(dd, mean, cri, cens, zero, unit, nn, tt): bad_p.append(f'{tag}: {label}: {e}')
        lab = 'Paired ratio' if (v['kind'] == 'ratio' and not has_diff(v)) else 'Against main'
        dd = rows.get(lab)
        if dd is None: bad_p.append(f'{tag}: row "{lab}" missing')
        else:
            for e in check_full(dd, v, unit): bad_p.append(f'{tag}: {lab}: {e}')
        pp = rows.get('P(better than main)')
        if pp != fmt_p(v['p']): bad_p.append(f'{tag}: P seen "{pp}" expected "{fmt_p(v["p"])}"')
        for arm_, lab_, mean_ in ((v['sol'], f'{SNAME[a]} seeds', v['mean']), ('main', 'Main seeds', v['mmean'])):
            exp = [x[1] for x in sorted(seeds[(arm_, g, metric)])]
            txt = rows.get(lab_)
            if not exp:
                if txt != '—': bad_p.append(f'{tag}: {lab_} "{txt}" expected "—"')
                continue
            k, sfx = unit_scale(unit, mean_ or max(exp) or 0)
            if not txt or not txt.endswith(sfx): bad_p.append(f'{tag}: {lab_} "{txt}" lacks unit "{sfx}"'); continue
            got = [t.strip() for t in txt[:len(txt) - len(sfx)].split(', ')]
            if len(got) != len(exp): bad_p.append(f'{tag}: {lab_} count {len(got)} expected {len(exp)} ("{txt}")')
            else:
                if len({len(x.split('.')[1]) if '.' in x else 0 for x in got}) != 1: bad_p.append(f'{tag}: {lab_} mixed precision "{txt}"')
                for gt, ev in zip(got, exp):
                    if not dec_ok(gt, ev, k): bad_p.append(f'{tag}: {lab_} {gt} vs {ev / k:.6g}{sfx}')
        bad_own += self_pane_rows(rows, a, g, metric, unit, tag)
        for lab_, dd in rows.items():
            if lab_.startswith('P(') or lab_.endswith(' seeds') or lab_ == 'Main seeds': continue     # posterior probabilities and raw seed values
            if re.search(r'\d', dd) and not re.search(r'\[|±|every paired seed|no interval yet|every seed|ratio of means|difference', dd):
                bad_c1.append(f'{tag}: pane "{lab_}" = "{dd}" has no interval or wording')
        mneg = re.search(r'.{0,20}[\[(]\s*[-−]\d.{0,15}', ' '.join(x for x in p['dds'] if 'difference' not in x))
        if mneg: bad_neg.append(f'{tag}: pane has a negative bound: {mneg.group(0)}')
        for z in p['zooms']:
            for r in z['rows']:
                if r['w'] < 20 - 0.5: bad_c3.append(f'{tag}: svg[{z["aria"][:36]}] {r["cls"]} bar {r["w"]:.1f}px')
    fails_to_rec('A4.pane-readout-vs-posterior-and-seeds', scope, bad + bad_p, f'{n} selected cells across all 14 metrics: arm/main mean [CrI], ratio [CrI], P, seeds, verdict word, heading')
    fails_to_rec('C1.pane-numbers-followed-by-interval', scope, bad_c1, f'{n} panes')
    fails_to_rec('C3.pane-zoomed-interval-bars-at-least-20px', scope, bad_c3, f'{n} panes')
    fails_to_rec('C4.pane-no-negative-lower-bound', scope, bad_neg, f'{n} panes')
    if CUR_VAR != 'baseline': fails_to_rec('A4.pane-paired-against-own-design', scope, bad_own, f'{n} panes: vs own baseline and per-key fix alone, ratio [CrI], verdict, P(better), from self_posterior.json')

ERROR_METRICS = {m for m in MET_ORDER if MET[m]['error'] in (True, 'true')}
def self_score_cell(txt, arm, rope):
    """The scorecard's vs-own-baseline cell, recomputed from self_posterior.json."""
    sol = SOLMAP[CUR_VAR][arm]
    cells = [v for (c_, s_, g_, m_), v in SELF_V.items() if c_ == 'vs_own_baseline' and s_ == sol]
    if not cells: return [] if txt.strip() == '—' else [f'{arm}@{rope}: "{txt}" expected — (no paired run against its own baseline)']
    bad = []; t = txt.replace('\n', ' ').strip()
    by = sorted(v['ratio'] for v in cells if v['metric'] == 'bytes_total' and v['paired'] and v['lo'] is not None)
    err = [v for v in cells if v['metric'] in ERROR_METRICS and v['kind'] != 'na']
    nb, nw = sum(v[rope] == 'better' for v in err), sum(v[rope] == 'worse' for v in err)
    m = re.match(r'^(?:×(\S+) bytes|bytes: no interval yet)\s*errors ▼(\d+) ▲(\d+)$', t)
    if not m: return [f'{arm}@{rope}: vs-own-baseline cell shape "{t}"']
    if by:
        med = by[len(by) // 2] if len(by) % 2 else (by[len(by) // 2 - 1] + by[len(by) // 2]) / 2
        if not m.group(1) or not sig_ok(m.group(1), med, 2 if med >= 1000 else 3 if med >= 1 else 2): bad.append(f'{arm}@{rope}: own-baseline bytes {m.group(1)} vs median {med:.4g}')
    elif m.group(1): bad.append(f'{arm}@{rope}: own-baseline bytes printed without an interval-backed comparison')
    if (int(m.group(2)), int(m.group(3))) != (nb, nw): bad.append(f'{arm}@{rope}: own-baseline errors ▼{m.group(2)} ▲{m.group(3)} expected ▼{nb} ▲{nw}')
    return bad

WORD_KEY = {'better': 'better', 'tie': 'tie', 'worse': 'worse', 'not settled': 'open', 'one seed, no interval yet': 'one'}
def self_pane_rows(rows, a, g, metric, unit, tag):
    """Improved views: the arm paired against its own design (vs_own_baseline) and the per-key fix alone (perkey_effect), from self_posterior.json."""
    if CUR_VAR == 'baseline': return []
    bad = []
    for cmp, sol, lab_of, plab in (('vs_own_baseline', SOLMAP[CUR_VAR][a], lambda v: f'Against its own baseline ({v["against"]})', 'P(better than its baseline)'),
                                   ('perkey_effect', SOLMAP['improved'][a], lambda v: f'The per-key fix alone ({v["arm"]} against {v["against"]})', 'P(the per-key fix helps)')):
        v = SELF_V.get((cmp, sol, g, metric)) if sol else None
        if not v or v['kind'] == 'na':
            if v and lab_of(v) in rows: bad.append(f'{tag}: "{lab_of(v)}" shown for an unmeasured comparison')
            continue
        lab = lab_of(v)
        if lab not in rows: bad.append(f'{tag}: no "{lab}" row'); continue
        m = re.match(r'^(.*) · (better|tie|worse|not settled|one seed, no interval yet)$', rows[lab])
        if not m: bad.append(f'{tag}: {lab}: shape "{rows[lab]}"'); continue
        bad += [f'{tag}: {lab}: {e}' for e in check_full(m.group(1), v, unit)]
        if WORD_KEY[m.group(2)] != v[0.05]: bad.append(f'{tag}: {lab}: verdict {m.group(2)} expected {v[0.05]}')
        if rows.get(plab) != fmt_p(v['p']): bad.append(f'{tag}: {plab} "{rows.get(plab)}" expected "{fmt_p(v["p"])}"')
    return bad

def check_main_missing(S, scope):
    S.pg.evaluate(JS_HELPERS)
    bad = []
    S.params(metric='part_time_to_heal_ms', regime='az-partition', arm='az-hier', rope='0.05')
    miss = [(g, [V[(a, g, 'part_time_to_heal_ms')] for a in CONTENDERS if (a, g, 'part_time_to_heal_ms') in V and V[(a, g, 'part_time_to_heal_ms')]['main_missing']]) for g in REG_ORDER]
    miss = [(g, l) for g, l in miss if l]
    if not miss and CUR_VAR == 'baseline': bad.append('data has no main-missing regime for part_time_to_heal_ms; check is vacuous')
    note = S.pg.evaluate("document.getElementById('g3-hub-missing').innerText").replace(' ', ' ')
    pane = S.pg.evaluate(JS_PANE)['text'].replace(' ', ' ')
    for g, l in miss:
        want_ = f'Baseline main has no valid time to heal in {g}'
        if want_ not in note: bad.append(f'#g3-hub-missing lacks "{want_}" (seen "{note[:110]}")')
        if want_ not in pane: bad.append(f'#g3-pane (regime selected) lacks "{want_}" (seen "{pane[:110]}")')
        lst = note.split('Arms that have one: ', 1)[1] if 'Arms that have one: ' in note else ''
        entries = {}
        for part in re.split(r'; |\. (?=[A-Z]|$)', lst):
            if ': ' in part: k_, _, val_ = part.partition(': '); entries[k_.strip()] = val_.strip().rstrip('.')
        for v in l:
            name = LNAME[v['arm']]
            if name not in entries: bad.append(f'#g3-hub-missing lacks an entry for "{name}" (entries {list(entries)[:4]})'); continue
            txt = entries[name]
            has_iv = v['cri'][0] is not None and v['cri'][1] is not None
            mm = re.match(r'^(.*?) \((\d+) seeds?\)$', txt) if has_iv else None
            if has_iv and (not mm or int(mm.group(2)) != v['n']): bad.append(f'note entry for {v["arm"]}: "{txt}" expected the value then "({v["n"]} seeds)"'); continue
            for e in check_abs(mm.group(1) if has_iv else txt, v['mean'], v['cri'], False, False, 'ms', v['n'], v['ntot']): bad.append(f'note value for {v["arm"]}: {e}')
    fails_to_rec('A4.main-has-no-value-message', scope, bad, f'regimes: {[g for g, _ in miss]}')

def check_forest_geometry(S, scope, vp):
    S.pg.evaluate(JS_HELPERS)
    seedr = collections.defaultdict(list)
    mainv = {(s['regime'], s['metric'], s['seed']): s['value'] for s in SEEDS if s['arm'] == 'main' and s['valid'] in (True, 'true')}
    for s in SEEDS:
        if s['arm'] == 'main' or s['valid'] not in (True, 'true'): continue
        mv = mainv.get((s['regime'], s['metric'], s['seed']))
        if mv and mv > 0 and s['value'] and s['value'] > 0: seedr[(s['arm'], s['regime'], s['metric'])].append(s['value'] / mv)
    bad_seed, bad_mt, bad_iv, bad_c2, bad_one, bad_off, bad_band, bad_tick, bad_cap, bad_occ, bad_offiv = [], [], [], [], [], [], [], [], [], [], []
    nseed = ncell = ntick = 0
    rope = 0.05; t = math.log10(1 + rope)
    for metric in MET_ORDER:
        S.params(metric=metric, rope=str(rope))
        F = S.pg.evaluate(JS_FOREST)
        if F is None: continue
        ci = F['colInfo']; scale = {}
        for j, c in ci.items():
            if c['ref'] is None or not c['band']: bad_tick.append(f'{metric}/{c["regime"]}: no ×1 line or tie band drawn'); continue
            ppd = (c['band']['r'] - c['band']['l']) / (2 * t)
            scale[j] = ppd
            if abs((c['band']['l'] + c['band']['r']) / 2 - c['ref']) > 1.0: bad_band.append(f'{metric}/{c["regime"]}: band centre {(c["band"]["l"] + c["band"]["r"]) / 2:.1f} not on the x1 line {c["ref"]:.1f}')
            for lab in c['labels']:
                if lab['l'] <= c['left'] + 2.5 or lab['r'] >= c['right'] - 2.5: continue   # clamped inside the column (the page keeps 2 px)
                near = min(c['grid'] + [c['ref']], key=lambda x: abs(x - lab['x']))
                if abs(near - lab['x']) > 0.6: continue          # label nudged to stay inside the column
                val = float(lab['text'][1:-1]) * 1000 if lab['text'].endswith('k') else float(lab['text'][1:])
                exp = c['ref'] + math.log10(val) * ppd
                ntick += 1
                if abs(exp - near) > 1.5: bad_tick.append(f'{metric}/{c["regime"]}: tick label {lab["text"]} at line x={near:.1f}, scale from tie band puts it at {exp:.1f}')
        for d in F['data']:
            v = V.get((d['arm'], d['regime'], metric)); j = str(d['j']); c = ci.get(j)
            if not v or v['kind'] != 'ratio' or c is None or j not in scale or d['mt'] is None: continue
            ncell += 1; ppd = scale[j]
            xof = lambda lr: c['ref'] + lr * ppd
            kinds_m = {V[(a_, g_, metric)]['kind'] for a_ in CONTENDERS for g_ in present_regimes(metric) if (a_, g_, metric) in V}
            zl = 20 if 'zero' in kinds_m else 0; ml = 20 if 'mainzero' in kinds_m else 0      # lane + gap, page geometry
            inner = (c['left'] + 10 + zl, c['right'] - 10 - ml)       # the drawn axis range: COL_PAD from each column edge
            clamp = lambda x: max(inner[0], min(inner[1], x))
            lr = math.log10(v['ratio']); exp = xof(lr); tag = f'{metric} {d["arm"]}/{d["regime"]}'
            in_inner = inner[0] - 0.5 <= exp <= inner[1] + 0.5
            txt = ' '.join(d['texts'])
            if in_inner:
                if abs(d['mt'] - exp) > 1.2: bad_mt.append(f'{tag}: mean tick x {d["mt"]:.1f} expected {exp:.1f} (ratio {v["ratio"]:.4g})')
            else:
                if abs(d['mt'] - clamp(exp)) > 1.2: bad_mt.append(f'{tag}: off-axis mark at {d["mt"]:.1f}, expected pinned to the edge {clamp(exp):.1f}')
                if not re.search(r'[→←]', txt): bad_off.append(f'{tag}: ratio {v["ratio"]:.3g} is beyond the axis but has no arrow note; texts={d["texts"]}')
                if v['paired'] and v['lo'] is not None and not is_exact(v) and not re.search(r'±|\[', txt): bad_offiv.append(f'{tag}: off-axis note drops the interval (half-width {100 * (v["hi"] / v["ratio"] - 1):.2g} %): texts={d["texts"]}')
            if abs(d['gl'] - d['mt']) > 0.6: bad_mt.append(f'{tag}: verdict mark x {d["gl"]:.1f} not over the mean tick {d["mt"]:.1f}')
            allseeds = seedr[(SOLMAP[CUR_VAR][d['arm']], d['regime'], metric)]
            exp_x = [xof(math.log10(r)) for r in allseeds]
            need = sum(1 for x in exp_x if inner[0] + 3 <= x <= inner[1] - 3)
            nseed += need
            if len(d['seeds']) < need: bad_seed.append(f'{tag}: {len(d["seeds"])} seed ticks, expected at least {need}')
            for gx in d['seeds']:
                if not any(abs(gx - ex) <= 1.2 for ex in exp_x): bad_seed.append(f'{tag}: seed tick at {gx:.1f} matches no seed ratio (expected {sorted(round(e, 1) for e in exp_x)})')
            if v['lo'] is not None and not is_exact(v) and d['ivl'] is not None and in_inner:
                lo_x, hi_x = xof(math.log10(v['lo'])), xof(math.log10(v['hi']))
                for got, ex, nm in ((d['ivl']['l'], clamp(lo_x), 'lo'), (d['ivl']['r'], clamp(hi_x), 'hi')):
                    if abs(got - ex) > 1.5 and not (nm == 'hi' and d['ivl']['r'] - d['ivl']['l'] <= 1.5): bad_iv.append(f'{tag}: interval {nm} end at {got:.1f} expected {ex:.1f}')
                ncap = sum(1 for x in (lo_x, hi_x) if inner[0] <= x <= inner[1])
                if len(d['caps']) != ncap: bad_cap.append(f'{tag}: {len(d["caps"])} caps drawn, {ncap} interval ends are on the axis')
            # C2: interval visible: line >= 6 px, or a cap pair plus a "±x %" label; never occluded by the verdict mark
            if v['paired'] and v['lo'] is not None and not is_exact(v) and in_inner:
                L = (d['ivl']['r'] - d['ivl']['l']) if d['ivl'] else 0.0
                # the printed interval follows the compact rule: ±x % up to a 50 % half-width, [lo, hi] beyond it
                hw_ = v['hi'] / v['ratio'] - 1
                has_pm = ('±' in txt) if hw_ <= 0.5 else bool(re.search(r'\[[\d.,k]+, [\d.,k]+\]', txt))
                if not (L >= 6 or (len(d['caps']) >= 1 and has_pm)):
                    bad_c2.append(f'{tag}: interval line {L:.1f}px (half-width {100 * hw_:.2g} %), caps={len(d["caps"])}, printed interval ({"±" if hw_ <= 0.5 else "[lo, hi]"})={has_pm}, texts={d["texts"]}')
                if d['ivl'] and d['glB'] > d['ivl']['t'] + 0.5 and d['glR'] > d['ivl']['l'] and d['glL'] < d['ivl']['r']:
                    bad_occ.append(f'{tag}: verdict glyph bottom {d["glB"]:.1f} overlaps the interval line top {d["ivl"]["t"]:.1f}')
            if v[0.05] == 'one' and v['kind'] == 'ratio':
                if not re.search(r'seed', txt): bad_one.append(f'{tag}: one-seed cell has no "seed" wording on screen (texts={d["texts"]}); aria says "{d["label"][:60]}"')
    fails_to_rec('A4.axis-tick-labels-match-tie-band-scale', scope, bad_tick + bad_band, f'{ntick} unnudged tick labels, x1 line and tie band consistent in every column')
    fails_to_rec('A4.seed-ticks-match-seed-ratios', scope, bad_seed, f'{nseed} in-axis seed ticks inverted through the drawn axis')
    fails_to_rec('A4.mean-tick-and-verdict-mark-position-match-ratio', scope, bad_mt, f'{ncell} ratio cells')
    fails_to_rec('A4.interval-ends-match-ratio_lo-hi', scope, bad_iv + bad_cap, 'interval line ends and caps vs posterior ratio_lo/ratio_hi')
    fails_to_rec('C1.off-axis-note-keeps-the-interval', scope, bad_offiv, 'cells pinned to the axis edge: is the interval still printed?')
    fails_to_rec('A4.off-axis-cells-labelled-with-arrow', scope, bad_off, 'marks pinned to the axis edge carry an arrow note')
    fails_to_rec('C2.forest-interval-visible (line >= 6 px, or caps + printed interval)', scope, bad_c2, f'{ncell} forest cells (n>=2 with an interval)')
    fails_to_rec('C2.forest-interval-not-occluded-by-verdict-mark', scope, bad_occ, 'glyph vs interval bbox')
    fails_to_rec('C1.one-seed-cells-say-so-in-words', scope, bad_one, 'one-seed forest cells')

def check_hover_targets(S, scope):
    S.pg.evaluate(JS_HELPERS)
    bad = []; n = 0
    for metric in METRICS6:
        S.params(metric=metric)
        r = S.pg.evaluate("""() => { const out = []; let n = 0; for (const c of document.querySelectorAll('#g3-hub-plot .cell')) { c.scrollIntoView({ block: 'center', inline: 'center' }); const f = c.querySelector('rect.focus').getBoundingClientRect(); const x = (f.left + f.right) / 2, y = (f.top + f.bottom) / 2;
            const e = document.elementFromPoint(x, y); n++; if (!e || e.closest('.cell') !== c) out.push(c.dataset.arm + '/' + c.dataset.regime + ' centre hits ' + (e ? e.tagName + '.' + (e.getAttribute('class') || '') : 'nothing')); } return { n, out }; }""")
        n += r['n']; bad += [f'{metric} {x}' for x in r['out']]
    fails_to_rec('A6.cell-centre-reaches-the-cell-not-a-grid-line', scope, bad, f'{n} cell centres hit-tested')

def check_tip(text, v, unit, arm_name):
    """Tooltip text of a ratio cell vs posterior (full-form intervals, P, seeds)."""
    bad = []; lines = [l for l in text.split('\n') if l.strip()]
    def line(prefix):
        return next((l[len(prefix):] for l in lines if l.startswith(prefix)), None)
    vm = line('vs main ')
    if vm is None: bad.append(f'no "vs main" line in {lines[:4]}')
    else: bad += check_full(vm, v, unit)
    al = next((l[len(arm_name) + 1:] for l in lines[1:] if l.startswith(arm_name + ' ') and not l.startswith(arm_name + ' ·')), None)
    if al is None: bad.append(f'no "{arm_name} ..." line')
    else: bad += [f'{arm_name} line: {e}' for e in check_abs(al, v['mean'], v['cri'], v['cens'], v['zero_all'], unit, v['n'], v['ntot'])]
    ml = line('main ')
    if ml is None: bad.append('no "main ..." line')
    else: bad += [f'main line: {e}' for e in check_abs(ml, v['mmean'], v['mcri'], v['mcens'], v['mzero_all'], unit, v['mn'], v['mntot'])]
    pl = line('P(better) ')
    if pl != fmt_p(v['p']): bad.append(f'P seen "{pl}" expected "{fmt_p(v["p"])}"')
    sl = line('seeds ')
    if sl != f'{v["n"]} arm, {v["mn"]} main': bad.append(f'seeds line "{sl}" expected "{v["n"]} arm, {v["mn"]} main"')
    return bad

def check_label_c1(S, scope):
    S.pg.evaluate(JS_HELPERS)
    bad = []; n = 0; bad_t = []; tipped = set(); nt = 0
    for metric in MET_ORDER:
        S.params(metric=metric, rope='0.05')
        cells = hub_cells(S); stride = max(1, len(cells) // 24)
        for i, c in enumerate(cells):
            tag = f'{metric} {c["arm"]}/{c["regime"]}'
            _, body = parse_label(c['label'], LNAME[c['arm']])
            head = body.split(' of main')[0]
            if head.startswith('×'):
                n += 1
                if not re.match(r'^×\S+ (\[|on every|\(|±)', head): bad.append(f'{tag}: "{head}"')
            if verdict_from_label(body) == 'one' and 'seed' not in body: bad.append(f'{tag}: one-seed label "{body}" lacks "seed"')
            # Tooltips: the first cell of each vs-main form (kind, interval or not, difference or not) in every metric,
            # plus a stride through bytes_total, so every branch of the full form is read back at every unit.
            v = V.get((c['arm'], c['regime'], metric))
            if not v or v['kind'] == 'na': continue
            form = (metric, v['kind'], v['lo'] is None, has_diff(v))
            if form in tipped and not (metric == 'bytes_total' and i % stride == 0): continue
            tipped.add(form); nt += 1
            try:
                S.pg.locator('#g3-hub-plot .cell').nth(i).hover(force=True, timeout=3000); S.pg.wait_for_timeout(40)
                bad_t += [f'{tag}: {e}' for e in check_tip(S.pg.evaluate("document.querySelector('.tip').innerText"), v, MET[metric]['unit'], SNAME[c['arm']])]
            except Exception as e:
                bad_t.append(f'{tag}: hover failed: {str(e)[:80]}')
    fails_to_rec('C1.aria-label-ratio-followed-by-interval', scope, bad, f'{n} labels')
    fails_to_rec('C1.tooltip-numbers-and-intervals', scope, bad_t, f'{nt} tooltips covering {len(tipped)} metric/vs-main forms: ratio, [CrI], P, seeds vs posterior')
    S.pg.mouse.move(0, 0)
    if CUR_VAR != 'baseline':
        bad_d = []; nd = 0
        for metric in ('bytes_total', 'conv_p99_incl_never_ms', 'stale_limited_p99_pct'):
            S.params(metric=metric, rope='0.05')
            for c in S.pg.evaluate("[...document.querySelectorAll('#g3-hub-plot .cell')].map(c => ({ arm: c.dataset.arm, regime: c.dataset.regime, d: c.getAttribute('aria-description') }))"):
                v = SELF_V.get(('vs_own_baseline', SOLMAP[CUR_VAR][c['arm']], c['regime'], metric)); main_v = V.get((c['arm'], c['regime'], metric))
                want_ = v is not None and v['kind'] != 'na' and main_v is not None and main_v['kind'] != 'na'
                if not want_:
                    if c['d']: bad_d.append(f'{metric} {c["arm"]}/{c["regime"]}: aria-description "{c["d"][:60]}" without a paired comparison')
                    continue
                nd += 1
                m = re.match(r'^Against its own baseline \(([^)]+)\): (.*), (better|tie|worse|not settled|one seed, no interval yet)$', c['d'] or '')
                if not m: bad_d.append(f'{metric} {c["arm"]}/{c["regime"]}: aria-description "{c["d"]}"'); continue
                if m.group(1) != v['against']: bad_d.append(f'{metric} {c["arm"]}/{c["regime"]}: against {m.group(1)} expected {v["against"]}')
                bad_d += [f'{metric} {c["arm"]}/{c["regime"]}: {e}' for e in check_full(m.group(2), v, MET[metric]['unit'])]
                if WORD_KEY[m.group(3)] != v[0.05]: bad_d.append(f'{metric} {c["arm"]}/{c["regime"]}: verdict {m.group(3)} expected {v[0.05]}')
        fails_to_rec('C1.aria-description-vs-own-baseline', scope, bad_d, f'{nd} cells')

ROWS_SEEN = {}          # variant -> the row names every view showed, for the cross-variant identity check
def check_order(S, scope):
    S.pg.evaluate(JS_HELPERS)
    exp_names = [DNAME[a] for a in ARM_ORDER]      # every arm is a row in every variant; one without the variant says why
    bad = []; views = 0
    js = """() => {
      const ys = (sel) => [...document.querySelectorAll(sel)].map(t => [t.getBoundingClientRect().top, t.textContent]).sort((a, b) => a[0] - b[0]).map(x => x[1]);
      const hubLabels = ys('#g3-hub-labels svg text.t-body, #g3-hub-labels svg text.t-cap').filter(t => !/^BY REGIME$/.test(t));
      const hubRows = [...new Set([...document.querySelectorAll('#g3-hub-plot .cell')].sort((a, b) => +a.dataset.rc.split(',')[0] - +b.dataset.rc.split(',')[0]).map(c => c.dataset.arm))];
      const score = [...document.querySelectorAll('#g3-score [data-fk^="score|"] .name')].map(t => t.firstChild.textContent);
      const supers = [...document.querySelectorAll('#g3-super svg')].filter(s => s.querySelector('.cell')).map(s => [...s.querySelectorAll('text.t-body')].map(t => [t.getBoundingClientRect().top, t.textContent]).sort((a, b) => a[0] - b[0]).map(x => x[1]));
      const superRows = [...document.querySelectorAll('#g3-super svg')].filter(s => s.querySelector('.cell')).map(s => [...new Set([...s.querySelectorAll('.cell')].sort((a, b) => +a.dataset.rc.split(',')[0] - +b.dataset.rc.split(',')[0]).map(c => c.dataset.arm))]);
      const fams = ys('#g3-hub-labels svg text.t-head').filter(t => !/^BY REGIME$/.test(t));
      const ids = (sel) => [...document.querySelectorAll(sel)].map(t => [t.getBoundingClientRect().top, t.dataset.arm]).sort((a, b) => a[0] - b[0]).map(x => x[1]);
      const rowIds = { hub: ids('#g3-hub-labels svg text[data-arm]'), score: [...document.querySelectorAll('#g3-score [data-fk^="score|"]')].map(r => r.dataset.fk.split('|')[1]),
        supers: [...document.querySelectorAll('#g3-super svg')].filter(s => s.querySelector('.cell')).map(s => [...s.querySelectorAll('text[data-arm]')].map(t => [t.getBoundingClientRect().top, t.dataset.arm]).sort((a, b) => a[0] - b[0]).map(x => x[1])) };
      const subs = [...document.querySelectorAll('#g3-hub-labels svg text.t-note')].map(t => t.textContent);
      const reasons = [...document.querySelectorAll('#g3-hub-plot svg text')].filter(t => !t.closest('.cell') && !t.closest('.hdr') && t.textContent.startsWith('— ')).map(t => t.textContent);
      return { hubLabels, hubRows, score, supers, superRows, fams, reasons, rowIds, subs };
    }"""
    exp_fams = list(dict.fromkeys(ARM_FAM[a] for a in ARM_ORDER))
    def chk(tag):
        nonlocal views
        o = S.pg.evaluate(js); views += 1
        ri = o['rowIds']
        if ri['hub']: ROWS_SEEN.setdefault((S.width, CUR_VAR), set()).add((tuple(ri['hub']), tuple(ri['score']), tuple(tuple(x) for x in ri['supers'])))
        if ri['score'] != ARM_ORDER: bad.append(f'{tag}: scorecard row ids {ri["score"]}')
        if ri['hub'] and ri['hub'] != ARM_ORDER: bad.append(f'{tag}: hub row ids {ri["hub"]}')
        exp_subs = [VAR_SUB[CUR_VAR] for a in CONTENDERS] if CUR_VAR != 'baseline' else []
        if ri['hub'] and o['subs'] != exp_subs: bad.append(f'{tag}: hub sub-labels {o["subs"]} expected {exp_subs}')
        missing = sorted(o['reasons']); exp_missing = sorted(f'— {reason_for(CUR_VAR, a)}' for a in ARM_ORDER if a not in CONTENDERS) if o['hubRows'] else []
        if missing != exp_missing: bad.append(f'{tag}: hub reason rows {missing} expected {exp_missing}')
        none_present = not o['hubRows'] and not o['hubLabels']
        if not none_present and o['hubLabels'] != exp_names: bad.append(f'{tag}: hub labels {o["hubLabels"]}')
        if o['hubRows'] and o['hubRows'] != CONTENDERS: bad.append(f'{tag}: hub rows {o["hubRows"]}')
        if o['score'] != exp_names: bad.append(f'{tag}: scorecard rows {o["score"]}')
        for k, s in enumerate(o['supers']):
            if s != exp_names: bad.append(f'{tag}: supertable band {k} names {s}')
        for k, s in enumerate(o['superRows']):
            if s != CONTENDERS: bad.append(f'{tag}: supertable band {k} rows {s}')
        if not none_present and [f.lower() for f in o['fams']] != [f.lower() for f in exp_fams]: bad.append(f'{tag}: hub family labels {o["fams"]} expected {exp_fams}')
    for metric in MET_ORDER:
        S.params(metric=metric, rope='0.05'); chk(f'metric={metric}')
    S.params(metric='bytes_total')
    for rp in ROPES:
        S.params(rope=str(rp)); chk(f'rope={rp}')
    for g in REG_ORDER:
        S.params(regime=g); chk(f'regime={g}')
    for a in ARM_ORDER:
        S.params(arm=a); chk(f'arm={a}')
    for metric in ('conv_p99_incl_never_ms', 'part_time_to_heal_ms'):
        for g in ('az-partition', 'idle'):
            S.params(metric=metric, regime=g, rope='0.25'); chk(f'combo {metric}/{g}/0.25')
    S.params(plane='all'); chk('plane=all'); S.params(plane='one')
    rec('A5.arm-order-equals-arms.json', scope, not bad, f'selector=#g3-hub-labels/#g3-hub-plot/#g3-score/#g3-super | {views} views | seen={bad[:3]} | expected={exp_names}' if bad else f'{views} views identical to arms.json order (+family grouping, variant row names and sub-labels)')

def check_rows_identical_across_variants():
    for w in sorted({w for w, _ in ROWS_SEEN}):
        flat = {v: ROWS_SEEN.get((w, v), set()) for v in VARIANTS}
        ok = all(len(s_) == 1 for s_ in flat.values()) and len({next(iter(s_)) for s_ in flat.values()}) == 1
        rec('A5.rows-identical-across-variants', f'{w}px', ok, f'hub, scorecard and supertable row identities (data-arm): {[(v, len(s_)) for v, s_ in flat.items()]} distinct row sets per variant (expected one, the same, for every variant)' if not ok else f'{len(VARIANTS)} variants, every view: one row order {ARM_ORDER}')

def check_interaction(S, scope):
    S.pg.evaluate(JS_HELPERS)
    fk = lambda: S.pg.evaluate("document.activeElement?.dataset?.fk || document.activeElement?.tagName")
    prm = lambda: S.pg.evaluate('dash.params()')
    S.reset()
    bad_focus, bad_hub, bad_super = [], [], []
    for metric in MET_ORDER:
        S.pg.locator(f'[data-fk="metric|{metric}"]').click(); S.settle(120)
        f = fk()
        if f != f'metric|{metric}': bad_focus.append(f'{metric}: focus on "{f}" after click')
        title = S.pg.evaluate("document.getElementById('g3-hub-title').textContent")
        if title != f'{MET[metric]["label"]}: every arm in every regime': bad_hub.append(f'{metric}: hub title "{title}"')
        if prm()['metric'] != metric: bad_hub.append(f'{metric}: param {prm()["metric"]}')
        pr = S.pg.evaluate("m => document.querySelector('[data-fk=\"metric|' + m + '\"]').getAttribute('aria-pressed')", metric)
        if pr != 'true': bad_hub.append(f'{metric}: aria-pressed={pr}')
        if S.pg.evaluate("[...document.querySelectorAll('#g3-metric [aria-pressed=true]')].length") != 1: bad_hub.append(f'{metric}: more than one pressed pill')
        sup = S.pg.evaluate("""m => { const h = document.querySelector('#g3-super [data-fk="sth|' + m + '"]'); return h ? [...h.querySelectorAll('text')].some(t => t.getAttribute('text-decoration') === 'underline') : null; }""", metric)
        if present_regimes(metric):
            if sup is not True: bad_super.append(f'{metric}: supertable header selected={sup}')
        elif metric.replace('_', ' ') and 'No arm can be compared' not in S.pg.evaluate("document.getElementById('g3-super-none').innerText"): bad_super.append(f'{metric}: no supertable column and no "cannot be compared" line')
    S.pg.locator('[data-fk="metric|stale_limited_p99_pct"]').focus(); S.pg.keyboard.press('Enter'); S.settle(120)
    if fk() != 'metric|stale_limited_p99_pct' or prm()['metric'] != 'stale_limited_p99_pct': bad_focus.append(f'keyboard Enter: focus "{fk()}" param {prm()["metric"]}')
    fails_to_rec('A6.metric-pill-keeps-focus', scope, bad_focus, '14 pills by mouse + Enter by keyboard')
    fails_to_rec('A6.metric-pill-changes-hub-title-and-pressed', scope, bad_hub, '14 pills')
    fails_to_rec('A6.metric-pill-changes-supertable-metric', scope, bad_super, 'supertable header underline follows the metric (#g3-super .hdr text-decoration)')
    bad_rope = []
    S.params(metric='bytes_total', rope='0.05')
    before = {(c['arm'], c['regime']): verdict_from_label(parse_label(c['label'], LNAME[c['arm']])[1]) for c in hub_cells(S)}
    for r in ROPES:
        S.pg.locator('#g3-rope').focus(); S.pg.select_option('#g3-rope', str(r)); S.settle(150)
        for c in hub_cells(S):
            v = V.get((c['arm'], c['regime'], 'bytes_total'))
            if v and v['kind'] != 'na':
                got = verdict_from_label(parse_label(c['label'], LNAME[c['arm']])[1])
                if got != v[r]: bad_rope.append(f'select {r}: {c["arm"]}/{c["regime"]} {got} vs {v[r]}')
        if S.pg.evaluate("document.activeElement.id") != 'g3-rope': bad_rope.append(f'select {r}: focus left the select ({fk()})')
    exp_changes = any(V[k][0.01] != V[k][0.25] for k in V if k[2] == 'bytes_total')
    after = {(c['arm'], c['regime']): verdict_from_label(parse_label(c['label'], LNAME[c['arm']])[1]) for c in hub_cells(S)}
    if exp_changes and after == before: bad_rope.append('verdicts did not change between 0.05 and 0.25 although the data say they should')
    fails_to_rec('A6.tie-band-select-changes-verdicts', scope, bad_rope, 'select_option at 0.01/0.05/0.10/0.25 matches the Python verdicts; focus stays on the select')
    S.params(rope='0.05')
    bad_click = []
    S.params(metric='bytes_total')
    for a, g in [('thicket', 'prod-peak'), ('owner-agg', 'idle'), ('plumtree', 'loss-20')]:
        if g not in present_regimes('bytes_total'): continue
        S.pg.locator(f'#g3-hub-plot .cell[data-arm="{a}"][data-regime="{g}"]').click(force=True); S.settle(150)
        p = prm()
        if p.get('arm') != a or p.get('regime') != g: bad_click.append(f'click {a}/{g}: params arm={p.get("arm")} regime={p.get("regime")}')
        h3 = S.pg.evaluate("document.querySelector('#g3-pane h3').textContent")
        if h3 != f'{LNAME[a]} · {MET["bytes_total"]["short"]} · {g}': bad_click.append(f'click {a}/{g}: pane h3 "{h3}"')
        sel = S.pg.evaluate("[...document.querySelectorAll('#g3-hub-plot .cell[aria-selected=true]')].map(c => c.dataset.arm + '/' + c.dataset.regime)")
        if sel != [f'{a}/{g}']: bad_click.append(f'click {a}/{g}: aria-selected cells {sel}')
        if fk() != f'hub|{a}|{g}': bad_click.append(f'click {a}/{g}: focus {fk()}')
    fails_to_rec('A6.click-cell-selects-and-updates-pane', scope, bad_click, 'params + pane heading + aria-selected + focus restored')
    bad_key = []
    S.params(metric='bytes_total', arm='flat-once', regime='low-3')
    first = S.pg.locator('#g3-hub-plot .cell[tabindex="0"]')
    if first.count() != 1: bad_key.append(f'{first.count()} cells with tabindex=0 (roving tab stop wants exactly 1)')
    first.first.focus()
    rc = lambda: S.pg.evaluate("document.activeElement.dataset.rc")
    i0, j0 = map(int, rc().split(','))
    for key, di, dj in (('ArrowRight', 0, 1), ('ArrowDown', 1, 0), ('ArrowLeft', 0, -1), ('ArrowUp', -1, 0)):
        S.pg.keyboard.press(key); got = rc(); i0, j0 = i0 + di, j0 + dj
        if got != f'{i0},{j0}': bad_key.append(f'{key}: focus rc {got} expected {i0},{j0}')
    S.pg.evaluate("document.querySelector('#g3-hub-plot .cell[data-rc=\"0,0\"]').focus()"); S.pg.keyboard.press('ArrowUp'); S.pg.keyboard.press('ArrowLeft')
    if rc() != '0,0': bad_key.append(f'edge: focus moved to {rc()} at the top-left corner')
    S.pg.keyboard.press('ArrowRight'); S.pg.keyboard.press('ArrowDown'); S.pg.keyboard.press('Enter'); S.settle(150)
    p = prm(); sel = S.pg.evaluate("[...document.querySelectorAll('#g3-hub-plot .cell[aria-selected=true]')].map(c => c.dataset.rc)")
    pres = present_regimes('bytes_total')
    if sel != ['1,1'] or p.get('arm') != CONTENDERS[1] or p.get('regime') != pres[1]: bad_key.append(f'Enter on rc 1,1: params arm={p.get("arm")} regime={p.get("regime")} selected={sel} expected {CONTENDERS[1]}/{pres[1]}')
    if rc() != '1,1': bad_key.append(f'after Enter focus rc={rc()}')
    fails_to_rec('A6.arrow-keys-move-grid-focus-and-enter-selects', scope, bad_key, 'roving tabindex, 4 arrows, edge, Enter')
    bad_prog = []
    for g in ('hot-key', 'idle'):
        S.pg.locator(f'[data-fk="prog|{g}"]').click(); S.settle(150)
        if prm().get('regime') != g: bad_prog.append(f'{g}: param {prm().get("regime")}')
        if S.pg.evaluate("g => document.querySelector('[data-fk=\"prog|' + g + '\"]').getAttribute('aria-pressed')", g) != 'true': bad_prog.append(f'{g}: not pressed')
        h3 = S.pg.evaluate("document.querySelector('#g3-pane h3').textContent")
        if not h3.endswith(g): bad_prog.append(f'{g}: pane h3 "{h3}"')
    fails_to_rec('A6.progress-strip-click-selects-regime', scope, bad_prog, 'param, aria-pressed, pane')
    bad_st = []
    for a, m in (('snow', 'conv_p99_incl_never_ms'), ('az-hier', 'drop_rcvbuf_pkts')):
        S.pg.evaluate("window.scrollTo(0, document.getElementById('g3-super').getBoundingClientRect().top + window.scrollY - 40)"); S.settle(100)
        S.pg.locator(f'#g3-super .cell[data-arm="{a}"][data-metric="{m}"]').click(force=True); S.settle(250)
        p = prm()
        if p.get('arm') != a or p.get('metric') != m: bad_st.append(f'click strip {a}/{m}: params arm={p.get("arm")} metric={p.get("metric")}')
        S.pg.wait_for_timeout(900)
        top = S.pg.evaluate("document.getElementById('g3-hub-sec').getBoundingClientRect().top")
        if abs(top) > 60: bad_st.append(f'click strip {a}/{m}: #g3-hub-sec top at {top:.0f}px after scroll (expected ~0)')
    S.pg.evaluate("document.querySelector('#g3-super .cell[tabindex=\"0\"]').focus()"); S.pg.keyboard.press('ArrowDown'); S.pg.keyboard.press('Enter'); S.settle(250)
    fails_to_rec('A6.supertable-strip-click-sets-metric-and-arm-and-scrolls', scope, bad_st, 'params + scroll to hub')
    bad_sc = []
    S.params(arm='snow')
    S.pg.locator('[data-fk="score|bimodal"]').click(); S.settle(120)
    if prm().get('arm') != 'bimodal': bad_sc.append(f'click row: arm={prm().get("arm")}')
    S.pg.locator('[data-fk="score|thicket"]').focus(); S.pg.keyboard.press('Enter'); S.settle(120)
    if prm().get('arm') != 'thicket': bad_sc.append(f'Enter on row: arm={prm().get("arm")}')
    if S.pg.evaluate("document.querySelector('[data-fk=\"score|thicket\"]').getAttribute('aria-selected')") != 'true': bad_sc.append('row aria-selected not true')
    if fk() != 'score|thicket': bad_sc.append(f'focus after Enter on row: {fk()}')
    fails_to_rec('A6.scorecard-row-click-and-enter-set-arm', scope, bad_sc, 'click and Enter')
    bad_tip, bad_esc, bad_blur = [], [], []
    S.params(metric='bytes_total', arm='flat-once', regime='prod-typical', rope='0.05'); S.pg.mouse.move(0, 0)
    a, g = 'plumtree', 'prod-typical'; v = V[(a, g, 'bytes_total')]
    c = S.pg.locator(f'#g3-hub-plot .cell[data-arm="{a}"][data-regime="{g}"]'); c.hover(force=True); S.pg.wait_for_timeout(80)
    t = S.pg.evaluate("(() => { const t = document.querySelector('.tip'); return { hidden: t.hidden, text: t.innerText }; })()")
    if t['hidden']: bad_tip.append('hover: tooltip hidden')
    else:
        for e in check_tip(t['text'], v, 'bytes', SNAME[a]): bad_tip.append(f'hover: {e}')
    S.pg.mouse.move(0, 0); S.pg.wait_for_timeout(60)
    if not S.pg.evaluate("document.querySelector('.tip').hidden"): bad_tip.append('mouseleave did not hide the tooltip')
    c.focus(); S.pg.wait_for_timeout(60)
    if S.pg.evaluate("document.querySelector('.tip').hidden"): bad_tip.append('keyboard focus on a cell did not show the tooltip')
    S.pg.keyboard.press('Escape'); S.pg.wait_for_timeout(60)
    if not S.pg.evaluate("document.querySelector('.tip').hidden"): bad_esc.append('selector=.tip | seen=still visible after Escape on a focused cell | expected=hidden')
    c.focus(); S.pg.wait_for_timeout(40)
    S.pg.evaluate("document.querySelector('#g3-rope').focus()"); S.pg.wait_for_timeout(60)
    if not S.pg.evaluate("document.querySelector('.tip').hidden"): bad_blur.append('tooltip stays visible after focus moved away (blur)')
    fails_to_rec('A6.tooltip-numbers-on-hover-and-focus', scope, bad_tip, 'tooltip text vs posterior; mouseleave hides; focus shows')
    fails_to_rec('A6.escape-hides-tooltip', scope, bad_esc, 'Escape on a focused cell')
    fails_to_rec('A6.blur-hides-tooltip', scope, bad_blur, 'blur')
    bad_pl = []
    S.pg.locator('[data-fk="plane|all"]').click(); S.settle(250)
    panels = S.pg.evaluate("document.querySelectorAll('#g3-plane svg[role=img]').length")
    ymet = 'conv_p99_incl_never_ms'
    exp_panels = len([g for g in REG_ORDER if any(a != REF and (a, g, 'bytes_total') in V and V[(a, g, 'bytes_total')]['kind'] != 'na' and (a, g, ymet) in V and V[(a, g, ymet)]['kind'] != 'na' for a in CONTENDERS)])
    ypar = prm().get('y')
    if ypar == ymet and panels != exp_panels: bad_pl.append(f'every-regime view draws {panels} panels, regimes with both metrics: {exp_panels}')
    S.pg.locator('[data-fk="plane|one"]').click(); S.settle(200)
    S.pg.locator('[data-fk="y|stale_limited_p99_pct"]').click(); S.settle(200)
    if prm().get('y') != 'stale_limited_p99_pct' or fk() != 'y|stale_limited_p99_pct': bad_pl.append(f'error-measure pill did not set y or lost focus (y={prm().get("y")}, focus={fk()})')
    fails_to_rec('A6.plane-controls', scope, bad_pl, 'one/all regimes toggle, error measure pill')
    S.reset()

def check_focus_and_a11y(S, scope):
    S.pg.evaluate(JS_HELPERS)
    SHOTS.mkdir(exist_ok=True)
    S.reset()
    st = S.pg.evaluate("""() => { const out = []; for (const g of document.querySelectorAll('[role=grid]')) {
        const cells = [...g.querySelectorAll('[role=gridcell]')]; const noLabel = cells.filter(c => !(c.getAttribute('aria-label') || '').trim()).length;
        const badRow = cells.filter(c => !c.parentElement.matches('[role=row]')).length; const heads = [...g.querySelectorAll('[role=columnheader]')];
        const headNoRow = heads.filter(h => !h.closest('[role=row]')).length; const tabs0 = cells.filter(c => c.getAttribute('tabindex') === '0').length;
        const host = g.closest('#g3-hub-plot, #g3-super, #g3-plane');
        out.push({ id: host ? host.id : '?', cells: cells.length, noLabel, badRow, heads: heads.length, headNoRow, tabs0, label: !!g.getAttribute('aria-label') }); } return out; }""")
    bad_g = []
    if not st: bad_g.append('no [role=grid]')
    for g in st:
        if g['cells'] == 0: bad_g.append(f'{g["id"]}: grid without gridcells')
        if g['noLabel']: bad_g.append(f'{g["id"]}: {g["noLabel"]} gridcells without aria-label')
        if not g['label']: bad_g.append(f'{g["id"]}: grid without aria-label')
        if g['tabs0'] != 1: bad_g.append(f'{g["id"]}: {g["tabs0"]} gridcells with tabindex=0 (expected exactly 1)')
    fails_to_rec('A7.grid-role-gridcell-aria-labels-one-tab-stop', scope, bad_g, f'{len(st)} grids')
    bad_s = []
    for g in st:
        if g['badRow']: bad_s.append(f'{g["id"]}: {g["badRow"]} role=gridcell not owned by a role=row')
        if g['headNoRow']: bad_s.append(f'{g["id"]}: {g["headNoRow"]} role=columnheader not inside a role=row')
    fails_to_rec('A7.grid-aria-ownership-columnheader-in-row', scope, bad_s, 'ARIA grid required-context structure')
    bad_f = S.pg.evaluate("""() => { const bad = []; for (const e of document.querySelectorAll('#g3 button, #g3 select, #g3 summary, #g3 [role=gridcell], #g3 [role=columnheader], #g3 [data-fk^="score|"]')) {
        if (e.closest('.measure') || !e.getClientRects().length) continue; const ti = e.getAttribute('tabindex'); const nativeFocusable = ['BUTTON','SELECT','SUMMARY'].includes(e.tagName);
        if (!nativeFocusable && ti === null) bad.push(e.tagName + '.' + (e.getAttribute('class') || '') + ' has no tabindex'); }
        return bad.slice(0, 6); }""")
    rec('A7.interactive-elements-are-focusable', scope, not bad_f, f'selector=buttons/selects/summary/gridcells/headers/rows | seen={bad_f} | expected=[]')
    S.pg.keyboard.press('Shift')
    kinds = [('pill', '[data-fk="metric|bytes_total"]'), ('select', '#g3-rope'), ('progress', '[data-fk^="prog|"]'), ('score-row', '#g3-score [data-fk^="score|"]'), ('hub-cell', '#g3-hub-plot .cell[tabindex="0"]'),
             ('hub-header', '#g3-hub-plot .hdr'), ('super-cell', '#g3-super .cell[tabindex="0"]'), ('super-header', '#g3-super .hdr'), ('summary', '#g3-hub-sec details summary'), ('plane-pill', '[data-fk^="y|"]')]
    bad_ind = []
    for name, sel in kinds:
        loc = S.pg.locator(sel).first
        if not loc.count(): bad_ind.append(f'{name}: selector {sel} not found'); continue
        loc.scroll_into_view_if_needed(); loc.focus(); S.pg.keyboard.press('Shift'); loc.focus(); S.pg.wait_for_timeout(60)
        info = S.pg.evaluate("""() => { const T = window.__t; const e = document.activeElement; const cs = getComputedStyle(e); const bg = T.pageBg();
            const f = e.querySelector && e.querySelector('rect.focus'); let ind = null;
            if (f) { const fs = getComputedStyle(f); ind = { kind: 'svg-rect', stroke: fs.stroke, width: parseFloat(fs.strokeWidth), c: fs.stroke === 'none' ? 0 : T.contrast(T.cssRgba(fs.stroke).slice(0, 3), bg) }; }
            else { const ow = parseFloat(cs.outlineWidth); ind = { kind: 'outline', style: cs.outlineStyle, width: ow, color: cs.outlineColor, c: cs.outlineStyle !== 'none' && ow > 0 ? T.contrast(T.cssRgba(cs.outlineColor).slice(0, 3), bg) : 0, shadow: cs.boxShadow }; }
            const r = e.getBoundingClientRect(); return { tag: e.tagName, fk: e.dataset.fk, focusVisible: e.matches(':focus-visible'), ind, rect: [r.left, r.top, r.width, r.height] }; }""")
        i = info['ind']
        ok = (i['kind'] == 'svg-rect' and i['width'] >= 1.5 and i['c'] >= 3) or (i['kind'] == 'outline' and i['width'] >= 2 and i['c'] >= 3 and i['style'] != 'none') or (i['kind'] == 'outline' and i.get('shadow', 'none') != 'none')
        if not ok or not info['focusVisible']: bad_ind.append(f'{name} [{sel}]: :focus-visible={info["focusVisible"]} indicator={i}')
        try:
            r = info['rect']
            if r[2] > 0 and r[3] > 0 and r[1] >= 0:
                S.pg.screenshot(path=str(SHOTS / f'focus-{name}-{S.width}-{S.theme}.png'), clip={'x': max(0, r[0] - 8), 'y': max(0, r[1] - 8), 'width': max(1, min(S.width - max(0, r[0] - 8), r[2] + 16)), 'height': r[3] + 16})
        except Exception: pass
    fails_to_rec('A7.visible-focus-indicator-3-to-1', scope, bad_ind, f'{len(kinds)} control kinds, screenshots in shots-test/')
    tg = S.pg.evaluate("""() => { const bad = []; let n = 0; for (const e of document.querySelectorAll('#g3 button, #g3 select, #g3 summary, #g3 [role=gridcell], #g3 [role=columnheader], #g3 [data-fk^="score|"]')) {
        if (e.closest('.measure') || !e.getClientRects().length) continue; const f = e.querySelector && e.querySelector(':scope > rect.focus'); const r = (f || e).getBoundingClientRect(); if (r.width === 0 || r.height === 0) continue; n++;
        if (r.width < 24 - 0.5 || r.height < 24 - 0.5) bad.push((e.tagName + '.' + (e.getAttribute('class') || '')).slice(0, 40) + ' ' + r.width.toFixed(0) + 'x' + r.height.toFixed(0)); }
        return { n, bad }; }""")
    byc = collections.Counter(tg['bad'])
    rec('A7.targets-at-least-24px', scope, not tg['bad'], f'selector=interactive elements ({tg["n"]}) | seen={len(tg["bad"])} below 24px: {list(byc.items())[:5]} | expected>=24x24')
    S.reset()

def check_contrast(S, scope):
    S.pg.evaluate(JS_HELPERS)
    txt_bad = {}; gr_bad = {}; nt = ng = 0
    for metric in MET_ORDER:
        S.params(metric=metric, rope='0.05')
        o = S.pg.evaluate("""() => { const T = window.__t; const bg0 = T.pageBg(); const out = { text: [], gr: [] };
            const seen = new Set();
            for (const it of T.visibleItems()) { const e = it.el; const cs = getComputedStyle(e); let col, bg;
              if (it.svg) { col = T.cssRgba(cs.fill); col[3] *= parseFloat(cs.fillOpacity); bg = bg0; } else { col = T.cssRgba(cs.color); bg = T.bgOf(e); }
              col[3] *= T.opacityChain(e); const eff = T.blend(col, bg); const c = T.contrast(eff, bg);
              const k = T.sel(e) + '|' + cs.color + '|' + cs.fill + '|' + cs.opacity; if (seen.has(k)) continue; seen.add(k);
              out.text.push({ sel: T.sel(e), c, ex: it.text.slice(0, 24), fg: eff.slice(0, 3).map(Math.round).join(',') }); }
            const gseen = new Set();
            for (const e of document.querySelectorAll('#g3 svg path.gl, #g3 svg path.pf, #g3 svg .cap, #g3 svg line.seed, #g3 svg line.ivl, #g3 svg line.mt')) { if (T.inMeasure(e)) continue; const cs = getComputedStyle(e); const isFill = e.matches('.gl.better, .gl.worse, .pf.better, .pf.worse, .pf.tie'); let col = T.cssRgba(isFill ? cs.fill : cs.stroke); col[3] *= parseFloat(isFill ? cs.fillOpacity : cs.strokeOpacity) * T.opacityChain(e);
              const eff = T.blend(col, bg0); const c = T.contrast(eff, bg0); const k = e.tagName + '.' + e.getAttribute('class') + '|' + col.join(','); if (gseen.has(k)) continue; gseen.add(k); out.gr.push({ sel: e.tagName + '.' + e.getAttribute('class'), c, fg: eff.slice(0, 3).map(Math.round).join(',') }); }
            return out; }""")
        for t in o['text']:
            nt += 1
            if t['c'] < 4.5: txt_bad.setdefault((t['sel'], t['fg']), (t['c'], t['ex']))
        for t in o['gr']:
            ng += 1
            if t['c'] < 3: gr_bad.setdefault((t['sel'], t['fg']), t['c'])
    fails_to_rec('A7.text-contrast-at-least-4.5', scope, [f'{k[0]} fg rgb({k[1]}) = {v[0]:.2f}:1 e.g. "{v[1]}"' for k, v in txt_bad.items()], f'{nt} distinct text styles over 14 metrics')
    iv = {k: v for k, v in gr_bad.items() if re.search(r'\.ivl\b', k[0])}
    rest = {k: v for k, v in gr_bad.items() if k not in iv}
    fails_to_rec('A7.graphics-contrast-at-least-3 (glyphs, brackets, seed ticks)', scope, [f'{k[0]} rgb({k[1]}) = {v:.2f}:1' for k, v in rest.items()], f'{ng} distinct mark styles')
    fails_to_rec('A7.interval-bar-contrast-at-least-3 (line.ivl)', scope, [f'{k[0]} rgb({k[1]}) = {v:.2f}:1' for k, v in iv.items()], 'interval bars (brackets carry the same information at full opacity)')

def check_source_literals():
    class P(HTMLParser):
        def __init__(self):
            super().__init__(); self.stack = []; self.bad = []; self.skip = 0
        def handle_starttag(self, tag, attrs):
            if tag in ('script', 'style'): self.skip += 1
            if tag in ('br', 'meta', 'link', 'img', 'input', 'hr'): return
            self.stack.append((tag, any(k == 'data-source' for k, _ in attrs)))
        def handle_endtag(self, tag):
            if tag in ('script', 'style'): self.skip -= 1; return
            for i in range(len(self.stack) - 1, -1, -1):
                if self.stack[i][0] == tag: del self.stack[i:]; break
        def handle_data(self, d):
            if self.skip or not re.search(r'\d', d): return
            if any(ds for _, ds in self.stack): return
            self.bad.append(f'{"/".join(t for t, _ in self.stack[-3:])}: "{d.strip()[:60]}"')
    p = P(); p.feed(PAGE.read_text())
    rec('A9.no-literal-numbers-in-static-markup', 'source', not p.bad, f'selector=text nodes outside [data-source] in index.html | seen={p.bad[:6]} | expected=none')

def check_calculations(S, scope):
    """The Calculations disclosure dash.js appends lists every dash.calc the page registers, in order."""
    src = PAGE.read_text()
    exp = [(t.replace("\\'", "'"), d.replace("\\'", "'")) for t, d in re.findall(r"dash\.calc\('\w+', \{\s*title: '((?:[^'\\]|\\.)*)',\s*description: '((?:[^'\\]|\\.)*)'", src)]
    seen = S.pg.evaluate("[...document.querySelectorAll('#g3-calcs p > b')].map((b) => [b.textContent, b.parentElement.textContent])")
    want_ = [[f'{t}.', f'{t}. {d}'] for t, d in exp]
    rec('A4.calculations-disclosure-lists-every-calc', scope, exp and seen == want_, f'selector=#g3-calcs | seen {len(seen)} entries, expected {len(exp)} registered calcs in order | first mismatch: {next(([a, b] for a, b in zip(seen, want_) if a != b), None)}')

def check_link(S, scope):
    """A view's URL hash restores it on load, and a control change rewrites the hash."""
    S.pg.goto(S.base + '/dashboard/#metric=conv_p99_incl_never_ms&regime=az-partition'); S.pg.reload()
    S.pg.wait_for_selector('[data-fk="metric|bytes_total"]', timeout=25000)
    p = S.pg.evaluate('dash.params()')
    pressed = S.pg.locator('[data-fk^="metric|"][aria-pressed="true"]').evaluate_all('ps => ps.map((p) => p.dataset.fk)')
    S.params(metric='bytes_total')
    h = S.pg.evaluate('location.hash')
    rec('A6.view-state-round-trips-through-the-link', scope,
        pressed == ['metric|conv_p99_incl_never_ms'] and p['metric'] == 'conv_p99_incl_never_ms' and p['regime'] == 'az-partition' and 'metric=bytes_total' in h and 'regime=az-partition' in h,
        f'selector=location.hash | seen=params from link {p["metric"]}/{p["regime"]}, pressed {pressed}, hash after a pill click {h} | expected the link restored, then metric=bytes_total beside regime=az-partition')

def shoot(S, name, params):
    for vp in VIEWPORTS:
        for th in THEMES:
            S.open(vp, th)
            try:
                S.params(**params)
                S.pg.screenshot(path=str(SHOT_DIR / name / f'w{vp}-{th}.png'), full_page=True)
                o = S.pg.evaluate("() => ({ errors: window.__ERRORS__, overflow: document.documentElement.scrollWidth > window.innerWidth })")
                bad = o['errors'] + [l for l in S.logs if l.startswith(('error', 'pageerror'))] + S.fails + (['page overflows sideways'] if o['overflow'] else [])
                rec('shots.renders-clean', f'{name}/{vp}/{th}', not bad, f'seen={bad[:3]}' if bad else 'no errors, no page overflow')
            finally: S.close()

def safe(fn, S, *a, **k):
    try: fn(S, *a, **k)
    except Exception as e:
        rec('harness.' + fn.__name__, str(a[:1]), False, f'harness error (machine under memory pressure?): {type(e).__name__}: {str(e)[:160]}')

def check_fonts(S, scope):
    """A10: the self-hosted faces are loaded and every request stays on the page's own origin."""
    o = S.pg.evaluate("""() => ({
        loaded: [...document.fonts].filter((f) => f.status === 'loaded').map((f) => f.family.replace(/"/g, '')),
        check: document.fonts.check('16px "Source Sans 3"') && document.fonts.check('16px "Source Serif 4"'),
        foreign: performance.getEntriesByType('resource').map((e) => e.name).filter((u) => new URL(u).origin !== location.origin),
    })""")
    want_faces = {'Source Sans 3', 'Source Serif 4'}
    rec('A10.faces-loaded', scope, o['check'] and want_faces <= set(o['loaded']), f'selector=document.fonts | seen={sorted(set(o["loaded"]))} | expected={sorted(want_faces)} loaded')
    rec('A10.requests-same-origin', scope, not o['foreign'], f'selector=performance resources | seen={o["foreign"][:3]} | expected=none off the page origin')
    check_font_gate(S, scope)

# Marks when the page has parsed params.json: set before the page's own continuations run,
# in the same microtask checkpoint, so a check made after it sees what they did.
JS_FONT_GATE = r'''
(() => {
  const fetch0 = window.fetch;
  window.fetch = (url, ...rest) => fetch0(url, ...rest).then((r) => {
    if (!String(url).endsWith('params.json')) return r;
    const json = r.json.bind(r);
    r.json = () => json().then((v) => ((window.__paramsRead = true), v));
    return r;
  });
})();
'''

def check_font_gate(S, scope):
    """A10: the page holds its first render for the faces. The face files are held back and
    the page's clock paused, so its fallback timer cannot fire: once params.json is read, the
    page must still be waiting. Then the clock runs past the fallback, so the page draws with
    the fallback face; released, the faces must replace its measured widths and line boxes,
    so the redrawn page has no text that overlaps or is clipped."""
    ctx = S.browser.new_context(viewport={'width': 1440, 'height': 900}, device_scale_factor=1)
    try:
        pg = ctx.new_page(); held = []
        pg.route('**/fonts/*.woff2', lambda route: held.append(route))
        pg.add_init_script(JS_FONT_GATE)
        pg.clock.install(); pg.clock.pause_at(datetime.datetime.now() + datetime.timedelta(seconds=1))
        pg.goto(S.base + '/dashboard/', wait_until='commit')
        for _ in range(400):
            if pg.evaluate('() => window.__paramsRead === true') and held: break
            pg.wait_for_timeout(50)
        else: raise RuntimeError(f'params.json was never read, or no face requested; faces held={len(held)}')
        waiting = pg.evaluate('() => Object.keys(dash.params()).length === 0')
        pg.clock.run_for(10_000); pg.clock.resume()
        pg.wait_for_selector('[data-fk="metric|bytes_total"]', timeout=25000)
        # the fallback draw is marked, so the redraw the faces bring is seen when it replaces the mark
        early = pg.evaluate("""() => !document.fonts.check('16px "Source Sans 3"') && !!(document.querySelector('#g3-hub-plot svg').dataset.fallback = '1')""")
        for route in held: route.continue_()
        pg.unroute('**/fonts/*.woff2')
        for _ in range(250):
            redrawn = pg.evaluate("""() => document.fonts.check('16px "Source Sans 3"') && !document.querySelector('#g3-hub-plot svg[data-fallback]')""")
            if redrawn: break
            pg.wait_for_timeout(40)
        pg.evaluate(JS_HELPERS); o = pg.evaluate(JS_SCAN)
        rec('A10.faces-before-first-render', scope, waiting, f'selector=dash.params with the faces held | seen=waiting {waiting} | expected=waiting for the faces')
        rec('A10.late-faces-redraw', scope, early and redrawn and o['overlapCount'] == 0 and o['clippedCount'] == 0,
            f'selector=visible text after the faces load late | seen=drew before the faces {early}, redrew {redrawn}, {o["overlapCount"]} overlaps {o["overlaps"][:2]}, {o["clippedCount"]} clipped {o["clipped"][:2]} | expected=True, True, 0, 0')
    finally:
        ctx.close()

def main():
    handler = functools.partial(Quiet, directory=str(STUDY))
    srv = http.server.ThreadingHTTPServer(('127.0.0.1', 0), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f'http://127.0.0.1:{srv.server_address[1]}'
    if want('A9'): check_source_literals()
    def step(msg): print('  ..', msg, file=sys.stderr, flush=True)
    with sync_playwright() as pw:
        S = Session(pw, base)
        try:
            if any(want(p) for p in ('A1', 'A2', 'A3', 'A8', 'C4')):
                for vp in VIEWPORTS:
                    for th in THEMES:
                        S.open(vp, th)
                        try: safe(run_state_checks, S, vp, th)
                        finally: S.close()
                        step(f'state checks {vp}/{th}')
            if any(want(p) for p in ('A4', 'C1', 'C3', 'C4', 'A5')):
                for var in VARIANTS:
                    use_variant(var); sc = f'1440/light/{var}'
                    S.open(1440, 'light')
                    try:
                        S.params(variant=var)
                        if want('A4'):
                            safe(check_verdict_labels, S, sc); step(f'labels {var}'); safe(check_hub_table, S, sc); safe(check_scorecard, S, sc); step(f'scorecard {var}'); safe(check_main_missing, S, sc)
                            if var == 'baseline': safe(check_progress, S, sc); safe(check_calculations, S, sc)
                        if want('A4') or want('C1') or want('C3') or want('C4'): safe(check_panes, S, sc); step(f'panes {var}')
                        if want('C1'): safe(check_label_c1, S, sc)
                        if want('A5'): safe(check_order, S, sc); step(f'order {var}')
                    finally: S.close()
                    S.open(400, 'dark')
                    try:
                        S.params(variant=var)
                        if want('A4'): safe(check_verdict_labels, S, f'400/dark/{var}', phone=True)
                        if want('A5'): safe(check_order, S, f'400/dark/{var}')
                    finally: S.close()
                    step(f'400/dark {var}')
                use_variant('baseline')
                if want('A5'): check_rows_identical_across_variants()
            if any(want(p) for p in ('A5', 'A6', 'A7', 'A10')):
                S.open(1440, 'light')
                try:
                    if want('A6'): safe(check_hover_targets, S, '1440/light')
                    if want('A6'): safe(check_interaction, S, '1440/light'); step('interaction')
                    if want('A7'): safe(check_focus_and_a11y, S, '1440/light'); safe(check_contrast, S, '1440/light'); step('a11y light')
                    if want('A6'): safe(check_link, S, '1440/light')
                    if want('A10'): safe(check_fonts, S, '1440/light')
                finally: S.close()
                S.open(1440, 'dark')
                try:
                    if want('A7'): safe(check_focus_and_a11y, S, '1440/dark'); safe(check_contrast, S, '1440/dark')
                    if want('A6'): safe(check_interaction, S, '1440/dark')
                finally: S.close()
                step('1440/dark')
                S.open(400, 'dark')
                try:
                    if want('A7'): safe(check_focus_and_a11y, S, '400/dark')
                finally: S.close()
                step('400/dark')
                for vp in (1280, 1100, 768, 400):
                    S.open(vp, 'light')
                    try:
                        if want('A7'): safe(check_focus_and_a11y, S, f'{vp}/light')
                        if want('A5') and vp != 400: safe(check_order, S, f'{vp}/light')
                    finally: S.close()
                    step(f'a11y {vp}')
            if want('A4') or want('C2') or want('C1'):
                for var in VARIANTS:
                    use_variant(var)
                    for vp in (1440, 1280, 1100, 768):
                        for th in (('light', 'dark') if vp == 1440 else ('light',)):
                            S.open(vp, th)
                            try:
                                S.params(variant=var); safe(check_forest_geometry, S, f'{vp}/{th}/{var}', vp)
                            finally: S.close()
                            step(f'forest {vp}/{th}/{var}')
                use_variant('baseline')
            if want('A6'):
                for vp in (1440, 1280, 1100):
                    S.open(vp, 'light')
                    try:
                        S.pg.evaluate(JS_HELPERS)
                        for metric in ('bytes_total', 'conv_p99_incl_never_ms'):
                            S.params(metric=metric)
                            p = S.pg.evaluate(JS_PANE)['pos']
                            beside = p['left'] >= p['hubRight'] - 1 and p['top'] < p['hubBottom']
                            below = p['top'] >= p['hubBottom'] - 1
                            rec('A6.detail-pane-beside-or-below-hub', f'{vp}/light/{metric}', beside or below, f'selector=#g3-pane | seen=left {p["left"]:.0f} top {p["top"]:.0f} hub right {p["hubRight"]:.0f} bottom {p["hubBottom"]:.0f} | expected beside or below')
                    finally: S.close()
            if SHOT_DIR:
                for name, params in SHOT_SETS.items():
                    (SHOT_DIR / name).mkdir(parents=True, exist_ok=True); safe(shoot, S, name, params); step(f'shots {name}')
        finally:
            S.browser.close()
    srv.shutdown()

def report():
    by = collections.OrderedDict()
    for cid, scope, ok, ev in RES: by.setdefault(cid, []).append((scope, ok, ev))
    p = f = 0
    print('=' * 100)
    for cid, items in sorted(by.items(), key=lambda kv: kv[0]):
        fl = [i for i in items if not i[1]]; ps = [i for i in items if i[1]]
        if ps:
            print(f'PASS {cid} [{len(ps)} scope{"s" if len(ps) != 1 else ""}: {", ".join(s for s, _, _ in ps[:3])}{"..." if len(ps) > 3 else ""}] {ps[0][2][:150]}')
        for scope, ok, ev in fl:
            print(f'FAIL {cid} [{scope}] {ev[:900]}')
        p += len(ps); f += len(fl)
    print('=' * 100)
    print(f'SUMMARY: {p} PASS, {f} FAIL scope-checks; {sum(1 for i in by.values() if all(x[1] for x in i))} of {len(by)} check ids pass everywhere, {sum(1 for i in by.values() if any(not x[1] for x in i))} check ids fail somewhere')
    return f

if __name__ == '__main__':
    try: main()
    except Exception as e: rec('harness.main', 'run', False, f'harness error: {type(e).__name__}: {str(e)[:600]}')
    sys.exit(1 if report() else 0)
