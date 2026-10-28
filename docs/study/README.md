# The gossip study

A comparative study of gossip designs for gabion: twelve baseline solutions and
their improved variants, run through the deterministic simulator in
[`crates/gossip-bench`](../../crates/gossip-bench/README.md) across 13 regimes
and 3 seeds each (1,287 runs), then judged against `main` with credible
intervals and posterior probabilities.

- [`report.md`](report.md): the method, the results and the reproduction
  commands.
- [`research.md`](research.md): the related-work survey the report builds on.
- `dashboard/`: the results as one static page (below).
- `data/`, `figures/`, `scripts/`, `tests/`: the snapshot, the report's figures,
  the code that rebuilds both, and the checks that guard them.

## Open the dashboard

`dashboard/index.html` fetches its data, so it needs a server; `file://` fails.

```sh
python3 -m http.server -d docs/study 8000
```

Then open `http://localhost:8000/dashboard/`. On every push to `main`,
`.github/workflows/wasm.yml` publishes `dashboard/` and `data/` beside the
visualizer on GitHub Pages, in the same layout:
<https://prosody-events.github.io/gabion/study/dashboard/> fetches `../data/`.
Every view has a link: the selected metric, regime, arm and tie band live in the
URL hash, for example `#metric=conv_p99_incl_never_ms&regime=az-partition`.

## How the page runs

The page was first published as a hosted dashboard, whose runtime supplied the
data, the derived calculations, the view state and the theme. `dashboard/dash.js`
replaces that runtime with exactly the five calls the page makes:

| Call | Behaviour |
|---|---|
| `data(id)` | A dataset is `missing` until its `fetch('../data/<file>.json')` resolves, then `ok`, or `error` when the fetch fails (the page then says so). A calculation returns its memoised result once all its inputs are `ok`; a failing calculation is logged to the console and `window.__ERRORS__`. |
| `calc(id, spec)` | Registers a calculation: a pure function of its input datasets. |
| `onData(fn)` | Subscribes `fn` and calls it now and after each dataset arrives. |
| `params()`, `setParams(p)` | View state starts as `params.json` overlaid with the URL hash; `setParams` merges, rewrites the hash and re-runs subscribers. |

Under the method section, a "Calculations" disclosure lists each calculation's
title and description in registration order and links the page source for the
code. `tokens.css` is a byte-identical copy of
`crates/gabion-wasm/web/src/tokens.css`; `make hygiene` fails when they differ.
`vendor/` holds d3 7.9.0 (SHA-256
`f2094bbf6141b359722c4fe454eb6c4b0f0e42cc10cc7af921fc158fceb86539`), so the page
and its tests need no network.

## Presentation rules

The shared rules are in
[`crates/gabion-wasm/web/README.md`](../../crates/gabion-wasm/web/README.md#design-rules).
The study adds:

- Credible intervals and posterior probabilities only. No other statistic is
  printed.
- Every estimate is followed by its 95 % credible interval and, on the detail
  pane, the seeds behind it.
- Arm order is fixed by `order` in `arms.json`, grouped by `family`, and never
  follows the selected metric. Rank is text ("fewest losses"), not position.
- The tie band (5 % by default, selectable) decides better, tie, worse and open
  for every cell; the same band draws the shaded span around the x1 line.
- Seed ticks show each valid seed's ratio to `main`'s same seed on the shared
  log axis; a cell with one seed says so in words.
- Shape carries the verdict and colour repeats it.
- The scorecard counts verdicts per metric family and names each arm's worst
  settled loss.

## Testing

```sh
pip install -r docs/study/tests/requirements.txt
python3 -m playwright install chromium
make study-test
```

`make study-test` runs `tests/test_compact.py` (raw against compacted data) and
`tests/test_ui.py`, the browser audit: 1,156 checks over the 1440, 1280, 1100,
768 and 400 px viewports in both themes, which recompute every printed number
from the data in Python and never reuse the page's code. It exits 1 on any FAIL.
`--only A3,A6` runs the checks whose ids start with those prefixes, and
`--shots DIR` also writes a full-page PNG of each named view (`bytes`, `p99`,
`stale`, `drops`, `heal`, `cell`) at every viewport and theme into
`DIR/<view>/w<width>-<theme>.png`, checking each renders without errors.

CI runs the same three commands in `.github/workflows/study.yml` when
`docs/study/` or `crates/gabion-wasm/web/src/tokens.css` changes (`rail.toml`'s
`study` work) and uploads `tests/shots-test/` when they fail; `make ci-full`
runs `make study-test` too.

## Refreshing the data

```sh
docs/study/scripts/refresh.sh <harness-checkout> <baseline-results> <improved-results> <scratch>
```

`<harness-checkout>` must be `sim/harness` at `896ea20`, the instrument the study
used (its `crates/gossip-bench` tree is `83697d1`); the in-tree
`crates/gossip-bench/judge/` has moved on and writes different files. The two
results directories hold `<solution>/<regime>/seed-<n>.json`; symlinks are
followed. The script:

1. judges every run against `main` (`data/judge/`) and writes `posterior.json`
   and `runs.json`;
2. writes `seed_metrics.json` (`export_seeds.py`, the judge's own metric
   extractors);
3. writes `self_posterior.json` (`self_judge.py`);
4. compacts the four generated datasets (`compact.py`);
5. runs `make study-test`.

The figures follow with `python3 docs/study/figures/make_figures.py`, which
needs matplotlib and takes its verdicts from
`crates/gossip-bench/judge/verdict.py`.

## Data

Eight datasets, each a JSON array of flat rows:

| File | Source |
|---|---|
| `posterior.json`, `self_posterior.json`, `runs.json`, `seed_metrics.json` | generated by `refresh.sh`, then compacted |
| `arms.json`, `metrics.json`, `regimes.json` | hand-edited definitions |
| `scale.json` | a frozen one-off measurement, not rebuilt by refresh |

Compaction writes compact separators and keeps every row, key and value
exactly. It rounds nothing: the page prints differences of nearly equal floats
(an interval's half-width is `ratio_hi / ratio - 1`), so rounding at any
precision short of a float's own changes printed text. The four files went from
6,900,716 to 6,271,714 bytes (gzip 796 KB to 783 KB). `test_compact.py` proves
it from the judge's full-precision `data/judge/summary.csv`.

`scale.json` draws "Beyond the autoscaler maximum" from one-off runs of
`imp-az-hier-nopk` at `328f7cf` on harness tree `011e8be`; its inputs are not in
the repository, so refresh cannot reproduce it.

## Adding an arm or regime

Add one row to `data/arms.json` (or `regimes.json`) with its `order` and `family`
(`group` for a regime), put the results under `<improved-results>`, and run
`refresh.sh`. Order stays fixed by `order`.

## Provenance

- Refresh instrument: `sim/harness` at `896ea20`.
- Runs: baseline arms on harness tree `8fb4e2d` (commit `fdf7390`), improved arms
  on `011e8be` (commit `133375f`), as `report.md` states.
- `data/provenance.json` holds `run_at_default`, the `run_at` given to results
  without `finished_at`. The study's results predate that field, so it is the
  date the first `runs.json` recorded, the only run date the study has; a refresh
  rewrites `runs.json` byte for byte.
- The footer reads "newest run" from the latest `run_at`.
