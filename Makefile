SHELL := /bin/sh

COMPOSE := docker compose --profile module -f deploy/nginx/docker-compose.yml
CARGO_ENV := CARGO_BUILD_RUSTC_WRAPPER=
# `rustup run` resolves the correct toolchain for the host triple and puts
# the matching `rustc` on PATH for the duration of the call — works the
# same on macOS arm64 contributor laptops and the `x86_64-unknown-linux-gnu`
# GitHub Actions runners we ship CI on. The `require-*` targets below
# verify the toolchains are installed before any cargo invocation.
STABLE_CARGO := rustup run stable cargo
NIGHTLY_CARGO := rustup run nightly cargo

# `cargo nextest` is the *only* sanctioned test runner for this repo.
# Faster than `cargo test`, surfaces failures earlier, supports per-test
# timeouts, and is what CI runs.
# Install with `cargo install cargo-nextest --locked`.
NEXTEST := $(STABLE_CARGO) nextest run

# `cargo +nightly fmt` is the *only* sanctioned formatter. Stable
# rustfmt does not understand all the unstable knobs the codebase
# relies on; running it will produce a diff CI rejects.
FMT := $(NIGHTLY_CARGO) fmt

# Miri lives on nightly. Install with `rustup component add miri --toolchain nightly`.
MIRI := $(NIGHTLY_CARGO) miri nextest run --no-fail-fast

# Friendly install-check. Run before any test target so a missing tool
# surfaces a one-line fix instead of a cryptic cargo error.
.PHONY: require-nextest
require-nextest:
	@$(STABLE_CARGO) nextest --version >/dev/null 2>&1 || { \
		printf '%s\n' \
			'cargo-nextest is not installed.' \
			'Gabion uses nextest as its only test runner; cargo test is unsupported.' \
			'Install with: cargo install cargo-nextest --locked' >&2; \
		exit 1; \
	}

.PHONY: require-nightly-fmt
require-nightly-fmt:
	@$(NIGHTLY_CARGO) fmt --version >/dev/null 2>&1 || { \
		printf '%s\n' \
			'cargo +nightly fmt is not available.' \
			'Gabion formats with nightly rustfmt; stable rustfmt is unsupported.' \
			'Install with: rustup toolchain install nightly --component rustfmt' >&2; \
		exit 1; \
	}

# The gabion-wasm crate (the visualizer's gossip core) cross-compiles to
# WebAssembly. getrandom 0.4 selects its browser backend from the `wasm_js`
# feature alone (gabion's wasm32 manifest enables it), so no RUSTFLAGS cfg is
# needed any more.
WASM_TARGET := wasm32-unknown-unknown

.PHONY: require-wasm-target
require-wasm-target:
	@rustup target list --installed 2>/dev/null | grep -qx '$(WASM_TARGET)' || { \
		printf '%s\n' \
			'The $(WASM_TARGET) target is not installed.' \
			'gabion-wasm compiles the gossip + CRDT core to WebAssembly for the visualizer.' \
			'Install with: rustup target add $(WASM_TARGET)' >&2; \
		exit 1; \
	}

# The visualizer frontend (crates/gabion-wasm/web) builds with wasm-pack + pnpm.
WEB_DIR := crates/gabion-wasm/web

.PHONY: require-wasm-pack
require-wasm-pack:
	@wasm-pack --version >/dev/null 2>&1 || { \
		printf '%s\n' \
			'wasm-pack is not installed.' \
			'It builds the gabion-wasm crate into the package the frontend imports.' \
			'Install with: cargo install wasm-pack --locked' >&2; \
		exit 1; \
	}

.PHONY: require-pnpm
require-pnpm:
	@pnpm --version >/dev/null 2>&1 || { \
		printf '%s\n' \
			'pnpm is not installed.' \
			'The visualizer frontend ($(WEB_DIR)) uses pnpm for its JS toolchain.' \
			'Install with: corepack enable pnpm   (or: npm install -g pnpm)' >&2; \
		exit 1; \
	}

# The *release* wasm build (web/package.json `build:wasm`) is size-optimized with
# nightly's `-Z build-std` + `panic=immediate-abort`, which recompiles the
# standard library from source — shaving ~15% off the gzipped download. That
# needs nightly's `rust-src` component. (`--dev` builds stay on stable and need
# none of this; only the size-optimized release path pins nightly.)
.PHONY: require-wasm-nightly
require-wasm-nightly:
	@rustup component list --toolchain nightly 2>/dev/null | grep -q 'rust-src (installed)' || { \
		printf '%s\n' \
			'The nightly rust-src component is not installed.' \
			'The size-optimized release wasm build recompiles std with -Z build-std.' \
			'Install with: rustup toolchain install nightly --component rust-src' \
			'         and: rustup target add $(WASM_TARGET) --toolchain nightly' >&2; \
		exit 1; \
	}

.PHONY: help
help:
	@printf '%s\n' 'Gabion test targets:'
	@printf '%s\n' '  make format          Format with cargo +nightly fmt (the only sanctioned formatter)'
	@printf '%s\n' '  make fmt             Check formatting with cargo +nightly fmt --check'
	@printf '%s\n' '  make clippy          Run cargo clippy for all workspace targets'
	@printf '%s\n' '  make test            Run formatting, clippy, workspace tests, hygiene, and safety tests'
	@printf '%s\n' '  make unit            Run cargo nextest across the workspace (the only sanctioned test runner)'
	@printf '%s\n' '  make safety          Run gabion-nginx safety integration tests (cargo nextest)'
	@printf '%s\n' '  make miri-safety     Run safety tests under miri (Stacked Borrows)'
	@printf '%s\n' '  make miri-safety-tb  Run safety tests under miri (Tree Borrows)'
	@printf '%s\n' '  make miri-lib        Run all gabion-nginx lib tests under miri'
	@printf '%s\n' '  make miri-all        Run miri (Stacked + Tree Borrows) on every gabion-nginx test'
	@printf '%s\n' '  make bench-check     Compile gabion::crdt benchmarks'
	@printf '%s\n' '  make wasm-check      Cross-compile gabion-wasm, run its native tests, and build the web frontend'
	@printf '%s\n' '  make nginx-config    Validate the base nginx:stable-alpine config'
	@printf '%s\n' '  make nginx-module    Build and load-test the Gabion NGINX module config'
	@printf '%s\n' '  make nginx-test      Build NGINX module and assert 200, 200, 429 responses'
	@printf '%s\n' '  make nginx-matrix    Build Gabion NGINX images for common official NGINX tags'
	@printf '%s\n' '  make openresty-matrix Build Gabion OpenResty images for common OpenResty tags'
	@printf '%s\n' '  make kubernetes-test Run guarded local kind EndpointSlice convergence tests'
	@printf '%s\n' '  make kubernetes-nginx-test Run guarded local kind NGINX scale rate-limit tests'
	@printf '%s\n' '  make kubernetes-mixed-test Run guarded local kind NGINX plus Gabion server gossip test'
	@printf '%s\n' '  make kubernetes-gossip-bench Run guarded local kind gossip propagation benchmark'
	@printf '%s\n' '  make kubernetes-clean Delete local Kubernetes test namespaces'
	@printf '%s\n' '  make ci              Fast contributor pre-merge: test, miri-safety, bench-check, wasm-check, nginx-config, nginx-module, nginx-test'
	@printf '%s\n' '  make ci-full         Mirror GitHub Actions end-to-end: ci + miri-all + every kubernetes-* target'

.PHONY: fmt
fmt: fmt-check

.PHONY: fmt-check
fmt-check: require-nightly-fmt
	$(FMT) --check

.PHONY: format
format: require-nightly-fmt
	$(FMT)

.PHONY: clippy
clippy:
	$(CARGO_ENV) $(STABLE_CARGO) clippy --workspace --all-targets --tests -- -D warnings

.PHONY: unit
unit: require-nextest
	$(CARGO_ENV) $(NEXTEST) --workspace

# Safety integration tests live in crates/nginx/tests/safety.rs. They
# exercise every nginx-side unsafe boundary (mmap-style SHM init, MPSC
# queue, single-writer / multi-reader aggregate, leader-lease takeover,
# end-to-end access → leader-apply → reread).
.PHONY: safety
safety: require-nextest
	$(CARGO_ENV) $(NEXTEST) -p gabion-nginx --test safety

.PHONY: miri-safety
miri-safety:
	$(CARGO_ENV) $(MIRI) -p gabion-nginx --test safety

# Tree Borrows is the more rigorous Stacked Borrows successor; the safety
# tests pass under both modes.
.PHONY: miri-safety-tb
miri-safety-tb:
	MIRIFLAGS="-Zmiri-tree-borrows" $(CARGO_ENV) $(MIRI) -p gabion-nginx --test safety

.PHONY: miri-lib
miri-lib:
	# `identity::tests` reads `SystemTime::now()` via `clock_gettime(REALTIME)`,
	# which miri refuses under default isolation. Disabling isolation is
	# the official recommendation from miri's own error message and is
	# scoped per-target by the make variable so the rest of the gate
	# (safety integration tests, etc.) still runs under default isolation.
	MIRIFLAGS="-Zmiri-disable-isolation" $(CARGO_ENV) $(MIRI) -p gabion-nginx --lib

# Full miri coverage: both lib tests and safety integration tests, under
# both Stacked Borrows and Tree Borrows. Ramps up CI time significantly
# (a few minutes); not part of `make test` by default — run before merge.
.PHONY: miri-all
miri-all: miri-lib miri-safety miri-safety-tb
	MIRIFLAGS="-Zmiri-tree-borrows -Zmiri-disable-isolation" $(CARGO_ENV) $(MIRI) -p gabion-nginx --lib

.PHONY: hygiene
hygiene:
	! rg -n 'Box\s*<\s*dyn|dyn\s+' --glob '!crates/nginx/src/log.rs' Cargo.toml crates deploy
	! rg -n 'version\s*=\s*"|=\s*\{\s*version' crates/*/Cargo.toml

.PHONY: test
test: fmt clippy unit safety hygiene

.PHONY: bench-check
bench-check:
	$(CARGO_ENV) $(STABLE_CARGO) bench -p gabion --bench crdt --no-run

# Gate the visualizer's wasm bridge and frontend: (1) type-check gabion-wasm
# for wasm32 with the browser getrandom backend (the make-or-break
# cross-compile), (2) run its native engine + shim tests (no wasm toolchain
# needed), and (3) build the frontend — `pnpm run build` rebuilds the wasm
# package with wasm-pack, type-checks the Svelte/TS with svelte-check, and runs
# the production Vite build. The Playwright screenshot/in-browser smoke
# (`pnpm run screenshot`) downloads a browser, so it stays a documented
# on-demand run rather than part of this gate — same treatment as smoke.cjs.
.PHONY: wasm-check
wasm-check: require-nextest require-wasm-target require-wasm-pack require-pnpm require-wasm-nightly
	$(CARGO_ENV) $(STABLE_CARGO) check --target $(WASM_TARGET) -p gabion-wasm
	$(CARGO_ENV) $(NEXTEST) -p gabion-wasm
	cd $(WEB_DIR) && pnpm install --frozen-lockfile && pnpm run build

.PHONY: nginx-config
nginx-config:
	docker compose -f deploy/nginx/docker-compose.yml run --rm nginx-config-smoke

.PHONY: nginx-module
nginx-module:
	$(COMPOSE) run --build --rm nginx-module-smoke

.PHONY: nginx-test
nginx-test:
	$(COMPOSE) run --build --rm nginx-module-request-smoke

.PHONY: nginx-matrix
nginx-matrix:
	sh deploy/nginx/build-matrix.sh

.PHONY: openresty-matrix
openresty-matrix:
	sh deploy/nginx/build-openresty-matrix.sh

.PHONY: kubernetes-test
kubernetes-test:
	sh deploy/kubernetes/local-smoke.sh

.PHONY: kubernetes-nginx-test
kubernetes-nginx-test:
	sh deploy/kubernetes/nginx-scale-rate-limit.sh

.PHONY: kubernetes-mixed-test
kubernetes-mixed-test:
	sh deploy/kubernetes/mixed-nginx-gabion-gossip.sh

.PHONY: kubernetes-gossip-bench
kubernetes-gossip-bench:
	python3 deploy/kubernetes/gossip-propagation-bench.py

.PHONY: kubernetes-clean
kubernetes-clean:
	sh deploy/kubernetes/local-clean.sh

# `ci` is the fast subset a contributor runs before opening a PR: it pairs
# the local test gate with the slower miri / nginx / wasm smokes that
# `make test` alone skips. It deliberately does *not* run miri-all or any
# kubernetes-* target (kind boot + image builds are too slow for the
# pre-merge loop) — `make ci-full` does, and matches what GitHub Actions
# runs end-to-end.
.PHONY: ci
ci: test miri-safety bench-check wasm-check nginx-config nginx-module nginx-test

.PHONY: ci-full
ci-full: ci miri-all kubernetes-test kubernetes-nginx-test kubernetes-mixed-test kubernetes-gossip-bench
