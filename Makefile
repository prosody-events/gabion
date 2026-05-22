SHELL := /bin/sh

COMPOSE := docker compose --profile module -f deploy/nginx/docker-compose.yml
CARGO_ENV := CARGO_BUILD_RUSTC_WRAPPER=
# Resolve toolchain-pinned cargo binaries explicitly so the Makefile works
# under stripped-down environments (e.g. CI), where PATH probing may fall
# through to a broken `cargo` shim. Each cargo invocation needs `rustc`
# from the same toolchain on PATH.
STABLE_BIN := $(HOME)/.rustup/toolchains/stable-aarch64-apple-darwin/bin
NIGHTLY_BIN := $(HOME)/.rustup/toolchains/nightly-aarch64-apple-darwin/bin
STABLE_CARGO := PATH="$(STABLE_BIN):$$PATH" $(STABLE_BIN)/cargo
NIGHTLY_CARGO := PATH="$(NIGHTLY_BIN):$$PATH" $(NIGHTLY_BIN)/cargo

# nextest is the standard test runner for this repo. It's faster than
# `cargo test`, surfaces failures earlier, and supports per-test timeouts.
# Install with `cargo install cargo-nextest --locked`.
NEXTEST := $(STABLE_CARGO) nextest run

# Miri lives on nightly. Install with `rustup component add miri --toolchain nightly`.
MIRI := $(NIGHTLY_CARGO) miri nextest run --no-fail-fast

.PHONY: help
help:
	@printf '%s\n' 'Gabion test targets:'
	@printf '%s\n' '  make format          Run cargo +nightly fmt'
	@printf '%s\n' '  make fmt             Run cargo +nightly fmt --check'
	@printf '%s\n' '  make clippy          Run cargo clippy for all workspace targets'
	@printf '%s\n' '  make test            Run formatting, clippy, workspace tests, hygiene, and safety tests'
	@printf '%s\n' '  make unit            Run cargo nextest across the workspace'
	@printf '%s\n' '  make safety          Run gabion-nginx safety integration tests (cargo nextest)'
	@printf '%s\n' '  make miri-safety     Run safety tests under miri (Stacked Borrows)'
	@printf '%s\n' '  make miri-safety-tb  Run safety tests under miri (Tree Borrows)'
	@printf '%s\n' '  make miri-lib        Run all gabion-nginx lib tests under miri'
	@printf '%s\n' '  make miri-all        Run miri (Stacked + Tree Borrows) on every gabion-nginx test'
	@printf '%s\n' '  make bench-check     Compile core and gossip benchmarks'
	@printf '%s\n' '  make nginx-config    Validate the base nginx:stable-alpine config'
	@printf '%s\n' '  make nginx-module    Build and load-test the Gabion NGINX module config'
	@printf '%s\n' '  make nginx-test      Build NGINX module and assert 200, 200, 429 responses'
	@printf '%s\n' '  make nginx-matrix    Build Gabion NGINX images for common official NGINX tags'
	@printf '%s\n' '  make openresty-matrix Build Gabion OpenResty images for common OpenResty tags'
	@printf '%s\n' '  make kubernetes-test Run guarded local OrbStack EndpointSlice convergence tests'
	@printf '%s\n' '  make kubernetes-nginx-test Run guarded local OrbStack NGINX scale rate-limit tests'
	@printf '%s\n' '  make kubernetes-mixed-test Run guarded local OrbStack NGINX plus Gabion server gossip test'
	@printf '%s\n' '  make kubernetes-gossip-bench Run guarded local OrbStack gossip propagation benchmark'
	@printf '%s\n' '  make kubernetes-clean Delete local Kubernetes test namespaces'
	@printf '%s\n' '  make ci              Run test, miri-safety, bench-check, nginx-config, nginx-module, nginx-test'

.PHONY: fmt
fmt: fmt-check

.PHONY: fmt-check
fmt-check:
	$(NIGHTLY_CARGO) fmt --check

.PHONY: format
format:
	$(NIGHTLY_CARGO) fmt

.PHONY: clippy
clippy:
	$(CARGO_ENV) $(STABLE_CARGO) clippy --workspace --all-targets --tests -- -D warnings

.PHONY: unit
unit:
	$(CARGO_ENV) $(NEXTEST) --workspace

# Safety integration tests live in crates/nginx/tests/safety.rs. They
# exercise every nginx-side unsafe boundary (mmap-style SHM init, MPSC
# queue, single-writer / multi-reader aggregate, leader-lease takeover,
# end-to-end access → leader-apply → reread).
.PHONY: safety
safety:
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
	$(CARGO_ENV) $(MIRI) -p gabion-nginx --lib

# Full miri coverage: both lib tests and safety integration tests, under
# both Stacked Borrows and Tree Borrows. Ramps up CI time significantly
# (a few minutes); not part of `make test` by default — run before merge.
.PHONY: miri-all
miri-all: miri-lib miri-safety miri-safety-tb
	MIRIFLAGS="-Zmiri-tree-borrows" $(CARGO_ENV) $(MIRI) -p gabion-nginx --lib

.PHONY: hygiene
hygiene:
	! rg -n 'Box\s*<\s*dyn|dyn\s+' Cargo.toml crates deploy docs
	! rg -n 'version\s*=\s*"|=\s*\{\s*version' crates/*/Cargo.toml

.PHONY: test
test: fmt clippy unit safety hygiene

.PHONY: bench-check
bench-check:
	$(CARGO_ENV) $(STABLE_CARGO) bench -p gabion-core --bench core_engine --no-run
	$(CARGO_ENV) $(STABLE_CARGO) bench -p gabion-gossip --bench gossip_codec --no-run

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

.PHONY: ci
ci: test miri-safety bench-check nginx-config nginx-module nginx-test
