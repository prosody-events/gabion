SHELL := /bin/sh

COMPOSE := docker compose --profile module -f deploy/nginx/docker-compose.yml
CARGO_ENV := CARGO_BUILD_RUSTC_WRAPPER=

.PHONY: help
help:
	@printf '%s\n' 'Gabion test targets:'
	@printf '%s\n' '  make format          Run cargo +nightly fmt'
	@printf '%s\n' '  make fmt             Run cargo +nightly fmt --check'
	@printf '%s\n' '  make clippy          Run cargo clippy for all workspace targets'
	@printf '%s\n' '  make test            Run formatting, clippy, workspace tests, and hygiene checks'
	@printf '%s\n' '  make bench-check     Compile core and gossip benchmarks'
	@printf '%s\n' '  make nginx-config    Validate the base nginx:stable-alpine config'
	@printf '%s\n' '  make nginx-module    Build and load-test the Gabion NGINX module config'
	@printf '%s\n' '  make nginx-test      Build NGINX module and assert 200, 200, 429 responses'
	@printf '%s\n' '  make kubernetes-test Run guarded local OrbStack EndpointSlice convergence tests'
	@printf '%s\n' '  make kubernetes-clean Delete local Kubernetes test namespaces'
	@printf '%s\n' '  make ci              Run test, bench-check, nginx-config, nginx-module, nginx-test'

.PHONY: fmt
fmt: fmt-check

.PHONY: fmt-check
fmt-check:
	cargo +nightly fmt --check

.PHONY: format
format:
	cargo +nightly fmt

.PHONY: clippy
clippy:
	env $(CARGO_ENV) cargo clippy --workspace --all-targets --tests -- -D warnings

.PHONY: unit
unit:
	env $(CARGO_ENV) cargo test --workspace

.PHONY: hygiene
hygiene:
	! rg -n 'Box\s*<\s*dyn|dyn\s+' Cargo.toml crates deploy docs
	! rg -n 'version\s*=\s*"|=\s*\{\s*version' crates/*/Cargo.toml

.PHONY: test
test: fmt clippy unit hygiene

.PHONY: bench-check
bench-check:
	env $(CARGO_ENV) cargo bench -p gabion-core --bench core_engine --no-run
	env $(CARGO_ENV) cargo bench -p gabion-gossip --bench gossip_codec --no-run

.PHONY: nginx-config
nginx-config:
	docker compose -f deploy/nginx/docker-compose.yml run --rm nginx-config-smoke

.PHONY: nginx-module
nginx-module:
	$(COMPOSE) run --build --rm nginx-module-smoke

.PHONY: nginx-test
nginx-test:
	$(COMPOSE) run --build --rm nginx-module-request-smoke

.PHONY: kubernetes-test
kubernetes-test:
	sh deploy/kubernetes/local-smoke.sh

.PHONY: kubernetes-clean
kubernetes-clean:
	sh deploy/kubernetes/local-clean.sh

.PHONY: ci
ci: test bench-check nginx-config nginx-module nginx-test
