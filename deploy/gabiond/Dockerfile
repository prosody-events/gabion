# syntax=docker/dockerfile:1.7

FROM rust:alpine AS chef

RUN apk add --no-cache cargo-chef musl-dev

WORKDIR /workspace

FROM chef AS planner

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

COPY --from=planner /workspace/recipe.json recipe.json

# No `--mount=type=cache` on target/ or the registry: the cooked deps must
# land in this layer so the GHCR registry cache (`mode=max`) carries them
# to the next runner. A cache mount lives outside the layer and starts
# empty on every ephemeral CI runner, which left this step a cached no-op
# and pushed the full dependency compile into the build step below.
# The build step drops target/ once the binary is copied out: that layer
# depends on `COPY crates`, so it can never be reused, and leaving the
# workspace artifacts in it only bloats every `mode=max` cache export.
RUN cargo chef cook --release --package gabion-server --no-default-features --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build -p gabion-server --release --bin gabiond --no-default-features \
    && mkdir -p /out \
    && cp target/release/gabiond /out/gabiond \
    && rm -rf target

FROM alpine:3.24

RUN apk add --no-cache ca-certificates

COPY --from=builder /out/gabiond /usr/local/bin/gabiond

ENTRYPOINT ["/usr/local/bin/gabiond"]
