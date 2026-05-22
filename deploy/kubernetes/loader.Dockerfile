# syntax=docker/dockerfile:1.7

FROM rust:alpine AS chef
RUN apk add --no-cache cargo-chef musl-dev sccache
WORKDIR /workspace

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /workspace/recipe.json recipe.json
RUN --mount=type=cache,id=gabion-cargo-registry,target=/root/.cargo/registry \
    --mount=type=cache,id=gabion-cargo-git,target=/root/.cargo/git \
    --mount=type=cache,id=gabion-loader-target,target=/workspace/target \
    --mount=type=cache,id=gabion-loader-sccache,target=/workspace/.cache/sccache \
    SCCACHE_DIR=/workspace/.cache/sccache \
    CARGO_BUILD_RUSTC_WRAPPER=sccache \
    cargo chef cook --release --package gabion-loader --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,id=gabion-cargo-registry,target=/root/.cargo/registry \
    --mount=type=cache,id=gabion-cargo-git,target=/root/.cargo/git \
    --mount=type=cache,id=gabion-loader-target,target=/workspace/target \
    --mount=type=cache,id=gabion-loader-sccache,target=/workspace/.cache/sccache \
    SCCACHE_DIR=/workspace/.cache/sccache \
    CARGO_BUILD_RUSTC_WRAPPER=sccache \
    cargo build -p gabion-loader --release \
    && mkdir -p /out \
    && cp target/release/gabion-loader /out/gabion-loader

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
COPY --from=builder /out/gabion-loader /usr/local/bin/gabion-loader
ENTRYPOINT ["/usr/local/bin/gabion-loader"]
