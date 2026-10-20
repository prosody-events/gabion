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
# No cache mounts: the cooked deps must land in this layer to be reusable
# (see deploy/gabiond/Dockerfile).
RUN cargo chef cook --release --package gabion-loader --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build -p gabion-loader --release \
    && mkdir -p /out \
    && cp target/release/gabion-loader /out/gabion-loader \
    && rm -rf target

FROM alpine:3.24
RUN apk add --no-cache ca-certificates
COPY --from=builder /out/gabion-loader /usr/local/bin/gabion-loader
ENTRYPOINT ["/usr/local/bin/gabion-loader"]
