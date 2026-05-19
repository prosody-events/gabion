FROM rust:alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /workspace

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build -p gabion-server --release --bin gabiond --no-default-features \
    && mkdir -p /out \
    && cp target/release/gabiond /out/gabiond

FROM alpine:3.22

RUN apk add --no-cache ca-certificates

COPY --from=builder /out/gabiond /usr/local/bin/gabiond

ENTRYPOINT ["/usr/local/bin/gabiond"]
