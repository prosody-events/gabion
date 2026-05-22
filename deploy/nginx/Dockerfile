# syntax=docker/dockerfile:1.7

ARG NGINX_BASE_IMAGE=nginx:stable-alpine

FROM ${NGINX_BASE_IMAGE} AS chef

ARG NGINX_BASE_IMAGE=nginx:stable-alpine

SHELL ["/bin/sh", "-euxc"]

RUN if command -v apk >/dev/null 2>&1; then \
        apk add --no-cache \
            build-base \
            ca-certificates \
            cargo \
            clang \
            clang-dev \
            curl \
            linux-headers \
            llvm-dev \
            openssl-dev \
            pcre2-dev \
            rust \
            cargo-chef \
            sccache \
            zlib-dev; \
    elif command -v apt-get >/dev/null 2>&1; then \
        apt-get update; \
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
            build-essential \
            ca-certificates \
            cargo \
            clang \
            curl \
            libclang-dev \
            libpcre2-dev \
            libssl-dev \
            llvm-dev \
            rustc \
            sccache \
            zlib1g-dev; \
        rm -rf /var/lib/apt/lists/*; \
    else \
        echo "Unsupported nginx base image package manager: ${NGINX_BASE_IMAGE}" >&2; \
        exit 1; \
    fi

WORKDIR /workspace

RUN --mount=type=cache,id=gabion-cargo-registry,target=/root/.cargo/registry \
    --mount=type=cache,id=gabion-cargo-git,target=/root/.cargo/git \
    if ! command -v cargo-chef >/dev/null 2>&1; then \
        cargo install cargo-chef --locked; \
    fi

RUN nginx_version="$(nginx -v 2>&1 | sed 's#nginx version: nginx/##')" \
    && curl -fsSLO "https://nginx.org/download/nginx-${nginx_version}.tar.gz" \
    && tar -xzf "nginx-${nginx_version}.tar.gz" \
    && cd "nginx-${nginx_version}" \
    && ./configure --with-compat

FROM chef AS planner

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS module-builder

COPY --from=planner /workspace/recipe.json recipe.json

RUN --mount=type=cache,id=gabion-cargo-registry,target=/root/.cargo/registry \
    --mount=type=cache,id=gabion-cargo-git,target=/root/.cargo/git \
    --mount=type=cache,id=gabion-nginx-target,target=/workspace/target \
    --mount=type=cache,id=gabion-nginx-sccache,target=/workspace/.cache/sccache \
    nginx_version="$(nginx -v 2>&1 | sed 's#nginx version: nginx/##')" \
    && SCCACHE_DIR=/workspace/.cache/sccache \
       CARGO_BUILD_RUSTC_WRAPPER=sccache \
       NGINX_SOURCE_DIR="/workspace/nginx-${nginx_version}" \
       NGINX_BUILD_DIR="/workspace/nginx-${nginx_version}/objs" \
       cargo chef cook --release --package gabion-nginx --features ngx-module --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN --mount=type=cache,id=gabion-cargo-registry,target=/root/.cargo/registry \
    --mount=type=cache,id=gabion-cargo-git,target=/root/.cargo/git \
    --mount=type=cache,id=gabion-nginx-target,target=/workspace/target \
    --mount=type=cache,id=gabion-nginx-sccache,target=/workspace/.cache/sccache \
    nginx_version="$(nginx -v 2>&1 | sed 's#nginx version: nginx/##')" \
    && SCCACHE_DIR=/workspace/.cache/sccache \
       CARGO_BUILD_RUSTC_WRAPPER=sccache \
       NGINX_SOURCE_DIR="/workspace/nginx-${nginx_version}" \
       NGINX_BUILD_DIR="/workspace/nginx-${nginx_version}/objs" \
       cargo build -p gabion-nginx --release --features ngx-module \
    && mkdir -p /out \
    && cp target/release/libgabion_nginx.so /out/ngx_http_gabion_module.so

FROM ${NGINX_BASE_IMAGE}

SHELL ["/bin/sh", "-euxc"]

RUN if command -v apk >/dev/null 2>&1; then \
        apk add --no-cache curl; \
    elif command -v apt-get >/dev/null 2>&1; then \
        apt-get update; \
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends curl; \
        rm -rf /var/lib/apt/lists/*; \
    fi

COPY --from=module-builder /out/ngx_http_gabion_module.so /etc/nginx/modules/ngx_http_gabion_module.so
COPY deploy/nginx/nginx.module.conf /etc/nginx/nginx.conf
COPY deploy/nginx/module-request-smoke.sh /usr/local/bin/gabion-nginx-request-smoke

RUN chmod +x /usr/local/bin/gabion-nginx-request-smoke
