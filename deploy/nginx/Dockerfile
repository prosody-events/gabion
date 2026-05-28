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
            clang \
            curl \
            libclang-dev \
            libpcre2-dev \
            libssl-dev \
            llvm-dev \
            zlib1g-dev; \
        rm -rf /var/lib/apt/lists/*; \
        # Distro `rustc`/`cargo` on Debian 12 / Ubuntu 22.04 are too old for \
        # cargo-chef's MSRV (1.86+) and can't read Cargo.lock v4. Install a \
        # current stable toolchain via rustup, and grab a prebuilt `sccache` \
        # (Ubuntu 22.04 lacks an apt package; building from source is slow). \
        # The Alpine path above already ships new-enough rust/cargo/sccache. \
        curl -fsSL https://sh.rustup.rs \
            | sh -s -- -y --default-toolchain stable --profile minimal --no-modify-path; \
        ln -s /root/.cargo/bin/cargo /root/.cargo/bin/rustc /usr/local/bin/; \
        case "$(uname -m)" in \
            x86_64)  sccache_arch=x86_64-unknown-linux-musl ;; \
            aarch64) sccache_arch=aarch64-unknown-linux-musl ;; \
            *) echo "Unsupported architecture for sccache: $(uname -m)" >&2; exit 1 ;; \
        esac; \
        sccache_version=v0.15.0; \
        curl -fsSL "https://github.com/mozilla/sccache/releases/download/${sccache_version}/sccache-${sccache_version}-${sccache_arch}.tar.gz" \
            | tar -xz --strip-components=1 -C /usr/local/bin "sccache-${sccache_version}-${sccache_arch}/sccache"; \
        chmod +x /usr/local/bin/sccache; \
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

# The published image: identical to its upstream base image (entrypoint
# chain, /etc/nginx/nginx.conf, default server, env, EXPOSE, STOPSIGNAL
# all inherited) except that the gabion module is auto-loaded via a
# one-line shim invoked with `-c`. A user mounting their own nginx.conf
# at /etc/nginx/nginx.conf gets `gabion_*` directives without needing
# `load_module ...;` of their own.
FROM ${NGINX_BASE_IMAGE} AS published

COPY --from=module-builder /out/ngx_http_gabion_module.so /etc/nginx/modules/ngx_http_gabion_module.so
COPY deploy/nginx/gabion-load-module.conf /etc/nginx/gabion-load-module.conf

CMD ["nginx", "-c", "/etc/nginx/gabion-load-module.conf", "-g", "daemon off;"]

# Smoke variant: published image + diagnostic tooling (curl, gdb,
# binutils) + the smoke harness scripts and config. Opt-in via
# `--target smoke`; deploy/nginx/docker-compose.yml selects it for the
# `make nginx-module`/`nginx-test` services so the smoke rig tests the
# same published image the registry ships.
FROM published AS smoke

SHELL ["/bin/sh", "-euxc"]

RUN if command -v apk >/dev/null 2>&1; then \
        apk add --no-cache curl gdb binutils; \
    elif command -v apt-get >/dev/null 2>&1; then \
        apt-get update; \
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends curl gdb binutils; \
        rm -rf /var/lib/apt/lists/*; \
    fi

COPY deploy/nginx/nginx.smoke.conf /etc/nginx/nginx.conf
COPY deploy/nginx/module-request-smoke.sh /usr/local/bin/gabion-nginx-request-smoke
COPY deploy/nginx/smoke-with-bt.sh /usr/local/bin/gabion-smoke-with-bt

RUN chmod +x /usr/local/bin/gabion-nginx-request-smoke /usr/local/bin/gabion-smoke-with-bt

# Default build target = the published image. The unnamed final stage is
# an alias for `published` so `docker build -f Dockerfile .` (and every
# CI build that doesn't pass --target) produces the registry-shaped
# image. The `smoke` stage above is opt-in via --target smoke.
FROM published
