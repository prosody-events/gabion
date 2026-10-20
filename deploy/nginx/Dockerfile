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
        # current stable toolchain via rustup. The Alpine path above already \
        # ships new-enough rust/cargo. \
        curl -fsSL https://sh.rustup.rs \
            | sh -s -- -y --default-toolchain stable --profile minimal --no-modify-path; \
        ln -s /root/.cargo/bin/cargo /root/.cargo/bin/rustc /usr/local/bin/; \
    else \
        echo "Unsupported nginx base image package manager: ${NGINX_BASE_IMAGE}" >&2; \
        exit 1; \
    fi

WORKDIR /workspace

RUN if ! command -v cargo-chef >/dev/null 2>&1; then \
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

# No cache mounts: the cooked deps must land in this layer to be reusable
# (see deploy/gabiond/Dockerfile).
RUN nginx_version="$(nginx -v 2>&1 | sed 's#nginx version: nginx/##')" \
    && NGINX_SOURCE_DIR="/workspace/nginx-${nginx_version}" \
       NGINX_BUILD_DIR="/workspace/nginx-${nginx_version}/objs" \
       cargo chef cook --release --package gabion-nginx --features ngx-module --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN nginx_version="$(nginx -v 2>&1 | sed 's#nginx version: nginx/##')" \
    && NGINX_SOURCE_DIR="/workspace/nginx-${nginx_version}" \
       NGINX_BUILD_DIR="/workspace/nginx-${nginx_version}/objs" \
       cargo build -p gabion-nginx --release --features ngx-module \
    && mkdir -p /out \
    && cp target/release/libgabion_nginx.so /out/ngx_http_gabion_module.so \
    && rm -rf target

FROM ${NGINX_BASE_IMAGE} AS published

COPY --from=module-builder /out/ngx_http_gabion_module.so /etc/nginx/modules/ngx_http_gabion_module.so
COPY deploy/nginx/gabion-load-module.conf /etc/nginx/gabion-load-module.conf

CMD ["nginx", "-c", "/etc/nginx/gabion-load-module.conf", "-g", "daemon off;"]

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

# Default build target = `published`. The trailing unnamed stage makes
# `docker build -f Dockerfile .` produce the registry-shaped image
# without --target; smoke is opt-in via --target smoke.
FROM published
