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

# `nginx_layout` fails the build when nginx-sys disagrees with this nginx's C
# struct layout (see README.md, "Binding layout check").
RUN nginx_version="$(nginx -v 2>&1 | sed 's#nginx version: nginx/##')" \
    && export NGINX_SOURCE_DIR="/workspace/nginx-${nginx_version}" \
              NGINX_BUILD_DIR="/workspace/nginx-${nginx_version}/objs" \
    && cargo build -p gabion-nginx --release --features ngx-module \
    && cargo run -p gabion-nginx --release --features ngx-module --bin nginx_layout \
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

# AddressSanitizer build of nginx and the module (`make nginx-asan`). Needs
# the Debian base (`NGINX_BASE_IMAGE=nginx:stable`): ASan doesn't support
# musl, and nightly comes from the rustup install only that path has.
FROM chef AS asan-builder

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends libclang-rt-dev \
    && rm -rf /var/lib/apt/lists/* \
    && /root/.cargo/bin/rustup toolchain install nightly --profile minimal

# Rebuild the base image's nginx from its own configure arguments with
# clang's ASan. Under `NGX_DEBUG_PALLOC` every pool allocation is its own
# malloc, so ASan bounds it instead of seeing one opaque pool block.
RUN ln -s "nginx-$(nginx -v 2>&1 | sed 's#nginx version: nginx/##')" nginx \
    && cd nginx \
    && eval "./configure $(nginx -V 2>&1 | sed -n 's/^configure arguments: //p') \
        --with-cc=clang \
        --with-cc-opt='-g -O1 -fno-omit-frame-pointer -fsanitize=address -DNGX_DEBUG_PALLOC=1' \
        --with-ld-opt=-fsanitize=address" \
    && make -j"$(nproc)"

# rustc links no sanitizer runtime into a cdylib, so the module's
# `__asan_*` calls bind to the one clang linked into nginx. std stays
# uninstrumented (no `-Zbuild-std`): ASan has no false positives from
# uninstrumented code, and the generic std code gabion uses is
# instrumented where gabion monomorphizes it. `--target` keeps RUSTFLAGS
# off build scripts and proc macros, which run inside uninstrumented
# cargo/rustc. `-z nodelete` keeps the module mapped after nginx's
# exit-time dlclose, so LeakSanitizer still scans its statics and
# symbolizes its frames. The dev profile adds debug assertions.
ENV NGINX_SOURCE_DIR=/workspace/nginx \
    NGINX_BUILD_DIR=/workspace/nginx/objs \
    CARGO_PROFILE_DEV_DEBUG=line-tables-only \
    RUSTFLAGS="-Zsanitizer=address -Clink-arg=-Wl,-z,nodelete"

COPY --from=planner /workspace/recipe.json recipe.json

RUN cargo +nightly chef cook --target "$(uname -m)-unknown-linux-gnu" --package gabion-nginx --features ngx-module --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo +nightly build --target "$(uname -m)-unknown-linux-gnu" -p gabion-nginx --features ngx-module \
    && mkdir -p /out \
    && cp nginx/objs/nginx /out/nginx \
    && cp target/*/debug/libgabion_nginx.so /out/ngx_http_gabion_module.so \
    && rm -rf target

FROM ${NGINX_BASE_IMAGE} AS asan

SHELL ["/bin/sh", "-euxc"]

# `llvm` provides the llvm-symbolizer ASan uses to resolve report frames.
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends llvm \
    && rm -rf /var/lib/apt/lists/*

COPY --from=asan-builder /out/nginx /usr/sbin/nginx
COPY --from=asan-builder /out/ngx_http_gabion_module.so /etc/nginx/modules/ngx_http_gabion_module.so
COPY deploy/nginx/gabion-load-module.conf /etc/nginx/gabion-load-module.conf
COPY deploy/nginx/nginx.smoke.conf /etc/nginx/nginx.conf
COPY deploy/nginx/lsan.supp /etc/nginx/lsan.supp
COPY --chmod=755 deploy/nginx/module-request-smoke.sh /usr/local/bin/gabion-nginx-request-smoke
COPY --chmod=755 deploy/nginx/reload-smoke.sh /usr/local/bin/gabion-nginx-reload-smoke
COPY --chmod=755 deploy/nginx/smoke-with-bt.sh /usr/local/bin/gabion-smoke-with-bt

# Reports go to files, not stderr: nginx respawns a worker ASan aborted, so
# only `gabion-smoke-with-bt` checking /tmp/asan.* can fail the run.
ENV ASAN_OPTIONS=abort_on_error=1:halt_on_error=1:detect_leaks=1:log_path=/tmp/asan \
    LSAN_OPTIONS=suppressions=/etc/nginx/lsan.supp:print_suppressions=0

# Default build target = `published`. The trailing unnamed stage makes
# `docker build -f Dockerfile .` produce the registry-shaped image
# without --target; smoke is opt-in via --target smoke.
FROM published
