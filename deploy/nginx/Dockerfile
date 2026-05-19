FROM nginx:stable-alpine AS module-builder

RUN apk add --no-cache \
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
    zlib-dev

WORKDIR /workspace

RUN nginx_version="$(nginx -v 2>&1 | sed 's#nginx version: nginx/##')" \
    && curl -fsSLO "https://nginx.org/download/nginx-${nginx_version}.tar.gz" \
    && tar -xzf "nginx-${nginx_version}.tar.gz" \
    && cd "nginx-${nginx_version}" \
    && ./configure --with-compat

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN nginx_version="$(nginx -v 2>&1 | sed 's#nginx version: nginx/##')" \
    && NGINX_SOURCE_DIR="/workspace/nginx-${nginx_version}" \
       NGINX_BUILD_DIR="/workspace/nginx-${nginx_version}/objs" \
       cargo build -p gabion-nginx --release --features ngx-module \
    && mkdir -p /out \
    && cp target/release/libgabion_nginx.so /out/ngx_http_gabion_module.so

FROM nginx:stable-alpine

RUN apk add --no-cache curl

COPY --from=module-builder /out/ngx_http_gabion_module.so /etc/nginx/modules/ngx_http_gabion_module.so
COPY deploy/nginx/nginx.module.conf /etc/nginx/nginx.conf
COPY deploy/nginx/module-request-smoke.sh /usr/local/bin/gabion-nginx-request-smoke

RUN chmod +x /usr/local/bin/gabion-nginx-request-smoke
