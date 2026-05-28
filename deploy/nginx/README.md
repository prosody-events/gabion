# NGINX images and smoke harness

This directory ships two things:

1. The **published NGINX and OpenResty Docker images** that we build and
   push to GHCR (`ghcr.io/<repo>/nginx:*`, `ghcr.io/<repo>/openresty:*`).
2. The **smoke harness** that exercises those images in CI and locally.

## What the published image is

Each published image is a **drop-in replacement** for its upstream base
(`nginx:stable-alpine`, `openresty/openresty:alpine`, etc.). It
inherits the upstream `ENTRYPOINT`, `EXPOSE`, `STOPSIGNAL`, `WORKDIR`,
env vars, the `/docker-entrypoint.d/` script chain (on nginx), and the
unmodified upstream `/etc/nginx/nginx.conf`. The only delta is:

- `ngx_http_gabion_module.so` lives at `modules/ngx_http_gabion_module.so`
  under each binary's `--modules-path`.
- A one-line shim, `gabion-load-module.conf`, sits next to the upstream
  nginx.conf. It contains exactly:
  ```nginx
  load_module modules/ngx_http_gabion_module.so;
  include <upstream nginx.conf path>;
  ```
- The image's `CMD` is overridden to invoke nginx with `-c <shim>`, so
  the shim is what gets parsed; the shim then includes the upstream (or
  user-supplied) nginx.conf.

The contract this gives the user: **`gabion_*` directives are available
in their config without writing `load_module ...;` themselves**.
Concretely:

- `docker run ghcr.io/<repo>/nginx:stable-alpine` → identical to
  `docker run nginx:stable-alpine`, with the gabion module loaded.
  Serves the upstream "Welcome to nginx" page on `:80`.
- `docker run -v ./my-nginx.conf:/etc/nginx/nginx.conf
  ghcr.io/<repo>/nginx:stable-alpine` → the user's nginx.conf is
  parsed; `gabion_limit_zone`, `gabion_limit_rule`, `gabion_limit`,
  etc. work directly. No `load_module` needed.
- `FROM ghcr.io/<repo>/nginx:stable-alpine` in a downstream Dockerfile
  works the same way — `COPY my-nginx.conf /etc/nginx/nginx.conf` and
  you're done.

Users writing their own nginx.conf must **not** declare
`load_module ngx_http_gabion_module.so;` themselves — the shim already
does, and nginx errors on a double-load.

The published Dockerfiles define a single multi-stage build. The
default build target (the trailing `FROM published` alias) is the
published image; passing `--target smoke` selects a separate stage
that adds diagnostic tooling (`curl`, `gdb`, `binutils`) and the smoke
harness scripts and config. The smoke target is opt-in and is only
used by the smoke services in `docker-compose.yml`.

# NGINX smoke harness

The smoke harness builds the Gabion NGINX module against an upstream
base image, loads it into a running NGINX, and asserts that the
configured rules admit and reject requests as expected. Running it
regularly catches breakage in the FFI glue, in the Rust module, or in
the tracked NGINX version before a production operator runs into the
same problem. The Makefile is the front door to the harness, and each
of its targets delegates to one of the shell scripts described below;
you can also invoke those scripts directly when you need to debug a
failing build or extend the matrix.

## Validate the base image

Confirm the unmodified `nginx:stable-alpine` config we ship still
parses cleanly:

```sh
make nginx-config
```

## Build the module

Build `gabion-nginx` with the `ngx-module` feature against
`nginx:stable-alpine` and run `nginx -t` against a config that loads
the resulting `.so`:

```sh
make nginx-module
```

Override the base image:

```sh
NGINX_BASE_IMAGE=nginx:mainline-alpine make nginx-module
```

## Run the request-level assertions

```sh
make nginx-test
```

This target builds the module, starts NGINX with the smoke config, and
runs `deploy/nginx/module-request-smoke.sh`. The script first checks
that the rendered config carries every directive it should, and then
verifies that the rate-limit rules return `200`, `200`, `429` in
order. When an assertion fails, you can run the script by hand inside
the container to narrow down which step went wrong.

## Build matrices

Sweep the common official NGINX variants, then the OpenResty variants:

```sh
make nginx-matrix
make openresty-matrix
```

Both targets wrap a shell script, so you can override the matrix by
passing bases explicitly to either one, or drive the OpenResty
Dockerfile directly for a one-off build:

```sh
sh deploy/nginx/build-matrix.sh nginx:1.27-alpine nginx:1.27
sh deploy/nginx/build-openresty-matrix.sh openresty/openresty:alpine
docker build -f deploy/nginx/Dockerfile.openresty \
  --build-arg OPENRESTY_BASE_IMAGE=openresty/openresty:alpine \
  -t gabion-openresty:alpine .
```

## What the smoke image does

The module smoke image installs build dependencies into the selected
NGINX base image and then fetches matching NGINX source for the
version reported by `nginx -v`. It builds `gabion-nginx` with the
`ngx-module` feature, copies the resulting `.so` back into the same
base image, and runs either `nginx -t` or the request-level assertions
with `load_module` pointing at the freshly built object.

The OpenResty Dockerfile follows the same flow, except that it
downloads the matching OpenResty source bundle and then points the
Rust build at the bundled nginx source and its generated build
directory.
