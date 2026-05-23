# NGINX smoke harness

The smoke harness builds the Gabion NGINX module against an upstream
base image, loads it into a running NGINX, and asserts that the
configured rules admit and reject requests as expected. It catches
breakage in the FFI glue, the Rust module, or the tracked NGINX
version before a production operator hits it. The Makefile is the
front door; the shell scripts below are what those targets invoke —
use them directly to debug a failing build or extend the matrix.

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

This builds the module, starts NGINX with the smoke config, and runs
`deploy/nginx/module-request-smoke.sh`. The script checks that the
rendered config carries every directive it should and that the
rate-limit rules return `200`, `200`, `429` in order. Run it by hand
inside the container when you need to narrow down a failing assertion.

## Build matrices

Sweep the common official NGINX variants, then the OpenResty variants:

```sh
make nginx-matrix
make openresty-matrix
```

Both targets wrap a shell script. Override the matrix by passing bases
explicitly to either, or drive the OpenResty Dockerfile directly for a
one-off build:

```sh
sh deploy/nginx/build-matrix.sh nginx:1.27-alpine nginx:1.27
sh deploy/nginx/build-openresty-matrix.sh openresty/openresty:alpine
docker build -f deploy/nginx/Dockerfile.openresty \
  --build-arg OPENRESTY_BASE_IMAGE=openresty/openresty:alpine \
  -t gabion-openresty:alpine .
```

## What the smoke image does

The module smoke image installs build dependencies into the selected
NGINX base image and fetches matching NGINX source from the version
reported by `nginx -v`. It builds `gabion-nginx` with the `ngx-module`
feature, copies the resulting `.so` back into the same base image, and
runs `nginx -t` or the request-level assertions with `load_module`.

The OpenResty Dockerfile follows the same flow, but downloads the
matching OpenResty source bundle and points the Rust build at the
bundled nginx source and generated build directory.
