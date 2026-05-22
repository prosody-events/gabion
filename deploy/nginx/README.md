# NGINX Smoke Harness

Validate the base standard NGINX image:

```sh
make nginx-config
```

Build and validate the Gabion dynamic module against the standard
`nginx:stable-alpine` image:

```sh
make nginx-module
```

Build against a different official NGINX base image:

```sh
NGINX_BASE_IMAGE=nginx:mainline-alpine make nginx-module
```

Build the common official NGINX variants:

```sh
sh deploy/nginx/build-matrix.sh
```

Build an OpenResty image:

```sh
docker build \
  -f deploy/nginx/Dockerfile.openresty \
  --build-arg OPENRESTY_BASE_IMAGE=openresty/openresty:alpine \
  -t gabion-openresty:alpine \
  .
```

Build the common OpenResty variants:

```sh
sh deploy/nginx/build-openresty-matrix.sh
```

Override the matrix by passing bases explicitly:

```sh
sh deploy/nginx/build-matrix.sh nginx:1.27-alpine nginx:1.27
```

Build the module, start NGINX, and verify requests are allowed and then rejected:

```sh
make nginx-test
```

The module smoke image installs build dependencies into the selected NGINX base
image, configures matching NGINX source from the version reported by `nginx -v`,
builds `gabion-nginx` with the `ngx-module` feature, copies the resulting `.so`
into the same selected NGINX base image, and runs `nginx -t` or request-level
assertions with `load_module`.

The OpenResty Dockerfile follows the same flow, but downloads the matching
OpenResty source bundle and points the Rust build at the bundled nginx source
and generated nginx build directory.
