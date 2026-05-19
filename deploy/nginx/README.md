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

Build the module, start NGINX, and verify requests are allowed and then rejected:

```sh
make nginx-test
```

The module smoke image installs build dependencies into an `nginx:stable-alpine`
builder stage, configures matching NGINX source, builds `gabion-nginx` with the
`ngx-module` feature, copies the resulting `.so` into the final standard NGINX
image, and runs `nginx -t` or request-level assertions with `load_module`.
