#!/bin/sh
set -eu

repo="${GABION_OPENRESTY_IMAGE_REPO:-gabion-openresty}"
dockerfile="${GABION_OPENRESTY_DOCKERFILE:-deploy/nginx/Dockerfile.openresty}"
context="${GABION_OPENRESTY_CONTEXT:-.}"

if [ "$#" -gt 0 ]; then
    bases="$*"
else
    bases="${GABION_OPENRESTY_BASE_IMAGES:-openresty/openresty:alpine openresty/openresty:bookworm openresty/openresty:bullseye openresty/openresty:jammy openresty/openresty:focal}"
fi

for base in $bases; do
    tag="$(printf '%s' "$base" | tr '/:' '--')"
    docker build \
        -f "$dockerfile" \
        --build-arg "OPENRESTY_BASE_IMAGE=$base" \
        -t "$repo:$tag" \
        "$context"
done
