#!/bin/sh
set -eu

repo="${GABION_OPENRESTY_IMAGE_REPO:-gabion-openresty}"
dockerfile="${GABION_OPENRESTY_DOCKERFILE:-deploy/nginx/Dockerfile.openresty}"
context="${GABION_OPENRESTY_CONTEXT:-.}"

if [ "$#" -gt 0 ]; then
    bases="$*"
else
    bases="${GABION_OPENRESTY_BASES:-alpine bookworm openresty/openresty:bullseye jammy openresty/openresty:focal}"
fi

for base in $bases; do
    tag="$(printf '%s' "$base" | tr '/:' '--')"
    docker build \
        -f "$dockerfile" \
        --build-arg "OPENRESTY_BASE=$base" \
        -t "$repo:$tag" \
        "$context"
done
