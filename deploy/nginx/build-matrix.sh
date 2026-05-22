#!/bin/sh
set -eu

repo="${GABION_NGINX_IMAGE_REPO:-gabion-nginx}"
dockerfile="${GABION_NGINX_DOCKERFILE:-deploy/nginx/Dockerfile}"
context="${GABION_NGINX_CONTEXT:-.}"

if [ "$#" -gt 0 ]; then
    bases="$*"
else
    bases="${GABION_NGINX_BASE_IMAGES:-nginx:stable-alpine nginx:mainline-alpine nginx:alpine nginx:stable nginx:mainline nginx:latest}"
fi

for base in $bases; do
    tag="$(printf '%s' "$base" | tr '/:' '--')"
    docker build \
        -f "$dockerfile" \
        --build-arg "NGINX_BASE_IMAGE=$base" \
        -t "$repo:$tag" \
        "$context"
done
