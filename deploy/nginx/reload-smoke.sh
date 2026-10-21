#!/bin/sh
set -eu

# Reload nginx a few times under light traffic: twice with the config
# unchanged (gabion reuses its SHM zone), then twice on a renamed
# `gabion_limit_zone` (a fresh zone replaces the old one). Every reload
# retires the old workers and their leader threads. `make nginx-asan`
# runs this after the request smoke so ASan watches pool, zone and
# leader lifetimes across reload.
shim=/etc/nginx/gabion-load-module.conf
nginx -c "$shim"

while :; do
    curl -s -o /dev/null http://127.0.0.1:8080/api/index.html || :
    curl -s -o /dev/null http://127.0.0.1:8080/stacked/index.html || :
    sleep 0.05
done &
traffic=$!

for zone in api:128m api:128m reloaded:64m reloaded:64m; do
    sed -i "s/gabion_limit_zone zone=[^;]*;/gabion_limit_zone zone=$zone;/" /etc/nginx/nginx.conf
    nginx -c "$shim" -s reload
    sleep 1
done

kill "$traffic"
curl -fsS -o /dev/null http://127.0.0.1:8080/inherits/index.html
nginx -c "$shim" -s quit
