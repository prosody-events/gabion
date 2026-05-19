#!/bin/sh
set -eu

nginx -t
rendered="$(nginx -T 2>&1)"

printf '%s\n' "$rendered" | grep -F 'load_module /etc/nginx/modules/ngx_http_gabion_module.so;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_zone api 128m;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_rule uri_api 2r/m key=$uri window=60s bucket=1s overflow=aggregate;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_rule ip_api 2r/m key=$remote_addr window=60s bucket=1s overflow=aggregate;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_rule tenant_api 1r/m key=$arg_tenant key=$uri window=60s bucket=1s overflow=aggregate;'
printf '%s\n' "$rendered" | grep -F 'gabion_gossip_discovery kubernetes;'
printf '%s\n' "$rendered" | grep -F 'gabion_gossip_bind 0.0.0.0:9000;'
printf '%s\n' "$rendered" | grep -F 'gabion_gossip_self 127.0.0.1:9000;'
printf '%s\n' "$rendered" | grep -F 'gabion_gossip_cluster 1;'
printf '%s\n' "$rendered" | grep -F 'gabion_gossip_fanout 8;'
printf '%s\n' "$rendered" | grep -F 'gabion_gossip_payload 64k;'
printf '%s\n' "$rendered" | grep -F 'gabion_gossip_max_cells 1024;'
printf '%s\n' "$rendered" | grep -F 'gabion_gossip_peer 127.0.0.2:9000;'
printf '%s\n' "$rendered" | grep -F 'gabion_gossip_peer_file /etc/gabion/peers.txt;'
printf '%s\n' "$rendered" | grep -F 'gabion_gossip_linger 250ms;'
printf '%s\n' "$rendered" | grep -F 'gabion_gossip_endpoint_slice default gabion-grpc gossip;'
printf '%s\n' "$rendered" | grep -F 'gabion_gossip_endpoint_slice default gabion-nginx gossip;'
printf '%s\n' "$rendered" | grep -F 'gabion off;'

nginx

api_url="http://127.0.0.1:8080/api/index.html"
ip_url="http://127.0.0.1:8080/ip/index.html"
tenant_a_url="http://127.0.0.1:8080/tenant/index.html?tenant=a"
tenant_b_url="http://127.0.0.1:8080/tenant/index.html?tenant=b"
tenant_missing_url="http://127.0.0.1:8080/tenant/index.html"
off_url="http://127.0.0.1:8080/off/index.html"

first="$(curl -fsS -o /dev/null -w '%{http_code}' "$api_url")"
second="$(curl -fsS -o /dev/null -w '%{http_code}' "$api_url")"
third="$(curl -sS -o /dev/null -w '%{http_code}' "$api_url")"

test "$first" = 200
test "$second" = 200
test "$third" = 429

ip_first="$(curl -fsS -o /dev/null -w '%{http_code}' "$ip_url")"
ip_second="$(curl -fsS -o /dev/null -w '%{http_code}' "$ip_url")"
ip_third="$(curl -sS -o /dev/null -w '%{http_code}' "$ip_url")"

test "$ip_first" = 200
test "$ip_second" = 200
test "$ip_third" = 429

tenant_a_first="$(curl -fsS -o /dev/null -w '%{http_code}' "$tenant_a_url")"
tenant_a_second="$(curl -sS -o /dev/null -w '%{http_code}' "$tenant_a_url")"
tenant_b_first="$(curl -fsS -o /dev/null -w '%{http_code}' "$tenant_b_url")"
tenant_missing="$(curl -fsS -o /dev/null -w '%{http_code}' "$tenant_missing_url")"

test "$tenant_a_first" = 200
test "$tenant_a_second" = 429
test "$tenant_b_first" = 200
test "$tenant_missing" = 200

for _ in 1 2 3 4; do
    off_status="$(curl -fsS -o /dev/null -w '%{http_code}' "$off_url")"
    test "$off_status" = 200
done

nginx -s quit
