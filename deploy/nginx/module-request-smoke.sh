#!/bin/sh
set -eu

# The published image's contract is "run nginx -c <shim>"; the shim does
# `load_module modules/ngx_http_gabion_module.so;` and includes the
# user's nginx.conf. The smoke harness exercises that exact path —
# nginx.smoke.conf is mounted as /etc/nginx/nginx.conf and carries no
# `load_module` of its own. Detect openresty vs nginx so the same script
# works against both published images.
if command -v openresty >/dev/null 2>&1; then
    SHIM_CONF="/usr/local/openresty/nginx/conf/gabion-load-module.conf"
else
    SHIM_CONF="/etc/nginx/gabion-load-module.conf"
fi

nginx -t -c "$SHIM_CONF"
rendered="$(nginx -T -c "$SHIM_CONF" 2>&1)"

# Module loads and core directives are present at the expected level.
# `load_module` lives in the shim in its relative form
# (`modules/ngx_http_gabion_module.so`), resolved against each binary's
# configured `--modules-path`.
printf '%s\n' "$rendered" | grep -F 'load_module modules/ngx_http_gabion_module.so;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_zone zone=api:128m;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_rule uri_api    $uri                            rate=2r/m bucket=1s;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_rule ip_api     $remote_addr                    rate=2r/m bucket=1s;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_rule tenant_api tenant:$arg_tenant path:$uri    rate=1r/m bucket=1s;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_rule per_ip_stacked     ip:$remote_addr    rate=2r/m bucket=1s except_if=$trusted_ip;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_rule per_bot_stacked    class:$bot_class   rate=5r/m bucket=1s;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_rule shadow_canary      $uri               rate=1r/s bucket=1s dry_run;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_rule baseline_rule      $uri               rate=3r/m bucket=1s;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit baseline_rule;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_rule window_demo       demo:$arg_demo     rate=2r/m window=2m bucket=30s;'
printf '%s\n' "$rendered" | grep -F 'gabion_gossip_bind 0.0.0.0:9000;'
printf '%s\n' "$rendered" | grep -F 'gabion_gossip_cluster 1;'
printf '%s\n' "$rendered" | grep -F 'gabion_gossip_fanout 8;'
printf '%s\n' "$rendered" | grep -F 'gabion off;'

nginx -c "$SHIM_CONF"

api_url="http://127.0.0.1:8080/api/index.html"
ip_url="http://127.0.0.1:8080/ip/index.html"
tenant_a_url="http://127.0.0.1:8080/tenant/index.html?tenant=a"
tenant_b_url="http://127.0.0.1:8080/tenant/index.html?tenant=b"
tenant_missing_url="http://127.0.0.1:8080/tenant/index.html"
off_url="http://127.0.0.1:8080/off/index.html"
dryrun_url="http://127.0.0.1:8080/dryrun/index.html"

# `curl_headers <url>` runs a GET and emits only the response headers on
# stdout (status line + headers, body discarded), so assertions can grep
# both the HTTP code and the `X-RateLimit-*` triplet from a single call.
curl_headers() {
    curl -sS -D - -o /dev/null "$1"
}

# `assert_header_present <headers-blob> <name>` exits non-zero with a
# diagnostic when the named header is missing. Case-insensitive on the
# name because nginx may normalise the casing.
assert_header_present() {
    if ! printf '%s\n' "$1" | grep -i -q "^$2:"; then
        echo "expected header '$2' missing from response:"
        printf '%s\n' "$1"
        exit 1
    fi
}

# `assert_header_absent <headers-blob> <name>` is the dual — used to
# verify allowed responses don't carry `Retry-After`.
assert_header_absent() {
    if printf '%s\n' "$1" | grep -i -q "^$2:"; then
        echo "unexpected header '$2' present on response:"
        printf '%s\n' "$1"
        exit 1
    fi
}

# `assert_header_equals <headers-blob> <name> <expected>` exits non-zero
# unless the named header's value equals `<expected>` exactly. Uses
# `tr -d` to strip carriage returns rather than bash-only `$'...'`
# escapes, since this script targets `#!/bin/sh` (dash on the nginx
# image).
assert_header_equals() {
    actual="$(printf '%s\n' "$1" | grep -i "^$2:" | head -n1 \
        | sed -e 's/^[^:]*:[[:space:]]*//' -e 's/[[:space:]]*$//' | tr -d '\r')"
    if [ "$actual" != "$3" ]; then
        echo "header '$2': expected '$3', got '$actual'"
        printf '%s\n' "$1"
        exit 1
    fi
}

# Gossip-pipeline settle time. Each request flows
# worker→SHM queue→leader→GossipRuntime→AggregateStore before the next
# worker sees it; under load this is usually <50ms but Docker on macOS
# can jitter higher. A short sleep between the at-budget batch and the
# over-budget probe keeps the assertions deterministic.
SETTLE_SLEEP="${GABION_SMOKE_SETTLE_SLEEP:-0.25}"

# /api/ — uri_api at 2r/m: first two pass, third gets 429. Each response
# must carry `X-RateLimit-{Limit,Remaining,Reset}`; the 429 also gets
# `Retry-After`.
first_h="$(curl_headers "$api_url")"
first="$(printf '%s\n' "$first_h" | head -n1 | awk '{print $2}')"
test "$first" = 200
assert_header_present "$first_h" 'X-RateLimit-Limit'
assert_header_present "$first_h" 'X-RateLimit-Remaining'
assert_header_present "$first_h" 'X-RateLimit-Reset'
assert_header_absent  "$first_h" 'Retry-After'

second_h="$(curl_headers "$api_url")"
second="$(printf '%s\n' "$second_h" | head -n1 | awk '{print $2}')"
test "$second" = 200
assert_header_present "$second_h" 'X-RateLimit-Limit'
assert_header_present "$second_h" 'X-RateLimit-Remaining'
assert_header_present "$second_h" 'X-RateLimit-Reset'
assert_header_absent  "$second_h" 'Retry-After'

sleep "$SETTLE_SLEEP"
third_h="$(curl_headers "$api_url")"
third="$(printf '%s\n' "$third_h" | head -n1 | awk '{print $2}')"
test "$third" = 429
assert_header_present "$third_h" 'X-RateLimit-Limit'
assert_header_equals  "$third_h" 'X-RateLimit-Remaining' '0'
assert_header_present "$third_h" 'X-RateLimit-Reset'
assert_header_present "$third_h" 'Retry-After'

# /ip/ — ip_api keyed on $remote_addr at 2r/m: same shape as /api/.
ip_first="$(curl -fsS -o /dev/null -w '%{http_code}' "$ip_url")"
ip_second="$(curl -fsS -o /dev/null -w '%{http_code}' "$ip_url")"
sleep "$SETTLE_SLEEP"
ip_third="$(curl -sS -o /dev/null -w '%{http_code}' "$ip_url")"

test "$ip_first" = 200
test "$ip_second" = 200
test "$ip_third" = 429

# /tenant/ — tenant_api at 1r/m keyed on (tenant, path): each distinct
# tenant gets its own bucket. Missing $arg_tenant declines (allow-by-default
# under the binding model).
tenant_a_first="$(curl -fsS -o /dev/null -w '%{http_code}' "$tenant_a_url")"
sleep "$SETTLE_SLEEP"
tenant_a_second="$(curl -sS -o /dev/null -w '%{http_code}' "$tenant_a_url")"
tenant_b_first="$(curl -fsS -o /dev/null -w '%{http_code}' "$tenant_b_url")"
tenant_missing="$(curl -fsS -o /dev/null -w '%{http_code}' "$tenant_missing_url")"

test "$tenant_a_first" = 200
test "$tenant_a_second" = 429
test "$tenant_b_first" = 200
test "$tenant_missing" = 200

# /off/ — `gabion off` suppresses the access handler entirely.
for _ in 1 2 3 4; do
    off_status="$(curl -fsS -o /dev/null -w '%{http_code}' "$off_url")"
    test "$off_status" = 200
done

# /stacked/ — three rules layered: per-IP rule with except_if=$trusted_ip,
# per-UA-class rule capped at 5r/m, and a dry_run shadow that never rejects.
# All requests come from 127.0.0.1 so $trusted_ip=1 → per_ip_stacked is
# exempted. shadow_canary records hits but never rejects regardless of rate.
# That leaves per_bot_stacked (5r/m, default UA → "other") as the only
# enforcer. First five pass, sixth gets 429.
stacked_url="http://127.0.0.1:8080/stacked/index.html"
for i in 1 2 3 4 5; do
    stacked_status="$(curl -fsS -o /dev/null -w '%{http_code}' "$stacked_url")"
    test "$stacked_status" = 200 || { echo "request $i expected 200, got $stacked_status"; exit 1; }
done
sleep "$SETTLE_SLEEP"
stacked_429="$(curl -sS -o /dev/null -w '%{http_code}' "$stacked_url")"
test "$stacked_429" = 429

# A distinct UA class gets its own per_bot_stacked bucket — five
# Googlebot requests still pass even though "other" has been exhausted.
googlebot_url="$stacked_url"
for i in 1 2 3 4 5; do
    bot_status="$(curl -fsS -A 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)' \
        -o /dev/null -w '%{http_code}' "$googlebot_url")"
    test "$bot_status" = 200 || { echo "googlebot request $i expected 200, got $bot_status"; exit 1; }
done
sleep "$SETTLE_SLEEP"
googlebot_429="$(curl -sS -A 'Googlebot/2.1' -o /dev/null -w '%{http_code}' "$googlebot_url")"
test "$googlebot_429" = 429

# /inherits/ — no `gabion_limit` declared at this location, so the
# http-level `gabion_limit baseline_rule` (3r/m on $uri) is inherited.
# First three requests pass, fourth gets 429.
inherits_url="http://127.0.0.1:8080/inherits/index.html"
for i in 1 2 3; do
    inh_status="$(curl -fsS -o /dev/null -w '%{http_code}' "$inherits_url")"
    test "$inh_status" = 200 || { echo "inherits request $i expected 200, got $inh_status"; exit 1; }
done
sleep "$SETTLE_SLEEP"
inherits_429="$(curl -sS -o /dev/null -w '%{http_code}' "$inherits_url")"
test "$inherits_429" = 429

# /overrides/ — `gabion_limit uri_api;` REPLACES the inherited baseline;
# uri_api is 2r/m. Only uri_api gates this location; baseline_rule
# does not apply, so /overrides/ has its own independent budget keyed on
# this distinct $uri. (uri_api was already exhausted earlier in this
# script via /api/, so this URI's bucket is fresh — first two pass.)
overrides_url="http://127.0.0.1:8080/overrides/index.html"
o_first="$(curl -fsS -o /dev/null -w '%{http_code}' "$overrides_url")"
o_second="$(curl -fsS -o /dev/null -w '%{http_code}' "$overrides_url")"
sleep "$SETTLE_SLEEP"
o_third="$(curl -sS -o /dev/null -w '%{http_code}' "$overrides_url")"
test "$o_first" = 200
test "$o_second" = 200
test "$o_third" = 429

# /dryrun/ — shadow_canary at 1r/s in DryRun mode. Every request is
# allowed (DryRun never rejects), but `X-RateLimit-Remaining: 0` rides
# the response so operators can graph "share that would have been
# 429'd". No `Retry-After` because no admission decision was negative.
dryrun_h="$(curl_headers "$dryrun_url")"
dryrun_status="$(printf '%s\n' "$dryrun_h" | head -n1 | awk '{print $2}')"
test "$dryrun_status" = 200
assert_header_present "$dryrun_h" 'X-RateLimit-Limit'
assert_header_equals  "$dryrun_h" 'X-RateLimit-Remaining' '0'
assert_header_present "$dryrun_h" 'X-RateLimit-Reset'
assert_header_absent  "$dryrun_h" 'Retry-After'

nginx -c "$SHIM_CONF" -s quit
