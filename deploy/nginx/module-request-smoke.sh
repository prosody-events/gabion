#!/bin/sh
set -eu

nginx -t
rendered="$(nginx -T 2>&1)"

# Module loads and core directives are present at the expected level.
printf '%s\n' "$rendered" | grep -F 'load_module /etc/nginx/modules/ngx_http_gabion_module.so;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_zone zone=api:128m;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_rule uri_api    $uri                            rate=2r/m window=60s bucket=1s;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_rule ip_api     $remote_addr                    rate=2r/m window=60s bucket=1s;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_rule tenant_api tenant:$arg_tenant path:$uri    rate=1r/m window=60s bucket=1s;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_rule per_ip_stacked     ip:$remote_addr    rate=2r/m window=60s bucket=1s except_if=$trusted_ip;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_rule per_bot_stacked    class:$bot_class   rate=5r/m window=60s bucket=1s;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_rule shadow_canary      $uri               rate=1r/s window=1s bucket=1s dry_run;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit_rule baseline_rule      $uri               rate=3r/m window=60s bucket=1s;'
printf '%s\n' "$rendered" | grep -F 'gabion_limit baseline_rule;'
printf '%s\n' "$rendered" | grep -F 'gabion_gossip_bind 0.0.0.0:9000;'
printf '%s\n' "$rendered" | grep -F 'gabion_gossip_cluster 1;'
printf '%s\n' "$rendered" | grep -F 'gabion_gossip_fanout 8;'
printf '%s\n' "$rendered" | grep -F 'gabion off;'

nginx

api_url="http://127.0.0.1:8080/api/index.html"
ip_url="http://127.0.0.1:8080/ip/index.html"
tenant_a_url="http://127.0.0.1:8080/tenant/index.html?tenant=a"
tenant_b_url="http://127.0.0.1:8080/tenant/index.html?tenant=b"
tenant_missing_url="http://127.0.0.1:8080/tenant/index.html"
off_url="http://127.0.0.1:8080/off/index.html"

# Gossip-pipeline settle time. Each request flows
# worker→SHM queue→leader→GossipRuntime→AggregateStore before the next
# worker sees it; under load this is usually <50ms but Docker on macOS
# can jitter higher. A short sleep between the at-budget batch and the
# over-budget probe keeps the assertions deterministic.
SETTLE_SLEEP="${GABION_SMOKE_SETTLE_SLEEP:-0.25}"

# /api/ — uri_api at 2r/m: first two pass, third gets 429.
first="$(curl -fsS -o /dev/null -w '%{http_code}' "$api_url")"
second="$(curl -fsS -o /dev/null -w '%{http_code}' "$api_url")"
sleep "$SETTLE_SLEEP"
third="$(curl -sS -o /dev/null -w '%{http_code}' "$api_url")"

test "$first" = 200
test "$second" = 200
test "$third" = 429

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

nginx -s quit
