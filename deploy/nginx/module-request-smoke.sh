#!/bin/sh
set -eu

nginx

url="http://127.0.0.1:8080/api/index.html"

first="$(curl -fsS -o /dev/null -w '%{http_code}' "$url")"
second="$(curl -fsS -o /dev/null -w '%{http_code}' "$url")"
third="$(curl -sS -o /dev/null -w '%{http_code}' "$url")"

test "$first" = 200
test "$second" = 200
test "$third" = 429

nginx -s quit
