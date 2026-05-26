#!/bin/sh
# Run the wrapped smoke command. On any non-zero exit, look for a
# core file under /tmp/cores, render a full backtrace with gdb, and
# exit with the original return code. The release profile carries
# `debug = "line-tables-only"` (root Cargo.toml) so the backtrace
# resolves addresses to Rust source file:line.
#
# Cores only land in /tmp/cores when the host's `kernel.core_pattern`
# is pointed there *and* the container is launched with
# `--ulimit core=-1 -v /tmp/cores:/tmp/cores`. The CI workflows
# (`.github/workflows/nginx.yml`, `.github/workflows/kubernetes.yml`)
# arrange both. Outside that environment this wrapper is still safe;
# it just won't find anything to backtrace.

set -u

ulimit -c unlimited 2>/dev/null || true
mkdir -p /tmp/cores

# Run the wrapped command directly so its real exit status is visible
# — `set -e` would mask SIGSEGV.
"$@"
rc=$?
[ "$rc" -eq 0 ] && exit 0

printf '\n=== gabion smoke exited rc=%s ===\n' "$rc" >&2

# Resolve the nginx binary that crashed. `command -v` works for both
# Alpine (/usr/sbin/nginx) and OpenResty (/usr/local/openresty/nginx/sbin/nginx).
nginx_bin="$(command -v nginx 2>/dev/null || true)"
[ -n "$nginx_bin" ] || nginx_bin=/usr/sbin/nginx

found_any=0
for c in /tmp/cores/core.* /core /core.*; do
    [ -e "$c" ] || continue
    found_any=1
    printf '=== gdb -batch on %s (binary=%s) ===\n' "$c" "$nginx_bin" >&2
    gdb -batch \
        -ex 'set print frame-arguments all' \
        -ex 'set print pretty on' \
        -ex 'info shared' \
        -ex 'thread apply all bt full' \
        "$nginx_bin" "$c" >&2 || true
done

if [ "$found_any" -eq 0 ]; then
    printf '(no core files found under /tmp/cores)\n' >&2
fi

exit "$rc"
