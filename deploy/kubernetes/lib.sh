# Polling helpers shared by the kind smokes, sourced after `namespace` is
# set. A cluster has no event we can block on from a shell, so each helper
# re-checks an observable condition once a second: a healthy cluster moves
# on as soon as it is ready, and the attempt budget only bounds failure.

# retry <attempts> <command...>: run the command until it succeeds; name it
# on stderr if the attempts run out.
retry() {
    retry_left="$1"
    shift
    until "$@"; do
        retry_left=$((retry_left - 1))
        if [ "$retry_left" -le 0 ]; then
            printf '%s\n' "gave up waiting for: $*" >&2
            return 1
        fi
        sleep 1
    done
}

endpoint_count_is() {
    [ "$(kubectl -n "$namespace" get endpointslice \
        -l "kubernetes.io/service-name=$1" \
        -o 'jsonpath={range .items[*].endpoints[*]}{.addresses[0]}{"\n"}{end}' \
        | grep -c .)" = "$2" ]
}

# wait_for_endpoint_count <service> <expected>
wait_for_endpoint_count() {
    retry 180 endpoint_count_is "$1" "$2" && return 0
    kubectl -n "$namespace" get endpointslice -o wide >&2
    return 1
}
