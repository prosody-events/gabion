# Polling helpers shared by the kind smokes, sourced after `namespace` is
# set. A cluster has no event we can block on from a shell, so each helper
# re-checks an observable condition once a second: a healthy cluster moves
# on as soon as it is ready, and the attempt budget only bounds failure.

# retry <attempts> <command...>: run the command until it succeeds.
retry() {
    retry_left="$1"
    shift
    until "$@"; do
        retry_left=$((retry_left - 1))
        [ "$retry_left" -gt 0 ] || return 1
        sleep 1
    done
}

endpoint_count() {
    kubectl -n "$namespace" get endpointslice \
        -l "kubernetes.io/service-name=$1" \
        -o 'jsonpath={range .items[*].endpoints[*]}{.addresses[0]}{"\n"}{end}' \
        | sed '/^$/d' \
        | wc -l \
        | tr -d ' '
}

endpoint_count_is() {
    [ "$(endpoint_count "$1")" = "$2" ]
}

# wait_for_endpoint_count <service> <expected> [attempts]
wait_for_endpoint_count() {
    retry "${3:-180}" endpoint_count_is "$1" "$2" && return 0
    printf '%s\n' "timed out waiting for $2 EndpointSlice addresses on $1; saw $(endpoint_count "$1")" >&2
    kubectl -n "$namespace" get endpointslice -o wide >&2
    return 1
}
