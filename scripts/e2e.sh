#!/usr/bin/env bash
# Runs the image as a two-instance cluster and tests it from outside, with Go clients and real browsers.
source "$(dirname "$0")/common.sh"

compose() {
    container compose -f e2e/compose.yaml --profile browser "$@"
}

trap 'compose down --volumes' EXIT
compose up --build --detach app other relay edge remote
CGO_ENABLED=0 go test -count=1 -tags=e2e ./e2e/
compose run --rm browser
