#!/usr/bin/env bash
source "$(dirname "$0")/common.sh"
require_php

unformatted="$(find . -name '*.go' -not -path './.*' -exec gofmt -l {} +)"
if [ -n "$unformatted" ]; then
    echo "not gofmt-ed:"
    echo "$unformatted"
    exit 1
fi
go vet -tags="$TAGS,e2e" ./...
