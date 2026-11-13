#!/usr/bin/env bash
source "$(dirname "$0")/common.sh"
require_php

version=v2.15.0
golangci_lint="$PWD/bin/golangci-lint-$version"
if [ ! -x "$golangci_lint" ]; then
    mkdir -p bin
    # Built without cgo, which golangci-lint does not need and PHP's linker flags would break.
    CGO_ENABLED=0 GOBIN="$PWD/bin" go install "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$version"
    mv bin/golangci-lint "$golangci_lint"
fi
"$golangci_lint" run ./...
