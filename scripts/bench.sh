#!/usr/bin/env bash
source "$(dirname "$0")/common.sh"
require_php

go test -run '^$' -bench . -benchtime "${BENCHTIME:-2s}" -benchmem -ldflags=-linkmode=external -tags="$TAGS" ./...
