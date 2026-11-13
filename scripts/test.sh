#!/usr/bin/env bash
source "$(dirname "$0")/common.sh"
require_php

go test -race -ldflags=-linkmode=external -tags="$TAGS" ./...
