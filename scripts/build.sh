#!/usr/bin/env bash
source "$(dirname "$0")/common.sh"
require_php

frankenphp_version="$(go list -m -f '{{.Version}}' github.com/dunglas/frankenphp)"
caddy_version="$(go list -m -f '{{.Version}}' github.com/caddyserver/caddy/v2)"
# Pinned, as older xcaddy releases ignore XCADDY_GO_BUILD_FLAGS and with it the build tags.
xcaddy="${XCADDY:-go run github.com/caddyserver/xcaddy/cmd/xcaddy@v0.4.7}"

XCADDY_GO_BUILD_FLAGS="-ldflags='-w -s' -tags=$TAGS" $xcaddy build "$caddy_version" \
    --output frankenphp \
    --with "github.com/dunglas/frankenphp@$frankenphp_version" \
    --with "github.com/dunglas/frankenphp/caddy@$frankenphp_version" \
    --with github.com/chapterjason/frankenphp-websocket=.
