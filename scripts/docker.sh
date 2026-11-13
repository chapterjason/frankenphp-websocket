#!/usr/bin/env bash
source "$(dirname "$0")/common.sh"

container build -t "${IMAGE:-frankenphp-websocket}" .
