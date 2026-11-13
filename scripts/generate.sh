#!/usr/bin/env bash
# Regenerates the PHP glue of the extensions, internal/websocket from websocket.go and
# internal/clusterphp from cluster.go, with FrankenPHP's extension generator.
source "$(dirname "$0")/common.sh"

frankenphp="${FRANKENPHP:-./frankenphp}"
if [ ! -x "$frankenphp" ]; then
    echo "$frankenphp is missing; build it with scripts/build.sh, or set FRANKENPHP" >&2
    exit 1
fi
frankenphp="$(realpath "$frankenphp")"

# gen_stub.php comes with the sources of the PHP version built against; it installs the PHP-Parser it needs next to itself.
gen_stub_script="${GEN_STUB_SCRIPT:-}"
if [ -z "$gen_stub_script" ]; then
    version="$("$(php_config)" --version)"
    gen_stub_script="$PWD/bin/php-src-$version/build/gen_stub.php"
    if [ ! -f "$gen_stub_script" ]; then
        mkdir -p "bin/php-src-$version"
        curl -fsSL "https://www.php.net/distributions/php-$version.tar.xz" |
            tar -xJ -C "bin/php-src-$version" --strip-components 1 "php-$version/build/gen_stub.php"
    fi
fi
gen_stub_script="$(realpath "$gen_stub_script")"

generate() {
    local dir="$1" source="$2"

    # The generator writes next to its source, and a README.md there too, which is not wanted.
    if [ -e "$dir/README.md" ]; then
        echo "$dir/README.md exists; the generator would keep it" >&2
        exit 1
    fi
    (cd "$dir" && GEN_STUB_SCRIPT="$gen_stub_script" "$frankenphp" extension-init "$source")
    rm "$dir/README.md"
}

generate internal/websocket websocket.go
generate internal/clusterphp cluster.go
