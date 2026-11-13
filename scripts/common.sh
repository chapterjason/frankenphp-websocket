# Sourced by the scripts next to it; not meant to be run on its own.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

TAGS="${TAGS:-nobadger,nomysql,nopgx,nowatcher,deprecated_topic,deprecated_claim}"

# php_config prints the php-config of the PHP to build against.
php_config() {
    # Distributions like Fedora ship the ZTS build, which FrankenPHP needs, as php-config-zts.
    local php_config="${PHP_CONFIG:-$(command -v php-config-zts || command -v php-config || true)}"
    if [ -z "$php_config" ]; then
        echo "no php-config found; set PHP_CONFIG to the php-config of a ZTS PHP built with --enable-embed" >&2
        exit 1
    fi
    echo "$php_config"
}

require_php() {
    local php_config
    php_config="$(php_config)"
    export CGO_ENABLED=1
    CGO_CFLAGS="$("$php_config" --includes)"
    # The explicit lib directory is for PHP builds, like Homebrew's, whose ldflags leave it out.
    CGO_LDFLAGS="-L$("$php_config" --prefix)/lib $("$php_config" --ldflags) $("$php_config" --libs)"
    export CGO_CFLAGS CGO_LDFLAGS
}

container() {
    local runtime="${CONTAINER:-$(command -v docker >/dev/null && echo docker || echo podman)}"
    "$runtime" "$@"
}
