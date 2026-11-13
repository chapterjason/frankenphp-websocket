# Installation

The extension is compiled into FrankenPHP, like every FrankenPHP extension.

## Docker

The image is on Docker Hub, for `linux/amd64`:

```console
docker pull chapterjason/frankenphp-websocket:latest
```

`latest` is the latest release, `edge` the latest commit on `master`.

The `Dockerfile` builds FrankenPHP with this extension and replaces the binary
of the official `dunglas/frankenphp` image, the way
[FrankenPHP documents custom builds](https://frankenphp.dev/docs/docker/#how-to-install-more-caddy-modules):

```console
docker build -t frankenphp-websocket .
```

The FrankenPHP, Caddy and PHP versions are build arguments
(`FRANKENPHP_VERSION`, `CADDY_VERSION`, `PHP_VERSION`); FrankenPHP and Caddy
must match those in `go.mod`. The image is used like the official one.

## xcaddy

To build a binary, follow [FrankenPHP's compile instructions](https://frankenphp.dev/docs/compile/)
and add the module:

```console
CGO_ENABLED=1 \
XCADDY_GO_BUILD_FLAGS="-ldflags='-w -s' -tags=nobadger,nomysql,nopgx,deprecated_topic,deprecated_claim" \
CGO_CFLAGS=$(php-config --includes) \
CGO_LDFLAGS="$(php-config --ldflags) $(php-config --libs)" \
xcaddy build \
    --output frankenphp \
    --with github.com/dunglas/frankenphp/caddy \
    --with github.com/chapterjason/frankenphp-websocket
```

PHP must be built with ZTS and `--enable-embed`, as FrankenPHP requires. In
this repository, `scripts/build.sh` does the same with the versions of `go.mod`.
