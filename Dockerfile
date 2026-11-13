# FrankenPHP and Caddy versions must match those in go.mod.
ARG FRANKENPHP_VERSION=1.13.5
ARG CADDY_VERSION=2.11.9
ARG PHP_VERSION=8.5

FROM caddy:${CADDY_VERSION}-builder AS xcaddy

FROM dunglas/frankenphp:${FRANKENPHP_VERSION}-builder-php${PHP_VERSION} AS builder

ARG CADDY_VERSION

COPY --from=xcaddy /usr/bin/xcaddy /usr/bin/xcaddy

COPY . /src/frankenphp-websocket

# The builder image's working directory holds the FrankenPHP sources.
RUN CGO_ENABLED=1 \
    XCADDY_SETCAP=1 \
    XCADDY_GO_BUILD_FLAGS="-ldflags='-w -s' -tags=nobadger,nomysql,nopgx,deprecated_topic,deprecated_claim" \
    CGO_CFLAGS=$(php-config --includes) \
    CGO_LDFLAGS="$(php-config --ldflags) $(php-config --libs)" \
    xcaddy build v${CADDY_VERSION} \
        --output /usr/local/bin/frankenphp \
        --with github.com/dunglas/frankenphp=./ \
        --with github.com/dunglas/frankenphp/caddy=./caddy/ \
        --with github.com/dunglas/caddy-cbrotli \
        --with github.com/dunglas/mercure/caddy \
        --with github.com/dunglas/vulcain/caddy \
        --with github.com/chapterjason/frankenphp-websocket=/src/frankenphp-websocket

FROM dunglas/frankenphp:${FRANKENPHP_VERSION}-php${PHP_VERSION} AS runner

COPY --from=builder /usr/local/bin/frankenphp /usr/local/bin/frankenphp
