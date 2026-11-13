# Changelog

All notable changes are documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- The `websocket` Caddy directive: WebSockets over HTTP/1.1, HTTP/2 and HTTP/3
  for FrankenPHP, handled by a PHP worker.
- The handshake as an ordinary HTTP request to PHP, which accepts or refuses it
  and sets its response headers.
- The `open`, `message`, `ping`, `pong` and `close` events, with the
  connection's attributes, tags and handshake request.
- PHP functions to send, broadcast, send to a tag, ping and close, to query
  connections and to manage their attributes and tags.
- `permessage-deflate` compression, when PHP asks for it.
- Limits on connections, message size and message rate; idle timeout,
  keepalive pings, write and close timeouts.
- A cluster bus over NATS (with credentials, NKeys, tokens, users and TLS) or
  Unix sockets, reaching connections and answering queries across instances.
- A Docker image built on the official FrankenPHP image.
