# FrankenPHP WebSocket

A [FrankenPHP](https://frankenphp.dev) extension, written in Go, that gives PHP
applications WebSocket support. Go holds the connections; PHP decides on every
handshake, handles every event and pushes data to any connection, from its
workers and from regular requests. Several instances can form a cluster over
NATS or Unix sockets.

- WebSockets over HTTP/1.1 (RFC 6455), HTTP/2 (RFC 8441) and HTTP/3 (RFC 9220)
- The handshake reaches PHP as an ordinary HTTP request, so an application
  firewall applies to it as to any other request
- `permessage-deflate` compression (RFC 7692), when PHP asks for it
- Attributes and tags on connections, sends to a tag or to all connections
- Limits on connections, message size and message rate; idle timeout and
  keepalive pings

## Documentation

- [Installation](docs/installation.md)
- [Configuration](docs/configuration.md)
- [The PHP worker](docs/worker.md)
- [PHP functions](docs/functions.md)
- [Cluster](docs/cluster.md)
- [Performance](docs/performance.md)
- [Known limitations](docs/limitations.md)
- [Development](docs/development.md)
