# FrankenPHP WebSocket

[![CI](https://github.com/chapterjason/frankenphp-websocket/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/chapterjason/frankenphp-websocket/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A [FrankenPHP](https://frankenphp.dev) extension, written in Go, that gives PHP
applications WebSocket support. Go holds the connections; PHP decides on every
handshake, handles every event and pushes data to any connection, from its
workers and from regular requests. Several instances can form a cluster,
connected directly or through relays.

- WebSockets over HTTP/1.1 (RFC 6455), HTTP/2 (RFC 8441) and HTTP/3 (RFC 9220)
- The handshake reaches PHP as an ordinary HTTP request, so an application
  firewall applies to it as to any other request
- `permessage-deflate` compression (RFC 7692), when PHP asks for it
- Attributes and tags on connections, sends to a tag or to all connections
- Limits on connections, message size and message rate; idle timeout and
  keepalive pings
- Clusters of servers, connected directly or through relays, so any server
  reaches every connection
- PHP messaging between the servers of a cluster, with or without WebSockets
- Queue workers, schedulers and other PHP commands in the cluster, without
  accepting connections

## How it fits together

Clients connect to a server; servers can form a cluster:

```
┌──────────────────┐                      ┌──────────┐                  ┌─────────────────┐
│     Clients      │  WebSocket over      │          │     cluster      │  Other servers  │
│ (browsers, apps) │═════════════════════▶│  Server  │◀════════════════▶│ (mesh, relays)  │
│                  │  HTTP/1.1, /2, /3    │          │                  │                 │
└──────────────────┘                      └──────────┘                  └─────────────────┘
```

Inside a server, the extension holds the clients' connections and hands their
events to a PHP worker; PHP pushes back from that worker and from regular
requests. As a member of a cluster, its PHP also talks to the PHP of the other
members:

```
                       Clients (browsers, apps)
                                │
                                │ WebSocket over HTTP/1.1, HTTP/2 or HTTP/3
┌───────────────────────────────┼──── Server: Caddy + FrankenPHP + this extension ───────┐
│                               ▼                                                        │
│                 ┌───────────────────────────┐    events      ┌──────────────────────┐  │
│                 │    websocket directive    │───────────────▶│   WebSocket worker   │  │
│                 │   holds the connections   │◀───────────────│    websocket.php     │  │
│                 └─────────────┬─────────────┘   replies,     └──────────────────────┘  │
│                               │               websocket_*()                            │
│                               │                                                        │
│                               │         websocket_*()        ┌──────────────────────┐  │
│                               │◀─────────────────────────────│     PHP requests     │  │
│   connections on              │                              │      php_server      │  │
│   other servers               │                              └───────────┬──────────┘  │
│                               │                                          │             │
│                               │                                          │             │
│                               │                                          │ cluster_*() │
│                               ▼                                          │             │
│                 ┌───────────────────────────┐                            │             │
│                 │                           │◀───────────────────────────┘             │
│                 │      cluster member       │   messages     ┌──────────────────────┐  │
│                 │       mesh, uplink        │───────────────▶│    Cluster worker    │  │
│                 │                           │◀───────────────│     cluster.php      │  │
│                 └─────────────┬─────────────┘  cluster_*()   └──────────────────────┘  │
│                               │                                                        │
│                               │                                                        │
└───────────────────────────────┼────────────────────────────────────────────────────────┘
                                │ other members: connections (mesh) or tunnels (relay)
                                ▼
```

In a cluster, members connect directly in a mesh, or through relays when they
cannot reach each other, such as behind NAT:

```
   Clients                 Clients                          Clients
      │                       │                                │
 ┌────▼─────┐   direct   ┌────▼─────┐                    ┌─────▼────┐
 │ Server A │◀══════════▶│ Server B │                    │ Server C │
 │ mesh     │            │ mesh     │                    │ uplink   │ behind NAT
 │ uplink   │            │ uplink   │                    └─────┬────┘
 └────┬─────┘            └────┬─────┘                          │
      │                       │         ┌────────────────┐     │
      └─────── uplink ────────┴────────▶│    Relay R     │◀────┘
                                        │ cluster relay  │
                                        │ passes tunnels │
                                        │ no clients     │
                                        └────────────────┘

   A ↔ B: direct      A ↔ C, B ↔ C: tunnels through R
```

| Participant | What it is | Configured with |
|---|---|---|
| Client | A browser or app with a WebSocket connection | [Clients](docs/clients.md) |
| Server | Caddy with FrankenPHP and this extension | [Installation](docs/installation.md) |
| WebSocket worker | PHP that decides on handshakes and handles the events of connections | [The WebSocket worker](docs/worker.md) |
| PHP requests | Regular PHP, which pushes with the `websocket_*` functions | [PHP functions](docs/functions.md) |
| Member | A server in a cluster, reaching the connections and the PHP of the other members | [Mesh](docs/mesh.md) [Uplink](docs/uplink.md) |
| Cluster worker | PHP that gets what other members send with the `cluster_*` functions | [The cluster worker](docs/cluster-worker.md) |
| Relay | Joins members that cannot reach each other and passes their tunnels through; a server can be a relay and a member at once | [Relay](docs/relay.md) |
| CLI participant | PHP run with `frankenphp php-cli`, such as a queue worker or a scheduler, that takes part like a member but accepts no connections | [CLI participants](docs/cli.md) |

## Documentation

- [Installation](docs/installation.md)
- [Configuration](docs/configuration.md)
- [Clients](docs/clients.md)
- [The WebSocket worker](docs/worker.md)
- [PHP functions](docs/functions.md)
- [Cluster](docs/cluster.md)
  - [Mesh](docs/mesh.md)
  - [Uplink](docs/uplink.md)
  - [Relay](docs/relay.md)
  - [The cluster worker](docs/cluster-worker.md)
  - [CLI participants](docs/cli.md)
- [Performance](docs/performance.md)
- [Known limitations](docs/limitations.md)
- [Development](docs/development.md)
