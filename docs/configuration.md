# Configuration

Each part of the extension has its own page with its options.

## Sites

| Directive | Configures | See |
|---|---|---|
| `websocket [<matcher>] { … }` | Where clients connect, and the PHP worker of their connections | [The WebSocket worker](worker.md#the-websocket-directive) |

## Global options

| Option | Configures | See |
|---|---|---|
| `cluster mesh { … }` | A member that connects directly to the other members | [Mesh](mesh.md) |
| `uplink { … }` | A member that reaches the other members through relays | [Uplink](uplink.md) |
| `cluster relay { … }` | A relay, which joins the members that use it | [Relay](relay.md) |
| `cluster member { … }` | How the server takes part as a member, and the PHP worker of the messages of other members | [The cluster worker](cluster-worker.md) |

Without `cluster mesh` or `uplink`, the server runs on its own. How the parts
of a cluster combine, authenticate and deliver is in [Cluster](cluster.md).

## Environment

| Variable | Configures | See |
|---|---|---|
| `FRANKENPHP_CLUSTER_CONFIG` | For `frankenphp php-cli`: the configuration of the servers, whose cluster the command joins | [CLI participants](cli.md) |

The values take placeholders, such as `secret {env.CLUSTER_SECRET}`, to keep
secrets out of the configuration.

## HTTP/2

Go's HTTP/2 server only accepts WebSockets when FrankenPHP is started with
`GODEBUG=http2xconnect=1` in its environment; it cannot be enabled from the
configuration. Without it, browsers use HTTP/1.1 for WebSockets. HTTP/1.1 and
HTTP/3 need nothing.
