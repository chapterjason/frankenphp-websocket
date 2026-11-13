# Cluster

With a `bus`, instances reach each other's connections. A connection ID names
the instance holding it (`<instance>.<random>`), so a call on one connection
goes straight to that instance; sends to a tag or to all go to every instance.

- Pushes to another instance (`websocket_send`, `websocket_ping`,
  `websocket_close`) are fire and forget: `true` means the message is on the
  bus.
- Queries (`websocket_is_open`, the attribute and tag functions) wait for the
  instance holding the connection, at most `query_timeout`.
- `websocket_connections`, `websocket_send_to_tag` and `websocket_broadcast`
  wait for every instance known to be alive. Instances announce themselves
  every `presence_interval` (two seconds by default) and say goodbye when they
  stop; one silent for three announcements counts as gone.

## NATS

Every instance subscribes to `<subject>.instance.<id>` and `<subject>.all`.

## Unix sockets

Every instance listens on `<directory>/<id>.sock` and writes to the sockets of
the others; the directory is created if missing. Each peer has its own writer
and a queue of `queue_size` messages (1024 by default), so a peer that does not keep up misses messages
instead of holding up the others; a call that reaches it then reports
`WEBSOCKET_ERROR_BUS_FAILED`. On shutdown, queued messages get up to five
seconds to be written. The path of a socket must stay
below 104 bytes, so the directory must be short. Sockets left by an instance
that died are removed.

## Writing an adapter

A bus adapter is a Caddy module in the `websocket.bus` namespace implementing
the `Bus` interface of this package: `Open(instance, deliver)`,
`Publish(instance, payload)` and `Close()`. It carries opaque payloads; routing,
queries and membership are handled above it.
