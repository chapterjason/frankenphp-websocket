# Cluster

In a cluster, instances reach each other's connections. A connection ID names
the instance holding it (`<instance>.<random>`), so a call on one connection
goes straight to that instance; sends to a tag or to all go to every instance.
`websocket_instance()` tells PHP which instance that is.

- Pushes to another instance (`websocket_send`, `websocket_ping`,
  `websocket_close`) are fire and forget: `true` means the message is queued
  for that instance.
- Queries (`websocket_is_open`, the attribute and tag functions) wait for the
  instance holding the connection, at most `query_timeout`.
- `websocket_connections`, `websocket_connections_with_tag`,
  `websocket_send_to_tag` and `websocket_broadcast` wait for every instance known to be alive, except
  [CLI participants](cli.md), which hold no connections. Instances announce
  themselves every `presence_interval` (two seconds by default) and say
  goodbye when they stop; one silent for three announcements counts as gone.
  A member that leaves either way is reported to one
  [cluster worker](cluster-worker.md#when-a-member-leaves).

## Members and relays

A server takes part in a cluster in up to three ways, which combine freely:

| Configuration | The server is | See |
|---|---|---|
| `cluster mesh { … }` | A member that connects directly to the other members. | [Mesh](mesh.md) |
| `uplink { … }` | A member that reaches the other members through relays. | [Uplink](uplink.md) |
| `cluster relay { … }` | A relay, which joins the members that use it. It is not a member itself. | [Relay](relay.md) |
| `cluster member { … }` | Tunes how a member takes part, and takes the PHP messages of the other members. | [The cluster worker](cluster-worker.md) |

PHP from the command line, such as queue workers and schedulers, takes part
too, with `frankenphp php-cli` and the configuration of the servers: as a
[CLI participant](cli.md), which dials the members but accepts no
connections. The servers pass on what CLI participants send each other.

`cluster mesh` and `uplink` make the server a member: its clients'
WebSocket connections and its PHP reach those of the other members. A server
with only `cluster relay` is not a member and serves no clients of its own.

Members and relays talk the cluster's own protocol, on listeners of their
own, apart from the sites Caddy serves. It is framed as WebSocket (RFC 6455)
between members, but has nothing to do with the WebSocket connections of
clients: those are one of its users, PHP messaging is the other, each on a
channel of its own.

Every pair of members shares one connection, with its own queue on both
sides: directly, or through a tunnel when both use the same relay. When there
are both, the direct one is kept. A cluster can mix the two; it works as long
as every pair of members can reach each other one way or the other:

```
   Data center (mesh)                 Relay R              Behind NAT (uplink only)

   A ◀════ direct ════▶ B
     ╲                 ╱
      ╲═══ direct ════╱
             C
   A, B, C ── uplink ──▶  R  ◀── uplink ── E
                             ◀── uplink ── F

   A↔B, A↔C, B↔C : direct
   A↔E, B↔F, E↔F : tunnels through R
```

A mesh member without an `uplink` cannot reach the members that only use
relays.

## Authentication and TLS

Members and relays authenticate each other with at least one of:

- `secret`, shared by all of them. Both sides of a handshake prove they know
  it, with an HMAC-SHA256 over the instance IDs, the time and a nonce; a
  handshake more than 30 seconds off or replayed is refused.
- `tls` with `ca`: every server has a certificate signed by the CA, which it
  serves to those dialing it and presents to those it dials. One without such
  a certificate is refused.

The secret does not encrypt: across networks you do not trust, add `tls`. With
`tls` and a `ca` on the uplink, members also run TLS with each other inside
each tunnel, so a relay can neither read nor alter their traffic; without a
`ca`, the relay sees what passes through it, like the network would.

`tls` without `ca` encrypts and verifies the servers dialed against the
system's roots, but does not authenticate those dialing, so it needs a
`secret`.

| Option | Description |
|---|---|
| `tls { ca <file> }` | Verifies the other servers against the CAs in `<file>`, and refuses those without a valid certificate. |
| `tls { cert <file> key <file> }` | The certificate of this server: served to those dialing it, presented to those it dials. Required to listen with TLS or with a `ca`. |
| `tls { server_name <name> }` | Verifies dialed servers against `<name>` instead of the host dialed. |

The values take placeholders, such as `secret {env.CLUSTER_SECRET}`, to keep
secrets out of the configuration.

## PHP messaging

Besides reaching each other's WebSocket connections, the PHP of the members
can talk to each other directly, with or without WebSockets; see
[The cluster worker](cluster-worker.md).

## Delivery

Each member has a queue of `queue_size` messages (1024 by default) for each
other member, so a member that does not keep up misses messages instead of
holding up the others; a call that reaches it then reports
`WEBSOCKET_ERROR_BUS_FAILED`.

A member whose connection drops keeps its queue for ten seconds, so a quick
reconnect only loses what was being written; after that, it is forgotten
with its queue. Every connection carries a heartbeat every five seconds, and
one silent for fifteen seconds is dropped. On shutdown, queued messages get up
to five seconds to be written.

A config reload starts the new configuration before it stops the old one;
they share the listeners, and new connections reach the new configuration.
