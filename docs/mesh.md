# Mesh

`cluster mesh` makes the server a [member](cluster.md) that connects directly
to the other members.

```caddyfile
{
	cluster mesh {
		listen :7070
		dns members.internal:7070
		secret {env.CLUSTER_SECRET}
		tls {
			ca /etc/cluster/ca.pem
			cert /etc/cluster/server.pem
			key /etc/cluster/server-key.pem
		}
	}
}
```

| Option | Description |
|---|---|
| `listen <address>` | Accepts other members on `<address>`, such as `:7070`. Without it, the member only dials. |
| `peers <address>...` | Dials the members at these addresses (`host:port`). Repeatable. |
| `dns <name:port>...` | Dials every address `<name>` resolves to, on `<port>`. Repeatable. |
| `resolve_interval <duration>` | How often the `dns` names are resolved again. Default: `5s`. |
| `secret <secret>` | Authenticates the members to each other; all of them share it. |
| `tls { … }` | See [Authentication and TLS](cluster.md#authentication-and-tls). |

It needs `listen`, `peers` or `dns`, and a `secret` or `tls` with a `ca`.

## How members find each other

A mesh member accepts the members dialing its listener, apart from the sites
Caddy serves, and dials:

- the addresses in `peers`,
- the addresses its `dns` names resolve to, again every `resolve_interval`,
  such as a Kubernetes headless service or a Docker Compose service. Addresses
  that drop out of the record are no longer dialed; connections already up
  stay.

A member may list itself, so all can share one configuration: it notices that
it dialed itself and stops dialing that address. When two members dial each
other, both keep the connection dialed by the lower instance ID. A lost
connection is dialed again, backing off up to five seconds.

## Size

A mesh is one connection per pair of members, and a message to all is written
once per member, which suits up to a few dozen members. Members that cannot
reach each other directly use [relays](relay.md) through an [uplink](uplink.md);
a mesh member without an uplink cannot reach the members that only use relays.
