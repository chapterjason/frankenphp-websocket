# Uplink

`uplink` makes the server a [member](cluster.md) that reaches the other
members through [relays](relay.md).

```caddyfile
{
	uplink {
		connect relay-1:7071 relay-2:7071
		secret {env.CLUSTER_SECRET}
	}
}
```

| Option | Description |
|---|---|
| `connect <address>...` | The relays to register with. Required. |
| `secret <secret>` | Authenticates this member to the relays and to the other members. |
| `tls { … }` | See [Authentication and TLS](cluster.md#authentication-and-tls). With a `ca`, the members also run TLS with each other inside every tunnel. |

It needs a `secret` or `tls` with a `ca`. It can be combined with a
[mesh](mesh.md): members reach each other directly where they can, and
through a relay where they cannot.

## How it works

The member registers with every relay it lists, and each relay tells its
members who else is registered. To reach another member, it asks a relay for
a tunnel, which the relay asks the other member to accept; the two members then
connect inside the tunnel as they would directly. Both dial out to the relay,
so either can be behind NAT or a firewall that lets only outgoing connections
through.

For redundancy, list several relays: the member registers with all of them,
and opens each tunnel through one that the other member is registered with.
Every member using relays must be registered with all of them.
