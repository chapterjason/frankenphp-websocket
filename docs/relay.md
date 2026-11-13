# Relay

`cluster relay` makes the server a relay, which joins the
[members](cluster.md) that use it through their [uplink](uplink.md). It can be
combined with a [mesh](mesh.md) and an uplink on the same server, on another
port; a server with only a relay is not a member and serves no clients of its
own.

```caddyfile
{
	cluster relay {
		listen :7071
		secret {env.CLUSTER_SECRET}
	}
}
```

| Option | Description |
|---|---|
| `listen <address>` | Accepts members on `<address>`. Required. |
| `secret <secret>` | Authenticates the members; they share it with the relay. |
| `tls { … }` | See [Authentication and TLS](cluster.md#authentication-and-tls). `cert` and `key` are required with it. |

It needs a `secret` or `tls` with a `ca`.

## How it works

The relay keeps a list of the members registered with it and tells each of
them who else is. When a member asks for a tunnel to another, the relay asks
that member to accept it, then only passes the bytes through: the two members
authenticate each other inside the tunnel, and with TLS and a `ca` on their
uplinks, the relay can neither read nor alter their traffic.

Each tunnel is its own TCP connection on both sides of the relay, so a member
that does not keep up only holds up the tunnels into it, and its senders find
its queue full as they would in a mesh. The relay queues and drops nothing. It
holds two connections per pair of members using it.
