# Configuration

## The `websocket` directive

```caddyfile
example.com {
	websocket {
		worker websocket.php 4
	}

	php_server
}
```

The directive takes WebSocket handshakes and passes every other request on, so
it can share a site with `php_server`. It accepts a request matcher, like
`websocket /ws { … }`. Its options:

| Option | Default | Description |
|---|---|---|
| `worker <file> [<num>]` | required | The PHP worker script and its number of threads (FrankenPHP's default for workers when omitted). |
| `allow_cross_origin` | off | Pass cross-origin handshakes to PHP instead of refusing them with 403. PHP must then check `$_SERVER['HTTP_ORIGIN']`. |
| `max_message_size <size>` | `32KiB` | The largest message accepted, after decompression. A larger one closes the connection with 1009. |
| `write_timeout <duration>` | `10s` | Bounds every write. A write that times out ends the connection. |
| `close_timeout <duration>` | `5s` | How long the peer has to answer a close before the connection is cut. |
| `idle_timeout <duration>` | none | Closes a connection that sent nothing for that long, with 1001 `idle timeout`. |
| `ping_interval <duration>` | none | Sends a ping at that interval. Their pongs do not reach PHP. |
| `max_connections <count>` | none | Caps the connections of this directive, handshakes included. More are refused with 503. |
| `max_connections_per_client <count>` | none | Caps the connections of one client IP, as Caddy determines it (trusted proxies respected). More are refused with 429. |
| `message_rate <count> [<interval>]` | none | How many frames a connection may send per interval (default `1s`), in bursts of up to `<count>`. A faster connection is closed with 1008. |

## Global options

```caddyfile
{
	websocket {
		bus nats nats://nats:4222
		query_timeout 1s
	}
}
```

| Option | Default | Description |
|---|---|---|
| `bus unix <directory> { queue_size <count> }` | queue of `1024` | Connect the instances of one machine through Unix sockets in `<directory>`. `queue_size` is how many messages may wait for one peer before it misses them. |
| `bus nats [<urls>] { … }` | `nats://127.0.0.1:4222` | Connect instances through NATS; `<urls>` is comma-separated. See below. |
| `query_timeout <duration>` | `1s` | How long a query waits for the other instances to answer. |
| `presence_interval <duration>` | `2s` | How often an instance announces itself; one silent for three intervals counts as gone. |

Without a `bus`, the instance runs on its own. See [Cluster](cluster.md).

### NATS

```caddyfile
{
	websocket {
		bus nats tls://nats-1:4222,tls://nats-2:4222 {
			subject chat
			credentials /etc/nats/websocket.creds
			tls {
				ca /etc/nats/ca.pem
			}
		}
	}
}
```

| Option | Description |
|---|---|
| `subject <prefix>` | Prefixes the subjects of the bus, so several clusters can share a NATS server. Default: `frankenphp.websocket`. |
| `credentials <file>` | Authenticates with a JWT credentials file (`.creds`). |
| `nkey <file>` | Authenticates with the NKey user seed in `<file>`. |
| `token <token>` | Authenticates with a token. |
| `user <name> <password>` | Authenticates with a user and password. |
| `tls { ca <file> }` | Connects with TLS, verifying the server against the CAs in `<file>`; without `ca`, against the system's roots. |
| `tls { cert <file> key <file> }` | Presents a client certificate, for servers that verify clients. |
| `tls { server_name <name> }` | Verifies the server certificate against `<name>` instead of the host of the URL. |

Use one authentication method at most. The values take placeholders, such as
`token {env.NATS_TOKEN}`, to keep secrets out of the configuration.

## HTTP/2

Go's HTTP/2 server only accepts WebSockets when FrankenPHP is started with
`GODEBUG=http2xconnect=1` in its environment; it cannot be enabled from the
configuration. Without it, browsers use HTTP/1.1 for WebSockets. HTTP/1.1 and
HTTP/3 need nothing.
