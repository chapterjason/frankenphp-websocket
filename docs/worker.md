# The WebSocket worker

The `websocket` directive takes the WebSocket connections of
[clients](clients.md) on a site, and its worker script decides on every
handshake and handles every event.

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
| `idle_timeout <duration>\|off` | `1m` | Closes a connection that sent nothing for that long, with 1001 `idle timeout`. A pong counts, so a client answering the keepalive pings stays connected. |
| `ping_interval <duration>\|off` | `30s` | Sends a keepalive ping at that interval. Their pongs do not reach PHP. |
| `max_connections <count>` | none | Caps the connections of this directive, handshakes included. More are refused with 503. |
| `max_connections_per_client <count>` | none | Caps the connections of one client IP, as Caddy determines it (trusted proxies respected). More are refused with 429. |
| `message_rate <count> [<interval>]` | none | How many frames a connection may send per interval (default `1s`), in bursts of up to `<count>`. A faster connection is closed with 1008. |

With both on, `idle_timeout` must be longer than `ping_interval`, or a client
answering every ping would still be closed as idle.

## The worker script

The worker script handles both the handshake and the events, through the one
callback FrankenPHP's `frankenphp_handle_request()` takes:

```php
<?php

function handle(?array $event = null): ?string
{
    if ($event === null) {
        handleHandshake();

        return null;
    }

    return match ($event['event']) {
        'open' => handleOpen($event),
        'message' => handleMessage($event),
        'close' => handleClose($event),
        default => null,
    };
}

function handleHandshake(): void
{
    if (!isset($_COOKIE['session'])) {
        http_response_code(403);

        return;
    }

    websocket_set_attribute($_SERVER['HTTP_WEBSOCKET_CONNECTION'], 'user', $_COOKIE['session']);
}

function handleOpen(array $event): ?string
{
    return 'welcome';
}

function handleMessage(array $event): ?string
{
    return 'echo: ' . $event['data'];
}

function handleClose(array $event): ?string
{
    return null;
}

while (frankenphp_handle_request('handle')) {
    gc_collect_cycles();
}
```

## The handshake

The callback is called without an argument, in HTTP mode: the handshake is an
ordinary request with `$_SERVER`, the headers, `$_COOKIE`, `$_GET`, the path and
the client address. Sessions and firewalls work as for any request.

- The method is `GET` on every HTTP version, and `SERVER_PROTOCOL` is
  `HTTP/1.1`, `HTTP/2.0` or `HTTP/3.0`. The request has no body.
- `$_SERVER['HTTP_WEBSOCKET_CONNECTION']` is the ID of the connection; the
  extension sets that header, a client cannot.
- A 2xx response, or no status at all, accepts the connection. The headers PHP
  sets, such as `Set-Cookie`, go on the handshake response; what PHP prints is
  discarded.
- Any other status refuses it, and PHP's status, headers and body reach the
  client as they are: a 403, or a redirect to a login page.
- `header('Sec-WebSocket-Protocol: …')` chooses a subprotocol; it must be one
  the client offered, or the handshake fails with 500.
- `header('Sec-WebSocket-Extensions: permessage-deflate')` turns compression on
  when the client offered it in a form Go can serve.
- Attributes and tags can be set during the handshake.

## Events

After an accepted handshake, the callback gets an array:

| `event` | Fields | When |
|---|---|---|
| `open` | | The connection is up; `websocket_send()` reaches it from now on. |
| `message` | `data` (string), `binary` (bool) | A complete message arrived. |
| `ping` | `data` | The client sent a ping; the pong is already sent. |
| `pong` | `data` | The client answered a ping of `websocket_ping()`. |
| `close` | `code` (int), `reason` (string) | The connection ended: the client's or the server's close code, 1006 when it ended without one. |

Every event also has `connection` (the ID), `attributes`, `tags` and `request`
(the handshake as PHP saw it: `method`, `uri`, `protocol`, `remote_addr` and
`headers`, lowercased).

The return value of `open`, `message`, `ping` and `pong` is sent back to the
connection: a string as a text message when it is valid UTF-8, as a binary
message otherwise; `null` sends nothing. `close` follows every `open`, and only
an `open`. The events of a connection arrive one at a time, in order.

## Stopping

When the server stops or restarts, it closes every connection with 1001
`server shutting down` as soon as it starts shutting down, and exits only
once their `close` events have run, so what PHP records about connections
does not outlive them. Give the server the time that takes: a process manager
that kills it first, such as Docker after its 10 seconds, cuts the `close`
events short. Handshakes that arrive meanwhile are closed with 1001 right
away, without `open`.

A server that crashes runs no `close` events; in a cluster, the
[`left` event](cluster-worker.md#when-a-member-leaves) tells another server.
