# The PHP worker

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
