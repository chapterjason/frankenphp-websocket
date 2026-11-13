# Clients

A client is anything with a WebSocket: a browser, an app, another server.
Nothing is configured for it; what it meets is set by the
[`websocket` directive](worker.md#the-websocket-directive) and decided by the
[WebSocket worker](worker.md).

```js
const socket = new WebSocket('wss://example.com/ws');
socket.onopen = () => socket.send('hello');
socket.onmessage = (event) => console.log(event.data);
```

## Protocols

Clients connect over HTTP/1.1 (RFC 6455), HTTP/2 (RFC 8441) or HTTP/3
(RFC 9220). Browsers pick the version themselves; HTTP/2 needs the server
started with `GODEBUG=http2xconnect=1` (see [Configuration](configuration.md#http2)),
and without it they use HTTP/1.1.

## The handshake

The handshake reaches PHP as an ordinary HTTP request, with the client's
cookies, headers and query, so sessions and firewalls apply as to any request.
PHP accepts or refuses it, and its status, headers and body reach the client
as they are: a 403, or a redirect to a login page.

- A cross-origin handshake is refused with 403, unless the directive has
  `allow_cross_origin`.
- A subprotocol the client offers is used when PHP chooses it.
- `permessage-deflate` compression (RFC 7692) is used when the client offers it
  and PHP asks for it.

## What a client runs into

| When | The client gets |
|---|---|
| A message larger than `max_message_size` | A close with 1009. |
| More frames than `message_rate` allows | A close with 1008. |
| Nothing sent, not even a pong, for `idle_timeout` (1 minute by default) | A close with 1001 `idle timeout`. |
| `max_connections` reached | The handshake refused with 503. |
| `max_connections_per_client` reached for its IP | The handshake refused with 429. |
| Every `ping_interval` (30 seconds by default) | A ping, which it answers. |
| The server stopping or restarting | A close with 1001 `server shutting down`. |
| The server cut off from its cluster long enough to be reported gone, once it is back | A close with 1012 `rejoining the cluster`. |
| The server's configuration replaced | A close with 1001. |
