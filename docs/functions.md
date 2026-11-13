# PHP functions

The functions work from the WebSocket worker and from any other PHP request.
With a bus, they reach connections on every instance.

| Function | Returns |
|---|---|
| `websocket_send(string $connection, string $data, int $type = 0): bool` | Sends a message. `$type` is `WEBSOCKET_AUTO` (0, text when `$data` is valid UTF-8), `WEBSOCKET_TEXT` or `WEBSOCKET_BINARY`. |
| `websocket_send_to_tag(string $tag, string $data, int $type = 0): int` | Sends to every connection with the tag; returns to how many. |
| `websocket_broadcast(string $data, int $type = 0): int` | Sends to every connection; returns to how many. |
| `websocket_ping(string $connection, string $payload = ""): bool` | Sends a ping of at most 125 bytes; the pong arrives as a `pong` event. |
| `websocket_close(string $connection, int $code = 1000, string $reason = ""): bool` | Starts the closing handshake. The code is 1000 to 1003, 1007 to 1014 or 3000 to 4999; the reason at most 123 bytes. |
| `websocket_is_open(string $connection): bool` | Whether messages can be sent to the connection. |
| `websocket_connections(): array` | The IDs of the open connections. |
| `websocket_set_attribute(string $connection, string $name, string $value): bool` | Attaches a value to the connection. |
| `websocket_remove_attribute(string $connection, string $name): bool` | Removes it. |
| `websocket_attributes(string $connection): array` | The attributes of the connection. |
| `websocket_tag(string $connection, string $tag): bool` | Adds the connection to a tag. |
| `websocket_untag(string $connection, string $tag): bool` | Removes it from the tag. |
| `websocket_tags(string $connection): array` | The tags of the connection. |
| `websocket_last_error(): int` | The `WEBSOCKET_ERROR_*` code of the last `websocket_*` call of this PHP thread. |

The error codes:

| Constant | Meaning |
|---|---|
| `WEBSOCKET_ERROR_NONE` | The call succeeded. |
| `WEBSOCKET_ERROR_NOT_FOUND` | There is no such connection. |
| `WEBSOCKET_ERROR_NOT_OPEN` | The connection is not open yet, or closing. |
| `WEBSOCKET_ERROR_INVALID_ARGUMENT` | An argument is invalid, such as forced text that is not UTF-8. |
| `WEBSOCKET_ERROR_WRITE_FAILED` | The write failed; the connection ends. |
| `WEBSOCKET_ERROR_BUS_FAILED` | The bus could not take the message. |
| `WEBSOCKET_ERROR_TIMEOUT` | An instance did not answer within `query_timeout`. |

Functions returning an array or a count return what they have on an error:
`[]` for an unknown connection, and the connections of the instances that
answered on a timeout.
