# The cluster worker

The PHP of the [members](cluster.md) of a cluster can talk to each other,
with or without WebSockets. What other members send reaches the worker of
`cluster member`, which also tunes how the server takes part as a member:

```caddyfile
{
	cluster member {
		query_timeout 1s
		worker cluster.php 2
	}
}
```

| Option | Default | Description |
|---|---|---|
| `query_timeout <duration>` | `1s` | How long a query waits for the other members to answer. |
| `presence_interval <duration>` | `2s` | How often a member announces itself; one silent for three intervals counts as gone. |
| `rejoin_window <duration>` | `24h` | How long after reporting a member gone after a timeout the servers still make it close its connections when it comes back; see [When a member leaves](#when-a-member-leaves). |
| `queue_size <count>` | `1024` | How many messages may wait for one other member before it misses them. |
| `worker <file> [<num>]` | none | The PHP worker that gets the messages of the other members, and its number of threads (FrankenPHP's default for workers when omitted). |

Every option is optional, and so is the block; the settings apply to the
server's WebSockets too.

## Functions

| Function | Does |
|---|---|
| `cluster_self(): string` | The instance ID of this member. |
| `cluster_members(): array` | The instance IDs of the other members known to be alive. |
| `cluster_member_info(): array` | The other members known to be alive, by instance ID: `['role' => 'server'\|'cli', 'receives' => bool]`. |
| `cluster_send(string $instance, string $data): bool` | Sends `$data` to the worker of one member. Fire and forget: `true` means it is queued for that member, or that the member is gone; `false` with `CLUSTER_ERROR_NO_RECEIVER` for a member that does not receive. |
| `cluster_publish(string $data): int` | Sends `$data` to all other members that receive and returns for how many it was queued. |
| `cluster_request(string $instance, string $data, int $timeout = 0): string` | Sends `$data` to the worker of one member and returns its answer. `$timeout` is in milliseconds; `0` waits the `query_timeout`. |
| `cluster_receive(callable $handler, int $timeout = 0): bool` | For [CLI participants](cli.md#receiving): waits up to `$timeout` milliseconds for a message and hands it to `$handler`. |
| `cluster_last_error(): int` | The `CLUSTER_ERROR_*` code of the last `cluster_*` call of this PHP thread. |

| Constant | Meaning |
|---|---|
| `CLUSTER_ERROR_NONE` | No error. |
| `CLUSTER_ERROR_NOT_JOINED` | This server is not a member. |
| `CLUSTER_ERROR_INVALID_ARGUMENT` | An empty instance ID, or a negative timeout. |
| `CLUSTER_ERROR_BUS_FAILED` | The queue to a member was full. |
| `CLUSTER_ERROR_TIMEOUT` | The member did not answer in time, or no message came for `cluster_receive()`. |
| `CLUSTER_ERROR_NO_RECEIVER` | The member does not receive: a server without a worker, or a CLI participant that has not called `cluster_receive()` yet. |

When there is no answer, `cluster_request()` returns an empty string; only
`cluster_last_error()` tells it from an empty answer. Members are addressed
by instance ID, which changes when the server reloads its configuration.

## The worker

```php
<?php

function handle(array $event): ?string
{
    return match ($event['event']) {
        'message' => handleMessage($event),
        'left' => handleLeft($event),
        default => null,
    };
}

function handleMessage(array $event): ?string
{
    // ['event' => 'message', 'from' => '<instance>', 'data' => '…', 'request' => bool]
    return $event['request'] ? 'the answer' : null;
}

function handleLeft(array $event): null
{
    // ['event' => 'left', 'instance' => '<instance>', 'reason' => 'leave'|'timeout', 'role' => 'server'|'cli']
    return null;
}

while (frankenphp_handle_request('handle')) {
    gc_collect_cycles();
}
```

| `event` | Fields | When |
|---|---|---|
| `message` | `from`, `data`, `request` (bool) | Another member sent something. |
| `left` | `instance`, `reason`, `role` | A member left the cluster; see below. |

The worker's return value answers a request, as an empty string for `null`,
and is ignored for other messages and for `left`.

## Who receives

A server receives when it has a worker, a [CLI participant](cli.md) from its
first `cluster_receive()` on, and each announces it to the others at once.
A request to a member that does not receive fails right away, as that member
answers so itself. Sends and publishes go by what the sender last heard:
`cluster_send()` to a member known not to receive fails, and
`cluster_publish()` leaves such members out, also one that started receiving
so recently that the sender has not heard of it yet.

## When a member leaves

A member that leaves is reported once, to the worker of one server: of the
servers alive that have a worker, the one with the lowest instance ID. It
tells the other servers, so none reports the member again. Only when two
parts of a split network each report the same member, and meet again, is it
reported twice: keep what the `left` event does safe to repeat.

| Field | Value |
|---|---|
| `instance` | The instance ID of the member. |
| `reason` | `leave`: it said goodbye, as a server that stops or reloads and a CLI participant that ends do. `timeout`: it fell silent for three presence intervals, as a server that crashed or was cut off. |
| `role` | `server`, or `cli` for a [CLI participant](cli.md). |

A server that leaves with a goodbye has closed its connections and run their
`close` events. One that times out could not: its connections are gone with
it, and no `close` event runs for them. To clean up after it, store the
instance of each connection when it opens, with `websocket_instance()`, and
remove what is stored under the instance of a `left` event with `timeout`.

A member reported after a timeout that was only cut off, not gone, learns so
as soon as it reaches a server again, within `rejoin_window` after the report. A
server then closes its connections with 1012 `rejoining the cluster`, which
runs their `close` events, so their clients reconnect; until it is done, the
other members ignore it. It then rejoins under the same instance ID, and no
connection outlives the `left` event that reported it gone.

A [CLI participant](cli.md) has no worker; it takes its messages with
`cluster_receive()`.
