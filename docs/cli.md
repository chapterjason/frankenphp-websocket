# CLI participants

PHP that runs from the command line, such as a queue worker or a scheduled
command, can take part in the cluster as a CLI participant: it reaches the
WebSocket connections and the PHP of the members like a member does, and the
members reach its PHP, but it accepts no connections of its own.

```console
FRANKENPHP_CLUSTER_CONFIG=/etc/frankenphp/Caddyfile frankenphp php-cli bin/console messenger:consume
```

It runs with `frankenphp php-cli`, which loads the extension; the `php`
binary does not. `FRANKENPHP_CLUSTER_CONFIG` names the configuration of the
servers, a Caddyfile (a file named `Caddyfile…` or `….caddyfile`) or Caddy's
JSON otherwise, and the participant takes from it:

| From | It takes | It leaves out |
|---|---|---|
| `cluster mesh` | `peers`, `dns`, `resolve_interval`, `secret`, `tls` | `listen` |
| `uplink` | all of it | |
| `cluster member` | `query_timeout`, `presence_interval`, `rejoin_window`, `queue_size` | `worker` |
| `cluster relay`, sites | | all of it |

Its placeholders, such as `{env.CLUSTER_SECRET}`, are read from the
environment of the command. A mesh needs `peers` or `dns`, as the participant
only dials. With `tls` and a `ca`, it presents the `cert` of the configuration
to the members it dials, so it needs to read that and its `key`.

## Joining and leaving

A command joins on its first `cluster_*` or `websocket_*` call that needs the
cluster, so a command that never uses it never joins. That call waits until
the members are connected and have answered, so a send to all of them right
away reaches them; with members that cannot be reached, it waits a few
seconds at most. A configuration that cannot be used, or a failed join, is
logged to stderr, and the command runs as if there were no cluster:
`cluster_*` calls fail with `CLUSTER_ERROR_NOT_JOINED`, and `websocket_*`
calls find no connections.

When the command ends, the participant says goodbye and the members forget
it at once; queued messages get up to five seconds to be written first. A
command that is killed is forgotten after three presence intervals. Either
way, one server's [cluster worker](cluster-worker.md#when-a-member-leaves)
gets a `left` event with the role `cli`.

Every run is a participant of its own, with an instance ID of its own.

## What it can do

- All `websocket_*` functions: it holds no connections, so it reaches those
  of the servers. Sends to all connections and queries of all connections do
  not wait for CLI participants.
- All `cluster_*` functions. It is one of the `cluster_members()` of the
  others, and they can `cluster_send()`, `cluster_publish()` and
  `cluster_request()` to it.
- `cluster_receive()` takes what the others send it.

## Between CLI participants

CLI participants do not dial each other, so the servers pass on what they send
each other: a scheduler can `cluster_request()` one messenger consumer, and a
`cluster_publish()` reaches the other CLI participants too, each once.

A server tells the CLI participants connected to it of each other: a new one
knows those already there once it has joined, and the others learn of it a
round trip later. Each CLI participant sends to another through the server it
last heard of it through; when that server goes away, the next presence comes
through another one. Through relays, CLI participants also reach each other
directly, which is then preferred.

A server passes on only what a CLI participant sends itself, to a CLI
participant connected to it.

## Receiving

```php
<?php

$running = true;

while ($running) {
    cluster_receive(function (array $event): ?string {
        // ['event' => 'message', 'from' => '<instance>', 'data' => '…', 'request' => bool]
        return $event['request'] ? 'the answer' : null;
    }, 1000);
}
```

The participant receives from its first call of `cluster_receive()` on, which
it announces to the others: before, requests to it fail with
`CLUSTER_ERROR_NO_RECEIVER`, and publishes leave it out. A command that only
sends never receives.

`cluster_receive(callable $handler, int $timeout = 0): bool` waits up to
`$timeout` milliseconds for the next message, `0` not at all, and calls
`$handler` with it: the same event and the same answer as the
[cluster worker](cluster-worker.md#the-worker). It returns `true` once the
handler ran, and `false` when no message came, with `CLUSTER_ERROR_TIMEOUT`.

- A request is answered with the handler's return value: a string, an empty
  one for `null`. Another value raises a warning and leaves the request
  unanswered, as does an exception, which `cluster_receive()` passes on.
- PHP runs its signal handlers only after the wait: keep the timeout short
  in a command that stops on a signal.
- Up to `queue_size` messages wait for `cluster_receive()`; more are missed.
- In a server, `cluster_receive()` throws an `Error`: the
  [cluster worker](cluster-worker.md) gets the messages there.
