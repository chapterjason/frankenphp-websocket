<?php

function handle(?array $event = null): ?string
{
    if ($event === null) {
        handleHandshake();

        return null;
    }

    if ($event['event'] !== 'message') {
        return null;
    }

    return handleMessage($event, $event['data'], $event['binary']);
}

function handleHandshake(): void
{
    if (($_GET['access'] ?? '') === 'denied') {
        http_response_code(403);
        header('X-Refused: yes');
        echo 'access denied';

        return;
    }

    $connection = $_SERVER['HTTP_WEBSOCKET_CONNECTION'];

    header('X-Seen: ' . $_SERVER['REQUEST_METHOD'] . ' ' . $_SERVER['REQUEST_URI'] . ' ' . ($_SERVER['HTTP_X_CLIENT'] ?? '-'));
    header('X-Connection: ' . $connection);
    header('X-Origin: ' . ($_SERVER['HTTP_ORIGIN'] ?? '-'));
    setcookie('session', 'abc');

    if (isset($_GET['user'])) {
        websocket_set_attribute($connection, 'user', $_GET['user']);
    }

    if (isset($_GET['room'])) {
        websocket_tag($connection, 'room:' . $_GET['room']);
    }

    if (isset($_GET['protocol'])) {
        header('Sec-WebSocket-Protocol: ' . $_GET['protocol']);
    }

    if (isset($_GET['compress'])) {
        header('Sec-WebSocket-Extensions: permessage-deflate');
    }
}

function handleMessage(array $event, string $data, bool $binary): ?string
{
    $self = $event['connection'];
    [$command, $argument] = array_pad(explode(':', $data, 2), 2, '');

    return match ($command) {
        'silent' => null,
        'send-to' => sendTo($argument),
        'send-as' => sendAs($argument),
        'close' => closeSelf($self, $argument),
        'ping' => pingSelf($self, $argument),
        'status' => status($argument),
        'instance' => json_encode([websocket_instance($argument), websocket_last_error()]),
        'set' => setAttribute($self, $argument),
        'remove' => removeAttribute($self, $argument),
        'tag' => result(websocket_tag($self, $argument)),
        'untag' => result(websocket_untag($self, $argument)),
        'tags' => json_encode(websocket_tags($self)),
        'attributes' => json_encode(websocket_attributes($self)),
        'to-tag' => sendToTag($argument),
        'connections-with-tag' => json_encode(websocket_connections_with_tag($argument)),
        'broadcast' => (string) websocket_broadcast($argument),
        'context' => context($event),
        default => echoMessage($data, $binary),
    };
}

function result(bool $ok): string
{
    return $ok ? 'ok' : 'error:' . websocket_last_error();
}

function sendTo(string $argument): string
{
    [$connection, $payload] = explode(':', $argument, 2);

    return websocket_send($connection, $payload) ? 'sent' : 'not sent';
}

function sendAs(string $argument): string
{
    [$type, $connection, $payload] = explode(':', $argument, 3);

    return result(websocket_send($connection, $payload, (int) $type));
}

function closeSelf(string $self, string $argument): ?string
{
    [$code, $reason] = array_pad(explode(':', $argument, 2), 2, '');

    if (websocket_close($self, (int) $code, $reason)) {
        return null;
    }

    return 'error:' . websocket_last_error();
}

function pingSelf(string $self, string $payload): ?string
{
    if (websocket_ping($self, $payload)) {
        return null;
    }

    return 'error:' . websocket_last_error();
}

function status(string $connection): string
{
    $open = websocket_is_open($connection);
    $error = websocket_last_error();

    return json_encode([
        'open' => $open,
        'error' => $error,
        'listed' => in_array($connection, websocket_connections(), true),
    ]);
}

function setAttribute(string $self, string $argument): string
{
    [$name, $value] = array_pad(explode(':', $argument, 2), 2, '');

    return result(websocket_set_attribute($self, $name, $value));
}

function removeAttribute(string $self, string $name): string
{
    return result(websocket_remove_attribute($self, $name));
}

function sendToTag(string $argument): string
{
    [$tag, $payload] = explode('|', $argument, 2);

    return (string) websocket_send_to_tag($tag, $payload);
}

function context(array $event): string
{
    return json_encode([
        'attributes' => $event['attributes'],
        'tags' => $event['tags'],
        'method' => $event['request']['method'],
        'uri' => $event['request']['uri'],
        'client' => $event['request']['headers']['x-client'] ?? null,
        'connection_header' => $event['request']['headers']['websocket-connection'] ?? null,
    ]);
}

function echoMessage(string $data, bool $binary): string
{
    if ($binary) {
        return strrev($data);
    }

    return 'echo:' . $data;
}

while (frankenphp_handle_request('handle')) {
    gc_collect_cycles();
}
