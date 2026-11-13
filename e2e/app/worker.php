<?php

function handle(?array $event = null): ?string
{
    if ($event === null) {
        handleHandshake();

        return null;
    }

    return match ($event['event']) {
        'open' => handleOpen($event),
        'message' => 'echo:' . $event['data'],
        default => null,
    };
}

function handleHandshake(): void
{
    if (($_GET['access'] ?? '') === 'denied') {
        http_response_code(403);
        echo 'access denied';

        return;
    }

    $connection = $_SERVER['HTTP_WEBSOCKET_CONNECTION'];

    header('X-Seen: ' . $_SERVER['REQUEST_METHOD'] . ' ' . $_SERVER['REQUEST_URI'] . ' ' . $_SERVER['SERVER_PROTOCOL']);
    websocket_set_attribute($connection, 'protocol', $_SERVER['SERVER_PROTOCOL']);
}

function handleOpen(array $event): string
{
    return json_encode([
        'connection' => $event['connection'],
        'protocol' => $event['attributes']['protocol'],
    ]);
}

while (frankenphp_handle_request('handle')) {
    gc_collect_cycles();
}
