<?php

// Answers every request with who this member is and what it got.
function handle(array $event): ?string
{
    if ($event['event'] !== 'message') {
        return null;
    }

    return json_encode(['self' => cluster_self(), 'from' => $event['from'], 'data' => $event['data']]);
}

while (frankenphp_handle_request('handle')) {
    gc_collect_cycles();
}
