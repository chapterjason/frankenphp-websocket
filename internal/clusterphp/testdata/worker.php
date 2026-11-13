<?php

// Each message is a command: what it does and the reply tell the test what
// PHP saw and what the cluster_* functions returned.
function handle(array $event): ?string
{
    if ($event['event'] === 'left') {
        cluster_publish('left:' . json_encode($event));

        return null;
    }

    [$command, $rest] = explode(':', $event['data'], 2) + [1 => ''];

    switch ($command) {
        case 'echo':
            return json_encode(['data' => $rest, 'from' => $event['from'], 'request' => $event['request']]);
        case 'self':
            return cluster_self();
        case 'members':
            return json_encode(cluster_members());
        case 'send':
            [$to, $data] = explode(':', $rest, 2);

            return json_encode([cluster_send($to, $data), cluster_last_error()]);
        case 'publish':
            return json_encode([cluster_publish($rest), cluster_last_error()]);
        case 'request':
            [$to, $timeout, $data] = explode(':', $rest, 3);
            $answer = cluster_request($to, $data, (int) $timeout);

            return json_encode([$answer, cluster_last_error()]);
        case 'after-error':
            [$to, $call] = explode(':', $rest, 2);
            cluster_request($to, 'nobody answers', 100);
            $failed = cluster_last_error();
            match ($call) {
                'self' => cluster_self(),
                'members' => cluster_members(),
                'member-info' => cluster_member_info(),
            };

            return json_encode([$failed, cluster_last_error()]);
        case 'member-info':
            return json_encode(cluster_member_info());
        case 'receive':
            [$mode, $timeout] = explode(':', $rest, 2);

            return receive($mode, (int) $timeout);
        case 'null':
            return null;
    }

    return 'unknown command';
}

// receive takes one message with cluster_receive(); mode says what the
// handler answers.
function receive(string $mode, int $timeout): string
{
    $seen = null;
    $warning = null;
    set_error_handler(function (int $level, string $message) use (&$warning): bool {
        $warning = $message;

        return true;
    });

    try {
        $handled = cluster_receive(function (array $event) use ($mode, &$seen): mixed {
            $seen = $event;

            return match ($mode) {
                'answer' => 'cli:' . $event['data'],
                'null' => null,
                'int' => 42,
                'throw' => throw new RuntimeException('the handler failed'),
            };
        }, $timeout);
    } catch (Throwable $e) {
        return json_encode(['thrown' => $e::class . ': ' . $e->getMessage(), 'event' => $seen]);
    } finally {
        restore_error_handler();
    }

    return json_encode(['handled' => $handled, 'error' => cluster_last_error(), 'event' => $seen, 'warning' => $warning]);
}

while (frankenphp_handle_request('handle')) {
    gc_collect_cycles();
}
