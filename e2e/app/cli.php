<?php

// A CLI participant, run once like a scheduled command: it takes one request,
// asks a server, pushes to a connection, then exits and leaves the cluster.
[, $server, $connection] = $argv;

$answer = function (array $event): string {
    return json_encode(['self' => cluster_self(), 'from' => $event['from'], 'data' => $event['data'], 'sapi' => PHP_SAPI]);
};

// Receiving starts with the first call; the test asks only once it is told who this is.
cluster_receive($answer, 0);
$members = cluster_members();
echo json_encode(['self' => cluster_self(), 'members' => $members]), "\n";

$handled = cluster_receive($answer, 10000);
$receiveError = cluster_last_error();

$answer = cluster_request($server, 'hello from the cli', 2000);
$requestError = cluster_last_error();

$sent = websocket_send($connection, 'from the cli');
$sendError = websocket_last_error();

$listed = in_array($connection, websocket_connections(), true);
$listError = websocket_last_error();

echo json_encode([
    'handled' => $handled,
    'receive_error' => $receiveError,
    'answer' => json_decode($answer, true),
    'request_error' => $requestError,
    'sent' => $sent,
    'send_error' => $sendError,
    'listed' => $listed,
    'list_error' => $listError,
]), "\n";
