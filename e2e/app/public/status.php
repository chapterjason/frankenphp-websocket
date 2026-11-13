<?php

header('Content-Type: application/json');

$connection = $_GET['connection'] ?? '';
$open = websocket_is_open($connection);

echo json_encode([
    'open' => $open,
    'attributes' => websocket_attributes($connection),
    'listed' => in_array($connection, websocket_connections(), true),
]);
