<?php

header('Content-Type: application/json');

echo json_encode([
    'sent' => websocket_send($_GET['connection'] ?? '', $_GET['data'] ?? ''),
    'error' => websocket_last_error(),
]);
