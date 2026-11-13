<?php

header('Content-Type: application/json');

$answer = cluster_request($_GET['to'] ?? '', $_GET['data'] ?? '', 2000);

echo json_encode(['answer' => json_decode($answer, true), 'error' => cluster_last_error()]);
