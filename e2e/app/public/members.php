<?php

header('Content-Type: application/json');

echo json_encode(['self' => cluster_self(), 'members' => cluster_members()]);
