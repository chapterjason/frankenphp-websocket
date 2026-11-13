<?php

echo websocket_send($_GET['connection'] ?? '', $_GET['data'] ?? '') ? 'sent' : 'not sent';
