// Package frankenphpwebsocket adds WebSockets to FrankenPHP. Build it into
// FrankenPHP with xcaddy; importing it registers the Caddy modules and the PHP
// extension.
package frankenphpwebsocket

import (
	_ "github.com/chapterjason/frankenphp-websocket/internal/clusterphp"
	_ "github.com/chapterjason/frankenphp-websocket/internal/websocket"
)
