{
	admin off
	local_certs
	skip_install_trust
	auto_https disable_redirects

	websocket {
		bus nats nats://nats:4222
	}
}

localhost, app {
	root public

	websocket /ws {
		worker worker.php 2
	}

	php_server
}
