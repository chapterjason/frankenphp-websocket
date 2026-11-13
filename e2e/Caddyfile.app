{
	admin off
	local_certs
	skip_install_trust
	auto_https disable_redirects

	cluster mesh {
		listen :7070
		peers app:7070 other:7070
		secret {env.CLUSTER_SECRET}
	}

	uplink {
		connect relay:7071
		secret {env.CLUSTER_SECRET}
	}

	cluster member {
		worker cluster-worker.php 1
	}
}

localhost, app {
	root public

	websocket /ws {
		worker worker.php 2
	}

	php_server
}

http://localhost:8080, http://app:8080 {
	root public

	websocket /ws {
		worker worker.php 2
	}

	php_server
}
