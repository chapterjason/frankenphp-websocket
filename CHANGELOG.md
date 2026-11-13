# Changelog

All notable changes are documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- The `websocket` Caddy directive: WebSockets over HTTP/1.1, HTTP/2 and HTTP/3
  for FrankenPHP, handled by a PHP worker.
- The handshake as an ordinary HTTP request to PHP, which accepts or refuses it
  and sets its response headers.
- The `open`, `message`, `ping`, `pong` and `close` events, with the
  connection's attributes, tags and handshake request.
- PHP functions to send, broadcast, send to a tag, ping and close, to query
  connections and the instance holding them, and to manage their attributes
  and tags.
- `permessage-deflate` compression, when PHP asks for it.
- Limits on connections, message size and message rate; write and close
  timeouts; an idle timeout and keepalive pings, on by default.
- Graceful shutdown: on stop or restart, connections are closed with 1001 as
  the server starts shutting down, and it exits once their close events ran.
- Clusters of instances, reaching connections and answering queries across
  them: members connect directly (`cluster mesh`, with peers from a list or
  DNS) or through relays (`uplink`, `cluster relay`), authenticated by a
  shared secret or mutual TLS.
- PHP messaging between the members of a cluster, without WebSockets too:
  `cluster_send`, `cluster_publish`, `cluster_request`, `cluster_members` and
  `cluster_self`, received by the worker of `cluster member`.
- CLI participants: PHP run with `frankenphp php-cli`, such as queue workers
  and schedulers, joins the cluster of `FRANKENPHP_CLUSTER_CONFIG` without
  accepting connections, and receives with `cluster_receive`; the servers
  pass on what CLI participants send each other.
- Members announce whether they receive: a server with a cluster worker, a
  CLI participant from its first `cluster_receive`. Requests to a member that
  does not fail at once with `CLUSTER_ERROR_NO_RECEIVER`, sends to it fail and
  publishes leave it out; `cluster_member_info` tells each member's role and
  whether it receives.
- The `left` event: a member that leaves, with a goodbye or by timing out,
  is reported once to the cluster worker of one server; a member reported
  after a timeout that comes back closes its connections and rejoins.
- A Docker image built on the official FrankenPHP image.
