# Security policy

## Reporting a vulnerability

Report vulnerabilities privately through GitHub's
[private vulnerability reporting](https://github.com/chapterjason/frankenphp-websocket/security/advisories/new),
not in Discussions, which are public.

Include what is affected, how to reproduce it, and the impact you see. You will
get an answer within a week; a fix and an advisory follow once the report is
confirmed.

## Supported versions

No version has been released yet. Fixes go into the `master` branch.

## Scope

The extension is on the network: it parses WebSocket frames, holds connections
and passes the handshake to PHP. Reports about these are in scope, and so are
the cluster buses. Vulnerabilities in FrankenPHP, Caddy, PHP or NATS themselves
belong to those projects.
