# Concept websocket extension

## Role
FrankenPHP extension written in Go that gives PHP applications WebSocket
support. Go holds the connections, PHP handles every event and pushes data
back.

## Components

### Connection handler (Go)
- Caddy directive that accepts WebSocket connections and holds them.
- Dispatches connection events to PHP.

### PHP workers
- Dedicated FrankenPHP worker threads that receive the events: open,
  message, close.
- A reply goes back to the same connection.

### PHP functions
- Exported from Go; let PHP push data to any connection.
- Usable from the workers and from regular HTTP requests.

### Cluster bus (optional)
- Connects multiple instances so a push reaches a connection held by
  another instance.
- Local connections are written directly; others go over the bus to the
  instance that holds them.
- Generic adapter interface; implementations per backend (e.g. Redis, NATS, Unix socket).
- Without an adapter configured, the extension runs as a single instance.

## Technical Constraints
- Built on FrankenPHP's extension worker API, marked experimental.
- Requires a custom FrankenPHP build.
