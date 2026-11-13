package websocket

import (
	"errors"
	"sync"

	"github.com/gobwas/ws"
)

var (
	errTooManyConnections       = errors.New("too many websocket connections")
	errTooManyClientConnections = errors.New("too many websocket connections from this client")
)

// registry holds the connections of a handler and counts, per client too, the
// slots reserved for them from the handshake on. A zero limit is no limit.
type registry struct {
	maxConnections          int
	maxConnectionsPerClient int

	mu        sync.Mutex
	conns     map[string]*conn
	closed    bool
	reserved  int
	perClient map[string]int
	// held counts the connections from add to remove, which comes after their
	// close event; adds stop once closed, so it is never added to while waited on.
	held sync.WaitGroup
}

func newRegistry(maxConnections, maxConnectionsPerClient int) *registry {
	return &registry{
		maxConnections:          maxConnections,
		maxConnectionsPerClient: maxConnectionsPerClient,
		conns:                   make(map[string]*conn),
		perClient:               make(map[string]int),
	}
}

// reserve takes a slot for a connection of client, to be given back with
// release once the connection is refused or has ended.
func (r *registry) reserve(client string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.maxConnections > 0 && r.reserved >= r.maxConnections {
		return errTooManyConnections
	}
	if r.maxConnectionsPerClient > 0 && r.perClient[client] >= r.maxConnectionsPerClient {
		return errTooManyClientConnections
	}

	r.reserved++
	r.perClient[client]++

	return nil
}

func (r *registry) release(client string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.reserved--
	r.perClient[client]--
	if r.perClient[client] <= 0 {
		delete(r.perClient, client)
	}
}

func (r *registry) add(id string, c *conn) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return false
	}

	r.conns[id] = c
	r.held.Add(1)

	return true
}

// remove forgets a connection that add took, once its close event ran.
func (r *registry) remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.conns, id)
	r.held.Done()
}

func (r *registry) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.conns)
}

// closeAll rejects further adds, then closes all held connections concurrently,
// since each close waits for the peer's close handshake, and returns once
// every one of them is removed, after its close event.
func (r *registry) closeAll(code ws.StatusCode, reason string) {
	r.mu.Lock()
	r.closed = true
	conns := make([]*conn, 0, len(r.conns))
	for _, c := range r.conns {
		conns = append(conns, c)
	}
	r.mu.Unlock()

	var wg sync.WaitGroup
	for _, c := range conns {
		wg.Go(func() {
			c.close(code, reason)
		})
	}
	wg.Wait()
	r.held.Wait()
}
