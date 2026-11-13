package websocket

import (
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/chapterjason/frankenphp-websocket/internal/cluster"
	"github.com/gobwas/ws"
)

// busFactories create buses that reach each other, one fresh cluster per
// test: in a mesh every bus dials those created before it; through a relay
// the buses only dial the relay.
var busFactories = map[string]func(t testing.TB) func() *cluster.Bus{
	"mesh": func(t testing.TB) func() *cluster.Bus {
		var (
			mu    sync.Mutex
			buses []*cluster.Bus
		)

		return func() *cluster.Bus {
			mu.Lock()
			defer mu.Unlock()

			var peers []string
			for _, b := range buses {
				if address := b.MeshAddr(); address != "" {
					peers = append(peers, address)
				}
			}
			b := newTestBus(t, &cluster.Mesh{Listen: "127.0.0.1:0", Peers: peers, Secret: testClusterSecret}, nil)
			buses = append(buses, b)

			return b
		}
	},
	"relay": func(t testing.TB) func() *cluster.Bus {
		relay, err := cluster.StartRelay(&cluster.Relay{Listen: "127.0.0.1:0", Secret: testClusterSecret}, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(relay.Stop)

		return func() *cluster.Bus {
			return newTestBus(t, nil, &cluster.Uplink{Connect: []string{relay.Addr()}, Secret: testClusterSecret})
		}
	},
}

const testClusterSecret = "test secret"

func newTestBus(t testing.TB, mesh *cluster.Mesh, uplink *cluster.Uplink) *cluster.Bus {
	t.Helper()

	b, err := cluster.NewBus(nil, 0, mesh, uplink)
	if err != nil {
		t.Fatal(err)
	}

	return b
}

// stuckMember joins as an instance whose handling of connections blocks, so
// once asked about them it takes nothing more off its connections and writes
// to it block when the socket buffers are full.
func stuckMember(t testing.TB, newBus func() *cluster.Bus) *cluster.Membership {
	t.Helper()

	release := make(chan struct{})
	m := cluster.NewMembership()
	m.Handle(connectionsChannel, func(cluster.Message, func([]byte)) { <-release })
	b := newBus()
	if err := m.Join(b, cluster.Settings{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		close(release)
		m.Leave(b)
		_ = b.Close()
	})

	return m
}

// fillStuckQueue gets the stuck member stuck, then publishes to it until its
// queue is full; how many messages that takes depends on the socket buffers
// of the system.
func fillStuckQueue(t testing.TB, n *node, stuck *cluster.Membership) {
	t.Helper()

	go func() { _, _ = n.member.Request(stuck.ID(), connectionsChannel, []byte("{}"), time.Second) }()

	bus := n.member.Bus()
	for range 20 * cluster.DefaultQueueSize {
		if bus.Publish(stuck.ID(), make([]byte, 16<<10)) != nil {
			return
		}
	}
	t.Fatal("the stuck peer's queue never filled")
}

func TestBroadcastCountsTheInstancesAStuckPeerDoesNotHoldUp(t *testing.T) {
	newBus := busFactories["mesh"](t)
	stuck := stuckMember(t, newBus)

	sender, _, senderClient, _ := startNode(t, newBus)
	holder, _, client, _ := startNode(t, newBus)
	waitForPeer(t, sender, holder)
	waitUntilMember(t, sender, stuck.ID())

	fillStuckQueue(t, sender, stuck)

	sent, code := sender.broadcast(true, []byte("despite the stuck peer"))
	if code != WEBSOCKET_ERROR_BUS_FAILED {
		t.Fatalf("broadcast code = %d, want the stuck peer's failure reported", code)
	}
	if sent != 2 {
		t.Fatalf("broadcast sent = %d, want both instances' connections counted", sent)
	}
	senderClient.expectText(t, "despite the stuck peer")
	client.expectText(t, "despite the stuck peer")
}

func joinBus(t testing.TB, n *node, b *cluster.Bus) {
	t.Helper()

	joinWith(t, n.member, b, cluster.Settings{})
}

func joinWith(t testing.TB, m *cluster.Membership, b *cluster.Bus, settings cluster.Settings) {
	t.Helper()

	if err := m.Join(b, settings); err != nil {
		t.Fatalf("join: %v", err)
	}
	t.Cleanup(func() {
		m.Leave(b)
		_ = b.Close()
	})
}

// startNode serves a fresh instance, joined to a bus of newBus, with a client
// connected to it.
func startNode(t testing.TB, newBus func() *cluster.Bus) (*node, *testServer, *wsClient, string) {
	t.Helper()

	return startNodeWith(t, newBus, cluster.Settings{})
}

func startNodeWith(t testing.TB, newBus func() *cluster.Bus, settings cluster.Settings) (*node, *testServer, *wsClient, string) {
	t.Helper()

	n := newNode(cluster.NewMembership())
	joinWith(t, n.member, newBus(), settings)

	s := startServerWith(t, "HTTP/1.1", testWorkers, func(h *Handler) { h.node = n })
	client := s.dial(t)

	return n, s, client, connectionID(t, s.workers, 0)
}

// expectNothingMore fails when a frame other than the pong to a fresh ping is
// pending, so a duplicate delivery shows up.
func (c *wsClient) expectNothingMore(t testing.TB) {
	t.Helper()

	c.send(t, ws.OpPing, true, []byte("nothing more"))
	if op, payload := c.receive(t); op != ws.OpPong || string(payload) != "nothing more" {
		t.Fatalf("got opcode %v payload %q, want only the pong", op, payload)
	}
}

func TestPushReachesAnotherInstance(t *testing.T) {
	for name, factory := range busFactories {
		t.Run(name, func(t *testing.T) {
			newBus := factory(t)

			sender, _, senderClient, _ := startNode(t, newBus)
			holder, holderServer, client, id := startNode(t, newBus)
			waitForPeer(t, sender, holder)
			if self := holder.member.ID(); instanceOf(id) != self {
				t.Fatalf("connection id %q does not name the holding instance %q", id, self)
			}
			holderServer.handler.node.sessions.tag(id, "room")

			if code := sender.send(id, true, []byte("hello")); code != WEBSOCKET_ERROR_NONE {
				t.Fatalf("send = %d, want none", code)
			}
			client.expectText(t, "hello")

			if code := sender.ping(id, []byte("are you there")); code != WEBSOCKET_ERROR_NONE {
				t.Fatalf("ping = %d, want none", code)
			}
			if op, payload := client.receive(t); op != ws.OpPing || string(payload) != "are you there" {
				t.Fatalf("got opcode %v payload %q, want ping", op, payload)
			}

			if sent, code := sender.sendToTag("room", true, []byte("to the room")); sent != 1 || code != WEBSOCKET_ERROR_NONE {
				t.Fatalf("sendToTag = %d, %d; want 1, none", sent, code)
			}
			client.expectText(t, "to the room")

			if sent, code := sender.broadcast(true, []byte("to all")); sent != 2 || code != WEBSOCKET_ERROR_NONE {
				t.Fatalf("broadcast = %d, %d; want 2, none", sent, code)
			}
			client.expectText(t, "to all")
			senderClient.expectText(t, "to all")
			senderClient.expectNothingMore(t)
			client.expectNothingMore(t)

			if code := sender.close(id, 4000, "bye"); code != WEBSOCKET_ERROR_NONE {
				t.Fatalf("close = %d, want none", code)
			}
			client.expectCloseWithReason(t, 4000, "bye")

			if code := holder.send(id+"x", true, []byte("x")); code != WEBSOCKET_ERROR_NOT_FOUND {
				t.Fatalf("send to an unknown local id = %d, want not found", code)
			}
			if code := sender.send("unknown", true, []byte("x")); code != WEBSOCKET_ERROR_NOT_FOUND {
				t.Fatalf("send to an id naming no instance = %d, want not found", code)
			}
		})
	}
}

func TestWithoutBusOtherInstancesAreNotFound(t *testing.T) {
	n := newNode(cluster.NewMembership())

	if code := n.send("OTHERINSTANCE.ABC", true, []byte("x")); code != WEBSOCKET_ERROR_NOT_FOUND {
		t.Fatalf("send = %d, want not found", code)
	}
	if instance, code := n.instance("OTHERINSTANCE.ABC"); instance != "" || code != WEBSOCKET_ERROR_NOT_FOUND {
		t.Fatalf("instance = %q, %d; want not found", instance, code)
	}
	if sent, code := n.broadcast(true, []byte("x")); sent != 0 || code != WEBSOCKET_ERROR_NONE {
		t.Fatalf("broadcast = %d, %d; want 0, none", sent, code)
	}
}

func TestReloadDeliversMessagesToAllOnce(t *testing.T) {
	for name, factory := range busFactories {
		t.Run(name, func(t *testing.T) {
			newBus := factory(t)

			sender, _, _, _ := startNode(t, newBus)
			holder, _, client, id := startNode(t, newBus)

			// A config reload starts the new bus before it stops the old one.
			joinBus(t, holder, newBus())
			waitForPeer(t, sender, holder)

			if _, code := sender.broadcast(true, []byte("once")); code != WEBSOCKET_ERROR_NONE {
				t.Fatalf("broadcast = %d", code)
			}
			client.expectText(t, "once")
			client.expectNothingMore(t)

			if code := sender.send(id, true, []byte("still reachable")); code != WEBSOCKET_ERROR_NONE {
				t.Fatalf("send to a connection named after the old instance = %d", code)
			}
			client.expectText(t, "still reachable")
		})
	}
}

func TestPHPPushesToAnotherInstance(t *testing.T) {
	newBus := busFactories["mesh"](t)
	joinBus(t, local, newBus())

	remoteNode, _, remote, remoteID := startNode(t, newBus)
	waitForPeer(t, local, remoteNode)

	s := startServer(t, "HTTP/1.1")
	client := s.dial(t)

	client.sendText(t, "send-to:"+remoteID+":across instances")
	remote.expectText(t, "across instances")
	client.expectText(t, "sent")

	client.sendText(t, "status:"+remoteID)
	client.expectText(t, `{"open":true,"error":0,"listed":true}`)
}

func TestAppJoinsAndLeavesTheBus(t *testing.T) {
	config := `{"admin":{"disabled":true},"apps":{"cluster":{"mesh":{"listen":"127.0.0.1:0","secret":"s"}}}}`
	if err := caddy.Load([]byte(config), true); err != nil {
		t.Fatalf("load: %v", err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = caddy.Stop()
		}
	})

	id, bus := local.member.ID(), local.member.Bus()
	if bus == nil {
		t.Fatal("the local node did not join the bus")
	}
	address := bus.MeshAddr()
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("the mesh of instance %s does not listen: %v", id, err)
	}
	_ = conn.Close()
	if !strings.HasPrefix(local.connectionID(), id+".") {
		t.Fatal("new connection ids do not name the instance")
	}

	if err := caddy.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	stopped = true
	if local.member.Bus() != nil {
		t.Fatal("the local node is still on the bus after stop")
	}
	if conn, err := net.Dial("tcp", address); err == nil {
		_ = conn.Close()
		t.Fatal("the mesh still listens after stop")
	}
}

// waitForPeer waits until n counts other, under its current instance ID, as
// alive.
func waitForPeer(t testing.TB, n, other *node) {
	t.Helper()

	otherID := other.member.ID()
	deadline := time.Now().Add(5 * time.Second)
	for !slices.Contains(n.member.Members(), otherID) {
		if time.Now().After(deadline) {
			t.Fatalf("instance %s never saw %s", n.member.ID(), otherID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestQueriesReachAnotherInstance(t *testing.T) {
	for name, factory := range busFactories {
		t.Run(name, func(t *testing.T) {
			newBus := factory(t)

			asker, _, _, askerID := startNode(t, newBus)
			holder, _, client, id := startNode(t, newBus)
			waitForPeer(t, asker, holder)

			if code := asker.isOpen(id); code != WEBSOCKET_ERROR_NONE {
				t.Fatalf("isOpen = %d, want none", code)
			}
			if instance, code := asker.instance(id); instance != holder.member.ID() || code != WEBSOCKET_ERROR_NONE {
				t.Fatalf("instance = %q, %d; want the holder %s", instance, code, holder.member.ID())
			}
			if instance, code := holder.instance(id); instance != holder.member.ID() || code != WEBSOCKET_ERROR_NONE {
				t.Fatalf("instance on the holder = %q, %d; want itself", instance, code)
			}
			if instance, code := asker.instance(askerID); instance != asker.member.ID() || code != WEBSOCKET_ERROR_NONE {
				t.Fatalf("instance of its own connection = %q, %d; want itself", instance, code)
			}
			for _, unknown := range []string{"NOSUCHINSTANCE.ABC", "NODOT", "", asker.member.ID() + ".GONE"} {
				if instance, code := asker.instance(unknown); instance != "" || code != WEBSOCKET_ERROR_NOT_FOUND {
					t.Fatalf("instance of %q = %q, %d; want not found", unknown, instance, code)
				}
			}

			if code := asker.setAttribute(id, "user", "alice"); code != WEBSOCKET_ERROR_NONE {
				t.Fatalf("setAttribute = %d", code)
			}
			if code := asker.addTag(id, "room"); code != WEBSOCKET_ERROR_NONE {
				t.Fatalf("addTag = %d", code)
			}
			if attributes, code := asker.attributes(id); code != WEBSOCKET_ERROR_NONE || attributes["user"] != "alice" {
				t.Fatalf("attributes = %v, %d", attributes, code)
			}
			if tags, code := asker.tags(id); code != WEBSOCKET_ERROR_NONE || !slices.Equal(tags, []string{"room"}) {
				t.Fatalf("tags = %v, %d", tags, code)
			}

			if sent, code := asker.sendToTag("room", true, []byte("tagged remotely")); sent != 1 || code != WEBSOCKET_ERROR_NONE {
				t.Fatalf("sendToTag = %d, %d; want 1, none", sent, code)
			}
			client.expectText(t, "tagged remotely")

			if ids, code := asker.connectionsWithTag("room"); code != WEBSOCKET_ERROR_NONE || !slices.Equal(ids, []string{id}) {
				t.Fatalf("connectionsWithTag = %v, %d; want the remote connection", ids, code)
			}
			if code := asker.addTag(askerID, "room"); code != WEBSOCKET_ERROR_NONE {
				t.Fatalf("addTag on its own connection = %d", code)
			}
			if ids, code := asker.connectionsWithTag("room"); code != WEBSOCKET_ERROR_NONE || !slices.Equal(ids, slices.Sorted(slices.Values([]string{id, askerID}))) {
				t.Fatalf("connectionsWithTag = %v, %d; want both instances' connections", ids, code)
			}
			if ids, code := asker.connectionsWithTag("nobody"); code != WEBSOCKET_ERROR_NONE || ids == nil || len(ids) != 0 {
				t.Fatalf("connectionsWithTag of an unused tag = %#v, %d; want an empty list", ids, code)
			}

			if code := asker.removeAttribute(id, "user"); code != WEBSOCKET_ERROR_NONE {
				t.Fatalf("removeAttribute = %d", code)
			}
			if code := asker.removeTag(id, "room"); code != WEBSOCKET_ERROR_NONE {
				t.Fatalf("removeTag = %d", code)
			}
			if attributes, _ := asker.attributes(id); len(attributes) != 0 {
				t.Fatalf("attributes after removal = %v", attributes)
			}

			ids, code := asker.connections()
			if code != WEBSOCKET_ERROR_NONE || !slices.Contains(ids, id) || !slices.Contains(ids, askerID) {
				t.Fatalf("connections = %v, %d; want both instances' connections", ids, code)
			}

			client.send(t, ws.OpClose, true, ws.NewCloseFrameBody(ws.StatusNormalClosure, ""))
			client.expectClose(t, ws.StatusNormalClosure)

			deadline := time.Now().Add(5 * time.Second)
			for asker.isOpen(id) != WEBSOCKET_ERROR_NOT_FOUND {
				if time.Now().After(deadline) {
					t.Fatalf("isOpen after close = %d, want not found", asker.isOpen(id))
				}
				time.Sleep(10 * time.Millisecond)
			}
			if code := asker.setAttribute(id, "user", "bob"); code != WEBSOCKET_ERROR_NOT_FOUND {
				t.Fatalf("setAttribute after close = %d, want not found", code)
			}
		})
	}
}

// silentPeer is a member that announces itself and takes the queries of
// connections, but never answers them.
func silentPeer(t testing.TB, newBus func() *cluster.Bus) (*cluster.Membership, *cluster.Bus) {
	t.Helper()

	m := cluster.NewMembership()
	m.Handle(connectionsChannel, func(cluster.Message, func([]byte)) {})
	b := newBus()
	joinWith(t, m, b, cluster.Settings{})

	return m, b
}

func TestQueriesTimeOutWhenAnInstanceDoesNotAnswer(t *testing.T) {
	for name, factory := range busFactories {
		t.Run(name, func(t *testing.T) {
			newBus := factory(t)

			asker := newNode(cluster.NewMembership())
			joinWith(t, asker.member, newBus(), cluster.Settings{QueryTimeout: 200 * time.Millisecond, PresenceInterval: 100 * time.Millisecond})

			silent, silentBus := silentPeer(t, newBus)
			silentID := silent.ID()
			deadline := time.Now().Add(5 * time.Second)
			for !slices.Contains(asker.member.Members(), silentID) {
				if time.Now().After(deadline) {
					t.Fatal("the silent peer was never seen")
				}
				time.Sleep(10 * time.Millisecond)
			}

			started := time.Now()
			if _, code := asker.connections(); code != WEBSOCKET_ERROR_TIMEOUT {
				t.Fatalf("connections = %d, want timeout", code)
			}
			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatalf("connections waited %v with a 200ms query timeout", elapsed)
			}

			if _, code := asker.connectionsWithTag("room"); code != WEBSOCKET_ERROR_TIMEOUT {
				t.Fatalf("connectionsWithTag = %d, want timeout", code)
			}

			if code := asker.isOpen(silentID + ".ABC"); code != WEBSOCKET_ERROR_TIMEOUT {
				t.Fatalf("isOpen = %d, want timeout", code)
			}

			// Closing the bus without leaving, the peer just stops announcing itself.
			_ = silentBus.Close()
			deadline = time.Now().Add(5 * time.Second)
			for {
				if _, code := asker.connections(); code == WEBSOCKET_ERROR_NONE {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("a peer that stopped announcing itself is still waited for")
				}
				time.Sleep(50 * time.Millisecond)
			}
		})
	}
}

func TestLeavingInstanceIsNotWaitedFor(t *testing.T) {
	newBus := busFactories["mesh"](t)

	asker, _, _, _ := startNodeWith(t, newBus, cluster.Settings{QueryTimeout: 2 * time.Second})

	leaver := newNode(cluster.NewMembership())
	b := newBus()
	if err := leaver.member.Join(b, cluster.Settings{}); err != nil {
		t.Fatal(err)
	}
	waitForPeer(t, asker, leaver)

	leaver.member.Leave(b)
	_ = b.Close()

	leaverID := leaver.member.ID()
	deadline := time.Now().Add(5 * time.Second)
	for slices.Contains(asker.member.Members(), leaverID) {
		if time.Now().After(deadline) {
			t.Fatal("the instance that left is still counted")
		}
		time.Sleep(10 * time.Millisecond)
	}

	started := time.Now()
	if _, code := asker.connections(); code != WEBSOCKET_ERROR_NONE || time.Since(started) > time.Second {
		t.Fatalf("connections = %d after %v, want none without waiting", code, time.Since(started))
	}
}

func TestAPureRelayDoesNotJoinTheCluster(t *testing.T) {
	config := `{"admin":{"disabled":true},"apps":{"cluster":{"relay":{"listen":"127.0.0.1:0","secret":"s"}}}}`
	if err := caddy.Load([]byte(config), true); err != nil {
		t.Fatalf("load: %v", err)
	}
	t.Cleanup(func() { _ = caddy.Stop() })

	if local.member.Bus() != nil {
		t.Fatal("a pure relay joined the local node to a bus")
	}
}

func TestAConnectionAcceptedBeforeJoiningIsReachable(t *testing.T) {
	newBus := busFactories["mesh"](t)

	// Caddy may start serving before the cluster app joins.
	holder := newNode(cluster.NewMembership())
	s := startServerWith(t, "HTTP/1.1", testWorkers, func(h *Handler) { h.node = holder })
	client := s.dial(t)
	id := connectionID(t, s.workers, 0)

	joinBus(t, holder, newBus())
	sender, _, _, _ := startNode(t, newBus)
	waitForPeer(t, sender, holder)

	if code := sender.isOpen(id); code != WEBSOCKET_ERROR_NONE {
		t.Fatalf("isOpen = %d, want the connection reachable", code)
	}
	if code := sender.send(id, true, []byte("after joining")); code != WEBSOCKET_ERROR_NONE {
		t.Fatalf("send = %d, want none", code)
	}
	client.expectText(t, "after joining")
}

func TestAnInstanceReportedGoneClosesItsConnections(t *testing.T) {
	n := newNode(cluster.NewMembership())
	s := startServerWith(t, "HTTP/1.1", testWorkers, func(h *Handler) {
		h.node = n
		h.CloseTimeout = caddy.Duration(300 * time.Millisecond)
	})
	first, second := s.dial(t), s.dial(t)
	waitForConnections(t, s.handler, 2)

	n.closeAll(statusRejoining, "rejoining the cluster")

	closes := 0
	for _, event := range s.workers.recorded() {
		if event["event"] == "close" && event["code"] == int(statusRejoining) {
			closes++
		}
	}
	if closes != 2 {
		t.Fatalf("%d close events with 1012 had run when closeAll returned, want 2", closes)
	}
	for _, client := range []*wsClient{first, second} {
		client.expectCloseWithReason(t, statusRejoining, "rejoining the cluster")
	}
	if len(n.sessions.all()) != 0 {
		t.Fatal("sessions are left after closeAll")
	}
}

func waitUntilMember(t testing.TB, n *node, id string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for !slices.Contains(n.member.Members(), id) {
		if time.Now().After(deadline) {
			t.Fatalf("instance %s never saw %s", n.member.ID(), id)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
