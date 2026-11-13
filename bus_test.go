package frankenphpwebsocket

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/gobwas/ws"
	natsserver "github.com/nats-io/nats-server/v2/server"
)

// busFactories create buses that reach each other, one fresh network per test.
var busFactories = map[string]func(t testing.TB) func() Bus{
	"unix": func(t testing.TB) func() Bus {
		dir := shortTempDir(t)

		return func() Bus { return &UnixBus{Directory: dir} }
	},
	"nats": func(t testing.TB) func() Bus {
		url := startNATSServer(t)

		return func() Bus {
			b := &NATSBus{URL: url}
			if err := b.Provision(caddy.Context{}); err != nil {
				t.Fatal(err)
			}

			return b
		}
	},
}

// shortTempDir keeps socket paths under the 104-byte limit of macOS.
func shortTempDir(t testing.TB) string {
	t.Helper()

	dir, err := os.MkdirTemp("", "b")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	return dir
}

func startNATSServer(t testing.TB) string {
	t.Helper()

	srv, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	t.Cleanup(srv.Shutdown)

	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server did not start")
	}

	return srv.ClientURL()
}

type received struct {
	mu       sync.Mutex
	payloads []string
}

func (r *received) deliver(payload []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.payloads = append(r.payloads, string(payload))
}

func (r *received) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.payloads...)
}

func (r *received) waitFor(t testing.TB, want string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, payload := range r.snapshot() {
			if payload == want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("payload %q not delivered; got %q", want, r.snapshot())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func openBus(t testing.TB, newBus func() Bus, instance string, r *received) Bus {
	t.Helper()

	b := newBus()
	if err := b.Open(instance, r.deliver); err != nil {
		t.Fatalf("open %s: %v", instance, err)
	}
	t.Cleanup(func() { _ = b.Close() })

	return b
}

func TestBusContract(t *testing.T) {
	for name, factory := range busFactories {
		t.Run(name, func(t *testing.T) {
			newBus := factory(t)

			var a, b, c received
			busA := openBus(t, newBus, "AAAA", &a)
			openBus(t, newBus, "BBBB", &b)
			openBus(t, newBus, "CCCC", &c)

			if err := busA.Publish("BBBB", []byte("to b")); err != nil {
				t.Fatalf("publish to b: %v", err)
			}
			b.waitFor(t, "to b")

			if err := busA.Publish("", []byte("to all")); err != nil {
				t.Fatalf("publish to all: %v", err)
			}
			b.waitFor(t, "to all")
			c.waitFor(t, "to all")

			if err := busA.Publish("GONE", []byte("to nobody")); err != nil {
				t.Fatalf("publish to a gone instance: %v", err)
			}

			if err := busA.Publish("BBBB", []byte("after")); err != nil {
				t.Fatalf("publish after: %v", err)
			}
			b.waitFor(t, "after")

			for _, payload := range c.snapshot() {
				if payload == "to b" || payload == "after" {
					t.Fatalf("instance c got %q, which was addressed to b", payload)
				}
			}
		})
	}
}

func TestBusDeliversAgainAfterThePeerRestarts(t *testing.T) {
	for name, factory := range busFactories {
		t.Run(name, func(t *testing.T) {
			newBus := factory(t)

			var a, b received
			busA := openBus(t, newBus, "AAAA", &a)
			busB := openBus(t, newBus, "BBBB", &b)

			if err := busA.Publish("BBBB", []byte("first")); err != nil {
				t.Fatal(err)
			}
			b.waitFor(t, "first")

			if err := busB.Close(); err != nil {
				t.Fatal(err)
			}
			var restarted received
			openBus(t, newBus, "BBBB", &restarted)

			if err := busA.Publish("BBBB", []byte("second")); err != nil {
				t.Fatalf("publish after restart: %v", err)
			}
			restarted.waitFor(t, "second")
		})
	}
}

func TestUnixBusSkipsAndRemovesStaleSockets(t *testing.T) {
	dir := shortTempDir(t)

	stale, err := net.Listen("unix", filepath.Join(dir, "DEAD"+unixSocketSuffix))
	if err != nil {
		t.Fatal(err)
	}
	stale.(*net.UnixListener).SetUnlinkOnClose(false)
	_ = stale.Close()

	var b received
	busA := openBus(t, func() Bus { return &UnixBus{Directory: dir} }, "AAAA", new(received))
	openBus(t, func() Bus { return &UnixBus{Directory: dir} }, "BBBB", &b)

	if err := busA.Publish("", []byte("to all")); err != nil {
		t.Fatalf("publish with a stale socket around: %v", err)
	}
	b.waitFor(t, "to all")

	if _, err := os.Stat(filepath.Join(dir, "DEAD"+unixSocketSuffix)); !os.IsNotExist(err) {
		t.Fatalf("stale socket still there: %v", err)
	}
}

func TestUnixBusRefusesTooLongSocketPaths(t *testing.T) {
	b := &UnixBus{Directory: filepath.Join(shortTempDir(t), strings.Repeat("d", 100))}
	if err := b.Open("AAAA", func([]byte) {}); err == nil || !strings.Contains(err.Error(), "shorter directory") {
		t.Fatalf("err = %v, want the socket path length explained", err)
	}
}

// stuckPeer listens like an instance but never reads, so writes to it block
// once the socket buffers are full.
func stuckPeer(t testing.TB, dir, instance string) {
	t.Helper()

	listener, err := net.Listen("unix", filepath.Join(dir, instance+unixSocketSuffix))
	if err != nil {
		t.Fatal(err)
	}

	var (
		mu    sync.Mutex
		conns []net.Conn
	)
	go func() {
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()

	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
}

func TestUnixBusStuckPeerDoesNotHoldUpTheOthers(t *testing.T) {
	dir := shortTempDir(t)
	stuckPeer(t, dir, "STUCK")

	var b received
	busA := openBus(t, func() Bus { return &UnixBus{Directory: dir} }, "AAAA", new(received))
	openBus(t, func() Bus { return &UnixBus{Directory: dir} }, "BBBB", &b)

	// Publish until the stuck peer's queue is full and a hundred more; how many
	// that takes depends on the socket buffers of the system.
	payload := make([]byte, 16<<10)
	var (
		queueFull error
		published int
		afterFull int
	)
	started := time.Now()
	for published < 20*defaultUnixQueueSize && afterFull < 100 {
		err := busA.Publish("", payload)
		published++
		if err != nil {
			if !strings.Contains(err.Error(), "STUCK"+unixSocketSuffix) || !strings.Contains(err.Error(), "queue") {
				t.Fatalf("publish error = %v, want only the stuck peer's full queue", err)
			}
			queueFull = err
		}
		if queueFull != nil {
			afterFull++
		}
	}
	if queueFull == nil {
		t.Fatal("the stuck peer's full queue was not reported")
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("publishing %d messages took %v with a stuck peer", published, elapsed)
	}

	started = time.Now()
	if err := busA.Publish("BBBB", []byte("after the flood")); err != nil {
		t.Fatalf("publish to b: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("a publish to b took %v behind the stuck peer", elapsed)
	}
	b.waitFor(t, "after the flood")

	if got := len(b.snapshot()); got != published+1 {
		t.Fatalf("b got %d payloads, want all %d", got, published+1)
	}

	started = time.Now()
	if err := busA.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if elapsed := time.Since(started); elapsed > unixWriteTimeout+2*time.Second {
		t.Fatalf("close took %v with a stuck peer, want at most about the %v write timeout", elapsed, unixWriteTimeout)
	}
	if err := busA.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// fillStuckQueue publishes to the stuck peer until its queue is full; how many
// messages that takes depends on the socket buffers of the system.
func fillStuckQueue(t testing.TB, n *node) {
	t.Helper()

	_, bus := n.current()
	for range 20 * defaultUnixQueueSize {
		if bus.Publish("STUCK", make([]byte, 16<<10)) != nil {
			return
		}
	}
	t.Fatal("the stuck peer's queue never filled")
}

func TestBroadcastCountsTheInstancesAStuckPeerDoesNotHoldUp(t *testing.T) {
	dir := shortTempDir(t)
	newBus := func() Bus { return &UnixBus{Directory: dir} }
	stuckPeer(t, dir, "STUCK")

	sender, _, senderClient, _ := startNode(t, newBus)
	holder, _, client, _ := startNode(t, newBus)
	waitForPeer(t, sender, holder)

	fillStuckQueue(t, sender)

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

func TestUnixBusPublishToAMissingInstanceStartsNoWriter(t *testing.T) {
	dir := shortTempDir(t)

	bus := &UnixBus{Directory: dir}
	if err := bus.Open("AAAA", func([]byte) {}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close() })

	if err := bus.Publish("GONE", []byte("x")); err != nil {
		t.Fatal(err)
	}

	bus.mu.Lock()
	defer bus.mu.Unlock()
	if len(bus.peers) != 0 {
		t.Fatalf("peers = %v, want none for a missing instance", bus.peers)
	}
}

func TestUnixBusCloseRemovesItsSocket(t *testing.T) {
	dir := shortTempDir(t)

	b := &UnixBus{Directory: dir}
	if err := b.Open("AAAA", func([]byte) {}); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(filepath.Join(dir, "AAAA"+unixSocketSuffix)); !os.IsNotExist(err) {
		t.Fatalf("socket still there after close: %v", err)
	}
}

func joinBus(t testing.TB, n *node, b Bus) {
	t.Helper()

	if err := n.joinBus(b, busSettings{}); err != nil {
		t.Fatalf("join bus: %v", err)
	}
	t.Cleanup(func() {
		n.leaveBus(b)
		_ = b.Close()
	})
}

// startNode serves a fresh instance, joined to a bus of newBus, with a client
// connected to it.
func startNode(t testing.TB, newBus func() Bus) (*node, *testServer, *wsClient, string) {
	t.Helper()

	n := newNode()
	joinBus(t, n, newBus())

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
			if self, _ := holder.current(); instanceOf(id) != self {
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
	n := newNode()

	if code := n.send("OTHERINSTANCE.ABC", true, []byte("x")); code != WEBSOCKET_ERROR_NOT_FOUND {
		t.Fatalf("send = %d, want not found", code)
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
	newBus := busFactories["unix"](t)
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

func TestUnmarshalGlobalOption(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{input: "websocket {\n\tbus unix /run/ws\n}", want: `{"bus":{"adapter":"unix","directory":"/run/ws"}}`},
		{input: "websocket {\n\tbus nats\n}", want: `{"bus":{"adapter":"nats"}}`},
		{input: "websocket {\n\tbus nats nats://a:4222,nats://b:4222 {\n\t\tsubject chat\n\t}\n}", want: `{"bus":{"adapter":"nats","subject":"chat","url":"nats://a:4222,nats://b:4222"}}`},
		{input: "websocket", want: `{}`},
		{input: "websocket {\n\tbus unix\n}", wantErr: true},
		{input: "websocket {\n\tbus redis\n}", wantErr: true},
		{input: "websocket {\n\tbus nats {\n\t\tunknown\n\t}\n}", wantErr: true},
		{input: "websocket {\n\tunknown\n}", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseGlobalOption(caddyfile.NewTestDispenser(tt.input), nil)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}

			app := got.(httpcaddyfile.App)
			var gotJSON, wantJSON any
			if err := json.Unmarshal(app.Value, &gotJSON); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(tt.want), &wantJSON); err != nil {
				t.Fatal(err)
			}
			if app.Name != "websocket" || !jsonEqual(gotJSON, wantJSON) {
				t.Fatalf("got app %q %s, want %s", app.Name, app.Value, tt.want)
			}
		})
	}
}

func jsonEqual(a, b any) bool {
	encodedA, _ := json.Marshal(a)
	encodedB, _ := json.Marshal(b)

	return string(encodedA) == string(encodedB)
}

func TestAppJoinsAndLeavesTheBus(t *testing.T) {
	dir := shortTempDir(t)

	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	defer cancel()

	app := &App{BusRaw: json.RawMessage(`{"adapter":"unix","directory":"` + dir + `"}`)}
	if err := app.Provision(ctx); err != nil {
		t.Fatalf("provision: %v", err)
	}
	if err := app.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	id, bus := local.current()
	if bus != app.bus {
		t.Fatal("the local node did not join the bus")
	}
	if _, err := os.Stat(filepath.Join(dir, id+unixSocketSuffix)); err != nil {
		t.Fatalf("no socket for instance %s: %v", id, err)
	}
	if !strings.HasPrefix(local.connectionID(), id+".") {
		t.Fatal("new connection ids do not name the instance")
	}

	if err := app.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if _, bus := local.current(); bus != nil {
		t.Fatal("the local node is still on the bus after stop")
	}
}

// waitForPeer waits until n counts other, under its current instance ID, as
// alive.
func waitForPeer(t testing.TB, n, other *node) {
	t.Helper()

	otherID, _ := other.current()
	deadline := time.Now().Add(5 * time.Second)
	for !slices.Contains(n.livePeers(), otherID) {
		if time.Now().After(deadline) {
			t.Fatalf("instance %s never saw %s", func() string { id, _ := n.current(); return id }(), otherID)
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

// silentPeer joins the bus and announces itself like an instance, but answers
// nothing.
func silentPeer(t testing.TB, newBus func() Bus, instance string) Bus {
	t.Helper()

	b := openBus(t, newBus, instance, new(received))
	presence, err := json.Marshal(busMessage{Origin: instance, Kind: busPresence})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Publish("", presence); err != nil {
		t.Fatal(err)
	}

	return b
}

func TestQueriesTimeOutWhenAnInstanceDoesNotAnswer(t *testing.T) {
	for name, factory := range busFactories {
		t.Run(name, func(t *testing.T) {
			newBus := factory(t)

			asker := newNode()
			b := newBus()
			if err := asker.joinBus(b, busSettings{queryTimeout: 200 * time.Millisecond, presenceInterval: 100 * time.Millisecond}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				asker.leaveBus(b)
				_ = b.Close()
			})

			silent := silentPeer(t, newBus, "SILENTPEER123")
			deadline := time.Now().Add(5 * time.Second)
			for !slices.Contains(asker.livePeers(), "SILENTPEER123") {
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

			if code := asker.isOpen("SILENTPEER123.ABC"); code != WEBSOCKET_ERROR_TIMEOUT {
				t.Fatalf("isOpen = %d, want timeout", code)
			}

			_ = silent.Close()
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
	newBus := busFactories["unix"](t)

	asker, _, _, _ := startNode(t, newBus)
	asker.mu.Lock()
	asker.queryTimeout = 2 * time.Second
	asker.mu.Unlock()

	leaver := newNode()
	b := newBus()
	if err := leaver.joinBus(b, busSettings{}); err != nil {
		t.Fatal(err)
	}
	waitForPeer(t, asker, leaver)

	leaver.leaveBus(b)
	_ = b.Close()

	leaverID, _ := leaver.current()
	deadline := time.Now().Add(5 * time.Second)
	for slices.Contains(asker.livePeers(), leaverID) {
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

func TestUnmarshalGlobalQueryTimeout(t *testing.T) {
	got, err := parseGlobalOption(caddyfile.NewTestDispenser("websocket {\n\tbus nats\n\tquery_timeout 3s\n}"), nil)
	if err != nil {
		t.Fatal(err)
	}

	var app App
	if err := json.Unmarshal(got.(httpcaddyfile.App).Value, &app); err != nil {
		t.Fatal(err)
	}
	if app.QueryTimeout != caddy.Duration(3*time.Second) {
		t.Fatalf("query timeout = %v, want 3s", app.QueryTimeout)
	}

	for _, invalid := range []string{"websocket {\n\tquery_timeout\n}", "websocket {\n\tquery_timeout 0s\n}", "websocket {\n\tquery_timeout soon\n}"} {
		if _, err := parseGlobalOption(caddyfile.NewTestDispenser(invalid), nil); err == nil {
			t.Errorf("expected an error for %q", invalid)
		}
	}
}

func TestUnixBusQueueSizeIsConfigurable(t *testing.T) {
	dir := shortTempDir(t)
	stuckPeer(t, dir, "STUCK")

	busA := openBus(t, func() Bus { return &UnixBus{Directory: dir, QueueSize: 4} }, "AAAA", new(received))

	var failedAt int
	for i := range 200 {
		if err := busA.Publish("STUCK", make([]byte, 64<<10)); err != nil {
			failedAt = i
			break
		}
	}
	if failedAt == 0 || failedAt > 50 {
		t.Fatalf("the queue of 4 filled after %d messages, want a handful", failedAt)
	}
}

func TestUnmarshalBusTuning(t *testing.T) {
	got, err := parseGlobalOption(caddyfile.NewTestDispenser("websocket {\n\tbus unix /run/ws {\n\t\tqueue_size 64\n\t}\n\tpresence_interval 500ms\n}"), nil)
	if err != nil {
		t.Fatal(err)
	}

	var app struct {
		Bus              UnixBus        `json:"bus"`
		PresenceInterval caddy.Duration `json:"presence_interval"`
	}
	if err := json.Unmarshal(got.(httpcaddyfile.App).Value, &app); err != nil {
		t.Fatal(err)
	}
	if app.Bus.QueueSize != 64 || app.PresenceInterval != caddy.Duration(500*time.Millisecond) {
		t.Fatalf("queue size %d, presence interval %v; want 64, 500ms", app.Bus.QueueSize, app.PresenceInterval)
	}

	for _, invalid := range []string{
		"websocket {\n\tbus unix /run/ws {\n\t\tqueue_size 0\n\t}\n}",
		"websocket {\n\tbus unix /run/ws {\n\t\tqueue_size\n\t}\n}",
		"websocket {\n\tbus unix /run/ws {\n\t\tunknown\n\t}\n}",
		"websocket {\n\tpresence_interval 0s\n}",
	} {
		if _, err := parseGlobalOption(caddyfile.NewTestDispenser(invalid), nil); err == nil {
			t.Errorf("expected an error for %q", invalid)
		}
	}
}
