package cluster

import (
	"bufio"
	"crypto/rand"
	"crypto/tls"
	"net"
	"net/http"
	"slices"
	"strconv"
	"testing"
	"time"
)

func TestRelayDeliversEachMessageOnce(t *testing.T) {
	newBus := busFactories["relay"](t)

	var a, b, c received
	busA := openBus(t, newBus, "AAAA", &a)
	openBus(t, newBus, "BBBB", &b)
	openBus(t, newBus, "CCCC", &c)

	if err := busA.Publish("", []byte("to all")); err != nil {
		t.Fatal(err)
	}
	if err := busA.Publish("CCCC", []byte("to c")); err != nil {
		t.Fatal(err)
	}
	b.waitFor(t, "to all")
	c.waitFor(t, "to all")
	c.waitFor(t, "to c")
	time.Sleep(100 * time.Millisecond)

	if got := b.snapshot(); !slices.Equal(got, []string{"to all"}) {
		t.Fatalf("b got %q, want the message to all once", got)
	}
	if got := c.snapshot(); !slices.Equal(got, []string{"to all", "to c"}) {
		t.Fatalf("c got %q, want the message to all once and the one to c", got)
	}
	if got := a.snapshot(); len(got) != 0 {
		t.Fatalf("the sender got %q back", got)
	}
}

func TestRelayJoinsEveryPairWithItsOwnTunnel(t *testing.T) {
	newBus := busFactories["relay"](t)

	buses := map[string]*Bus{}
	for _, instance := range []string{"AAAA", "BBBB", "CCCC"} {
		buses[instance] = (openBus(t, newBus, instance, new(received)))
	}

	for instance, b := range buses {
		for other := range buses {
			if other == instance {
				continue
			}
			waitUntil(t, instance+" has a tunnel to "+other, func() bool {
				c := b.connTo(other)

				return c != nil && c.kind == connTunnel
			})
		}
		waitUntil(t, instance+" keeps one connection per member", func() bool { return b.connCount() == 2 })
	}
}

func TestUplinkRefusesARelayThatCannotProveTheSecret(t *testing.T) {
	u := &Uplink{Connect: []string{fakeRelay(t)}, Secret: testBusSecret}
	if err := u.setup(); err != nil {
		t.Fatal(err)
	}
	b := newBus(nil, 0, nil, u)
	openPeerBus(t, b, "BBBB", new(received))

	link := b.uplink
	time.Sleep(200 * time.Millisecond)
	link.mu.Lock()
	defer link.mu.Unlock()
	if len(link.controls) != 0 {
		t.Fatal("the uplink kept a relay that does not know the secret")
	}
}

func TestRelayRefusesAWrongSecret(t *testing.T) {
	relay := startTestRelay(t, &Relay{Listen: "127.0.0.1:0", Secret: "right"})

	a := openPeerBus(t, uplinkBus(t, &Uplink{Connect: []string{relay.Addr()}, Secret: "right"}), "AAAA", new(received))
	openPeerBus(t, uplinkBus(t, &Uplink{Connect: []string{relay.Addr()}, Secret: "wrong"}), "BBBB", new(received))

	time.Sleep(200 * time.Millisecond)
	relay.mu.Lock()
	_, registered := relay.members["BBBB"]
	relay.mu.Unlock()
	if registered || a.connected("BBBB") {
		t.Fatal("a member with the wrong secret got through the relay")
	}
}

func TestRelayRefusesAReplayedHandshake(t *testing.T) {
	relay := &Relay{Listen: "127.0.0.1:0", Secret: testBusSecret}
	server := startTestRelay(t, relay)

	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := rand.Text()
	header := http.Header{
		headerInstance:  {"REPLAYER"},
		headerTime:      {timestamp},
		headerNonce:     {nonce},
		headerSignature: {relay.auth.sign("relay control", "REPLAYER", timestamp, nonce)},
	}

	status := func() int {
		conn, err := net.Dial("tcp", server.Addr())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()

		if err := writeUpgradeRequest(conn, relayControlPath, header); err != nil {
			t.Fatal(err)
		}
		response, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()

		return response.StatusCode
	}

	if got := status(); got != http.StatusSwitchingProtocols {
		t.Fatalf("a signed handshake got %d", got)
	}
	if got := status(); got != http.StatusUnauthorized {
		t.Fatalf("the replayed handshake got %d, want 401", got)
	}
}

func TestRelayMutualTLSEndToEnd(t *testing.T) {
	ca := newTestCA(t)
	relay := startTestRelay(t, &Relay{Listen: "127.0.0.1:0", TLS: ca.clusterTLS(t)})

	a := openPeerBus(t, uplinkBus(t, &Uplink{Connect: []string{relay.Addr()}, TLS: ca.clusterTLS(t)}), "AAAA", new(received))
	var b received
	busB := openPeerBus(t, uplinkBus(t, &Uplink{Connect: []string{relay.Addr()}, TLS: ca.clusterTLS(t)}), "BBBB", &b)
	waitUntil(t, "a is connected to b", func() bool { return a.connected("BBBB") })

	if err := a.Publish("BBBB", []byte("through the relay over tls")); err != nil {
		t.Fatal(err)
	}
	b.waitFor(t, "through the relay over tls")

	if _, ok := busB.connTo("AAAA").conn.(*tls.Conn); !ok {
		t.Fatal("the tunnel does not run tls end to end")
	}

	openPeerBus(t, uplinkBus(t, &Uplink{Connect: []string{relay.Addr()}, TLS: ca.intruderTLS(t)}), "INTRUDER", new(received))
	time.Sleep(200 * time.Millisecond)
	relay.mu.Lock()
	_, registered := relay.members["INTRUDER"]
	relay.mu.Unlock()
	if registered || a.connected("INTRUDER") {
		t.Fatal("a member with a certificate of another CA got through the relay")
	}
}

func TestRelaySharesItsListenerAcrossAReload(t *testing.T) {
	address := freeAddress(t)

	old := startTestRelay(t, &Relay{Listen: address, Secret: testBusSecret})
	startTestRelay(t, &Relay{Listen: address, Secret: testBusSecret})
	old.Stop()

	a := openPeerBus(t, uplinkBus(t, &Uplink{Connect: []string{address}, Secret: testBusSecret}), "AAAA", new(received))
	openPeerBus(t, uplinkBus(t, &Uplink{Connect: []string{address}, Secret: testBusSecret}), "BBBB", new(received))
	waitUntil(t, "members meet through the relay that replaced the old one", func() bool { return a.connected("BBBB") })
}

func TestMixedClusterReachesEveryMember(t *testing.T) {
	relay := startTestRelay(t, &Relay{Listen: "127.0.0.1:0", Secret: testBusSecret})
	uplink := func() *Uplink { return &Uplink{Connect: []string{relay.Addr()}, Secret: testBusSecret} }
	both := func(m *Mesh) *Bus {
		u := uplink()
		if err := u.setup(); err != nil {
			t.Fatal(err)
		}

		return newBus(nil, 0, mustMesh(t, m), u)
	}

	inboxes := map[string]*received{"AAAA": {}, "BBBB": {}, "EEEE": {}}
	a := openPeerBus(t, both(&Mesh{Listen: "127.0.0.1:0", Secret: testBusSecret}), "AAAA", inboxes["AAAA"])
	b := openPeerBus(t, both(&Mesh{Listen: "127.0.0.1:0", Peers: []string{a.MeshAddr()}, Secret: testBusSecret}), "BBBB", inboxes["BBBB"])
	e := openPeerBus(t, uplinkBus(t, uplink()), "EEEE", inboxes["EEEE"])

	buses := map[string]*Bus{"AAAA": a, "BBBB": b, "EEEE": e}
	for from, bus := range buses {
		for to := range buses {
			if to == from {
				continue
			}
			waitUntil(t, from+" reaches "+to, func() bool { return bus.connected(to) })
			if err := bus.Publish(to, []byte(from+" to "+to)); err != nil {
				t.Fatal(err)
			}
			inboxes[to].waitFor(t, from+" to "+to)
		}
	}

	waitUntil(t, "the mesh members keep their direct connection over the tunnel", func() bool {
		ab, ba := a.connTo("BBBB"), b.connTo("AAAA")

		return ab != nil && ba != nil && ab.kind == connDirect && ba.kind == connDirect && a.connCount() == 2 && b.connCount() == 2
	})
	if c := e.connTo("AAAA"); c == nil || c.kind != connTunnel {
		t.Fatal("the uplink-only member does not reach a through a tunnel")
	}
}
