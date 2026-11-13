package cluster

import (
	"context"
	"crypto/rand"
	"errors"
	"net"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/gobwas/ws"
)

func TestMeshRefusesAWrongSecret(t *testing.T) {
	a := openPeerBus(t, meshBus(t, &Mesh{Listen: "127.0.0.1:0", Secret: "right"}), "AAAA", new(received))
	b := openPeerBus(t, meshBus(t, &Mesh{Peers: []string{a.MeshAddr()}, Secret: "wrong"}), "BBBB", new(received))

	time.Sleep(200 * time.Millisecond)
	if a.connected("BBBB") || b.connected("AAAA") {
		t.Fatal("members with different secrets connected")
	}
}

func TestMeshRefusesAMemberThatCannotProveTheSecret(t *testing.T) {
	b := openPeerBus(t, meshBus(t, &Mesh{Peers: []string{fakeMember(t)}, Secret: testBusSecret}), "BBBB", new(received))

	time.Sleep(200 * time.Millisecond)
	if b.peerCount() != 0 {
		t.Fatal("the mesh took a member that does not know the secret")
	}
}

func TestMeshRefusesReplayedAndStaleHandshakes(t *testing.T) {
	mesh := mustMesh(t, &Mesh{Listen: "127.0.0.1:0", Secret: testBusSecret})
	a := openPeerBus(t, newBus(nil, 0, mesh, nil), "AAAA", new(received))

	signed := func(at time.Time, nonce string) http.Header {
		timestamp := strconv.FormatInt(at.Unix(), 10)

		return http.Header{
			headerInstance:  {"REPLAYER"},
			headerTime:      {timestamp},
			headerNonce:     {nonce},
			headerSignature: {mesh.auth.sign("member", "REPLAYER", timestamp, nonce)},
		}
	}
	refused := func(err error) bool {
		var status ws.StatusError

		return errors.As(err, &status) && int(status) == http.StatusUnauthorized
	}

	handshake := signed(time.Now(), rand.Text())
	if err := dialAs(t, a.MeshAddr(), handshake); err != nil {
		t.Fatalf("a signed handshake: %v", err)
	}
	if err := dialAs(t, a.MeshAddr(), handshake); !refused(err) {
		t.Fatalf("replayed handshake: err = %v, want 401", err)
	}
	if err := dialAs(t, a.MeshAddr(), signed(time.Now().Add(-time.Minute), rand.Text())); !refused(err) {
		t.Fatalf("stale handshake: err = %v, want 401", err)
	}

	forged := signed(time.Now(), rand.Text())
	forged.Set(headerInstance, "SOMEONEELSE")
	if err := dialAs(t, a.MeshAddr(), forged); !refused(err) {
		t.Fatalf("handshake signed for another instance: err = %v, want 401", err)
	}
	if err := dialAs(t, a.MeshAddr(), http.Header{headerInstance: {"UNSIGNED"}}); !refused(err) {
		t.Fatalf("unsigned handshake: err = %v, want 401", err)
	}
}

func TestMeshMutualTLS(t *testing.T) {
	for name, secret := range map[string]string{"certificates only": "", "certificates and secret": testBusSecret} {
		t.Run(name, func(t *testing.T) {
			ca := newTestCA(t)

			a := openPeerBus(t, meshBus(t, &Mesh{Listen: "127.0.0.1:0", Secret: secret, TLS: ca.clusterTLS(t)}), "AAAA", new(received))
			var b received
			openPeerBus(t, meshBus(t, &Mesh{Peers: []string{a.MeshAddr()}, Secret: secret, TLS: ca.clusterTLS(t)}), "BBBB", &b)
			waitUntil(t, "a is connected to b", func() bool { return a.connected("BBBB") })

			if err := a.Publish("BBBB", []byte("over tls")); err != nil {
				t.Fatal(err)
			}
			b.waitFor(t, "over tls")

			intruder := openPeerBus(t, meshBus(t, &Mesh{Peers: []string{a.MeshAddr()}, Secret: secret, TLS: ca.intruderTLS(t)}), "INTRUDER", new(received))
			time.Sleep(200 * time.Millisecond)
			if a.connected("INTRUDER") || intruder.peerCount() != 0 {
				t.Fatal("a member with a certificate of another CA connected")
			}
		})
	}
}

func TestMeshSkipsItself(t *testing.T) {
	address := freeAddress(t)
	_, port, _ := net.SplitHostPort(address)

	m := &Mesh{Listen: address, Peers: []string{address}, DNS: []string{"self.test:" + port}, Secret: testBusSecret}
	m.lookupHost = func(context.Context, string) ([]string, error) { return []string{"localhost"}, nil }
	b := openPeerBus(t, meshBus(t, m), "SELF", new(received))

	for _, target := range []string{address, "localhost:" + port} {
		waitUntil(t, "the mesh knows "+target+" is itself", func() bool {
			found := b.meshTarget(target)
			if found == nil {
				return false
			}
			found.mu.Lock()
			defer found.mu.Unlock()

			return found.self
		})
	}
	if b.peerCount() != 0 {
		t.Fatal("the mesh connected to itself")
	}
}

func TestMeshKeepsOneConnectionPerPair(t *testing.T) {
	addressA, addressB := freeAddress(t), freeAddress(t)

	var a, b received
	busA := openPeerBus(t, meshBus(t, &Mesh{Listen: addressA, Peers: []string{addressB}, Secret: testBusSecret}), "AAAA", &a)
	busB := openPeerBus(t, meshBus(t, &Mesh{Listen: addressB, Peers: []string{addressA}, Secret: testBusSecret}), "BBBB", &b)

	waitUntil(t, "both dialed each other", func() bool {
		return busA.meshTarget(addressB).reached() == "BBBB" && busB.meshTarget(addressA).reached() == "AAAA"
	})
	waitUntil(t, "one connection is left on each side", func() bool { return busA.connCount() == 1 && busB.connCount() == 1 })

	for _, side := range []*Bus{busA, busB} {
		side.mu.Lock()
		for c := range side.conns {
			if c.dialer != "AAAA" {
				t.Errorf("instance %s kept the connection dialed by %s, want the one of the lower ID", side.instance, c.dialer)
			}
		}
		side.mu.Unlock()
	}

	if err := busA.Publish("BBBB", []byte("a to b")); err != nil {
		t.Fatal(err)
	}
	if err := busB.Publish("AAAA", []byte("b to a")); err != nil {
		t.Fatal(err)
	}
	b.waitFor(t, "a to b")
	a.waitFor(t, "b to a")
}

func TestMeshFindsMembersThroughDNS(t *testing.T) {
	a := openPeerBus(t, meshBus(t, &Mesh{Listen: "127.0.0.1:0", Secret: testBusSecret}), "AAAA", new(received))
	_, port, _ := net.SplitHostPort(a.MeshAddr())

	var (
		mu       sync.Mutex
		resolved []string
	)
	setResolved := func(addresses ...string) {
		mu.Lock()
		defer mu.Unlock()
		resolved = addresses
	}

	m := &Mesh{DNS: []string{"members.test:" + port}, ResolveInterval: caddy.Duration(50 * time.Millisecond), Secret: testBusSecret}
	m.lookupHost = func(_ context.Context, host string) ([]string, error) {
		if host != "members.test" {
			return nil, errors.New("unknown host " + host)
		}
		mu.Lock()
		defer mu.Unlock()

		return slices.Clone(resolved), nil
	}
	c := openPeerBus(t, meshBus(t, m), "CCCC", new(received))

	if c.connected("AAAA") {
		t.Fatal("connected before the name resolved to anything")
	}

	setResolved("127.0.0.1")
	waitUntil(t, "c finds a through DNS", func() bool { return c.connected("AAAA") })

	setResolved()
	waitUntil(t, "c stops dialing what the name no longer resolves to", func() bool { return c.meshTarget("127.0.0.1:"+port) == nil })
	if !c.connected("AAAA") {
		t.Fatal("the connection that is up was dropped with the DNS record")
	}
}

func TestMeshSharesItsListenerAcrossAReload(t *testing.T) {
	address := freeAddress(t)

	old := openPeerBus(t, meshBus(t, &Mesh{Listen: address, Secret: testBusSecret}), "OLD", new(received))
	reloaded := openPeerBus(t, meshBus(t, &Mesh{Listen: address, Secret: testBusSecret}), "NEW", new(received))

	first := openPeerBus(t, meshBus(t, &Mesh{Peers: []string{address}, Secret: testBusSecret}), "FIRST", new(received))
	waitUntil(t, "a new member reaches the newest bus", func() bool { return first.connected("NEW") })
	if first.connected("OLD") {
		t.Fatal("a new member reached the bus being replaced")
	}

	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	second := openPeerBus(t, meshBus(t, &Mesh{Peers: []string{address}, Secret: testBusSecret}), "SECOND", new(received))
	waitUntil(t, "the listener still accepts after the old bus closed", func() bool { return second.connected("NEW") })
	if !first.connected("NEW") || !reloaded.connected("FIRST") {
		t.Fatal("closing the old bus dropped the new bus's connections")
	}
}

func TestMeshCloseStopsListening(t *testing.T) {
	b := openPeerBus(t, meshBus(t, &Mesh{Listen: "127.0.0.1:0", Secret: testBusSecret}), "AAAA", new(received))
	address := b.MeshAddr()

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if conn, err := net.Dial("tcp", address); err == nil {
		_ = conn.Close()
		t.Fatal("the mesh still listens after close")
	}
}

func TestClusterSecretFromTheEnvironment(t *testing.T) {
	t.Setenv("CLUSTER_TEST_SECRET", "from-env")

	a := openPeerBus(t, meshBus(t, &Mesh{Listen: "127.0.0.1:0", Secret: "{env.CLUSTER_TEST_SECRET}"}), "AAAA", new(received))
	openPeerBus(t, meshBus(t, &Mesh{Peers: []string{a.MeshAddr()}, Secret: "from-env"}), "BBBB", new(received))

	waitUntil(t, "a is connected to b", func() bool { return a.connected("BBBB") })
}

func TestMeshLosesNothingWhileItDropsADuplicateConnection(t *testing.T) {
	var delivered atomic.Int64
	busA := meshBus(t, &Mesh{Listen: "127.0.0.1:0", Secret: testBusSecret})
	if err := busA.Open("AAAA", func([]byte) { delivered.Add(1) }); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = busA.Close() })
	busB := openPeerBus(t, meshBus(t, &Mesh{Listen: "127.0.0.1:0", Peers: []string{busA.MeshAddr()}, Secret: testBusSecret}), "BBBB", new(received))
	waitUntil(t, "b reaches a", func() bool { return busB.connected("AAAA") })

	// B publishes over the connection it dialed while A dials B too; A's
	// connection, dialed by the lower ID, then replaces B's on both sides.
	const messages = 50000
	published := make(chan struct{})
	go func() {
		defer close(published)
		for i := 0; i < messages; {
			if busB.Publish("AAAA", []byte("message "+strconv.Itoa(i))) != nil {
				time.Sleep(100 * time.Microsecond)

				continue
			}
			i++
		}
	}()
	busA.mesh.addTarget(busB.MeshAddr(), "127.0.0.1", "")
	<-published

	waitUntil(t, "the connection dialed by a replaced the one of b", func() bool {
		c := busA.connTo("BBBB")

		return c != nil && c.dialer == "AAAA" && busA.connCount() == 1 && busB.connCount() == 1
	})
	waitUntil(t, "a got every message", func() bool { return delivered.Load() == messages })
}
