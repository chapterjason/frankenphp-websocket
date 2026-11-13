package cluster

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gobwas/ws"
)

// busFactories create buses that reach each other, one fresh cluster per
// test: in a mesh every bus dials those created before it; through a relay
// the buses only dial the relay.
var busFactories = map[string]func(t testing.TB) func() *Bus{
	"mesh": func(t testing.TB) func() *Bus {
		network := new(busNetwork)

		return func() *Bus {
			b := meshBus(t, &Mesh{Listen: "127.0.0.1:0", Peers: network.addresses(), Secret: testBusSecret})
			network.add(b)

			return b
		}
	},
	"relay": func(t testing.TB) func() *Bus {
		relay := startTestRelay(t, &Relay{Listen: "127.0.0.1:0", Secret: testBusSecret})

		return func() *Bus {
			return uplinkBus(t, &Uplink{Connect: []string{relay.Addr()}, Secret: testBusSecret})
		}
	},
}

const testBusSecret = "test secret"

// busNetwork hands every new bus the addresses of the buses open before it.
type busNetwork struct {
	mu    sync.Mutex
	buses []*Bus
}

func (n *busNetwork) addresses() []string {
	n.mu.Lock()
	defer n.mu.Unlock()

	var addresses []string
	for _, b := range n.buses {
		if address := b.MeshAddr(); address != "" {
			addresses = append(addresses, address)
		}
	}

	return addresses
}

func (n *busNetwork) add(b *Bus) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.buses = append(n.buses, b)
}

func meshBus(t testing.TB, m *Mesh) *Bus {
	t.Helper()

	b, err := NewBus(nil, 0, m, nil)
	if err != nil {
		t.Fatalf("mesh: %v", err)
	}

	return b
}

func uplinkBus(t testing.TB, u *Uplink) *Bus {
	t.Helper()

	b, err := NewBus(nil, 0, nil, u)
	if err != nil {
		t.Fatalf("uplink: %v", err)
	}

	return b
}

func startTestRelay(t testing.TB, r *Relay) *RelayServer {
	t.Helper()

	server, err := StartRelay(r, nil)
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	t.Cleanup(server.Stop)

	return server
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

func openBus(t testing.TB, newBus func() *Bus, instance string, r *received) *Bus {
	t.Helper()

	b := newBus()
	if err := b.Open(instance, r.deliver); err != nil {
		t.Fatalf("open %s: %v", instance, err)
	}
	t.Cleanup(func() { _ = b.Close() })

	return b
}

// stuckBus joins like an instance but never takes a payload off its
// connections, so writes to it block once the socket buffers are full.
func stuckBus(t testing.TB, newBus func() *Bus, instance string) {
	t.Helper()

	release := make(chan struct{})
	b := newBus()
	if err := b.Open(instance, func([]byte) { <-release }); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		close(release)
		_ = b.Close()
	})
}

func waitUntil(t testing.TB, what string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// freeAddress is a loopback address with a port nothing listens on, for
// buses that must know their address before they open.
func freeAddress(t testing.TB) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := ln.Addr().String()
	_ = ln.Close()

	return address
}

func openPeerBus(t testing.TB, b *Bus, instance string, r *received) *Bus {
	t.Helper()

	openBus(t, func() *Bus { return b }, instance, r)

	return b
}

func (b *Bus) peerCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.peers)
}

func (b *Bus) connCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.conns)
}

// connTo is the connection to instance, or nil.
func (b *Bus) connTo(instance string) *busConn {
	b.mu.Lock()
	defer b.mu.Unlock()

	if p := b.peers[instance]; p != nil {
		return p.conn
	}

	return nil
}

func (b *Bus) meshTarget(address string) *meshTarget {
	b.mesh.mu.Lock()
	defer b.mesh.mu.Unlock()

	return b.mesh.targets[address]
}

func mustMesh(t testing.TB, m *Mesh) *Mesh {
	t.Helper()

	if err := m.setup(); err != nil {
		t.Fatal(err)
	}

	return m
}

// fakeMember accepts every member handshake and answers as instance FAKE,
// without knowing the secret.
func fakeMember(t testing.TB) string {
	t.Helper()

	return fakeServer(t, func(conn net.Conn) {
		upgrader := ws.Upgrader{OnBeforeUpgrade: func() (ws.HandshakeHeader, error) {
			return ws.HandshakeHeaderHTTP(http.Header{headerInstance: {"FAKE"}, headerSignature: {"forged"}}), nil
		}}
		_, _ = upgrader.Upgrade(conn)
	})
}

// fakeRelay answers every relay handshake, without knowing the secret.
func fakeRelay(t testing.TB) string {
	t.Helper()

	return fakeServer(t, func(conn net.Conn) {
		_ = writeUpgradeResponse(conn, http.StatusSwitchingProtocols, http.Header{headerSignature: {"forged"}})
	})
}

func fakeServer(t testing.TB, handle func(net.Conn)) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	var (
		mu    sync.Mutex
		conns []net.Conn
	)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			go handle(conn)
		}
	}()

	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
	})

	return ln.Addr().String()
}

func dialAs(t testing.TB, address string, header http.Header) error {
	t.Helper()

	conn, _, _, err := ws.Dialer{Header: ws.HandshakeHeaderHTTP(header), Timeout: 2 * time.Second}.Dial(context.Background(), "ws://"+address+"/")
	if err == nil {
		_ = conn.Close()
	}

	return err
}

type testCA struct {
	file string
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func writeTestFile(t testing.TB, name string, content []byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}

	return path
}

func newTestCA(t testing.TB) testCA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "cluster test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}

	return testCA{file: writeTestFile(t, "ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), cert: cert, key: key}
}

// issue writes the certificate of a server on 127.0.0.1, which it serves and
// presents as a client, and returns its cert and key files.
func (ca testCA) issue(t testing.TB) (string, string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "member"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}

	return writeTestFile(t, "cert.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		writeTestFile(t, "key.pem", pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}

func (ca testCA) clusterTLS(t testing.TB) *TLS {
	t.Helper()

	cert, key := ca.issue(t)

	return &TLS{CA: ca.file, Cert: cert, Key: key}
}

// intruderTLS trusts the cluster's CA, but its own certificate is from another.
func (ca testCA) intruderTLS(t testing.TB) *TLS {
	t.Helper()

	intruder := newTestCA(t).clusterTLS(t)
	intruder.CA = ca.file

	return intruder
}
