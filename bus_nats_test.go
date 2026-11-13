package frankenphpwebsocket

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/nats-io/jwt/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nkeys"
)

func startNATSServerWith(t testing.TB, opts *natsserver.Options) string {
	t.Helper()

	opts.Host, opts.Port, opts.NoLog, opts.NoSigs = "127.0.0.1", -1, true, true

	srv, err := natsserver.NewServer(opts)
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

func writeTestFile(t testing.TB, name string, content []byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}

	return path
}

// provisionedNATSBus returns the provisioned bus, or the provisioning error.
func provisionedNATSBus(t testing.TB, b *NATSBus) (*NATSBus, error) {
	t.Helper()

	return b, b.Provision(caddy.Context{})
}

// expectNATSBusWorks opens two buses configured by configure and passes a
// payload between them.
func expectNATSBusWorks(t testing.TB, url string, configure func(*NATSBus)) {
	t.Helper()

	newBus := func() Bus {
		b := &NATSBus{URL: url}
		configure(b)
		if _, err := provisionedNATSBus(t, b); err != nil {
			t.Fatalf("provision: %v", err)
		}

		return b
	}

	var b received
	busA := openBus(t, newBus, "AAAA", new(received))
	openBus(t, newBus, "BBBB", &b)

	if err := busA.Publish("BBBB", []byte("authenticated")); err != nil {
		t.Fatal(err)
	}
	b.waitFor(t, "authenticated")
}

func expectNATSBusRefused(t testing.TB, url string, configure func(*NATSBus)) {
	t.Helper()

	b := &NATSBus{URL: url}
	configure(b)
	if _, err := provisionedNATSBus(t, b); err != nil {
		t.Fatalf("provision: %v", err)
	}

	if err := b.Open("AAAA", func([]byte) {}); err == nil {
		_ = b.Close()
		t.Fatal("open succeeded, want the server to refuse it")
	}
}

func TestNATSBusToken(t *testing.T) {
	url := startNATSServerWith(t, &natsserver.Options{Authorization: "s3cr3t"})

	expectNATSBusWorks(t, url, func(b *NATSBus) { b.Token = "s3cr3t" })
	expectNATSBusRefused(t, url, func(b *NATSBus) { b.Token = "wrong" })
	expectNATSBusRefused(t, url, func(*NATSBus) {})
}

func TestNATSBusTokenFromTheEnvironment(t *testing.T) {
	url := startNATSServerWith(t, &natsserver.Options{Authorization: "from-env"})
	t.Setenv("WEBSOCKET_TEST_NATS_TOKEN", "from-env")

	expectNATSBusWorks(t, url, func(b *NATSBus) { b.Token = "{env.WEBSOCKET_TEST_NATS_TOKEN}" })
}

func TestNATSBusUserAndPassword(t *testing.T) {
	url := startNATSServerWith(t, &natsserver.Options{Username: "ws", Password: "pa55"})

	expectNATSBusWorks(t, url, func(b *NATSBus) { b.Username, b.Password = "ws", "pa55" })
	expectNATSBusRefused(t, url, func(b *NATSBus) { b.Username, b.Password = "ws", "wrong" })
}

func TestNATSBusNKey(t *testing.T) {
	user, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	public, _ := user.PublicKey()
	seed, _ := user.Seed()
	seedFile := writeTestFile(t, "user.nk", seed)

	other, _ := nkeys.CreateUser()
	otherSeed, _ := other.Seed()
	otherSeedFile := writeTestFile(t, "other.nk", otherSeed)

	url := startNATSServerWith(t, &natsserver.Options{Nkeys: []*natsserver.NkeyUser{{Nkey: public}}})

	expectNATSBusWorks(t, url, func(b *NATSBus) { b.NKey = seedFile })
	expectNATSBusRefused(t, url, func(b *NATSBus) { b.NKey = otherSeedFile })
}

func TestNATSBusCredentials(t *testing.T) {
	operator, _ := nkeys.CreateOperator()
	operatorPublic, _ := operator.PublicKey()
	operatorClaims := jwt.NewOperatorClaims(operatorPublic)
	if _, err := operatorClaims.Encode(operator); err != nil {
		t.Fatal(err)
	}

	account, _ := nkeys.CreateAccount()
	accountPublic, _ := account.PublicKey()
	accountJWT, err := jwt.NewAccountClaims(accountPublic).Encode(operator)
	if err != nil {
		t.Fatal(err)
	}

	credentials := func(name string, signer nkeys.KeyPair) string {
		user, _ := nkeys.CreateUser()
		userPublic, _ := user.PublicKey()
		userSeed, _ := user.Seed()
		userJWT, err := jwt.NewUserClaims(userPublic).Encode(signer)
		if err != nil {
			t.Fatal(err)
		}
		creds, err := jwt.FormatUserConfig(userJWT, userSeed)
		if err != nil {
			t.Fatal(err)
		}

		return writeTestFile(t, name, creds)
	}

	resolver := new(natsserver.MemAccResolver)
	if err := resolver.Store(accountPublic, accountJWT); err != nil {
		t.Fatal(err)
	}
	url := startNATSServerWith(t, &natsserver.Options{
		TrustedOperators: []*jwt.OperatorClaims{operatorClaims},
		AccountResolver:  resolver,
	})

	signed := credentials("user.creds", account)
	foreignAccount, _ := nkeys.CreateAccount()
	foreign := credentials("foreign.creds", foreignAccount)

	expectNATSBusWorks(t, url, func(b *NATSBus) { b.Credentials = signed })
	expectNATSBusRefused(t, url, func(b *NATSBus) { b.Credentials = foreign })
}

type testCertificates struct {
	caFile, serverCertFile, serverKeyFile, clientCertFile, clientKeyFile string
	server                                                               tls.Certificate
	caPool                                                               *x509.CertPool
}

func newTestCertificates(t testing.TB) testCertificates {
	t.Helper()

	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "websocket test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(caDER)

	issue := func(serial int64, usage x509.ExtKeyUsage) ([]byte, []byte, tls.Certificate) {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		template := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: "127.0.0.1"},
			IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		}
		der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
		pair, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			t.Fatal(err)
		}

		return certPEM, keyPEM, pair
	}

	serverCert, serverKey, server := issue(2, x509.ExtKeyUsageServerAuth)
	clientCert, clientKey, _ := issue(3, x509.ExtKeyUsageClientAuth)

	pool := x509.NewCertPool()
	pool.AddCert(ca)

	return testCertificates{
		caFile:         writeTestFile(t, "ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})),
		serverCertFile: writeTestFile(t, "server.pem", serverCert),
		serverKeyFile:  writeTestFile(t, "server-key.pem", serverKey),
		clientCertFile: writeTestFile(t, "client.pem", clientCert),
		clientKeyFile:  writeTestFile(t, "client-key.pem", clientKey),
		server:         server,
		caPool:         pool,
	}
}

func TestNATSBusTLS(t *testing.T) {
	certs := newTestCertificates(t)
	url := startNATSServerWith(t, &natsserver.Options{
		TLS:       true,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{certs.server}, MinVersion: tls.VersionTLS12},
	})

	expectNATSBusWorks(t, url, func(b *NATSBus) { b.TLS = &NATSTLS{CA: certs.caFile} })
	expectNATSBusRefused(t, url, func(b *NATSBus) { b.TLS = &NATSTLS{} })
}

func TestNATSBusMutualTLS(t *testing.T) {
	certs := newTestCertificates(t)
	url := startNATSServerWith(t, &natsserver.Options{
		TLS:       true,
		TLSVerify: true,
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{certs.server},
			ClientCAs:    certs.caPool,
			ClientAuth:   tls.RequireAndVerifyClientCert,
			MinVersion:   tls.VersionTLS12,
		},
	})

	expectNATSBusWorks(t, url, func(b *NATSBus) {
		b.TLS = &NATSTLS{CA: certs.caFile, Cert: certs.clientCertFile, Key: certs.clientKeyFile}
	})
	expectNATSBusRefused(t, url, func(b *NATSBus) { b.TLS = &NATSTLS{CA: certs.caFile} })
}

func TestNATSBusRejectsInvalidSettings(t *testing.T) {
	certs := newTestCertificates(t)
	notPEM := writeTestFile(t, "not.pem", []byte("not a certificate"))
	notSeed := writeTestFile(t, "not.nk", []byte("not a seed"))

	tests := map[string]NATSBus{
		"two auth methods":              {Token: "a", Username: "b", Password: "c"},
		"password without user":         {Password: "c"},
		"cert without key":              {TLS: &NATSTLS{Cert: certs.clientCertFile}},
		"missing ca":                    {TLS: &NATSTLS{CA: filepath.Join(t.TempDir(), "missing.pem")}},
		"ca file without a certificate": {TLS: &NATSTLS{CA: notPEM}},
		"invalid nkey seed":             {NKey: notSeed},
	}

	for name, b := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := provisionedNATSBus(t, &b); err == nil {
				t.Fatal("provision succeeded, want an error")
			}
		})
	}
}

func TestUnmarshalNATSBusOptions(t *testing.T) {
	input := `nats tls://nats:4222 {
	subject chat
	credentials /etc/nats/ws.creds
	tls {
		ca /etc/nats/ca.pem
		cert /etc/nats/client.pem
		key /etc/nats/client-key.pem
		server_name nats.internal
	}
}`

	var got NATSBus
	if err := got.UnmarshalCaddyfile(caddyfile.NewTestDispenser(input)); err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(got)
	want := `{"url":"tls://nats:4222","subject":"chat","credentials":"/etc/nats/ws.creds","tls":{"ca":"/etc/nats/ca.pem","cert":"/etc/nats/client.pem","key":"/etc/nats/client-key.pem","server_name":"nats.internal"}}`
	if string(encoded) != want {
		t.Fatalf("got %s, want %s", encoded, want)
	}

	var withUser NATSBus
	if err := withUser.UnmarshalCaddyfile(caddyfile.NewTestDispenser("nats {\n\tuser ws pa55\n\tnkey /seed\n\ttoken t\n}")); err != nil {
		t.Fatal(err)
	}
	if withUser.Username != "ws" || withUser.Password != "pa55" || withUser.NKey != "/seed" || withUser.Token != "t" {
		t.Fatalf("got %+v", withUser)
	}

	for _, invalid := range []string{
		"nats {\n\tuser ws\n}",
		"nats {\n\tuser ws pa55 extra\n}",
		"nats {\n\ttoken\n}",
		"nats {\n\ttls {\n\t\tunknown x\n\t}\n}",
		"nats {\n\ttls {\n\t\tca\n\t}\n}",
		"nats {\n\ttls extra\n}",
	} {
		if err := new(NATSBus).UnmarshalCaddyfile(caddyfile.NewTestDispenser(invalid)); err == nil {
			t.Errorf("expected an error for %q", invalid)
		}
	}
}
