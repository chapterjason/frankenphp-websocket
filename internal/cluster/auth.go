package cluster

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

const (
	headerInstance  = "X-App-Instance"
	headerTime      = "X-App-Time"
	headerNonce     = "X-App-Nonce"
	headerSignature = "X-App-Signature"
	headerTunnelTo  = "X-App-Tunnel-To"
	headerTunnelID  = "X-App-Tunnel-Id"

	handshakeTimeout = 5 * time.Second
	handshakeMaxSkew = 30 * time.Second
)

// TLS configures TLS between the servers of a cluster. A server's
// certificate is served to those dialing it and presented to those it dials.
type TLS struct {
	// CA is a PEM file of the certificate authorities that verify the other servers. With it, a server without a valid certificate is refused; without it, the system's roots verify the servers dialed.
	CA string `json:"ca,omitempty"`
	// Cert and Key are PEM files of the certificate of this server.
	Cert string `json:"cert,omitempty"`
	Key  string `json:"key,omitempty"`
	// ServerName is the name to verify the certificates of dialed servers against. Default: the host dialed.
	ServerName string `json:"server_name,omitempty"`
}

// credentials is how one part of the cluster authenticates the others: with
// a shared secret, certificates, or both.
type credentials struct {
	secret string
	server *tls.Config
	client *tls.Config
	pool   *x509.CertPool
	cert   *tls.Certificate
}

// newCredentials loads the TLS settings. A server certificate is required when
// the part serves TLS or verifies its peers by certificate.
func newCredentials(secret string, t *TLS, serves bool) (*credentials, error) {
	a := &credentials{secret: secret}
	if t == nil {
		return a, nil
	}

	if (t.Cert == "") != (t.Key == "") {
		return nil, errors.New("tls: cert and key go together")
	}
	if t.Cert == "" && (serves || t.CA != "") {
		return nil, errors.New("tls: cert and key are required")
	}

	var certificates []tls.Certificate
	if t.Cert != "" {
		certificate, err := tls.LoadX509KeyPair(t.Cert, t.Key)
		if err != nil {
			return nil, fmt.Errorf("tls: %w", err)
		}
		a.cert = &certificate
		certificates = []tls.Certificate{certificate}
	}

	if t.CA != "" {
		pem, err := os.ReadFile(t.CA)
		if err != nil {
			return nil, fmt.Errorf("tls: %w", err)
		}
		a.pool = x509.NewCertPool()
		if !a.pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tls: no certificate in %s", t.CA)
		}
	}

	a.server = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: certificates}
	if a.pool != nil {
		a.server.ClientCAs = a.pool
		a.server.ClientAuth = tls.RequireAndVerifyClientCert
	}
	a.client = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: certificates, RootCAs: a.pool, ServerName: t.ServerName}

	return a, nil
}

// authenticates reports whether the settings keep out servers that are not
// part of the cluster.
func (a *credentials) authenticates() bool {
	return a.secret != "" || a.pool != nil
}

// dialTLS starts TLS on a connection this side dialed, verifying the other
// side against serverName unless the settings name one.
func (a *credentials) dialTLS(ctx context.Context, conn net.Conn, serverName string) (net.Conn, error) {
	if a.client == nil {
		return conn, nil
	}

	config := a.client.Clone()
	if config.ServerName == "" {
		config.ServerName = serverName
	}
	tlsConn := tls.Client(conn, config)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return nil, err
	}

	return tlsConn, nil
}

// serverConn serves TLS on an accepted connection; the handshake runs with
// the first read, under the deadline of the handshake that follows.
func (a *credentials) serverConn(conn net.Conn) net.Conn {
	if a.server == nil {
		return conn
	}

	return tls.Server(conn, a.server)
}

// endToEnd reports whether members verify each other by certificate inside a
// tunnel, so the relay can neither read nor alter their traffic.
func (a *credentials) endToEnd() bool {
	return a.pool != nil && a.cert != nil
}

// tunnelTLS starts TLS between two members inside a tunnel. There is no host
// name to verify, so any certificate of the cluster's CA is a member.
func (a *credentials) tunnelTLS(ctx context.Context, conn net.Conn, dialing bool) (net.Conn, error) {
	if !a.endToEnd() {
		return conn, nil
	}

	var tlsConn *tls.Conn
	if dialing {
		tlsConn = tls.Client(conn, &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{*a.cert},
			// The chain is verified below, against the cluster's CA and without a host name.
			InsecureSkipVerify:    true,
			VerifyPeerCertificate: a.verifyMember,
		})
	} else {
		tlsConn = tls.Server(conn, &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{*a.cert},
			ClientCAs:    a.pool,
			ClientAuth:   tls.RequireAndVerifyClientCert,
		})
	}
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return nil, err
	}

	return tlsConn, nil
}

func (a *credentials) verifyMember(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	if len(rawCerts) == 0 {
		return errors.New("no certificate")
	}

	certs := make([]*x509.Certificate, len(rawCerts))
	for i, raw := range rawCerts {
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			return err
		}
		certs[i] = cert
	}

	intermediates := x509.NewCertPool()
	for _, cert := range certs[1:] {
		intermediates.AddCert(cert)
	}
	_, err := certs[0].Verify(x509.VerifyOptions{Roots: a.pool, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})

	return err
}

func (a *credentials) sign(fields ...string) string {
	mac := hmac.New(sha256.New, []byte(a.secret))
	for _, field := range fields {
		mac.Write([]byte(field))
		mac.Write([]byte{'\n'})
	}

	return hex.EncodeToString(mac.Sum(nil))
}

func (a *credentials) verify(signature string, fields ...string) bool {
	return hmac.Equal([]byte(signature), []byte(a.sign(fields...)))
}

// signRequest signs a handshake over purpose, fields, the time and a fresh
// nonce, which it returns, for the answer to be signed over.
func (a *credentials) signRequest(header http.Header, purpose string, fields ...string) string {
	nonce := rand.Text()
	if a.secret == "" {
		return nonce
	}

	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	header.Set(headerTime, timestamp)
	header.Set(headerNonce, nonce)
	header.Set(headerSignature, a.sign(append(append([]string{purpose}, fields...), timestamp, nonce)...))

	return nonce
}

// checkRequest authenticates a handshake signed by signRequest: a recent time
// and a nonce not seen before, so a handshake cannot be replayed. It returns
// the nonce.
func (a *credentials) checkRequest(header http.Header, purpose string, fields ...string) (string, error) {
	nonce := header.Get(headerNonce)
	if a.secret == "" {
		return nonce, nil
	}

	timestamp := header.Get(headerTime)
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return "", errors.New("missing or invalid time")
	}
	skew := time.Since(time.Unix(seconds, 0))
	if skew > handshakeMaxSkew || skew < -handshakeMaxSkew {
		return "", fmt.Errorf("handshake time off by %v", skew.Round(time.Second))
	}
	if len(nonce) < 16 {
		return "", errors.New("missing or short nonce")
	}
	if !a.verify(header.Get(headerSignature), append(append([]string{purpose}, fields...), timestamp, nonce)...) {
		return "", errors.New("invalid signature")
	}
	if !handshakeNonces.claim(nonce) {
		return "", errors.New("replayed handshake")
	}

	return nonce, nil
}

// signAnswer signs the answer to a handshake, over the nonce of its request,
// so the side that dialed knows the other side holds the secret too.
func (a *credentials) signAnswer(header http.Header, purpose, nonce string, fields ...string) {
	if a.secret != "" {
		header.Set(headerSignature, a.sign(append(append([]string{purpose + " answer"}, fields...), nonce)...))
	}
}

func (a *credentials) checkAnswer(signature, purpose, nonce string, fields ...string) bool {
	return a.secret == "" || a.verify(signature, append(append([]string{purpose + " answer"}, fields...), nonce)...)
}

// handshakeNonces are the handshake nonces seen within the allowed time skew,
// shared by the whole process, so a reload does not accept a replay.
var handshakeNonces = &nonceSet{seen: make(map[string]time.Time)}

type nonceSet struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

func (s *nonceSet) claim(nonce string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for seen, at := range s.seen {
		if now.Sub(at) > 2*handshakeMaxSkew {
			delete(s.seen, seen)
		}
	}
	if _, ok := s.seen[nonce]; ok {
		return false
	}
	s.seen[nonce] = now

	return true
}

// replaceKnown resolves placeholders such as {env.SECRET} in the fields.
func replaceKnown(fields ...*string) {
	repl := caddy.NewReplacer()
	for _, field := range fields {
		*field = repl.ReplaceKnown(*field, "")
	}
}

func (t *TLS) fields() []*string {
	if t == nil {
		return nil
	}

	return []*string{&t.CA, &t.Cert, &t.Key, &t.ServerName}
}

func validateAddresses(addresses ...string) error {
	for _, address := range addresses {
		if _, _, err := net.SplitHostPort(address); err != nil {
			return fmt.Errorf("address %q: %w", address, err)
		}
	}

	return nil
}

// unmarshalTLS parses:
//
//	tls {
//		ca <file>
//		cert <file>
//		key <file>
//		server_name <name>
//	}
func unmarshalTLS(d *caddyfile.Dispenser) (*TLS, error) {
	if d.NextArg() {
		return nil, d.ArgErr()
	}

	t := new(TLS)
	for nesting := d.Nesting(); d.NextBlock(nesting); {
		option := d.Val()
		field, ok := map[string]*string{"ca": &t.CA, "cert": &t.Cert, "key": &t.Key, "server_name": &t.ServerName}[option]
		if !ok {
			return nil, d.Errf("unrecognized tls option '%s'", option)
		}
		value, err := singleArg(d)
		if err != nil {
			return nil, err
		}
		*field = value
	}

	return t, nil
}

func singleArg(d *caddyfile.Dispenser) (string, error) {
	if !d.NextArg() {
		return "", d.ArgErr()
	}
	value := d.Val()
	if d.NextArg() {
		return "", d.ArgErr()
	}

	return value, nil
}
