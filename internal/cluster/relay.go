package cluster

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"go.uber.org/zap"
)

const (
	relayProtocol        = "frankenphp-cluster"
	relayControlPath     = "/control"
	relayTunnelPath      = "/tunnel"
	relayControlQueue    = 256
	maxRelayControlFrame = 1 << 20
)

// Relay makes this server a relay: members that cannot reach each other
// directly connect to it, and it joins them with tunnels. It only passes
// the bytes of a tunnel through; it is not a member itself.
type Relay struct {
	// Listen is the address to accept members on, such as ":7070".
	Listen string `json:"listen,omitempty"`
	// Secret authenticates the members; they share it with the relay.
	Secret string `json:"secret,omitempty"`
	// TLS encrypts the connections and, with a CA, authenticates the members by their certificates.
	TLS *TLS `json:"tls,omitempty"`

	auth *credentials
}

func (r *Relay) setup() error {
	replaceKnown(append([]*string{&r.Listen, &r.Secret}, r.TLS.fields()...)...)

	if r.Listen == "" {
		return errors.New("cluster relay: listen is required")
	}

	auth, err := newCredentials(r.Secret, r.TLS, true)
	if err != nil {
		return fmt.Errorf("cluster relay: %w", err)
	}
	if !auth.authenticates() {
		return errors.New("cluster relay: set a secret, or tls with a ca, so only your members can connect")
	}
	r.auth = auth

	return nil
}

// relayMessage goes from the relay to a member over its control connection.
type relayMessage struct {
	Type      string   `json:"type"`
	Directory []string `json:"directory,omitempty"`
	Tunnel    string   `json:"tunnel,omitempty"`
	From      string   `json:"from,omitempty"`
}

const (
	relayMessageDirectory = "directory"
	relayMessageTunnel    = "tunnel"
)

// RelayServer is a running relay: the members registered with it and the
// tunnels waiting for the member they were opened to.
type RelayServer struct {
	relay    *Relay
	logger   *zap.Logger
	listener *sharedListener
	ctx      context.Context
	cancel   context.CancelFunc

	mu      sync.Mutex
	closed  bool
	members map[string]*relayMember
	tunnels map[string]*relayTunnel
	conns   map[net.Conn]struct{}

	wg sync.WaitGroup
}

// StartRelay runs a relay.
func StartRelay(r *Relay, logger *zap.Logger) (*RelayServer, error) {
	if err := r.setup(); err != nil {
		return nil, err
	}

	return startRelay(r, logger)
}

func startRelay(r *Relay, logger *zap.Logger) (*RelayServer, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	s := &RelayServer{
		relay:   r,
		logger:  logger,
		members: make(map[string]*relayMember),
		tunnels: make(map[string]*relayTunnel),
		conns:   make(map[net.Conn]struct{}),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())

	listener, err := acquireListener("relay", r.Listen, s, logger)
	if err != nil {
		s.cancel()

		return nil, fmt.Errorf("cluster relay: %w", err)
	}
	s.listener = listener

	return s, nil
}

// Addr is the address the relay accepts members on.
func (s *RelayServer) Addr() string {
	return s.listener.addr()
}

// Stop closes the relay with its tunnels.
func (s *RelayServer) Stop() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()

		return
	}
	s.closed = true
	members := make([]*relayMember, 0, len(s.members))
	for _, m := range s.members {
		members = append(members, m)
	}
	conns := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.mu.Unlock()

	s.listener.release(s)
	s.cancel()
	for _, m := range members {
		m.close()
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
	s.wg.Wait()
}

func (s *RelayServer) accepted(raw net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		_ = raw.Close()

		return
	}
	s.conns[raw] = struct{}{}
	s.wg.Go(func() { s.serve(raw) })
}

func (s *RelayServer) forget(raw net.Conn) {
	_ = raw.Close()

	s.mu.Lock()
	delete(s.conns, raw)
	s.mu.Unlock()
}

func (s *RelayServer) serve(raw net.Conn) {
	_ = raw.SetDeadline(time.Now().Add(handshakeTimeout))

	tlsConn := s.relay.auth.serverConn(raw)
	br := bufio.NewReader(tlsConn)
	request, err := http.ReadRequest(br)
	if err != nil {
		s.logger.Debug("cluster relay: handshake failed", zap.String("remote", raw.RemoteAddr().String()), zap.Error(err))
		s.forget(raw)

		return
	}
	_ = request.Body.Close()
	conn := &bufferedConn{Conn: tlsConn, r: br}

	instance := request.Header.Get(headerInstance)
	to := request.Header.Get(headerTunnelTo)
	id := request.Header.Get(headerTunnelID)

	refuse := func(status int) {
		_ = writeUpgradeResponse(conn, status, nil)
		s.forget(raw)
	}

	if instance == "" {
		refuse(http.StatusBadRequest)

		return
	}

	var purpose string
	var fields []string
	switch request.URL.Path {
	case relayControlPath:
		purpose, fields = "relay control", []string{instance}
	case relayTunnelPath:
		if (to == "") == (id == "") {
			refuse(http.StatusBadRequest)

			return
		}
		purpose, fields = "relay tunnel", []string{instance, to, id}
	default:
		refuse(http.StatusNotFound)

		return
	}

	nonce, err := s.relay.auth.checkRequest(request.Header, purpose, fields...)
	if err != nil {
		s.logger.Warn("cluster relay: refused a member", zap.String("remote", raw.RemoteAddr().String()), zap.Error(err))
		refuse(http.StatusUnauthorized)

		return
	}

	switch {
	case request.URL.Path == relayControlPath:
		s.register(instance, raw, conn, nonce)
	case to != "":
		s.openTunnel(instance, to, raw, conn, nonce)
	default:
		s.acceptTunnel(instance, id, raw, conn, nonce)
	}
}

// relayMember is a member registered over its control connection; the
// messages for it are written by their own goroutine.
type relayMember struct {
	id   string
	conn net.Conn
	send chan []byte

	done      chan struct{}
	closeOnce sync.Once
}

func (m *relayMember) close() {
	m.closeOnce.Do(func() {
		_ = m.conn.Close()
		close(m.done)
	})
}

func (s *RelayServer) register(id string, raw net.Conn, conn net.Conn, nonce string) {
	defer s.forget(raw)

	m := &relayMember{id: id, conn: conn, send: make(chan []byte, relayControlQueue), done: make(chan struct{})}
	defer m.close()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()

		return
	}
	previous := s.members[id]
	s.members[id] = m
	s.mu.Unlock()

	if previous != nil {
		previous.close()
	}
	defer s.unregister(m)

	header := http.Header{}
	s.relay.auth.signAnswer(header, "relay control", nonce)
	if err := writeUpgradeResponse(conn, http.StatusSwitchingProtocols, header); err != nil {
		return
	}
	_ = raw.SetDeadline(time.Time{})

	s.wg.Go(func() { m.write(s.logger) })
	s.announceDirectory()

	for {
		if err := conn.SetReadDeadline(time.Now().Add(busReadTimeout)); err != nil {
			return
		}
		if _, err := readControlFrame(conn); err != nil {
			return
		}
	}
}

func (s *RelayServer) unregister(m *relayMember) {
	s.mu.Lock()
	removed := s.members[m.id] == m
	if removed {
		delete(s.members, m.id)
	}
	s.mu.Unlock()

	if removed {
		s.announceDirectory()
	}
}

func (m *relayMember) write(logger *zap.Logger) {
	heartbeat := time.NewTicker(busHeartbeatInterval)
	defer heartbeat.Stop()

	for {
		var frame []byte
		select {
		case <-m.done:
			return
		case frame = <-m.send:
		case <-heartbeat.C:
		}

		err := m.conn.SetWriteDeadline(time.Now().Add(busWriteTimeout))
		if err == nil {
			err = writeControlFrame(m.conn, frame)
		}
		if err != nil {
			logger.Debug("cluster relay: writing to a member failed", zap.String("instance", m.id), zap.Error(err))
			m.close()

			return
		}
	}
}

// tell queues a message for m; a member that does not keep up with its
// control messages is disconnected, to register again.
func (m *relayMember) tell(message relayMessage) bool {
	payload, err := json.Marshal(message)
	if err != nil {
		return false
	}

	select {
	case m.send <- payload:
		return true
	default:
		go m.close()

		return false
	}
}

func (s *RelayServer) announceDirectory() {
	s.mu.Lock()
	defer s.mu.Unlock()

	directory := make([]string, 0, len(s.members))
	for id := range s.members {
		directory = append(directory, id)
	}
	slices.Sort(directory)

	for _, m := range s.members {
		m.tell(relayMessage{Type: relayMessageDirectory, Directory: directory})
	}
}

// relayTunnel is a tunnel opened by one member, waiting for the member it
// was opened to.
type relayTunnel struct {
	id    string
	from  string
	to    string
	raw   net.Conn
	conn  net.Conn
	nonce string

	matched chan struct{}
}

func (s *RelayServer) openTunnel(from, to string, raw net.Conn, conn net.Conn, nonce string) {
	t := &relayTunnel{id: rand.Text(), from: from, to: to, raw: raw, conn: conn, nonce: nonce, matched: make(chan struct{})}

	s.mu.Lock()
	target := s.members[to]
	if s.closed || target == nil || to == from {
		s.mu.Unlock()
		_ = writeUpgradeResponse(conn, http.StatusNotFound, nil)
		s.forget(raw)

		return
	}
	s.tunnels[t.id] = t
	told := target.tell(relayMessage{Type: relayMessageTunnel, Tunnel: t.id, From: from})
	if !told {
		delete(s.tunnels, t.id)
	}
	s.mu.Unlock()

	if !told {
		_ = writeUpgradeResponse(conn, http.StatusServiceUnavailable, nil)
		s.forget(raw)

		return
	}

	timer := time.NewTimer(handshakeTimeout)
	defer timer.Stop()

	select {
	case <-t.matched:
		return
	case <-timer.C:
	case <-s.ctx.Done():
	}

	s.mu.Lock()
	pending := s.tunnels[t.id] == t
	if pending {
		delete(s.tunnels, t.id)
	}
	s.mu.Unlock()

	if !pending {
		// The member accepted it meanwhile, and the tunnel is its now.
		<-t.matched

		return
	}
	_ = raw.SetDeadline(time.Now().Add(busWriteTimeout))
	_ = writeUpgradeResponse(conn, http.StatusGatewayTimeout, nil)
	s.forget(raw)
}

func (s *RelayServer) acceptTunnel(instance, id string, raw net.Conn, conn net.Conn, nonce string) {
	s.mu.Lock()
	t := s.tunnels[id]
	if t == nil || t.to != instance {
		s.mu.Unlock()
		_ = writeUpgradeResponse(conn, http.StatusNotFound, nil)
		s.forget(raw)

		return
	}
	delete(s.tunnels, id)
	s.mu.Unlock()
	close(t.matched)

	defer s.forget(t.raw)
	defer s.forget(raw)

	for _, side := range []struct {
		raw   net.Conn
		conn  net.Conn
		nonce string
	}{{t.raw, t.conn, t.nonce}, {raw, conn, nonce}} {
		header := http.Header{}
		s.relay.auth.signAnswer(header, "relay tunnel", side.nonce)
		_ = side.raw.SetDeadline(time.Now().Add(busWriteTimeout))
		if err := writeUpgradeResponse(side.conn, http.StatusSwitchingProtocols, header); err != nil {
			return
		}
	}
	_ = t.raw.SetDeadline(time.Time{})
	_ = raw.SetDeadline(time.Time{})

	splice(t.conn, conn)
}

// splice copies between a and b until either side ends, then closes both.
func splice(a, b net.Conn) {
	var wg sync.WaitGroup
	wg.Go(func() {
		_, _ = io.Copy(b, a)
		_ = a.Close()
		_ = b.Close()
	})
	wg.Go(func() {
		_, _ = io.Copy(a, b)
		_ = a.Close()
		_ = b.Close()
	})
	wg.Wait()
}

func writeUpgradeRequest(w io.Writer, path string, header http.Header) error {
	bw := bufio.NewWriter(w)
	_, _ = fmt.Fprintf(bw, "GET %s HTTP/1.1\r\nHost: cluster\r\nConnection: Upgrade\r\nUpgrade: %s\r\n", path, relayProtocol)
	_ = header.Write(bw)
	_, _ = bw.WriteString("\r\n")

	return bw.Flush()
}

func writeUpgradeResponse(w io.Writer, status int, header http.Header) error {
	bw := bufio.NewWriter(w)
	_, _ = fmt.Fprintf(bw, "HTTP/1.1 %d %s\r\n", status, http.StatusText(status))
	if status == http.StatusSwitchingProtocols {
		_, _ = fmt.Fprintf(bw, "Connection: Upgrade\r\nUpgrade: %s\r\n", relayProtocol)
	} else {
		_, _ = bw.WriteString("Connection: close\r\nContent-Length: 0\r\n")
	}
	_ = header.Write(bw)
	_, _ = bw.WriteString("\r\n")

	return bw.Flush()
}

// writeControlFrame writes a length-prefixed message; an empty one is a heartbeat.
func writeControlFrame(w io.Writer, payload []byte) error {
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame, uint32(len(payload)))
	copy(frame[4:], payload)
	_, err := w.Write(frame)

	return err
}

func readControlFrame(r io.Reader) ([]byte, error) {
	var size [4]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(size[:])
	if length > maxRelayControlFrame {
		return nil, fmt.Errorf("control message of %d bytes exceeds %d", length, maxRelayControlFrame)
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	return payload, nil
}

// UnmarshalCaddyfile parses the block of:
//
//	cluster relay {
//		listen <address>
//		secret <secret>
//		tls { … }
//	}
func (r *Relay) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for nesting := d.Nesting(); d.NextBlock(nesting); {
		switch option := d.Val(); option {
		case "listen", "secret":
			value, err := singleArg(d)
			if err != nil {
				return err
			}
			*map[string]*string{"listen": &r.Listen, "secret": &r.Secret}[option] = value
		case "tls":
			t, err := unmarshalTLS(d)
			if err != nil {
				return err
			}
			r.TLS = t
		default:
			return d.Errf("unrecognized cluster relay option '%s'", option)
		}
	}

	return nil
}
