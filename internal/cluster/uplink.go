package cluster

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"go.uber.org/zap"
)

// A member already registered leaves a newcomer this long to open the
// tunnel itself, so the two do not open one each.
const uplinkNewcomerGrace = busOpenWait

// Uplink makes this server a member that reaches the other members through
// relays. Every member using relays must be connected to all of them.
type Uplink struct {
	// Connect are the addresses (host:port) of the relays.
	Connect []string `json:"connect,omitempty"`
	// Secret authenticates this member to the relays and to the other members.
	Secret string `json:"secret,omitempty"`
	// TLS encrypts the connections to the relays and, with a CA, authenticates by certificate and encrypts every tunnel end to end.
	TLS *TLS `json:"tls,omitempty"`

	auth *credentials
}

func (u *Uplink) setup() error {
	replaceKnown(append([]*string{&u.Secret}, u.TLS.fields()...)...)
	for i := range u.Connect {
		replaceKnown(&u.Connect[i])
	}

	if len(u.Connect) == 0 {
		return errors.New("uplink: connect needs the address of at least one relay")
	}
	if err := validateAddresses(u.Connect...); err != nil {
		return fmt.Errorf("uplink: %w", err)
	}

	auth, err := newCredentials(u.Secret, u.TLS, false)
	if err != nil {
		return fmt.Errorf("uplink: %w", err)
	}
	if !auth.authenticates() {
		return errors.New("uplink: set a secret, or tls with a ca, so only your members can connect")
	}
	u.auth = auth

	return nil
}

// uplinkLink is the uplink of one bus: its control connections to the
// relays, what they report, and the tunnels it opens.
type uplinkLink struct {
	uplink *Uplink
	b      *Bus
	relays []*uplinkRelay
	wake   chan struct{}

	mu        sync.Mutex
	closed    bool
	controls  map[net.Conn]struct{}
	initial   map[string]bool
	firstSeen map[string]time.Time
	opening   map[string]bool
	retry     map[string]time.Time
	backoff   map[string]time.Duration
}

// uplinkRelay is one relay and the members registered with it.
type uplinkRelay struct {
	address    string
	serverName string

	reported     chan struct{}
	reportedOnce sync.Once

	mu        sync.Mutex
	directory map[string]bool
}

func (r *uplinkRelay) markReported() {
	r.reportedOnce.Do(func() { close(r.reported) })
}

func (r *uplinkRelay) has(instance string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.directory[instance]
}

func (l *uplinkLink) open(b *Bus) error {
	l.b = b
	l.wake = make(chan struct{}, 1)
	l.controls = make(map[net.Conn]struct{})
	l.initial = make(map[string]bool)
	l.firstSeen = make(map[string]time.Time)
	l.opening = make(map[string]bool)
	l.retry = make(map[string]time.Time)
	l.backoff = make(map[string]time.Duration)

	for _, address := range l.uplink.Connect {
		host, _, _ := net.SplitHostPort(address)
		r := &uplinkRelay{address: address, serverName: host, reported: make(chan struct{}), directory: make(map[string]bool)}
		l.relays = append(l.relays, r)
		b.wg.Go(func() { l.controlLoop(r) })
	}
	b.wg.Go(l.tunnelLoop)

	l.awaitInitialTunnels()

	return nil
}

func (l *uplinkLink) close() {
	l.mu.Lock()
	l.closed = true
	controls := make([]net.Conn, 0, len(l.controls))
	for conn := range l.controls {
		controls = append(controls, conn)
	}
	l.mu.Unlock()

	for _, conn := range controls {
		_ = conn.Close()
	}
}

// awaitInitialTunnels waits for the relays to report their members and for
// the tunnels to those members, so this member is reachable once open.
func (l *uplinkLink) awaitInitialTunnels() {
	deadline := time.NewTimer(busOpenWait)
	defer deadline.Stop()

	for _, r := range l.relays {
		select {
		case <-r.reported:
		case <-deadline.C:
			return
		}
	}

	for {
		l.mu.Lock()
		waiting := false
		for instance := range l.initial {
			if !l.b.connected(instance) {
				waiting = true

				break
			}
		}
		l.mu.Unlock()

		if !waiting {
			return
		}
		select {
		case <-deadline.C:
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (l *uplinkLink) notify() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

func (l *uplinkLink) controlLoop(r *uplinkRelay) {
	defer r.markReported()

	backoff := busRedialMin
	for {
		conn, err := l.dialRelay(r, relayControlPath, nil, "relay control", l.b.instance)
		if err != nil {
			r.markReported()
			l.b.logger.Debug("uplink: connecting to a relay failed", zap.String("relay", r.address), zap.Error(err))
			if !l.b.pause(backoff, nil) {
				return
			}
			backoff = min(2*backoff, busRedialMax)

			continue
		}
		backoff = busRedialMin

		if !l.track(conn) {
			_ = conn.Close()

			return
		}
		l.control(r, conn)
		l.untrack(conn)

		r.mu.Lock()
		r.directory = make(map[string]bool)
		r.mu.Unlock()
		l.notify()

		if !l.b.pause(busRedialMin, nil) {
			return
		}
	}
}

func (l *uplinkLink) track(conn net.Conn) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return false
	}
	l.controls[conn] = struct{}{}

	return true
}

func (l *uplinkLink) untrack(conn net.Conn) {
	_ = conn.Close()

	l.mu.Lock()
	delete(l.controls, conn)
	l.mu.Unlock()
}

// control reads what the relay reports until the connection ends, and
// keeps it alive with heartbeats.
func (l *uplinkLink) control(r *uplinkRelay, conn net.Conn) {
	done := make(chan struct{})
	defer close(done)

	l.b.wg.Go(func() {
		heartbeat := time.NewTicker(busHeartbeatInterval)
		defer heartbeat.Stop()

		for {
			select {
			case <-done:
				return
			case <-heartbeat.C:
				err := conn.SetWriteDeadline(time.Now().Add(busWriteTimeout))
				if err == nil {
					err = writeControlFrame(conn, nil)
				}
				if err != nil {
					_ = conn.Close()

					return
				}
			}
		}
	})

	first := true
	for {
		if err := conn.SetReadDeadline(time.Now().Add(busReadTimeout)); err != nil {
			return
		}
		payload, err := readControlFrame(conn)
		if err != nil {
			return
		}
		if len(payload) == 0 {
			continue
		}

		var message relayMessage
		if err := json.Unmarshal(payload, &message); err != nil {
			l.b.logger.Debug("uplink: malformed message from a relay", zap.String("relay", r.address), zap.Error(err))

			return
		}

		switch message.Type {
		case relayMessageDirectory:
			l.learn(r, message.Directory, first)
			first = false
			r.markReported()
		case relayMessageTunnel:
			l.b.wg.Go(func() { l.acceptTunnel(r, message.Tunnel, message.From) })
		}
	}
}

// learn takes the members a relay reports. Those in the first report of a
// relay are there before this member, which opens the tunnels to them.
func (l *uplinkLink) learn(r *uplinkRelay, members []string, first bool) {
	directory := make(map[string]bool, len(members))
	for _, instance := range members {
		if instance != l.b.instance {
			directory[instance] = true
		}
	}

	r.mu.Lock()
	r.directory = directory
	r.mu.Unlock()

	l.mu.Lock()
	now := time.Now()
	for instance := range directory {
		if _, seen := l.firstSeen[instance]; !seen {
			l.firstSeen[instance] = now
			if first {
				l.initial[instance] = true
			}
		}
	}
	l.mu.Unlock()

	l.notify()
}

func (l *uplinkLink) tunnelLoop() {
	ticker := time.NewTicker(busRedialCheck)
	defer ticker.Stop()

	for {
		select {
		case <-l.b.ctx.Done():
			return
		case <-ticker.C:
		case <-l.wake:
		}
		l.openTunnels()
	}
}

// openTunnels opens a tunnel to every member a relay reports that this one
// has no connection to.
func (l *uplinkLink) openTunnels() {
	known := make(map[string]bool)
	for _, r := range l.relays {
		r.mu.Lock()
		for instance := range r.directory {
			known[instance] = true
		}
		r.mu.Unlock()
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return
	}

	now := time.Now()
	for instance := range l.firstSeen {
		if !known[instance] {
			delete(l.firstSeen, instance)
			delete(l.initial, instance)
			delete(l.retry, instance)
			delete(l.backoff, instance)
		}
	}

	for instance := range known {
		if l.opening[instance] || now.Before(l.retry[instance]) || l.b.connected(instance) {
			continue
		}
		if !l.initial[instance] && now.Sub(l.firstSeen[instance]) < uplinkNewcomerGrace {
			continue
		}

		l.opening[instance] = true
		l.b.wg.Go(func() {
			err := l.openTunnel(instance)

			l.mu.Lock()
			defer l.mu.Unlock()
			delete(l.opening, instance)
			if err != nil {
				l.b.logger.Debug("uplink: opening a tunnel failed", zap.String("instance", instance), zap.Error(err))
				l.backoff[instance] = min(2*max(l.backoff[instance], busRedialMin/2), busRedialMax)
				l.retry[instance] = time.Now().Add(l.backoff[instance])
			} else {
				delete(l.backoff, instance)
				delete(l.retry, instance)
			}
		})
	}
}

func (l *uplinkLink) relayFor(instance string) *uplinkRelay {
	for _, r := range l.relays {
		if r.has(instance) {
			return r
		}
	}

	return nil
}

// openTunnel joins this member to instance through a relay both are
// connected to, and runs the member handshake through it as the side that dialed.
func (l *uplinkLink) openTunnel(instance string) error {
	r := l.relayFor(instance)
	if r == nil {
		return errors.New("no relay reports the member")
	}

	header := http.Header{}
	header.Set(headerTunnelTo, instance)
	conn, err := l.dialRelay(r, relayTunnelPath, header, "relay tunnel", l.b.instance, instance, "")
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(l.b.ctx, handshakeTimeout)
	defer cancel()

	inner, err := l.uplink.auth.tunnelTLS(ctx, conn, true)
	if err != nil {
		_ = conn.Close()

		return err
	}
	peer, err := l.b.dialMember(ctx, inner, l.uplink.auth, connTunnel)
	if err != nil {
		return err
	}
	if peer != instance {
		return fmt.Errorf("the tunnel to %s reached %s", instance, peer)
	}

	return nil
}

// acceptTunnel takes a tunnel another member opened, and runs the member
// handshake through it as the side that was dialed.
func (l *uplinkLink) acceptTunnel(r *uplinkRelay, id, from string) {
	header := http.Header{}
	header.Set(headerTunnelID, id)
	conn, err := l.dialRelay(r, relayTunnelPath, header, "relay tunnel", l.b.instance, "", id)
	if err != nil {
		l.b.logger.Debug("uplink: accepting a tunnel failed", zap.String("instance", from), zap.Error(err))

		return
	}

	inner := conn
	if l.uplink.auth.endToEnd() {
		ctx, cancel := context.WithTimeout(l.b.ctx, handshakeTimeout)
		inner, err = l.uplink.auth.tunnelTLS(ctx, conn, false)
		cancel()
		if err != nil {
			_ = conn.Close()
			l.b.logger.Debug("uplink: tls in a tunnel failed", zap.String("instance", from), zap.Error(err))

			return
		}
	}
	l.b.serveMember(inner, l.uplink.auth, connTunnel)
}

// dialRelay connects to a relay with a handshake signed over purpose and
// fields, and returns the connection once the relay answers.
func (l *uplinkLink) dialRelay(r *uplinkRelay, path string, header http.Header, purpose string, fields ...string) (net.Conn, error) {
	// Opening a tunnel waits for the other member to accept it.
	ctx, cancel := context.WithTimeout(l.b.ctx, 2*handshakeTimeout)
	defer cancel()

	raw, err := new(net.Dialer).DialContext(ctx, "tcp", r.address)
	if err != nil {
		return nil, err
	}
	conn, err := l.uplink.auth.dialTLS(ctx, raw, r.serverName)
	if err != nil {
		_ = raw.Close()

		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	if header == nil {
		header = http.Header{}
	}
	header.Set(headerInstance, l.b.instance)
	nonce := l.uplink.auth.signRequest(header, purpose, fields...)

	if err := writeUpgradeRequest(conn, path, header); err != nil {
		_ = conn.Close()

		return nil, err
	}

	br := bufio.NewReader(conn)
	response, err := http.ReadResponse(br, nil)
	if err != nil {
		_ = conn.Close()

		return nil, err
	}
	_ = response.Body.Close()

	if response.StatusCode != http.StatusSwitchingProtocols {
		_ = conn.Close()

		return nil, fmt.Errorf("the relay answered %s", response.Status)
	}
	if !l.uplink.auth.checkAnswer(response.Header.Get(headerSignature), purpose, nonce) {
		_ = conn.Close()
		l.b.logger.Warn("uplink: a relay failed to authenticate", zap.String("relay", r.address))

		return nil, errors.New("the relay failed to authenticate")
	}
	if !stop() {
		return nil, ctx.Err()
	}

	return &bufferedConn{Conn: conn, r: br}, nil
}

// UnmarshalCaddyfile parses:
//
//	uplink {
//		connect <address>...
//		secret <secret>
//		tls { … }
//	}
func (u *Uplink) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next()
	if d.NextArg() {
		return d.ArgErr()
	}

	for d.NextBlock(0) {
		switch option := d.Val(); option {
		case "connect":
			values := d.RemainingArgs()
			if len(values) == 0 {
				return d.ArgErr()
			}
			u.Connect = append(u.Connect, values...)
		case "secret":
			value, err := singleArg(d)
			if err != nil {
				return err
			}
			u.Secret = value
		case "tls":
			t, err := unmarshalTLS(d)
			if err != nil {
				return err
			}
			u.TLS = t
		default:
			return d.Errf("unrecognized uplink option '%s'", option)
		}
	}

	return nil
}
