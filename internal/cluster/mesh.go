package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"go.uber.org/zap"
)

const defaultMeshResolveInterval = 5 * time.Second

// Mesh makes this server a member that connects directly to the other
// members: those at its peers, those its DNS names resolve to, and those
// dialing its listener.
type Mesh struct {
	// Listen is the address to accept other members on, such as ":7070". Without it, the member only dials.
	Listen string `json:"listen,omitempty"`
	// Peers are the addresses (host:port) of other members to dial.
	Peers []string `json:"peers,omitempty"`
	// DNS names (host:port) resolve to the addresses of other members, re-resolved every ResolveInterval.
	DNS []string `json:"dns,omitempty"`
	// ResolveInterval is how often the DNS names are resolved again. Default: 5s.
	ResolveInterval caddy.Duration `json:"resolve_interval,omitempty"`
	// Secret authenticates the members to each other; all of them share it.
	Secret string `json:"secret,omitempty"`
	// TLS encrypts the connections and, with a CA, authenticates the members by their certificates.
	TLS *TLS `json:"tls,omitempty"`

	auth            *credentials
	resolveInterval time.Duration
	lookupHost      func(ctx context.Context, host string) ([]string, error)
}

func (m *Mesh) setup() error {
	replaceKnown(append([]*string{&m.Listen, &m.Secret}, m.TLS.fields()...)...)
	for i := range m.Peers {
		replaceKnown(&m.Peers[i])
	}
	for i := range m.DNS {
		replaceKnown(&m.DNS[i])
	}

	if m.Listen == "" && len(m.Peers) == 0 && len(m.DNS) == 0 {
		return errors.New("cluster mesh: set listen, peers or dns")
	}
	if err := validateAddresses(append(m.Peers, m.DNS...)...); err != nil {
		return fmt.Errorf("cluster mesh: %w", err)
	}
	if m.ResolveInterval < 0 {
		return fmt.Errorf("cluster mesh: invalid resolve_interval %v", time.Duration(m.ResolveInterval))
	}

	auth, err := newCredentials(m.Secret, m.TLS, m.Listen != "")
	if err != nil {
		return fmt.Errorf("cluster mesh: %w", err)
	}
	if !auth.authenticates() {
		return errors.New("cluster mesh: set a secret, or tls with a ca, so only your members can join")
	}
	m.auth = auth

	m.resolveInterval = orDefault(time.Duration(m.ResolveInterval), defaultMeshResolveInterval)
	if m.lookupHost == nil {
		m.lookupHost = net.DefaultResolver.LookupHost
	}

	return nil
}

// meshLink is the mesh of one bus: its listener and the addresses it dials.
type meshLink struct {
	mesh *Mesh
	b    *Bus

	listener *sharedListener

	mu      sync.Mutex
	closed  bool
	targets map[string]*meshTarget
}

func (l *meshLink) open(b *Bus) error {
	l.b = b
	l.targets = make(map[string]*meshTarget)

	if l.mesh.Listen != "" {
		listener, err := acquireListener("mesh", l.mesh.Listen, l, b.logger)
		if err != nil {
			return fmt.Errorf("cluster mesh: %w", err)
		}
		l.listener = listener
	}

	for _, address := range l.mesh.Peers {
		host, _, _ := net.SplitHostPort(address)
		l.addTarget(address, host, "")
	}
	if len(l.mesh.DNS) > 0 {
		l.resolve()
		b.wg.Go(l.resolveLoop)
	}

	// The members reachable now are connected before this one can be addressed.
	l.awaitFirstDials()

	return nil
}

func (l *meshLink) close() {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()

	if l.listener != nil {
		l.listener.release(l)
	}
}

// addr is the address the mesh accepts members on, or "" without a listener.
func (l *meshLink) addr() string {
	if l.listener == nil {
		return ""
	}

	return l.listener.addr()
}

func (l *meshLink) accepted(raw net.Conn) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		_ = raw.Close()

		return
	}

	conn := l.mesh.auth.serverConn(raw)
	l.b.wg.Go(func() { l.b.serveMember(conn, l.mesh.auth, connDirect) })
}

func (l *meshLink) awaitFirstDials() {
	l.mu.Lock()
	targets := make([]*meshTarget, 0, len(l.targets))
	for _, t := range l.targets {
		targets = append(targets, t)
	}
	l.mu.Unlock()

	timer := time.NewTimer(busOpenWait)
	defer timer.Stop()

	for _, t := range targets {
		select {
		case <-t.attempted:
		case <-timer.C:
			return
		}
	}
}

// meshTarget is an address dialed for as long as it is configured or
// resolved; it remembers the member it reached, to dial again only when
// that member has no connection.
type meshTarget struct {
	address    string
	serverName string
	dnsName    string

	attempted     chan struct{}
	attemptedOnce sync.Once
	done          chan struct{}

	mu       sync.Mutex
	instance string
	self     bool
}

func (t *meshTarget) markAttempted() {
	t.attemptedOnce.Do(func() { close(t.attempted) })
}

func (t *meshTarget) reached() string {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.instance
}

func (l *meshLink) addTarget(address, serverName, dnsName string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return
	}
	if _, ok := l.targets[address]; ok {
		return
	}

	t := &meshTarget{address: address, serverName: serverName, dnsName: dnsName, attempted: make(chan struct{}), done: make(chan struct{})}
	l.targets[address] = t
	l.b.wg.Go(func() { l.dialLoop(t) })
}

func (l *meshLink) resolveLoop() {
	ticker := time.NewTicker(l.mesh.resolveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-l.b.ctx.Done():
			return
		case <-ticker.C:
			l.resolve()
		}
	}
}

// resolve dials the addresses the DNS names resolve to and stops dialing
// those they no longer do; the connections already up stay.
func (l *meshLink) resolve() {
	for _, name := range l.mesh.DNS {
		host, port, _ := net.SplitHostPort(name)

		ctx, cancel := context.WithTimeout(l.b.ctx, handshakeTimeout)
		addresses, err := l.mesh.lookupHost(ctx, host)
		cancel()
		if err != nil {
			l.b.logger.Warn("cluster mesh: resolving members failed", zap.String("dns", name), zap.Error(err))

			continue
		}

		resolved := make(map[string]bool, len(addresses))
		for _, address := range addresses {
			address = net.JoinHostPort(address, port)
			resolved[address] = true
			l.addTarget(address, host, name)
		}

		l.mu.Lock()
		for address, t := range l.targets {
			if t.dnsName == name && !resolved[address] {
				delete(l.targets, address)
				close(t.done)
			}
		}
		l.mu.Unlock()
	}
}

func (l *meshLink) dialLoop(t *meshTarget) {
	defer t.markAttempted()

	backoff := busRedialMin
	for {
		if instance := t.reached(); instance == "" || !l.b.connected(instance) {
			instance, err := l.dial(t)
			t.markAttempted()

			switch {
			case errors.Is(err, errBusSelf):
				t.mu.Lock()
				t.self = true
				t.mu.Unlock()

				return
			case err != nil:
				l.b.logger.Debug("cluster mesh: dialing a member failed", zap.String("address", t.address), zap.Error(err))
				if !l.b.pause(backoff, t.done) {
					return
				}
				backoff = min(2*backoff, busRedialMax)

				continue
			}

			t.mu.Lock()
			t.instance = instance
			t.mu.Unlock()
			backoff = busRedialMin
		}

		if !l.b.pause(busRedialCheck, t.done) {
			return
		}
	}
}

func (l *meshLink) dial(t *meshTarget) (string, error) {
	ctx, cancel := context.WithTimeout(l.b.ctx, handshakeTimeout)
	defer cancel()

	raw, err := new(net.Dialer).DialContext(ctx, "tcp", t.address)
	if err != nil {
		return "", err
	}
	conn, err := l.mesh.auth.dialTLS(ctx, raw, t.serverName)
	if err != nil {
		_ = raw.Close()

		return "", err
	}

	return l.b.dialMember(ctx, conn, l.mesh.auth, connDirect)
}

// UnmarshalCaddyfile parses the block of:
//
//	cluster mesh {
//		listen <address>
//		peers <address>...
//		dns <name:port>...
//		resolve_interval <duration>
//		secret <secret>
//		tls { … }
//	}
func (m *Mesh) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for nesting := d.Nesting(); d.NextBlock(nesting); {
		switch option := d.Val(); option {
		case "listen", "secret":
			value, err := singleArg(d)
			if err != nil {
				return err
			}
			*map[string]*string{"listen": &m.Listen, "secret": &m.Secret}[option] = value
		case "peers", "dns":
			values := d.RemainingArgs()
			if len(values) == 0 {
				return d.ArgErr()
			}
			if option == "peers" {
				m.Peers = append(m.Peers, values...)
			} else {
				m.DNS = append(m.DNS, values...)
			}
		case "resolve_interval":
			value, err := singleArg(d)
			if err != nil {
				return err
			}
			interval, err := caddy.ParseDuration(value)
			if err != nil || interval <= 0 {
				return d.Errf("invalid resolve_interval '%s'", value)
			}
			m.ResolveInterval = caddy.Duration(interval)
		case "tls":
			t, err := unmarshalTLS(d)
			if err != nil {
				return err
			}
			m.TLS = t
		default:
			return d.Errf("unrecognized cluster mesh option '%s'", option)
		}
	}

	return nil
}
