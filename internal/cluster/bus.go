package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/gobwas/ws"
	"go.uber.org/zap"
)

// DefaultQueueSize is how many messages may wait for one other member, unless configured.
const DefaultQueueSize = 1024

const (
	maxBusPayload        = 64 << 20
	busOpenWait          = 2 * time.Second
	busWriteTimeout      = 5 * time.Second
	busCloseFrameTimeout = time.Second
	busHeartbeatInterval = 5 * time.Second
	busReadTimeout       = 3 * busHeartbeatInterval
	// A peer without a connection keeps its queue this long, so a reconnect loses nothing.
	busPeerLinger  = 10 * time.Second
	busRedialMin   = 100 * time.Millisecond
	busRedialMax   = 5 * time.Second
	busRedialCheck = 250 * time.Millisecond
	busWriteBatch  = 64
)

var errBusSelf = errors.New("cluster: dialed itself")

type connKind int

const (
	connDirect connKind = iota
	connTunnel
)

// Bus connects a member to the other members of the cluster: one connection
// and one queue per other member, directly through its mesh or through a
// relay of its uplink. It carries opaque payloads; delivery is at most once.
type Bus struct {
	logger    *zap.Logger
	queueSize int
	linger    time.Duration
	mesh      *meshLink
	uplink    *uplinkLink

	instance string
	deliver  func(peer string, payload []byte)
	ctx      context.Context
	cancel   context.CancelFunc
	closing  chan struct{}

	mu     sync.Mutex
	opened bool
	closed bool
	peers  map[string]*busPeer
	conns  map[*busConn]struct{}

	wg      sync.WaitGroup
	writers sync.WaitGroup
}

// NewBus is the bus of a member with a mesh, an uplink or both.
func NewBus(logger *zap.Logger, queueSize int, mesh *Mesh, uplink *Uplink) (*Bus, error) {
	if mesh == nil && uplink == nil {
		return nil, errors.New("cluster: a member needs a mesh or an uplink")
	}
	if mesh != nil {
		if err := mesh.setup(); err != nil {
			return nil, err
		}
	}
	if uplink != nil {
		if err := uplink.setup(); err != nil {
			return nil, err
		}
	}

	return newBus(logger, queueSize, mesh, uplink), nil
}

// newBus takes a mesh and an uplink already set up; either may be nil.
func newBus(logger *zap.Logger, queueSize int, mesh *Mesh, uplink *Uplink) *Bus {
	if logger == nil {
		logger = zap.NewNop()
	}

	b := &Bus{logger: logger, queueSize: orDefault(queueSize, DefaultQueueSize), linger: busPeerLinger}
	if mesh != nil {
		b.mesh = &meshLink{mesh: mesh}
	}
	if uplink != nil {
		b.uplink = &uplinkLink{uplink: uplink}
	}

	return b
}

// Open joins the cluster as the given instance. deliver receives every
// payload published to that instance or to all instances; it must not block
// for long.
func (b *Bus) Open(instance string, deliver func(payload []byte)) error {
	return b.open(instance, func(_ string, payload []byte) { deliver(payload) })
}

// open is Open with deliver told the instance whose connection carried the
// payload.
func (b *Bus) open(instance string, deliver func(peer string, payload []byte)) error {
	b.mu.Lock()
	if b.opened {
		b.mu.Unlock()

		return errors.New("cluster: the bus is already open")
	}
	b.opened = true
	b.instance = instance
	b.deliver = deliver
	b.peers = make(map[string]*busPeer)
	b.conns = make(map[*busConn]struct{})
	b.closing = make(chan struct{})
	b.ctx, b.cancel = context.WithCancel(context.Background())
	b.mu.Unlock()

	if b.mesh != nil {
		if err := b.mesh.open(b); err != nil {
			_ = b.Close()

			return err
		}
	}
	if b.uplink != nil {
		if err := b.uplink.open(b); err != nil {
			_ = b.Close()

			return err
		}
	}

	return nil
}

func (b *Bus) connected(instance string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	p := b.peers[instance]

	return p != nil && p.conn != nil
}

// has reports whether instance is a peer: connected, or briefly without its
// connection.
func (b *Bus) has(instance string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.peers[instance] != nil
}

// connectedPeers are the instances with a connection.
func (b *Bus) connectedPeers() []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	var ids []string
	for id, p := range b.peers {
		if p.conn != nil {
			ids = append(ids, id)
		}
	}

	return ids
}

// pause waits for d and reports whether the bus is still open; done ends
// the wait early too.
func (b *Bus) pause(d time.Duration, done <-chan struct{}) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-done:
		return false
	case <-b.ctx.Done():
		return false
	}
}

// dialMember runs the member handshake on conn as the side that dialed, then
// reads from the connection in the background. It returns the other member's ID.
func (b *Bus) dialMember(ctx context.Context, conn net.Conn, auth *credentials, kind connKind) (string, error) {
	header := http.Header{}
	header.Set(headerInstance, b.instance)
	nonce := auth.signRequest(header, "member", b.instance)

	var peer, signature string
	dialer := ws.Dialer{
		Header:  ws.HandshakeHeaderHTTP(header),
		NetDial: func(context.Context, string, string) (net.Conn, error) { return conn, nil },
		OnHeader: func(key, value []byte) error {
			switch http.CanonicalHeaderKey(string(key)) {
			case headerInstance:
				peer = string(value)
			case headerSignature:
				signature = string(value)
			}

			return nil
		},
	}

	_, br, _, err := dialer.Dial(ctx, "ws://cluster/")
	if err != nil {
		_ = conn.Close()

		var status ws.StatusError
		if errors.As(err, &status) && int(status) == http.StatusConflict {
			return "", errBusSelf
		}

		return "", err
	}

	if peer == "" {
		_ = conn.Close()

		return "", errors.New("the other side did not name its instance")
	}
	if !auth.checkAnswer(signature, "member", nonce, peer) {
		_ = conn.Close()
		b.logger.Warn("cluster: a dialed member failed to authenticate", zap.String("instance", peer))

		return "", errors.New("the other side failed to authenticate")
	}

	var r io.Reader = conn
	if br != nil {
		r = br
	}
	c := newBusConn(conn, r, true, peer, b.instance, kind)
	close(c.ready)

	if !b.attach(c) {
		b.dropConn(c, ws.StatusNormalClosure, "duplicate")

		return peer, nil
	}
	b.wg.Go(func() { b.read(c) })

	return peer, nil
}

// serveMember runs the member handshake on conn as the side that was dialed,
// then reads from the connection until it closes.
func (b *Bus) serveMember(conn net.Conn, auth *credentials, kind connKind) {
	_ = conn.SetDeadline(time.Now().Add(handshakeTimeout))
	stopCancel := context.AfterFunc(b.ctx, func() { _ = conn.Close() })

	var (
		request = http.Header{}
		c       *busConn
		keep    bool
	)
	upgrader := ws.Upgrader{
		OnHeader: func(key, value []byte) error {
			request.Add(string(key), string(value))

			return nil
		},
		OnBeforeUpgrade: func() (ws.HandshakeHeader, error) {
			peer := request.Get(headerInstance)
			if peer == "" {
				return nil, ws.RejectConnectionError(ws.RejectionStatus(http.StatusBadRequest), ws.RejectionReason("missing instance"))
			}
			if peer == b.instance {
				return nil, ws.RejectConnectionError(ws.RejectionStatus(http.StatusConflict), ws.RejectionReason("dialed itself"))
			}
			nonce, err := auth.checkRequest(request, "member", peer)
			if err != nil {
				b.logger.Warn("cluster: refused a member", zap.String("remote", conn.RemoteAddr().String()), zap.Error(err))

				return nil, ws.RejectConnectionError(ws.RejectionStatus(http.StatusUnauthorized), ws.RejectionReason("unauthorized"))
			}

			response := http.Header{}
			response.Set(headerInstance, b.instance)
			auth.signAnswer(response, "member", nonce, b.instance)

			// Attached before the answer goes out, so the member is addressable once its dial returns.
			c = newBusConn(conn, conn, false, peer, peer, kind)
			keep = b.attach(c)

			return ws.HandshakeHeaderHTTP(response), nil
		},
	}

	_, err := upgrader.Upgrade(conn)
	if !stopCancel() && err == nil {
		err = errors.New("the bus closed")
	}
	if err != nil {
		b.logger.Debug("cluster: member handshake failed", zap.String("remote", conn.RemoteAddr().String()), zap.Error(err))
		if c != nil {
			b.dropConn(c, ws.StatusNormalClosure, "")
		} else {
			_ = conn.Close()
		}

		return
	}
	_ = conn.SetDeadline(time.Time{})
	close(c.ready)

	if !keep {
		b.dropConn(c, ws.StatusNormalClosure, "duplicate")

		return
	}
	b.read(c)
}

// busPeer is another member: its connection, if any, and the frames waiting
// for it, written by its own goroutine.
type busPeer struct {
	id    string
	queue chan []byte
	wake  chan struct{}

	// Guarded by the bus's mu.
	conn *busConn
	lost time.Time
	// forgotten is a peer no longer a member, dropped once without a
	// connection instead of waiting for it to reconnect.
	forgotten bool
}

func (p *busPeer) notify() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// attach makes c the connection to its peer, unless the peer has one it
// prefers, and reports whether c was kept.
func (b *Bus) attach(c *busConn) bool {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()

		return false
	}
	b.conns[c] = struct{}{}

	p := b.peers[c.peer]
	if p == nil {
		p = &busPeer{id: c.peer, queue: make(chan []byte, b.queueSize), wake: make(chan struct{}, 1)}
		b.peers[c.peer] = p
		b.writers.Go(func() { b.runPeer(p) })
	}

	p.forgotten = false
	var replaced *busConn
	keep := true
	switch {
	case p.conn == nil:
		p.conn = c
	case b.prefers(c, p.conn):
		replaced, p.conn = p.conn, c
	default:
		keep = false
	}
	if keep {
		p.notify()
	}
	b.mu.Unlock()

	if replaced != nil {
		go b.dropConn(replaced, ws.StatusNormalClosure, "duplicate")
	}

	return keep
}

// prefers reports whether c is to replace current, so that both members keep
// the same one: a direct connection before a tunnel, then the one dialed by
// the lower instance ID, then the newer.
func (b *Bus) prefers(c, current *busConn) bool {
	if c.kind != current.kind {
		return c.kind == connDirect
	}
	lower := min(b.instance, c.peer)

	return c.dialer == lower || current.dialer != lower
}

// dropConn detaches c from its peer and closes it.
func (b *Bus) dropConn(c *busConn, code ws.StatusCode, reason string) {
	b.mu.Lock()
	delete(b.conns, c)
	if p := b.peers[c.peer]; p != nil && p.conn == c {
		p.conn = nil
		p.lost = time.Now()
		p.notify()
	}
	b.mu.Unlock()

	c.close(code, reason)
}

func (b *Bus) runPeer(p *busPeer) {
	heartbeat := time.NewTicker(busHeartbeatInterval)
	defer heartbeat.Stop()

	// pending is a frame taken from the queue for a connection that started
	// closing before it was written; the next connection writes it.
	var pending []byte

	for {
		b.mu.Lock()
		c, lost := p.conn, p.lost
		b.mu.Unlock()

		if c == nil {
			if !b.awaitConn(p, lost) {
				return
			}

			continue
		}

		if !awaitReady(c, b.closing) {
			select {
			case <-b.closing:
				return
			default:
				continue
			}
		}

		if pending != nil {
			drained, err := c.writeBatch(pending, nil)
			if !b.wrote(p, c, err) {
				continue
			}
			pending = nil
			if drained {
				return
			}
		}

		select {
		case frame, ok := <-p.queue:
			if !ok {
				return
			}
			drained, err := c.writeBatch(frame, p.queue)
			if errors.Is(err, errClosing) {
				pending = frame

				continue
			}
			b.wrote(p, c, err)
			if drained {
				return
			}
		case <-heartbeat.C:
			if _, err := c.writeBatch(nil, nil); err != nil && !errors.Is(err, errClosing) {
				b.dropConn(c, ws.StatusGoingAway, "")
			}
		case <-p.wake:
		case <-c.closed:
		}
	}
}

// wrote handles the outcome of a write to c and reports whether it went out;
// a connection that failed to write is dropped, losing what it was writing.
func (b *Bus) wrote(p *busPeer, c *busConn, err error) bool {
	switch {
	case err == nil:
		return true
	case errors.Is(err, errClosing):
		return false
	default:
		b.logger.Debug("cluster: writing to a member failed", zap.String("instance", p.id), zap.Error(err))
		b.dropConn(c, ws.StatusGoingAway, "")

		return true
	}
}

// awaitReady waits for the handshake of c and reports whether it completed;
// a closing bus does not stop a connection that is ready from draining.
func awaitReady(c *busConn, closing chan struct{}) bool {
	select {
	case <-c.ready:
		return true
	default:
	}

	select {
	case <-c.ready:
		return true
	case <-c.closed:
		return false
	case <-closing:
		return false
	}
}

// awaitConn waits for the peer to get a connection again and reports whether
// it did; a peer that stays without one past the linger time, or that is no
// longer a member, is dropped with what was queued for it.
func (b *Bus) awaitConn(p *busPeer, lost time.Time) bool {
	if b.dropIfForgotten(p) {
		return false
	}

	linger := time.NewTimer(time.Until(lost.Add(b.linger)))
	defer linger.Stop()

	select {
	case <-p.wake:
		return true
	case <-b.closing:
		return false
	case <-linger.C:
		b.mu.Lock()
		defer b.mu.Unlock()

		if p.conn != nil {
			return true
		}
		if b.peers[p.id] == p {
			delete(b.peers, p.id)
		}

		return false
	}
}

// dropIfForgotten drops p when it is forgotten and without a connection, and
// reports whether it did.
func (b *Bus) dropIfForgotten(p *busPeer) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !p.forgotten || p.conn != nil {
		return false
	}
	if b.peers[p.id] == p {
		delete(b.peers, p.id)
	}

	return true
}

// forget drops what is queued for instance, which is no longer a member: at
// once without a connection, else once it has none, without waiting for it to
// reconnect.
func (b *Bus) forget(instance string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if p := b.peers[instance]; p != nil {
		p.forgotten = true
		p.notify()
	}
}

// remember undoes forget for instance, a member again.
func (b *Bus) remember(instance string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if p := b.peers[instance]; p != nil {
		p.forgotten = false
	}
}

// Publish sends payload to one instance, or to all instances when instance is
// empty. Publishing to an instance that is gone is not an error.
func (b *Bus) Publish(instance string, payload []byte) error {
	if instance == "" {
		_, err := b.publishAll(slices.Clone(payload))

		return err
	}

	return b.publish(instance, slices.Clone(payload))
}

// publish sends frame, which the bus may keep, to one instance.
func (b *Bus) publish(instance string, frame []byte) error {
	if err := checkPayload(frame); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.opened || b.closed {
		return errors.New("cluster: the bus is closed")
	}

	if p := b.peers[instance]; p != nil {
		return enqueue(p, frame)
	}

	return nil
}

// publishAll sends frame, which the bus may keep, to every other member and
// returns for how many it was queued.
func (b *Bus) publishAll(frame []byte) (int, error) {
	if err := checkPayload(frame); err != nil {
		return 0, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.opened || b.closed {
		return 0, errors.New("cluster: the bus is closed")
	}

	return b.enqueueAll(frame)
}

func checkPayload(payload []byte) error {
	if len(payload) == 0 {
		return errors.New("cluster: empty payload")
	}
	if len(payload) > maxBusPayload {
		return fmt.Errorf("cluster: payload of %d bytes exceeds %d", len(payload), maxBusPayload)
	}

	return nil
}

// enqueueAll queues frame for every peer. b.mu must be held.
func (b *Bus) enqueueAll(frame []byte) (int, error) {
	queued := 0
	var errs []error
	for _, p := range b.peers {
		if err := enqueue(p, frame); err != nil {
			errs = append(errs, err)
		} else {
			queued++
		}
	}

	return queued, errors.Join(errs...)
}

// enqueue hands frame to the writer of p. A peer whose queue is full misses
// the frame, so that it cannot hold up the others.
func enqueue(p *busPeer, frame []byte) error {
	select {
	case p.queue <- frame:
		return nil
	default:
		return fmt.Errorf("cluster: the queue to %s is full", p.id)
	}
}

func (b *Bus) read(c *busConn) {
	c.reading.Store(true)
	defer b.dropConn(c, ws.StatusNormalClosure, "")
	defer c.stoppedReading()

	for {
		payload, err := c.readFrame()
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				b.logger.Debug("cluster: reading from a member failed", zap.String("instance", c.peer), zap.Error(err))
			}

			return
		}
		if len(payload) > 0 {
			b.deliver(c.peer, payload)
		}
	}
}

// Close lets the writers send what is queued, such as the goodbye of the
// member, for up to the write timeout, then closes the connections.
func (b *Bus) Close() error {
	b.mu.Lock()
	if !b.opened || b.closed {
		b.mu.Unlock()

		return nil
	}
	b.closed = true
	close(b.closing)
	for _, p := range b.peers {
		// Publish holds b.mu and checks closed, so nothing is sent on a closed queue.
		close(p.queue)
	}
	b.mu.Unlock()

	if b.mesh != nil {
		b.mesh.close()
	}
	if b.uplink != nil {
		b.uplink.close()
	}
	b.cancel()

	drained := make(chan struct{})
	go func() {
		b.writers.Wait()
		close(drained)
	}()

	timer := time.NewTimer(busWriteTimeout)
	defer timer.Stop()

	select {
	case <-drained:
	case <-timer.C:
	}

	b.mu.Lock()
	conns := make([]*busConn, 0, len(b.conns))
	for c := range b.conns {
		conns = append(conns, c)
	}
	b.mu.Unlock()

	var closing sync.WaitGroup
	for _, c := range conns {
		closing.Go(func() { b.dropConn(c, ws.StatusGoingAway, "shutting down") })
	}
	closing.Wait()

	<-drained
	b.wg.Wait()

	return nil
}

// MeshAddr is the address the mesh of the bus accepts members on, or "" without one.
func (b *Bus) MeshAddr() string {
	if b.mesh == nil {
		return ""
	}

	return b.mesh.addr()
}

func orDefault[T int | time.Duration](value, fallback T) T {
	if value > 0 {
		return value
	}

	return fallback
}
