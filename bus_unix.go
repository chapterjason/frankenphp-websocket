package frankenphpwebsocket

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"go.uber.org/zap"
)

const (
	unixSocketSuffix = ".sock"
	maxUnixFrame     = 64 << 20
	// maxUnixSocketPath is the smallest limit among the supported systems, that of macOS (104 bytes with the terminator).
	maxUnixSocketPath    = 103
	unixDialTimeout      = time.Second
	unixWriteTimeout     = 5 * time.Second
	defaultUnixQueueSize = 1024
)

func init() {
	caddy.RegisterModule(new(UnixBus))
}

// UnixBus connects the instances of one machine through Unix sockets: every
// instance listens on <directory>/<instance>.sock and writes to the sockets
// of the others.
type UnixBus struct {
	// Directory holds the sockets of all instances; it is created if missing.
	Directory string `json:"directory,omitempty"`
	// QueueSize is how many messages may wait for one peer before it misses them. Default: 1024.
	QueueSize int `json:"queue_size,omitempty"`

	logger   *zap.Logger
	instance string
	listener net.Listener

	mu       sync.Mutex
	peers    map[string]*unixPeer
	accepted map[net.Conn]struct{}
	closed   bool
	wg       sync.WaitGroup
	writers  sync.WaitGroup
}

func (*UnixBus) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "websocket.bus.unix",
		New: func() caddy.Module { return new(UnixBus) },
	}
}

func (u *UnixBus) Provision(ctx caddy.Context) error {
	if u.Directory == "" {
		return errors.New("websocket unix bus: a directory is required")
	}

	u.logger = ctx.Logger()

	return nil
}

func (u *UnixBus) socketPath(instance string) string {
	return filepath.Join(u.Directory, instance+unixSocketSuffix)
}

func (u *UnixBus) Open(instance string, deliver func(payload []byte)) error {
	if u.logger == nil {
		u.logger = zap.NewNop()
	}

	if err := os.MkdirAll(u.Directory, 0o700); err != nil {
		return err
	}

	path := u.socketPath(instance)
	if len(path) > maxUnixSocketPath {
		return fmt.Errorf("websocket unix bus: socket path %q is %d bytes, more than the %d a Unix socket allows; use a shorter directory", path, len(path), maxUnixSocketPath)
	}

	listener, err := net.Listen("unix", path)
	if err != nil {
		return err
	}

	u.instance = instance
	u.listener = listener
	u.peers = make(map[string]*unixPeer)
	u.accepted = make(map[net.Conn]struct{})

	u.wg.Go(func() { u.accept(deliver) })

	return nil
}

func (u *UnixBus) accept(deliver func(payload []byte)) {
	for {
		c, err := u.listener.Accept()
		if err != nil {
			return
		}

		u.mu.Lock()
		if u.closed {
			u.mu.Unlock()
			_ = c.Close()

			return
		}
		u.accepted[c] = struct{}{}
		u.mu.Unlock()

		u.wg.Go(func() {
			defer func() {
				u.mu.Lock()
				delete(u.accepted, c)
				u.mu.Unlock()
				_ = c.Close()
			}()

			r := bufio.NewReader(c)
			for {
				payload, err := readUnixFrame(r)
				if err != nil {
					return
				}
				deliver(payload)
			}
		})
	}
}

func readUnixFrame(r io.Reader) ([]byte, error) {
	var size [4]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(size[:])
	if length > maxUnixFrame {
		return nil, fmt.Errorf("websocket unix bus: frame of %d bytes exceeds %d", length, maxUnixFrame)
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	return payload, nil
}

func (u *UnixBus) Publish(instance string, payload []byte) error {
	if len(payload) > maxUnixFrame {
		return fmt.Errorf("websocket unix bus: payload of %d bytes exceeds %d", len(payload), maxUnixFrame)
	}

	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame, uint32(len(payload)))
	copy(frame[4:], payload)

	u.mu.Lock()
	defer u.mu.Unlock()

	if u.closed {
		return errors.New("websocket unix bus: closed")
	}

	if instance != "" {
		path := u.socketPath(instance)
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return u.enqueue(path, frame)
	}

	paths, err := filepath.Glob(filepath.Join(u.Directory, "*"+unixSocketSuffix))
	if err != nil {
		return err
	}

	var errs []error
	for _, path := range paths {
		if path == u.socketPath(u.instance) {
			continue
		}
		if err := u.enqueue(path, frame); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// enqueue hands frame to the writer of the peer on path, starting it if
// needed. A peer whose queue is full misses the frame, so that it cannot hold
// up the others. u.mu must be held.
func (u *UnixBus) enqueue(path string, frame []byte) error {
	p, ok := u.peers[path]
	if !ok {
		p = &unixPeer{path: path, queue: make(chan []byte, orDefault(u.QueueSize, defaultUnixQueueSize)), done: make(chan struct{})}
		u.peers[path] = p
		u.writers.Go(func() { u.runPeer(p) })
	}

	select {
	case p.queue <- frame:
		return nil
	default:
		return fmt.Errorf("websocket unix bus: the queue to %s is full", path)
	}
}

// unixPeer is the connection to one other instance and the frames waiting for
// it, written by its own goroutine.
type unixPeer struct {
	path  string
	queue chan []byte
	done  chan struct{}

	mu      sync.Mutex
	conn    net.Conn
	stopped bool
}

func (p *unixPeer) current() net.Conn {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.conn
}

// attach keeps c as the connection, unless the peer was stopped meanwhile.
func (p *unixPeer) attach(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stopped {
		return false
	}
	p.conn = c

	return true
}

func (p *unixPeer) dropConn() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
	}
}

// stop ends the writer, unblocking a write in progress.
func (p *unixPeer) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.stopped = true
	close(p.done)
	if p.conn != nil {
		_ = p.conn.Close()
	}
}

func (u *UnixBus) runPeer(p *unixPeer) {
	defer func() {
		p.dropConn()

		u.mu.Lock()
		if u.peers[p.path] == p {
			delete(u.peers, p.path)
		}
		u.mu.Unlock()
	}()

	for {
		select {
		case <-p.done:
			return
		case frame, ok := <-p.queue:
			if !ok {
				return
			}
			if gone := u.write(p, frame); gone {
				return
			}
		}
	}
}

// write sends frame to the peer, redialing once when its connection turns out
// to be broken. It reports the peer as gone when its socket is missing, or
// left behind by an instance that died, which it then removes.
func (u *UnixBus) write(p *unixPeer, frame []byte) (gone bool) {
	for attempt := 0; attempt < 2; attempt++ {
		c := p.current()
		if c == nil {
			var err error
			c, err = net.DialTimeout("unix", p.path, unixDialTimeout)
			if errors.Is(err, os.ErrNotExist) {
				return true
			}
			if errors.Is(err, syscall.ECONNREFUSED) {
				u.removeStaleSocket(p.path)

				return true
			}
			if err != nil {
				u.logger.Debug("websocket unix bus: dialing peer failed", zap.String("path", p.path), zap.Error(err))

				return false
			}
			if !p.attach(c) {
				_ = c.Close()

				return true
			}
		}

		err := c.SetWriteDeadline(time.Now().Add(unixWriteTimeout))
		if err == nil {
			_, err = c.Write(frame)
		}
		if err == nil {
			return false
		}

		p.dropConn()
		if attempt == 1 {
			u.logger.Debug("websocket unix bus: writing to peer failed", zap.String("path", p.path), zap.Error(err))
		}
	}

	return false
}

func (u *UnixBus) removeStaleSocket(path string) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		u.logger.Warn("websocket unix bus: removing stale socket failed", zap.String("path", path), zap.Error(err))
	}
}

// Close lets the writers send what is queued, such as the goodbye of the
// instance, for up to the write timeout, then stops them.
func (u *UnixBus) Close() error {
	if u.listener == nil {
		return nil
	}

	u.mu.Lock()
	if u.closed {
		u.mu.Unlock()

		return nil
	}
	u.closed = true
	peers := make([]*unixPeer, 0, len(u.peers))
	for _, p := range u.peers {
		// Publish holds u.mu and checks closed, so nothing is sent on a closed queue.
		close(p.queue)
		peers = append(peers, p)
	}
	u.mu.Unlock()

	drained := make(chan struct{})
	go func() {
		u.writers.Wait()
		close(drained)
	}()

	timer := time.NewTimer(unixWriteTimeout)
	defer timer.Stop()

	select {
	case <-drained:
	case <-timer.C:
		for _, p := range peers {
			p.stop()
		}
		<-drained
	}

	u.mu.Lock()
	for c := range u.accepted {
		_ = c.Close()
	}
	u.mu.Unlock()

	// Closing the listener also removes its socket file.
	err := u.listener.Close()
	u.wg.Wait()

	return err
}

// UnmarshalCaddyfile parses:
//
//	bus unix <directory> {
//		queue_size <count>
//	}
func (u *UnixBus) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next()

	if !d.NextArg() {
		return d.ArgErr()
	}
	u.Directory = d.Val()

	if d.NextArg() {
		return d.ArgErr()
	}

	for d.NextBlock(0) {
		switch d.Val() {
		case "queue_size":
			if !d.NextArg() {
				return d.ArgErr()
			}
			size, err := strconv.Atoi(d.Val())
			if err != nil || size < 1 {
				return d.Errf("invalid queue_size '%s'", d.Val())
			}
			u.QueueSize = size
			if d.NextArg() {
				return d.ArgErr()
			}
		default:
			return d.Errf("unrecognized unix bus option '%s'", d.Val())
		}
	}

	if strings.TrimSpace(u.Directory) == "" {
		return d.Err("the unix bus directory must not be empty")
	}

	return nil
}

var (
	_ Bus                   = (*UnixBus)(nil)
	_ caddy.Provisioner     = (*UnixBus)(nil)
	_ caddyfile.Unmarshaler = (*UnixBus)(nil)
)
