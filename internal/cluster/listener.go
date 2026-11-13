package cluster

import (
	"errors"
	"net"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"
)

// acceptor takes the connections of a listener.
type acceptor interface {
	accepted(conn net.Conn)
}

// sharedListener accepts the connections of an address. A config reload
// starts the new config before it stops the old one, so the owners in the
// process share the listener of an address, and new connections go to the
// newest.
type sharedListener struct {
	key string
	ln  net.Listener

	mu     sync.Mutex
	owners []acceptor
}

var (
	sharedListenersMu sync.Mutex
	sharedListeners   = make(map[string]*sharedListener)
)

func acquireListener(kind, address string, owner acceptor, logger *zap.Logger) (*sharedListener, error) {
	sharedListenersMu.Lock()
	defer sharedListenersMu.Unlock()

	key := kind + " " + address
	shared := sharedAddress(address)
	if l, ok := sharedListeners[key]; ok && shared {
		l.mu.Lock()
		l.owners = append(l.owners, owner)
		l.mu.Unlock()

		return l, nil
	}

	ln, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	l := &sharedListener{key: key, ln: ln, owners: []acceptor{owner}}
	if shared {
		sharedListeners[key] = l
	}
	go l.accept(logger)

	return l, nil
}

// sharedAddress reports whether owners listening on address can share its
// listener: not when the system picks the port, which differs every time.
func sharedAddress(address string) bool {
	_, port, err := net.SplitHostPort(address)

	return err == nil && port != "" && port != "0"
}

func (l *sharedListener) addr() string {
	return l.ln.Addr().String()
}

func (l *sharedListener) accept(logger *zap.Logger) {
	for {
		conn, err := l.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			logger.Debug("cluster: accepting failed", zap.String("listener", l.key), zap.Error(err))
			time.Sleep(50 * time.Millisecond)

			continue
		}

		l.mu.Lock()
		var owner acceptor
		if len(l.owners) > 0 {
			owner = l.owners[len(l.owners)-1]
		}
		l.mu.Unlock()

		if owner == nil {
			_ = conn.Close()

			continue
		}
		owner.accepted(conn)
	}
}

func (l *sharedListener) release(owner acceptor) {
	sharedListenersMu.Lock()
	defer sharedListenersMu.Unlock()

	l.mu.Lock()
	l.owners = slices.DeleteFunc(l.owners, func(o acceptor) bool { return o == owner })
	last := len(l.owners) == 0
	l.mu.Unlock()

	if !last {
		return
	}
	if sharedListeners[l.key] == l {
		delete(sharedListeners, l.key)
	}
	_ = l.ln.Close()
}
