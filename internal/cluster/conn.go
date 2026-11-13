package cluster

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobwas/ws"
)

// busConn is one connection to another member, framed as WebSocket. The writer of its
// peer is the only one writing frames to it, besides the close frame.
type busConn struct {
	conn   net.Conn
	r      io.Reader
	client bool
	peer   string
	dialer string
	kind   connKind

	ready     chan struct{}
	closed    chan struct{}
	closeOnce sync.Once

	// readDone is closed when the reader stops, after the other side's close
	// frame or an error; reading tells whether a reader runs at all.
	reading      atomic.Bool
	readDone     chan struct{}
	readDoneOnce sync.Once

	writeMu sync.Mutex
	closing bool
	w       *bufio.Writer
	scratch []byte
}

// errClosing is returned for a frame not written because the connection is
// closing; it goes on another connection instead.
var errClosing = errors.New("cluster: the connection is closing")

func newBusConn(conn net.Conn, r io.Reader, client bool, peer, dialer string, kind connKind) *busConn {
	switch r.(type) {
	case *bufio.Reader, *bufferedConn:
	default:
		// Unbuffered, every frame header would cost reads of a few bytes each.
		r = bufio.NewReaderSize(r, 64<<10)
	}

	return &busConn{
		conn:     conn,
		r:        r,
		client:   client,
		peer:     peer,
		dialer:   dialer,
		kind:     kind,
		ready:    make(chan struct{}),
		closed:   make(chan struct{}),
		readDone: make(chan struct{}),
		w:        bufio.NewWriterSize(conn, 64<<10),
	}
}

// writeBatch writes frame and what else is queued, up to a batch, in one
// flush; a nil frame writes a heartbeat. It reports whether the queue was
// closed and drained.
func (c *busConn) writeBatch(frame []byte, queue chan []byte) (drained bool, err error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.closing {
		return false, errClosing
	}

	if err := c.conn.SetWriteDeadline(time.Now().Add(busWriteTimeout)); err != nil {
		return false, err
	}
	if err := c.writeFrame(ws.OpBinary, frame); err != nil {
		return false, err
	}

	for range busWriteBatch - 1 {
		if queue == nil {
			break
		}
		var ok bool
		select {
		case frame, ok = <-queue:
		default:
			return false, c.w.Flush()
		}
		if !ok {
			return true, c.w.Flush()
		}
		if err := c.writeFrame(ws.OpBinary, frame); err != nil {
			return false, err
		}
	}

	return false, c.w.Flush()
}

// writeFrame writes one frame, masked when this side dialed, as RFC 6455
// requires of clients. c.writeMu must be held.
func (c *busConn) writeFrame(op ws.OpCode, payload []byte) error {
	header := ws.Header{Fin: true, OpCode: op, Length: int64(len(payload))}
	if !c.client {
		if err := ws.WriteHeader(c.w, header); err != nil {
			return err
		}
		_, err := c.w.Write(payload)

		return err
	}

	header.Masked = true
	header.Mask = ws.NewMask()
	if err := ws.WriteHeader(c.w, header); err != nil {
		return err
	}
	c.scratch = append(c.scratch[:0], payload...)
	ws.Cipher(c.scratch, header.Mask, 0)
	_, err := c.w.Write(c.scratch)

	return err
}

// readFrame returns the payload of the next data frame, empty for a
// heartbeat, or io.EOF once the other side closes.
func (c *busConn) readFrame() ([]byte, error) {
	for {
		if err := c.conn.SetReadDeadline(time.Now().Add(busReadTimeout)); err != nil {
			return nil, err
		}

		header, err := ws.ReadHeader(c.r)
		if err != nil {
			return nil, err
		}
		if header.Masked == c.client {
			return nil, errors.New("frame masking does not match the side that dialed")
		}
		if !header.Fin {
			return nil, errors.New("fragmented frame")
		}
		if header.Length > maxBusPayload {
			return nil, fmt.Errorf("frame of %d bytes exceeds the limit", header.Length)
		}

		payload := make([]byte, header.Length)
		if _, err := io.ReadFull(c.r, payload); err != nil {
			return nil, err
		}
		if header.Masked {
			ws.Cipher(payload, header.Mask, 0)
		}

		switch header.OpCode {
		case ws.OpBinary:
			return payload, nil
		case ws.OpClose:
			return nil, io.EOF
		case ws.OpPing, ws.OpPong:
		default:
			return nil, fmt.Errorf("unexpected opcode %v", header.OpCode)
		}
	}
}

// stoppedReading marks the reader as done.
func (c *busConn) stoppedReading() {
	c.readDoneOnce.Do(func() { close(c.readDone) })
}

// close sends a close frame, once the handshake is done, and closes the
// connection after the other side's close frame, so what it sent before is
// still read and delivered; at most for the close frame timeout. Nothing is
// written after the close frame. A write in progress is cut short by the
// deadline.
func (c *busConn) close(code ws.StatusCode, reason string) {
	c.closeOnce.Do(func() {
		select {
		case <-c.ready:
			_ = c.conn.SetWriteDeadline(time.Now().Add(busCloseFrameTimeout))
			c.writeMu.Lock()
			c.closing = true
			written := c.writeFrame(ws.OpClose, ws.NewCloseFrameBody(code, reason)) == nil && c.w.Flush() == nil
			c.writeMu.Unlock()

			if written && c.reading.Load() {
				timer := time.NewTimer(busCloseFrameTimeout)
				select {
				case <-c.readDone:
				case <-timer.C:
				}
				timer.Stop()
			}
		default:
		}

		_ = c.conn.Close()
		close(c.closed)
	})
}

// bufferedConn reads through the buffer that read the handshake, which may
// already hold what followed it.
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.r.Read(p)
}
