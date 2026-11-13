package websocket

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gobwas/ws"
	"golang.org/x/time/rate"
)

const (
	defaultMaxMessageSize = 32 << 10
	defaultWriteTimeout   = 10 * time.Second
	defaultCloseTimeout   = 5 * time.Second
	defaultIdleTimeout    = time.Minute
	defaultPingInterval   = 30 * time.Second
)

var errConnClosed = errors.New("websocket connection closed")

// limits bound a connection. A zero idleTimeout, pingInterval or messageRate
// disables it.
type limits struct {
	maxMessageSize int64
	writeTimeout   time.Duration
	closeTimeout   time.Duration
	idleTimeout    time.Duration
	pingInterval   time.Duration
	// messageRate frames may arrive per messageRateInterval, in bursts of up to messageRate.
	messageRate         int
	messageRateInterval time.Duration
}

// transport is the byte stream a WebSocket runs on: a hijacked HTTP/1.1
// connection, or an HTTP/2 or HTTP/3 extended CONNECT stream.
type transport struct {
	src              io.Reader
	dst              io.Writer
	flush            func() error
	setReadDeadline  func(time.Time) error
	setWriteDeadline func(time.Time) error
	// shutdown must unblock a pending read on src.
	shutdown func() error
}

// frameHandlers receive what the peer sends; a nil handler ignores it. An
// error fails the connection with status 1011.
type frameHandlers struct {
	message func(text bool, data []byte) error
	ping    func(payload []byte) error
	pong    func(payload []byte) error
}

type conn struct {
	transport transport
	limits    limits
	compress  bool
	// keepalivePayload tells the pongs answering keepalive pings apart.
	keepalivePayload []byte
	// rate is nil without a message rate limit.
	rate *rate.Limiter

	writeMu   sync.Mutex
	closeSent bool
	finished  bool
	// status and reason are those of the close that ended the connection;
	// status is 0 while unknown.
	status ws.StatusCode
	reason string

	shutdownOnce sync.Once
	done         chan struct{}
}

func newConn(t transport, l limits, compress bool) *conn {
	keepalivePayload := make([]byte, 16)
	_, _ = rand.Read(keepalivePayload)

	c := &conn{
		transport:        t,
		limits:           l,
		compress:         compress,
		keepalivePayload: keepalivePayload,
		done:             make(chan struct{}),
	}
	if l.messageRate > 0 {
		c.rate = rate.NewLimiter(rate.Every(l.messageRateInterval/time.Duration(l.messageRate)), l.messageRate)
	}

	return c
}

// serve reads frames until the connection ends, passing them to handlers when
// it is not nil. Once it returns, no further writes reach the transport, so
// the HTTP handler may return.
func (c *conn) serve(handlers *frameHandlers) {
	defer func() {
		c.shutdownTransport()

		c.writeMu.Lock()
		c.finished = true
		c.writeMu.Unlock()

		close(c.done)
	}()

	if handlers == nil {
		handlers = &frameHandlers{}
	}

	if c.limits.pingInterval > 0 {
		go c.keepalive()
	}

	var (
		message    []byte
		messageOp  ws.OpCode
		compressed bool
		state      = ws.StateServerSide
	)
	if c.compress {
		state = state.Set(ws.StateExtended)
	}

	for {
		if c.limits.idleTimeout > 0 {
			if err := c.transport.setReadDeadline(time.Now().Add(c.limits.idleTimeout)); err != nil {
				return
			}
		}

		header, err := ws.ReadHeader(c.transport.src)
		if err != nil {
			c.closeIfIdle(err)

			return
		}

		if err := ws.CheckHeader(header, state); err != nil || !c.validReservedBits(header) {
			c.fail(ws.StatusProtocolError)

			return
		}

		if header.OpCode.IsData() && int64(len(message))+header.Length > c.limits.maxMessageSize {
			c.fail(ws.StatusMessageTooBig)

			return
		}

		payload := make([]byte, header.Length)
		if _, err := io.ReadFull(c.transport.src, payload); err != nil {
			c.closeIfIdle(err)

			return
		}
		ws.Cipher(payload, header.Mask, 0)

		if c.rate != nil && header.OpCode != ws.OpClose && !c.rate.Allow() {
			_ = c.writeFrame(ws.NewCloseFrame(ws.NewCloseFrameBody(ws.StatusPolicyViolation, "message rate exceeded")))

			return
		}

		switch header.OpCode {
		case ws.OpPing:
			if err := c.writeFrame(ws.NewPongFrame(payload)); err != nil {
				return
			}
			if !c.handle(handlers.ping, payload) {
				return
			}

			continue
		case ws.OpPong:
			if !bytes.Equal(payload, c.keepalivePayload) && !c.handle(handlers.pong, payload) {
				return
			}

			continue
		case ws.OpClose:
			c.answerClose(payload)

			return
		case ws.OpText, ws.OpBinary:
			message, messageOp, compressed = payload, header.OpCode, header.Rsv1()
		case ws.OpContinuation:
			message = append(message, payload...)
		}

		if !header.Fin {
			state = state.Set(ws.StateFragmented)

			continue
		}
		state = state.Clear(ws.StateFragmented)

		if compressed {
			message, err = inflate(message, c.limits.maxMessageSize)
			if errors.Is(err, errMessageTooBig) {
				c.fail(ws.StatusMessageTooBig)

				return
			}
			if err != nil {
				c.fail(ws.StatusInvalidFramePayloadData)

				return
			}
		}

		if messageOp == ws.OpText && !utf8.Valid(message) {
			c.fail(ws.StatusInvalidFramePayloadData)

			return
		}

		if handlers.message != nil {
			if err := handlers.message(messageOp == ws.OpText, message); err != nil {
				c.fail(ws.StatusInternalServerError)

				return
			}
		}

		message = nil
	}
}

func (c *conn) handle(handler func([]byte) error, payload []byte) bool {
	if handler == nil {
		return true
	}

	if err := handler(payload); err != nil {
		c.fail(ws.StatusInternalServerError)

		return false
	}

	return true
}

// validReservedBits allows RSV1 only as the compression bit of a message's
// first frame (RFC 7692, section 6), and only once compression is negotiated.
func (c *conn) validReservedBits(header ws.Header) bool {
	if !c.compress {
		return true
	}

	if header.Rsv2() || header.Rsv3() {
		return false
	}

	return !header.Rsv1() || header.OpCode == ws.OpText || header.OpCode == ws.OpBinary
}

func (c *conn) closeIfIdle(err error) {
	var netErr net.Error
	if errors.Is(err, os.ErrDeadlineExceeded) || errors.As(err, &netErr) && netErr.Timeout() {
		_ = c.writeFrame(ws.NewCloseFrame(ws.NewCloseFrameBody(ws.StatusGoingAway, "idle timeout")))
	}
}

func (c *conn) keepalive() {
	ticker := time.NewTicker(c.limits.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			if err := c.writeFrame(ws.NewPingFrame(c.keepalivePayload)); err != nil {
				return
			}
		}
	}
}

func (c *conn) answerClose(payload []byte) {
	if len(payload) == 0 {
		c.recordStatus(ws.StatusNoStatusRcvd, "")
		_ = c.writeFrame(ws.NewCloseFrame(nil))

		return
	}

	if len(payload) < 2 {
		c.fail(ws.StatusProtocolError)

		return
	}

	code, reason := ws.ParseCloseFrameData(payload)
	if err := ws.CheckCloseFrameData(code, reason); err != nil {
		if errors.Is(err, ws.ErrProtocolInvalidUTF8) {
			c.fail(ws.StatusInvalidFramePayloadData)
		} else {
			c.fail(ws.StatusProtocolError)
		}

		return
	}

	c.recordStatus(code, reason)
	_ = c.writeFrame(ws.NewCloseFrame(ws.NewCloseFrameBody(code, "")))
}

func (c *conn) fail(code ws.StatusCode) {
	_ = c.writeFrame(ws.NewCloseFrame(ws.NewCloseFrameBody(code, "")))
}

// close runs the whole closing handshake; see startClose and awaitClose.
func (c *conn) close(code ws.StatusCode, reason string) {
	_ = c.startClose(code, reason)
	c.awaitClose()
}

// startClose sends the close frame that starts the closing handshake.
func (c *conn) startClose(code ws.StatusCode, reason string) error {
	return c.writeFrame(ws.NewCloseFrame(ws.NewCloseFrameBody(code, reason)))
}

// awaitClose waits for the peer to answer the close, cutting the transport if
// it does not answer within the close timeout.
func (c *conn) awaitClose() {
	timer := time.NewTimer(c.limits.closeTimeout)
	defer timer.Stop()

	select {
	case <-c.done:
	case <-timer.C:
		c.shutdownTransport()
		<-c.done
	}
}

// send writes a complete message, as a text frame when text is true.
func (c *conn) send(text bool, data []byte) error {
	op := ws.OpBinary
	if text {
		op = ws.OpText
	}

	if !c.compress {
		return c.writeFrame(ws.NewFrame(op, true, data))
	}

	compressed, err := deflate(data)
	if err != nil {
		return err
	}

	frame := ws.NewFrame(op, true, compressed)
	frame.Header.Rsv = ws.Rsv(true, false, false)

	return c.writeFrame(frame)
}

func (c *conn) ping(payload []byte) error {
	return c.writeFrame(ws.NewPingFrame(payload))
}

// open reports whether messages can still be sent: no close was sent and the
// connection has not ended.
func (c *conn) open() bool {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	return !c.closeSent && !c.finished
}

// closeStatus reports the close that ended the connection, status 1006 when
// it ended without a close frame. Valid once serve has returned.
func (c *conn) closeStatus() (ws.StatusCode, string) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.status == 0 {
		return ws.StatusAbnormalClosure, ""
	}

	return c.status, c.reason
}

func (c *conn) recordStatus(code ws.StatusCode, reason string) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.recordStatusLocked(code, reason)
}

func (c *conn) recordStatusLocked(code ws.StatusCode, reason string) {
	if c.status == 0 {
		c.status, c.reason = code, reason
	}
}

func (c *conn) writeFrame(frame ws.Frame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.finished || c.closeSent {
		return errConnClosed
	}

	if frame.Header.OpCode == ws.OpClose {
		c.closeSent = true
	}

	err := c.transport.setWriteDeadline(time.Now().Add(c.limits.writeTimeout))
	if err == nil {
		err = ws.WriteFrame(c.transport.dst, frame)
	}
	if err == nil {
		err = c.transport.flush()
	}
	if err != nil {
		c.shutdownTransport()

		return err
	}

	if frame.Header.OpCode == ws.OpClose {
		code, reason := ws.ParseCloseFrameData(frame.Payload)
		if code == 0 {
			code = ws.StatusNoStatusRcvd
		}
		c.recordStatusLocked(code, reason)
	}

	return nil
}

func (c *conn) shutdownTransport() {
	c.shutdownOnce.Do(func() {
		_ = c.transport.shutdown()
	})
}
