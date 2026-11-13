package websocket

import (
	"bufio"
	"bytes"
	"compress/flate"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/chapterjason/frankenphp-websocket/internal/cluster"
	"github.com/dunglas/frankenphp"
	"github.com/gobwas/ws"
	"github.com/quic-go/quic-go/http3"
	"go.uber.org/zap/zaptest"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

var testWorkers frankenphp.Workers

// TestMain re-runs the tests with HTTP/2 extended CONNECT enabled: net/http
// only reads http2xconnect from the GODEBUG environment variable.
func TestMain(m *testing.M) {
	godebug := os.Getenv("GODEBUG")
	if strings.Contains(godebug, "http2xconnect=1") {
		os.Exit(runWithFrankenPHP(m))
	}

	if godebug != "" {
		godebug += ","
	}

	cmd := exec.Command(os.Args[0], os.Args[1:]...)
	cmd.Env = append(os.Environ(), "GODEBUG="+godebug+"http2xconnect=1")
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr

	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			os.Exit(exitErr.ExitCode())
		}
		panic(err)
	}
	os.Exit(0)
}

func runWithFrankenPHP(m *testing.M) int {
	workers, opt := frankenphp.WithExtensionWorkers("websocket-test", "testdata/worker.php", 2)
	if err := frankenphp.Init(opt); err != nil {
		fmt.Fprintln(os.Stderr, "frankenphp init:", err)

		return 1
	}
	defer frankenphp.Shutdown()

	testWorkers = workers

	return m.Run()
}

// recordingWorkers passes events through to the PHP worker and records them.
type recordingWorkers struct {
	frankenphp.Workers

	mu     sync.Mutex
	events []map[string]any
}

func (r *recordingWorkers) SendMessage(ctx context.Context, message any, rw http.ResponseWriter) (any, error) {
	if event, ok := message.(frankenphp.AssociativeArray[any]); ok {
		r.mu.Lock()
		r.events = append(r.events, maps.Clone(event.Map))
		r.mu.Unlock()
	}

	return r.Workers.SendMessage(ctx, message, rw)
}

// withoutContext drops the keys every event carries, keeping what is specific to it.
func withoutContext(events []map[string]any) []map[string]any {
	stripped := make([]map[string]any, len(events))
	for i, event := range events {
		stripped[i] = maps.Clone(event)
		delete(stripped[i], "attributes")
		delete(stripped[i], "tags")
		delete(stripped[i], "request")
	}

	return stripped
}

func (r *recordingWorkers) recorded() []map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Clone(r.events)
}

var protocols = []string{"HTTP/1.1", "HTTP/2", "HTTP/3"}

type wsClient struct {
	r     io.Reader
	w     io.Writer
	close func()
}

type testServer struct {
	handler    *Handler
	workers    *recordingWorkers
	nextCalled *atomic.Bool
	handshake  func(t testing.TB, path string, header http.Header) handshakeResult
	addr       string
}

// handshakeResult has a client only when the handshake was accepted.
type handshakeResult struct {
	client *wsClient
	status int
	header http.Header
	body   string
}

func (s *testServer) dial(t testing.TB) *wsClient {
	t.Helper()

	res := s.handshake(t, "/", nil)
	if res.client == nil {
		t.Fatalf("handshake status = %d, want it accepted", res.status)
	}

	return res.client
}

type caddyHandlerFunc func(http.ResponseWriter, *http.Request) error

func (f caddyHandlerFunc) ServeHTTP(w http.ResponseWriter, r *http.Request) error {
	return f(w, r)
}

func newTestHandler(t testing.TB, workers frankenphp.Workers, configure func(*Handler)) (*Handler, http.Handler, *atomic.Bool) {
	t.Helper()

	h := new(Handler)
	if configure != nil {
		configure(h)
	}
	h.setup(zaptest.NewLogger(t), workers)

	nextCalled := new(atomic.Bool)
	next := caddyHandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		nextCalled.Store(true)
		w.WriteHeader(http.StatusTeapot)

		return nil
	})

	// Runs after the servers and clients are closed, so no handler outlives the test.
	var inFlight sync.WaitGroup
	t.Cleanup(inFlight.Wait)

	return h, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inFlight.Add(1)
		defer inFlight.Done()

		if err := h.ServeHTTP(w, r, next); err != nil {
			var handlerErr caddyhttp.HandlerError
			if errors.As(err, &handlerErr) {
				w.WriteHeader(handlerErr.StatusCode)

				return
			}
			t.Errorf("ServeHTTP: %v", err)
		}
	}), nextCalled
}

func startServer(t testing.TB, proto string) *testServer {
	t.Helper()

	return startServerWith(t, proto, testWorkers, nil)
}

func startServerWith(t testing.TB, proto string, workers frankenphp.Workers, configure func(*Handler)) *testServer {
	t.Helper()

	recorder := &recordingWorkers{Workers: workers}
	h, handler, nextCalled := newTestHandler(t, recorder, configure)
	s := &testServer{handler: h, workers: recorder, nextCalled: nextCalled}

	switch proto {
	case "HTTP/1.1":
		srv := httptest.NewServer(handler)
		t.Cleanup(srv.Close)
		s.addr = srv.Listener.Addr().String()
		s.handshake = func(t testing.TB, path string, header http.Header) handshakeResult {
			return dialHTTP1(t, s.addr, path, header)
		}
	case "HTTP/2":
		srv := httptest.NewUnstartedServer(handler)
		srv.EnableHTTP2 = true
		srv.StartTLS()
		t.Cleanup(srv.Close)
		s.addr = srv.Listener.Addr().String()
		s.handshake = func(t testing.TB, path string, header http.Header) handshakeResult {
			return dialHTTP2(t, s.addr, path, header)
		}
	case "HTTP/3":
		cert, pool := selfSignedCert(t)

		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		srv := &http3.Server{Handler: handler, TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}}}
		go func() { _ = srv.Serve(pc) }()
		t.Cleanup(func() {
			_ = srv.Close()
			_ = pc.Close()
		})

		tr := &http3.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}
		t.Cleanup(func() { _ = tr.Close() })

		s.addr = pc.LocalAddr().String()
		s.handshake = func(t testing.TB, path string, header http.Header) handshakeResult {
			pr, pw := io.Pipe()
			req, err := http.NewRequest(http.MethodConnect, "https://"+s.addr+path, pr)
			if err != nil {
				t.Fatal(err)
			}
			req.Proto = "websocket"
			for name, values := range header {
				req.Header[name] = values
			}
			req.Header.Set("Sec-WebSocket-Version", "13")

			return roundTripStream(t, tr, req, pw)
		}
	default:
		t.Fatalf("unknown protocol %q", proto)
	}

	t.Cleanup(func() { _ = h.Cleanup() })

	return s
}

func roundTripStream(t testing.TB, tr http.RoundTripper, req *http.Request, body *io.PipeWriter) handshakeResult {
	t.Helper()

	resp, err := tr.RoundTrip(req) //nolint:bodyclose // the body is the WebSocket stream, closed by client.close at cleanup
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}

	client := &wsClient{r: resp.Body, w: body, close: func() {
		_ = body.Close()
		_ = resp.Body.Close()
	}}
	t.Cleanup(client.close)

	res := handshakeResult{status: resp.StatusCode, header: resp.Header}
	if resp.StatusCode == http.StatusOK {
		res.client = client

		return res
	}

	refusal, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read refusal body: %v", err)
	}
	res.body = string(refusal)

	return res
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) {
	return f(p)
}

// dialHTTP2 speaks raw HTTP/2 frames, because the net/http client refuses
// the :protocol pseudo-header needed for an extended CONNECT.
func dialHTTP2(t testing.TB, addr, path string, header http.Header) handshakeResult {
	t.Helper()

	const streamID = 1

	tc, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h2"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tc.Close() })

	if _, err := io.WriteString(tc, http2.ClientPreface); err != nil {
		t.Fatal(err)
	}
	fr := http2.NewFramer(tc, tc)
	fr.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	if err := fr.WriteSettings(); err != nil {
		t.Fatal(err)
	}

	for {
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatalf("read settings: %v", err)
		}
		settings, ok := f.(*http2.SettingsFrame)
		if !ok || settings.IsAck() {
			continue
		}
		if v, ok := settings.Value(http2.SettingEnableConnectProtocol); !ok || v != 1 {
			t.Fatal("server does not announce extended CONNECT; run with GODEBUG=http2xconnect=1")
		}
		if err := fr.WriteSettingsAck(); err != nil {
			t.Fatal(err)
		}

		break
	}

	fields := []hpack.HeaderField{
		{Name: ":method", Value: http.MethodConnect},
		{Name: ":protocol", Value: "websocket"},
		{Name: ":scheme", Value: "https"},
		{Name: ":path", Value: path},
		{Name: ":authority", Value: addr},
		{Name: "sec-websocket-version", Value: "13"},
	}
	for name, values := range header {
		for _, value := range values {
			fields = append(fields, hpack.HeaderField{Name: strings.ToLower(name), Value: value})
		}
	}

	var block bytes.Buffer
	enc := hpack.NewEncoder(&block)
	for _, field := range fields {
		if err := enc.WriteField(field); err != nil {
			t.Fatal(err)
		}
	}
	if err := fr.WriteHeaders(http2.HeadersFrameParam{StreamID: streamID, BlockFragment: block.Bytes(), EndHeaders: true}); err != nil {
		t.Fatal(err)
	}

	var res handshakeResult
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		if headers, ok := f.(*http2.MetaHeadersFrame); ok && headers.StreamID == streamID {
			res.status, err = strconv.Atoi(headers.PseudoValue("status"))
			if err != nil {
				t.Fatalf("handshake status: %v", err)
			}
			res.header = make(http.Header)
			for _, field := range headers.RegularFields() {
				res.header.Add(field.Name, field.Value)
			}
			if headers.StreamEnded() {
				return res
			}

			break
		}
	}
	if res.status != http.StatusOK {
		for {
			f, err := fr.ReadFrame()
			if err != nil {
				t.Fatalf("read refusal body: %v", err)
			}
			if data, ok := f.(*http2.DataFrame); ok && data.StreamID == streamID {
				res.body += string(data.Data())
				if data.StreamEnded() {
					return res
				}
			}
		}
	}

	var writeMu sync.Mutex
	pr, pw := io.Pipe()

	go func() {
		for {
			f, err := fr.ReadFrame()
			if err != nil {
				_ = pw.CloseWithError(err)

				return
			}

			switch f := f.(type) {
			case *http2.DataFrame:
				if n := len(f.Data()); n > 0 {
					data := append([]byte(nil), f.Data()...)
					writeMu.Lock()
					_ = fr.WriteWindowUpdate(0, uint32(n))
					_ = fr.WriteWindowUpdate(streamID, uint32(n))
					writeMu.Unlock()
					if _, err := pw.Write(data); err != nil {
						return
					}
				}
				if f.StreamEnded() {
					_ = pw.Close()

					return
				}
			case *http2.RSTStreamFrame:
				_ = pw.CloseWithError(fmt.Errorf("stream reset: %v", f.ErrCode))

				return
			case *http2.GoAwayFrame:
				// A stream the GOAWAY still covers runs on until it ends (RFC 9113, section 6.8).
				if f.LastStreamID < streamID {
					_ = pw.CloseWithError(fmt.Errorf("goaway: %v", f.ErrCode))

					return
				}
			case *http2.PingFrame:
				if !f.IsAck() {
					writeMu.Lock()
					_ = fr.WritePing(true, f.Data)
					writeMu.Unlock()
				}
			case *http2.SettingsFrame:
				if !f.IsAck() {
					writeMu.Lock()
					_ = fr.WriteSettingsAck()
					writeMu.Unlock()
				}
			}
		}
	}()

	w := writerFunc(func(p []byte) (int, error) {
		writeMu.Lock()
		defer writeMu.Unlock()

		written := 0
		for written < len(p) {
			chunk := p[written:min(len(p), written+16384)]
			if err := fr.WriteData(streamID, false, chunk); err != nil {
				return written, err
			}
			written += len(chunk)
		}

		return written, nil
	})

	res.client = &wsClient{r: pr, w: w, close: func() { _ = tc.Close() }}

	return res
}

func dialHTTP1(t testing.TB, addr, path string, header http.Header) handshakeResult {
	t.Helper()

	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nc.Close() })

	key := make([]byte, 16)
	_, _ = rand.Read(key)

	req, err := http.NewRequest(http.MethodGet, "http://"+addr+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Key", base64.StdEncoding.EncodeToString(key))
	req.Header.Set("Sec-WebSocket-Version", "13")
	for name, values := range header {
		req.Header[name] = values
	}

	if err := req.Write(nc); err != nil {
		t.Fatal(err)
	}

	br := bufio.NewReader(nc)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	res := handshakeResult{status: resp.StatusCode, header: resp.Header}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read refusal body: %v", err)
		}
		res.body = string(body)

		return res
	}

	if got, want := resp.Header.Get("Sec-WebSocket-Accept"), acceptKey(req.Header.Get("Sec-WebSocket-Key")); got != want {
		t.Fatalf("Sec-WebSocket-Accept = %q, want %q", got, want)
	}
	res.client = &wsClient{r: br, w: nc, close: func() { _ = nc.Close() }}

	return res
}

func selfSignedCert(t testing.TB) (tls.Certificate, *x509.CertPool) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(leaf)

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, pool
}

func (c *wsClient) send(t testing.TB, op ws.OpCode, fin bool, payload []byte) {
	t.Helper()

	c.sendFrame(t, ws.MaskFrameInPlace(ws.NewFrame(op, fin, append([]byte(nil), payload...))))
}

func (c *wsClient) sendFrame(t testing.TB, frame ws.Frame) {
	t.Helper()

	if err := ws.WriteFrame(c.w, frame); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

func (c *wsClient) receive(t testing.TB) (ws.OpCode, []byte) {
	t.Helper()

	type result struct {
		op      ws.OpCode
		payload []byte
		err     error
	}
	ch := make(chan result, 1)
	go func() {
		header, err := ws.ReadHeader(c.r)
		if err != nil {
			ch <- result{err: err}

			return
		}
		payload := make([]byte, header.Length)
		_, err = io.ReadFull(c.r, payload)
		ch <- result{op: header.OpCode, payload: payload, err: err}
	}()

	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatalf("read frame: %v", res.err)
		}

		return res.op, res.payload
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a frame")

		return 0, nil
	}
}

func (c *wsClient) expectClose(t testing.TB, want ws.StatusCode) {
	t.Helper()

	op, payload := c.receive(t)
	if op != ws.OpClose {
		t.Fatalf("opcode = %v, want close", op)
	}
	if code, _ := ws.ParseCloseFrameData(payload); code != want {
		t.Fatalf("close status = %d, want %d", code, want)
	}
}

func waitForConnections(t testing.TB, h *Handler, want int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for h.connections.len() != want {
		if time.Now().After(deadline) {
			t.Fatalf("held connections = %d, want %d", h.connections.len(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func forEachProtocol(t *testing.T, test func(t *testing.T, s *testServer)) {
	forEachProtocolWith(t, func() frankenphp.Workers { return testWorkers }, nil, test)
}

func forEachProtocolConfigured(t *testing.T, configure func(*Handler), test func(t *testing.T, s *testServer)) {
	forEachProtocolWith(t, func() frankenphp.Workers { return testWorkers }, configure, test)
}

func forEachProtocolWith(t *testing.T, workers func() frankenphp.Workers, configure func(*Handler), test func(t *testing.T, s *testServer)) {
	for _, proto := range protocols {
		t.Run(proto, func(t *testing.T) {
			t.Parallel()
			test(t, startServerWith(t, proto, workers(), configure))
		})
	}
}

func TestConnectionIsHeldUntilClientCloses(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		first := s.dial(t)
		second := s.dial(t)
		waitForConnections(t, s.handler, 2)

		if s.nextCalled.Load() {
			t.Fatal("next handler was called for a WebSocket request")
		}

		first.send(t, ws.OpClose, true, ws.NewCloseFrameBody(ws.StatusNormalClosure, "bye"))
		first.expectClose(t, ws.StatusNormalClosure)
		waitForConnections(t, s.handler, 1)

		second.send(t, ws.OpClose, true, nil)
		if op, payload := second.receive(t); op != ws.OpClose || len(payload) != 0 {
			t.Fatalf("got opcode %v with %d bytes, want empty close", op, len(payload))
		}
		waitForConnections(t, s.handler, 0)
	})
}

func TestPingIsAnsweredWithPong(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		client := s.dial(t)

		client.send(t, ws.OpPing, true, []byte("hello"))

		op, payload := client.receive(t)
		if op != ws.OpPong || string(payload) != "hello" {
			t.Fatalf("got opcode %v payload %q, want pong %q", op, payload, "hello")
		}
	})
}

func TestFragmentedMessageIsAccepted(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		client := s.dial(t)

		client.send(t, ws.OpText, false, []byte("hel"))
		client.send(t, ws.OpPing, true, []byte("between"))
		client.send(t, ws.OpContinuation, true, []byte("lo"))
		client.send(t, ws.OpBinary, true, []byte{0xff})

		if op, payload := client.receive(t); op != ws.OpPong || string(payload) != "between" {
			t.Fatalf("got opcode %v payload %q, want pong %q", op, payload, "between")
		}
		if s.handler.connections.len() != 1 {
			t.Fatalf("held connections = %d, want 1", s.handler.connections.len())
		}
	})
}

func TestCleanupClosesHeldConnectionsAsGoingAway(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		client := s.dial(t)
		waitForConnections(t, s.handler, 1)

		cleaned := make(chan struct{})
		go func() {
			_ = s.handler.Cleanup()
			close(cleaned)
		}()

		client.expectClose(t, ws.StatusGoingAway)
		client.send(t, ws.OpClose, true, ws.NewCloseFrameBody(ws.StatusGoingAway, ""))

		select {
		case <-cleaned:
		case <-time.After(defaultCloseTimeout / 2):
			t.Fatal("cleanup did not finish after the client answered the close")
		}
		waitForConnections(t, s.handler, 0)
	})
}

func TestCleanupCutsUnresponsiveConnection(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		client := s.dial(t)
		waitForConnections(t, s.handler, 1)

		cleaned := make(chan struct{})
		go func() {
			_ = s.handler.Cleanup()
			close(cleaned)
		}()

		client.expectClose(t, ws.StatusGoingAway)

		select {
		case <-cleaned:
		case <-time.After(defaultCloseTimeout + 5*time.Second):
			t.Fatal("cleanup did not cut the unresponsive connection")
		}
		waitForConnections(t, s.handler, 0)
	})
}

func TestProtocolViolationsCloseTheConnection(t *testing.T) {
	tests := []struct {
		name string
		send func(t *testing.T, c *wsClient)
		want ws.StatusCode
	}{
		{
			name: "unmasked frame",
			send: func(t *testing.T, c *wsClient) {
				c.sendFrame(t, ws.NewTextFrame([]byte("x")))
			},
			want: ws.StatusProtocolError,
		},
		{
			name: "continuation without start",
			send: func(t *testing.T, c *wsClient) {
				c.send(t, ws.OpContinuation, true, []byte("x"))
			},
			want: ws.StatusProtocolError,
		},
		{
			name: "new message inside fragmented message",
			send: func(t *testing.T, c *wsClient) {
				c.send(t, ws.OpText, false, []byte("x"))
				c.send(t, ws.OpText, true, []byte("y"))
			},
			want: ws.StatusProtocolError,
		},
		{
			name: "message too big",
			send: func(t *testing.T, c *wsClient) {
				c.send(t, ws.OpBinary, false, make([]byte, defaultMaxMessageSize))
				c.send(t, ws.OpContinuation, true, []byte{0})
			},
			want: ws.StatusMessageTooBig,
		},
		{
			name: "invalid UTF-8 text",
			send: func(t *testing.T, c *wsClient) {
				c.send(t, ws.OpText, true, []byte{0xff, 0xfe})
			},
			want: ws.StatusInvalidFramePayloadData,
		},
		{
			name: "one-byte close payload",
			send: func(t *testing.T, c *wsClient) {
				c.send(t, ws.OpClose, true, []byte{0x03})
			},
			want: ws.StatusProtocolError,
		},
		{
			name: "reserved close code",
			send: func(t *testing.T, c *wsClient) {
				c.send(t, ws.OpClose, true, ws.NewCloseFrameBody(ws.StatusNoStatusRcvd, ""))
			},
			want: ws.StatusProtocolError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			forEachProtocol(t, func(t *testing.T, s *testServer) {
				client := s.dial(t)
				waitForConnections(t, s.handler, 1)

				tt.send(t, client)

				client.expectClose(t, tt.want)
				waitForConnections(t, s.handler, 0)
			})
		})
	}
}

func TestNonWebSocketRequestPassesToNext(t *testing.T) {
	s := startServer(t, "HTTP/1.1")

	resp, err := http.Get("http://" + s.addr)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	if !s.nextCalled.Load() {
		t.Fatal("next handler was not called")
	}
	if resp.StatusCode != http.StatusTeapot {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusTeapot)
	}
}

func TestHandshakeRejections(t *testing.T) {
	tests := []struct {
		name   string
		header http.Header
		want   int
	}{
		{name: "unsupported version", header: http.Header{"Sec-Websocket-Version": {"8"}}, want: http.StatusUpgradeRequired},
		{name: "invalid key", header: http.Header{"Sec-Websocket-Key": {"short"}}, want: http.StatusBadRequest},
		{name: "cross origin", header: http.Header{"Origin": {"https://evil.example"}}, want: http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := startServer(t, "HTTP/1.1")

			if res := dialHTTP1(t, s.addr, "/", tt.header); res.status != tt.want {
				t.Fatalf("status = %d, want %d", res.status, tt.want)
			}
			if s.handler.connections.len() != 0 {
				t.Fatalf("held connections = %d, want 0", s.handler.connections.len())
			}
		})
	}
}

func TestSameOriginIsAccepted(t *testing.T) {
	s := startServer(t, "HTTP/1.1")

	if res := dialHTTP1(t, s.addr, "/", http.Header{"Origin": {"http://" + s.addr}}); res.status != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", res.status)
	}
}

func TestConnectionAfterCleanupIsClosedAsGoingAway(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		if err := s.handler.Cleanup(); err != nil {
			t.Fatalf("cleanup: %v", err)
		}

		client := s.dial(t)
		client.expectClose(t, ws.StatusGoingAway)
		client.send(t, ws.OpClose, true, ws.NewCloseFrameBody(ws.StatusGoingAway, ""))

		if s.handler.connections.len() != 0 {
			t.Fatalf("held connections = %d, want 0", s.handler.connections.len())
		}
	})
}

func TestEventsReachTheWorkerAndRepliesReachTheConnection(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		client := s.dial(t)

		client.send(t, ws.OpText, true, []byte("hello"))
		if op, payload := client.receive(t); op != ws.OpText || string(payload) != "echo:hello" {
			t.Fatalf("got opcode %v payload %q, want text %q", op, payload, "echo:hello")
		}

		client.send(t, ws.OpBinary, true, []byte{0x01, 0xff})
		if op, payload := client.receive(t); op != ws.OpBinary || !bytes.Equal(payload, []byte{0xff, 0x01}) {
			t.Fatalf("got opcode %v payload %x, want binary ff01", op, payload)
		}

		client.send(t, ws.OpText, true, []byte("silent"))
		client.send(t, ws.OpPing, true, []byte("after silent"))
		if op, payload := client.receive(t); op != ws.OpPong || string(payload) != "after silent" {
			t.Fatalf("got opcode %v payload %q, want pong; a null reply must send nothing", op, payload)
		}

		client.send(t, ws.OpClose, true, ws.NewCloseFrameBody(ws.StatusNormalClosure, ""))
		client.expectClose(t, ws.StatusNormalClosure)
		waitForEvents(t, s.workers, 6)

		events := withoutContext(s.workers.recorded())
		id := events[0]["connection"]
		want := []map[string]any{
			{"event": "open", "connection": id},
			{"event": "message", "connection": id, "data": "hello", "binary": false},
			{"event": "message", "connection": id, "data": string([]byte{0x01, 0xff}), "binary": true},
			{"event": "message", "connection": id, "data": "silent", "binary": false},
			{"event": "ping", "connection": id, "data": "after silent"},
			{"event": "close", "connection": id, "code": int(ws.StatusNormalClosure), "reason": ""},
		}
		if id == "" || !slices.EqualFunc(events, want, maps.Equal) {
			t.Fatalf("events = %v, want %v", events, want)
		}
	})
}

func TestCloseEventReportsTheClosingStatus(t *testing.T) {
	tests := []struct {
		name  string
		close func(t *testing.T, s *testServer, c *wsClient)
		want  ws.StatusCode
	}{
		{
			name: "client close without status",
			close: func(t *testing.T, _ *testServer, c *wsClient) {
				c.send(t, ws.OpClose, true, nil)
				_, _ = c.receive(t)
			},
			want: ws.StatusNoStatusRcvd,
		},
		{
			name: "server shutdown",
			close: func(t *testing.T, s *testServer, c *wsClient) {
				go func() { _ = s.handler.Cleanup() }()
				c.expectClose(t, ws.StatusGoingAway)
				c.send(t, ws.OpClose, true, ws.NewCloseFrameBody(ws.StatusGoingAway, ""))
			},
			want: ws.StatusGoingAway,
		},
		{
			name: "protocol violation",
			close: func(t *testing.T, _ *testServer, c *wsClient) {
				c.sendFrame(t, ws.NewTextFrame([]byte("unmasked")))
				c.expectClose(t, ws.StatusProtocolError)
			},
			want: ws.StatusProtocolError,
		},
		{
			name: "connection dropped",
			close: func(t *testing.T, _ *testServer, c *wsClient) {
				c.close()
			},
			want: ws.StatusAbnormalClosure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			forEachProtocol(t, func(t *testing.T, s *testServer) {
				client := s.dial(t)
				waitForEvents(t, s.workers, 1)

				tt.close(t, s, client)
				waitForEvents(t, s.workers, 2)

				closeEvent := s.workers.recorded()[1]
				if closeEvent["event"] != "close" || closeEvent["code"] != int(tt.want) {
					t.Fatalf("event = %v, want close with code %d", closeEvent, tt.want)
				}
			})
		})
	}
}

func TestUnavailableWorkerFailsTheHandshake(t *testing.T) {
	notStarted := func() frankenphp.Workers {
		workers, _ := frankenphp.WithExtensionWorkers("websocket-not-started", "testdata/worker.php", 1)

		return workers
	}

	forEachProtocolWith(t, notStarted, nil, func(t *testing.T, s *testServer) {
		res := s.handshake(t, "/", nil)
		if res.client != nil || res.status != http.StatusInternalServerError {
			t.Fatalf("handshake status = %d, want 500", res.status)
		}
		if events := s.workers.recorded(); len(events) != 0 {
			t.Fatalf("events = %v, want none", events)
		}
	})
}

func TestHandshakeReachesPHPAsHTTPRequest(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		res := s.handshake(t, "/chat?room=1", http.Header{
			"X-Client":         {"test"},
			connectionIDHeader: {"forged"},
		})
		if res.client == nil {
			t.Fatalf("handshake status = %d, want it accepted", res.status)
		}

		if got, want := res.header.Get("X-Seen"), "GET /chat?room=1 test"; got != want {
			t.Fatalf("PHP saw %q, want %q", got, want)
		}
		if got, want := res.header.Get("X-Connection"), connectionID(t, s.workers, 0); got != want {
			t.Fatalf("PHP got connection id %q, want %q", got, want)
		}
		if got := res.header.Get("Set-Cookie"); !strings.HasPrefix(got, "session=abc") {
			t.Fatalf("Set-Cookie = %q, want the cookie PHP set", got)
		}
		if got := res.header.Get("Content-Type"); got != "" {
			t.Fatalf("Content-Type = %q on an accepted handshake, want none", got)
		}

		res.client.send(t, ws.OpText, true, []byte("hello"))
		if op, payload := res.client.receive(t); op != ws.OpText || string(payload) != "echo:hello" {
			t.Fatalf("got opcode %v payload %q, want text %q", op, payload, "echo:hello")
		}
	})
}

func TestPHPRefusesTheHandshake(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		res := s.handshake(t, "/chat?access=denied", nil)
		if res.client != nil || res.status != http.StatusForbidden {
			t.Fatalf("handshake status = %d, want 403", res.status)
		}
		if got := res.header.Get("X-Refused"); got != "yes" {
			t.Fatalf("X-Refused = %q, want the header PHP set", got)
		}
		if res.body != "access denied" {
			t.Fatalf("body = %q, want %q", res.body, "access denied")
		}
		if s.handler.connections.len() != 0 {
			t.Fatalf("held connections = %d, want 0", s.handler.connections.len())
		}
		if events := s.workers.recorded(); len(events) != 0 {
			t.Fatalf("events = %v, want none", events)
		}
	})
}

func waitForEvents(t testing.TB, workers *recordingWorkers, want int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for len(workers.recorded()) < want {
		if time.Now().After(deadline) {
			t.Fatalf("recorded events = %v, want %d", workers.recorded(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestProvisionRequiresWorker(t *testing.T) {
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	defer cancel()

	if err := new(Handler).Provision(ctx); err == nil {
		t.Fatal("expected an error without a worker script")
	}
}

func TestUnmarshalCaddyfile(t *testing.T) {
	tests := []struct {
		input   string
		want    Handler
		wantErr bool
	}{
		{input: "websocket {\n\tworker ws.php\n}", want: Handler{Worker: "ws.php"}},
		{input: "websocket {\n\tworker ws.php 4\n}", want: Handler{Worker: "ws.php", Num: 4}},
		{input: "websocket", wantErr: true},
		{input: "websocket unexpected", wantErr: true},
		{input: "websocket {\n\tworker\n}", wantErr: true},
		{input: "websocket {\n\tworker ws.php 0\n}", wantErr: true},
		{input: "websocket {\n\tworker ws.php many\n}", wantErr: true},
		{input: "websocket {\n\tworker ws.php 2 extra\n}", wantErr: true},
		{input: "websocket {\n\tunknown\n}", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			var got Handler
			err := got.UnmarshalCaddyfile(caddyfile.NewTestDispenser(tt.input))
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && (got.Worker != tt.want.Worker || got.Num != tt.want.Num) {
				t.Fatalf("got worker %q num %d, want worker %q num %d", got.Worker, got.Num, tt.want.Worker, tt.want.Num)
			}
		})
	}
}

func connectionID(t testing.TB, workers *recordingWorkers, index int) string {
	t.Helper()

	waitForEvents(t, workers, index+1)
	id, _ := workers.recorded()[index]["connection"].(string)
	if id == "" {
		t.Fatalf("event %d has no connection id: %v", index, workers.recorded()[index])
	}

	return id
}

func TestWorkerSendsToAnotherConnection(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		sender := s.dial(t)
		senderID := connectionID(t, s.workers, 0)
		receiver := s.dial(t)
		receiverID := connectionID(t, s.workers, 1)
		if senderID == receiverID {
			t.Fatal("both connections got the same id")
		}

		sender.send(t, ws.OpText, true, []byte("send-to:"+receiverID+":hello"))

		if op, payload := receiver.receive(t); op != ws.OpText || string(payload) != "hello" {
			t.Fatalf("receiver got opcode %v payload %q, want text %q", op, payload, "hello")
		}
		if op, payload := sender.receive(t); op != ws.OpText || string(payload) != "sent" {
			t.Fatalf("sender got opcode %v payload %q, want text %q", op, payload, "sent")
		}

		sender.send(t, ws.OpBinary, true, []byte("send-to:"+receiverID+":"+string([]byte{0xff})))
		if op, payload := receiver.receive(t); op != ws.OpBinary || !bytes.Equal(payload, []byte{0xff}) {
			t.Fatalf("receiver got opcode %v payload %x, want binary ff", op, payload)
		}
	})
}

func TestWorkerSendToUnknownConnectionFails(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		client := s.dial(t)

		client.send(t, ws.OpText, true, []byte("send-to:unknown:hello"))

		if op, payload := client.receive(t); op != ws.OpText || string(payload) != "not sent" {
			t.Fatalf("got opcode %v payload %q, want text %q", op, payload, "not sent")
		}
	})
}

func TestHTTPRequestSendsToConnection(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		client := s.dial(t)
		id := connectionID(t, s.workers, 0)

		if got := runPushScript(t, id, "from http"); got != "sent" {
			t.Fatalf("push.php printed %q, want %q", got, "sent")
		}
		if op, payload := client.receive(t); op != ws.OpText || string(payload) != "from http" {
			t.Fatalf("got opcode %v payload %q, want text %q", op, payload, "from http")
		}

		client.send(t, ws.OpClose, true, ws.NewCloseFrameBody(ws.StatusNormalClosure, ""))
		client.expectClose(t, ws.StatusNormalClosure)
		waitForConnections(t, s.handler, 0)

		if got := runPushScript(t, id, "too late"); got != "not sent" {
			t.Fatalf("push.php printed %q after close, want %q", got, "not sent")
		}
	})
}

func runPushScript(t testing.TB, connection, data string) string {
	t.Helper()

	root, err := filepath.Abs("testdata")
	if err != nil {
		t.Fatal(err)
	}

	query := url.Values{"connection": {connection}, "data": {data}}
	req, err := frankenphp.NewRequestWithContext(
		httptest.NewRequest(http.MethodGet, "/push.php?"+query.Encode(), nil),
		frankenphp.WithRequestDocumentRoot(root, false),
	)
	if err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	if err := frankenphp.ServeHTTP(rec, req); err != nil {
		t.Fatalf("serve push.php: %v", err)
	}

	return rec.Body.String()
}

func (c *wsClient) sendText(t testing.TB, data string) {
	t.Helper()

	c.send(t, ws.OpText, true, []byte(data))
}

func (c *wsClient) expectText(t testing.TB, want string) {
	t.Helper()

	if op, payload := c.receive(t); op != ws.OpText || string(payload) != want {
		t.Fatalf("got opcode %v payload %q, want text %q", op, payload, want)
	}
}

func (c *wsClient) expectCloseWithReason(t testing.TB, wantCode ws.StatusCode, wantReason string) {
	t.Helper()

	op, payload := c.receive(t)
	if op != ws.OpClose {
		t.Fatalf("opcode = %v, want close", op)
	}
	if code, reason := ws.ParseCloseFrameData(payload); code != wantCode || reason != wantReason {
		t.Fatalf("close = %d %q, want %d %q", code, reason, wantCode, wantReason)
	}
}

func lastEvent(t testing.TB, workers *recordingWorkers, name string) map[string]any {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		events := workers.recorded()
		for i := len(events) - 1; i >= 0; i-- {
			if events[i]["event"] == name {
				return events[i]
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %q event in %v", name, withoutContext(events))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPHPClosesTheConnection(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		client := s.dial(t)

		client.sendText(t, "close:4000:bye")
		client.expectCloseWithReason(t, 4000, "bye")
		client.send(t, ws.OpClose, true, ws.NewCloseFrameBody(4000, ""))

		closeEvent := lastEvent(t, s.workers, "close")
		if closeEvent["code"] != 4000 || closeEvent["reason"] != "bye" {
			t.Fatalf("close event = %v, want code 4000 reason bye", withoutContext([]map[string]any{closeEvent}))
		}
		waitForConnections(t, s.handler, 0)
	})
}

func TestPHPCloseRejectsInvalidArguments(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		client := s.dial(t)

		for _, command := range []string{"close:1005:", "close:999:", "close:5000:", "close:4000:" + strings.Repeat("x", 124)} {
			client.sendText(t, command)
			client.expectText(t, "error:3")
		}

		client.sendText(t, "hello")
		client.expectText(t, "echo:hello")
	})
}

func TestCloseEventCarriesTheClientReason(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		client := s.dial(t)

		client.send(t, ws.OpClose, true, ws.NewCloseFrameBody(4001, "gone"))
		client.expectClose(t, 4001)

		closeEvent := lastEvent(t, s.workers, "close")
		if closeEvent["code"] != 4001 || closeEvent["reason"] != "gone" {
			t.Fatalf("close event = %v, want code 4001 reason gone", withoutContext([]map[string]any{closeEvent}))
		}
	})
}

func TestPHPPingsAndReceivesThePong(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		client := s.dial(t)

		client.sendText(t, "ping:are you there")
		if op, payload := client.receive(t); op != ws.OpPing || string(payload) != "are you there" {
			t.Fatalf("got opcode %v payload %q, want ping", op, payload)
		}
		client.send(t, ws.OpPong, true, []byte("are you there"))

		if pong := lastEvent(t, s.workers, "pong"); pong["data"] != "are you there" {
			t.Fatalf("pong event data = %v", pong["data"])
		}

		client.sendText(t, "ping:"+strings.Repeat("x", 126))
		client.expectText(t, "error:3")
	})
}

func TestClientPingReachesPHP(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		client := s.dial(t)

		client.send(t, ws.OpPing, true, []byte("hi"))
		if op, _ := client.receive(t); op != ws.OpPong {
			t.Fatalf("opcode = %v, want pong", op)
		}

		if ping := lastEvent(t, s.workers, "ping"); ping["data"] != "hi" {
			t.Fatalf("ping event data = %v", ping["data"])
		}
	})
}

func TestPHPSeesWhetherAConnectionIsOpen(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		observer := s.dial(t)
		observed := s.dial(t)
		observedID := connectionID(t, s.workers, 1)

		observer.sendText(t, "status:"+observedID)
		observer.expectText(t, `{"open":true,"error":0,"listed":true}`)

		observed.send(t, ws.OpClose, true, ws.NewCloseFrameBody(ws.StatusNormalClosure, ""))
		observed.expectClose(t, ws.StatusNormalClosure)
		waitForConnections(t, s.handler, 1)

		observer.sendText(t, "status:"+observedID)
		observer.expectText(t, `{"open":false,"error":1,"listed":false}`)
	})
}

func TestPHPNamesTheInstanceOfAConnection(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		client := s.dial(t)
		id := connectionID(t, s.workers, 0)

		client.sendText(t, "instance:"+id)
		client.expectText(t, `["`+cluster.Local.ID()+`",0]`)

		client.sendText(t, "instance:NOSUCHINSTANCE.ABC")
		client.expectText(t, `["",`+strconv.Itoa(WEBSOCKET_ERROR_NOT_FOUND)+`]`)
	})
}

func TestPHPChoosesTheMessageType(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		client := s.dial(t)
		id := connectionID(t, s.workers, 0)

		client.sendText(t, "send-as:2:"+id+":as binary")
		if op, payload := client.receive(t); op != ws.OpBinary || string(payload) != "as binary" {
			t.Fatalf("got opcode %v payload %q, want binary", op, payload)
		}
		client.expectText(t, "ok")

		client.send(t, ws.OpBinary, true, []byte("send-as:1:"+id+":"+string([]byte{0xff})))
		client.expectText(t, "error:3")

		client.sendText(t, "send-as:9:"+id+":x")
		client.expectText(t, "error:3")

		client.sendText(t, "send-as:1:unknown:x")
		client.expectText(t, "error:1")
	})
}

func TestAttributesAndTagsReachTheEvents(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		res := s.handshake(t, "/chat?user=alice&room=7", http.Header{"X-Client": {"test"}})
		if res.client == nil {
			t.Fatalf("handshake status = %d, want it accepted", res.status)
		}
		client := res.client

		client.sendText(t, "set:color:blue")
		client.expectText(t, "ok")
		client.sendText(t, "tag:vip")
		client.expectText(t, "ok")

		client.sendText(t, "context")
		client.expectText(t, `{"attributes":{"color":"blue","user":"alice"},"tags":["room:7","vip"],`+
			`"method":"GET","uri":"\/chat?user=alice&room=7","client":"test","connection_header":"`+connectionID(t, s.workers, 0)+`"}`)

		client.sendText(t, "remove:color")
		client.expectText(t, "ok")
		client.sendText(t, "untag:vip")
		client.expectText(t, "ok")
		client.sendText(t, "attributes")
		client.expectText(t, `{"user":"alice"}`)
		client.sendText(t, "tags")
		client.expectText(t, `["room:7"]`)

		client.sendText(t, "set::empty")
		client.expectText(t, "error:3")
		client.sendText(t, "tag:")
		client.expectText(t, "error:3")
	})
}

func dialPath(t testing.TB, s *testServer, path string) *wsClient {
	t.Helper()

	res := s.handshake(t, path, nil)
	if res.client == nil {
		t.Fatalf("handshake status = %d, want it accepted", res.status)
	}

	return res.client
}

func TestSendToTag(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		// Tags are shared by every handler of the process, and the protocols run in parallel.
		room := rand.Text()

		first := dialPath(t, s, "/?room="+room)
		second := dialPath(t, s, "/?room="+room)
		other := dialPath(t, s, "/?room=other-"+room)
		waitForConnections(t, s.handler, 3)

		other.sendText(t, "to-tag:room:"+room+"|hello room")
		first.expectText(t, "hello room")
		second.expectText(t, "hello room")
		other.expectText(t, "2")

		other.sendText(t, "to-tag:room:nobody-"+room+"|nobody")
		other.expectText(t, "0")
	})
}

func TestConnectionsWithTag(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		// Tags are shared by every handler of the process, and the protocols run in parallel.
		room := rand.Text()

		dialPath(t, s, "/?room="+room)
		firstID := connectionID(t, s.workers, 0)
		dialPath(t, s, "/?room="+room)
		secondID := connectionID(t, s.workers, 1)
		other := dialPath(t, s, "/?room=other-"+room)
		waitForConnections(t, s.handler, 3)

		ids := slices.Sorted(slices.Values([]string{firstID, secondID}))
		other.sendText(t, "connections-with-tag:room:"+room)
		other.expectText(t, `["`+ids[0]+`","`+ids[1]+`"]`)

		other.sendText(t, "connections-with-tag:room:nobody-"+room)
		other.expectText(t, "[]")
	})
}

func TestBroadcast(t *testing.T) {
	// A broadcast reaches every connection of the process, so the protocols run one after another.
	for _, proto := range protocols {
		t.Run(proto, func(t *testing.T) {
			s := startServer(t, proto)

			first := s.dial(t)
			second := s.dial(t)
			waitForConnections(t, s.handler, 2)

			first.sendText(t, "broadcast:to all")
			first.expectText(t, "to all")
			second.expectText(t, "to all")
			first.expectText(t, "2")
		})
	}
}

func TestCrossOriginHandshakeCanBeLeftToPHP(t *testing.T) {
	s := startServerWith(t, "HTTP/1.1", testWorkers, func(h *Handler) { h.AllowCrossOrigin = true })

	res := dialHTTP1(t, s.addr, "/", http.Header{"Origin": {"https://other.example"}})
	if res.status != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", res.status)
	}
	if got := res.header.Get("X-Origin"); got != "https://other.example" {
		t.Fatalf("PHP saw origin %q, want https://other.example", got)
	}
}

func TestSubprotocolChosenByPHP(t *testing.T) {
	forEachProtocol(t, func(t *testing.T, s *testServer) {
		res := s.handshake(t, "/?protocol=chat", http.Header{"Sec-Websocket-Protocol": {"superchat, chat"}})
		if res.client == nil {
			t.Fatalf("handshake status = %d, want it accepted", res.status)
		}
		if got := res.header.Get("Sec-WebSocket-Protocol"); got != "chat" {
			t.Fatalf("Sec-WebSocket-Protocol = %q, want chat", got)
		}

		if res := s.handshake(t, "/?protocol=chat", http.Header{"Sec-Websocket-Protocol": {"other"}}); res.status != http.StatusInternalServerError {
			t.Fatalf("status = %d for a subprotocol the client did not offer, want 500", res.status)
		}
	})
}

func compressMessage(t testing.TB, data []byte) []byte {
	t.Helper()

	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	return bytes.TrimSuffix(buf.Bytes(), []byte{0x00, 0x00, 0xff, 0xff})
}

func (c *wsClient) sendCompressed(t testing.TB, op ws.OpCode, data []byte) {
	t.Helper()

	frame := ws.NewFrame(op, true, compressMessage(t, data))
	frame.Header.Rsv = ws.Rsv(true, false, false)
	c.sendFrame(t, ws.MaskFrameInPlace(frame))
}

func (c *wsClient) receiveCompressed(t testing.TB) (ws.OpCode, []byte) {
	t.Helper()

	header, err := ws.ReadHeader(c.r)
	if err != nil {
		t.Fatalf("read header: %v", err)
	}
	payload := make([]byte, header.Length)
	if _, err := io.ReadFull(c.r, payload); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if !header.Rsv1() {
		t.Fatalf("frame %v is not compressed", header.OpCode)
	}

	r := flate.NewReader(io.MultiReader(bytes.NewReader(payload), bytes.NewReader([]byte{0x00, 0x00, 0xff, 0xff})))
	data, err := io.ReadAll(r)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("inflate: %v", err)
	}

	return header.OpCode, data
}

func TestCompressionWhenPHPAndClientAgree(t *testing.T) {
	offer := http.Header{"Sec-Websocket-Extensions": {"permessage-deflate; client_max_window_bits"}}

	forEachProtocol(t, func(t *testing.T, s *testServer) {
		res := s.handshake(t, "/?compress=1", offer)
		if res.client == nil {
			t.Fatalf("handshake status = %d, want it accepted", res.status)
		}
		want := "permessage-deflate; server_no_context_takeover; client_no_context_takeover"
		if got := res.header.Get("Sec-WebSocket-Extensions"); got != want {
			t.Fatalf("Sec-WebSocket-Extensions = %q, want %q", got, want)
		}

		res.client.sendCompressed(t, ws.OpText, []byte("hello"))
		if op, data := res.client.receiveCompressed(t); op != ws.OpText || string(data) != "echo:hello" {
			t.Fatalf("got opcode %v data %q, want compressed echo", op, data)
		}

		res.client.sendText(t, "uncompressed")
		if op, data := res.client.receiveCompressed(t); op != ws.OpText || string(data) != "echo:uncompressed" {
			t.Fatalf("got opcode %v data %q, want compressed echo", op, data)
		}

		res.client.sendCompressed(t, ws.OpBinary, make([]byte, defaultMaxMessageSize+1))
		res.client.expectClose(t, ws.StatusMessageTooBig)
	})
}

func TestNoCompressionUnlessBothAgree(t *testing.T) {
	offer := http.Header{"Sec-Websocket-Extensions": {"permessage-deflate"}}

	forEachProtocol(t, func(t *testing.T, s *testServer) {
		withoutPHP := s.handshake(t, "/", offer)
		if got := withoutPHP.header.Get("Sec-WebSocket-Extensions"); withoutPHP.client == nil || got != "" {
			t.Fatalf("status %d, Sec-WebSocket-Extensions %q without PHP asking for it", withoutPHP.status, got)
		}
		withoutPHP.client.sendCompressed(t, ws.OpText, []byte("hello"))
		withoutPHP.client.expectClose(t, ws.StatusProtocolError)

		withoutClient := s.handshake(t, "/?compress=1", http.Header{"Sec-Websocket-Extensions": {"permessage-deflate; server_max_window_bits=10"}})
		if got := withoutClient.header.Get("Sec-WebSocket-Extensions"); withoutClient.client == nil || got != "" {
			t.Fatalf("status %d, Sec-WebSocket-Extensions %q for an offer that cannot be met", withoutClient.status, got)
		}
		withoutClient.client.sendText(t, "hello")
		withoutClient.client.expectText(t, "echo:hello")
	})
}

func TestNegotiateDeflate(t *testing.T) {
	base := "permessage-deflate; server_no_context_takeover; client_no_context_takeover"
	tests := []struct {
		offers []string
		want   string
		ok     bool
	}{
		{offers: []string{"permessage-deflate"}, want: base, ok: true},
		{offers: []string{"permessage-deflate; server_max_window_bits=15"}, want: base + "; server_max_window_bits=15", ok: true},
		{offers: []string{"permessage-deflate; server_max_window_bits=10, permessage-deflate"}, want: base, ok: true},
		{offers: []string{"x-webkit-deflate-frame", "permessage-deflate; client_max_window_bits=12"}, want: base, ok: true},
		{offers: []string{"permessage-deflate; server_max_window_bits=10"}},
		{offers: []string{"permessage-deflate; client_max_window_bits=20"}},
		{offers: []string{"permessage-deflate; unknown"}},
		{offers: []string{"permessage-deflate; server_no_context_takeover; server_no_context_takeover"}},
		{offers: []string{"other"}},
		{offers: nil},
	}

	for _, tt := range tests {
		got, ok := negotiateDeflate(tt.offers)
		if got != tt.want || ok != tt.ok {
			t.Errorf("negotiateDeflate(%q) = %q, %v; want %q, %v", tt.offers, got, ok, tt.want, tt.ok)
		}
	}
}

func TestMaxMessageSizeIsConfigurable(t *testing.T) {
	forEachProtocolConfigured(t, func(h *Handler) { h.MaxMessageSize = 10 }, func(t *testing.T, s *testServer) {
		client := s.dial(t)

		client.sendText(t, "0123456789")
		client.expectText(t, "echo:0123456789")

		client.sendText(t, "0123456789a")
		client.expectClose(t, ws.StatusMessageTooBig)
	})
}

func TestIdleConnectionIsClosed(t *testing.T) {
	forEachProtocolConfigured(t, func(h *Handler) {
		h.IdleTimeout = caddy.Duration(300 * time.Millisecond)
		h.PingInterval = -1
	}, func(t *testing.T, s *testServer) {
		client := s.dial(t)

		client.expectCloseWithReason(t, ws.StatusGoingAway, "idle timeout")

		closeEvent := lastEvent(t, s.workers, "close")
		if closeEvent["code"] != int(ws.StatusGoingAway) || closeEvent["reason"] != "idle timeout" {
			t.Fatalf("close event = %v, want 1001 idle timeout", withoutContext([]map[string]any{closeEvent}))
		}
	})
}

func TestActivityKeepsAnIdleTimeoutAway(t *testing.T) {
	forEachProtocolConfigured(t, func(h *Handler) {
		h.IdleTimeout = caddy.Duration(400 * time.Millisecond)
		h.PingInterval = -1
	}, func(t *testing.T, s *testServer) {
		client := s.dial(t)

		for range 4 {
			time.Sleep(200 * time.Millisecond)
			client.sendText(t, "silent")
		}

		client.sendText(t, "still here")
		client.expectText(t, "echo:still here")
	})
}

func TestKeepalivePingsDoNotReachPHP(t *testing.T) {
	forEachProtocolConfigured(t, func(h *Handler) { h.PingInterval = caddy.Duration(100 * time.Millisecond) }, func(t *testing.T, s *testServer) {
		client := s.dial(t)

		for range 2 {
			op, payload := client.receive(t)
			if op != ws.OpPing {
				t.Fatalf("opcode = %v, want a keepalive ping", op)
			}
			client.send(t, ws.OpPong, true, payload)
		}

		client.sendText(t, "hello")
		for {
			op, payload := client.receive(t)
			if op == ws.OpText {
				if string(payload) != "echo:hello" {
					t.Fatalf("payload = %q, want echo:hello", payload)
				}

				break
			}
		}

		for _, event := range s.workers.recorded() {
			if event["event"] == "pong" {
				t.Fatalf("keepalive pong reached PHP: %v", withoutContext([]map[string]any{event}))
			}
		}
	})
}

func TestAnsweredKeepalivePingsKeepAnIdleTimeoutAway(t *testing.T) {
	forEachProtocolConfigured(t, func(h *Handler) {
		h.IdleTimeout = caddy.Duration(300 * time.Millisecond)
		h.PingInterval = caddy.Duration(100 * time.Millisecond)
	}, func(t *testing.T, s *testServer) {
		client := s.dial(t)

		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			op, payload := client.receive(t)
			if op != ws.OpPing {
				t.Fatalf("opcode = %v, want a keepalive ping", op)
			}
			client.send(t, ws.OpPong, true, payload)
		}

		client.sendText(t, "still here")
		for {
			op, payload := client.receive(t)
			if op == ws.OpText {
				if string(payload) != "echo:still here" {
					t.Fatalf("payload = %q, want echo:still here", payload)
				}

				return
			}
		}
	})
}

func TestKeepaliveAndIdleTimeoutAreOnByDefault(t *testing.T) {
	h := new(Handler)
	h.setup(zaptest.NewLogger(t), nil)
	if h.limits.idleTimeout != defaultIdleTimeout || h.limits.pingInterval != defaultPingInterval {
		t.Fatalf("idle timeout %v, ping interval %v, want %v and %v", h.limits.idleTimeout, h.limits.pingInterval, defaultIdleTimeout, defaultPingInterval)
	}

	off := &Handler{IdleTimeout: -1, PingInterval: -1}
	off.setup(zaptest.NewLogger(t), nil)
	if off.limits.idleTimeout != 0 || off.limits.pingInterval != 0 {
		t.Fatalf("idle timeout %v, ping interval %v, want both off", off.limits.idleTimeout, off.limits.pingInterval)
	}
}

func TestIdleTimeoutMustOutlastPingInterval(t *testing.T) {
	for _, h := range []Handler{
		{IdleTimeout: caddy.Duration(10 * time.Second)},
		{IdleTimeout: caddy.Duration(20 * time.Second), PingInterval: caddy.Duration(20 * time.Second)},
		{PingInterval: caddy.Duration(2 * time.Minute)},
	} {
		if err := h.checkKeepalive(); err == nil {
			t.Errorf("idle_timeout %v with ping_interval %v: expected an error", time.Duration(h.IdleTimeout), time.Duration(h.PingInterval))
		}
	}

	for _, h := range []Handler{
		{},
		{IdleTimeout: caddy.Duration(10 * time.Second), PingInterval: -1},
		{IdleTimeout: -1, PingInterval: caddy.Duration(2 * time.Minute)},
		{IdleTimeout: caddy.Duration(20 * time.Second), PingInterval: caddy.Duration(5 * time.Second)},
	} {
		if err := h.checkKeepalive(); err != nil {
			t.Errorf("idle_timeout %v with ping_interval %v: %v", time.Duration(h.IdleTimeout), time.Duration(h.PingInterval), err)
		}
	}

	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	defer cancel()
	invalid := &Handler{Worker: "ws.php", IdleTimeout: caddy.Duration(time.Second)}
	if err := invalid.Provision(ctx); err == nil || !strings.Contains(err.Error(), "ping_interval") {
		t.Fatalf("Provision = %v, want the idle timeout refused", err)
	}
}

func TestCloseTimeoutIsConfigurable(t *testing.T) {
	forEachProtocolConfigured(t, func(h *Handler) { h.CloseTimeout = caddy.Duration(300 * time.Millisecond) }, func(t *testing.T, s *testServer) {
		client := s.dial(t)
		waitForConnections(t, s.handler, 1)

		started := time.Now()
		if err := s.handler.Cleanup(); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(started); elapsed > 2*time.Second {
			t.Fatalf("cleanup took %v with a 300ms close timeout", elapsed)
		}
		client.expectClose(t, ws.StatusGoingAway)
	})
}

// slowCloseWorkers takes a while over every close event, as a PHP close
// handler writing to a database does, and counts those it finished.
type slowCloseWorkers struct {
	frankenphp.Workers

	finished atomic.Int32
}

func (w *slowCloseWorkers) SendMessage(ctx context.Context, message any, rw http.ResponseWriter) (any, error) {
	reply, err := w.Workers.SendMessage(ctx, message, rw)
	if event, ok := message.(frankenphp.AssociativeArray[any]); ok && event.Map["event"] == "close" {
		time.Sleep(200 * time.Millisecond)
		w.finished.Add(1)
	}

	return reply, err
}

func TestShutdownWaitsForTheCloseEvents(t *testing.T) {
	for _, proto := range protocols {
		t.Run(proto, func(t *testing.T) {
			t.Parallel()

			workers := &slowCloseWorkers{Workers: testWorkers}
			s := startServerWith(t, proto, workers, func(h *Handler) { h.CloseTimeout = caddy.Duration(300 * time.Millisecond) })
			first, second := s.dial(t), s.dial(t)
			waitForConnections(t, s.handler, 2)

			s.handler.shutdown()
			if finished := workers.finished.Load(); finished != 2 {
				t.Fatalf("shutdown returned after %d of 2 close events", finished)
			}
			for _, client := range []*wsClient{first, second} {
				client.expectCloseWithReason(t, ws.StatusGoingAway, "server shutting down")
			}
			opens := len(slices.DeleteFunc(s.workers.recorded(), func(e map[string]any) bool { return e["event"] != "open" }))
			if res := s.handshake(t, "/", nil); res.client != nil {
				res.client.expectCloseWithReason(t, ws.StatusGoingAway, "server shutting down")
			}
			if after := len(slices.DeleteFunc(s.workers.recorded(), func(e map[string]any) bool { return e["event"] != "open" })); after != opens {
				t.Fatal("a connection opened after shutdown")
			}
		})
	}
}

// TestServerShutdownClosesStreams shuts down an HTTP/2 server with a
// WebSocket stream open: the server waits for its streams, so it would wait
// for the connection unless its shutdown closes it.
func TestServerShutdownClosesStreams(t *testing.T) {
	recorder := &recordingWorkers{Workers: testWorkers}
	h, handler, _ := newTestHandler(t, recorder, func(h *Handler) { h.CloseTimeout = caddy.Duration(300 * time.Millisecond) })
	srv := httptest.NewUnstartedServer(handler)
	srv.EnableHTTP2 = true
	srv.Config.RegisterOnShutdown(h.shutdown)
	srv.StartTLS()
	t.Cleanup(srv.Close)

	res := dialHTTP2(t, srv.Listener.Addr().String(), "/", nil)
	if res.client == nil {
		t.Fatalf("handshake status = %d, want it accepted", res.status)
	}
	waitForConnections(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Config.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	res.client.expectCloseWithReason(t, ws.StatusGoingAway, "server shutting down")
	if closeEvent := lastEvent(t, recorder, "close"); closeEvent["code"] != int(ws.StatusGoingAway) {
		t.Fatalf("close event = %v, want 1001", withoutContext([]map[string]any{closeEvent}))
	}
}

func TestUnmarshalCaddyfileOptions(t *testing.T) {
	input := `websocket {
	worker ws.php 2
	allow_cross_origin
	max_message_size 1MiB
	write_timeout 3s
	close_timeout 2s
	idle_timeout 1m
	ping_interval 20s
}`

	var got Handler
	if err := got.UnmarshalCaddyfile(caddyfile.NewTestDispenser(input)); err != nil {
		t.Fatal(err)
	}

	want := Handler{
		Worker:           "ws.php",
		Num:              2,
		AllowCrossOrigin: true,
		MaxMessageSize:   1 << 20,
		WriteTimeout:     caddy.Duration(3 * time.Second),
		CloseTimeout:     caddy.Duration(2 * time.Second),
		IdleTimeout:      caddy.Duration(time.Minute),
		PingInterval:     caddy.Duration(20 * time.Second),
	}
	if got.Worker != want.Worker || got.Num != want.Num || got.AllowCrossOrigin != want.AllowCrossOrigin ||
		got.MaxMessageSize != want.MaxMessageSize || got.WriteTimeout != want.WriteTimeout ||
		got.CloseTimeout != want.CloseTimeout || got.IdleTimeout != want.IdleTimeout || got.PingInterval != want.PingInterval {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	var off Handler
	if err := off.UnmarshalCaddyfile(caddyfile.NewTestDispenser("websocket {\n\tworker ws.php\n\tidle_timeout off\n\tping_interval off\n}")); err != nil {
		t.Fatal(err)
	}
	if off.IdleTimeout >= 0 || off.PingInterval >= 0 {
		t.Fatalf("idle_timeout %v, ping_interval %v, want both off", off.IdleTimeout, off.PingInterval)
	}

	for _, invalid := range []string{
		"websocket {\n\tworker ws.php\n\tallow_cross_origin yes\n}",
		"websocket {\n\tworker ws.php\n\tmax_message_size lots\n}",
		"websocket {\n\tworker ws.php\n\tmax_message_size 0\n}",
		"websocket {\n\tworker ws.php\n\tidle_timeout\n}",
		"websocket {\n\tworker ws.php\n\tidle_timeout -1s\n}",
		"websocket {\n\tworker ws.php\n\tping_interval soon\n}",
		"websocket {\n\tworker ws.php\n\twrite_timeout off\n}",
		"websocket {\n\tworker ws.php\n\tidle_timeout off 1s\n}",
	} {
		if err := new(Handler).UnmarshalCaddyfile(caddyfile.NewTestDispenser(invalid)); err == nil {
			t.Errorf("expected an error for %q", invalid)
		}
	}
}

func TestMaxConnectionsRefusesBeyondTheCap(t *testing.T) {
	forEachProtocolConfigured(t, func(h *Handler) { h.MaxConnections = 1 }, func(t *testing.T, s *testServer) {
		first := s.dial(t)

		if res := s.handshake(t, "/", nil); res.status != http.StatusServiceUnavailable {
			t.Fatalf("second handshake status = %d, want 503", res.status)
		}
		if events := withoutContext(s.workers.recorded()); len(events) != 1 {
			t.Fatalf("events = %v, want only the first connection's open", events)
		}

		first.send(t, ws.OpClose, true, ws.NewCloseFrameBody(ws.StatusNormalClosure, ""))
		first.expectClose(t, ws.StatusNormalClosure)
		waitForConnections(t, s.handler, 0)

		deadline := time.Now().Add(5 * time.Second)
		for s.handshake(t, "/", nil).client == nil {
			if time.Now().After(deadline) {
				t.Fatal("the slot of the closed connection was never given back")
			}
			time.Sleep(20 * time.Millisecond)
		}
	})
}

func TestMaxConnectionsPerClientRefusesBeyondTheCap(t *testing.T) {
	forEachProtocolConfigured(t, func(h *Handler) { h.MaxConnectionsPerClient = 2 }, func(t *testing.T, s *testServer) {
		s.dial(t)
		s.dial(t)

		if res := s.handshake(t, "/", nil); res.status != http.StatusTooManyRequests {
			t.Fatalf("third handshake status = %d, want 429", res.status)
		}
	})
}

func TestRefusedHandshakeGivesItsSlotBack(t *testing.T) {
	forEachProtocolConfigured(t, func(h *Handler) { h.MaxConnections = 1 }, func(t *testing.T, s *testServer) {
		for range 3 {
			if res := s.handshake(t, "/?access=denied", nil); res.status != http.StatusForbidden {
				t.Fatalf("handshake status = %d, want 403 from PHP", res.status)
			}
		}

		s.dial(t)
	})
}

func TestMessageRateLimit(t *testing.T) {
	forEachProtocolConfigured(t, func(h *Handler) {
		h.MessageRate = 3
		h.MessageRateInterval = caddy.Duration(time.Second)
	}, func(t *testing.T, s *testServer) {
		client := s.dial(t)

		for range 3 {
			client.sendText(t, "silent")
		}
		client.sendText(t, "one too many")

		client.expectCloseWithReason(t, ws.StatusPolicyViolation, "message rate exceeded")
	})
}

func TestMessageRateRefills(t *testing.T) {
	forEachProtocolConfigured(t, func(h *Handler) {
		h.MessageRate = 2
		h.MessageRateInterval = caddy.Duration(200 * time.Millisecond)
	}, func(t *testing.T, s *testServer) {
		client := s.dial(t)

		for range 6 {
			client.sendText(t, "silent")
			time.Sleep(150 * time.Millisecond)
		}

		client.sendText(t, "still allowed")
		client.expectText(t, "echo:still allowed")
	})
}

func TestWriteTimeoutCutsAConnectionThatDoesNotRead(t *testing.T) {
	const writeTimeout = 300 * time.Millisecond

	forEachProtocolConfigured(t, func(h *Handler) { h.WriteTimeout = caddy.Duration(writeTimeout) }, func(t *testing.T, s *testServer) {
		s.dial(t)
		id := connectionID(t, s.workers, 0)

		// The client never reads, so the transport's buffers fill up and a write blocks.
		payload := make([]byte, 256<<10)
		var code int64
		var elapsed time.Duration
		for range 400 {
			started := time.Now()
			code = local.send(id, false, payload)
			elapsed = time.Since(started)
			if code != WEBSOCKET_ERROR_NONE {
				break
			}
		}

		if code != WEBSOCKET_ERROR_WRITE_FAILED {
			t.Fatalf("send = %d, want a failed write once the buffers are full", code)
		}
		if elapsed < writeTimeout/2 || elapsed > writeTimeout+2*time.Second {
			t.Fatalf("the failing write took %v, want about the %v write timeout", elapsed, writeTimeout)
		}

		closeEvent := lastEvent(t, s.workers, "close")
		if closeEvent["code"] != int(ws.StatusAbnormalClosure) {
			t.Fatalf("close event = %v, want 1006", withoutContext([]map[string]any{closeEvent}))
		}
	})
}

func TestUnmarshalCaddyfileLimits(t *testing.T) {
	input := `websocket {
	worker ws.php
	max_connections 100
	max_connections_per_client 5
	message_rate 20 10s
}`

	var got Handler
	if err := got.UnmarshalCaddyfile(caddyfile.NewTestDispenser(input)); err != nil {
		t.Fatal(err)
	}
	if got.MaxConnections != 100 || got.MaxConnectionsPerClient != 5 || got.MessageRate != 20 || got.MessageRateInterval != caddy.Duration(10*time.Second) {
		t.Fatalf("got %+v", got)
	}

	var withDefaultInterval Handler
	if err := withDefaultInterval.UnmarshalCaddyfile(caddyfile.NewTestDispenser("websocket {\n\tworker ws.php\n\tmessage_rate 5\n}")); err != nil {
		t.Fatal(err)
	}
	if withDefaultInterval.MessageRate != 5 || withDefaultInterval.MessageRateInterval != 0 {
		t.Fatalf("got %+v", withDefaultInterval)
	}

	for _, invalid := range []string{
		"websocket {\n\tworker ws.php\n\tmax_connections 0\n}",
		"websocket {\n\tworker ws.php\n\tmax_connections_per_client many\n}",
		"websocket {\n\tworker ws.php\n\tmessage_rate\n}",
		"websocket {\n\tworker ws.php\n\tmessage_rate 0\n}",
		"websocket {\n\tworker ws.php\n\tmessage_rate 5 never\n}",
		"websocket {\n\tworker ws.php\n\tmessage_rate 5 1s extra\n}",
	} {
		if err := new(Handler).UnmarshalCaddyfile(caddyfile.NewTestDispenser(invalid)); err == nil {
			t.Errorf("expected an error for %q", invalid)
		}
	}
}
