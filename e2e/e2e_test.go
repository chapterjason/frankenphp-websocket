//go:build e2e

// Package e2e tests the Docker image end to end: the stack of compose.yaml
// must be running, see "make e2e".
package e2e

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/quic-go/quic-go/http3"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

var (
	websocketAddr = envOr("E2E_APP_ADDR", "localhost:18443")
	otherAddr     = envOr("E2E_OTHER_ADDR", "localhost:19443")
	// The stack uses Caddy's internal CA, which the host does not trust.
	insecure = &tls.Config{InsecureSkipVerify: true}
)

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}

type client struct {
	r     io.Reader
	w     io.Writer
	close func()
}

type handshake struct {
	client *client
	status int
	header http.Header
	body   string
}

type dialer func(t *testing.T, path string, header http.Header) handshake

var protocols = map[string]dialer{
	"HTTP/1.1": dialHTTP1,
	"HTTP/2":   dialHTTP2,
	"HTTP/3":   dialHTTP3,
}

func TestMain(m *testing.M) {
	deadline := time.Now().Add(2 * time.Minute)
	for _, addr := range []string{websocketAddr, otherAddr} {
		for {
			conn, err := tls.Dial("tcp", addr, insecure)
			if err == nil {
				_ = conn.Close()

				break
			}
			if time.Now().After(deadline) {
				fmt.Fprintln(os.Stderr, "the stack is not up:", err)
				os.Exit(1)
			}
			time.Sleep(time.Second)
		}
	}

	os.Exit(m.Run())
}

func dialHTTP1(t *testing.T, path string, header http.Header) handshake {
	t.Helper()

	c := insecure.Clone()
	c.NextProtos = []string{"http/1.1"}
	conn, err := tls.Dial("tcp", websocketAddr, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	key := make([]byte, 16)
	_, _ = rand.Read(key)

	req, err := http.NewRequest(http.MethodGet, "https://"+websocketAddr+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, values := range header {
		req.Header[name] = values
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", base64.StdEncoding.EncodeToString(key))
	if err := req.Write(conn); err != nil {
		t.Fatal(err)
	}

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	result := handshake{status: resp.StatusCode, header: resp.Header}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		body, _ := io.ReadAll(resp.Body)
		result.body = string(body)

		return result
	}
	result.client = &client{r: br, w: conn, close: func() { _ = conn.Close() }}

	return result
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// dialHTTP2 speaks raw HTTP/2 frames, because net/http's client refuses the
// :protocol pseudo-header of an extended CONNECT.
func dialHTTP2(t *testing.T, path string, header http.Header) handshake {
	t.Helper()

	const streamID = 1

	c := insecure.Clone()
	c.NextProtos = []string{"h2"}
	conn, err := tls.Dial("tcp", websocketAddr, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
		t.Fatal(err)
	}
	fr := http2.NewFramer(conn, conn)
	fr.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	if err := fr.WriteSettings(); err != nil {
		t.Fatal(err)
	}

	for {
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		if settings, ok := f.(*http2.SettingsFrame); ok && !settings.IsAck() {
			if v, ok := settings.Value(http2.SettingEnableConnectProtocol); !ok || v != 1 {
				t.Fatal("the server does not announce extended CONNECT; is GODEBUG=http2xconnect=1 set?")
			}
			if err := fr.WriteSettingsAck(); err != nil {
				t.Fatal(err)
			}

			break
		}
	}

	host, _, _ := net.SplitHostPort(websocketAddr)
	fields := []hpack.HeaderField{
		{Name: ":method", Value: http.MethodConnect},
		{Name: ":protocol", Value: "websocket"},
		{Name: ":scheme", Value: "https"},
		{Name: ":path", Value: path},
		{Name: ":authority", Value: host},
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

	var result handshake
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		headers, ok := f.(*http2.MetaHeadersFrame)
		if !ok || headers.StreamID != streamID {
			continue
		}

		result.status, _ = strconv.Atoi(headers.PseudoValue("status"))
		result.header = make(http.Header)
		for _, field := range headers.RegularFields() {
			result.header.Add(field.Name, field.Value)
		}
		if result.status == http.StatusOK {
			break
		}
		if headers.StreamEnded() {
			return result
		}

		for {
			f, err := fr.ReadFrame()
			if err != nil {
				t.Fatal(err)
			}
			if data, ok := f.(*http2.DataFrame); ok && data.StreamID == streamID {
				result.body += string(data.Data())
				if data.StreamEnded() {
					return result
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
			case *http2.PingFrame:
				if !f.IsAck() {
					writeMu.Lock()
					_ = fr.WritePing(true, f.Data)
					writeMu.Unlock()
				}
			}
		}
	}()

	w := writerFunc(func(p []byte) (int, error) {
		writeMu.Lock()
		defer writeMu.Unlock()

		for written := 0; written < len(p); {
			chunk := p[written:min(len(p), written+16384)]
			if err := fr.WriteData(streamID, false, chunk); err != nil {
				return written, err
			}
			written += len(chunk)
		}

		return len(p), nil
	})
	result.client = &client{r: pr, w: w, close: func() { _ = conn.Close() }}

	return result
}

func dialHTTP3(t *testing.T, path string, header http.Header) handshake {
	t.Helper()

	tr := &http3.Transport{TLSClientConfig: insecure}
	t.Cleanup(func() { _ = tr.Close() })

	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodConnect, "https://"+websocketAddr+path, pr)
	if err != nil {
		t.Fatal(err)
	}
	req.Proto = "websocket"
	for name, values := range header {
		req.Header[name] = values
	}
	req.Header.Set("Sec-WebSocket-Version", "13")

	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	result := handshake{status: resp.StatusCode, header: resp.Header}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		result.body = string(body)

		return result
	}
	result.client = &client{r: resp.Body, w: pw, close: func() {
		_ = pw.Close()
		_ = resp.Body.Close()
	}}

	return result
}

func (c *client) send(t *testing.T, op ws.OpCode, payload []byte) {
	t.Helper()

	if err := ws.WriteFrame(c.w, ws.MaskFrameInPlace(ws.NewFrame(op, true, append([]byte(nil), payload...)))); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func (c *client) receive(t *testing.T) (ws.OpCode, []byte) {
	t.Helper()

	type frame struct {
		op      ws.OpCode
		payload []byte
		err     error
	}
	ch := make(chan frame, 1)
	go func() {
		header, err := ws.ReadHeader(c.r)
		if err != nil {
			ch <- frame{err: err}

			return
		}
		payload := make([]byte, header.Length)
		_, err = io.ReadFull(c.r, payload)
		ch <- frame{op: header.OpCode, payload: payload, err: err}
	}()

	select {
	case f := <-ch:
		if f.err != nil {
			t.Fatalf("read: %v", f.err)
		}

		return f.op, f.payload
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for a frame")

		return 0, nil
	}
}

type opened struct {
	Connection string `json:"connection"`
	Protocol   string `json:"protocol"`
}

// open accepts the handshake and reads the worker's reply to the open event.
func open(t *testing.T, dial dialer, path string, header http.Header) (*client, handshake, opened) {
	t.Helper()

	result := dial(t, path, header)
	if result.client == nil {
		t.Fatalf("handshake status = %d, want it accepted", result.status)
	}

	op, payload := result.client.receive(t)
	var reply opened
	if op != ws.OpText || json.Unmarshal(payload, &reply) != nil || reply.Connection == "" {
		t.Fatalf("got opcode %v payload %q, want the open reply", op, payload)
	}

	return result.client, result, reply
}

func TestHandshakeEchoAndClose(t *testing.T) {
	for name, dial := range protocols {
		t.Run(name, func(t *testing.T) {
			wantProtocol := map[string]string{"HTTP/1.1": "HTTP/1.1", "HTTP/2": "HTTP/2.0", "HTTP/3": "HTTP/3.0"}[name]

			c, result, reply := open(t, dial, "/ws?room=1", nil)
			if got, want := result.header.Get("X-Seen"), "GET /ws?room=1 "+wantProtocol; got != want {
				t.Fatalf("PHP saw %q, want %q", got, want)
			}
			if reply.Protocol != wantProtocol {
				t.Fatalf("protocol = %q, want %q", reply.Protocol, wantProtocol)
			}

			c.send(t, ws.OpText, []byte("hello"))
			if op, payload := c.receive(t); op != ws.OpText || string(payload) != "echo:hello" {
				t.Fatalf("got opcode %v payload %q, want the echo", op, payload)
			}

			c.send(t, ws.OpClose, ws.NewCloseFrameBody(ws.StatusNormalClosure, ""))
			op, payload := c.receive(t)
			if code, _ := ws.ParseCloseFrameData(payload); op != ws.OpClose || code != ws.StatusNormalClosure {
				t.Fatalf("got opcode %v close %d, want close 1000", op, code)
			}
		})
	}
}

func TestPHPRefusesTheHandshake(t *testing.T) {
	for name, dial := range protocols {
		t.Run(name, func(t *testing.T) {
			result := dial(t, "/ws?access=denied", nil)
			if result.client != nil || result.status != http.StatusForbidden {
				t.Fatalf("status = %d, want 403", result.status)
			}
			if result.body != "access denied" {
				t.Fatalf("body = %q, want PHP's", result.body)
			}
		})
	}
}

func get(t *testing.T, path string, query url.Values, into any) {
	t.Helper()

	httpClient := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: insecure}}
	resp, err := httpClient.Get("https://" + otherAddr + path + "?" + query.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || json.Unmarshal(body, into) != nil {
		t.Fatalf("%s: status %d, body %q", path, resp.StatusCode, body)
	}
}

func TestTheOtherInstanceReachesTheConnection(t *testing.T) {
	for name, dial := range protocols {
		t.Run(name, func(t *testing.T) {
			c, _, reply := open(t, dial, "/ws", nil)

			var status struct {
				Open       bool              `json:"open"`
				Attributes map[string]string `json:"attributes"`
				Listed     bool              `json:"listed"`
			}
			get(t, "/status.php", url.Values{"connection": {reply.Connection}}, &status)
			if !status.Open || !status.Listed || status.Attributes["protocol"] != reply.Protocol {
				t.Fatalf("status from the other instance = %+v", status)
			}

			var push struct {
				Sent  bool `json:"sent"`
				Error int  `json:"error"`
			}
			get(t, "/push.php", url.Values{"connection": {reply.Connection}, "data": {"from the other instance"}}, &push)
			if !push.Sent {
				t.Fatalf("push = %+v", push)
			}
			if op, payload := c.receive(t); op != ws.OpText || string(payload) != "from the other instance" {
				t.Fatalf("got opcode %v payload %q, want the push", op, payload)
			}
		})
	}
}
