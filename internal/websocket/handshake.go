package websocket

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/http/httpguts"
)

// acceptGUID is the fixed GUID from RFC 6455, section 1.3.
const acceptGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// connectionIDHeader carries the connection ID into the PHP handshake request,
// where it reads as $_SERVER['HTTP_WEBSOCKET_CONNECTION'].
const connectionIDHeader = "Websocket-Connection"

// handshakeOwnedHeaders are set by the WebSocket handshake itself or describe a
// response body, which an accepted handshake does not have.
var handshakeOwnedHeaders = []string{
	"Connection",
	"Content-Encoding",
	"Content-Length",
	"Content-Type",
	"Keep-Alive",
	"Sec-WebSocket-Accept",
	"Sec-WebSocket-Extensions",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

type rejection struct {
	status int
	reason string
}

func (r *rejection) Error() string {
	return r.reason
}

// isWebSocketRequest recognises the HTTP/1.1 upgrade (RFC 6455) and the
// extended CONNECT of HTTP/2 (RFC 8441) and HTTP/3 (RFC 9220).
func isWebSocketRequest(r *http.Request) bool {
	switch r.ProtoMajor {
	case 1:
		return httpguts.HeaderValuesContainsToken(r.Header["Connection"], "upgrade") &&
			httpguts.HeaderValuesContainsToken(r.Header["Upgrade"], "websocket")
	case 2:
		return r.Method == http.MethodConnect && strings.EqualFold(r.Header.Get(":protocol"), "websocket")
	case 3:
		return r.Method == http.MethodConnect && strings.EqualFold(r.Proto, "websocket")
	}

	return false
}

func checkHandshake(w http.ResponseWriter, r *http.Request, allowCrossOrigin bool) *rejection {
	if r.ProtoMajor == 1 && r.Method != http.MethodGet {
		return &rejection{http.StatusMethodNotAllowed, "websocket upgrade requires GET"}
	}

	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		w.Header().Set("Sec-WebSocket-Version", "13")

		return &rejection{http.StatusUpgradeRequired, "unsupported Sec-WebSocket-Version"}
	}

	if r.ProtoMajor == 1 {
		key, err := base64.StdEncoding.DecodeString(r.Header.Get("Sec-WebSocket-Key"))
		if err != nil || len(key) != 16 {
			return &rejection{http.StatusBadRequest, "invalid Sec-WebSocket-Key"}
		}
	}

	if !allowCrossOrigin && !sameOrigin(r) {
		return &rejection{http.StatusForbidden, "cross-origin websocket request"}
	}

	return nil
}

// checkSubprotocol makes sure PHP chose at most one of the subprotocols the
// client offered (RFC 6455, section 4.2.2).
func checkSubprotocol(r *http.Request, chosen string) error {
	if chosen == "" {
		return nil
	}

	for _, header := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, offered := range strings.Split(header, ",") {
			if strings.TrimSpace(offered) == chosen {
				return nil
			}
		}
	}

	return fmt.Errorf("subprotocol %q chosen by PHP was not offered by the client", chosen)
}

func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}

	u, err := url.Parse(origin)
	if err != nil {
		return false
	}

	return strings.EqualFold(u.Host, r.Host)
}

// phpHandshakeRequest is the request PHP handles for the handshake: a plain
// GET on every HTTP version, without the WebSocket stream as its body.
func phpHandshakeRequest(r *http.Request, id string) *http.Request {
	req := r.Clone(r.Context())
	req.Header.Del(":protocol")
	req.Header.Set(connectionIDHeader, id)

	if req.Method == http.MethodConnect {
		req.Method = http.MethodGet
	}
	if req.ProtoMajor == 3 {
		req.Proto = "HTTP/3.0"
	}

	req.Body = http.NoBody
	req.ContentLength = 0

	return req
}

// handshakeResponse buffers PHP's response to the handshake request. A 2xx
// status accepts the connection; any other is sent to the client as is.
type handshakeResponse struct {
	header http.Header
	status int
	body   bytes.Buffer
}

// newHandshakeResponse starts from the headers already set on the client
// response, as PHP would see them when writing to it directly.
func newHandshakeResponse(base http.Header) *handshakeResponse {
	return &handshakeResponse{header: base.Clone()}
}

func (resp *handshakeResponse) Header() http.Header {
	return resp.header
}

func (resp *handshakeResponse) WriteHeader(status int) {
	// Informational responses such as 103 Early Hints are not the final status.
	if status < 200 || resp.status != 0 {
		return
	}

	resp.status = status
}

func (resp *handshakeResponse) Write(p []byte) (int, error) {
	resp.WriteHeader(http.StatusOK)

	return resp.body.Write(p)
}

func (resp *handshakeResponse) Flush() {}

func (resp *handshakeResponse) accepted() bool {
	return resp.status == 0 || resp.status >= 200 && resp.status < 300
}

func (resp *handshakeResponse) writeTo(w http.ResponseWriter) error {
	replaceHeader(w.Header(), resp.header)
	w.WriteHeader(resp.status)
	_, err := w.Write(resp.body.Bytes())

	return err
}

// acceptedHeader is the part of PHP's response that goes on the accepted
// handshake response.
func (resp *handshakeResponse) acceptedHeader() http.Header {
	header := resp.header.Clone()
	for _, name := range handshakeOwnedHeaders {
		header.Del(name)
	}

	return header
}

func replaceHeader(dst, src http.Header) {
	clear(dst)
	for name, values := range src {
		dst[name] = values
	}
}

// acceptHTTP1 hijacks the connection. Once hijacked is true, the caller can
// no longer send an HTTP response, even on error.
func acceptHTTP1(w http.ResponseWriter, r *http.Request, header http.Header, l limits, compress bool) (c *conn, hijacked bool, err error) {
	netConn, brw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		return nil, false, err
	}

	// The connection now outlives the HTTP request, so request timeouts no longer apply.
	if err := netConn.SetDeadline(time.Time{}); err != nil {
		_ = netConn.Close()

		return nil, true, err
	}

	_, err = fmt.Fprintf(brw,
		"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n",
		acceptKey(r.Header.Get("Sec-WebSocket-Key")),
	)
	if err == nil {
		err = header.Write(brw)
	}
	if err == nil {
		_, err = brw.WriteString("\r\n")
	}
	if err == nil {
		err = brw.Flush()
	}
	if err != nil {
		_ = netConn.Close()

		return nil, true, err
	}

	return newConn(transport{
		src:              brw.Reader,
		dst:              brw.Writer,
		flush:            brw.Flush,
		setReadDeadline:  netConn.SetReadDeadline,
		setWriteDeadline: netConn.SetWriteDeadline,
		shutdown:         netConn.Close,
	}, l, compress), true, nil
}

// acceptStream answers an extended CONNECT; the response is committed even
// when an error is returned.
func acceptStream(w http.ResponseWriter, r *http.Request, header http.Header, l limits, compress bool) (*conn, error) {
	rc := http.NewResponseController(w)

	replaceHeader(w.Header(), header)
	w.WriteHeader(http.StatusOK)
	if err := rc.Flush(); err != nil {
		return nil, err
	}

	return newConn(transport{
		src:              r.Body,
		dst:              w,
		flush:            rc.Flush,
		setReadDeadline:  rc.SetReadDeadline,
		setWriteDeadline: rc.SetWriteDeadline,
		shutdown:         r.Body.Close,
	}, l, compress), nil
}

func acceptKey(key string) string {
	sum := sha1.Sum([]byte(key + acceptGUID))

	return base64.StdEncoding.EncodeToString(sum[:])
}
