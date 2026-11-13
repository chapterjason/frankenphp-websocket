package frankenphpwebsocket

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/dunglas/frankenphp"
	frankenphpcaddy "github.com/dunglas/frankenphp/caddy"
	"github.com/dustin/go-humanize"
	"github.com/gobwas/ws"
	"go.uber.org/zap"
)

func init() {
	caddy.RegisterModule(Handler{})
	httpcaddyfile.RegisterHandlerDirective("websocket", parseCaddyfile)
	httpcaddyfile.RegisterDirectiveOrder("websocket", httpcaddyfile.Before, "reverse_proxy")
}

type Handler struct {
	// Worker is the PHP worker script that handles the handshakes and receives the connection events.
	Worker string `json:"worker,omitempty"`
	// Num is the number of worker threads. Default: FrankenPHP's default for workers.
	Num int `json:"num,omitempty"`
	// AllowCrossOrigin passes cross-origin handshakes to PHP instead of refusing them; PHP must then check the Origin header.
	AllowCrossOrigin bool `json:"allow_cross_origin,omitempty"`
	// MaxMessageSize is the largest message accepted, in bytes, after decompression. Default: 32 KiB.
	MaxMessageSize int64 `json:"max_message_size,omitempty"`
	// WriteTimeout bounds every write to a connection. Default: 10s.
	WriteTimeout caddy.Duration `json:"write_timeout,omitempty"`
	// CloseTimeout is how long the peer has to answer a close before the connection is cut. Default: 5s.
	CloseTimeout caddy.Duration `json:"close_timeout,omitempty"`
	// IdleTimeout closes a connection that received nothing for that long. Default: none.
	IdleTimeout caddy.Duration `json:"idle_timeout,omitempty"`
	// PingInterval sends a ping to every connection at that interval. Default: none.
	PingInterval caddy.Duration `json:"ping_interval,omitempty"`
	// MaxConnections caps the connections of this handler, handshakes included; more are refused with 503. Default: none.
	MaxConnections int `json:"max_connections,omitempty"`
	// MaxConnectionsPerClient caps the connections of one client IP, as Caddy determines it; more are refused with 429. Default: none.
	MaxConnectionsPerClient int `json:"max_connections_per_client,omitempty"`
	// MessageRate is how many frames a connection may send per MessageRateInterval, in bursts of up to MessageRate; a faster connection is closed with 1008. Default: none.
	MessageRate int `json:"message_rate,omitempty"`
	// MessageRateInterval is the interval of MessageRate. Default: 1s.
	MessageRateInterval caddy.Duration `json:"message_rate_interval,omitempty"`

	logger      *zap.Logger
	workers     frankenphp.Workers
	node        *node
	connections *registry
	limits      limits
}

func (Handler) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.websocket",
		New: func() caddy.Module { return new(Handler) },
	}
}

func (h *Handler) Provision(ctx caddy.Context) error {
	if h.Worker == "" {
		return errors.New("websocket: a worker script is required")
	}

	// Loading the app makes FrankenPHP start, and with it the registered workers.
	if _, err := ctx.App("frankenphp"); err != nil {
		return fmt.Errorf("websocket: loading frankenphp app: %w", err)
	}

	h.setup(ctx.Logger(), frankenphpcaddy.RegisterWorkers("websocket:"+h.Worker, h.Worker, h.Num))

	return nil
}

func (h *Handler) setup(logger *zap.Logger, workers frankenphp.Workers) {
	h.logger = logger
	h.workers = workers
	if h.node == nil {
		h.node = local
	}
	h.connections = newRegistry(h.MaxConnections, h.MaxConnectionsPerClient)
	h.limits = limits{
		maxMessageSize:      orDefault(h.MaxMessageSize, defaultMaxMessageSize),
		writeTimeout:        orDefault(time.Duration(h.WriteTimeout), defaultWriteTimeout),
		closeTimeout:        orDefault(time.Duration(h.CloseTimeout), defaultCloseTimeout),
		idleTimeout:         time.Duration(h.IdleTimeout),
		pingInterval:        time.Duration(h.PingInterval),
		messageRate:         h.MessageRate,
		messageRateInterval: orDefault(time.Duration(h.MessageRateInterval), time.Second),
	}
}

// clientAddress is the client IP as Caddy determines it, which respects
// trusted proxies, or the peer address outside Caddy.
func clientAddress(r *http.Request) string {
	if ip, ok := caddyhttp.GetVar(r.Context(), caddyhttp.ClientIPVarKey).(string); ok && ip != "" {
		return ip
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}

	return host
}

func orDefault[T int | int64 | time.Duration](value, fallback T) T {
	if value > 0 {
		return value
	}

	return fallback
}

func (h *Handler) Cleanup() error {
	h.connections.closeAll(ws.StatusGoingAway, "server shutting down")

	return nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	if !isWebSocketRequest(r) {
		return next.ServeHTTP(w, r)
	}

	if rej := checkHandshake(w, r, h.AllowCrossOrigin); rej != nil {
		return caddyhttp.Error(rej.status, rej)
	}

	client := clientAddress(r)
	if err := h.connections.reserve(client); err != nil {
		if errors.Is(err, errTooManyClientConnections) {
			return caddyhttp.Error(http.StatusTooManyRequests, err)
		}

		return caddyhttp.Error(http.StatusServiceUnavailable, err)
	}
	defer h.connections.release(client)

	id := h.node.connectionID()
	phpRequest := phpHandshakeRequest(r, id)

	// PHP can address the connection from the handshake on, so the session
	// exists before PHP sees the request and lasts until the close event ran.
	s := newSession(id, phpRequest)
	h.node.sessions.add(s)
	defer h.node.sessions.remove(id)

	php := newHandshakeResponse(w.Header())
	if err := h.workers.SendRequest(php, phpRequest); err != nil {
		return caddyhttp.Error(http.StatusInternalServerError, fmt.Errorf("websocket handshake request: %w", err))
	}
	if !php.accepted() {
		if err := php.writeTo(w); err != nil {
			h.logger.Debug("websocket handshake refusal not delivered", zap.Error(err))
		}

		return nil
	}

	header := php.acceptedHeader()
	if err := checkSubprotocol(r, header.Get("Sec-WebSocket-Protocol")); err != nil {
		h.logger.Error("websocket handshake response invalid", zap.Error(err))

		return caddyhttp.Error(http.StatusInternalServerError, err)
	}

	compress := false
	if wantsDeflate(php.header.Values("Sec-WebSocket-Extensions")) {
		if extension, ok := negotiateDeflate(r.Header.Values("Sec-WebSocket-Extensions")); ok {
			header.Set("Sec-WebSocket-Extensions", extension)
			compress = true
		}
	}

	var (
		c         *conn
		committed bool
		err       error
	)
	if r.ProtoMajor == 1 {
		c, committed, err = acceptHTTP1(w, r, header, h.limits, compress)
	} else {
		c, err = acceptStream(w, r, header, h.limits, compress)
		committed = true
	}
	if err != nil {
		if !committed {
			return caddyhttp.Error(http.StatusInternalServerError, err)
		}

		// Caddy's error handling could no longer write a response.
		h.logger.Debug("websocket handshake failed", zap.Error(err))

		return nil
	}

	if !h.connections.add(id, c) {
		go c.close(ws.StatusGoingAway, "server shutting down")
		c.serve(nil)

		return nil
	}
	defer h.connections.remove(id)

	s.attach(c)

	logger := h.logger.With(zap.String("id", id))
	logger.Debug("websocket connection opened", zap.Int("http_version", r.ProtoMajor), zap.Bool("compression", compress))

	ctx := r.Context()

	if err := h.dispatch(ctx, logger, c, s, "open"); err != nil {
		go c.close(ws.StatusInternalServerError, "")
		c.serve(nil)

		return nil
	}

	c.serve(&frameHandlers{
		message: func(text bool, data []byte) error {
			return h.dispatch(ctx, logger, c, s, "message", "data", string(data), "binary", !text)
		},
		ping: func(payload []byte) error {
			return h.dispatch(ctx, logger, c, s, "ping", "data", string(payload))
		},
		pong: func(payload []byte) error {
			return h.dispatch(ctx, logger, c, s, "pong", "data", string(payload))
		},
	})

	code, reason := c.closeStatus()
	logger.Debug("websocket connection closed", zap.Int("status", int(code)), zap.String("reason", reason))

	// The close event must reach PHP even when the request context ended with the connection.
	_ = h.dispatch(context.WithoutCancel(ctx), logger, nil, s, "close", "code", int(code), "reason", reason)

	return nil
}

// dispatch sends an event to the worker and its reply to c; a nil c ignores
// the reply.
func (h *Handler) dispatch(ctx context.Context, logger *zap.Logger, c *conn, s *session, name string, fields ...any) error {
	keysAndValues := append([]any{"event", name, "connection", s.id}, fields...)
	keysAndValues = append(keysAndValues,
		"attributes", s.attributesArray(),
		"tags", h.node.sessions.tagsArray(s.id),
		"request", s.request,
	)

	reply, err := h.workers.SendMessage(ctx, newEvent(keysAndValues...), nil)
	if err != nil {
		logger.Error("websocket event failed", zap.String("event", name), zap.Error(err))

		return err
	}

	if c != nil {
		h.reply(logger, c, reply)
	}

	return nil
}

// reply sends the worker's return value to the connection: a string is sent
// as a text message when it is valid UTF-8, as a binary message otherwise;
// null sends nothing. A reply that cannot be sent, because PHP closed the
// connection or the write failed and ended it, does not fail the connection.
func (h *Handler) reply(logger *zap.Logger, c *conn, value any) {
	switch v := value.(type) {
	case nil:
	case string:
		if err := c.send(utf8.ValidString(v), []byte(v)); err != nil {
			logger.Debug("websocket reply not sent", zap.Error(err))
		}
	default:
		logger.Error("websocket worker reply ignored: expected string or null", zap.String("go_type", fmt.Sprintf("%T", v)))
	}
}

func newEvent(keysAndValues ...any) frankenphp.AssociativeArray[any] {
	event := frankenphp.AssociativeArray[any]{
		Map:   make(map[string]any, len(keysAndValues)/2),
		Order: make([]string, 0, len(keysAndValues)/2),
	}
	for i := 0; i < len(keysAndValues); i += 2 {
		key := keysAndValues[i].(string)
		event.Map[key] = keysAndValues[i+1]
		event.Order = append(event.Order, key)
	}

	return event
}

// UnmarshalCaddyfile parses:
//
//	websocket [<matcher>] {
//		worker <file> [<num>]
//		allow_cross_origin
//		max_message_size <size>
//		write_timeout <duration>
//		close_timeout <duration>
//		idle_timeout <duration>
//		ping_interval <duration>
//		max_connections <count>
//		max_connections_per_client <count>
//		message_rate <count> [<interval>]
//	}
func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next()

	if d.NextArg() {
		return d.ArgErr()
	}

	for d.NextBlock(0) {
		switch d.Val() {
		case "worker":
			if !d.NextArg() {
				return d.ArgErr()
			}
			h.Worker = d.Val()

			if d.NextArg() {
				num, err := strconv.Atoi(d.Val())
				if err != nil || num < 1 {
					return d.Errf("invalid worker thread count '%s'", d.Val())
				}
				h.Num = num
			}
		case "allow_cross_origin":
			h.AllowCrossOrigin = true
		case "max_message_size":
			if !d.NextArg() {
				return d.ArgErr()
			}
			size, err := humanize.ParseBytes(d.Val())
			if err != nil || size == 0 || size > 1<<40 {
				return d.Errf("invalid max_message_size '%s'", d.Val())
			}
			h.MaxMessageSize = int64(size)
		case "max_connections", "max_connections_per_client":
			name := d.Val()
			if !d.NextArg() {
				return d.ArgErr()
			}
			count, err := strconv.Atoi(d.Val())
			if err != nil || count < 1 {
				return d.Errf("invalid %s '%s'", name, d.Val())
			}
			if name == "max_connections" {
				h.MaxConnections = count
			} else {
				h.MaxConnectionsPerClient = count
			}
		case "message_rate":
			if !d.NextArg() {
				return d.ArgErr()
			}
			count, err := strconv.Atoi(d.Val())
			if err != nil || count < 1 {
				return d.Errf("invalid message_rate count '%s'", d.Val())
			}
			h.MessageRate = count

			if d.NextArg() {
				interval, err := caddy.ParseDuration(d.Val())
				if err != nil || interval <= 0 {
					return d.Errf("invalid message_rate interval '%s'", d.Val())
				}
				h.MessageRateInterval = caddy.Duration(interval)
			}
		case "write_timeout", "close_timeout", "idle_timeout", "ping_interval":
			name := d.Val()
			if !d.NextArg() {
				return d.ArgErr()
			}
			duration, err := caddy.ParseDuration(d.Val())
			if err != nil || duration <= 0 {
				return d.Errf("invalid %s '%s'", name, d.Val())
			}
			*h.durationField(name) = caddy.Duration(duration)
		default:
			return d.Errf("unrecognized subdirective '%s'", d.Val())
		}

		if d.NextArg() {
			return d.ArgErr()
		}
	}

	if h.Worker == "" {
		return d.Err("missing worker subdirective")
	}

	return nil
}

func (h *Handler) durationField(name string) *caddy.Duration {
	switch name {
	case "write_timeout":
		return &h.WriteTimeout
	case "close_timeout":
		return &h.CloseTimeout
	case "idle_timeout":
		return &h.IdleTimeout
	default:
		return &h.PingInterval
	}
}

func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	handler := new(Handler)
	err := handler.UnmarshalCaddyfile(h.Dispenser)

	return handler, err
}

var (
	_ caddy.Provisioner           = (*Handler)(nil)
	_ caddy.CleanerUpper          = (*Handler)(nil)
	_ caddyhttp.MiddlewareHandler = (*Handler)(nil)
	_ caddyfile.Unmarshaler       = (*Handler)(nil)
)
