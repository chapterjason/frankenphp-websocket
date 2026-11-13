package frankenphpwebsocket

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/nats-io/nats.go"
)

const (
	defaultNATSSubject = "frankenphp.websocket"
	natsFlushTimeout   = 5 * time.Second
)

func init() {
	caddy.RegisterModule(NATSBus{})
}

// NATSBus connects instances through a NATS server: every instance subscribes
// to <subject>.instance.<instance> and to <subject>.all.
type NATSBus struct {
	// URL lists the NATS servers, comma-separated. Default: nats://127.0.0.1:4222.
	URL string `json:"url,omitempty"`
	// Subject prefixes the subjects of the bus, so several clusters can share a NATS server. Default: frankenphp.websocket.
	Subject string `json:"subject,omitempty"`
	// Credentials is a JWT credentials file (.creds).
	Credentials string `json:"credentials,omitempty"`
	// NKey is a file holding an NKey user seed.
	NKey string `json:"nkey,omitempty"`
	// Token authenticates with a token.
	Token string `json:"token,omitempty"`
	// Username and Password authenticate with a user and password.
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	// TLS connects with TLS.
	TLS *NATSTLS `json:"tls,omitempty"`

	options  []nats.Option
	conn     *nats.Conn
	instance string
}

// NATSTLS configures the TLS connection to NATS. Without CA, the system's
// trusted roots verify the server.
type NATSTLS struct {
	// CA is a PEM file of the certificate authorities that verify the server.
	CA string `json:"ca,omitempty"`
	// Cert and Key are PEM files of the client certificate, for servers that verify clients.
	Cert string `json:"cert,omitempty"`
	Key  string `json:"key,omitempty"`
	// ServerName is the name to verify the server certificate against. Default: the host of the URL.
	ServerName string `json:"server_name,omitempty"`
}

func (NATSBus) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "websocket.bus.nats",
		New: func() caddy.Module { return new(NATSBus) },
	}
}

func (n *NATSBus) Provision(caddy.Context) error {
	repl := caddy.NewReplacer()
	for _, field := range []*string{&n.URL, &n.Subject, &n.Credentials, &n.NKey, &n.Token, &n.Username, &n.Password} {
		*field = repl.ReplaceKnown(*field, "")
	}

	if n.URL == "" {
		n.URL = nats.DefaultURL
	}
	if n.Subject == "" {
		n.Subject = defaultNATSSubject
	}

	options, err := n.connectOptions()
	if err != nil {
		return fmt.Errorf("websocket nats bus: %w", err)
	}
	n.options = options

	return nil
}

func (n *NATSBus) connectOptions() ([]nats.Option, error) {
	var (
		options []nats.Option
		methods int
	)

	if n.Credentials != "" {
		methods++
		options = append(options, nats.UserCredentials(n.Credentials))
	}
	if n.NKey != "" {
		methods++
		option, err := nats.NkeyOptionFromSeed(n.NKey)
		if err != nil {
			return nil, fmt.Errorf("nkey: %w", err)
		}
		options = append(options, option)
	}
	if n.Token != "" {
		methods++
		options = append(options, nats.Token(n.Token))
	}
	if n.Username != "" || n.Password != "" {
		methods++
		if n.Username == "" {
			return nil, errors.New("a password needs a user")
		}
		options = append(options, nats.UserInfo(n.Username, n.Password))
	}
	if methods > 1 {
		return nil, errors.New("use only one of credentials, nkey, token and user")
	}

	if n.TLS != nil {
		config, err := n.TLS.config()
		if err != nil {
			return nil, fmt.Errorf("tls: %w", err)
		}
		options = append(options, nats.Secure(config))
	}

	return options, nil
}

func (t *NATSTLS) config() (*tls.Config, error) {
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: t.ServerName}

	if t.CA != "" {
		pem, err := os.ReadFile(t.CA)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificate in %s", t.CA)
		}
		config.RootCAs = pool
	}

	if (t.Cert == "") != (t.Key == "") {
		return nil, errors.New("cert and key go together")
	}
	if t.Cert != "" {
		certificate, err := tls.LoadX509KeyPair(t.Cert, t.Key)
		if err != nil {
			return nil, err
		}
		config.Certificates = []tls.Certificate{certificate}
	}

	return config, nil
}

func (n *NATSBus) instanceSubject(instance string) string {
	return n.Subject + ".instance." + instance
}

func (n *NATSBus) allSubject() string {
	return n.Subject + ".all"
}

func (n *NATSBus) Open(instance string, deliver func(payload []byte)) error {
	if n.Subject == "" {
		return errors.New("websocket nats bus: not provisioned")
	}

	options := append([]nats.Option{
		nats.Name("frankenphp-websocket " + instance),
		nats.MaxReconnects(-1),
	}, n.options...)

	conn, err := nats.Connect(n.URL, options...)
	if err != nil {
		return err
	}

	handler := func(msg *nats.Msg) { deliver(msg.Data) }
	for _, subject := range []string{n.instanceSubject(instance), n.allSubject()} {
		if _, err := conn.Subscribe(subject, handler); err != nil {
			conn.Close()

			return err
		}
	}

	// The subscriptions must reach the server before the instance can be addressed.
	if err := conn.FlushTimeout(natsFlushTimeout); err != nil {
		conn.Close()

		return err
	}

	n.conn = conn
	n.instance = instance

	return nil
}

func (n *NATSBus) Publish(instance string, payload []byte) error {
	subject := n.allSubject()
	if instance != "" {
		subject = n.instanceSubject(instance)
	}

	return n.conn.Publish(subject, payload)
}

func (n *NATSBus) Close() error {
	if n.conn == nil {
		return nil
	}

	err := n.conn.FlushTimeout(natsFlushTimeout)
	n.conn.Close()

	return err
}

// UnmarshalCaddyfile parses:
//
//	bus nats [<url>] {
//		subject <prefix>
//		credentials <file>
//		nkey <seed file>
//		token <token>
//		user <name> <password>
//		tls {
//			ca <file>
//			cert <file>
//			key <file>
//			server_name <name>
//		}
//	}
func (n *NATSBus) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next()

	if d.NextArg() {
		n.URL = d.Val()
	}
	if d.NextArg() {
		return d.ArgErr()
	}

	for d.NextBlock(0) {
		switch option := d.Val(); option {
		case "subject", "credentials", "nkey", "token":
			value, err := singleArg(d)
			if err != nil {
				return err
			}
			*map[string]*string{"subject": &n.Subject, "credentials": &n.Credentials, "nkey": &n.NKey, "token": &n.Token}[option] = value
		case "user":
			if !d.NextArg() {
				return d.ArgErr()
			}
			n.Username = d.Val()
			if !d.NextArg() {
				return d.ArgErr()
			}
			n.Password = d.Val()
			if d.NextArg() {
				return d.ArgErr()
			}
		case "tls":
			if d.NextArg() {
				return d.ArgErr()
			}
			n.TLS = new(NATSTLS)
			for nesting := d.Nesting(); d.NextBlock(nesting); {
				tlsOption := d.Val()
				fields := map[string]*string{"ca": &n.TLS.CA, "cert": &n.TLS.Cert, "key": &n.TLS.Key, "server_name": &n.TLS.ServerName}
				field, ok := fields[tlsOption]
				if !ok {
					return d.Errf("unrecognized nats tls option '%s'", tlsOption)
				}
				value, err := singleArg(d)
				if err != nil {
					return err
				}
				*field = value
			}
		default:
			return d.Errf("unrecognized nats bus option '%s'", option)
		}
	}

	return nil
}

func singleArg(d *caddyfile.Dispenser) (string, error) {
	if !d.NextArg() {
		return "", d.ArgErr()
	}
	value := d.Val()
	if d.NextArg() {
		return "", d.ArgErr()
	}

	return value, nil
}

var (
	_ Bus                   = (*NATSBus)(nil)
	_ caddy.Provisioner     = (*NATSBus)(nil)
	_ caddyfile.Unmarshaler = (*NATSBus)(nil)
)
