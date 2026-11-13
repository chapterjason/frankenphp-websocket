package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"go.uber.org/zap"
)

func init() {
	caddy.RegisterModule(App{})
	caddy.RegisterModule(UplinkApp{})
	httpcaddyfile.RegisterGlobalOption("cluster", parseClusterOption)
	httpcaddyfile.RegisterGlobalOption("uplink", parseUplinkOption)
}

// App is what this server is in a cluster: a mesh member, a relay, or
// both, and how it takes part as a member. A relay only joins members that
// cannot reach each other directly.
type App struct {
	Mesh   *Mesh   `json:"mesh,omitempty"`
	Relay  *Relay  `json:"relay,omitempty"`
	Member *Member `json:"member,omitempty"`

	ctx    caddy.Context
	logger *zap.Logger
	relay  *RelayServer
	bus    *Bus
}

// Member tunes how this server takes part as a member, for its WebSockets and
// its PHP alike.
type Member struct {
	// QueryTimeout bounds how long a query waits for other members to answer. Default: 1s.
	QueryTimeout caddy.Duration `json:"query_timeout,omitempty"`
	// PresenceInterval is how often a member announces itself; one silent for three intervals counts as gone. Default: 2s.
	PresenceInterval caddy.Duration `json:"presence_interval,omitempty"`
	// RejoinWindow is how long after reporting a member as left after a timeout the servers still tell it so when it comes back. Default: 24h.
	RejoinWindow caddy.Duration `json:"rejoin_window,omitempty"`
	// QueueSize is how many messages may wait for one other member before it misses them. Default: 1024.
	QueueSize int `json:"queue_size,omitempty"`
	// Worker is the PHP worker that gets the messages other members send to this one.
	Worker *Worker `json:"worker,omitempty"`
}

// Worker is a PHP worker script and its number of threads.
type Worker struct {
	File string `json:"file"`
	// Num is the number of worker threads. Default: FrankenPHP's default for workers.
	Num int `json:"num,omitempty"`
}

// workerSetup registers the worker of a member with FrankenPHP; the PHP
// extension of the cluster provides it, so this package needs no PHP.
var workerSetup func(ctx caddy.Context, membership *Membership, worker Worker) error

// SetWorkerSetup is how the PHP extension of the cluster takes the worker of
// `cluster member`.
func SetWorkerSetup(setup func(ctx caddy.Context, membership *Membership, worker Worker) error) {
	workerSetup = setup
}

func (App) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "cluster",
		New: func() caddy.Module { return new(App) },
	}
}

func (c *App) Provision(ctx caddy.Context) error {
	c.ctx = ctx
	c.logger = ctx.Logger()

	if c.Mesh != nil {
		if err := c.Mesh.setup(); err != nil {
			return err
		}
	}
	if c.Relay != nil {
		if err := c.Relay.setup(); err != nil {
			return err
		}
	}
	if c.Member != nil {
		if err := c.Member.setup(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (m *Member) setup(ctx caddy.Context) error {
	if err := m.validate(); err != nil {
		return err
	}
	if m.Worker == nil {
		return nil
	}
	if m.Worker.File == "" {
		return errors.New("cluster member: the worker needs a file")
	}
	if workerSetup == nil {
		return errors.New("cluster member: a worker needs the PHP extension of the cluster")
	}

	return workerSetup(ctx, Local, *m.Worker)
}

func (m *Member) validate() error {
	if m.QueryTimeout < 0 || m.PresenceInterval < 0 || m.RejoinWindow < 0 || m.QueueSize < 0 {
		return errors.New("cluster member: query_timeout, presence_interval, rejoin_window and queue_size must not be negative")
	}

	return nil
}

// settings are how a member takes part in role; m may be nil for the defaults.
func (m *Member) settings(role Role) Settings {
	if m == nil {
		return Settings{Role: role}
	}

	return Settings{
		QueryTimeout:     time.Duration(m.QueryTimeout),
		PresenceInterval: time.Duration(m.PresenceInterval),
		RejoinWindow:     time.Duration(m.RejoinWindow),
		Role:             role,
	}
}

func (m *Member) queueSize() int {
	if m == nil {
		return 0
	}

	return m.QueueSize
}

// Start runs the relay and joins the cluster as a member when a mesh or an
// uplink is configured. The uplink app loads this one while it provisions, so
// it is looked up only now, once all are provisioned.
func (c *App) Start() error {
	if c.Relay != nil {
		server, err := startRelay(c.Relay, c.logger)
		if err != nil {
			return err
		}
		c.relay = server
	}

	var uplink *Uplink
	app, err := c.ctx.AppIfConfigured("uplink")
	switch {
	case err == nil:
		uplink = &app.(*UplinkApp).Uplink
	case !errors.Is(err, caddy.ErrNotConfigured):
		return fmt.Errorf("loading the uplink app: %w", err)
	}
	if c.Mesh == nil && uplink == nil {
		return nil
	}
	if c.Member == nil || c.Member.Worker == nil {
		// A worker of a configuration this one replaces would still take the messages.
		Local.Handle(MessagesChannel, nil)
		Local.OnMemberLeft(nil)
	}

	bus := newBus(c.logger, c.Member.queueSize(), c.Mesh, uplink)
	if err := Local.Join(bus, c.Member.settings(RoleServer)); err != nil {
		return fmt.Errorf("cluster: joining: %w", err)
	}
	c.bus = bus

	return nil
}

func (c *App) Stop() error {
	var err error
	if c.bus != nil {
		Local.Leave(c.bus)
		err = c.bus.Close()
	}
	if c.relay != nil {
		c.relay.Stop()
	}

	return err
}

// UplinkApp connects this server, as a member, to relays.
type UplinkApp struct {
	Uplink
}

func (UplinkApp) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "uplink",
		New: func() caddy.Module { return new(UplinkApp) },
	}
}

// Provision loads the cluster app, which joins the cluster with the uplink,
// also when no cluster option is configured.
func (u *UplinkApp) Provision(ctx caddy.Context) error {
	if err := u.setup(); err != nil {
		return err
	}
	if _, err := ctx.App("cluster"); err != nil {
		return fmt.Errorf("loading the cluster app: %w", err)
	}

	return nil
}

// Start and Stop do nothing: the cluster app opens the uplink.
func (*UplinkApp) Start() error { return nil }

func (*UplinkApp) Stop() error { return nil }

// parseClusterOption parses each of these once, in any combination:
//
//	cluster mesh { … }
//	cluster relay { … }
//	cluster member { … }
func parseClusterOption(d *caddyfile.Dispenser, existing any) (any, error) {
	cluster := new(App)
	if app, ok := existing.(httpcaddyfile.App); ok {
		if err := json.Unmarshal(app.Value, cluster); err != nil {
			return nil, err
		}
	}

	d.Next()
	if !d.NextArg() {
		return nil, d.Err("cluster needs mesh, relay or member")
	}
	mode := d.Val()
	if d.NextArg() {
		return nil, d.ArgErr()
	}

	switch mode {
	case "mesh":
		if cluster.Mesh != nil {
			return nil, d.Err("cluster mesh is configured twice")
		}
		cluster.Mesh = new(Mesh)
		if err := cluster.Mesh.UnmarshalCaddyfile(d); err != nil {
			return nil, err
		}
	case "relay":
		if cluster.Relay != nil {
			return nil, d.Err("cluster relay is configured twice")
		}
		cluster.Relay = new(Relay)
		if err := cluster.Relay.UnmarshalCaddyfile(d); err != nil {
			return nil, err
		}
	case "member":
		if cluster.Member != nil {
			return nil, d.Err("cluster member is configured twice")
		}
		cluster.Member = new(Member)
		if err := cluster.Member.UnmarshalCaddyfile(d); err != nil {
			return nil, err
		}
	default:
		return nil, d.Errf("unrecognized cluster '%s', want mesh, relay or member", mode)
	}

	return httpcaddyfile.App{Name: "cluster", Value: caddyconfig.JSON(cluster, nil)}, nil
}

// UnmarshalCaddyfile parses the block of:
//
//	cluster member {
//		query_timeout <duration>
//		presence_interval <duration>
//		rejoin_window <duration>
//		queue_size <count>
//		worker <file> [<num>]
//	}
func (m *Member) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for nesting := d.Nesting(); d.NextBlock(nesting); {
		switch option := d.Val(); option {
		case "query_timeout", "presence_interval", "rejoin_window":
			value, err := singleArg(d)
			if err != nil {
				return err
			}
			duration, err := caddy.ParseDuration(value)
			if err != nil || duration <= 0 {
				return d.Errf("invalid %s '%s'", option, value)
			}
			*map[string]*caddy.Duration{
				"query_timeout":     &m.QueryTimeout,
				"presence_interval": &m.PresenceInterval,
				"rejoin_window":     &m.RejoinWindow,
			}[option] = caddy.Duration(duration)
		case "queue_size":
			value, err := singleArg(d)
			if err != nil {
				return err
			}
			size, err := strconv.Atoi(value)
			if err != nil || size < 1 {
				return d.Errf("invalid queue_size '%s'", value)
			}
			m.QueueSize = size
		case "worker":
			args := d.RemainingArgs()
			if len(args) < 1 || len(args) > 2 {
				return d.ArgErr()
			}
			m.Worker = &Worker{File: args[0]}
			if len(args) == 2 {
				num, err := strconv.Atoi(args[1])
				if err != nil || num < 1 {
					return d.Errf("invalid number of worker threads '%s'", args[1])
				}
				m.Worker.Num = num
			}
		default:
			return d.Errf("unrecognized cluster member option '%s'", option)
		}
	}

	return nil
}

func parseUplinkOption(d *caddyfile.Dispenser, existing any) (any, error) {
	if existing != nil {
		return nil, d.Err("uplink is configured twice")
	}

	uplink := new(UplinkApp)
	if err := uplink.UnmarshalCaddyfile(d); err != nil {
		return nil, err
	}

	return httpcaddyfile.App{Name: "uplink", Value: caddyconfig.JSON(uplink, nil)}, nil
}

var (
	_ caddy.App         = (*App)(nil)
	_ caddy.Provisioner = (*App)(nil)
	_ caddy.App         = (*UplinkApp)(nil)
	_ caddy.Provisioner = (*UplinkApp)(nil)
)
