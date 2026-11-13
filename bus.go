package frankenphpwebsocket

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
)

func init() {
	caddy.RegisterModule(App{})
	httpcaddyfile.RegisterGlobalOption("websocket", parseGlobalOption)
}

// App is the process-wide part of the extension: the bus that connects this
// instance to the others. Without a bus, the instance runs on its own.
type App struct {
	// BusRaw is the bus adapter, a module of the websocket.bus namespace.
	BusRaw json.RawMessage `json:"bus,omitempty" caddy:"namespace=websocket.bus inline_key=adapter"`
	// QueryTimeout bounds how long a query waits for other instances to answer. Default: 1s.
	QueryTimeout caddy.Duration `json:"query_timeout,omitempty"`
	// PresenceInterval is how often an instance announces itself; one silent for three intervals counts as gone. Default: 2s.
	PresenceInterval caddy.Duration `json:"presence_interval,omitempty"`

	bus Bus
}

func (App) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "websocket",
		New: func() caddy.Module { return new(App) },
	}
}

func (a *App) Provision(ctx caddy.Context) error {
	if a.BusRaw == nil {
		return nil
	}

	module, err := ctx.LoadModule(a, "BusRaw")
	if err != nil {
		return fmt.Errorf("websocket: loading bus adapter: %w", err)
	}

	bus, ok := module.(Bus)
	if !ok {
		return fmt.Errorf("websocket: module %T is not a bus adapter", module)
	}
	a.bus = bus

	return nil
}

func (a *App) Start() error {
	if a.bus == nil {
		return nil
	}

	settings := busSettings{queryTimeout: time.Duration(a.QueryTimeout), presenceInterval: time.Duration(a.PresenceInterval)}
	if err := local.joinBus(a.bus, settings); err != nil {
		return fmt.Errorf("websocket: joining bus: %w", err)
	}

	return nil
}

func (a *App) Stop() error {
	if a.bus == nil {
		return nil
	}

	local.leaveBus(a.bus)

	return a.bus.Close()
}

// parseGlobalOption parses:
//
//	websocket {
//		bus <adapter> ...
//		query_timeout <duration>
//		presence_interval <duration>
//	}
func parseGlobalOption(d *caddyfile.Dispenser, _ any) (any, error) {
	app := new(App)

	d.Next()
	if d.NextArg() {
		return nil, d.ArgErr()
	}

	for d.NextBlock(0) {
		switch d.Val() {
		case "bus":
			if !d.NextArg() {
				return nil, d.ArgErr()
			}
			adapter := d.Val()

			unmarshaler, err := caddyfile.UnmarshalModule(d, "websocket.bus."+adapter)
			if err != nil {
				return nil, err
			}
			app.BusRaw = caddyconfig.JSONModuleObject(unmarshaler, "adapter", adapter, nil)
		case "query_timeout", "presence_interval":
			name := d.Val()
			if !d.NextArg() {
				return nil, d.ArgErr()
			}
			duration, err := caddy.ParseDuration(d.Val())
			if err != nil || duration <= 0 {
				return nil, d.Errf("invalid %s '%s'", name, d.Val())
			}
			if name == "query_timeout" {
				app.QueryTimeout = caddy.Duration(duration)
			} else {
				app.PresenceInterval = caddy.Duration(duration)
			}
			if d.NextArg() {
				return nil, d.ArgErr()
			}
		default:
			return nil, d.Errf("unrecognized websocket option '%s'", d.Val())
		}
	}

	return httpcaddyfile.App{Name: "websocket", Value: caddyconfig.JSON(app, nil)}, nil
}

var (
	_ caddy.App         = (*App)(nil)
	_ caddy.Provisioner = (*App)(nil)
)
