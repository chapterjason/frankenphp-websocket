package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/caddyserver/caddy/v2/caddyconfig"
	"go.uber.org/zap"
)

// CLIConfigEnv names the environment variable with the path of the server
// configuration a CLI participant takes its cluster options from.
const CLIConfigEnv = "FRANKENPHP_CLUSTER_CONFIG"

// CLIConfig is what a CLI participant takes from the configuration of the
// servers: the mesh it dials, the relays of its uplink and how it takes part
// as a member. It accepts no connections, so it has no listener, no relay and
// no worker.
type CLIConfig struct {
	Mesh   *Mesh
	Uplink *Uplink
	Member *Member
}

// LoadCLIConfig reads the cluster options of a Caddyfile, a file named
// Caddyfile* or *.caddyfile, or of a JSON configuration otherwise, as Caddy
// tells them apart.
func LoadCLIConfig(path string) (*CLIConfig, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cluster: reading the configuration: %w", err)
	}

	base := filepath.Base(path)
	if strings.HasPrefix(base, "Caddyfile") || filepath.Ext(base) == ".caddyfile" {
		adapter := caddyconfig.GetAdapter("caddyfile")
		if adapter == nil {
			return nil, errors.New("cluster: the caddyfile adapter is not available")
		}
		body, _, err = adapter.Adapt(body, map[string]any{"filename": path})
		if err != nil {
			return nil, fmt.Errorf("cluster: adapting %s: %w", path, err)
		}
	}

	var config struct {
		Apps struct {
			Cluster *App       `json:"cluster"`
			Uplink  *UplinkApp `json:"uplink"`
		} `json:"apps"`
	}
	if err := json.Unmarshal(body, &config); err != nil {
		return nil, fmt.Errorf("cluster: parsing %s: %w", path, err)
	}

	cli := new(CLIConfig)
	if app := config.Apps.Cluster; app != nil {
		cli.Mesh, cli.Member = app.Mesh, app.Member
	}
	if config.Apps.Uplink != nil {
		cli.Uplink = &config.Apps.Uplink.Uplink
	}
	if cli.Mesh == nil && cli.Uplink == nil {
		return nil, fmt.Errorf("cluster: %s has neither a cluster mesh nor an uplink", path)
	}

	if cli.Mesh != nil {
		cli.Mesh.Listen = ""
		if len(cli.Mesh.Peers) == 0 && len(cli.Mesh.DNS) == 0 {
			return nil, errors.New("cluster mesh: a CLI participant only dials, so it needs peers or dns")
		}
	}
	if cli.Member != nil {
		if err := cli.Member.validate(); err != nil {
			return nil, err
		}
	}

	return cli, nil
}

// JoinCLI makes m a CLI participant of the cluster of config and returns its
// bus, which the caller leaves and closes when the process ends. It returns
// once the members it reached are known, so a command that sends to all of
// them right away reaches them.
func JoinCLI(m *Membership, config *CLIConfig, logger *zap.Logger) (*Bus, error) {
	b, err := NewBus(logger, config.Member.queueSize(), config.Mesh, config.Uplink)
	if err != nil {
		return nil, err
	}
	if err := m.Join(b, config.Member.settings(RoleCLI)); err != nil {
		return nil, fmt.Errorf("cluster: joining: %w", err)
	}
	m.awaitPresence(b)

	return b, nil
}
