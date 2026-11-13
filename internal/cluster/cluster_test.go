package cluster

import (
	"encoding/json"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
)

func TestClusterRejectsInvalidSettings(t *testing.T) {
	ca := newTestCA(t)
	cert, key := ca.issue(t)
	notPEM := writeTestFile(t, "not.pem", []byte("not a certificate"))

	setups := map[string]func() error{
		"mesh with nothing to listen on or dial":  (&Mesh{Secret: "s"}).setup,
		"mesh without authentication":             (&Mesh{Listen: ":7070"}).setup,
		"mesh with tls but no ca and no secret":   (&Mesh{Listen: ":7070", TLS: &TLS{Cert: cert, Key: key}}).setup,
		"mesh with an invalid peer address":       (&Mesh{Listen: ":7070", Secret: "s", Peers: []string{"no-port"}}).setup,
		"mesh with an invalid dns name":           (&Mesh{Listen: ":7070", Secret: "s", DNS: []string{"no-port"}}).setup,
		"mesh listening on tls without a cert":    (&Mesh{Listen: ":7070", Secret: "s", TLS: &TLS{}}).setup,
		"mesh with a cert without key":            (&Mesh{Listen: ":7070", TLS: &TLS{CA: ca.file, Cert: cert}}).setup,
		"mesh with a missing ca":                  (&Mesh{Listen: ":7070", TLS: &TLS{CA: filepath.Join(t.TempDir(), "missing.pem"), Cert: cert, Key: key}}).setup,
		"mesh with a ca file without certificate": (&Mesh{Listen: ":7070", TLS: &TLS{CA: notPEM, Cert: cert, Key: key}}).setup,
		"relay without listen":                    (&Relay{Secret: "s"}).setup,
		"relay without authentication":            (&Relay{Listen: ":7070"}).setup,
		"relay with tls but no cert":              (&Relay{Listen: ":7070", Secret: "s", TLS: &TLS{}}).setup,
		"uplink without relays":                   (&Uplink{Secret: "s"}).setup,
		"uplink without authentication":           (&Uplink{Connect: []string{"relay:7070"}}).setup,
		"uplink with an invalid relay address":    (&Uplink{Connect: []string{"no-port"}, Secret: "s"}).setup,
		"uplink with a ca but no cert":            (&Uplink{Connect: []string{"relay:7070"}, TLS: &TLS{CA: ca.file}}).setup,
	}

	for name, setup := range setups {
		t.Run(name, func(t *testing.T) {
			if err := setup(); err == nil {
				t.Fatal("provision succeeded, want an error")
			}
		})
	}
}

func TestUnmarshalClusterOptions(t *testing.T) {
	input := `cluster mesh {
	listen :7070
	peers a:7070 b:7070
	peers c:7070
	dns members.internal:7070
	resolve_interval 10s
	secret {env.CLUSTER_SECRET}
	tls {
		ca /etc/cluster/ca.pem
		cert /etc/cluster/cert.pem
		key /etc/cluster/key.pem
		server_name cluster.internal
	}
}`
	got, err := parseClusterOption(caddyfile.NewTestDispenser(input), nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err = parseClusterOption(caddyfile.NewTestDispenser("cluster relay {\n\tlisten :7071\n\tsecret s\n}"), got)
	if err != nil {
		t.Fatal(err)
	}

	app := got.(httpcaddyfile.App)
	want := `{"mesh":{"listen":":7070","peers":["a:7070","b:7070","c:7070"],"dns":["members.internal:7070"],"resolve_interval":10000000000,"secret":"{env.CLUSTER_SECRET}","tls":{"ca":"/etc/cluster/ca.pem","cert":"/etc/cluster/cert.pem","key":"/etc/cluster/key.pem","server_name":"cluster.internal"}},"relay":{"listen":":7071","secret":"s"}}`
	if app.Name != "cluster" || string(app.Value) != want {
		t.Fatalf("got app %q %s, want %s", app.Name, app.Value, want)
	}

	if _, err := parseClusterOption(caddyfile.NewTestDispenser("cluster mesh {\n\tlisten :7072\n}"), got); err == nil {
		t.Error("expected an error for a second cluster mesh")
	}

	for _, invalid := range []string{
		"cluster",
		"cluster ring",
		"cluster mesh extra",
		"cluster mesh {\n\tlisten\n}",
		"cluster mesh {\n\tlisten a b\n}",
		"cluster mesh {\n\tpeers\n}",
		"cluster mesh {\n\tdns\n}",
		"cluster mesh {\n\tresolve_interval soon\n}",
		"cluster mesh {\n\tconnect a:7070\n}",
		"cluster mesh {\n\ttls extra\n}",
		"cluster mesh {\n\ttls {\n\t\tunknown x\n\t}\n}",
		"cluster mesh {\n\ttls {\n\t\tca\n\t}\n}",
		"cluster relay {\n\tpeers a:7070\n}",
	} {
		if _, err := parseClusterOption(caddyfile.NewTestDispenser(invalid), nil); err == nil {
			t.Errorf("expected an error for %q", invalid)
		}
	}
}

func TestUnmarshalUplinkOptions(t *testing.T) {
	got, err := parseUplinkOption(caddyfile.NewTestDispenser("uplink {\n\tconnect relay-1:7070 relay-2:7070\n\tsecret s\n\ttls {\n\t\tca /ca.pem\n\t}\n}"), nil)
	if err != nil {
		t.Fatal(err)
	}

	app := got.(httpcaddyfile.App)
	var uplink UplinkApp
	if err := json.Unmarshal(app.Value, &uplink); err != nil {
		t.Fatal(err)
	}
	if app.Name != "uplink" || !slices.Equal(uplink.Connect, []string{"relay-1:7070", "relay-2:7070"}) || uplink.Secret != "s" || uplink.TLS == nil || uplink.TLS.CA != "/ca.pem" {
		t.Fatalf("got app %q %s", app.Name, app.Value)
	}

	if _, err := parseUplinkOption(caddyfile.NewTestDispenser("uplink {\n\tconnect relay:7070\n}"), got); err == nil {
		t.Error("expected an error for a second uplink")
	}
	for _, invalid := range []string{
		"uplink extra",
		"uplink {\n\tconnect\n}",
		"uplink {\n\tlisten :7070\n}",
		"uplink {\n\tsecret\n}",
	} {
		if _, err := parseUplinkOption(caddyfile.NewTestDispenser(invalid), nil); err == nil {
			t.Errorf("expected an error for %q", invalid)
		}
	}
}

func TestClusterAppRunsARelay(t *testing.T) {
	address := freeAddress(t)
	config := `{"admin":{"disabled":true},"apps":{"cluster":{"relay":{"listen":"` + address + `","secret":"s"}}}}`
	if err := caddy.Load([]byte(config), true); err != nil {
		t.Fatalf("load: %v", err)
	}
	t.Cleanup(func() { _ = caddy.Stop() })

	a := openPeerBus(t, uplinkBus(t, &Uplink{Connect: []string{address}, Secret: "s"}), "AAAA", new(received))
	openPeerBus(t, uplinkBus(t, &Uplink{Connect: []string{address}, Secret: "s"}), "BBBB", new(received))
	waitUntil(t, "members meet through the relay of the app", func() bool { return a.connected("BBBB") })
}

func TestUnmarshalClusterMember(t *testing.T) {
	got, err := parseClusterOption(caddyfile.NewTestDispenser("cluster member {\n\tquery_timeout 3s\n\tpresence_interval 500ms\n\trejoin_window 1h\n\tqueue_size 64\n\tworker cluster.php 2\n}"), nil)
	if err != nil {
		t.Fatal(err)
	}

	app := got.(httpcaddyfile.App)
	want := `{"member":{"query_timeout":3000000000,"presence_interval":500000000,"rejoin_window":3600000000000,"queue_size":64,"worker":{"file":"cluster.php","num":2}}}`
	if app.Name != "cluster" || string(app.Value) != want {
		t.Fatalf("got app %q %s, want %s", app.Name, app.Value, want)
	}
	if _, err := parseClusterOption(caddyfile.NewTestDispenser("cluster member {\n\tqueue_size 8\n}"), got); err == nil {
		t.Error("expected an error for a second cluster member")
	}

	for _, invalid := range []string{
		"cluster member extra",
		"cluster member {\n\tquery_timeout\n}",
		"cluster member {\n\tquery_timeout 0s\n}",
		"cluster member {\n\tpresence_interval soon\n}",
		"cluster member {\n\trejoin_window 0s\n}",
		"cluster member {\n\tqueue_size 0\n}",
		"cluster member {\n\tworker\n}",
		"cluster member {\n\tworker cluster.php 0\n}",
		"cluster member {\n\tworker cluster.php 2 3\n}",
		"cluster member {\n\tlisten :7070\n}",
	} {
		if _, err := parseClusterOption(caddyfile.NewTestDispenser(invalid), nil); err == nil {
			t.Errorf("expected an error for %q", invalid)
		}
	}
}

func TestClusterMemberWorkerNeedsThePHPExtension(t *testing.T) {
	previous := workerSetup
	t.Cleanup(func() { workerSetup = previous })
	workerSetup = nil

	if err := (&Member{Worker: &Worker{File: "cluster.php"}}).setup(caddy.Context{}); err == nil || !strings.Contains(err.Error(), "PHP extension") {
		t.Fatalf("err = %v, want the missing PHP extension explained", err)
	}

	var setUp Worker
	workerSetup = func(_ caddy.Context, membership *Membership, worker Worker) error {
		if membership != Local {
			t.Error("the worker is not set up for the process's membership")
		}
		setUp = worker

		return nil
	}
	if err := (&Member{Worker: &Worker{File: "cluster.php", Num: 2}}).setup(caddy.Context{}); err != nil || setUp != (Worker{File: "cluster.php", Num: 2}) {
		t.Fatalf("setup = %v, worker %+v; want it handed to the PHP extension", err, setUp)
	}
}

func TestClusterAppJoinsAsAMember(t *testing.T) {
	config := `{"admin":{"disabled":true},"apps":{"cluster":{"mesh":{"listen":"127.0.0.1:0","secret":"s"},"member":{"query_timeout":250000000}}}}`
	if err := caddy.Load([]byte(config), true); err != nil {
		t.Fatalf("load: %v", err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = caddy.Stop()
		}
	})

	if Local.Bus() == nil || Local.timeout() != 250*time.Millisecond {
		t.Fatalf("bus %v, query timeout %v; want joined with the member settings", Local.Bus(), Local.timeout())
	}

	if err := caddy.Stop(); err != nil {
		t.Fatal(err)
	}
	stopped = true
	if Local.Bus() != nil {
		t.Fatal("still a member after stop")
	}
}

func TestUplinkAloneJoinsAsAMember(t *testing.T) {
	relay := startTestRelay(t, &Relay{Listen: "127.0.0.1:0", Secret: "s"})

	config := `{"admin":{"disabled":true},"apps":{"uplink":{"connect":["` + relay.Addr() + `"],"secret":"s"}}}`
	if err := caddy.Load([]byte(config), true); err != nil {
		t.Fatalf("load: %v", err)
	}
	t.Cleanup(func() { _ = caddy.Stop() })

	if Local.Bus() == nil {
		t.Fatal("an uplink without a cluster option did not join")
	}
}

func TestClusterMemberRefusesANegativeRejoinWindow(t *testing.T) {
	if err := (&Member{RejoinWindow: -1}).validate(); err == nil {
		t.Fatal("a negative rejoin_window was accepted")
	}
}
