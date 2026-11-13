package cluster

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
)

func TestLoadCLIConfigFromACaddyfile(t *testing.T) {
	path := writeTestFile(t, "Caddyfile", []byte(`{
	cluster mesh {
		listen :7070
		dns members.internal:7070
		secret {env.CLUSTER_SECRET}
	}
	cluster relay {
		listen :7071
		secret s
	}
	cluster member {
		query_timeout 2s
		queue_size 10
		worker cluster.php
	}
	uplink {
		connect relay:7071
		secret s
	}
}
`))

	config, err := LoadCLIConfig(path)
	if err != nil {
		t.Fatal(err)
	}

	if config.Mesh == nil || config.Mesh.Listen != "" || !slices.Equal(config.Mesh.DNS, []string{"members.internal:7070"}) || config.Mesh.Secret != "{env.CLUSTER_SECRET}" {
		t.Fatalf("mesh %+v, want the dns and secret without the listener", config.Mesh)
	}
	if config.Uplink == nil || !slices.Equal(config.Uplink.Connect, []string{"relay:7071"}) {
		t.Fatalf("uplink %+v, want the relay", config.Uplink)
	}
	if config.Member == nil || config.Member.QueueSize != 10 || time.Duration(config.Member.QueryTimeout) != 2*time.Second {
		t.Fatalf("member %+v, want the queue size and query timeout", config.Member)
	}
}

func TestLoadCLIConfigFromJSON(t *testing.T) {
	path := writeTestFile(t, "caddy.json", []byte(`{"apps":{"uplink":{"connect":["relay:7071"],"secret":"s"}}}`))

	config, err := LoadCLIConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.Mesh != nil || config.Uplink == nil || config.Member != nil {
		t.Fatalf("config %+v, want only the uplink", config)
	}
}

func TestLoadCLIConfigRefusesWhatCannotJoin(t *testing.T) {
	configs := map[string]struct{ name, body, want string }{
		"no cluster":               {"Caddyfile", "{\n\tcluster member {\n\t\tqueue_size 10\n\t}\n}\n", "neither a cluster mesh nor an uplink"},
		"a mesh that only listens": {"Caddyfile", "{\n\tcluster mesh {\n\t\tlisten :7070\n\t\tsecret s\n\t}\n}\n", "needs peers or dns"},
		"invalid json":             {"caddy.json", "{", "parsing"},
		"invalid caddyfile":        {"Caddyfile", "{\n\tcluster nothing\n}\n", "adapting"},
	}

	for name, c := range configs {
		t.Run(name, func(t *testing.T) {
			_, err := LoadCLIConfig(writeTestFile(t, c.name, []byte(c.body)))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %v, want one about %q", err, c.want)
			}
		})
	}

	if _, err := LoadCLIConfig(t.TempDir() + "/missing"); err == nil {
		t.Fatal("a missing configuration loaded")
	}
}

func TestCLIParticipantJoinsWithoutListening(t *testing.T) {
	server := NewMembership()
	var got inbox
	server.Handle("orders", got.handle)
	serverBus := meshBus(t, &Mesh{Listen: "127.0.0.1:0", Secret: testBusSecret})
	joinMembership(t, server, serverBus, Settings{QueryTimeout: time.Second})

	participant := NewMembership()
	var participantGot inbox
	participant.Handle("orders", participantGot.handle)
	config := &CLIConfig{Mesh: &Mesh{Peers: []string{serverBus.MeshAddr()}, Secret: testBusSecret}}
	b, err := JoinCLI(participant, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		participant.Leave(b)
		_ = b.Close()
	})

	if b.MeshAddr() != "" {
		t.Fatalf("the CLI participant listens on %s", b.MeshAddr())
	}
	if members := participant.Members(); !slices.Equal(members, []string{server.ID()}) {
		t.Fatalf("right after joining, the CLI participant knows %v, want the server", members)
	}
	waitForMember(t, server, participant.ID())

	answer, err := participant.Request(server.ID(), "orders", []byte("from the cli"), 0)
	if err != nil || string(answer) != "re:from the cli" {
		t.Fatalf("request = %q, %v; want the server's answer", answer, err)
	}
	if err := server.Send(participant.ID(), "orders", []byte("to the cli")); err != nil {
		t.Fatal(err)
	}
	participantGot.waitFor(t, "to the cli")

	started := time.Now()
	replies, err := server.Gather("nobody", []byte("x"))
	if err != nil || len(replies) != 0 || time.Since(started) > 500*time.Millisecond {
		t.Fatalf("gather = %v, %v after %v; want no wait for the CLI participant", replies, err, time.Since(started))
	}
}

func TestJoinOnFirstUse(t *testing.T) {
	newBus := busFactories["mesh"](t)

	m := NewMembership()
	joins := 0
	m.JoinOnFirstUse(func() {
		joins++
		joinMembership(t, m, newBus(), Settings{})
	})

	m.ID()
	if joins != 0 {
		t.Fatal("asking for the instance ID joined")
	}
	if m.Bus() == nil || joins != 1 {
		t.Fatalf("bus after first use, %d joins; want joined once", joins)
	}
	m.Members()
	if _, err := m.Publish("orders", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if joins != 1 {
		t.Fatalf("%d joins, want one", joins)
	}
}

func TestJoinOnFirstUseThatFails(t *testing.T) {
	m := NewMembership()
	m.JoinOnFirstUse(func() {})

	if err := m.Send("OTHER", "orders", []byte("x")); !errors.Is(err, ErrNotJoined) {
		t.Fatalf("send = %v, want not joined", err)
	}
}

func joinCLI(t *testing.T, m *Membership, config *CLIConfig) *Bus {
	t.Helper()

	b, err := JoinCLI(m, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		m.Leave(b)
		_ = b.Close()
	})

	return b
}

func TestCLIParticipantsReachEachOtherThroughServers(t *testing.T) {
	slow := Settings{QueryTimeout: time.Second, PresenceInterval: time.Hour}
	first, second := NewMembership(), NewMembership()
	var firstGot, secondGot inbox
	first.Handle("orders", firstGot.handle)
	second.Handle("orders", secondGot.handle)
	firstBus := meshBus(t, &Mesh{Listen: "127.0.0.1:0", Secret: testBusSecret})
	joinMembership(t, first, firstBus, slow)
	secondBus := meshBus(t, &Mesh{Listen: "127.0.0.1:0", Peers: []string{firstBus.MeshAddr()}, Secret: testBusSecret})
	joinMembership(t, second, secondBus, slow)
	waitForMember(t, first, second.ID())

	config := func() *CLIConfig {
		return &CLIConfig{
			Mesh:   &Mesh{Peers: []string{firstBus.MeshAddr(), secondBus.MeshAddr()}, Secret: testBusSecret},
			Member: &Member{QueryTimeout: caddy.Duration(time.Second), PresenceInterval: caddy.Duration(time.Hour)},
		}
	}
	scheduler, messenger := NewMembership(), NewMembership()
	var schedulerGot, messengerGot inbox
	scheduler.Handle("orders", schedulerGot.handle)
	messenger.Handle("orders", messengerGot.handle)
	joinCLI(t, messenger, config())
	schedulerBus := joinCLI(t, scheduler, config())

	want := []string{first.ID(), second.ID(), messenger.ID()}
	slices.Sort(want)
	if members := scheduler.Members(); !slices.Equal(members, want) {
		t.Fatalf("right after joining, the scheduler knows %v, want both servers and the messenger", members)
	}
	waitForMember(t, messenger, scheduler.ID())

	answer, err := scheduler.Request(messenger.ID(), "orders", []byte("pause"), 0)
	if err != nil || string(answer) != "re:pause" {
		t.Fatalf("request = %q, %v; want the messenger's answer", answer, err)
	}
	if err := messenger.Send(scheduler.ID(), "orders", []byte("paused")); err != nil {
		t.Fatal(err)
	}
	schedulerGot.waitFor(t, "paused")

	queued, err := scheduler.Publish("orders", []byte("to all"))
	if err != nil || queued != 3 {
		t.Fatalf("publish = %d, %v; want it queued for both servers and the messenger", queued, err)
	}
	for _, got := range []*inbox{&firstGot, &secondGot, &messengerGot} {
		got.waitFor(t, "to all")
	}
	time.Sleep(100 * time.Millisecond)
	for name, got := range map[string]*inbox{"first": &firstGot, "second": &secondGot, "messenger": &messengerGot} {
		if n := got.count("to all"); n != 1 {
			t.Fatalf("%s got the message to all %d times, want once", name, n)
		}
	}

	scheduler.Leave(schedulerBus)
	_ = schedulerBus.Close()
	waitUntil(t, "the messenger forgets the scheduler that left", func() bool {
		return !slices.Contains(messenger.Members(), scheduler.ID())
	})
}

func TestServersForwardOnlyWhatCLIParticipantsSend(t *testing.T) {
	server := NewMembership()
	serverBus := meshBus(t, &Mesh{Listen: "127.0.0.1:0", Secret: testBusSecret})
	joinMembership(t, server, serverBus, Settings{})

	dialing := &Mesh{Peers: []string{serverBus.MeshAddr()}, Secret: testBusSecret}
	member := NewMembership()
	memberBus := meshBus(t, dialing)
	joinMembership(t, member, memberBus, Settings{})

	target, sender := NewMembership(), NewMembership()
	var got inbox
	target.Handle("orders", got.handle)
	joinCLI(t, target, &CLIConfig{Mesh: &Mesh{Peers: dialing.Peers, Secret: testBusSecret}})
	senderBus := joinCLI(t, sender, &CLIConfig{Mesh: &Mesh{Peers: dialing.Peers, Secret: testBusSecret}})
	waitForMember(t, server, member.ID())

	forward := func(b *Bus, origin, innerOrigin, data string) {
		t.Helper()

		inner, err := encode(envelope{Origin: innerOrigin, Kind: kindMessage, Channel: "orders", Data: []byte(data)})
		if err != nil {
			t.Fatal(err)
		}
		frame, err := encode(envelope{Origin: origin, Kind: kindForward, Channel: target.ID(), Data: inner})
		if err != nil {
			t.Fatal(err)
		}
		if err := b.publish(server.ID(), frame); err != nil {
			t.Fatal(err)
		}
	}

	forward(memberBus, member.ID(), member.ID(), "from a member")
	forward(senderBus, sender.ID(), "SOMEONEELSE", "in another's name")
	forward(senderBus, sender.ID(), sender.ID(), "from a CLI participant")

	got.waitFor(t, "from a CLI participant")
	time.Sleep(100 * time.Millisecond)
	for _, refused := range []string{"from a member", "in another's name"} {
		if got.count(refused) != 0 {
			t.Fatalf("the server forwarded %q", refused)
		}
	}
}
