package cluster

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
)

func joinMembership(t testing.TB, m *Membership, b *Bus, settings Settings) {
	t.Helper()

	if err := m.Join(b, settings); err != nil {
		t.Fatalf("join: %v", err)
	}
	t.Cleanup(func() {
		m.Leave(b)
		_ = b.Close()
	})
}

func waitForMember(t testing.TB, m *Membership, id string) {
	t.Helper()

	waitUntil(t, m.ID()+" sees "+id, func() bool { return slices.Contains(m.Members(), id) })
}

// inbox records what a channel handler gets, and answers requests with "re:"
// and the data.
type inbox struct {
	mu       sync.Mutex
	messages []Message
}

func (i *inbox) handle(message Message, respond func([]byte)) {
	i.mu.Lock()
	i.messages = append(i.messages, message)
	i.mu.Unlock()

	respond(append([]byte("re:"), message.Data...))
}

func (i *inbox) count(data string) int {
	i.mu.Lock()
	defer i.mu.Unlock()

	n := 0
	for _, message := range i.messages {
		if string(message.Data) == data {
			n++
		}
	}

	return n
}

func (i *inbox) waitFor(t testing.TB, data string) Message {
	t.Helper()

	waitUntil(t, "the handler gets "+data, func() bool { return i.count(data) > 0 })

	i.mu.Lock()
	defer i.mu.Unlock()
	for _, message := range i.messages {
		if string(message.Data) == data {
			return message
		}
	}

	return Message{}
}

func TestMembershipCarriesTheMessagesOfAChannel(t *testing.T) {
	for name, factory := range busFactories {
		t.Run(name, func(t *testing.T) {
			newBus := factory(t)

			sender, receiver := NewMembership(), NewMembership()
			var got, elsewhere inbox
			receiver.Handle("orders", got.handle)
			receiver.Handle("other", elsewhere.handle)
			joinMembership(t, sender, newBus(), Settings{})
			joinMembership(t, receiver, newBus(), Settings{})
			waitForMember(t, sender, receiver.ID())

			if err := sender.Send(receiver.ID(), "orders", []byte("one")); err != nil {
				t.Fatal(err)
			}
			if message := got.waitFor(t, "one"); message.From != sender.ID() || message.Request {
				t.Fatalf("got %+v, want a plain message from the sender", message)
			}

			if queued, err := sender.Publish("orders", []byte("to all")); err != nil || queued != 1 {
				t.Fatalf("publish = %d, %v; want queued for the one member", queued, err)
			}
			got.waitFor(t, "to all")

			answer, err := sender.Request(receiver.ID(), "orders", []byte("question"), 0)
			if err != nil || string(answer) != "re:question" {
				t.Fatalf("request = %q, %v; want the answer", answer, err)
			}
			if message := got.waitFor(t, "question"); !message.Request {
				t.Fatal("the handler did not see the request as one")
			}

			replies, err := sender.Gather("orders", []byte("everyone"))
			if err != nil || len(replies) != 1 || replies[0].From != receiver.ID() || string(replies[0].Data) != "re:everyone" {
				t.Fatalf("gather = %+v, %v; want the receiver's answer", replies, err)
			}

			time.Sleep(100 * time.Millisecond)
			if elsewhere.count("one") != 0 || got.count("to all") != 1 {
				t.Fatal("a message reached another channel or arrived twice")
			}
		})
	}
}

func TestMembershipAnswersLater(t *testing.T) {
	newBus := busFactories["mesh"](t)

	asker, answerer := NewMembership(), NewMembership()
	answerer.Handle("slow", func(message Message, respond func([]byte)) {
		go func() {
			time.Sleep(50 * time.Millisecond)
			respond([]byte("later"))
		}()
	})
	joinMembership(t, asker, newBus(), Settings{})
	joinMembership(t, answerer, newBus(), Settings{})
	waitForMember(t, asker, answerer.ID())

	if answer, err := asker.Request(answerer.ID(), "slow", []byte("x"), time.Second); err != nil || string(answer) != "later" {
		t.Fatalf("request = %q, %v; want the answer given after the handler returned", answer, err)
	}
}

func TestMembershipTimesOutWithoutAnAnswer(t *testing.T) {
	newBus := busFactories["mesh"](t)

	asker, silent := NewMembership(), NewMembership()
	silent.Handle("nobody", func(Message, func([]byte)) {})
	joinMembership(t, asker, newBus(), Settings{QueryTimeout: 100 * time.Millisecond})
	joinMembership(t, silent, newBus(), Settings{})
	waitForMember(t, asker, silent.ID())

	started := time.Now()
	if _, err := asker.Request(silent.ID(), "nobody", []byte("x"), 0); !errors.Is(err, ErrTimeout) {
		t.Fatalf("request = %v, want a timeout", err)
	}
	if _, err := asker.Gather("nobody", []byte("x")); !errors.Is(err, ErrTimeout) {
		t.Fatalf("gather = %v, want a timeout", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("waited %v with a query timeout of 100ms", elapsed)
	}
}

func TestMembershipWithoutABus(t *testing.T) {
	m := NewMembership()

	if err := m.Send("OTHER", "orders", []byte("x")); !errors.Is(err, ErrNotJoined) {
		t.Fatalf("send = %v, want not joined", err)
	}
	if _, err := m.Publish("orders", []byte("x")); !errors.Is(err, ErrNotJoined) {
		t.Fatalf("publish = %v, want not joined", err)
	}
	if _, err := m.Request("OTHER", "orders", []byte("x"), 0); !errors.Is(err, ErrNotJoined) {
		t.Fatalf("request = %v, want not joined", err)
	}
	if _, err := m.Gather("orders", []byte("x")); !errors.Is(err, ErrNotJoined) {
		t.Fatalf("gather = %v, want not joined", err)
	}
	if len(m.Members()) != 0 || m.Bus() != nil {
		t.Fatal("a membership without a bus has members or a bus")
	}
}

func TestMembershipAcrossAReload(t *testing.T) {
	newBus := busFactories["mesh"](t)

	sender, receiver := NewMembership(), NewMembership()
	var got inbox
	receiver.Handle("orders", got.handle)
	joinMembership(t, sender, newBus(), Settings{})
	joinMembership(t, receiver, newBus(), Settings{})
	oldID := receiver.ID()
	waitForMember(t, sender, oldID)

	// A config reload starts the new bus before it stops the old one.
	joinMembership(t, receiver, newBus(), Settings{})
	newID := receiver.ID()
	if newID == oldID || !receiver.IsOwn(oldID) || !receiver.IsOwn(newID) {
		t.Fatal("the reload did not give a new instance ID that is the process's own")
	}
	waitForMember(t, sender, newID)
	waitUntil(t, "the old instance left", func() bool { return !slices.Contains(sender.Members(), oldID) })

	if _, err := sender.Publish("orders", []byte("once")); err != nil {
		t.Fatal(err)
	}
	got.waitFor(t, "once")
	if err := sender.Send(oldID, "orders", []byte("to the old instance")); err != nil {
		t.Fatal(err)
	}
	got.waitFor(t, "to the old instance")

	time.Sleep(100 * time.Millisecond)
	if got.count("once") != 1 {
		t.Fatalf("the message to all arrived %d times during the reload, want once", got.count("once"))
	}
}

func TestMembershipForgetsAMemberThatLeaves(t *testing.T) {
	newBus := busFactories["mesh"](t)

	stayer, leaver := NewMembership(), NewMembership()
	joinMembership(t, stayer, newBus(), Settings{PresenceInterval: time.Hour})
	leaverBus := newBus()
	if err := leaver.Join(leaverBus, Settings{}); err != nil {
		t.Fatal(err)
	}
	waitForMember(t, stayer, leaver.ID())

	leaver.Leave(leaverBus)
	_ = leaverBus.Close()
	waitUntil(t, "the member that left is forgotten", func() bool { return !slices.Contains(stayer.Members(), leaver.ID()) })
}

func TestMembershipKeepsItsIDForTheFirstJoin(t *testing.T) {
	newBus := busFactories["mesh"](t)

	m := NewMembership()
	before := m.ID()
	joinMembership(t, m, newBus(), Settings{})
	if m.ID() != before {
		t.Fatalf("the first join changed the instance ID from %s to %s", before, m.ID())
	}

	joinMembership(t, m, newBus(), Settings{})
	if m.ID() == before || !m.IsOwn(before) {
		t.Fatal("replacing the bus kept the instance ID, or forgot the old one")
	}
}

// leftMembers records the members a member reports as left.
type leftMembers struct {
	mu   sync.Mutex
	seen []MemberLeft
}

func (d *leftMembers) record(left MemberLeft) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.seen = append(d.seen, left)
}

func (d *leftMembers) of(instance string) []MemberLeft {
	d.mu.Lock()
	defer d.mu.Unlock()

	return slices.DeleteFunc(slices.Clone(d.seen), func(left MemberLeft) bool { return left.Instance != instance })
}

func TestALeftMemberIsReportedOnceByTheLowestServer(t *testing.T) {
	fast := Settings{PresenceInterval: 50 * time.Millisecond}
	network := new(busNetwork)
	newBus := func() *Bus {
		b := meshBus(t, &Mesh{Listen: "127.0.0.1:0", Peers: network.addresses(), Secret: testBusSecret})
		network.add(b)

		return b
	}

	servers := make([]*Membership, 3)
	buses := make([]*Bus, 3)
	reported := make([]*leftMembers, 3)
	for i := range servers {
		servers[i], reported[i] = NewMembership(), new(leftMembers)
		servers[i].OnMemberLeft(reported[i].record)
		buses[i] = newBus()
		joinMembership(t, servers[i], buses[i], fast)
	}
	for _, s := range servers {
		for _, other := range servers {
			if other != s {
				waitForMember(t, s, other.ID())
			}
		}
	}
	lowest := 0
	for i, s := range servers {
		if s.ID() < servers[lowest].ID() {
			lowest = i
		}
	}

	expectOnce := func(instance string, want MemberLeft) {
		t.Helper()

		waitUntil(t, "that "+instance+" left is reported", func() bool { return len(reported[lowest].of(instance)) > 0 })
		time.Sleep(200 * time.Millisecond)
		for i, r := range reported {
			got := r.of(instance)
			switch {
			case i == lowest && (len(got) != 1 || got[0] != want):
				t.Fatalf("the lowest server reported %+v, want %+v once", got, want)
			case i != lowest && len(got) != 0:
				t.Fatalf("server %d, not the lowest, reported %+v", i, got)
			}
		}
	}

	leaver := NewMembership()
	leaverBus := newBus()
	if err := leaver.Join(leaverBus, fast); err != nil {
		t.Fatal(err)
	}
	waitForMember(t, servers[lowest], leaver.ID())
	leaver.Leave(leaverBus)
	_ = leaverBus.Close()
	expectOnce(leaver.ID(), MemberLeft{Instance: leaver.ID(), Reason: ReasonLeave})

	stale, err := encode(envelope{Origin: leaver.ID(), Kind: kindPresence})
	if err != nil {
		t.Fatal(err)
	}
	servers[lowest].receive(buses[lowest], leaver.ID(), stale)
	if slices.Contains(servers[lowest].Members(), leaver.ID()) {
		t.Fatal("a presence sent before the goodbye brought the member back")
	}

	vanisher := NewMembership()
	vanisherBus := newBus()
	if err := vanisher.Join(vanisherBus, fast); err != nil {
		t.Fatal(err)
	}
	waitForMember(t, servers[lowest], vanisher.ID())
	_ = vanisherBus.Close()
	expectOnce(vanisher.ID(), MemberLeft{Instance: vanisher.ID(), Reason: ReasonTimeout})

	participant := NewMembership()
	participantBus, err := JoinCLI(participant, &CLIConfig{
		Mesh:   &Mesh{Peers: network.addresses(), Secret: testBusSecret},
		Member: &Member{PresenceInterval: caddy.Duration(50 * time.Millisecond)},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForMember(t, servers[lowest], participant.ID())
	participant.Leave(participantBus)
	_ = participantBus.Close()
	expectOnce(participant.ID(), MemberLeft{Instance: participant.ID(), Reason: ReasonLeave, Role: RoleCLI})
}

// fastServers joins n servers that see each other, announcing every 50ms,
// and returns them with their buses and the network to join more.
func fastServers(t *testing.T, n int) ([]*Membership, []*Bus, func() *Bus) {
	t.Helper()

	network := new(busNetwork)
	newBus := func() *Bus {
		b := meshBus(t, &Mesh{Listen: "127.0.0.1:0", Peers: network.addresses(), Secret: testBusSecret})
		network.add(b)

		return b
	}

	servers := make([]*Membership, n)
	buses := make([]*Bus, n)
	for i := range servers {
		servers[i] = NewMembership()
		buses[i] = newBus()
		joinMembership(t, servers[i], buses[i], Settings{PresenceInterval: 50 * time.Millisecond})
	}
	for _, s := range servers {
		for _, other := range servers {
			if other != s {
				waitForMember(t, s, other.ID())
			}
		}
	}

	return servers, buses, newBus
}

func lowestOf(servers []*Membership) int {
	lowest := 0
	for i, s := range servers {
		if s.ID() < servers[lowest].ID() {
			lowest = i
		}
	}

	return lowest
}

func TestOnlyServersThatReportAreChosenToReport(t *testing.T) {
	servers, _, newBus := fastServers(t, 3)
	silent := lowestOf(servers)
	var reporters []*Membership
	for i, s := range servers {
		if i != silent {
			reporters = append(reporters, s)
		}
	}
	reported := []*leftMembers{new(leftMembers), new(leftMembers)}
	for i, s := range reporters {
		s.OnMemberLeft(reported[i].record)
	}
	waitUntil(t, "the reporting servers announce themselves", func() bool {
		return len(servers[silent].reporters()) == 2
	})

	vanisher := NewMembership()
	vanisherBus := newBus()
	if err := vanisher.Join(vanisherBus, Settings{PresenceInterval: 50 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	waitForMember(t, servers[silent], vanisher.ID())
	_ = vanisherBus.Close()

	lowest := lowestOf(reporters)
	waitUntil(t, "the lowest reporting server reports the member", func() bool { return len(reported[lowest].of(vanisher.ID())) == 1 })
	time.Sleep(200 * time.Millisecond)
	if got := reported[1-lowest].of(vanisher.ID()); len(got) != 0 {
		t.Fatalf("the other reporting server reported %+v too", got)
	}
	for _, s := range servers {
		if s == reporters[lowest] {
			continue
		}
		s.peersMu.Lock()
		_, recorded := s.reported[vanisher.ID()]
		s.peersMu.Unlock()
		if !recorded {
			t.Fatal("a server did not learn of the report, so it could report the member again")
		}
	}
}

func TestAMemberReportedAfterATimeoutRejoins(t *testing.T) {
	servers, _, newBus := fastServers(t, 2)
	reported := []*leftMembers{new(leftMembers), new(leftMembers)}
	var got inbox
	for i, s := range servers {
		s.OnMemberLeft(reported[i].record)
		s.Handle("orders", got.handle)
	}
	lowest := lowestOf(servers)
	other := servers[1-lowest]

	// Announcing once an hour, it falls silent for the servers while still connected.
	cutOff := NewMembership()
	var fences atomic.Int32
	cutOff.OnReportedGone(func() { fences.Add(1) })
	joinMembership(t, cutOff, newBus(), Settings{PresenceInterval: time.Hour})
	for _, s := range servers {
		waitForMember(t, s, cutOff.ID())
	}
	rejoined := func(want int32) {
		t.Helper()

		waitUntil(t, "the member ended its part and rejoined", func() bool {
			return fences.Load() == want && slices.Contains(servers[0].Members(), cutOff.ID()) && slices.Contains(servers[1].Members(), cutOff.ID())
		})
	}

	// The report reaches the member itself.
	waitUntil(t, "the silent member is reported", func() bool { return len(reported[lowest].of(cutOff.ID())) == 1 })
	rejoined(1)
	if n := len(reported[1-lowest].of(cutOff.ID())); n != 0 {
		t.Fatal("the other server reported the member too")
	}

	// A report the member missed, made while it was cut off, it learns when it talks again.
	record := func(at time.Time) {
		other.peersMu.Lock()
		other.reported[cutOff.ID()] = reportedLeft{at: at, timeout: true}
		other.peersMu.Unlock()
	}
	cleared := func() bool {
		other.peersMu.Lock()
		defer other.peersMu.Unlock()
		_, still := other.reported[cutOff.ID()]

		return !still
	}
	record(time.Now())
	if err := cutOff.Send(other.ID(), "orders", []byte("while reported")); err != nil {
		t.Fatal(err)
	}
	rejoined(2)
	waitUntil(t, "the server counts the member again", cleared)
	if got.count("while reported") != 0 {
		t.Fatal("a server took a message of a member reported as gone")
	}
	if err := cutOff.Send(other.ID(), "orders", []byte("after rejoining")); err != nil {
		t.Fatal(err)
	}
	got.waitFor(t, "after rejoining")

	// Told again of a report it already ended its part for, it only rejoins.
	record(time.Unix(0, cutOff.fencedFor.Load()))
	if err := cutOff.Send(other.ID(), "orders", []byte("told again")); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the server counts the member again", cleared)
	time.Sleep(100 * time.Millisecond)
	if n := fences.Load(); n != 2 {
		t.Fatalf("the member ended its part %d times for two reports", n)
	}
}

func TestReportsAreForgottenAfterTheRejoinWindow(t *testing.T) {
	m := NewMembership()
	joinMembership(t, m, busFactories["mesh"](t)(), Settings{PresenceInterval: 20 * time.Millisecond, RejoinWindow: 100 * time.Millisecond})

	m.record("GONE", reportedLeft{at: time.Now(), timeout: true})
	time.Sleep(50 * time.Millisecond)
	m.peersMu.Lock()
	_, kept := m.reported["GONE"]
	m.peersMu.Unlock()
	if !kept {
		t.Fatal("the report was forgotten within the rejoin window")
	}

	waitUntil(t, "the report is forgotten after the rejoin window", func() bool {
		m.peersMu.Lock()
		defer m.peersMu.Unlock()
		_, still := m.reported["GONE"]

		return !still
	})
}

func TestAMemberAnswersAtOnceForAChannelItDoesNotHandle(t *testing.T) {
	newBus := busFactories["mesh"](t)

	asker, other := NewMembership(), NewMembership()
	joinMembership(t, asker, newBus(), Settings{QueryTimeout: time.Second})
	joinMembership(t, other, newBus(), Settings{})
	waitForMember(t, asker, other.ID())

	started := time.Now()
	if _, err := asker.Request(other.ID(), "nobody", []byte("x"), 0); !errors.Is(err, ErrNoReceiver) {
		t.Fatalf("request = %v, want no receiver", err)
	}
	if replies, err := asker.Gather("nobody", []byte("x")); err != nil || len(replies) != 0 {
		t.Fatalf("gather = %v, %v; want no answers and no timeout", replies, err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("waited %v for a member that does not handle the channel", elapsed)
	}
}

func TestOnlyMembersThatReceiveGetMessages(t *testing.T) {
	newBus := busFactories["mesh"](t)

	sender, receiver, other := NewMembership(), NewMembership(), NewMembership()
	var got inbox
	receiver.Handle(MessagesChannel, got.handle)
	joinMembership(t, sender, newBus(), Settings{})
	joinMembership(t, receiver, newBus(), Settings{})
	// Announcing once an hour, it can only be seen to receive from the presence Handle sends.
	joinMembership(t, other, newBus(), Settings{PresenceInterval: time.Hour})
	waitForMember(t, sender, receiver.ID())
	waitForMember(t, sender, other.ID())
	waitUntil(t, "the sender knows who receives", func() bool {
		return sender.MembersInfo()[receiver.ID()].Capabilities.Has(Receives)
	})
	if info := sender.MembersInfo()[other.ID()]; info.Role != RoleServer || info.Capabilities.Has(Receives) {
		t.Fatalf("the member that does not receive is described as %+v", info)
	}

	if queued, err := sender.Publish(MessagesChannel, []byte("to all")); err != nil || queued != 1 {
		t.Fatalf("publish = %d, %v; want it queued for the one receiver", queued, err)
	}
	got.waitFor(t, "to all")
	if err := sender.Send(other.ID(), MessagesChannel, []byte("x")); !errors.Is(err, ErrNoReceiver) {
		t.Fatalf("send to a member that does not receive = %v, want no receiver", err)
	}
	if err := sender.Send(receiver.ID(), MessagesChannel, []byte("to one")); err != nil {
		t.Fatal(err)
	}
	got.waitFor(t, "to one")

	var later inbox
	other.Handle(MessagesChannel, later.handle)
	waitUntil(t, "the member that starts receiving is known to", func() bool {
		return sender.MembersInfo()[other.ID()].Capabilities.Has(Receives)
	})
	if queued, err := sender.Publish(MessagesChannel, []byte("to both")); err != nil || queued != 2 {
		t.Fatalf("publish = %d, %v; want it queued for both receivers", queued, err)
	}
	later.waitFor(t, "to both")

	other.Handle(MessagesChannel, nil)
	waitUntil(t, "the member that stops receiving is known to", func() bool {
		return !sender.MembersInfo()[other.ID()].Capabilities.Has(Receives)
	})
	if _, err := sender.Request(other.ID(), MessagesChannel, []byte("x"), time.Second); !errors.Is(err, ErrNoReceiver) {
		t.Fatalf("request to a member that stopped receiving = %v, want no receiver", err)
	}
}

// TestAPublishLeavesOutAMemberGoneSilent kills a member without a goodbye:
// its queue lingers for a reconnect, but once it is not a member any more, a
// publish does not count it.
func TestAPublishLeavesOutAMemberGoneSilent(t *testing.T) {
	newBus := busFactories["mesh"](t)
	fast := Settings{PresenceInterval: 50 * time.Millisecond}

	sender, receiver := NewMembership(), NewMembership()
	receiver.Handle(MessagesChannel, func(Message, func([]byte)) {})
	senderBus := newBus()
	joinMembership(t, sender, senderBus, fast)
	joinMembership(t, receiver, newBus(), fast)

	killed := NewMembership()
	killed.Handle(MessagesChannel, func(Message, func([]byte)) {})
	killedBus := newBus()
	if err := killed.Join(killedBus, fast); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "both receivers are known", func() bool {
		info := sender.MembersInfo()

		return info[receiver.ID()].Capabilities.Has(Receives) && info[killed.ID()].Capabilities.Has(Receives)
	})

	_ = killedBus.Close()
	waitUntil(t, "the killed member is not a member any more", func() bool {
		return !slices.Contains(sender.Members(), killed.ID())
	})
	if !senderBus.has(killed.ID()) {
		t.Fatal("the queue of the killed member is already gone, so this test proves nothing")
	}

	if queued, err := sender.Publish(MessagesChannel, []byte("x")); err != nil || queued != 1 {
		t.Fatalf("publish = %d, %v; want only the receiver that is still a member", queued, err)
	}
}

// TestAMemberGoneSilentIsLeftOut kills a member without a goodbye: the bus
// would keep its queue for a reconnect, but once it is not a member, nothing
// counts it and the bus drops it.
func TestAMemberGoneSilentIsLeftOut(t *testing.T) {
	newBus := busFactories["mesh"](t)
	fast := Settings{PresenceInterval: 50 * time.Millisecond, QueryTimeout: time.Second}

	sender, receiver := NewMembership(), NewMembership()
	receiver.Handle(MessagesChannel, func(Message, func([]byte)) {})
	senderBus := newBus()
	joinMembership(t, sender, senderBus, fast)
	joinMembership(t, receiver, newBus(), fast)

	killed := NewMembership()
	killed.Handle(MessagesChannel, func(Message, func([]byte)) {})
	killedBus := newBus()
	if err := killed.Join(killedBus, fast); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "both receivers are known", func() bool {
		info := sender.MembersInfo()

		return info[receiver.ID()].Capabilities.Has(Receives) && info[killed.ID()].Capabilities.Has(Receives)
	})

	_ = killedBus.Close()
	waitUntil(t, "the killed member is not a member any more", func() bool {
		return !slices.Contains(sender.Members(), killed.ID())
	})
	if queued, err := sender.Publish(MessagesChannel, []byte("x")); err != nil || queued != 1 {
		t.Fatalf("publish = %d, %v; want only the receiver that is still a member", queued, err)
	}
	started := time.Now()
	waitUntil(t, "the bus drops the queue of the member gone", func() bool { return !senderBus.has(killed.ID()) })
	if time.Since(started) >= busPeerLinger {
		t.Fatal("the bus kept the queue for the whole reconnect time")
	}
}

func TestTheQueueOfAMemberThatSaysGoodbyeIsDropped(t *testing.T) {
	newBus := busFactories["mesh"](t)

	stayer := NewMembership()
	stayerBus := newBus()
	joinMembership(t, stayer, stayerBus, Settings{})
	leaver := NewMembership()
	leaverBus := newBus()
	if err := leaver.Join(leaverBus, Settings{}); err != nil {
		t.Fatal(err)
	}
	waitForMember(t, stayer, leaver.ID())

	started := time.Now()
	leaver.Leave(leaverBus)
	_ = leaverBus.Close()
	waitUntil(t, "the bus drops the queue of the member that left", func() bool { return !stayerBus.has(leaver.ID()) })
	if time.Since(started) >= busPeerLinger {
		t.Fatal("the bus kept the queue for the whole reconnect time")
	}
}
