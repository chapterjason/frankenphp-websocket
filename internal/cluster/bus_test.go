package cluster

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestBusContract(t *testing.T) {
	for name, factory := range busFactories {
		t.Run(name, func(t *testing.T) {
			newBus := factory(t)

			var a, b, c received
			busA := openBus(t, newBus, "AAAA", &a)
			openBus(t, newBus, "BBBB", &b)
			openBus(t, newBus, "CCCC", &c)

			if err := busA.Publish("BBBB", []byte("to b")); err != nil {
				t.Fatalf("publish to b: %v", err)
			}
			b.waitFor(t, "to b")

			if err := busA.Publish("", []byte("to all")); err != nil {
				t.Fatalf("publish to all: %v", err)
			}
			b.waitFor(t, "to all")
			c.waitFor(t, "to all")

			if err := busA.Publish("GONE", []byte("to nobody")); err != nil {
				t.Fatalf("publish to a gone instance: %v", err)
			}

			if err := busA.Publish("BBBB", []byte("after")); err != nil {
				t.Fatalf("publish after: %v", err)
			}
			b.waitFor(t, "after")

			for _, payload := range c.snapshot() {
				if payload == "to b" || payload == "after" {
					t.Fatalf("instance c got %q, which was addressed to b", payload)
				}
			}
		})
	}
}

func TestBusDeliversAgainAfterThePeerRestarts(t *testing.T) {
	for name, factory := range busFactories {
		t.Run(name, func(t *testing.T) {
			newBus := factory(t)

			var a, b received
			busA := openBus(t, newBus, "AAAA", &a)
			busB := openBus(t, newBus, "BBBB", &b)

			if err := busA.Publish("BBBB", []byte("first")); err != nil {
				t.Fatal(err)
			}
			b.waitFor(t, "first")

			if err := busB.Close(); err != nil {
				t.Fatal(err)
			}
			var restarted received
			openBus(t, newBus, "BBBB", &restarted)

			if err := busA.Publish("BBBB", []byte("second")); err != nil {
				t.Fatalf("publish after restart: %v", err)
			}
			restarted.waitFor(t, "second")
		})
	}
}

func TestBusStuckPeerDoesNotHoldUpTheOthers(t *testing.T) {
	for name, factory := range busFactories {
		t.Run(name, func(t *testing.T) { testStuckPeerDoesNotHoldUpTheOthers(t, factory(t)) })
	}
}

func testStuckPeerDoesNotHoldUpTheOthers(t *testing.T, newBus func() *Bus) {
	stuckBus(t, newBus, "STUCK")

	var b received
	busA := openBus(t, newBus, "AAAA", new(received))
	openBus(t, newBus, "BBBB", &b)

	// Publish until the stuck peer's queue is full and a hundred more; how many
	// that takes depends on the socket buffers of the system.
	payload := make([]byte, 16<<10)
	var (
		queueFull error
		published int
		afterFull int
	)
	started := time.Now()
	for published < 20*DefaultQueueSize && afterFull < 100 {
		err := busA.Publish("", payload)
		published++
		if err != nil {
			if !strings.Contains(err.Error(), "STUCK") || !strings.Contains(err.Error(), "queue") || strings.Contains(err.Error(), "BBBB") {
				t.Fatalf("publish error = %v, want only the stuck peer's full queue", err)
			}
			queueFull = err
		}
		if queueFull != nil {
			afterFull++
		}
	}
	if queueFull == nil {
		t.Fatal("the stuck peer's full queue was not reported")
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("publishing %d messages took %v with a stuck peer", published, elapsed)
	}

	started = time.Now()
	if err := busA.Publish("BBBB", []byte("after the flood")); err != nil {
		t.Fatalf("publish to b: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("a publish to b took %v behind the stuck peer", elapsed)
	}
	b.waitFor(t, "after the flood")

	if got := len(b.snapshot()); got != published+1 {
		t.Fatalf("b got %d payloads, want all %d", got, published+1)
	}

	started = time.Now()
	if err := busA.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if elapsed := time.Since(started); elapsed > busWriteTimeout+2*time.Second {
		t.Fatalf("close took %v with a stuck peer, want at most about the %v write timeout", elapsed, busWriteTimeout)
	}
	if err := busA.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestBusPublishToAMissingInstanceStartsNoWriter(t *testing.T) {
	bus := openBus(t, busFactories["mesh"](t), "AAAA", new(received))

	if err := bus.Publish("GONE", []byte("x")); err != nil {
		t.Fatal(err)
	}

	b := bus
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.peers) != 0 {
		t.Fatalf("peers = %v, want none for a missing instance", b.peers)
	}
}

func TestBusKeepsWhatIsQueuedForAReconnectingPeer(t *testing.T) {
	a := openPeerBus(t, newBus(nil, 4, mustMesh(t, &Mesh{Listen: "127.0.0.1:0", Secret: testBusSecret}), nil), "AAAA", new(received))
	b := openPeerBus(t, meshBus(t, &Mesh{Peers: []string{a.MeshAddr()}, Secret: testBusSecret}), "BBBB", new(received))
	waitUntil(t, "a is connected to b", func() bool { return a.connected("BBBB") })

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "a lost b", func() bool { return !a.connected("BBBB") })

	for i := range 4 {
		if err := a.Publish("BBBB", []byte("queued "+strconv.Itoa(i))); err != nil {
			t.Fatalf("publish %d with a queue of 4: %v", i, err)
		}
	}
	if err := a.Publish("BBBB", []byte("one too many")); err == nil || !strings.Contains(err.Error(), "queue") {
		t.Fatalf("err = %v, want the full queue of 4 reported", err)
	}

	var back received
	openPeerBus(t, meshBus(t, &Mesh{Peers: []string{a.MeshAddr()}, Secret: testBusSecret}), "BBBB", &back)
	for i := range 4 {
		back.waitFor(t, "queued "+strconv.Itoa(i))
	}
}

func TestBusForgetsAPeerGoneForTheLingerTime(t *testing.T) {
	a := meshBus(t, &Mesh{Listen: "127.0.0.1:0", Secret: testBusSecret})
	a.linger = 100 * time.Millisecond
	openPeerBus(t, a, "AAAA", new(received))
	b := openPeerBus(t, meshBus(t, &Mesh{Peers: []string{a.MeshAddr()}, Secret: testBusSecret}), "BBBB", new(received))
	waitUntil(t, "a is connected to b", func() bool { return a.connected("BBBB") })

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "a forgot b", func() bool { return a.peerCount() == 0 })

	if err := a.Publish("BBBB", []byte("to nobody")); err != nil {
		t.Fatalf("publish to a forgotten instance: %v", err)
	}
	if a.peerCount() != 0 {
		t.Fatal("publishing to a forgotten instance brought it back")
	}
}
