package cluster

import (
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkBusPublish(b *testing.B) {
	for _, name := range []string{"mesh", "relay"} {
		b.Run(name, func(b *testing.B) {
			newBus := busFactories[name](b)

			var delivered atomic.Int64
			receiver := newBus()
			if err := receiver.Open("BBBB", func([]byte) { delivered.Add(1) }); err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = receiver.Close() })
			sender := openBus(b, newBus, "AAAA", new(received))

			payload := make([]byte, 1<<10)
			b.SetBytes(int64(len(payload)))
			var sent int64
			for b.Loop() {
				// A full queue means the publisher outran the receiver; wait for it,
				// so the benchmark measures sustained throughput.
				for sender.Publish("BBBB", payload) != nil {
					time.Sleep(10 * time.Microsecond)
				}
				sent++
			}

			deadline := time.Now().Add(30 * time.Second)
			for delivered.Load() < sent {
				if time.Now().After(deadline) {
					b.Fatalf("delivered %d messages, want %d", delivered.Load(), sent)
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}
