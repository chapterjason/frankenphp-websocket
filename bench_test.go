package frankenphpwebsocket

import (
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobwas/ws"
)

// drain reads frames off c until it fails, counting them.
func (c *wsClient) drain(count *atomic.Int64) {
	go func() {
		for {
			header, err := ws.ReadHeader(c.r)
			if err != nil {
				return
			}
			if _, err := io.CopyN(io.Discard, c.r, header.Length); err != nil {
				return
			}
			count.Add(1)
		}
	}()
}

func waitForCount(b *testing.B, count *atomic.Int64, want int64) {
	b.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for count.Load() < want {
		if time.Now().After(deadline) {
			b.Fatalf("received %d messages, want %d", count.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func BenchmarkHandshake(b *testing.B) {
	for _, proto := range protocols {
		b.Run(proto, func(b *testing.B) {
			s := startServer(b, proto)

			for b.Loop() {
				client := s.dial(b)
				client.close()
			}
		})
	}
}

func BenchmarkEchoThroughPHP(b *testing.B) {
	for _, proto := range protocols {
		b.Run(proto, func(b *testing.B) {
			s := startServer(b, proto)
			client := s.dial(b)
			message := []byte("hello")

			for b.Loop() {
				client.send(b, ws.OpText, true, message)
				header, err := ws.ReadHeader(client.r)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.CopyN(io.Discard, client.r, header.Length); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPushToConnection(b *testing.B) {
	for _, size := range []int{64, 4 << 10} {
		for _, proto := range protocols {
			b.Run(fmt.Sprintf("%s/%dB", proto, size), func(b *testing.B) {
				s := startServer(b, proto)
				client := s.dial(b)
				id := connectionID(b, s.workers, 0)
				payload := make([]byte, size)

				var received atomic.Int64
				client.drain(&received)

				b.SetBytes(int64(size))
				var sent int64
				for b.Loop() {
					if code := local.send(id, false, payload); code != WEBSOCKET_ERROR_NONE {
						b.Fatalf("send = %d", code)
					}
					sent++
				}
				waitForCount(b, &received, sent)
			})
		}
	}
}

func BenchmarkBroadcast(b *testing.B) {
	for _, connections := range []int{10, 100} {
		b.Run(fmt.Sprintf("%d_connections", connections), func(b *testing.B) {
			s := startServer(b, "HTTP/1.1")

			var received atomic.Int64
			for range connections {
				s.dial(b).drain(&received)
			}
			waitForConnections(b, s.handler, connections)

			var rounds int64
			for b.Loop() {
				if sent, code := local.broadcast(true, []byte("to all")); sent != int64(connections) || code != WEBSOCKET_ERROR_NONE {
					b.Fatalf("broadcast = %d, %d", sent, code)
				}
				rounds++
			}
			waitForCount(b, &received, rounds*int64(connections))
		})
	}
}

func BenchmarkBusPublish(b *testing.B) {
	for _, name := range []string{"unix", "nats"} {
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
			waitForCount(b, &delivered, sent)
		})
	}
}

func BenchmarkPushToAnotherInstance(b *testing.B) {
	for _, name := range []string{"unix", "nats"} {
		b.Run(name, func(b *testing.B) {
			newBus := busFactories[name](b)

			sender, _, _, _ := startNode(b, newBus)
			_, _, client, id := startNode(b, newBus)

			var received atomic.Int64
			client.drain(&received)

			payload := make([]byte, 64)
			var sent int64
			for b.Loop() {
				for {
					code := sender.send(id, true, payload)
					if code == WEBSOCKET_ERROR_NONE {
						break
					}
					// The bus refused a burst beyond its queue; wait for it to drain.
					if code != WEBSOCKET_ERROR_BUS_FAILED {
						b.Fatalf("send = %d", code)
					}
					time.Sleep(10 * time.Microsecond)
				}
				sent++
			}
			waitForCount(b, &received, sent)
		})
	}
}
