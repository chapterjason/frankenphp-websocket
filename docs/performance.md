# Performance

Measured with `make bench` on an Intel Core i9-9880H (8 cores, 16 threads,
macOS), Go 1.27.4, PHP 8.5.11 ZTS, FrankenPHP 1.13.5, with two PHP worker
threads. Client and server run on the same machine, in one process.

| Benchmark | Result |
|---|---|
| Handshake through PHP, HTTP/1.1 | 0.34 ms |
| Handshake through PHP, HTTP/2 | 2.9 ms |
| Handshake through PHP, HTTP/3 | 0.48 ms |
| Message round trip through the PHP worker, HTTP/1.1 | 0.12 ms |
| Message round trip through the PHP worker, HTTP/2 | 0.17 ms |
| Message round trip through the PHP worker, HTTP/3 | 0.23 ms |
| Push from Go to a connection, 64 B, HTTP/1.1 / HTTP/2 / HTTP/3 | 5.2 µs / 22 µs / 2.6 µs |
| Push from Go to a connection, 4 KiB, HTTP/1.1 / HTTP/2 / HTTP/3 | 381 MB/s / 89 MB/s / 91 MB/s |
| Broadcast to 10 connections | 61 µs |
| Broadcast to 100 connections | 0.43 ms |
| Bus publish, 1 KiB, Unix sockets / NATS | 258 MB/s / 915 MB/s |
| Push to a connection on another instance, Unix sockets / NATS | 8.9 µs / 4.5 µs |

What the numbers include:

- The test servers serve HTTP/1.1 without TLS, HTTP/2 and HTTP/3 with TLS. A
  handshake over HTTP/2 opens a new TLS connection each time, one over HTTP/3
  a new stream of a QUIC connection already up, which explains most of the
  difference between them.
- A handshake runs the PHP worker once for the handshake request and once for
  the `open` event, and the `close` event follows.
- A round trip is a message from the client, the `message` event in PHP, and
  PHP's reply back to the client.
- A push or broadcast counts the time to hand the message to the connections;
  every message is checked to arrive.
- The NATS server runs embedded in the benchmark process.
- A push to another instance is fire and forget, so its rate is the rate the
  bus takes messages at. Bursts beyond the Unix bus's `queue_size` to one
  instance are refused with `WEBSOCKET_ERROR_BUS_FAILED`; the benchmark waits
  them out to measure the sustained rate.
