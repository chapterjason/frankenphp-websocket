# Performance

Measured with `scripts/bench.sh` on an Intel Core i7-14700K (28 threads,
Linux), Go 1.27.4, PHP 8.5.12 ZTS, FrankenPHP 1.13.5, with two PHP worker
threads. Client and server run on the same machine, in one process.

| Benchmark | Result |
|---|---|
| Handshake through PHP, HTTP/1.1 | 0.40 ms |
| Handshake through PHP, HTTP/2 | 4.6 ms |
| Handshake through PHP, HTTP/3 | 0.49 ms |
| Message round trip through the PHP worker, HTTP/1.1 | 0.12 ms |
| Message round trip through the PHP worker, HTTP/2 | 0.22 ms |
| Message round trip through the PHP worker, HTTP/3 | 0.17 ms |
| Push from Go to a connection, 64 B, HTTP/1.1 / HTTP/2 / HTTP/3 | 2.0 µs / 12 µs / 2.9 µs |
| Push from Go to a connection, 4 KiB, HTTP/1.1 / HTTP/2 / HTTP/3 | 945 MB/s / 72 MB/s / 254 MB/s |
| Broadcast to 10 connections | 89 µs |
| Broadcast to 100 connections | 64 µs |
| Cluster publish, 1 KiB, mesh / relay | 561 MB/s / 346 MB/s |
| Push to a connection on another instance, mesh / relay | 3.9 µs / 4.0 µs |

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
- The members of a cluster, and the relay, run in the benchmark process and
  connect over loopback.
- A push to another instance is fire and forget, so its rate is the rate the
  cluster takes messages at. Bursts beyond the `queue_size` to one member are
  refused with `WEBSOCKET_ERROR_BUS_FAILED`; the benchmark waits them out to
  measure the sustained rate, and every message is checked to arrive.
