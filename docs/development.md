# Development

PHP must be built with ZTS and `--enable-embed`; the targets take its flags
from `php-config` (`PHP_CONFIG` overrides it).

| Command | Does |
|---|---|
| `make test` | Runs the tests with the race detector. A real FrankenPHP runs them, and NATS is embedded. |
| `make e2e` | Builds the Docker image, starts a two-instance cluster on NATS with Docker Compose (`e2e/compose.yaml`) and tests it from outside: Go clients over HTTP/1.1, HTTP/2 and HTTP/3, then Chromium and Firefox with Playwright, in its container. Needs Docker only. |
| `make bench` | Runs the benchmarks; `BENCHTIME` sets their duration. See [Performance](performance.md). |
| `make lint` | Runs `golangci-lint` with `.golangci.yml`; the Makefile builds the pinned version into `bin/`. |
| `make check` | Checks formatting and runs `go vet`. |
| `make build` | Builds `./frankenphp` with the extension. |
| `make generate` | Regenerates the PHP glue (`websocket.c`, `websocket.h`, `websocket_arginfo.h`, `websocket.stub.php`, `websocket_generated.go`) from `websocket.go` with FrankenPHP's extension generator. Set `GEN_STUB_SCRIPT` to the `build/gen_stub.php` of the PHP sources; `FRANKENPHP` defaults to `./frankenphp`. |
| `make docker` | Builds the Docker image. |

FrankenPHP's extension generator writes broken C for a default value that is a
constant or `null`, so the PHP signatures use literal defaults.
