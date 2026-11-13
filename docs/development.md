# Development

PHP must be built with ZTS and `--enable-embed`; the scripts take its flags
from `php-config-zts`, else `php-config` (`PHP_CONFIG` overrides it).

| Command | Does |
|---|---|
| `scripts/test.sh` | Runs the tests with the race detector. A real FrankenPHP runs them, and the cluster tests run instances over loopback. |
| `scripts/e2e.sh` | Builds the Docker image, starts a cluster with Docker Compose (`e2e/compose.yaml`) and tests it from outside: two mesh members, a relay, and two members that reach everyone only through the relay; Go clients over HTTP/1.1, HTTP/2 and HTTP/3 and pushes between every pair of instances, then Chromium and Firefox with Playwright, in its container. Needs Docker or Podman only (`CONTAINER` picks one). |
| `scripts/bench.sh` | Runs the benchmarks; `BENCHTIME` sets their duration. See [Performance](performance.md). |
| `scripts/lint.sh` | Runs `golangci-lint` with `.golangci.yml`; the script builds the pinned version into `bin/`. |
| `scripts/check.sh` | Checks formatting and runs `go vet`. |
| `scripts/build.sh` | Builds `./frankenphp` with the extension. |
| `scripts/generate.sh` | Regenerates the PHP glue of both extensions with FrankenPHP's extension generator: `internal/websocket` from `websocket.go` and `internal/clusterphp` from `cluster.go` (their `.c`, `.h`, `_arginfo.h`, `.stub.php` and `_generated.go`). It fetches `build/gen_stub.php` from the sources of the PHP version built against into `bin/`, where it installs the PHP-Parser it needs; `GEN_STUB_SCRIPT` overrides it. `FRANKENPHP` defaults to `./frankenphp`, built with `scripts/build.sh`. |
| `scripts/docker.sh` | Builds the Docker image. |

FrankenPHP's extension generator writes broken C for a default value that is a
constant or `null`, so the PHP signatures use literal defaults.
