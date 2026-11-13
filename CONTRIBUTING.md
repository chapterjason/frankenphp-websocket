# Contributing

## Questions, ideas and bug reports

Use [Discussions](https://github.com/chapterjason/frankenphp-websocket/discussions):
issues and pull requests are open to collaborators only. Report
vulnerabilities privately, as [SECURITY.md](SECURITY.md) describes.

## Development

PHP must be built with ZTS and `--enable-embed`, as FrankenPHP requires; the
scripts take their flags from `php-config-zts`, else `php-config`. FrankenPHP's builder image
(`dunglas/frankenphp:<version>-builder-php<php-version>`) has everything, and CI
runs in it.

| Command | Does |
|---|---|
| `scripts/check.sh` | Checks formatting and runs `go vet`. |
| `scripts/lint.sh` | Runs `golangci-lint`. |
| `scripts/test.sh` | Runs the tests with the race detector. |
| `scripts/e2e.sh` | Tests the Docker image as a cluster, with Go clients and real browsers. Needs Docker or Podman only (`CONTAINER` picks one). |
| `scripts/bench.sh` | Runs the benchmarks. |
| `scripts/generate.sh` | Regenerates the PHP glue from `internal/websocket/websocket.go` and `internal/clusterphp/cluster.go`. |

See [docs/development.md](docs/development.md) for the details.

## Changes

- Keep a change to one concern, with tests for what it changes.
- `scripts/check.sh`, `scripts/lint.sh`, `scripts/test.sh` and `scripts/e2e.sh` must pass.
- Change a PHP function only in `internal/websocket/websocket.go` or `internal/clusterphp/cluster.go`,
  then run `scripts/generate.sh`; the generated files are not edited by hand.
- Document user-facing changes in `docs/` and add them to `CHANGELOG.md`.

## Versions that move together

Some versions are pinned in several places and must change together, so
Dependabot leaves them alone:

- **FrankenPHP and Caddy**: `go.mod`, the build arguments in `Dockerfile`, and
  the builder image of the `test` job in `.github/workflows/ci.yml`. Caddy must
  be the version FrankenPHP uses.
- **Playwright**: the image in `e2e/compose.yaml` and `@playwright/test` in
  `e2e/browser/package.json`.
- **golangci-lint and the build tags**: `scripts/lint.sh`, `scripts/common.sh` and the `test` job in
  `.github/workflows/ci.yml`.
