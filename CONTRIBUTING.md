# Contributing

## Questions, ideas and bug reports

Use [Discussions](https://github.com/chapterjason/frankenphp-websocket/discussions):
issues and pull requests are open to collaborators only. Report
vulnerabilities privately, as [SECURITY.md](SECURITY.md) describes.

## Development

PHP must be built with ZTS and `--enable-embed`, as FrankenPHP requires; the
Makefile takes its flags from `php-config`. FrankenPHP's builder image
(`dunglas/frankenphp:<version>-builder-php<php-version>`) has everything, and CI
runs in it.

| Command | Does |
|---|---|
| `make check` | Checks formatting and runs `go vet`. |
| `make lint` | Runs `golangci-lint`. |
| `make test` | Runs the tests with the race detector. |
| `make e2e` | Tests the Docker image as a cluster, with Go clients and real browsers. Needs Docker only. |
| `make bench` | Runs the benchmarks. |
| `make generate` | Regenerates the PHP glue from `websocket.go`. |

See [docs/development.md](docs/development.md) for the details.

## Changes

- Keep a change to one concern, with tests for what it changes.
- `make check`, `make lint`, `make test` and `make e2e` must pass.
- Change a PHP function only in `websocket.go`, then run `make generate`; the
  generated files are not edited by hand.
- Document user-facing changes in `docs/` and add them to `CHANGELOG.md`.

## Versions that move together

Some versions are pinned in several places and must change together, so
Dependabot leaves them alone:

- **FrankenPHP and Caddy**: `go.mod`, the build arguments in `Dockerfile`, and
  the builder image of the `test` job in `.github/workflows/ci.yml`. Caddy must
  be the version FrankenPHP uses.
- **Playwright**: the image in `e2e/compose.yaml` and `@playwright/test` in
  `e2e/browser/package.json`.
