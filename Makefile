PHP_CONFIG ?= php-config
TAGS ?= nobadger,nomysql,nopgx,nowatcher,deprecated_topic,deprecated_claim
# FrankenPHP binary of the version in go.mod, for its extension generator.
FRANKENPHP ?= ./frankenphp
# gen_stub.php from the PHP sources of the PHP version built against.
GEN_STUB_SCRIPT ?=
IMAGE ?= frankenphp-websocket
BENCHTIME ?= 2s
GOLANGCI_LINT_VERSION = v2.15.0
GOLANGCI_LINT = $(CURDIR)/bin/golangci-lint-$(GOLANGCI_LINT_VERSION)
# Pinned, as older xcaddy releases ignore XCADDY_GO_BUILD_FLAGS and with it the build tags.
XCADDY ?= go run github.com/caddyserver/xcaddy/cmd/xcaddy@v0.4.7

FRANKENPHP_VERSION = $(shell go list -m -f '{{.Version}}' github.com/dunglas/frankenphp)
CADDY_VERSION = $(shell go list -m -f '{{.Version}}' github.com/caddyserver/caddy/v2)

# Without php-config, only the targets that need no PHP work, such as e2e.
ifneq ($(shell command -v $(PHP_CONFIG)),)
export CGO_ENABLED = 1
export CGO_CFLAGS = $(shell $(PHP_CONFIG) --includes)
# The explicit lib directory is for PHP builds, like Homebrew's, whose ldflags leave it out.
export CGO_LDFLAGS = -L$(shell $(PHP_CONFIG) --prefix)/lib $(shell $(PHP_CONFIG) --ldflags) $(shell $(PHP_CONFIG) --libs)
endif

COMPOSE = docker compose -f e2e/compose.yaml --profile browser

.PHONY: test bench e2e check lint build generate docker

test:
	go test -race -tags=$(TAGS) ./...

bench:
	go test -run '^$$' -bench . -benchtime $(BENCHTIME) -benchmem -tags=$(TAGS) ./...

# Runs the image as a cluster on NATS and tests it from outside, with Go clients and real browsers.
e2e:
	$(COMPOSE) up --build --detach nats app other
	@status=0; \
	go test -count=1 -tags=e2e ./e2e/ || status=$$?; \
	if [ $$status -eq 0 ]; then $(COMPOSE) run --rm browser || status=$$?; fi; \
	$(COMPOSE) down --volumes; \
	exit $$status

check:
	@unformatted="$$(find . -name '*.go' -not -path './.*' -exec gofmt -l {} +)"; if [ -n "$$unformatted" ]; then echo "not gofmt-ed:"; echo "$$unformatted"; exit 1; fi
	go vet -tags=$(TAGS),e2e ./...

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run ./...

# Built without cgo, which golangci-lint does not need and PHP's linker flags would break.
$(GOLANGCI_LINT):
	mkdir -p bin
	CGO_ENABLED=0 GOBIN=$(CURDIR)/bin go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	mv bin/golangci-lint $@

build:
	XCADDY_GO_BUILD_FLAGS="-ldflags='-w -s' -tags=$(TAGS)" $(XCADDY) build $(CADDY_VERSION) \
		--output frankenphp \
		--with github.com/dunglas/frankenphp@$(FRANKENPHP_VERSION) \
		--with github.com/dunglas/frankenphp/caddy@$(FRANKENPHP_VERSION) \
		--with github.com/chapterjason/frankenphp-websocket=.

# The generator writes next to its source, and a README.md there too unless one exists.
generate:
	@if [ -z "$(GEN_STUB_SCRIPT)" ]; then echo "set GEN_STUB_SCRIPT to the gen_stub.php of the PHP sources"; exit 1; fi
	@if [ ! -f README.md ]; then echo "README.md is missing; the generator would write its own"; exit 1; fi
	GEN_STUB_SCRIPT="$(abspath $(GEN_STUB_SCRIPT))" $(FRANKENPHP) extension-init websocket.go

docker:
	docker build -t $(IMAGE) .
