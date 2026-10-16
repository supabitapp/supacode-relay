.PHONY: build build-ts package-ts run test check test-race test-ts audit e2e-docker bench bench-docker

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/relay ./cmd/relay

run: build
	./bin/relay

build-ts:
	npm run build

package-ts: build-ts
	node scripts/package.mjs

test: check test-race test-ts

check:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }
	go mod tidy -diff
	go vet ./...
	go vet -tags dockere2e ./e2e/docker
	npm run check

test-race:
	go test -race -count=1 -shuffle=on ./...

test-ts:
	npm test
	RELAY_E2E_BIN="$$(pwd)/bin/relay-ts" go test -race -count=1 -shuffle=on ./e2e

audit:
	go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./cmd/relay
	npm audit

e2e-docker:
	./e2e/docker/run.sh

bench: build
	go run ./e2e/bench -spawn -relay-bin bin/relay

bench-docker:
	./e2e/docker/bench.sh
