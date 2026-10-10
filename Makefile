.PHONY: build run test e2e-docker bench bench-docker

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/relay ./cmd/relay

run: build
	./bin/relay

test:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }
	go vet ./...
	go vet -tags dockere2e ./e2e/docker
	go test -race -count=1 ./...

e2e-docker:
	./e2e/docker/run.sh

bench: build
	go run ./e2e/bench -spawn -relay-bin bin/relay

bench-docker:
	./e2e/docker/bench.sh
