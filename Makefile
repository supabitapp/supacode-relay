.PHONY: build run test e2e-docker bench-paths bench-docker

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/relay ./cmd/relay
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/relay-router ./cmd/router

run: build
	./bin/relay

test:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }
	go vet ./...
	go vet -tags dockere2e ./e2e/docker
	go test -race -count=1 ./...

e2e-docker:
	./e2e/docker/run.sh

bench-paths: build
	go run ./e2e/pathbench -spawn -relay-bin bin/relay -router-bin bin/relay-router

bench-docker:
	./e2e/docker/bench.sh
