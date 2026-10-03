.PHONY: build run test

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/relay ./cmd/relay

run: build
	./bin/relay

test:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }
	go vet ./...
	go test -race -count=1 ./...
