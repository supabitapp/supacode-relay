# Development

Requires Go 1.25 or later.

```bash
make build         # bin/relay and bin/relay-router
make run           # listen on 127.0.0.1:8080
make test          # gofmt, go vet, and all tests with -race
make e2e-docker    # Docker Compose suite: one router, three nodes
make bench-paths   # direct node vs router-to-node on loopback
make bench-docker  # the same comparison inside Docker
```
