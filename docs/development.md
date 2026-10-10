# Development

Requires Go 1.25 or later.

| Command | Purpose |
| --- | --- |
| `make build` | Build `bin/relay` |
| `make run` | Listen on `127.0.0.1:8080` |
| `make test` | Check formatting and `go.mod` tidiness, vet all packages and the container suite, and run all Go tests in shuffled order with the race detector |
| `make audit` | Report known vulnerabilities reachable from the relay binary with `govulncheck`. Standard library results depend on the local Go version |
| `make e2e-docker` | Verify one relay through the published port and a separate probe container |
| `make bench` | Measure standalone relay echo throughput and latency on loopback |
| `make bench-docker` | Measure the relay from another container and retain resource samples |

Pull requests run `make test` and `make audit` in [CI](../.github/workflows/ci.yml). Code changes pushed to `main` run `make test` before deploying.

The container suite checks the public surface, pairing and ordered streaming across 12 hosts, duplicate identity replacement, graceful restart and abrupt restart, per-client limits, and log privacy. It removes its containers and network on completion. Set `E2E_KEEP=1` to retain them, `E2E_SKIP_BUILD=1` to use existing images, or `E2E_RELAY_PORT` to change the published port.

The benchmark accepts `-url` for an existing relay or `-spawn` for a local process. Payload sizes, client counts, message window, and measurement duration are configurable. Verify payloads and stop on failed echoes when testing a live service; the retained [transport measurements](multi-node.md#live-transport-investigation) explain why sender bandwidth and queued data affect results.

The [fuzz and memory experiments](fuzz-testing.md) describe bounded fuzz campaigns, generated lifecycle checks, and opt-in isolated memory churn. Fixed fuzz seeds run with `make test`.
