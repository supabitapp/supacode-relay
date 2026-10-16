# Development

Use Go 1.27.2 or later. The module selects Go 1.27.2 through its `toolchain` directive; older Go installations download it when automatic toolchain selection is enabled.

Testing also requires the Node version in [`.node-version`](../.node-version). Run `npm ci` before `make test` or `make audit`. The TypeScript implementation uses pinned development dependencies and no runtime npm dependencies.

| Command | Purpose |
| --- | --- |
| `make build` | Build `bin/relay` |
| `make run` | Listen on `127.0.0.1:8080` |
| `make build-ts` | Compile TypeScript and create the executable `bin/relay-ts` launcher |
| `make package-ts` | Package the compiled application and checksum-verified official Linux Node runtime |
| `make test` | Check formatting, module tidiness and TypeScript types; run both vet commands, Go race tests, Node tests and the Go external suite against TypeScript |
| `make audit` | Run `govulncheck` against the Go relay and audit npm dependencies. Standard library results depend on the local Go version |
| `make e2e-docker` | Verify one relay through the published port and a separate probe container |
| `make bench` | Measure standalone relay echo throughput and latency on loopback |
| `make bench-docker` | Measure the relay from another container and retain resource samples |

Pull requests run `make -j2 test` and `make audit` in [CI](../.github/workflows/ci.yml). Code changes pushed to `main` run `make -j2 test` before deploying. The [CI ledger](ci-pipeline.md) records timings and the limits of the measured gain.

`test` requires `check`, `test-race` and `test-ts`. The Go race command remains `go test -race -count=1 -shuffle=on ./...`. `test-ts` runs Node tests and repeats the external Go suite against `bin/relay-ts`. See [TypeScript validation](typescript.md) for the container override, runtime metrics and isolated benchmark commands.

The container suite checks the public surface, pairing and ordered streaming across 12 hosts, duplicate identity replacement, graceful restart and abrupt restart, per-client limits, and log privacy. It removes its containers and network on completion. Set `E2E_KEEP=1` to retain them, `E2E_SKIP_BUILD=1` to use existing images, or `E2E_RELAY_PORT` to change the published port.

The benchmark accepts `-url` for an existing relay or `-spawn` for a local process. Payload sizes, client counts, message window, and measurement duration are configurable. Spawned relays default the per-host client network quota to the host capacity and preserve an explicit `RELAY_MAX_CLIENTS_PER_HOST_PER_IP` value. Container benchmarks set that quota to the host capacity. Existing relays retain their [configured quotas](configuration.md). Verify payloads and stop on failed echoes when testing a live service; the retained [transport measurements](multi-node.md#live-transport-investigation) explain why sender bandwidth and queued data affect results.

The [fuzz and memory experiments](fuzz-testing.md) describe bounded fuzz campaigns, generated lifecycle checks, and opt-in isolated memory churn. Fixed fuzz seeds run with `make test`.
