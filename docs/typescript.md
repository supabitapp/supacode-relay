# TypeScript relay

The TypeScript implementation preserves the standalone relay protocol using incremental WebSocket framing on Node. The default build, container target and production deployment use Go. Production adoption requires the performance criteria below; passing compatibility tests alone does not authorize a cutover.

## Build and run

Use the Node version in [`.node-version`](../.node-version), npm and the Go toolchain described in [development](development.md).

```sh
npm ci
make build-ts
./bin/relay-ts
```

`bin/relay-ts` executes the current build's Node interpreter directly, with SIGUSR1 debugger activation disabled. It supports the same `healthcheck` and `metrics` subcommands and `RELAY_*` settings as the Go relay. The compiled application and copied dashboard assets live in `dist/`.

The [decoder](../src/transport/decoder.ts) reads payload chunks within individual frames. [Peer](../src/transport/peer.ts) owns fixed 16 KiB forwarding buffers, socket backpressure, serialized writes and absolute deadlines. Interrupted input never produces a successful final fragment. Pair reservations remain owned until both sockets finish cleanup.

The [configuration loader](../src/config.ts) rejects NaN admission rates, integers above JavaScript's exact integer range, and durations that would overflow Go's signed duration representation. Heartbeats also exclude values whose doubled pong deadline would overflow. Valid durations above Node's native timer range use segmented monotonic deadlines. These validation exceptions replace Go's accepted overflow/NaN cases with startup errors.

## Compatibility and memory checks

```sh
make -j2 test
RELAY_E2E_BIN="$(pwd)/bin/relay-ts" go test -race -count=1 -shuffle=on ./e2e
E2E_RELAY_TARGET=relay-ts E2E_RELAY_IMAGE=supaterm-relay-ts:e2e make e2e-docker
```

The external harness defaults to building Go. `RELAY_E2E_BIN` selects an executable instead, while retaining the Go clients and probe. Container scenarios remain under `make e2e-docker`. Both implementations run the same ordered-message, authentication, lifecycle, privacy and encrypted-exchange checks.

Additional tests require prefix delivery within one large frame, actual sender saturation, bounded process-memory growth across concurrent 32 MiB messages, safe redirects and raw-frame rejection. Node tests check arbitrary TCP segmentation, control-write deadlines and retained memory after partial-message and empty-fragment churn. Forced GC runs only in an isolated test child, outside the release artifact.

TypeScript metrics carry `metricsSchemaVersion: 2` and `runtime: "node"`. Relay counters and listener privacy retain their meanings. Go runtime gauges are omitted rather than redefined. The harness requires the selected runtime gauges to exist.

| Gauge | Meaning |
| --- | --- |
| `rssBytes` | Process resident memory |
| `heapUsedBytes`, `heapTotalBytes` | V8 heap usage and capacity |
| `externalBytes` | Native allocations tracked by V8, including buffers |
| `arrayBufferBytes` | ArrayBuffer allocations, already included in `externalBytes` |
| `activeResources` | Node resources keeping the event loop alive |
| `bufferedBytes` | Known retained application-buffer backing capacity, deduplicated across peers and control queues, plus socket readable lengths |
| `eventLoopDelayP99Ms`, `eventLoopDelayMaxMs` | Event-loop delay histogram since process start |
| `dataBufferBytesPerSocket` | Configured readable high-water mark plus the fixed forwarding buffer |

`bufferedBytes` is a diagnostic, not an RSS bound. Readable lengths can overlap backing allocations visible to the decoder. Runtime allocations, framing and kernel socket memory add overhead beyond the configured data buffers. External RSS checks and isolated retained-memory tests supplement this diagnostic. The dashboard displays Node memory and resources; any cgroup limit applies to the process, not just its V8 heap.

## Performance adoption criteria

The comparison uses three Go baseline rounds followed by three candidate rounds. Each relay gets two CPUs, 3 GiB of memory and 131,072 file descriptors in a separate container. The driver runs separately. Workloads use 1 KiB and 64 KiB messages with one and 32 clients.

Before measuring the candidate, the policy requires zero failures/corruption, median throughput at least 80% of Go, and median p99 latency no more than 125% of Go in every workload. The [comparison tool](../scripts/compare.mjs) records this policy, image digests, runtime metrics, Docker memory samples and raw results.

```sh
docker build --target relay -t supaterm-relay-go:compare .
docker build --target relay-ts -t supaterm-relay-ts:e2e .
docker build --target tools -t supaterm-relay-tools:e2e .
node scripts/compare.mjs docs/typescript-performance-results.json
```

Run this on an isolated Docker host. The script removes its own containers and network. These loopback/container measurements do not establish public TLS performance or production's maximum connection capacity. The fixed 50,000-connection admission ceiling remains a limit, not a capacity claim.

The [retained comparison](typescript-performance-results.json), measured on October 16, 2026 on Linux aarch64, fails adoption in all four workloads. Ratios compare the candidate's median across three rounds with Go's median across three rounds.

| Payload | Clients | Throughput relative to Go | p99 latency relative to Go | Adoption |
| --- | ---: | ---: | ---: | --- |
| 1 KiB | 1 | 80.7% | 1.29x | Fails latency |
| 1 KiB | 32 | 43.4% | 1.66x | Fails both |
| 64 KiB | 1 | 59.1% | 2.82x | Fails both |
| 64 KiB | 32 | 46.0% | 0.98x | Fails throughput |

No measured echo failed validation or contained corrupt data. Go remains the production default. Additional optimization and a passing comparison are required before staging a production cutover.

## Release and recovery

`make package-ts` creates `bin/relay-ts-linux-x64.tar.gz` and its SHA-256 sidecar. The packager verifies the official Node archive checksum and records runtime/application hashes in a manifest. The package includes the runtime, compiled application, assets, executable launcher and [candidate service unit](../deploy/supaterm-relay-ts.service). It excludes test programs.

The [candidate installer](../deploy/install-relay-ts.sh) validates the artifact, retains the current executable and unit, stops the current service, changes the versioned runtime/application together and checks private health. Activation failure restores the previous release. Its `--rollback` option restores that same saved executable, unit and runtime target.

The packaged Linux x64 release passed six smoke exchanges in an unprivileged, read-only Linux/amd64 container. Its healthcheck, metrics, quiet SIGUSR1 handling and SIGTERM shutdown also passed. This validates the release launcher and bundled runtime; it does not replace a systemd installation/rollback rehearsal.

The production workflow remains unchanged. Before adoption, rehearse installation and rollback on isolated staging, promote the exact tested artifact, retain the CI dependency and serialized deployment, and run both existing public smoke probes. Also verify `relay.supacode.sh` and pairing across public aliases. Both aliases must reach one registry. A cutover or rollback disconnects current sessions, which must reconnect.
