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

The decoder unmasks aligned 32-bit words, with bytewise handling for unaligned edges. Peer batches writes during each bounded input pump and flushes on every exit, including errors and backpressure. Complete chunks bypass the intermediate forwarding copy; queued frames retain their own payload copy. Socket read and write high-water marks are 64 KiB each, allowing larger batches at the cost of more buffering per busy connection.

The [configuration loader](../src/config.ts) rejects NaN admission rates, integers above JavaScript's exact integer range, and durations that would overflow Go's signed duration representation. Heartbeats also exclude values whose doubled pong deadline would overflow. Valid durations above Node's native timer range use segmented monotonic deadlines. These validation exceptions replace Go's accepted overflow/NaN cases with startup errors.

## Compatibility and memory checks

```sh
make -j2 test
RELAY_E2E_BIN="$(pwd)/bin/relay-ts" go test -race -count=1 -shuffle=on ./e2e
E2E_RELAY_TARGET=relay-ts E2E_RELAY_IMAGE=supaterm-relay-ts:e2e make e2e-docker
```

The external harness defaults to building Go. `RELAY_E2E_BIN` selects an executable instead, while retaining the Go clients and probe. Container scenarios remain under `make e2e-docker`. Both implementations run the same ordered-message, authentication, lifecycle, privacy and encrypted-exchange checks.

Additional tests require prefix delivery within one large frame, actual sender saturation, bounded process-memory growth across concurrent 32 MiB messages, safe redirects and raw-frame rejection. Node tests check arbitrary TCP segmentation, unmasking across alignments and mask offsets, control-write deadlines and retained memory after partial-message and empty-fragment churn. Forced GC runs only in an isolated test child, outside the release artifact.

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
node scripts/compare.mjs bin/typescript-comparison.json
```

Run this on an isolated Docker host. The script removes its own containers and network. These loopback/container measurements do not establish public TLS performance or production's maximum connection capacity. The fixed 50,000-connection admission ceiling remains a limit, not a capacity claim.

The [retained comparison](typescript-optimized-performance-results.json), measured on October 16, 2026 on Linux aarch64, passes adoption in the 64 KiB, 32-client workload. The other three workloads fail. Ratios compare the candidate's median across three rounds with Go's median across three rounds.

| Payload | Clients | Throughput relative to Go | p99 latency relative to Go | Adoption |
| --- | ---: | ---: | ---: | --- |
| 1 KiB | 1 | 90.0% | 1.31x | Fails latency |
| 1 KiB | 32 | 75.2% | 1.59x | Fails both |
| 64 KiB | 1 | 95.9% | 2.43x | Fails latency |
| 64 KiB | 32 | 84.3% | 0.59x | Passes |

Against the [earlier candidate measurement](typescript-performance-results.json), the 32-client throughput medians improve 1.82x for 1 KiB messages and 1.85x for 64 KiB messages. These comparisons use the same container limits and workloads, measured in separate runs.

No measured echo failed validation or contained corrupt data. Go remains the production default. Additional optimization and a passing comparison in every workload are required before staging a production cutover.

### Further optimization experiments

The strongest measured prototype combines reusable write timers, a bounded output-buffer cache and Node's `--max-semi-space-size=1` setting. Against the TypeScript baseline at commit `6236c94`, it improves 32-client throughput by 18.8% for 1 KiB messages and 21.2% for 64 KiB messages. The corresponding p99 medians fall by 46.6% and 17.6%. These experimental patches are retained in the [exploration results](typescript-exploration-results.json); they are not applied to the application.

The confirmation uses three interleaved rounds on October 16, 2026, balancing implementation order and reversing workload order in the middle round. Each workload gets three seconds of warmup and eight seconds of measurement. Relay limits remain two CPUs and 3 GiB; the driver gets four CPUs. The host is an Apple M5 Pro with a nine-CPU Linux aarch64 Docker VM, Node 24.22.0 and Go 1.27.2. Values below are medians of per-run measurements, including all outliers.

| Payload | Clients | Current TS messages/s | Prototype messages/s | Current TS p99 | Prototype p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 KiB | 1 | 112,504 | 111,350 | 93.2 us | 95.8 us |
| 1 KiB | 32 | 207,225 | 246,185 | 1,671.8 us | 893.3 us |
| 64 KiB | 1 | 9,985 | 11,640 | 999.8 us | 498.5 us |
| 64 KiB | 32 | 12,303 | 14,910 | 12,733.8 us | 10,491.2 us |

The single-client 1 KiB case changes by -1.0% in throughput and +2.8% in p99, so the prototype does not establish an improvement there. Single-client 64 KiB throughput rises 16.6%, while p99 falls 50.1%. Median end-of-run RSS falls from 211.3 MiB to 101.1 MiB. These RSS snapshots describe these workloads, not peak memory or connection capacity.

| Payload | Clients | Prototype throughput relative to Go | Prototype p99 relative to Go | Threshold result |
| --- | ---: | ---: | ---: | --- |
| 1 KiB | 1 | 100.8% | 1.23x | Passes |
| 1 KiB | 32 | 91.6% | 0.80x | Passes |
| 64 KiB | 1 | 119.1% | 0.87x | Passes |
| 64 KiB | 32 | 72.7% | 0.70x | Fails throughput |

This comparison remains below the adoption criteria overall. It is separate from the earlier two-CPU-driver comparison above. In a two-round sensitivity check, increasing the driver from two to four CPUs changed Go throughput by -10.4% for 1 KiB messages and +40.8% for 64 KiB messages at 32 clients. The TypeScript changes were -2.0% and -7.2%. Driver configuration materially affects the relative result; four CPUs do not establish spare driver capacity.

The two-round screens identify which changes merit further work:

- Reusing idle write timers improves small-message throughput by 8.8%. Deadline renewal retains an existing earlier wakeup, idle completion disarms it, and teardown cancels it. Read suspension still cancels its timer.
- Recycling completed output buffers improves large-message throughput by 11.5%. Each peer caches at most four owned 16,388-byte buffers, returned only after the socket write callback. The cache can retain 65,552 bytes per previously busy peer and increases backing allocation for queued 4 KiB frames. The retained-buffer metric includes the cache, and socket close clears it.
- The smaller V8 semi-space substantially reduces small-message p99 in both screening sets, but does not consistently improve large-message throughput by itself. The combined prototype produces the stronger measured result.
- Larger socket reads, capacity-limited reads, short-length parsing without BigInt, borrowed-payload writes and a smaller V8 worker pool show little consistent throughput gain. The driver's 64 KiB messages arrive as 4 KiB masked frames, so a single-large-frame copy-bypass hypothesis does not match this traffic.

Enabling the image's five-second Docker healthcheck changes throughput by +0.2% for 1 KiB and -1.2% for 64 KiB messages in a two-round screen. The p99 changes are -2.0% and +7.0%, respectively. These short samples do not establish healthchecks as the main cause of the performance gap. The other experiments disable automatic healthchecks and retain explicit readiness checks.

The combined prototype passes all 24 Node tests, the external race-enabled compatibility suite, all five container scenarios, and targeted checks for write ownership, cache bounds, error cleanup and deadline rearming. No measured echo failed or contained corrupt data. Buffer pooling uses `Buffer.allocUnsafeSlow` to own each backing allocation; the pinned Node runtime has a 65,536-byte default Buffer pool.

The next unmeasured option is to coalesce directly into reusable framed output slabs and batch write completion bookkeeping. This could remove a payload copy and some allocation, but requires ownership, control-frame ordering, backpressure and error-delivery checks before benchmarking. Borrowed payloads can retain an entire input backing buffer, and completion callback reuse must preserve error ordering.

The results artifact includes raw benchmark output, median/range summaries, CPU counters, parsed Docker statistics, exact experimental patches, validation commands and reproduction source. Resource counters and sampled peaks cover entire runs, including setup and warmup. Early screens lack per-variant image IDs. The convenience reproducer is syntax checked but has not been replayed from a clean checkout; it uses the ambient Go toolchain and Docker architecture. These short, closed-loop tests do not establish long-run stability, public TLS performance or production connection capacity.

## Release and recovery

`make package-ts` creates `bin/relay-ts-linux-x64.tar.gz` and its SHA-256 sidecar. The packager verifies the official Node archive checksum and records runtime/application hashes in a manifest. The package includes the runtime, compiled application, assets, executable launcher and [candidate service unit](../deploy/supaterm-relay-ts.service). It excludes test programs.

The [candidate installer](../deploy/install-relay-ts.sh) validates the artifact, retains the current executable and unit, stops the current service, changes the versioned runtime/application together and checks private health. Activation failure restores the previous release. Its `--rollback` option restores that same saved executable, unit and runtime target.

The packaged Linux x64 release passed six smoke exchanges in an unprivileged, read-only Linux/amd64 container. Its healthcheck, metrics, quiet SIGUSR1 handling and SIGTERM shutdown also passed. This validates the release launcher and bundled runtime; it does not replace a systemd installation/rollback rehearsal.

The production workflow remains unchanged. Before adoption, rehearse installation and rollback on isolated staging, promote the exact tested artifact, retain the CI dependency and serialized deployment, and run both existing public smoke probes. Also verify `relay.supacode.sh` and pairing across public aliases. Both aliases must reach one registry. A cutover or rollback disconnects current sessions, which must reconnect.
