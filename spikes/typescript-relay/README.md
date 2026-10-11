# TypeScript relay spike

The results below are exploratory macOS runs and are unsuitable for the final cross-language chart. That chart requires the coordinator's sequential Linux/arm64 runs with one unchanged Go driver. See the [Linux handoff](LINUX.md) for portable Node and Bun relay wrappers, runtime pins, and compatibility checks.

This directory contains a runnable WebSocket relay and a benchmark driver that exercise the protocol in [docs/protocol.md](../../docs/protocol.md). The spike is isolated from the production Go relay. It covers host registration and Ed25519 challenge authentication, client connect, host accept, binary and text message forwarding, pair timeout, close notification, payload sizes, concurrency, warmup, measurement, sequence validation, and process sampling.

The benchmark sends four binary messages in flight per client. Each message carries a sequence number, a monotonic send timestamp, and random payload bytes. It reports messages per second, payload MiB per second, RTT p50 and p99 in microseconds, failures, corruption, relay CPU as a percentage of one core, and peak relay RSS. Results count only the measurement window after warmup. The checked-in JSON files are the complete machine-readable output:

- [`results/go.json`](results/go.json) is the original Go relay measured with the TypeScript driver.
- [`results/node.json`](results/node.json) is the TypeScript relay and driver on Node 26.1.0.
- [`results/bun.json`](results/bun.json) is the same compiled JavaScript on Bun 1.4.2.
- [`results/go-driver-against-typescript.json`](results/go-driver-against-typescript.json) is the unchanged repository Go driver against the TypeScript relay.
- [`results/go-driver-control.json`](results/go-driver-control.json) is the unchanged repository Go driver against the original Go relay.

## Setup

The dependency versions are pinned in [`package.json`](package.json) and [`package-lock.json`](package-lock.json): `ws` 8.22.0 at runtime, with TypeScript 5.9.3, `@types/node` 24.10.1, and `@types/ws` 8.18.1 for the build. Node 26 or Bun 1.4.2 can run the compiled output.

```sh
cd spikes/typescript-relay
npm ci
npm run build
```

Run the spike directly:

```sh
RELAY_ADDR=127.0.0.1:8080 node dist/relay.js
```

The relay prints a JSON listening event. It implements `/v1/control`, `/v1/connect`, `/v1/accept`, `/healthz`, and aggregate `/metrics`.

## Reproduce the comparison

Build the production relay from the repository root:

```sh
go build -o /tmp/supacode-relay-go ./cmd/relay
```

Run the commands from `spikes/typescript-relay`; output paths are relative to that directory. They use the same six cases, four echo hosts, four in-flight messages per client, two seconds of warmup, and five seconds of measurement per case. The `--spawn` option starts the relay, waits for its JSON listening event, samples its PID, and shuts it down after the matrix.

```sh
cd spikes/typescript-relay
npm run bench -- --spawn --relay-bin node --relay-arg dist/relay.js \
  --payloads 64,1024,65536 --clients 1,32 --hosts 4 \
  --warmup-ms 2000 --duration-ms 5000 --output results/node.json \
  --label typescript-relay-node

bun dist/bench.js --spawn --relay-bin bun --relay-arg dist/relay.js \
  --payloads 64,1024,65536 --clients 1,32 --hosts 4 \
  --warmup-ms 2000 --duration-ms 5000 --output results/bun.json \
  --label typescript-relay-bun-1.4.2

npm run bench -- --spawn --relay-bin /tmp/supacode-relay-go \
  --payloads 64,1024,65536 --clients 1,32 --hosts 4 \
  --warmup-ms 2000 --duration-ms 5000 --output results/go.json \
  --label go-relay
```

The recorded run used macOS 27.0 on arm64 with 18 logical CPUs, Go 1.27.1 for the relay build, Node v26.1.0 for the baseline and Go comparison driver, and Bun 1.4.2 for the Bun run. The Go run uses the Node driver so the workload and measurement code stay the same. `runtime` and `runtimeVersion` in each JSON file identify the benchmark runtime; Bun's Node compatibility value is retained separately as `nodeCompatVersion`.

## Validation with the repository Go driver

These two result files come from the unchanged `e2e/bench/main.go` driver. Each command starts one fresh relay process with `-spawn`, runs all six cases, and shuts that relay down before the command exits. The driver supplies the relay process with `RELAY_ADDR=127.0.0.1:0`, `RELAY_PRIVATE_ADDR=`, `RELAY_ADMISSION_RATE=1000000`, `RELAY_MAX_CONNS=100000`, and `RELAY_MAX_CONNS_PER_IP=100000`.

Run this validation block from the repository root. The TypeScript entrypoint needs an executable wrapper because the unchanged Go driver accepts one relay binary path:

```sh
cat >/tmp/supacode-relay-typescript-wrapper <<'SH'
#!/bin/sh
exec node /Users/khoi/.supacode/worktrees/supacode-relay/supacode-relay-spike-typescript/spikes/typescript-relay/dist/relay.js
SH
chmod +x /tmp/supacode-relay-typescript-wrapper
go build -o /tmp/supacode-relay-go ./cmd/relay

go run ./e2e/bench/main.go -spawn \
  -relay-bin /tmp/supacode-relay-typescript-wrapper \
  -payloads 64,1024,65536 -clients 1,32 -hosts 4 -inflight 4 \
  -warmup 2s -duration 5s \
  > spikes/typescript-relay/results/go-driver-against-typescript.json

go run ./e2e/bench/main.go -spawn \
  -relay-bin /tmp/supacode-relay-go \
  -payloads 64,1024,65536 -clients 1,32 -hosts 4 -inflight 4 \
  -warmup 2s -duration 5s \
  > spikes/typescript-relay/results/go-driver-control.json
```

Both files contain six cases with `failures=0` and `corrupt=0` for every case. The recorded environment was macOS 27.0 arm64, Go 1.27.1, Node v26.1.0, and 18 logical CPUs. The existing Node and Bun result files were not changed by this validation.

| Go driver target | payload | clients | messages/sec | MiB/sec | RTT p50/p99 us | CPU / RSS | failures/corrupt |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TypeScript relay | 64 B | 1 | 53,493.8 | 3.3 | 62.7 / 156.6 | 75.1% / 79.3 MiB | 0 / 0 |
| TypeScript relay | 64 B | 32 | 94,843.0 | 5.8 | 1,177.8 / 2,483.3 | 98.7% / 88.6 MiB | 0 / 0 |
| TypeScript relay | 1,024 B | 1 | 40,891.4 | 39.9 | 74.8 / 223.0 | 74.5% / 92.9 MiB | 0 / 0 |
| TypeScript relay | 1,024 B | 32 | 86,425.8 | 84.4 | 1,346.8 / 2,884.3 | 98.3% / 112.5 MiB | 0 / 0 |
| TypeScript relay | 65,536 B | 1 | 6,867.4 | 429.2 | 495.9 / 1,151.1 | 101.5% / 249.6 MiB | 0 / 0 |
| TypeScript relay | 65,536 B | 32 | 6,724.6 | 420.3 | 18,108.9 / 25,673.5 | 114.7% / 311.8 MiB | 0 / 0 |
| Original Go relay | 64 B | 1 | 57,296.0 | 3.5 | 56.8 / 146.1 | 95.5% / 20.3 MiB | 0 / 0 |
| Original Go relay | 64 B | 32 | 61,760.4 | 3.8 | 1,947.5 / 3,272.5 | 281.9% / 23.0 MiB | 0 / 0 |
| Original Go relay | 1,024 B | 1 | 35,309.0 | 34.5 | 86.0 / 418.0 | 82.3% / 23.8 MiB | 0 / 0 |
| Original Go relay | 1,024 B | 32 | 65,039.0 | 63.5 | 1,920.9 / 3,024.2 | 310.9% / 24.5 MiB | 0 / 0 |
| Original Go relay | 65,536 B | 1 | 4,130.8 | 258.2 | 725.8 / 3,974.7 | 98.5% / 24.8 MiB | 0 / 0 |
| Original Go relay | 65,536 B | 32 | 5,755.4 | 359.7 | 21,205.9 / 41,677.3 | 255.1% / 24.9 MiB | 0 / 0 |

## Semantic differences

The control and pairing messages follow the Go protocol: endpoint IDs are lowercase hex SHA-256 of the raw 32-byte Ed25519 public key, signatures cover `supacode-relay-v1\n` plus endpoint ID and nonce, tokens and connection IDs are unpadded Base64URL, and message boundaries and binary or text type are forwarded.

The `ws` package exposes complete messages rather than a streaming reader. The spike therefore buffers each WebSocket message in user space and queues pre-accept client messages up to its configured queue limits. It does not reproduce the Go relay's fixed writer-buffer streaming, TCP backpressure behavior for partial messages, heartbeat implementation, admission rate limiter, trusted-proxy handling, private listener, or detailed diagnostics. These differences make the spike useful for protocol and workload comparison, but its memory and large-message results are not a production capacity estimate.

## Results

All 18 recorded cases completed with zero failures and zero corrupt messages. These are the final reruns after pinning `ws` 8.22.0. Values are rounded as emitted by the driver. CPU is percent of one core and RSS is peak MiB for the relay process.

| relay | runtime | payload | clients | messages/sec | MiB/sec | RTT p50/p99 us | CPU | RSS | failures/corrupt |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Go | Node 26.1.0 | 64 B | 1 | 55,053.2 | 3.4 | 55.9 / 188.5 | 84.1% | 20.5 | 0 / 0 |
| Go | Node 26.1.0 | 64 B | 32 | 82,137.8 | 5.0 | 1,418.5 / 3,289.8 | 135.1% | 25.0 | 0 / 0 |
| Go | Node 26.1.0 | 1,024 B | 1 | 36,313.0 | 35.5 | 80.2 / 403.8 | 76.2% | 22.8 | 0 / 0 |
| Go | Node 26.1.0 | 1,024 B | 32 | 84,371.8 | 82.4 | 1,342.5 / 3,167.5 | 110.5% | 24.2 | 0 / 0 |
| Go | Node 26.1.0 | 65,536 B | 1 | 5,961.8 | 372.6 | 599.7 / 1,509.2 | 55.5% | 24.2 | 0 / 0 |
| Go | Node 26.1.0 | 65,536 B | 32 | 6,783.2 | 424.0 | 18,498.0 / 26,763.8 | 61.4% | 24.3 | 0 / 0 |
| TypeScript | Node 26.1.0 | 64 B | 1 | 14,526.6 | 0.9 | 231.3 / 1,002.3 | 43.4% | 70.0 | 0 / 0 |
| TypeScript | Node 26.1.0 | 64 B | 32 | 8,473.6 | 0.5 | 16,033.8 / 21,829.8 | 26.1% | 74.8 | 0 / 0 |
| TypeScript | Node 26.1.0 | 1,024 B | 1 | 52,063.8 | 50.8 | 65.2 / 174.8 | 80.5% | 78.7 | 0 / 0 |
| TypeScript | Node 26.1.0 | 1,024 B | 32 | 71,961.6 | 70.3 | 1,587.0 / 3,276.4 | 94.9% | 93.3 | 0 / 0 |
| TypeScript | Node 26.1.0 | 65,536 B | 1 | 6,799.0 | 424.9 | 525.5 / 1,143.4 | 76.0% | 201.8 | 0 / 0 |
| TypeScript | Node 26.1.0 | 65,536 B | 32 | 7,526.4 | 470.4 | 16,522.0 / 28,078.2 | 85.4% | 232.1 | 0 / 0 |
| TypeScript | Bun 1.4.2 | 64 B | 1 | 35,108.8 | 2.1 | 84.5 / 963.7 | 64.4% | 42.7 | 0 / 0 |
| TypeScript | Bun 1.4.2 | 64 B | 32 | 122,436.2 | 7.5 | 945.6 / 2,340.6 | 104.8% | 44.2 | 0 / 0 |
| TypeScript | Bun 1.4.2 | 1,024 B | 1 | 50,161.4 | 49.0 | 64.7 / 202.9 | 88.6% | 48.6 | 0 / 0 |
| TypeScript | Bun 1.4.2 | 1,024 B | 32 | 110,321.4 | 107.7 | 1,035.6 / 2,624.0 | 102.1% | 51.8 | 0 / 0 |
| TypeScript | Bun 1.4.2 | 65,536 B | 1 | 8,553.8 | 534.6 | 345.8 / 1,542.9 | 68.1% | 57.3 | 0 / 0 |
| TypeScript | Bun 1.4.2 | 65,536 B | 32 | 14,392.2 | 899.5 | 8,318.7 / 13,981.3 | 79.3% | 62.3 | 0 / 0 |
