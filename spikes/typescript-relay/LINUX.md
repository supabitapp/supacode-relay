# Linux relay handoff

The Rust coordinator owns all timed runs in the shared Linux/arm64 environment. Use [run-node.sh](run-node.sh) and [run-bun.sh](run-bun.sh) as the unchanged repository Go driver's `-relay-bin` targets. Both wrappers execute the same `dist/relay.js` and preserve its PID with `exec`, so the driver samples the relay's CPU and RSS.

## Build and runtime pins

Build once with the committed npm lockfile, then reuse that output for both runtimes. Both wrappers resolve the entrypoint relative to their own directory and work from any current directory. They take no runtime options, do not start a benchmark driver, and preserve all `RELAY_*` settings supplied by the Go driver.

The coordinator selected Linux/arm64 Node 22.20.0 and Bun 1.3.0 archives. These differ from the exploratory macOS versions, Node 26.1.0 and Bun 1.4.2. Verify the installed versions before timing; do not label Linux runs with the macOS versions.

Run from the repository root inside the coordinator's container:

```sh
export NODE_BIN="$(command -v node)"
export BUN_BIN="$(command -v bun)"
test "$("$NODE_BIN" --version)" = v22.20.0
test "$("$BUN_BIN" --version)" = 1.3.0
npm ci --prefix spikes/typescript-relay
npm run build --prefix spikes/typescript-relay
sha256sum spikes/typescript-relay/dist/relay.js e2e/bench/main.go
go build -o /tmp/relay-bench ./e2e/bench
```

`NODE_BIN` and `BUN_BIN` select one executable each; their defaults are `node` and `bun` from `PATH`. The build uses TypeScript 5.9.3 with ES2022/NodeNext output and `ws` 8.22.0. Do not run a separate Bun install or build. Source review confirms that `src/relay.ts` has no runtime-specific branches: both use `WebSocketServer({ noServer: true, maxPayload })` and the same environment-derived limits. Runtime implementations of Node APIs may still differ.

## Compatibility checks

The existing [Go smoke probe](../../e2e/docker/probe/main.go) uses the same `internal/endpoint` registration, connect, and accept helpers as the unchanged Go benchmark. It checks message bytes and text/binary type in both directions. It has no throughput measurement window.

Build the probe before timing begins:

```sh
go build -o /tmp/relay-probe ./e2e/docker/probe
RELAY_ADDR=127.0.0.1:8080 ./spikes/typescript-relay/run-bun.sh
```

While that relay runs, invoke the probe from another shell, then stop the relay:

```sh
/tmp/relay-probe -mode smoke -url ws://127.0.0.1:8080 -n 4
```

The local macOS check passed for both wrappers: four authenticated host/client pairs per runtime, each with 20 mixed text/binary messages. It also checked startup on an ephemeral port, invocation from `/tmp`, PID replacement, and clean SIGTERM exit. Bun 1.4.2 therefore supports the Go endpoint protocol locally. The coordinator must confirm the Linux Bun 1.3.0 result before including it in the common chart.

## Timed commands for the coordinator

These commands are handoff instructions, not additional measurements from this branch. Run them sequentially as part of the coordinator's rotated suite, after all builds and smoke checks finish. Each invocation uses a fresh relay process for its complete six-case matrix.

```sh
repo=$(pwd)
/tmp/relay-bench -spawn -relay-bin "$repo/spikes/typescript-relay/run-node.sh" \
  -payloads 64,1024,65536 -clients 1,32 -hosts 4 -inflight 4 \
  -warmup 2s -duration 5s > /tmp/go-driver-linux-node.json
/tmp/relay-bench -spawn -relay-bin "$repo/spikes/typescript-relay/run-bun.sh" \
  -payloads 64,1024,65536 -clients 1,32 -hosts 4 -inflight 4 \
  -warmup 2s -duration 5s > /tmp/go-driver-linux-bun.json
```

Record the actual executable paths, version output, image identity, relay and driver hashes, and environment with the common results. Keep the existing files under `results/` as exploratory evidence; they mix drivers and lack controlled host isolation. The full-message buffering and omitted production controls described in the [README](README.md#semantic-differences) still apply.
