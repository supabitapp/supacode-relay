# Rust relay spike

This directory is an isolated, runnable relay spike. It leaves the production Go relay unchanged and provides two binaries:

- `rust-relay` implements the authenticated host control flow, client registration, host acceptance, pair timeout, and binary/text WebSocket forwarding described in [`docs/protocol.md`](../../docs/protocol.md).
- `rust-bench` is an independent driver with the same host-register, client-connect, host-accept, echo, payload-size, concurrency, warmup, measurement, RTT, failure, corruption, CPU, and RSS contract as [`e2e/bench/main.go`](../../e2e/bench/main.go).

The spike uses Tokio, Axum WebSocket upgrades, and `tokio-tungstenite`. It is intended for local transport comparison, not production deployment.

## Build

From the repository root:

```sh
cargo build --manifest-path spikes/rust-relay/Cargo.toml --release --bins
```

The binaries are written under `spikes/rust-relay/target/release/`.

## Run the relay

```sh
RELAY_ADDR=127.0.0.1:8080 \
  spikes/rust-relay/target/release/rust-relay
```

The first stdout line is machine-readable and has the form `{"event":"listening","address":"127.0.0.1:8080"}`. `RELAY_PAIR_TIMEOUT_MS` and `RELAY_MAX_MESSAGE_BYTES` are supported for protocol experiments. The benchmark passes the same high admission limits as the Go benchmark.

## Run the benchmark

The default matrix is the requested `64,1024,65536` byte payloads and `1,32` concurrent clients. `--spawn` starts the selected relay, discovers its loopback listener from the JSON announcement, samples its process, and stops it when the run completes:

```sh
spikes/rust-relay/target/release/rust-bench \
  --spawn \
  --relay-bin spikes/rust-relay/target/release/rust-relay \
  --output spikes/rust-relay/results/rust-relay.json
```

To exercise an existing relay, replace `--spawn --relay-bin ...` with `--url ws://127.0.0.1:8080`. The driver prints a JSON document to stdout and can also write it with `--output`. Each result contains `messagesPerSec`, `payloadMiBPerSec`, `rttMicros` (`p50`, `p99`, `max`), `failures`, `corrupt`, and sampled `processes.relay` CPU/RSS.

The unchanged Go driver can exercise the Rust relay, which is an interoperability check:

```sh
go run ./e2e/bench \
  -url ws://127.0.0.1:8080 \
  -payloads 64,1024,65536 \
  -clients 1,32 \
  -warmup 2s \
  -duration 5s \
  > spikes/rust-relay/results/go-driver-against-rust.json
```

## Semantic differences

The spike intentionally has a smaller surface than the production relay:

- It implements the protocol paths and authentication needed by the benchmark, plus `/healthz` and a minimal `/metrics` response.
- It preserves WebSocket message boundaries and text/binary types and forwards in both directions after acceptance. It does not implement the production queue, admission limiter, per-host/client limits, trusted-proxy handling, diagnostics, private listener, or detailed metrics.
- It uses Axum's WebSocket size limits and a pair timeout. Delivery timeout, heartbeat, close-code forwarding, and bounded control queue behavior are simplified for this transport experiment.
- Pair IDs are UUID-derived opaque strings rather than the production generator. Endpoint IDs, Base64URL encoding, authentication message, and signed challenge are compatible.

These differences are why the results are comparative measurements under the same loopback workload, not a production readiness claim.

## Reproducibility

The recorded runs use one local macOS host, with no VM or container:

```sh
uname -a
sw_vers
sysctl -n hw.ncpu
go version
rustc --version
cargo --version
```

The recorded host was Darwin `27.0` build `26A425`, arm64, 18 logical CPUs, and 48 GiB RAM. It had Go `1.27.1`, Rust `1.95.0`, and Cargo `1.95.0`. Build both implementations, then run the Go driver with `-spawn -relay-bin /tmp/supacode-relay-go` and the Rust driver with `--spawn --relay-bin spikes/rust-relay/target/release/rust-relay`, using the same matrix, `warmup=2s`, `duration=5s`, `inflight=4`, and `hosts=4`. The machine-readable captures in `results/` include the exact driver settings and per-case process samples. The benchmark runs each implementation in a fresh relay process, sequentially, on loopback.

The table below is a preliminary native-driver comparison from the earlier local macOS spike. It is retained as historical evidence and is not the final cross-language result because each implementation used its own driver and host conditions. The final FAIR comparison uses the unchanged Go driver, one Linux/arm64 container, three rotated repeats, and all five relays; see [`comparison/README.md`](comparison/README.md) and [`comparison/results/aggregate.json`](comparison/results/aggregate.json). Values are messages/sec; RTT is p50/p99 microseconds; CPU is percent of one core and RSS is peak MiB.

| Payload | Clients | Go msg/s | Rust msg/s | Go RTT | Rust RTT | Go CPU/RSS | Rust CPU/RSS |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 B | 1 | 25,048 | 33,789 | 143/307 | 95/331 | 85.3/19.5 | 89.5/4.8 |
| 64 B | 32 | 32,622 | 50,236 | 4,223/5,971 | 1,523/6,521 | 173.0/25.4 | 122.8/12.0 |
| 1,024 B | 1 | 27,069 | 38,935 | 133/347 | 92/221 | 86.1/25.6 | 98.4/12.0 |
| 1,024 B | 32 | 37,001 | 48,982 | 3,538/6,798 | 1,967/6,235 | 186.1/24.2 | 122.9/12.9 |
| 65,536 B | 1 | 4,368 | 17,297 | 786/1,691 | 192/564 | 108.3/24.0 | 93.5/12.9 |
| 65,536 B | 32 | 4,706 | 17,236 | 26,146/36,871 | 6,162/15,383 | 189.1/24.0 | 135.6/26.7 |

All 12 primary cases reported `failures=0` and `corrupt=0`. `rust-relay-go-driver.json` is a second interoperability run of the unchanged Go driver against Rust; it also reports zero failures and corruption for all six cases. The full JSON captures are the source of truth for the rounded table.
