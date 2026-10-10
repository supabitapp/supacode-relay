# Rust relay spike

Recommendation: keep the production Go relay while evaluating a Rust implementation that preserves streaming and production behavior. This complete-message prototype shows lower CPU cost, with throughput gains that depend on workload. Its memory use grows with large messages, so these measurements do not justify a production port yet.

The Rust experiment in [src/main.rs](src/main.rs) uses the Go relay and the same [path benchmark](../../e2e/pathbench/main.go). Cloudflare Workers performance requires a separate experiment in its Wasm runtime.

## Measured results

Measurements from October 10, 2026 use Go 1.27.1 and Rust 1.95.0 on macOS 27 arm64, with 18 logical CPUs and 48 GiB RAM. The Go relay comes from [8aa1e49](https://github.com/supabitapp/supacode-relay/commit/8aa1e491c38d4827413a89926314823e895bd0f2). The tables report medians of three runs per implementation. A message per second counts one completed echo, which crosses the relay twice.

| Payload | Clients | Go msg/s | Rust msg/s | Rust throughput change | Peak RSS MiB, Go / Rust |
| --- | --- | --- | --- | --- | --- |
| 64 B | 1 | 64,193 | 45,994 | -28% | 19.2 / 3 |
| 64 B | 32 | 66,512 | 81,855 | +23% | 20.5 / 3.9 |
| 64 B | 128 | 68,643 | 70,785 | +3% | 30.8 / 5.5 |
| 1 KiB | 1 | 48,208 | 48,938 | +2% | 19.3 / 3 |
| 1 KiB | 32 | 67,724 | 83,849 | +24% | 20.5 / 5.4 |
| 1 KiB | 128 | 75,144 | 63,120 | -16% | 31.5 / 11.4 |
| 64 KiB | 1 | 4,915 | 8,795 | +79% | 17.2 / 3.7 |
| 64 KiB | 32 | 6,191 | 7,741 | +25% | 20.3 / 12.1 |
| 64 KiB | 128 | 6,939 | 7,379 | +6% | 31 / 34.3 |
| 1 MiB | 1 | 488 | 814 | +67% | 13.8 / 17.1 |
| 1 MiB | 32 | 567 | 616 | +9% | 17.9 / 164.1 |
| 1 MiB | 128 | 421 | 480 | excluded | 26.8 / 409.9 |

CPU time per completed echo is 6-58% lower in the eleven successful comparisons. At 32 clients, the 64 B, 1 KiB and 64 KiB workloads show lower CPU cost with higher throughput. The [summary](results/summary.json) includes p50 and p99 latency, per-case CPU cost, and throughput ranges.

The 1 MiB workload at 32 clients uses about nine times as much relay memory in Rust. Its complete-message buffering differs from Go's fixed-buffer streaming. A streaming Rust spike would give a closer comparison before attributing gains to the language.

The complete run records 7,587,292 echoes in the measurement windows and zero payload or ordering errors. The 1 MiB workload at 128 clients records failure counters of 61 for Go and 60 for Rust. These counters include write errors and sent messages still outstanding at the driver's drain cutoff. That workload is excluded from performance comparisons because both implementations fail it.

The run uses a shared machine without CPU affinity, TLS, or a real network path. Throughput varies between rounds, and the driver also consumes substantial CPU. The prototype omits production features listed below, so its CPU cost does not represent a complete port. These are measurements of the implementations in this workload, rather than a language-only comparison or a Cloudflare Workers benchmark.

## Measurement scope

The benchmark runs native release binaries on macOS arm64 over loopback. Each case starts a fresh relay process and four authenticated echo hosts. Go has four scheduler slots and Tokio has four worker threads. The Go driver generates the traffic and echoes it from the host connections.

Three rounds alternate implementation order for each payload and client count. Each client keeps four messages in flight, with two seconds of warmup and a three-second measurement window. The driver checks returned bytes and sequence numbers. CPU and resident memory are sampled during the measurement window; outstanding messages get a five-second drain budget.

The Rust spike uses Tokio and Tungstenite to read and forward complete messages. Go streams through fixed buffers, as described in the [protocol](../../docs/protocol.md). This comparison measures both implementations, including their buffering choices. It cannot isolate the effect of the language or predict production capacity from local throughput.

## Reproduce

From the repository root, run:

```sh
python3 spikes/rust-relay/bench.py
python3 spikes/rust-relay/bench.py --protocol-only
```

The benchmark builds both relays and the driver, then writes [raw measurements](results/raw.json) and [summary statistics](results/summary.json). It completes every case and exits unsuccessfully if any case records failures. Failed cases remain in the report, and their throughput does not establish a performance improvement. The `--out` option selects another output directory.

The protocol command starts each implementation separately and records the five checks in [protocol.json](results/protocol.json). Both implementations pass. `make test`, `cargo fmt --check --manifest-path spikes/rust-relay/Cargo.toml`, and `cargo clippy --locked --manifest-path spikes/rust-relay/Cargo.toml -- -D warnings` also pass. The [incomplete discovery run](results/discovery.json) is excluded from the summary; its CPU sampler includes drain time.

The Rust relay accepts loopback addresses only. It implements the three pairing routes, Ed25519 challenge authentication, one-use accept tokens, host supersession, and bidirectional text and binary forwarding. The [protocol checks](protocol_test.go) cover early messages, invalid authentication, token rejection and reuse, supersession, and host disconnects.

The spike omits production admission limits, per-IP rate limiting, cluster directory integration, diagnostics, and graceful shutdown. It also lacks Go's exact heartbeat and oversized-message close behavior. A production port needs those guarantees and tests for slow receivers before adoption.
