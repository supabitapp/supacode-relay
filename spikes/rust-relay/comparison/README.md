# Common Linux relay comparison

This directory contains the final common-suite run for Go, Rust, TypeScript on Node, TypeScript on Bun, and Elixir. Every timed case used the unchanged Go benchmark driver from Go source commit `060472bfeb8f3d97c460613d197431a4306661a1` (driver SHA256 `663f7af8868e19ad75541b19653f84b32ca46228fd67388dce28fb7209eea851`). The driver performs Ed25519 host registration, client connection, host acceptance, bidirectional echo, full sequence and payload validation, four in-flight messages per client, warmup, measurement, and relay CPU/RSS sampling.

The matrix is payloads `64, 1024, 65536` bytes × clients `1, 32`, with four hosts, four in-flight messages per client, 2 seconds warmup, 5 seconds measurement, loopback transport, and no compression or TLS. Three repeats ran in these rotated orders:

```text
1: go, rust, typescript-node, typescript-bun, elixir
2: typescript-node, typescript-bun, elixir, go, rust
3: elixir, go, rust, typescript-node, typescript-bun
```

All 90 timed cases passed with `failures=0`, `corrupt=0`, and positive throughput. `results/aggregate.json` is the machine-readable median/min/max summary for messages/sec, payload MiB/sec, RTT p50/p99, relay CPU, and relay RSS. `results/raw/` contains every driver JSON and compressed stderr capture; `results/runs.json` records order, commands, process identity, cgroup samples, and raw hashes. The finite compatibility smoke passed all five relays before timing.

## Source and environment manifest

The frozen sources are recorded in [`sources.json`](sources.json): Go `060472bfeb8f3d97c460613d197431a4306661a1`, Rust `3597a1b05fd751aeb6840d5a00029fdb5953d880`, TypeScript `179636592711210d3961b556c4ff40c8edcb95d7`, and Elixir `cc4f0949c6bb87730542139d75fad2d5844a1f85`. The TypeScript compiled relay hash is `288bce1e9f8fe75048c33b939d9bfa5489112329c66abbe3ca087f949cce8c82`.

The run used image `relay-common-toolchains:local`, image ID and digest `sha256:ee2282b8a23092530a18944f7c0802a5098ce496f2cd35250330cc09687cbb73`, on Linux arm64. The container was limited to CPUs `0-3`, `--cpus=4`, `--memory=4g`, `--memory-swap=4g`; its cgroup recorded `cpu.max=400000 100000`, `memory.max=4294967296`, and `memory.swap.max=0`. The host was OrbStack on an aarch64 machine with 18 logical CPUs and 16 GiB host memory; other host activity can still contend for the shared VM resources.

Recorded toolchain paths and versions are:

| Tool | Path | Version |
| --- | --- | --- |
| Go | `/usr/local/go/bin/go` | `go1.25.1 linux/arm64` |
| Rust | `/opt/rust/bin/rustc` | `1.95.0 (59807616e 2026-04-14)` |
| Cargo | `/opt/rust/bin/cargo` | `1.95.0 (f2d3ce0bd 2026-03-21)` |
| Node | `/opt/node/bin/node` | `v22.20.0` |
| Bun | `/opt/bun-linux-aarch64/bun` | `1.3.0`, revision `1.3.0+b0a6feca5` |
| Elixir/OTP | `/usr/local/bin/elixir` | Elixir `1.18.4`, OTP `27.3.4.18`, ERTS `15.2.7.13` |
| procps | `/usr/bin/ps` | `procps-ng 4.0.2` |

Linux `ps -o rss=,time=` reports elapsed CPU time at one-second resolution here. CPU percentages in the aggregate are therefore approximate over each five-second window; RSS is the relay process peak sampled by the unchanged driver.

## Median results

The table gives the median of the three rotated repeats. CPU is percent of one core and RSS is peak MiB. The full min/median/max values and all repeat values are in [`results/aggregate.json`](results/aggregate.json).

| Relay | Payload | Clients | Messages/s | MiB/s | RTT p50/p99 µs | CPU | RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Elixir | 64 | 1 | 39,985.6 | 2.4 | 84.9 / 259.0 | 79.9 | 81.4 |
| Elixir | 64 | 32 | 113,287.0 | 6.9 | 998.5 / 3,455.7 | 179.8 | 88.0 |
| Elixir | 1,024 | 1 | 28,924.8 | 28.2 | 106.5 / 474.6 | 79.9 | 86.0 |
| Elixir | 1,024 | 32 | 72,621.6 | 70.9 | 1,490.9 / 6,414.3 | 179.8 | 92.9 |
| Elixir | 65,536 | 1 | 1,407.0 | 87.9 | 1,374.8 / 10,645.8 | 39.2 | 105.8 |
| Elixir | 65,536 | 32 | 2,112.4 | 132.0 | 51,573.6 / 156,634.5 | 39.4 | 140.9 |
| Go | 64 | 1 | 83,103.6 | 5.1 | 33.6 / 108.5 | 59.9 | 13.0 |
| Go | 64 | 32 | 219,692.0 | 13.4 | 445.7 / 3,584.2 | 119.9 | 14.9 |
| Go | 1,024 | 1 | 54,701.0 | 53.4 | 40.0 / 214.8 | 40.0 | 14.1 |
| Go | 1,024 | 32 | 121,971.2 | 119.1 | 659.5 / 5,972.9 | 79.9 | 14.7 |
| Go | 65,536 | 1 | 1,572.6 | 98.3 | 925.4 / 8,655.4 | 20.0 | 13.8 |
| Go | 65,536 | 32 | 1,803.8 | 112.7 | 59,636.3 / 186,483.5 | 19.7 | 13.9 |
| Rust | 64 | 1 | 72,121.0 | 4.4 | 37.2 / 132.9 | 39.6 | 4.2 |
| Rust | 64 | 32 | 238,603.8 | 14.6 | 429.0 / 2,538.5 | 79.3 | 12.8 |
| Rust | 1,024 | 1 | 48,317.0 | 47.2 | 43.4 / 319.4 | 39.7 | 12.1 |
| Rust | 1,024 | 32 | 133,259.6 | 130.1 | 584.7 / 6,005.1 | 59.5 | 14.7 |
| Rust | 65,536 | 1 | 1,552.2 | 97.0 | 921.4 / 8,988.2 | 20.0 | 14.1 |
| Rust | 65,536 | 32 | 1,725.0 | 107.8 | 61,767.4 / 182,287.2 | 19.8 | 38.9 |
| TypeScript Node | 64 | 1 | 67,059.8 | 4.1 | 45.4 / 167.7 | 40.0 | 77.1 |
| TypeScript Node | 64 | 32 | 125,006.2 | 7.6 | 850.0 / 4,621.7 | 59.9 | 88.3 |
| TypeScript Node | 1,024 | 1 | 42,041.0 | 41.1 | 56.3 / 437.7 | 40.0 | 102.1 |
| TypeScript Node | 1,024 | 32 | 72,421.6 | 70.7 | 1,273.3 / 7,154.0 | 60.0 | 102.9 |
| TypeScript Node | 65,536 | 1 | 1,252.0 | 78.3 | 1,692.0 / 10,683.7 | 20.0 | 131.1 |
| TypeScript Node | 65,536 | 32 | 1,620.6 | 101.3 | 68,079.0 / 194,985.3 | 19.7 | 163.0 |
| TypeScript Bun | 64 | 1 | 81,623.6 | 5.0 | 34.2 / 114.0 | 40.0 | 66.7 |
| TypeScript Bun | 64 | 32 | 179,963.0 | 11.0 | 571.1 / 4,004.7 | 59.9 | 74.7 |
| TypeScript Bun | 1,024 | 1 | 48,999.0 | 47.9 | 40.8 / 421.8 | 20.0 | 74.2 |
| TypeScript Bun | 1,024 | 32 | 94,152.2 | 91.9 | 829.8 / 7,374.9 | 60.0 | 82.5 |
| TypeScript Bun | 65,536 | 1 | 1,460.8 | 91.3 | 1,160.6 / 9,438.0 | 0.0 | 82.5 |
| TypeScript Bun | 65,536 | 32 | 1,594.6 | 99.7 | 67,642.3 / 209,020.2 | 19.7 | 90.8 |

## Reproduce

Freeze the four source trees into `inputs/` and regenerate [`sources.json`](sources.json):

```sh
python3 spikes/rust-relay/comparison/freeze.py \
  --typescript-repo /Users/khoi/.supacode/worktrees/supacode-relay/supacode-relay-spike-typescript \
  --typescript-ref 1796365 \
  --elixir-repo /Users/khoi/.supacode/worktrees/supacode-relay/supacode-relay-spike-elixir \
  --elixir-ref cc4f094
```

Build the pinned arm64 image, then use one writable `results/` mount and read-only source/input mounts. The exact command used for the final run was:

```sh
docker build --platform linux/arm64 -t relay-common-toolchains:local spikes/rust-relay/comparison
docker run --rm --init --platform linux/arm64 \
  --name relay-common-suite --cpuset-cpus=0-3 --cpus=4 \
  --memory=4g --memory-swap=4g \
  -v "$PWD/spikes/rust-relay/comparison:/comparison:ro" \
  -v "$PWD/spikes/rust-relay/comparison/inputs:/inputs:ro" \
  -v "$PWD/spikes/rust-relay/comparison/results:/results" \
  relay-common-toolchains:local sh -eu -c \
  'sh /comparison/build.sh && python3 /comparison/run.py smoke && python3 /comparison/run.py suite'
```

`build.sh` builds all relay binaries and the unchanged Go benchmark/probe; `run.py smoke` performs the finite protocol check; `run.py suite` enforces sequential fresh relays, process identity, exact matrix, zero failures/corruption, and writes the aggregate. Do not reuse a nonempty `results/` directory for a new suite.

## Semantic scope

The driver workload is equivalent across implementations, but the relays are spikes. The Go relay is the production implementation. Rust, TypeScript, and Elixir implement the authenticated control flow, pairing, text/binary message forwarding, and close behavior needed by this workload, while omitting portions of production admission, diagnostics, private listeners, streaming/backpressure, and limit enforcement. Elixir's final source caches peer PIDs at pairing and sends frames directly; Store remains responsible for pairing teardown and counters. These differences make this a controlled workload comparison, not a production capacity or feature-parity claim.
