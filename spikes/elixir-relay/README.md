# Elixir relay spike

This directory contains an isolated OTP/Cowboy relay spike. It implements the protocol surface needed by `docs/protocol.md`: authenticated host registration, client connect, host accept, ordered WebSocket frame forwarding, pair expiry, superseded registrations, close propagation, health, and aggregate metrics.

The relay is supervised by `RelaySpike.Supervisor`; shared host and pair state lives in the `RelaySpike.Store` GenServer, while every control, client, and accepted host socket is a Cowboy WebSocket process. Each paired socket caches its peer PID after acceptance, so forwarding sends directly to that process. The Store still owns pair teardown and counters.

The spike deliberately keeps scope below production parity. It omits admission token buckets, private metrics, bounded control queues, streaming fragment forwarding, delivery deadlines, detailed diagnostics, and the full production limit matrix. Pending application frames use a bounded 64-frame process queue until a pair is accepted. Active forwarding uses process mailboxes and does not apply the production delivery timeout or fixed per-pair buffer. The Store remains a single GenServer, so pair lifecycle calls and forwarded-message counters serialize there even though the data frame path does not read its state.

## Reproducible benchmark

Run `./bench/run.sh` from this directory. The script builds a pinned Docker image containing Elixir/OTP, Go, and `procps`, compiles the existing Go benchmark and relay, builds the Mix release, then runs the same `e2e/bench` driver against the Go relay and this spike. It writes JSON output to `bench/results/` and records image, runtime, source, and command metadata in `bench/results/metadata.json`.

The default matrix is 64, 1024, and 65536 byte binary payloads with 1 and 32 clients, four in-flight messages per client, 2 seconds warmup, and 5 seconds measurement. Override with `PAYLOADS`, `CLIENTS`, `WARMUP`, `DURATION`, or `INFLIGHT`. The benchmark reports messages/sec, payload MiB/sec, RTT p50/p99/max in microseconds, failures, corruption, and relay CPU/RSS sampled by the existing Go driver.

The container is the execution environment because Elixir is not installed on the development host. The exact image digest and `docker`, `go`, `elixir`, and `otp_release` versions are recorded by the runner after execution.

## Shared-suite handoff

Use the existing `relay-spike-elixir:local` image as the common Linux build environment. Mount the Elixir spike and the Go worktree at stable paths, then run the following build and launch sequence from the container. The Rust coordinator owns the timed matrix and should write final output outside `bench/results/`.

```sh
docker run --rm --init --platform linux/arm64 \
  -v "$ELIXIR_TREE":/elixir \
  -v "$GO_TREE":/repo \
  -w /elixir/spikes/elixir-relay \
  relay-spike-elixir:local sh -eu -c '
    export MIX_ENV=prod
    mix local.hex --force
    mix local.rebar --force
    mix deps.get --only prod
    mix release --overwrite
    cd /repo
    go build -trimpath -o /tmp/relay ./cmd/relay
    go build -trimpath -o /tmp/relay-bench ./e2e/bench
    /tmp/relay-bench -spawn -relay-bin /tmp/relay \
      -payloads 64,1024,65536 -clients 1,32 -hosts 4 \
      -inflight 4 -warmup 2s -duration 5s
    /tmp/relay-bench -spawn \
      -relay-bin /elixir/spikes/elixir-relay/bench/run-relay.sh \
      -payloads 64,1024,65536 -clients 1,32 -hosts 4 \
      -inflight 4 -warmup 2s -duration 5s
  '
```

`bench/run-relay.sh` derives the release path from its own location, maps the benchmark's `RELAY_ADDR=127.0.0.1:0` to `RELAY_PORT=0`, and uses `exec`. The release script also uses `exec`, so the benchmark PID becomes the BEAM process. A smoke launch in the common image observed `/proc/$PID/exe` at `erts-15.2.7.13/bin/beam.smp` and `ps -o rss=,time= -p $PID` returned `80648 00:00:00` before traffic.

The Go driver samples that PID with `ps -o rss=,time=` every 250 ms. RSS is the BEAM process peak in KiB, and CPU is cumulative process time divided by elapsed sample time. The `time` field has one-second resolution in this environment, so the five-second CPU values are coarse. The sample excludes container-side build tools and other processes.

The checked-in `bench/results/*.json` files are preliminary results from an earlier run. They remain unchanged and are not final common-container comparison results.

The Elixir and Go relays have semantic gaps that affect interpretation: this spike has a 30-second pending-pair timeout, silently drops pending frames after 64, uses Cowboy's default frame limit, reports no `closed` control event, and sends generic 1001 close notifications. It has no production delivery deadline, admission token bucket, bounded active-pair buffer, or private metrics surface. The shared driver exercises successful host-register, client-connect, host-accept, echo, payload-size, concurrency, warmup, and measurement paths; it does not establish production-limit parity.

The image used for the prior preliminary run reported Debian 12, Linux aarch64, Docker Server 29.4.0 on the host, Elixir 1.18.4, Erlang/OTP 27.3.4.18 with ERTS 15.2.7.13, Go 1.25.1 linux/arm64, and procps-ng 4.0.2. That build resolved `elixir:1.18.4-otp-27` to base digest `sha256:a7b2c4772e05616cf0a3323758ba5abc3288af452a33d1263705840a52154e85`.
