# Elixir relay spike

This directory contains an isolated OTP/Cowboy relay spike. It implements the protocol surface needed by `docs/protocol.md`: authenticated host registration, client connect, host accept, ordered WebSocket frame forwarding, pair expiry, superseded registrations, close propagation, health, and aggregate metrics.

The relay is supervised by `RelaySpike.Supervisor`; shared host and pair state lives in the `RelaySpike.Store` GenServer, while every control, client, and accepted host socket is a Cowboy WebSocket process. Forwarding uses process messages and keeps WebSocket message boundaries and text/binary type.

The spike deliberately keeps scope below production parity. It omits admission token buckets, private metrics, bounded control queues, streaming fragment forwarding, delivery deadlines, detailed diagnostics, and the full production limit matrix. Pending application frames use a bounded 64-frame process queue until a pair is accepted; production relies on socket buffering and fixed forwarding buffers. These differences make the spike useful for an OTP process-model and throughput comparison, not a production replacement.

## Reproducible benchmark

Run `./bench/run.sh` from this directory. The script builds a pinned Docker image containing Elixir/OTP, Go, and `procps`, compiles the existing Go benchmark and relay, builds the Mix release, then runs the same `e2e/bench` driver against the Go relay and this spike. It writes JSON output to `bench/results/` and records image, runtime, source, and command metadata in `bench/results/metadata.json`.

The default matrix is 64, 1024, and 65536 byte binary payloads with 1 and 32 clients, four in-flight messages per client, 2 seconds warmup, and 5 seconds measurement. Override with `PAYLOADS`, `CLIENTS`, `WARMUP`, `DURATION`, or `INFLIGHT`. The benchmark reports messages/sec, payload MiB/sec, RTT p50/p99/max in microseconds, failures, corruption, and relay CPU/RSS sampled by the existing Go driver.

The container is the execution environment because Elixir is not installed on the development host. The exact image digest and `docker`, `go`, `elixir`, and `otp_release` versions are recorded by the runner after execution.
