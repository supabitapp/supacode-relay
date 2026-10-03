# Supacode relay

A single-node WebSocket relay that pairs a client with a registered host and forwards opaque messages between them. The relay authenticates hosts with Ed25519, but it never interprets application payloads, pairing requests, or encryption handshakes. Endpoints own encryption and authorization. A message named `hello` or `e2ee_hello` is just bytes to the relay.

Built on `github.com/gorilla/websocket` v1.5.3 and `golang.org/x/time/rate`. Go 1.25+.

## Commands

```bash
./build.sh    # bin/relay and bin/relay-bench (CGO_ENABLED=0, -trimpath, -ldflags="-s -w")
./run.sh      # runs bin/relay with the RELAY_* environment, never rebuilds
./test.sh     # gofmt, go vet, unit tests, real-socket e2e suite, all with -race
bash ./bench.sh
```

`test.sh` builds the relay with `-race`, then `e2e/` starts that binary on an ephemeral loopback port for each test and drives it with real host and client sockets. Extra arguments go to `go test`, for example `./test.sh -run TestDataTokens -v`.

`bench.sh` runs the sections `matrix-64 matrix-1024 matrix-65536 idle churn slow`. Each section starts its own relay and runs under a budget of 2000 connections. Sections are separated by a 35 s cooldown so TIME_WAIT sockets can clear. If the driver hits local port exhaustion (`EADDRNOTAVAIL`), it aborts the section without retrying. To rerun only failed sections into the same directory:

```bash
RELAY_BENCH_OUT=/path/to/previous/run RELAY_BENCH_SECTIONS="slow" bash ./bench.sh
```

Raw JSON is written to `$RELAY_BENCH_OUT`, which defaults to `$TMPDIR/supacode-relay-go-bench/<UTC timestamp>`. Each section writes a `section-*.json`, and the matrix also writes one `case-*.json` per case. The merged `results.json` is also printed to stdout. Other settings: `RELAY_BENCH_COOLDOWN_SECONDS`, plus the driver flags `-warmup`, `-duration`, `-inflight`, `-reps`, `-churn-cycles`, `-max-stalled`, `-max-dials`, and `-max-runtime`. When `-max-runtime` expires, the driver prints a goroutine dump, stops the processes it spawned, and exits 3.

## Protocol

All Base64URL values are canonical and unpadded. `endpointId` is the lowercase hex SHA-256 of the raw 32-byte Ed25519 host public key.

| Endpoint | Behavior |
| --- | --- |
| `GET /healthz` | `200 {"status":"ok"}`; `503 {"status":"draining"}` during drain |
| `GET /metrics` | JSON counters: `activeHosts`, `activePairs`, `pendingPairs`, `forwardedMessages`, `forwardedBytes`, `rejectedConnections`, `controlConnections`, `openSockets`, `rateLimiterEntries`, `goroutines`, `draining`. No payloads, keys, tokens, or connection ids |
| `GET /v1/control?publicKey=B64` | Host control socket. Rejects a noncanonical or non-32-byte key with 400 before upgrade |
| `GET /v1/connect?endpointId=HEX` | Client data socket. Returns 404 before upgrade if no host is registered |
| `GET /v1/accept?endpointId=HEX&connectionId=ID&token=TOKEN` | Host data socket for one pending pair. Returns 404 for an unknown endpoint, connection, expired pair, or old host generation. Returns 403 for a missing, wrong, or already-used token |

Control flow:

1. relay → host: `{"type":"challenge","nonce":"B64(32 random bytes)"}`
2. host → relay: `{"type":"authenticate","signature":"B64(Ed25519 signature)"}` over the exact bytes `"supacode-relay-v1\n" + endpointId + "\n" + nonce`
3. relay → host: `{"type":"registered","endpointId":"HEX"}`

The nonce is bound to that one socket and expires with the authentication timeout. An invalid signature, a timeout, a replayed signature, or a second registration of an already-active endpoint closes the socket with 1008. The existing host is left untouched. Once registered, the relay sends the host `{"type":"incoming","connectionId","token"}` for each admitted client and `{"type":"closed","connectionId"}` at most once when that pair is released. A host must not send further messages on the control socket. If it does, the relay closes the socket with 1008.

Data sockets carry only application frames. Text versus binary, message boundaries, contents, and order are preserved in both directions. Client messages sent before the host accepts are buffered within the queue limits and delivered in order after pairing.

Close codes the relay sends:

| Code | Reason |
| --- | --- |
| peer's code | A legal code from the peer is forwarded with its reason. 1005 (no status) becomes 1000 |
| 1001 | `peer disconnected` (abnormal close), `peer timeout` (missed pong), `host offline` (control socket gone), `relay shutting down` |
| 1008 | authentication failure, duplicate registration, unexpected control message |
| 1009 | a message exceeded `RELAY_MAX_MESSAGE_BYTES` |
| 1011 | upgrade or accept failure |
| 1013 | `queue limit exceeded`, `pair timeout`, `peer write timeout` |

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `RELAY_ADDR` | `127.0.0.1:8080` | Listen address. Port 0 picks a free port. Startup prints `{"event":"listening","address":"HOST:PORT"}` on stdout; all other logs go to stderr |
| `RELAY_MAX_MESSAGE_BYTES` | 1048576 | Largest single message. Must not exceed the queue byte limit |
| `RELAY_MAX_QUEUE_BYTES` | 4194304 | Queued payload bytes per data direction, including the pre-pairing buffer |
| `RELAY_MAX_QUEUE_MESSAGES` | 256 | Queued messages per data direction |
| `RELAY_MAX_CLIENTS` | 1024 | Global pending plus active pairs |
| `RELAY_MAX_CLIENTS_PER_HOST` | 128 | Pending plus active pairs per host |
| `RELAY_MAX_PENDING_PER_HOST` | 32 | Pending pairs per host. Must not exceed the per-host limit |
| `RELAY_MAX_HOSTS` | 1024 | Concurrent control sockets, including ones that are still authenticating |
| `RELAY_AUTH_TIMEOUT_MS` | 5000 | Time allowed to answer the challenge |
| `RELAY_PAIR_TIMEOUT_MS` | 5000 | Time a client waits for the host to accept |
| `RELAY_WRITE_TIMEOUT_MS` | 5000 | Deadline for each socket write |
| `RELAY_HEARTBEAT_MS` | 15000 | Ping interval on every socket. A socket that sends no frame and no pong for 2 × this interval is closed |
| `RELAY_ADMISSION_RATE` | 100 | Upgrade attempts per second per client IP, with a burst equal to the rate (minimum 1). Refused attempts get 429. Tracking is capped at 65536 IPs, and an IP's entry expires after 1 minute idle |
| `RELAY_TRUSTED_PROXIES` | empty | Comma-separated CIDRs. For requests arriving from these addresses, the rate limiter keys on the rightmost untrusted `X-Forwarded-For` hop |

An invalid value prints the offending variable to stderr and exits with code 2.

### Deployment

The relay listens on plain WS on loopback by default. A public deployment must sit behind a TLS-terminating reverse proxy, and clients must use `wss://`. Set `RELAY_TRUSTED_PROXIES` to the proxy's address so rate limiting applies per real client. The proxy must forward WebSocket upgrades and must not log query strings, because `publicKey`, `connectionId`, and `token` travel in the URL. The relay itself never logs URLs.

On SIGTERM or SIGINT the relay stops admitting new hosts and clients (503) and reports 503 on `/healthz`. Already-issued accepts may still complete. The whole shutdown fits in one 5 s budget. Active pairs may finish until 4.15 s. The relay then sends 1001 to every remaining socket, hard-closes whatever is still open at 4.9 s, and exits 0.

### Reconnecting

Relay state is held in memory. After a restart, hosts must register again and clients must establish fresh pairs. Reusing the host identity key preserves its endpoint ID. Host and client retry loops, persistent trust, fresh encrypted handshakes, and application state recovery belong in the endpoints; the included endpoint helper does not reconnect automatically. Run the relay under a process supervisor to restart it after a crash.

## Design notes

- Each socket has one reader goroutine and one writer goroutine. Each direction of a pair has its own bounded queue. A full queue closes that pair with 1013 instead of blocking the reader. A slow consumer therefore costs at most its queue limit plus kernel buffers, and other pairs are unaffected. A stalled socket is released within `RELAY_WRITE_TIMEOUT_MS` after its pair closes.
- A pair's state lives under one server mutex. Release happens exactly once. The host map is only modified if the entry is still the same host object, so a stale callback cannot tear down a newly registered host or a new pair. Tokens belong to a pair inside one host registration, so a re-registered host cannot use tokens from an old generation.
- The relay does not provide transparent socket migration, application-level acknowledgements, or exactly-once delivery. A dropped socket loses whatever was still queued.

## Test coverage (`e2e/`, real sockets against the built binary)

1. Registration and metrics; noncanonical keys (padding, trailing bits, wrong length, std alphabet, newline); invalid authentication in 9 forms; auth timeout; a stolen endpoint claim and a duplicate legitimate claim while the original stays live; challenge replay on a new socket and after registration; reconnect after a graceful or abrupt control loss.
2. Text, binary, empty, invalid-UTF-8 binary, 125/126/127/65535/65536 bytes, and max-1/max-size boundaries; 400 ordered mixed messages in each direction at the same time; JSON that looks like relay control records or handshake messages; client receives no relay records.
3. Three hosts with 8 simultaneous clients each and no cross-delivery; wrong, missing, truncated, cross-endpoint, unknown, reused, and expired tokens; stale-generation cleanup with an intact new host and no stray `closed` events.
4. Pre-pairing buffer order and its message and byte limits; pair timeout; oversize messages in both directions; a stalled reader flooding 64 KiB while two healthy pairs keep echoing; global, per-host, pending, and host connection caps; per-IP rate limiting; heartbeat timeout of a non-ponging peer while a healthy pair survives 6 pings; abrupt disconnect of either side with exactly one `closed` notification; preserved close codes and reasons; host control loss closing all pending and active pairs; drain that waits for a finishing pair; SIGTERM with 4 active pairs, a pending pair, and a non-reading peer exits within 5 s (measured 4.90 s, with 250 ms tolerance allowed).
5. TLS 1.3 between endpoints over a WebSocket-backed byte stream (`crypto/tls`, client pins a self-signed test certificate): successful request/response; an impostor endpoint with a different certificate is rejected; flipping one ciphertext bit fails with `bad record MAC`; replaying a recorded application-data record fails with `bad record MAC`; a fresh reconnect succeeds. The keys exist only in the test. The relay neither knows nor implements any of this.
6. JSON-RPC text and a length-prefixed varint binary record format, concurrently and unchanged through one relay.

Unit tests cover the rate limiter's refill, its 65536-entry cap, entry expiry, trusted-proxy IP selection, and canonical Base64 decoding. Configuration validation runs end to end against the binary.

Not covered by tests:

- Memory exhaustion is not demonstrated at scale. The queue bounds are tested, and the benchmark samples RSS.
- The 65536-entry rate-limiter cap is only checked by a unit test, because reaching it would take that many source IPs.
- No test runs behind a real TLS proxy. Trusted-proxy parsing is unit-tested only.
- SIGINT is handled by the same code path as SIGTERM, which is the only one tested.

## Benchmark results

Provisional numbers from a shared machine: Apple M5 Max, 18 cores, 128 GiB, macOS 27.0, Go 1.27.1, gorilla/websocket v1.5.3, release build. The driver, the relay, and the direct echo server all run on the same host over loopback.

Workload: each client keeps 4 messages in flight. Each case has a 2 s warmup followed by a 5 s measurement window. The relay case uses 4 echoing hosts. The direct baseline uses the same driver against a single gorilla echo server process. RTT is measured at the client, and throughput counts echoed messages received. CPU is the server process's CPU time over the window as a percentage of one core. Every case had 0 failures, 0 timeouts, and 0 corrupt messages.

| Payload | Clients | Relay p50 / p99 RTT | Relay msg/s | Relay MiB/s | Direct p50 / p99 | Direct msg/s | Relay CPU / RSS |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 64 B | 1 | 64 / 113 µs | 57.1k | 3.5 | 26 / 43 µs | 123k | 131% / 21 MiB |
| 64 B | 32 | 1.91 / 2.44 ms | 66.2k | 4.0 | 0.87 / 1.24 ms | 144k | 389% / 26 MiB |
| 64 B | 128 | 8.6 / 9.9 ms | 59.3k | 3.6 | 4.0 / 5.1 ms | 125k | 428% / 35 MiB |
| 1 KiB | 1 | 68 / 134 µs | 52.9k | 51.7 | 29 / 60 µs | 111k | 130% / 21 MiB |
| 1 KiB | 32 (rep 1/2/3) | 1.96 / 2.82, 2.13 / 3.03, 1.96 / 2.80 ms | 63.7k / 59.1k / 63.6k | 62.2 / 57.8 / 62.1 | ~0.87–0.91 / 1.29–1.36 ms | 142k / 142k / 135k | ~445% / 27 MiB |
| 1 KiB | 128 | 8.6 / 10.1 ms | 59.5k | 58.1 | 3.9 / 5.0 ms | 128k | 439% / 37 MiB |
| 64 KiB | 1 | 480 / 938 µs | 7.3k | 453 | 129 / 275 µs | 19.1k | 226% / 22 MiB |
| 64 KiB | 32 | 14.7 / 19.5 ms | 8.5k | 531 | 7.2 / 10.6 ms | 17.0k | 491% / 31 MiB |
| 64 KiB | 128 | 64.6 / 74.1 ms | 7.9k | 491 | 32.0 / 36.7 ms | 15.9k | 450% / 43 MiB |

The relay adds a second WebSocket hop, so a relayed echo crosses four sockets where the direct baseline crosses two. That roughly halves throughput and roughly doubles queueing RTT compared with direct echo. Latency at higher client counts is dominated by the 4-in-flight window: RTT ≈ clients × 4 / throughput.

Pair-ready latency (dial start until the first probe echo, including host accept): p50 0.57–1.0 ms with 1 client, about 2.7–2.9 ms with 32 clients connecting at once, and about 7.8–8.4 ms with 128.

Idle memory with 10 registered hosts:

| Paired clients | RSS | Goroutines | Relay sockets |
| --- | --- | --- | --- |
| no hosts | 11.4 MiB | | |
| 0 | 13.0 MiB | 26 | 10 |
| 100 | 19.9 MiB | 426 | 210 |
| 500 | 44.2 MiB | 2026 | 1010 |

That is about 4 goroutines and roughly 60 KiB of RSS per idle pair. After all 500 clients disconnected, every pair was released within 27 ms and goroutines returned to 26. RSS stayed at 45 MiB because the Go runtime keeps freed heap until it scavenges it.

Churn: 600 cycles of connect, host accept, one echo, and client close across 16 workers. All succeeded in 0.086 s: about 7.0k cycles/s, p50 2.2 ms, p99 3.6 ms, with 0 failures. State was back to baseline within 12 ms, and goroutines were 10 before and 10 after.

Slow reader (paced rerun of only this section, into the same raw directory): 4 workers kept opening pairs whose host never reads, 150 in total, and flooded each with 64 KiB messages. The flood was active for 5.8 s of the 7 s warmup-plus-measurement window. All 150 stalled pairs were closed with 1013 by queue overflow, 4 ms after opening at p50 and 24 ms at max. Relay RSS peaked at 51.9 MiB. A separate healthy 1 KiB pair with 1 message in flight measured p50 53 µs without the flood and 53 µs with it, and p99 90 µs → 154 µs, with 0 failures. The first unpaced run measured p99 98 → 104 µs, but its flood ended early in the window.

Limitations of these numbers: the machine was shared with other jobs. Everything ran over loopback on one host, so the driver competes with the relay for CPU. RSS is sampled with `ps` every 250 ms. Nothing here extrapolates to remote networks or to client counts that were not measured.
