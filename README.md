# Supacode relay

A small WebSocket relay that connects clients to hosts that aren't reachable directly. A host keeps a control socket open to the relay and proves its identity with an Ed25519 key. A client asks for that host by endpoint ID, and the relay pairs the two sockets and forwards frames between them unchanged.

The relay never looks inside the data it forwards. Encryption, pairing, and authorization between client and host belong to the endpoints.

## Public relay

A hosted instance runs at `wss://supacode-relay.exe.xyz`. Point hosts and clients at that base URL, for example `wss://supacode-relay.exe.xyz/v1/connect?endpointId=HEX`. Its health is at <https://supacode-relay.exe.xyz/healthz>.

Every push to `main` that passes the tests is deployed there automatically. Deploys restart the relay, so connected hosts and clients have to reconnect.

## Quick start

Requires Go 1.25 or later.

```bash
make build   # bin/relay
make run     # build, then listen on 127.0.0.1:8080
make test    # gofmt, go vet, unit and end-to-end tests with -race
```

On startup the relay prints one line to stdout, `{"event":"listening","address":"127.0.0.1:8080"}`. All other logs go to stderr.

## How it works

1. The host opens `/v1/control` with its public key and signs the relay's challenge.
2. A client opens `/v1/connect` with the host's endpoint ID.
3. The relay sends the host an `incoming` event with a connection ID and a one-time token.
4. The host opens `/v1/accept` with that token. The two sockets are now paired.
5. Frames flow both ways until either side closes. The relay then sends the host a `closed` event.

The relay keeps message boundaries, text and binary frame types, and order in both directions. Anything the client sends before the host accepts is buffered, within the queue limits, and delivered once the pair is ready.

## Protocol

Base64URL values are unpadded and must be canonical. `endpointId` is the lowercase hex SHA-256 of the host's raw 32-byte Ed25519 public key.

### Endpoints

| Endpoint | Purpose |
| --- | --- |
| `GET /v1/control?publicKey=B64` | Host control socket. A malformed key gets 400 |
| `GET /v1/connect?endpointId=HEX` | Client data socket. An unknown endpoint gets 404 |
| `GET /v1/accept?endpointId=HEX&connectionId=ID&token=TOKEN` | Host data socket for one pending pair. An unknown or expired pair gets 404, and a bad or reused token gets 403 |
| `GET /healthz` | `200 {"status":"ok"}`, or `503 {"status":"draining"}` during shutdown |
| `GET /metrics` | JSON counters for hosts, pairs, sockets, forwarded messages and bytes, and rejections. Never includes keys, tokens, or payloads |

Upgrade requests can also be refused with 429 when the client IP exceeds the admission rate, or 503 when a capacity limit is reached or the relay is draining.

### Host registration

```text
relay → host  {"type":"challenge","nonce":"B64(32 random bytes)"}
host  → relay {"type":"authenticate","signature":"B64(Ed25519 signature)"}
relay → host  {"type":"registered","endpointId":"HEX"}
```

The host signs the exact bytes `"supacode-relay-v1\n" + endpointId + "\n" + nonce`. Each nonce is good for one socket until the auth timeout. A bad signature, a timeout, or an attempt to register an endpoint that is already online closes the socket with 1008. The host that is already registered is not affected.

Once registered, the relay sends the host:

```text
{"type":"incoming","connectionId":"ID","token":"TOKEN"}
{"type":"closed","connectionId":"ID"}
```

`closed` is sent at most once per pair. The host must not send anything else on the control socket; doing so closes it with 1008.

### Close codes

| Code | Meaning |
| --- | --- |
| peer's code | The other side closed with this code and reason. A close without a status is forwarded as 1000 |
| 1001 | `peer disconnected`, `peer timeout`, `peer unreachable`, `host offline`, or `relay shutting down` |
| 1008 | Authentication failed, endpoint already registered, or unexpected control message |
| 1009 | A message exceeded `RELAY_MAX_MESSAGE_BYTES` |
| 1011 | The relay failed to complete the upgrade |
| 1013 | `queue limit exceeded`, `pair timeout`, or `peer write timeout` |

## Configuration

Everything is set through environment variables.

| Variable | Default | Description |
| --- | --- | --- |
| `RELAY_ADDR` | `127.0.0.1:8080` | Listen address. Port 0 picks a free port |
| `RELAY_MAX_MESSAGE_BYTES` | `1048576` | Largest single message. Must not exceed `RELAY_MAX_QUEUE_BYTES` |
| `RELAY_MAX_QUEUE_BYTES` | `4194304` | Bytes buffered per direction of a pair |
| `RELAY_MAX_QUEUE_MESSAGES` | `256` | Messages buffered per direction of a pair |
| `RELAY_MAX_CLIENTS` | `1024` | Pending and active pairs across the relay |
| `RELAY_MAX_CLIENTS_PER_HOST` | `128` | Pending and active pairs per host |
| `RELAY_MAX_PENDING_PER_HOST` | `32` | Pairs per host waiting to be accepted. Must not exceed `RELAY_MAX_CLIENTS_PER_HOST` |
| `RELAY_MAX_HOSTS` | `1024` | Control sockets, including ones still authenticating |
| `RELAY_AUTH_TIMEOUT_MS` | `5000` | Time a host has to answer the challenge |
| `RELAY_PAIR_TIMEOUT_MS` | `5000` | Time a client waits for the host to accept |
| `RELAY_WRITE_TIMEOUT_MS` | `5000` | Deadline for each socket write |
| `RELAY_HEARTBEAT_MS` | `15000` | Ping interval. A socket that stays silent for twice this long is closed |
| `RELAY_ADMISSION_RATE` | `100` | Upgrade attempts per second per client IP, with an equal burst |
| `RELAY_TRUSTED_PROXIES` | | Comma-separated CIDRs. Requests from these addresses are rate limited by the rightmost untrusted `X-Forwarded-For` hop |

An invalid value makes the relay print the variable's name and exit with code 2.

## Deployment

By default the relay listens for plain `ws://` on loopback. To expose it, put it behind a reverse proxy that terminates TLS and forwards WebSocket upgrades, and have clients connect over `wss://`. Set `RELAY_TRUSTED_PROXIES` to the proxy's address so rate limiting sees real client IPs.

Keys, connection IDs, and tokens travel in the query string, so configure the proxy not to log URLs. The relay never logs them.

On SIGTERM or SIGINT the relay stops accepting new hosts and clients and reports 503 on `/healthz`. Active pairs get a little over 4 seconds to finish. After that the relay closes every remaining socket with 1001 and exits within 5 seconds.

All state lives in memory. After a restart, hosts register again with the same key and keep the same endpoint ID, and clients open new pairs. Reconnecting is up to the endpoints, so run the relay under a process supervisor.

### Hosted instance

The public relay runs on the exe.dev VM `supacode-relay.exe.xyz`. The exe.dev proxy terminates TLS and forwards to the relay on `127.0.0.1:8080`. On the VM, the relay runs as the systemd service in `deploy/supacode-relay.service`.

`.github/workflows/deploy.yml` runs `make test`, builds a linux/amd64 binary, and installs it with `deploy/install.sh`. The install script restarts the service and waits for `/healthz` to report ok. The workflow connects with an SSH key stored as the `EXE_SSH_KEY` secret in the `production` environment, which only `main` can deploy to. That key is registered on exe.dev for VMs tagged `supacode-relay-deploy`, so it can't reach any other VM. To deploy by hand, run the workflow from the Actions tab.

## Design

- Every socket has one reader and one writer goroutine, and each direction of a pair has its own bounded queue. When a queue fills up, the relay closes that pair with 1013 instead of blocking, so a slow peer can't stall other pairs.
- Pair state is guarded by one mutex, and each pair is released exactly once. Tokens are tied to a single host registration, so they stop working once that host disconnects, even if it registers again.
- Delivery is best effort. There are no acknowledgements or retries, and anything still queued when a socket drops is lost.

## Project layout

```text
cmd/relay          entry point: config, listener, signal handling
internal/relay     server, host registration, pairing, forwarding, rate limiting
internal/endpoint  host and client helpers used by the tests
e2e                end-to-end tests that drive the built binary over real sockets
deploy             systemd unit and install script for the hosted instance
```

## License

[MIT](LICENSE)
