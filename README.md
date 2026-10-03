# Supacode relay

A WebSocket relay that connects clients to hosts that aren't reachable directly. Hosts authenticate with an Ed25519 key, and the relay forwards frames between each client and its host without looking at them. Encryption and authorization are up to the endpoints.

The public relay runs at `wss://supacode-relay.exe.xyz`.

## Development

Requires Go 1.25 or later.

```bash
make build   # bin/relay
make run     # listen on 127.0.0.1:8080
make test    # gofmt, go vet, and all tests with -race
```

## Protocol

1. The host opens `/v1/control?publicKey=B64` and answers the relay's challenge.
2. A client opens `/v1/connect?endpointId=HEX`.
3. The relay sends the host `{"type":"incoming","connectionId":"ID","token":"TOKEN"}`.
4. The host opens `/v1/accept?endpointId=HEX&connectionId=ID&token=TOKEN`, and the two sockets are paired.
5. When either side closes, the relay sends the host `{"type":"closed","connectionId":"ID"}`.

Frames keep their order, boundaries, and text or binary type. Anything a client sends before the host accepts is buffered and delivered once the pair is ready.

`endpointId` is the lowercase hex SHA-256 of the host's 32-byte Ed25519 public key. Base64 values are unpadded Base64URL.

### Authentication

```text
relay → host  {"type":"challenge","nonce":"B64"}
host  → relay {"type":"authenticate","signature":"B64"}
relay → host  {"type":"registered","endpointId":"HEX"}
```

The host signs `"supacode-relay-v1\n" + endpointId + "\n" + nonce`. A bad signature, a timeout, or registering an endpoint that's already online closes the socket with 1008. After registering, the host must not send anything on the control socket, or the relay closes it.

### Close codes

| Code | Reason |
| --- | --- |
| peer's code | Forwarded from the other side |
| 1001 | Peer disconnected or timed out, host went offline, or relay is shutting down |
| 1008 | Authentication failed or unexpected control message |
| 1009 | Message larger than `RELAY_MAX_MESSAGE_BYTES` |
| 1011 | Upgrade failed |
| 1013 | Queue full, pair timeout, or write timeout |

### HTTP

`GET /healthz` returns 200, or 503 while shutting down. `GET /metrics` returns JSON counters.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `RELAY_ADDR` | `127.0.0.1:8080` | Listen address |
| `RELAY_MAX_MESSAGE_BYTES` | `1048576` | Largest single message |
| `RELAY_MAX_QUEUE_BYTES` | `4194304` | Bytes buffered per direction of a pair |
| `RELAY_MAX_QUEUE_MESSAGES` | `256` | Messages buffered per direction of a pair |
| `RELAY_MAX_CLIENTS` | `1024` | Pairs across the relay |
| `RELAY_MAX_CLIENTS_PER_HOST` | `128` | Pairs per host |
| `RELAY_MAX_PENDING_PER_HOST` | `32` | Pairs per host waiting to be accepted |
| `RELAY_MAX_HOSTS` | `1024` | Connected hosts |
| `RELAY_AUTH_TIMEOUT_MS` | `5000` | Time to answer the challenge |
| `RELAY_PAIR_TIMEOUT_MS` | `5000` | Time for the host to accept a client |
| `RELAY_WRITE_TIMEOUT_MS` | `5000` | Deadline for each socket write |
| `RELAY_HEARTBEAT_MS` | `15000` | Ping interval |
| `RELAY_ADMISSION_RATE` | `100` | Connection attempts per second per IP |
| `RELAY_TRUSTED_PROXIES` | | CIDRs whose `X-Forwarded-For` is trusted |

## Deployment

Run the relay behind a reverse proxy that terminates TLS, and set `RELAY_TRUSTED_PROXIES` to the proxy's address. Keys and tokens travel in the query string, so don't let the proxy log URLs. On SIGTERM the relay stops accepting connections, gives active pairs a few seconds to finish, and exits within 5 seconds.

## License

[MIT](LICENSE)
