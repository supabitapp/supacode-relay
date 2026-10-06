# Supacode relay

A WebSocket relay that connects clients to hosts that aren't reachable directly. Hosts authenticate with an Ed25519 key, and the relay forwards frames between each client and its host without looking at them. Encryption and authorization are up to the endpoints.

The public relay runs at `wss://supacode-relay.exe.xyz`. It's a router in front of three relay nodes. See [docs/multi-node.md](docs/multi-node.md).

## Development

Requires Go 1.25 or later.

```bash
make build         # bin/relay and bin/relay-router
make run           # listen on 127.0.0.1:8080
make test          # gofmt, go vet, and all tests with -race
make e2e-docker    # Docker Compose suite: one router, three nodes
make bench-paths   # direct node vs router-to-node on loopback
make bench-docker  # the same comparison inside Docker
```

## Protocol

1. The host opens `/v1/control?publicKey=B64` and answers the relay's challenge.
2. A client opens `/v1/connect?endpointId=HEX`.
3. The relay sends the host `{"type":"incoming","connectionId":"ID","token":"TOKEN"}`.
4. The host opens `/v1/accept?endpointId=HEX&connectionId=ID&token=TOKEN`, and the two sockets are paired.
5. When either side closes, the relay sends the host `{"type":"closed","connectionId":"ID"}`.

Frames keep their order, boundaries, and text or binary type. Anything a client sends before the host accepts is buffered and delivered once the pair is ready.

Every buffered payload counts against one ingress budget per node. When the budget is full, the relay splits it evenly between the pair directions currently holding data. A pair holding more than its share is closed with 1013 to make room, so a pair under its share is never closed for capacity. A single busy pair can still use the whole budget while no other pair needs it.

`endpointId` is the lowercase hex SHA-256 of the host's 32-byte Ed25519 public key. Base64 values are unpadded Base64URL.

### Authentication

```text
relay → host  {"type":"challenge","nonce":"B64"}
host  → relay {"type":"authenticate","signature":"B64"}
relay → host  {"type":"registered","endpointId":"HEX"}
```

The host signs `"supacode-relay-v1\n" + endpointId + "\n" + nonce`. A bad signature or a timeout closes the socket with 1008. If the same endpoint registers again and authenticates, the newer registration wins and the older control socket closes with 4001. A host that gets 4001 shouldn't reconnect automatically, because something else holding its key took over. After registering, the host must not send anything on the control socket, or the relay closes it.

Treat `connectionId` as opaque. Behind a router it looks like `node-a.RANDOM`.

### Close codes

| Code | Reason |
| --- | --- |
| peer's code | Forwarded from the other side |
| 1001 | Peer disconnected or timed out, host went offline, relay is shutting down, or relay is draining (reconnect to land on another node) |
| 1008 | Authentication failed or unexpected control message |
| 1009 | Message larger than `RELAY_MAX_MESSAGE_BYTES` |
| 1011 | Upgrade failed |
| 1013 | Queue full, ingress capacity exceeded, pair timeout, or write timeout |
| 4001 | Registration superseded by a newer one for the same endpoint |

### HTTP

`GET /healthz` returns 200, or 503 while shutting down. `GET /metrics` returns JSON counters, including raw and weighted ingress reservations and `ingressEvictions`, the pairs closed to make room in the budget. Both move to `RELAY_PRIVATE_ADDR` when it's set. `relay healthcheck` and `relay metrics` query them locally.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `RELAY_ADDR` | `127.0.0.1:8080` | Listen address |
| `RELAY_MAX_MESSAGE_BYTES` | `33554418` | Largest single message |
| `RELAY_MAX_QUEUE_BYTES` | `67108864` | Bytes buffered per direction of a pair |
| `RELAY_MAX_QUEUE_MESSAGES` | `256` | Messages buffered per direction of a pair |
| `RELAY_MAX_CLIENTS` | `20000` | Pairs across the relay |
| `RELAY_MAX_CLIENTS_PER_HOST` | `20000` | Pairs per host |
| `RELAY_MAX_PENDING_PER_HOST` | `20000` | Pairs per host waiting to be accepted |
| `RELAY_MAX_HOSTS` | `20000` | Connected hosts |
| `RELAY_INGRESS_BUDGET_BYTES` | `536870912` | Weighted global payload budget |
| `RELAY_INGRESS_WEIGHT` | `4` | Multiplier applied while each payload is read, queued, or written |
| `RELAY_AUTH_TIMEOUT_MS` | `5000` | Time to answer the challenge |
| `RELAY_PAIR_TIMEOUT_MS` | `5000` | Time for the host to accept a client |
| `RELAY_WRITE_TIMEOUT_MS` | `5000` | Deadline for each socket write |
| `RELAY_HEARTBEAT_MS` | `15000` | Ping interval |
| `RELAY_ADMISSION_RATE` | `100` | Connection attempts per second per IP |
| `RELAY_TRUSTED_PROXIES` | | CIDRs whose `X-Forwarded-For` is trusted |
| `RELAY_CLIENT_IP_HEADER` | | Header carrying the client IP from a trusted proxy, such as `X-Relay-Client-Ip` from a router |
| `RELAY_PRIVATE_ADDR` | | Separate listener for `/healthz` and `/metrics` |
| `RELAY_ALLOWED_PEERS` | | CIDRs allowed to connect at all. Other peers are closed before any HTTP is read |
| `RELAY_PRIVATE_ALLOWED_PEERS` | | The same filter for the private listener |
| `RELAY_NODE_ID` | | Node id, used as the `connectionId` prefix |
| `RELAY_ROUTERS` | | Router directory URLs. Setting this turns on multi-node mode |
| `RELAY_DIRECTORY_TOKEN` | | Directory secret, or `RELAY_DIRECTORY_TOKEN_FILE` |
| `RELAY_ADVERTISE_URL` | listen address | Origin routers use to reach this node |
| `RELAY_DIRECTORY_HEARTBEAT_MS` | `1000` | Directory stream ping interval |
| `RELAY_DIRECTORY_RETRY_MAX_MS` | `2000` | Directory reconnect backoff cap |

The router (`cmd/router`) has its own `ROUTER_*` settings, listed in [docs/multi-node.md](docs/multi-node.md).

## License

[MIT](LICENSE)
