# Supacode relay

A WebSocket relay that connects clients to hosts that aren't reachable directly. Hosts authenticate with an Ed25519 key, and the relay forwards messages between each client and its host without looking at them. Encryption and authorization are up to the endpoints.

The public relay runs at `wss://relay.supacode.sh`. It's a router in front of three relay nodes. The original `wss://supacode-relay.exe.xyz` address also works. See [docs/multi-node.md](docs/multi-node.md).

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

Messages keep their order, boundaries, and text or binary type. Before acceptance the relay pauses application reads; early bytes wait in the socket buffers and arrive once the pair is ready.

Each direction streams directly into a fixed WebSocket writer buffer. A slow receiver stops its sender through TCP backpressure. The configured data buffers total about 40 KiB per pair, plus framing, connection state, goroutine stacks and socket memory. They do not grow with message size or elapsed traffic. Pending, active and closing connections retain their admission slots until cleanup finishes.

A blocked receiver is closed with 1013 after `RELAY_DELIVERY_TIMEOUT_MS`. Oversized or interrupted messages may forward partial fragments, but those fragments are never finalized into a successful shorter message. Close frames are attempted within a bounded deadline; an aborted or stalled transport can prevent their delivery. Control notifications and directory updates use separate bounded queues.

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
| 1013 | Pair timeout, delivery timeout, or control queue full |
| 4001 | Registration superseded by a newer one for the same endpoint |

### HTTP

`GET /healthz` returns 200, or 503 while shutting down. `GET /metrics` returns JSON counters, including active and closing admission slots, configured data-buffer capacity, Go heap and allocation counters, and `topHosts`. `topHosts` lists the 20 hosts on the node that have relayed the most bytes since they registered, with the first 16 hex characters of each endpoint ID, its diagnostic `traceTag`, bytes relayed to and from the host, and open pairs. Both move to `RELAY_PRIVATE_ADDR` when it's set. `relay healthcheck` and `relay metrics` query them locally.

The router exposes public stats at [https://relay.supacode.sh/metrics](https://relay.supacode.sh/metrics). See [router metrics](docs/multi-node.md#metrics) for the published fields and access limits.

## Diagnostics

Connection lifecycle logs are enabled by default and written as JSON to stderr. Follow `trace_id` across the router and node to see admission, directory lookup, upstream selection, upgrades, authentication, first-byte timing, pairing, and closure. `pair_tag` connects the client's and host's separate requests; `endpoint_tag` connects requests to registration and directory events. Close summaries include durations, close codes, and completed message and byte counts. Payloads, raw identifiers, credentials, and remote close-message contents are excluded.

To investigate a host, read its `topHosts[].traceTag` from node metrics, or run `supacode-relay trace-tag ENDPOINT_ID` with its full endpoint ID. The router binary supports the same command. Filter the relevant service's journal using that tag:

```sh
journalctl -u supacode-relay --since "10 minutes ago" -o cat |
  jq -R 'fromjson? | select(.endpoint_tag == "HOST_TRACE_TAG")'
```

Use `supacode-relay-router` as the journal unit for router records. A missing-endpoint record identifies a failed lookup; a pending pair closed for `pair timeout` identifies a host that never accepted; an active pair's peer failure and close summary identify where forwarding ended. These logs observe the outer relay connection. The encrypted application's own HTTP responses and errors remain opaque.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `RELAY_ADDR` | `127.0.0.1:8080` | Listen address |
| `RELAY_MAX_MESSAGE_BYTES` | `33554418` | Largest single message |
| `RELAY_MAX_QUEUE_BYTES` | `1048576` | Bytes queued per host control connection |
| `RELAY_MAX_QUEUE_MESSAGES` | `256` | Notifications queued per host control connection |
| `RELAY_MAX_CLIENTS` | `20000` | Pairs across the relay |
| `RELAY_MAX_CLIENTS_PER_HOST` | `256` | Pairs per host |
| `RELAY_MAX_PENDING_PER_HOST` | `64` | Pairs per host waiting to be accepted |
| `RELAY_MAX_HOSTS` | `20000` | Connected hosts |
| `RELAY_AUTH_TIMEOUT_MS` | `5000` | Time to answer the challenge |
| `RELAY_PAIR_TIMEOUT_MS` | `5000` | Time for the host to complete accepting a client |
| `RELAY_WRITE_TIMEOUT_MS` | `5000` | Deadline for control, close, and directory writes |
| `RELAY_DELIVERY_TIMEOUT_MS` | `30000` | How long a write to a pair's receiver may stay blocked before the receiver is closed |
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

The deployment units target 4 GiB machines: 8,000 hosts and pairs per node, 10,000 upgraded connections on the router, a 2 GiB soft Go memory target, and a 3 GiB systemd memory ceiling. These are admission and containment limits, not measured operating capacity. The systemd ceiling can terminate an overloaded service; allow space for control queues, TLS and socket memory when sizing a deployment.

`RELAY_INGRESS_BUDGET_BYTES` and `RELAY_INGRESS_WEIGHT` are ignored. Streaming forwarding has no payload queue or ingress eviction budget.

## License

[MIT](LICENSE)
