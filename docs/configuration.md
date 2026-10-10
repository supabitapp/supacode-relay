# Configuration

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

The router (`cmd/router`) has its own `ROUTER_*` settings, listed in [Multi-node relay](multi-node.md).

The deployment units target 4 GiB machines: 8,000 hosts and pairs per node, 10,000 upgraded connections on the router, a 2 GiB soft Go memory target, and a 3 GiB systemd memory ceiling. These are admission and containment limits, not measured operating capacity. The systemd ceiling can terminate an overloaded service; allow space for control queues, TLS and socket memory when sizing a deployment.

`RELAY_INGRESS_BUDGET_BYTES` and `RELAY_INGRESS_WEIGHT` are ignored. Streaming forwarding has no payload queue or ingress eviction budget.
