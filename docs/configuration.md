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
| `RELAY_MAX_CONNS` | `16384` | Concurrent protocol connections, including authentication and pairing |
| `RELAY_MAX_CONNS_PER_IP` | `512` | Concurrent protocol connections per client IP |
| `RELAY_AUTH_TIMEOUT_MS` | `5000` | Time to answer the challenge |
| `RELAY_PAIR_TIMEOUT_MS` | `5000` | Time for the host to complete accepting a client |
| `RELAY_WRITE_TIMEOUT_MS` | `5000` | Deadline for control and close writes |
| `RELAY_DELIVERY_TIMEOUT_MS` | `30000` | How long a write to a pair's receiver may stay blocked before the receiver is closed |
| `RELAY_HEARTBEAT_MS` | `15000` | Ping interval |
| `RELAY_ADMISSION_RATE` | `100` | Connection attempts per second per IP |
| `RELAY_TRUSTED_PROXIES` | | CIDRs whose `X-Forwarded-For` is trusted |
| `RELAY_CLIENT_IP_HEADER` | | Header carrying the client IP from a trusted proxy; otherwise use `X-Forwarded-For` |
| `RELAY_PRIVATE_ADDR` | | Separate listener for `/healthz` and `/metrics` |
| `RELAY_ALLOWED_PEERS` | | CIDRs allowed to connect at all. Other peers are closed before any HTTP is read |
| `RELAY_PRIVATE_ALLOWED_PEERS` | | The same filter for the private listener |

The [production service](../deploy/supacode-relay.service) targets a 4 GiB machine: 8,000 hosts and pairs, 10,000 protocol connections, and 1,500 connections per client IP. It sets a 2 GiB soft Go memory target and a 3 GiB systemd memory ceiling. These are admission and containment limits, not measured operating capacity. The systemd ceiling can terminate an overloaded service; allow space for control queues and socket memory when sizing a deployment.

`RELAY_INGRESS_BUDGET_BYTES` and `RELAY_INGRESS_WEIGHT` are ignored. Streaming forwarding has no payload queue or ingress eviction budget.
