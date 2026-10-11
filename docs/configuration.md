# Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `RELAY_ADDR` | `127.0.0.1:8080` | Listen address |
| `RELAY_MAX_MESSAGE_BYTES` | `33554418` | Largest single message |
| `RELAY_MAX_QUEUE_BYTES` | `1048576` | Bytes queued per host control connection |
| `RELAY_MAX_QUEUE_MESSAGES` | `256` | Notifications queued per host control connection |
| `RELAY_MAX_CLIENTS` | `20000` | Pairs across the relay |
| `RELAY_MAX_CLIENTS_PER_HOST` | `64` | Pairs per host. Matches the Supacode host's 64-pair cap; excess connections receive HTTP 503 |
| `RELAY_MAX_CLIENTS_PER_HOST_PER_IP` | `16` | Pairs per host from one client network, including pending, active and closing pairs. Excess connections receive HTTP 429 |
| `RELAY_MAX_PENDING_PER_HOST` | `64` | Pairs per host waiting to be accepted. Must not exceed `RELAY_MAX_CLIENTS_PER_HOST`, and takes effect only below it |
| `RELAY_MAX_HOSTS` | `20000` | Connected hosts |
| `RELAY_MAX_CONNS` | `16384` | Concurrent protocol connections, including authentication and pairing |
| `RELAY_MAX_CONNS_PER_IP` | `512` | Concurrent protocol connections per client network |
| `RELAY_AUTH_TIMEOUT_MS` | `5000` | Time to answer the challenge |
| `RELAY_PAIR_TIMEOUT_MS` | `5000` | Time for the host to complete accepting a client |
| `RELAY_WRITE_TIMEOUT_MS` | `5000` | Deadline for control and close writes |
| `RELAY_DELIVERY_TIMEOUT_MS` | `30000` | How long a write to a pair's receiver may stay blocked before the receiver is closed |
| `RELAY_HEARTBEAT_MS` | `15000` | Ping interval |
| `RELAY_ADMISSION_RATE` | `100` | Connection attempts per second per client network |
| `RELAY_TRUSTED_PROXIES` | | CIDRs whose `X-Forwarded-For` is trusted |
| `RELAY_CLIENT_IP_HEADER` | | Header carrying the client IP from a trusted proxy; otherwise use `X-Forwarded-For` |
| `RELAY_PRIVATE_ADDR` | | Separate listener for `/healthz` and `/metrics` |
| `RELAY_ALLOWED_PEERS` | | CIDRs allowed to connect at all. Other peers are closed before any HTTP is read |
| `RELAY_PRIVATE_ALLOWED_PEERS` | | The same filter for the private listener |

Client quotas use one IPv4 address or one IPv6 `/64` prefix. IPv4-mapped IPv6 addresses share the IPv4 quota. Devices behind the same IPv4 NAT or within the same IPv6 `/64` share these limits. Keep `RELAY_MAX_CLIENTS_PER_HOST_PER_IP` below `RELAY_MAX_CLIENTS_PER_HOST` to leave capacity for other networks. Hosts with many clients on one network may need a higher per-network limit. The relay cannot distinguish application-authenticated clients inside encrypted traffic.

The [production service](../deploy/supacode-relay.service) targets a 4 GiB machine: 8,000 hosts and pairs, 10,000 protocol connections, and 1,500 connections per client network. It sets a 2 GiB soft Go memory target and a 3 GiB systemd memory ceiling. These are admission and containment limits, not measured operating capacity. The systemd ceiling can terminate an overloaded service; allow space for control queues and socket memory when sizing a deployment.

`RELAY_INGRESS_BUDGET_BYTES` and `RELAY_INGRESS_WEIGHT` are ignored. Streaming forwarding has no payload queue or ingress eviction budget.
