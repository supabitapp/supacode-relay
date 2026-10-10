# Multi-node relay

A router in front of several relay nodes. Clients and hosts keep using `/v1/control`, `/v1/connect`, and `/v1/accept`. The router decides which node serves each request.

## Architecture

```text
clients/hosts ──▶ router public :8080 ──(standard HTTP upgrade proxy)──▶ node public :8080
                                   ▲
nodes ── authenticated directory stream ──▶ router private :9090 (/healthz, /metrics, /v1/directory)
```

**Ownership.** An endpoint is owned by the node that holds its authenticated control socket. There is no hashing of endpoints to nodes, and there is no external coordinator.

**Directory.** Each node keeps one WebSocket stream to every router's private `/v1/directory` endpoint, authenticated by `Authorization: Bearer <RELAY_DIRECTORY_TOKEN>`. The node sends `hello` (node id, advertised origin, draining flag), then a `snapshot` of its registrations, then `put` and `del` updates and `drain`. A registration is `{endpointId, nodeId, registrationId, version}`. `version` is a hybrid logical clock in Unix nanoseconds. It never goes backwards on a node, and it advances past every version the node has seen from routers. The router assigns each stream a session and caches the directory in memory. When the stream closes, misses heartbeats, or is replaced by a newer session for the same node id, the router purges that session's entries.

**Ordering and fencing.**

- Puts are ordered by `(version, registrationId)`. The newer registration wins regardless of arrival order. If an older registration from another node arrives or survives, the router sends `evict` to that node, and the node closes that control socket with 4001.
- A `del` applies only when the stored entry has the same node and registration id. A late remove from a node that lost the host cannot delete a newer registration elsewhere.
- Messages from a replaced session are ignored.
- Cross-node ordering assumes node clock skew is smaller than the time a host takes to reconnect. Control routing prefers the current owner node (see below), so a re-registration usually lands on the same node, where ordering is exact.

**Routing.** The router's public listener serves only `GET /v1/control`, `/v1/connect`, and `/v1/accept`. Every other path returns 404, and other methods on those paths return 405. Proxying uses `net/http/httputil.ReverseProxy`'s protocol-switch path. That path reads backend bytes through the transport's buffered reader first, so a challenge frame that arrives in the same TCP segment as the `101` response is delivered (tested with a raw backend that writes both in one `write`).

- Control: the current owner if it is healthy and not draining, otherwise the non-draining node with the fewest control sockets through this router. A dial failure, a response-header timeout, or a `503` (draining or full) is retried once on another node.
- Connect: directory lookup. On a miss, the router runs a refresh: a `sync` barrier sent to every node stream, which the node acks after all earlier updates. Then it looks up again. If the node answers `404` or `503` before the upgrade, or the dial fails, the router refreshes and retries once. The request has no body and nothing has reached the client, so the retry is safe. Concurrent refreshes share barrier rounds. A round waits at most `ROUTER_REFRESH_TIMEOUT_MS`.
- Accept: the node prefix of `connectionId`. Accepts are never retried elsewhere, because the pair exists on one node. A node that left the directory stays routable for accepts for `ROUTER_ACCEPT_GRACE_MS`.

**Drain.** On SIGTERM a multi-node relay publishes `drain`, removes its hosts from the directory, closes their control sockets with `1001 relay draining`, and keeps every pending and active pair until it finishes or the 5 s budget runs out. Hosts re-register through the router on another node. Accepts for pairs on the draining node still succeed. Registrations that race the drain get `1001` too. Routers stop sending controls to a draining node, and a `503` from it is retried elsewhere.

**Client IP.** The router overwrites `X-Forwarded-For` and `X-Relay-Client-Ip` with exactly one address: the TCP peer, or the rightmost untrusted hop when the peer is in `ROUTER_TRUSTED_PROXIES`. It also drops `Forwarded`, `X-Real-IP`, `True-Client-IP`, and `CF-Connecting-IP`. Nodes should trust only the routers (`RELAY_TRUSTED_PROXIES`), and should accept connections only from the routers (`RELAY_ALLOWED_PEERS`). If something between the router and the node rewrites `X-Forwarded-For`, set `RELAY_CLIENT_IP_HEADER=X-Relay-Client-Ip` on the node. Nodes honor that header only from trusted peers, and only when it is set. The router enforces its own per-client admission rate and concurrent-connection limits, because a node only sees the share of a client's traffic that the router sends to it.

**Half-closed clients.** When a node closes a proxied socket, the router forwards the FIN to the client and fully closes the client socket 2 s later if the client has not closed it. This stops a client that never closes from holding a router socket.

**Logs.** Connection records use shared trace IDs and hashed endpoint and pair tags, so requests, directory changes, retries, byte flow, and closures can be matched across the router and nodes. See [Diagnostics](../README.md#diagnostics) for filtering commands. Neither binary logs URLs, keys, raw endpoint or connection IDs, tokens, nonces, or payloads. The router also passes everything it logs (including `net/http` and `httputil` messages) through a redactor that removes query strings and 64-hex runs.


## Router configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `ROUTER_ADDR` | `127.0.0.1:8080` | Public listener (protocol paths only) |
| `ROUTER_PRIVATE_ADDR` | `127.0.0.1:9090` | `/healthz` (200 once at least one node is ready, 503 while draining), `/metrics`, `/v1/directory` |
| `ROUTER_PRIVATE_ALLOWED_PEERS` | empty | CIDRs allowed to connect to the private listener. Others are closed before any HTTP is read |
| `ROUTER_DIRECTORY_TOKEN` / `ROUTER_DIRECTORY_TOKEN_FILE` | required | Shared directory secret, at least 16 characters |
| `ROUTER_TRUSTED_PROXIES` | empty | Load balancers allowed to supply `X-Forwarded-For` |
| `ROUTER_ADMISSION_RATE` | 100 | Upgrade attempts per second per client IP (burst equals the rate). 429 when exceeded |
| `ROUTER_MAX_CONNS` | 16384 | Concurrent proxied connections. 503 when exceeded |
| `ROUTER_MAX_CONNS_PER_IP` | 512 | Concurrent proxied connections per client IP. 429 when exceeded |
| `ROUTER_DIRECTORY_HEARTBEAT_MS` | 1000 | Directory ping interval. A node silent for 2 × this interval is purged |
| `ROUTER_REFRESH_TIMEOUT_MS` | 500 | Longest wait of one refresh barrier round |
| `ROUTER_ACCEPT_GRACE_MS` | 30000 | How long a departed node stays routable for accepts |
| `ROUTER_UPSTREAM_DIAL_TIMEOUT_MS` | 2000 | Node dial timeout |
| `ROUTER_UPSTREAM_HEADER_TIMEOUT_MS` | 3000 | Wait for the node's upgrade response |
| `ROUTER_DRAIN_TIMEOUT_MS` | 5000 | On SIGTERM, health reports 503 and new controls and connects get 503. Accepts are still allowed, and existing sockets are cut after this long |

`relay-router healthcheck` and `relay-router metrics` work like the relay subcommands.


## Production on exe.dev

`wss://supacode-relay.exe.xyz` runs as four VMs:

| VM | Role | Listeners |
| --- | --- | --- |
| `supacode-relay` | router, the VM's public HTTPS port | `127.0.0.1:8080` public, `127.0.0.1:9090` private |
| `supacode-relay-node-a`, `-b`, `-c` | relay nodes, all ports private | `127.0.0.1:8080` relay, `127.0.0.1:9090` health and metrics |

exe.dev has no private network between VMs. Traffic between them uses peer integrations, which are HTTPS proxies that inject a VM-scoped credential and attest the caller:

- `relay-node-a`, `relay-node-b`, `relay-node-c`, attached to the router VM, reach each node's port 8080. Nodes advertise `https://relay-node-X.int.exe.xyz`.
- `relay-directory`, attached to the three nodes, reaches the router's port 9090. Nodes set `RELAY_ROUTERS=https://relay-directory.int.exe.xyz`.

The integration proxy replaces `X-Forwarded-For` with the router VM's own address, so nodes take the client IP from `X-Relay-Client-Ip`, sent by the trusted local proxy at `127.0.0.1`. Node ports are reachable only through these integrations or by members of the exe.dev account, so a client cannot reach a node directly to forge that header.

The directory token lives in `/etc/supacode-relay/directory-token` (root, `0600`) on all four VMs. systemd passes it to the services with `LoadCredential`, so it is never in an environment variable or in the repository. To rotate it, write the new value to all four VMs, then restart the router and the nodes.

Every push to `main` deploys through `.github/workflows/deploy.yml`:

1. Tests run.
2. The nodes roll one at a time with `deploy/install-node.sh`. Each node drains: its hosts re-register on other nodes, and its pairs finish or get the 5 s budget.
3. The router restarts with `deploy/install-router.sh`. This drops every proxied socket, and clients reconnect.
4. `relay-probe -mode smoke` registers hosts and exchanges verified messages through the public URL.

The deploy key is scoped to the `supacode-relay-deploy` tag, so a new node needs only that tag, a token file, an integration, and an entry in `NODES`.

## Container image and Compose harness

The `Dockerfile` has three `scratch` targets, each about 7 MB: `relay` (default), `router`, and `tools` (path benchmark and probe). All run as UID 65532. Listeners default to `0.0.0.0:8080` (public) and `0.0.0.0:9090` (private). Each image has a `HEALTHCHECK` that runs the built-in probe, and `STOPSIGNAL SIGTERM`. Allow at least 10 s of stop grace.

`e2e/docker/compose.yaml` runs one router and three nodes (`node-a`, `node-b`, `node-c`, plus `node-d` under the `extra` profile) with explicit node ids, read-only root filesystems, all capabilities dropped, and `no-new-privileges`. It uses two networks:

- `public` (10.231.2.0/24): the router's public listener, published only on `127.0.0.1:${E2E_ROUTER_PORT:-18480}`, plus the probe container at 10.231.2.50.
- `private` (10.231.1.0/24, `internal`): the router's private listener, bound to 10.231.1.10, and the nodes.

Nodes trust and accept only the router address. The router's private listener accepts only 10.231.1.8/29.

Platform note: on OrbStack, a container on `public` can reach addresses on the `internal` `private` network through the host bridge. The traffic arrives masqueraded as the private gateway `10.231.1.1`. Docker Engine on Linux normally drops that traffic between bridges. Do not rely on the network flag alone. The peer allowlists above close those connections before any HTTP is read, and `TestPublicSurface` checks this from the probe container.

## Compatibility and scope

- A single node without the new variables keeps its protocol, paths, and defaults, except that duplicate authenticated registration is now newest-wins with 4001 instead of 1008.
- In multi-node mode, clients use the same three paths through the router. Hosts must treat `connectionId` as opaque, as before. It now contains a node prefix and a `.`.
- Relay payloads stay opaque and end-to-end authentication stays in the endpoints. The router never looks inside upgraded streams.
- Not in this slice: an external registry (etcd or Consul), router-to-router state sharing, TLS between routers and nodes or on the directory stream (run them on a private network or behind mTLS sidecars), migrating live pairs between nodes, and service discovery for routers. Nodes list routers explicitly.

## Tests

`make e2e-docker` builds the images, starts the stack, and drives it from the host through the published router port, `docker compose` (kill, pause, network disconnect, restart, scale), and a probe container on the public network. Test hosts reconnect with jittered backoff (50 ms doubling to 500 ms), detect silent loss with a 250 ms ping and a 1 s deadline, and stop reconnecting on 4001. Every scenario checks payload type and bytes on each echo, which is tagged with the serving host. That catches corruption, duplication, reordering, and cross-delivery.

| Test | What it proves |
| --- | --- |
| `TestPublicSurface` | Through the public router, `/`, `/healthz`, `/metrics`, `/v1/directory` (even with the token and upgrade headers), and path tricks return 404. From the public network, the router's private listener and every node listener are unreachable |
| `TestPairingAcrossThreeNodes` | 12 hosts placed 4/4/4. 36 concurrent clients × 60 echoes, plus a 1000-message windowed stream per host with mixed text, binary, and sizes up to 4 KiB. No integrity failures and no failed accepts |
| `TestDuplicateIdentityNewestWins` | The second registration of a key routes to the same node, the old one gets 4001, and traffic goes only to the newest |
| `TestNodeGracefulDrain` | SIGTERM on a node. Its hosts re-register elsewhere, its active pairs keep exchanging, a delayed accept on the draining node succeeds, and hosts on other nodes are not disturbed |
| `TestNodeSIGKILL` | Hosts on the killed node re-register, the router purges it, everything is reachable, and other hosts are not disturbed |
| `TestNodeSilentPause` | `docker pause`, which keeps TCP open but silent. Hosts move after missed heartbeats, the router purges the node after 2 directory heartbeats, and after unpause the node rejoins with no stale routing |
| `TestNodeNetworkLoss` | `docker network disconnect` with the production 15 s relay heartbeat. Hosts move while stale registrations survive on the partitioned node. After healing, the router evicts every stale registration and none is ever routed |
| `TestMembershipChange` | Adds `node-d` under continuous load (6 workers), steers new hosts to it, and then drains it. Existing hosts never re-register, and there are no failed operations or accepts |
| `TestRouterRestart` | Graceful restart and SIGKILL of the router. All hosts come back and the directory is rebuilt from node snapshots |
| `TestRateLimitingThroughProxy` | With node rate 5, a client spoofing `X-Forwarded-For`, `X-Real-IP`, and `Forwarded` as the probe's address is limited, while the probe itself is still admitted. So nodes key on the real client IP propagated by the router. The same check runs with router rate 5 |
| Log hygiene (after all tests) | Every endpoint id, public key, nonce, connection id, and token used in the run is absent from all container logs, and so are `panic:` and `DATA RACE` |

The run writes `report.json` (to `$E2E_OUT` or a temp directory) with timings. Set `E2E_KEEP=1` to keep the stack, `E2E_SKIP_BUILD=1` to reuse images, and `E2E_ROUTER_PORT` to move the published port.

## Direct node vs router → node

Both paths serve the same node. In the direct path, hosts and clients dial the node. In the routed path, hosts register through the router and clients connect through it, so every echoed message crosses two more sockets. Workload: 4 echo hosts, 4 messages in flight per client, 2 s warmup, then a 5 s window, with direct and routed run back to back. Every case had 0 failures, 0 timeouts, and 0 corrupt messages.

Loopback on a shared Mac (`make bench-paths`, release build). The machine was busy, so compare the direct and routed columns with each other rather than reading them as absolute numbers. Node CPU is shown as direct / routed.

| Payload | Clients | Direct p50 / p99 | Direct msg/s | Routed p50 / p99 | Routed msg/s | Node CPU | Router CPU / RSS |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 64 B | 1 | 150 / 275 µs | 24.0k | 255 / 428 µs | 14.6k | 125% / 80% | 88% / 15 MiB |
| 64 B | 32 | 3.78 / 4.43 ms | 33.8k | 7.36 / 8.17 ms | 17.5k | 333% / 171% | 269% / 26 MiB |
| 1 KiB | 1 | 135 / 284 µs | 25.5k | 289 / 504 µs | 12.8k | 127% / 83% | 87% / 26 MiB |
| 1 KiB | 32 | 3.74 / 4.71 ms | 34.1k | 7.40 / 8.48 ms | 17.4k | 366% / 190% | 272% / 28 MiB |
| 64 KiB | 1 | 0.92 / 1.53 ms | 3.9k (245 MiB/s) | 1.49 / 2.28 ms | 2.5k (158 MiB/s) | 252% / 156% | 127% / 28 MiB |
| 64 KiB | 32 | 30.5 / 45.2 ms | 4.1k (255 MiB/s) | 57.2 / 62.6 ms | 2.3k (141 MiB/s) | 353% / 199% | 176% / 29 MiB |

Docker (`make bench-docker`), using OrbStack 29.4.0 (linux/arm64 VM) on the same Mac. The stack is the router plus `node-a`. The driver runs in a container attached to both networks. CPU and memory come from `docker stats`, sampled about every 1.5 s across each whole phase, including host setup.

| Payload | Clients | Direct p50 / p99 | Direct msg/s | Routed p50 / p99 | Routed msg/s |
| --- | --- | --- | --- | --- | --- |
| 64 B | 1 | 70 / 172 µs | 46.1k | 124 / 300 µs | 27.9k |
| 64 B | 32 | 0.34 / 5.99 ms | 165k | 0.99 / 5.90 ms | 102k |
| 1 KiB | 1 | 78 µs / 1.16 ms | 32.7k | 144 µs / 1.05 ms | 21.2k |
| 1 KiB | 32 | 0.70 / 8.43 ms | 72.6k | 1.81 / 8.16 ms | 49.6k |
| 64 KiB | 1 | 2.38 / 7.65 ms | 1.37k (86 MiB/s) | 2.33 / 8.19 ms | 1.34k (84 MiB/s) |
| 64 KiB | 32 | 17.6 / 69.1 ms | 5.68k (355 MiB/s) | 18.3 / 62.4 ms | 5.70k (356 MiB/s) |

Phase resources:

- Direct: the node averaged 132% CPU (peak 358%) and peaked at 87 MiB, while the router stayed idle at 1% and 5 MiB.
- Routed: the node averaged 109% CPU (peak 256%) and peaked at 109 MiB, and the router averaged 107% CPU (peak 309%) and peaked at 21 MiB.

At 64 KiB both paths are capped by something other than the router inside the VM, most likely the driver container or the bridge.

What these numbers support: for small and medium messages the router hop adds roughly 55–150 µs of median latency with one client. It cuts throughput by 30–50% at 32 clients, and the router uses about as much CPU as the node it fronts. They are not production measurements. Everything runs on one shared host over macOS loopback or a desktop VM's bridge, with no TLS, no real NICs, and no cross-host latency. Docker Desktop and OrbStack networking in particular is not representative of production networking.
