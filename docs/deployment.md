# Deployment

The deployment workflow installs one standalone relay on `supacode-relay.exe.xyz`. The relay serves clients and hosts directly behind the existing HTTPS proxy at `wss://relay.supacode.sh`.

The [service unit](../deploy/supacode-relay.service) binds protocol traffic to `127.0.0.1:8080` and private health and detailed metrics to `127.0.0.1:9090`. The exe.dev proxy forwards the public TLS endpoint to port 8080. Only the local proxy may supply the client IP, and the private listener accepts loopback peers only. Public `/metrics` contains aggregate counters; host traffic details stay on the private listener.

Code changes pushed to `main` run [the deployment workflow](../.github/workflows/deploy.yml): the race test suite, a Linux build, service installation, and verified smoke exchanges through both public hostnames. Restarting the relay closes its sockets, so hosts and clients must reconnect. Each relay instance owns its authenticated registrations and pairs in memory; distributing an endpoint's sockets across independent instances is unsupported.

The [installer](../deploy/install-relay.sh) stops an active legacy router before starting the relay on the same ports. If installation or the local readiness check fails, it restarts that router. After readiness succeeds, it removes the router unit, binary, and obsolete directory credentials. The standalone service requires no directory token or peer integration.

The former backend VMs (`supacode-relay-node-a`, `-b`, and `-c`) and the `relay-node-a`, `relay-node-b`, `relay-node-c`, and `relay-directory` integrations are outside the standalone deployment. Retire these account resources after the public smoke checks pass and clients reconnect. The workflow accesses only the public relay VM, so subsequent deployments do not depend on the former nodes.

The [retained performance measurements](multi-node.md#public-tls-colocation-measurements) motivate avoiding the outbound integration path. They measure colocation with the router still present, and do not establish throughput for the standalone release or its maximum capacity. A single relay also has one failure domain; this deployment does not implement automatic failover.
