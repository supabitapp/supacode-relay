# APNs gateway

`cmd/push` runs an optional notification service beside the live relay. Run it as
a separate process and HTTPS origin. The existing relay deployment does not
install or expose it.

The gateway sends the fixed alert “Supacode has an update” with opaque recipient
and notification identifiers. It stores APNs tokens, recipient generations,
revocation hashes, and bounded delivery receipts. It receives no conversation
content and has no notification queue. The Supacode host owns unsent work and
retries until expiry; APNs acceptance does not prove delivery to a device.

## Configure credentials

Create topic-specific APNs provider keys in the Apple Developer account. The
sandbox and production keys must cover the bundle IDs in `APNS_TOPICS`. Official
builds use `com.supaterm.supacode` and `com.supaterm.supacode.preview`. Development
builds use `com.supaterm.supacode.dev`, which needs its own covered topic. Forks
need keys from their signing team and a gateway with their bundle IDs allowed.

Keep `.p8` files in the password vault and mount them read-only for this service.
Do not put keys in the repository, image, environment values, or logs. When
exporting an authorized vault item, set `umask 077` and redirect its password
field into the mounted key file. Keep each environment's key ID with that file.

Set the gateway's environment:

```sh
PUSH_ADDR=127.0.0.1:8081
PUSH_DATABASE=/var/lib/supacode-push/push.sqlite
APNS_TEAM_ID=your-apple-team-id
APNS_TOPICS=com.supaterm.supacode,com.supaterm.supacode.preview
APNS_SANDBOX_KEY_ID=your-sandbox-key-id
APNS_SANDBOX_KEY_FILE=/run/secrets/apns-sandbox.p8
APNS_PRODUCTION_KEY_ID=your-production-key-id
APNS_PRODUCTION_KEY_FILE=/run/secrets/apns-production.p8
```

At least one environment credential is required. Enable Push Notifications on
each App ID and regenerate affected provisioning profiles before releasing the
iOS app. The app reads its signed APNs entitlement to select sandbox or production.

## Run and expose

`make build` produces `bin/push`. Run it with the environment above, then check
`GET /healthz`. Put its public API behind a TLS proxy on a separate hostname such
as `push.example.com`. Keep the listener private to that proxy and allow outbound
HTTPS to Apple's sandbox and production APNs endpoints.

Build the separate container with:

```sh
docker build -f Dockerfile.push -t supacode-push .
```

Mount a writable persistent `/data` directory owned by UID 65532 and read-only
key files. The image defaults to port 8081 and `/data/push.sqlite`. Protect the
whole database directory: its WAL and backup files also contain recipient tokens.

Set **Settings → General → Push gateway** on the Supacode host to the HTTPS
origin, then enroll a device from its iOS notification settings. A foreground
ownership challenge is hidden; a background challenge can be confirmed by tapping
its generic alert. If enrollment was interrupted, retry after one minute.

## Recovery and limits

Use one persistent SQLite database per gateway deployment. Multiple local
processes can share it, and delivery claims are fenced against stale workers.
Separate databases cannot coordinate recipient state or deduplication. SQLite
WAL must stay on a local filesystem; horizontal replicas need shared SQL storage.

The registry admits at most 100,000 recipients, including at most 10,000 pending
recipients and 64 per host. Unconfirmed entries expire after 24 hours. Revoked
entries retain their generation fence for 30 days. Delivery receipts expire five
minutes after the notification expires, with at most 500,000 retained receipts.
Capacity or transient APNs failures return a retryable response to the host.

`GET /metrics` exposes aggregate acceptance, retry, and failure counters without
tokens, recipient IDs, or payloads. Check it and `/healthz` after a restart. A
crash after APNs acceptance can cause a repeated delivery attempt; notification
IDs allow the client to reconcile duplicate taps. This is best-effort delivery.
