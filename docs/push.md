# APNs gateway

`cmd/push` runs an optional notification service beside the live relay. Run it as
a separate process and HTTPS origin. The existing relay deployment does not
install or expose it.

The gateway sends the fixed fallback alert “Supacode has an update” with opaque
recipient and notification identifiers. Optional encrypted previews pass through
unchanged and activate the iOS Notification Service Extension. The gateway
validates their encoding and byte limits without decrypting them. It stores
APNs tokens, recipient generations, revocation hashes, and bounded delivery
receipts. It receives no conversation plaintext and has no notification queue.
The Supacode host owns unsent work and
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

## Managed deployment

The production gateway runs on its own exe.dev VM, `supacode-push.exe.xyz`.
exe.dev's public HTTPS proxy forwards to `127.0.0.1:18771` and serves
`push.supacode.sh`. The gateway has one systemd unit and a separate deployment
workflow. Push-only changes do not restart the relay. The workflow uses the
`production` environment's `EXE_PUSH_SSH_KEY`, scoped to the
`supacode-push-deploy` VM tag, and pins exe.dev's published SSH host key. The key
is backed up in the password vault.

Create the VM with:

```sh
ssh exe.dev new --name=supacode-push --cpu=1 --memory=2GB --disk=25GB --no-email --json
ssh exe.dev tag supacode-push supacode-push-deploy --json
```

Provision `/etc/supacode-push` with mode `0700` and these root-owned files with
mode `0600` before running the installer:

- `push.env`: team ID, allowed topics, sandbox key ID, and production key ID.
- `apns-sandbox.p8` and `apns-production.p8`: the matching vault exports.

Build `cmd/push` for Linux AMD64, copy it to `/tmp/push`, and copy
`deploy/supacode-push.service` to `/tmp/` on the deployment host. Run
[`deploy/install-push.sh`](../deploy/install-push.sh) over SSH. It installs the
binary and unit, checks local gateway health, and enables the service. The unit
loads credentials through systemd and protects the persistent SQLite directory
with mode `0700`.

Select the gateway port and make its HTTPS proxy public:

```sh
ssh exe.dev share port supacode-push 18771 --json
ssh exe.dev share set-public supacode-push --json
```

Point a DNS-only CNAME for `push.supacode.sh` at `supacode-push.exe.xyz`, then
register the domain:

```sh
ssh exe.dev domain add supacode-push push.supacode.sh --json
```

exe.dev issues the TLS certificate. Keep Cloudflare proxying and CNAME
flattening disabled for this record, as required by
[exe.dev custom domains](https://exe.dev/docs/cnames).

Check `/healthz` through both the VM hostname and custom hostname, and confirm
an unsigned request to `/v1/push/enroll` returns `401`. After a gateway restart,
check health and recipient state again.
