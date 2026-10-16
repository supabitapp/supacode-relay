#!/usr/bin/env bash
set -euo pipefail

root=/usr/local/lib/supaterm-relay
binary=/usr/local/bin/supaterm-relay
unit=/etc/systemd/system/supaterm-relay.service

restore() {
  sudo test -f "$root/previous/relay"
  sudo test -f "$root/previous/service"
  sudo systemctl stop supaterm-relay.service
  sudo install -m 0755 "$root/previous/relay" "$binary"
  sudo install -m 0644 "$root/previous/service" "$unit"
  if sudo test -s "$root/previous/current"; then
    previous=$(sudo cat "$root/previous/current")
    sudo ln -sfn "$previous" "$root/current.next"
    sudo mv -Tf "$root/current.next" "$root/current"
  fi
  sudo systemctl daemon-reload
  sudo systemctl start supaterm-relay.service
}

healthy() {
  for _ in $(seq 40); do
    if curl -fsS http://127.0.0.1:9090/healthz >/dev/null; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

if [[ ${1:-} == --rollback ]]; then
  restore
  healthy
  exit
fi

archive=${1:-/tmp/relay-ts-linux-x64.tar.gz}
archive=$(realpath "$archive")
cd "$(dirname "$archive")"
sha256sum -c "$(basename "$archive").sha256"
digest=$(sha256sum "$archive" | cut -d ' ' -f 1)
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
tar -xzf "$archive" -C "$stage" --no-same-owner
test -x "$stage/relay"
test -x "$stage/node/bin/node"
test -f "$stage/app/main.js"
"$stage/node/bin/node" --input-type=module - "$stage" <<'JS'
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { resolve, sep } from "node:path";
const root = resolve(process.argv[2]);
const manifest = JSON.parse(readFileSync(resolve(root, "manifest.json"), "utf8"));
for (const [file, expected] of Object.entries(manifest.files)) {
  const path = resolve(root, file);
  if (!path.startsWith(root + sep)) throw new Error("invalid manifest path");
  if (createHash("sha256").update(readFileSync(path)).digest("hex") !== expected) throw new Error("release checksum mismatch");
}
if (process.versions.node !== manifest.node) throw new Error("release runtime mismatch");
JS
sudo install -d "$root/releases/$digest" "$root/previous"
sudo cp -a "$stage/." "$root/releases/$digest/"
sudo chmod -R a+rX "$root/releases/$digest"
sudo install -m 0755 "$binary" "$root/previous/relay"
sudo install -m 0644 "$unit" "$root/previous/service"
sudo readlink "$root/current" | sudo tee "$root/previous/current" >/dev/null || true
activate() {
  sudo systemctl stop supaterm-relay.service &&
    sudo ln -sfn "$root/releases/$digest" "$root/current.next" &&
    sudo mv -Tf "$root/current.next" "$root/current" &&
    sudo install -m 0755 "$root/current/relay" "$binary" &&
    sudo install -m 0644 "$root/current/supaterm-relay.service" "$unit" &&
    sudo systemctl daemon-reload &&
    sudo systemctl start supaterm-relay.service
}
if activate && healthy; then
  sudo systemctl enable supaterm-relay.service
else
  restore
  healthy
  exit 1
fi
