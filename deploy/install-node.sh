#!/usr/bin/env bash
set -euo pipefail
node="$1"
sudo test -s /etc/supacode-relay/directory-token || { echo "missing /etc/supacode-relay/directory-token" >&2; exit 1; }
sudo install -m 0755 /tmp/relay /usr/local/bin/supacode-relay
sudo install -m 0644 /tmp/supacode-relay-node.service /etc/systemd/system/supacode-relay.service
printf 'RELAY_NODE_ID=%s\nRELAY_ADVERTISE_URL=https://relay-%s.int.exe.xyz\n' "$node" "$node" | sudo tee /etc/supacode-relay/node.env >/dev/null
rm /tmp/relay /tmp/supacode-relay-node.service
sudo systemctl daemon-reload
sudo systemctl enable supacode-relay
sudo systemctl restart supacode-relay
for _ in $(seq 40); do
  if curl -fsS localhost:9090/metrics | grep -Eq '"directoryStreams":[[:space:]]*1[[:space:]]*[,}]'; then
    echo "$node joined the router"
    exit 0
  fi
  sleep 0.5
done
echo "$node failed to join the router" >&2
sudo journalctl -u supacode-relay -n 50 --no-pager
exit 1
