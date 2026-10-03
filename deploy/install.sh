#!/usr/bin/env bash
set -euo pipefail
sudo install -m 0755 /tmp/relay /usr/local/bin/supacode-relay
sudo install -m 0644 /tmp/supacode-relay.service /etc/systemd/system/supacode-relay.service
rm /tmp/relay /tmp/supacode-relay.service
sudo systemctl daemon-reload
sudo systemctl enable supacode-relay
sudo systemctl restart supacode-relay
for _ in $(seq 20); do
  if curl -fsS localhost:8080/healthz; then
    exit 0
  fi
  sleep 0.5
done
sudo journalctl -u supacode-relay -n 50 --no-pager
exit 1
