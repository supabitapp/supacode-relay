#!/usr/bin/env bash
set -euo pipefail
sudo install -m 0755 /tmp/relay /usr/local/bin/supaterm-relay
sudo install -m 0644 /tmp/supaterm-relay.service /etc/systemd/system/supaterm-relay.service
rm /tmp/relay /tmp/supaterm-relay.service
sudo systemctl daemon-reload
sudo systemctl restart supaterm-relay.service
for _ in $(seq 40); do
  if curl -fsS localhost:9090/healthz; then
    sudo systemctl enable supaterm-relay.service
    exit 0
  fi
  sleep 0.5
done
sudo journalctl -u supaterm-relay -n 50 --no-pager
exit 1
