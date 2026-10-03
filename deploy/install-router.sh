#!/usr/bin/env bash
set -euo pipefail
sudo test -s /etc/supacode-relay/directory-token || { echo "missing /etc/supacode-relay/directory-token" >&2; exit 1; }
sudo install -m 0755 /tmp/relay-router /usr/local/bin/supacode-relay-router
sudo install -m 0644 /tmp/supacode-relay-router.service /etc/systemd/system/supacode-relay-router.service
rm /tmp/relay-router /tmp/supacode-relay-router.service
sudo systemctl daemon-reload
sudo systemctl disable --now supacode-relay.service 2>/dev/null || true
sudo systemctl enable supacode-relay-router
sudo systemctl restart supacode-relay-router
for _ in $(seq 40); do
  if curl -fsS localhost:9090/healthz; then
    exit 0
  fi
  sleep 0.5
done
curl -sS localhost:9090/metrics || true
sudo journalctl -u supacode-relay-router -n 50 --no-pager
exit 1
