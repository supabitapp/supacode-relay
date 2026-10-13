#!/usr/bin/env bash
set -euo pipefail

sudo test -f /etc/supacode-push/push.env
sudo test -f /etc/supacode-push/apns-sandbox.p8
sudo test -f /etc/supacode-push/apns-production.p8
sudo install -m 0755 /tmp/push /usr/local/bin/supacode-push.next
sudo mv /usr/local/bin/supacode-push.next /usr/local/bin/supacode-push
sudo install -m 0644 /tmp/supacode-push.service /etc/systemd/system/supacode-push.service
sudo systemctl daemon-reload
sudo systemctl restart supacode-push.service
ready=false
for _ in $(seq 40); do
  if curl -fsS http://127.0.0.1:18771/healthz; then
    ready=true
    break
  fi
  sleep 0.5
done
if [[ "$ready" != true ]]; then
  sudo journalctl -u supacode-push -n 30 --no-pager
  exit 1
fi
sudo systemctl enable supacode-push.service
rm /tmp/push /tmp/supacode-push.service
