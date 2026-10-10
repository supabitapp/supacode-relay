#!/usr/bin/env bash
set -euo pipefail
restore_router=false
if sudo systemctl is-active --quiet supacode-relay-router.service; then
  restore_router=true
  sudo systemctl stop supacode-relay-router.service
fi
rollback() {
  if [[ "$restore_router" == true ]]; then
    sudo systemctl stop supacode-relay.service || true
    sudo systemctl start supacode-relay-router.service
  fi
}
trap rollback ERR

sudo install -m 0755 /tmp/relay /usr/local/bin/supacode-relay
sudo install -m 0644 /tmp/supacode-relay.service /etc/systemd/system/supacode-relay.service
rm /tmp/relay /tmp/supacode-relay.service
sudo systemctl daemon-reload
sudo systemctl restart supacode-relay.service
ready=false
for _ in $(seq 40); do
  if curl -fsS localhost:9090/healthz; then
    ready=true
    break
  fi
  sleep 0.5
done
if [[ "$ready" != true ]]; then
  sudo journalctl -u supacode-relay -n 50 --no-pager
  rollback
  exit 1
fi
sudo systemctl enable supacode-relay.service
restore_router=false
if sudo systemctl cat supacode-relay-router.service >/dev/null 2>&1; then
  sudo systemctl disable supacode-relay-router.service
fi
sudo rm -f /etc/systemd/system/supacode-relay-router.service /usr/local/bin/supacode-relay-router
sudo rm -f /etc/supacode-relay/directory-token /etc/supacode-relay/node.env
sudo systemctl daemon-reload
