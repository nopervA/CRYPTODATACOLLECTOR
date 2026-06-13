#!/usr/bin/env bash
# Apply reliability hardening on a running VM (run from repo root or /opt/...).
set -euo pipefail

REPO_ROOT="${1:-/opt/binance-futures-collector}"
cd "${REPO_ROOT}"

echo "==> Repairing script permissions"
chmod +x deployment/scripts/*.sh
chmod 0755 .venv/bin/python 2>/dev/null || true

echo "==> Reloading systemd"
sudo cp deployment/systemd/binance-futures-collector.service /etc/systemd/system/ 2>/dev/null || \
  sudo ln -sf "${REPO_ROOT}/deployment/systemd/binance-futures-collector.service" \
    /etc/systemd/system/binance-futures-collector.service
sudo systemctl daemon-reload

echo "==> Running preflight (as root, before restart)"
sudo deployment/scripts/ensure-runtime.sh
sudo deployment/scripts/preflight.sh

echo "==> Restarting collector"
sudo systemctl restart binance-futures-collector
sleep 5

echo "==> Health check"
sudo deployment/scripts/health-check.sh

echo "==> /status snapshot"
curl -sf "http://127.0.0.1:${HEALTH_PORT:-8080}/status" | python3 -m json.tool

echo "Deploy complete."
