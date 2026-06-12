#!/usr/bin/env bash
# Verify the collector health endpoint. Exit 0 when healthy, non-zero otherwise.
set -euo pipefail

REPO_ROOT="/opt/binance-futures-collector"
ENV_FILE="${REPO_ROOT}/deployment/systemd/binance-futures-collector.env"

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  set -a
  source "${ENV_FILE}"
  set +a
fi

HEALTH_HOST="${HEALTH_HOST:-127.0.0.1}"
HEALTH_PORT="${HEALTH_PORT:-8080}"
HEALTH_MAX_OI_AGE_MINUTES="${HEALTH_MAX_OI_AGE_MINUTES:-5}"
HEALTH_MAX_FUNDING_AGE_MINUTES="${HEALTH_MAX_FUNDING_AGE_MINUTES:-15}"
URL="http://${HEALTH_HOST}:${HEALTH_PORT}/status"

if ! command -v curl >/dev/null 2>&1; then
  echo "health-check: curl is required" >&2
  exit 2
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "health-check: python3 is required" >&2
  exit 2
fi

response="$(curl --fail --silent --show-error --max-time 5 "${URL}")" || {
  echo "health-check: HTTP request failed for ${URL}" >&2
  exit 1
}

python3 - "${response}" "${HEALTH_MAX_OI_AGE_MINUTES}" "${HEALTH_MAX_FUNDING_AGE_MINUTES}" <<'PY'
import json
import sys
from datetime import UTC, datetime, timedelta

payload = json.loads(sys.argv[1])
max_oi_minutes = float(sys.argv[2])
max_funding_minutes = float(sys.argv[3])

required = ("uptime_hours", "symbols", "last_oi_update", "last_funding_update")
missing = [key for key in required if key not in payload]
if missing:
    raise SystemExit(f"health-check: missing fields: {', '.join(missing)}")

symbols = int(payload["symbols"])
if symbols <= 0:
    raise SystemExit("health-check: symbols must be > 0")

uptime_hours = float(payload["uptime_hours"])
if uptime_hours < 0:
    raise SystemExit("health-check: invalid uptime_hours")

def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).astimezone(UTC)

now = datetime.now(UTC)

if uptime_hours >= max_oi_minutes / 60.0:
    last_oi = parse_iso(payload.get("last_oi_update"))
    if last_oi is None:
        raise SystemExit("health-check: last_oi_update is stale/missing")
    if now - last_oi > timedelta(minutes=max_oi_minutes):
        raise SystemExit(
            f"health-check: last_oi_update older than {max_oi_minutes} minutes"
        )

if uptime_hours >= max_funding_minutes / 60.0:
    last_funding = parse_iso(payload.get("last_funding_update"))
    if last_funding is None:
        raise SystemExit("health-check: last_funding_update is stale/missing")
    if now - last_funding > timedelta(minutes=max_funding_minutes):
        raise SystemExit(
            f"health-check: last_funding_update older than {max_funding_minutes} minutes"
        )

print(
    "health-check: ok "
    f"uptime={payload['uptime_hours']}h symbols={symbols} "
    f"trades={payload.get('trades_received', 0)}"
)
PY
